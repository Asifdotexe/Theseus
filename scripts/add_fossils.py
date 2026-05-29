"""
Fossil Finder — Backfill & Incremental Update Script
=====================================================
Manages two fossil types for each repo's data JSON without touching snapshot data:

  Genesis  (Historical Fossil) — the oldest line **ever written** in this repo's
            entire git history, found by blaming the very first commit(s).

  Survivor (Living Fossil)     — the oldest line that is **still alive today**,
            found by blaming all files at the current default-branch HEAD.

Modes
-----
  (no flags)          Full backfill: recompute both Genesis and Survivor for all repos.
  --update-survivor   Incremental: only refresh the Survivor fossil for each repo,
                      and only write to disk if the file:line has actually changed.
                      This is the mode used by the GitHub Actions workflow.
  --only REPO         Limit processing to a single named repo.
"""

import argparse
import concurrent.futures
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure sibling imports from _utils work in all invocation contexts
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from _utils import get_default_branch, run_command, load_config

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _blank_fossil() -> dict:
    return {
        "timestamp": 2_147_483_647,
        "file": "",
        "content": "",
        "year": "",
        "commit": "",
        "view_commit": "",
        "line": 0,
    }


def _blame_file(repo_path: str | Path, file_path: str, view_commit: str = "") -> dict:
    """Run git blame --line-porcelain on a single file and return the oldest fossil found."""
    try:
        blame_output = run_command(
            ["git", "blame", "--line-porcelain", file_path],
            cwd=repo_path,
        )
    except RuntimeError:
        return _blank_fossil()

    fossil = _blank_fossil()
    current_commit_data = {}
    line_num = 0

    for line in blame_output.splitlines():
        if line.startswith("\t"):
            line_num += 1
            timestamp = current_commit_data.get("author-time")
            content = line.lstrip("\t").strip()
            if timestamp is not None and timestamp < fossil["timestamp"] and content:
                fossil["timestamp"] = timestamp
                fossil["file"] = file_path
                fossil["content"] = content
                fossil["year"] = datetime.fromtimestamp(
                    timestamp, timezone.utc
                ).strftime("%Y")
                fossil["commit"] = current_commit_data.get("commit", "")[:7]
                fossil["view_commit"] = (
                    view_commit  # the checkout commit — file is guaranteed to exist here
                )
                fossil["line"] = line_num
        else:
            parts = line.split(" ")
            if parts and len(parts[0]) in (40, 64):
                current_commit_data = {"commit": parts[0]}
            elif line.startswith("author-time ") and len(parts) >= 2:
                try:
                    current_commit_data["author-time"] = int(parts[1])
                except ValueError:
                    pass

    return fossil


def _blame_files_parallel(
    repo_path: str | Path, files: list[str], view_commit: str = "", max_workers: int = 20
) -> dict:
    """Blame a list of files in parallel and return the single oldest fossil found."""
    global_oldest = _blank_fossil()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_blame_file, repo_path, f, view_commit): f for f in files
        }
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result["timestamp"] < global_oldest["timestamp"] and result["file"]:
                global_oldest = result

    return global_oldest


def _get_tracked_files(repo_path: str | Path) -> list[str]:
    """Return a list of files that are tracked by git and exist on disk."""
    files_output = run_command(["git", "ls-files"], cwd=repo_path)
    return [
        f
        for f in files_output.splitlines()
        if os.path.isfile(os.path.join(str(repo_path), f))
    ]


def _get_files_added_in_commit(repo_path: str | Path, commit_hash: str) -> list[str]:
    """
    Return files that were *added* (not modified, not renamed) by this commit.

    Uses ``git diff-tree --diff-filter=A`` which only lists new files
    introduced in the commit, compared to its parent(s).  For the root
    commit (no parent) the command fails so we fall back to ``git ls-files``.

    Complexity
    ----------
    Before (``_get_tracked_files``):
        O(all_tracked_files) per commit — every file at that checkout is
        blamed, even files that were added centuries earlier.

    After (``_get_files_added_in_commit``):
        O(added_files_only) per commit — only files that first appear in
        this commit are blamed.  Files from older commits were already
        handled in earlier iterations of the genesis loop, so re-blaming
        them is redundant.

    Why this is safe
    ----------------
    ``git blame --line-porcelain`` traces each line back to the commit
    that *last modified* that line.  If a file was added at commit K and
    never touched again, blaming it at K or at any later commit returns
    the same author-time == K.  If a file was added at K and modified at
    K+2, the modified lines will show author-time == K+2, which is never
    older than K.  Therefore the oldest line of any file is found by
    blaming that file exactly once — at the commit where it first
    appeared in the tree.
    """
    try:
        # For non-root commits — compare against parent(s)
        files_output = run_command(
            [
                "git",
                "diff-tree",
                "--no-commit-id",
                "-r",
                "--diff-filter=A",
                "--name-only",
                commit_hash,
            ],
            cwd=repo_path,
        )
        return files_output.splitlines() if files_output else []
    except RuntimeError:
        # Root commit has no parent — all tracked files are "new"
        files_output = run_command(["git", "ls-files"], cwd=repo_path)
        return files_output.splitlines()


def _fossil_identity(fossil: dict) -> tuple:
    """Return a hashable key that identifies which line this fossil refers to.

    Uses (file, blame_commit) — the authoring commit uniquely identifies the
    content.  Line numbers are intentionally excluded: a line that stays in
    the same file but shifts position (due to insertions/deletions above it)
    is still the same fossil.  Only a change in file or authoring commit
    (meaning the line was actually rewritten) counts as a different fossil.
    """
    return (fossil.get("file", ""), fossil.get("commit", ""))


# ---------------------------------------------------------------------------
# Genesis — Historical Fossil
# ---------------------------------------------------------------------------


def get_genesis_fossil(
    repo_path: str | Path,
    genesis_depth: int = 50,
    stale_limit: int = 5,
) -> dict:
    """
    Historical Fossil: the oldest line **ever authored** in this repo.

    Strategy
    --------
    Sort ALL commits by author-time (not committer-time), then scan the oldest
    ``genesis_depth`` commits.  This correctly handles repos migrated from
    SVN/Mercurial where old authored lines may appear in commits with much
    later committer timestamps.

    Early-exit heuristic
    ~~~~~~~~~~~~~~~~~~~~
    Once a fossil has been found, if ``stale_limit`` consecutive older commits
    fail to improve it (no line with a smaller author-time), the scan stops.
    The assumption is that if a long stretch of early commits doesn't contain
    anything older than what we already have, no older line exists anywhere.

    Why this is safe
    ~~~~~~~~~~~~~~~~
    The very first commit (lowest author-time) is always scanned first.  If the
    oldest code was added in one of the earliest commits, it will be found
    immediately.  The stale-limit window (default 5) gives enough room for
    repos where the first commit only contained a README and the real code was
    added in a slightly later commit, while stopping *well* before 50 in the
    common case.

    Before (hardcoded 50)
        Worst case: 50 blame passes over the full file tree at each commit.
        Even with the ``_get_files_added_in_commit`` optimisation, scanning 50
        commits is unnecessary for most repos.

    After (adaptive stale-limit + hard cap)
        Most repos stop after 5--10 commits.  Edge cases (e.g. a repo with
        many distinct old commits that each add new source files) still have
        the hard safety cap of ``genesis_depth=50``.
    """
    logger.info("Computing Genesis (Historical) fossil...")

    # Get every commit with its author-time so we can sort by actual authorship date
    log_output = run_command(
        ["git", "log", "--all", "--pretty=format:%H %at"],
        cwd=repo_path,
    )

    commit_pairs: list[tuple[str, int]] = []
    for line in log_output.splitlines():
        parts = line.strip().split(" ", 1)
        if len(parts) == 2:
            try:
                commit_pairs.append((parts[0], int(parts[1])))
            except ValueError:
                pass

    if not commit_pairs:
        logger.warning("No commits found in repo.")
        return _blank_fossil()

    # Sort by author-time ascending → oldest authored commits first
    commit_pairs.sort(key=lambda x: x[1])
    oldest_commits = [(c[0], c[1]) for c in commit_pairs[:genesis_depth]]

    global_oldest = _blank_fossil()
    stale_count = 0

    for i, (commit, author_ts) in enumerate(oldest_commits):
        logger.info(
            "  Genesis scan: commit %d/%d (%s, at=%s)",
            i + 1,
            len(oldest_commits),
            commit[:7],
            author_ts,
        )
        try:
            run_command(["git", "checkout", "--force", commit], cwd=repo_path)
        except RuntimeError as e:
            logger.warning("  Could not checkout %s: %s", commit[:7], e)
            continue

        # Only blame files that were *added* in this commit, not every
        # tracked file.  Files added in older commits have already been
        # blamed in previous loop iterations — re-blaming them is wasted work.
        # See _get_files_added_in_commit for the full reasoning.
        files = _get_files_added_in_commit(repo_path, commit)
        if not files:
            stale_count += 1
            if stale_count >= stale_limit:
                logger.info(
                    "  Stopping early after %d commits (%d consecutive with no new "
                    "files to blame).",
                    i + 1,
                    stale_limit,
                )
                break
            continue

        fossil = _blame_files_parallel(repo_path, files, view_commit=commit)

        if fossil["file"] and fossil["timestamp"] < global_oldest["timestamp"]:
            global_oldest = fossil
            stale_count = 0
        else:
            stale_count += 1

        if stale_count >= stale_limit:
            logger.info(
                "  Stopping early after %d commits (%d consecutive without "
                "improvement).",
                i + 1,
                stale_limit,
            )
            break

    return global_oldest


# ---------------------------------------------------------------------------
# Survivor — Living Fossil
# ---------------------------------------------------------------------------


def get_survivor_fossil(repo_path: str | Path) -> dict:
    """
    Living Fossil: the oldest line that is **still alive** in the codebase today.

    Strategy: checkout the current default branch HEAD, then blame every file.
    """
    logger.info("Computing Survivor (Living) fossil...")

    default_branch = get_default_branch(repo_path)
    logger.info("  Checking out default branch: %s", default_branch)

    try:
        run_command(
            ["git", "checkout", "-B", default_branch, f"origin/{default_branch}"],
            cwd=repo_path,
        )
    except RuntimeError:
        # Detached HEAD fallback
        run_command(
            ["git", "checkout", "--force", f"origin/{default_branch}"], cwd=repo_path
        )

    # For the Living Fossil, link to the branch name directly (not a frozen commit hash).
    # This means the GitHub URL points to the current, living file — which is what "living" means.
    # The file is guaranteed to exist on this branch since we ls-files it below.
    view_commit = default_branch

    files = _get_tracked_files(repo_path)
    if not files:
        logger.warning("No tracked files found at HEAD.")
        return _blank_fossil()

    return _blame_files_parallel(repo_path, files, view_commit=view_commit)


# ---------------------------------------------------------------------------
# Full backfill driver
# ---------------------------------------------------------------------------


def backfill_fossils(data_dir: str, repo_urls: dict[str, str]) -> bool:
    """
    For every repo JSON in data_dir, recompute both fossils without touching snapshots.
    Always forces a fresh recompute of both genesis and survivor.
    """
    data_path = Path(data_dir)
    temp_dir = Path("./temp_fossil_repos")
    temp_dir.mkdir(exist_ok=True)
    had_failures = False

    for json_file in sorted(data_path.glob("*.json")):
        if json_file.name == "manifest.json":
            continue

        repo_name = json_file.stem.replace("_data", "")
        repo_url = repo_urls.get(repo_name)

        if not repo_url:
            logger.warning("No URL found for '%s', skipping.", repo_name)
            continue

        logger.info("━━━ Processing: %s ━━━", repo_name)

        # 1. Load existing data (snapshots untouched)
        with open(json_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if isinstance(raw_data, list):
            snapshots = raw_data
        else:
            snapshots = raw_data.get("snapshots", [])

        if not snapshots:
            logger.warning("  No snapshots found in %s, skipping.", json_file.name)
            continue

        # 2. Clone the repo if we don't have it locally already
        local_repo = temp_dir / repo_name
        if not local_repo.exists():
            logger.info("  Cloning %s...", repo_url)
            run_command(["git", "clone", repo_url, str(local_repo)])
        else:
            logger.info("  Repo already cloned — fetching latest...")
            try:
                run_command(["git", "fetch", "--all"], cwd=local_repo)
            except RuntimeError as e:
                logger.warning("  Fetch failed (continuing with local): %s", e)

        # 3. Compute fossils
        try:
            genesis = get_genesis_fossil(local_repo)
            survivor = get_survivor_fossil(local_repo)

            fossils = {"genesis": genesis, "survivor": survivor}

            # Validate — warn if something looks wrong
            if not genesis.get("file"):
                logger.warning("  ⚠ Genesis fossil is empty for %s", repo_name)
            if not survivor.get("file"):
                logger.warning("  ⚠ Survivor fossil is empty for %s", repo_name)
            if genesis.get("commit") == survivor.get("commit") and genesis.get("file"):
                logger.warning(
                    "⚠ Genesis and Survivor share the same commit (%s) "
                    "this may indicate the repo was never fully rewritten, which is valid, "
                    "or there may be a data issue.",
                    genesis["commit"],
                )

            logger.info(
                "  Genesis  → %s | %s:%s | %s",
                genesis.get("year"),
                genesis.get("file"),
                genesis.get("line"),
                genesis.get("commit"),
            )
            logger.info(
                "  Survivor → %s | %s:%s | %s",
                survivor.get("year"),
                survivor.get("file"),
                survivor.get("line"),
                survivor.get("commit"),
            )

            # 4. Write back — snapshots are preserved as-is
            tmp_file = json_file.with_suffix(f"{json_file.suffix}.tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(
                    {"snapshots": snapshots, "fossils": fossils},
                    f,
                    separators=(",", ":"),
                )
            os.replace(tmp_file, json_file)

            logger.info("  ✓ Successfully wrote fossils for %s", repo_name)

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("  ✗ Error computing fossils for %s: %s", repo_name, e)
            had_failures = True

    return had_failures


# ---------------------------------------------------------------------------
# Incremental survivor-only update (used by GitHub Actions)
# ---------------------------------------------------------------------------


def update_survivor_fossils(data_dir: str, repo_urls: dict[str, str]) -> bool:
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """
    Refresh only the Survivor (Living) fossil for each repo.
    Skips writing to disk if the fossil's file:line:commit hasn't changed.

    This is designed to be fast and run on every monthly cron tick so that
    the living fossil stays current even when no new snapshots are being added.

    Returns the number of repos where the survivor was updated.
    """
    data_path = Path(data_dir)
    temp_dir = Path("./temp_fossil_repos")
    temp_dir.mkdir(exist_ok=True)

    updated_count = 0
    had_failures = False

    for json_file in sorted(data_path.glob("*.json")):
        if json_file.name == "manifest.json":
            continue

        repo_name = json_file.stem.replace("_data", "")
        repo_url = repo_urls.get(repo_name)

        if not repo_url:
            logger.warning("No URL found for '%s', skipping.", repo_name)
            continue

        logger.info("━━━ Checking survivor for: %s ━━━", repo_name)

        # 1. Load existing data
        with open(json_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if isinstance(raw_data, list):
            snapshots = raw_data
            existing_fossils = {}
        else:
            snapshots = raw_data.get("snapshots", [])
            existing_fossils = raw_data.get("fossils", {})

        if not snapshots:
            logger.warning("  No snapshots found in %s, skipping.", json_file.name)
            continue

        existing_survivor = existing_fossils.get("survivor", {})

        # 2. Clone or fetch the repo
        local_repo = temp_dir / repo_name
        if not local_repo.exists():
            logger.info("  Cloning %s...", repo_url)
            run_command(["git", "clone", repo_url, str(local_repo)])
        else:
            logger.info("  Fetching latest...")
            try:
                run_command(["git", "fetch", "--all"], cwd=local_repo)
            except RuntimeError as e:
                logger.warning("  Fetch failed (continuing with local): %s", e)

        # 3. Compute new survivor
        try:
            new_survivor = get_survivor_fossil(local_repo)

            old_identity = _fossil_identity(existing_survivor)
            new_identity = _fossil_identity(new_survivor)
            metadata_changed = existing_survivor.get("view_commit") != new_survivor.get(
                "view_commit"
            )

            if old_identity == new_identity and not metadata_changed:
                logger.info(
                    "  ✓ Survivor unchanged: %s:%s (commit %s) — skipping write.",
                    new_survivor.get("file"),
                    new_survivor.get("line"),
                    new_survivor.get("commit"),
                )
                continue

            # Something changed — log the diff clearly
            logger.info("  ↻ Survivor updated for %s:", repo_name)
            logger.info(
                "    OLD: %s:%s @ %s",
                existing_survivor.get("file"),
                existing_survivor.get("line"),
                existing_survivor.get("commit"),
            )
            logger.info(
                "    NEW: %s:%s @ %s",
                new_survivor.get("file"),
                new_survivor.get("line"),
                new_survivor.get("commit"),
            )

            # 4. Write back — genesis is preserved, only survivor is replaced
            updated_fossils = {**existing_fossils, "survivor": new_survivor}
            tmp_file = json_file.with_suffix(f"{json_file.suffix}.tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(
                    {"snapshots": snapshots, "fossils": updated_fossils},
                    f,
                    separators=(",", ":"),
                )
            os.replace(tmp_file, json_file)

            logger.info("  ✓ Wrote updated survivor for %s", repo_name)
            updated_count += 1

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("  ✗ Error updating survivor for %s: %s", repo_name, e)
            had_failures = True

    logger.info("\nSurvivor update complete. %d repo(s) updated.", updated_count)
    return had_failures


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    # pylint: disable=duplicate-code
    """
    Main entry point for fossil backfill and incremental survivor checking.
    """
    config = load_config()
    data_dir = config.get("dataDir", "./data")

    # Build dynamically from config: name -> github URL
    repo_urls = {
        repo["name"]: f"https://github.com/{repo['repo']}.git"
        for repo in config.get("repositories", [])
        if "name" in repo and "repo" in repo
    }

    if not repo_urls:
        logger.error("No valid repositories found in configuration.")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Manage fossil data for Theseus repos."
    )
    parser.add_argument(
        "--only",
        metavar="REPO",
        help=f"Process only this repo. Choices: {', '.join(repo_urls)}",
    )
    parser.add_argument(
        "--update-survivor",
        action="store_true",
        help=(
            "Incremental mode: only refresh the Survivor (Living) fossil. "
            "Skips writing if file:line:commit hasn't changed. "
            "Genesis is left untouched. Used by GitHub Actions."
        ),
    )
    args = parser.parse_args()

    if args.only:
        if args.only not in repo_urls:
            parser.error(
                f"Unknown repo '{args.only}'. Valid options: {', '.join(repo_urls)}"
            )
        selected = {args.only: repo_urls[args.only]}
        logger.info("Running for single repo: %s", args.only)
    else:
        selected = repo_urls

    if args.update_survivor:
        logger.info("Mode: incremental survivor update")
        had_failures = update_survivor_fossils(data_dir, selected)
    else:
        logger.info("Mode: full backfill (genesis + survivor)")
        had_failures = backfill_fossils(data_dir, selected)

    if had_failures:
        logger.error("One or more repositories failed to update. Exiting non-zero.")
        sys.exit(1)


if __name__ == "__main__":
    main()
