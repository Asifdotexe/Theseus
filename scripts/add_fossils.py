"""
Fossil Finder — Backfill & Incremental Update Script
=====================================================
Manages two fossil types for each repository's data JSON without touching
snapshot data.

Fossil data model
-----------------
A **fossil** is a single source-code line whose author-timestamp is the oldest
ever found in a given scope.  Each fossil records:

``timestamp``
    Unix-epoch author-time.
``file``
    Relative file path.
``content``
    The actual source line text.
``year``
    4-digit year derived from ``timestamp``.
``commit``
    First 7 characters of the commit that last modified this line.
``view_commit``
    The git ref (commit hash or branch name) at which the file is checked out.
``line``
    1-based line number within the file.

Two concrete fossil types are discovered by this script:

  **Genesis** (Historical Fossil)
      The single oldest-authored line *ever* to exist in the repository.
      Found by scanning the earliest commits sorted by author-time, blaming
      only the files that were *added* in each commit, and returning the line
      with the smallest author-timestamp across all scanned commits.
      An early-exit heuristic stops after ``stale_limit`` consecutive commits
      that fail to improve the oldest-yet result.

  **Survivor** (Living Fossil)
      The single oldest-authored line that *still exists* at the current HEAD.
      Found by blaming every tracked file on the default branch and returning
      the line with the smallest author-timestamp.

Modes
-----
  (no flags)          Full backfill: recompute both Genesis and Survivor
                      for all repos.
  --update-survivor   Incremental: only refresh the Survivor fossil.
                      Skips writing to disk if the file:line:commit has not
                      changed.  Used by the GitHub Actions workflow.
  --only REPO         Limit processing to a single named repo.
"""

import argparse
import logging
import sys
from pathlib import Path

import _path_guard  # noqa: F401  # pylint: disable=unused-import

from _blame import BlameRunner, _blank_fossil
from _data_io import load_snapshot_data, save_snapshot_data
from _utils import (
    get_default_branch,
    get_tracked_files,
    load_config,
    remove_path,
    run_command,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fossil_identity(fossil: dict) -> tuple:
    """
    Return a hashable key identifying which line this fossil refers to.

    Uses ``(file, blame_commit)`` — the authoring commit uniquely identifies
    the content.  Line numbers are intentionally excluded: a line that stays
    in the same file but shifts position (due to insertions/deletions above it)
    is still the same fossil.  Only a change in file or authoring commit
    (meaning the line was actually rewritten) counts as a different fossil.

    :param fossil: A fossil dict.
    :return: ``(file, commit)`` tuple.
    """
    return (fossil.get("file", ""), fossil.get("commit", ""))


def _get_files_added_in_commit(repo_path: str | Path, commit_hash: str) -> list[str]:
    """
    Return files that were *added* (not modified, not renamed) by this commit.

    Uses ``git diff-tree --diff-filter=A`` which only lists new files
    introduced in the commit, compared to its parent(s).  For the root commit
    (no parent) the command fails so we fall back to ``git ls-files``.

    :param repo_path: Path to the git repository.
    :param commit_hash: The commit to inspect.
    :return: List of relative file paths added in this commit.
    """
    try:
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
            cwd=str(repo_path),
        )
        return files_output.splitlines() if files_output else []
    except RuntimeError:
        files_output = run_command(["git", "ls-files"], cwd=str(repo_path))
        return files_output.splitlines()


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
    Sort ALL commits by author-time, then scan the oldest ``genesis_depth``
    commits.  Within each commit, blame only files that were *added* in that
    commit (files from older commits have already been blamed in previous
    iterations).  An early-exit heuristic stops after ``stale_limit``
    consecutive commits that fail to improve the oldest-yet result.

    :param repo_path: Path to the git repository.
    :param genesis_depth: Maximum number of oldest commits to scan (default 50).
    :param stale_limit: Stop after this many consecutive commits with no
                        improvement (default 5).
    :return: A fossil dict for the oldest line ever found, or a blank fossil
             if no lines could be blamed.
    """
    logger.info("Computing Genesis (Historical) fossil...")

    log_output = run_command(
        ["git", "log", "--all", "--pretty=format:%H %at"],
        cwd=str(repo_path),
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
            run_command(["git", "checkout", "--force", commit], cwd=str(repo_path))
        except RuntimeError as e:
            logger.warning("  Could not checkout %s: %s", commit[:7], e)
            continue

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

        fossil = BlameRunner(repo_path, max_workers=20).blame_oldest_fossil(
            files, view_commit=commit
        )

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

    :param repo_path: Path to the git repository.
    :return: A fossil dict for the oldest line still present, or a blank
             fossil if no lines could be blamed.
    """
    logger.info("Computing Survivor (Living) fossil...")

    default_branch = get_default_branch(str(repo_path))
    logger.info("  Checking out default branch: %s", default_branch)

    try:
        run_command(
            ["git", "checkout", "-B", default_branch, f"origin/{default_branch}"],
            cwd=str(repo_path),
        )
    except RuntimeError:
        run_command(
            ["git", "checkout", "--force", f"origin/{default_branch}"],
            cwd=str(repo_path),
        )

    view_commit = default_branch

    tracked_files = get_tracked_files(str(repo_path))
    if not tracked_files:
        logger.warning("No tracked files found at HEAD.")
        return _blank_fossil()

    logger.info("  Blaming %d tracked files...", len(tracked_files))
    return BlameRunner(repo_path, max_workers=20).blame_oldest_fossil(
        tracked_files, view_commit=view_commit
    )


# ---------------------------------------------------------------------------
# Shared repo-iteration helper
# ---------------------------------------------------------------------------


def _process_each_repo(
    data_dir: str,
    repo_urls: dict[str, str],
    process_fn,
    log_prefix: str = "Processing",
) -> bool:
    """
    Iterate over every JSON file in ``data_dir/raw/``, clone/fetch each repo,
    call *process_fn* with ``(json_file, snapshots, existing_fossils, local_repo, repo_name)``,
    then clean up the temp directory.

    Handles iteration, cloning, fetch, temp-dir cleanup, and error logging.
    *process_fn* should return ``None`` for success or a string error message.

    :param data_dir: Path to the ``data/`` directory.
    :param repo_urls: ``{repo_name: clone_url}`` mapping.
    :param process_fn: Callback taking ``(json_file, snapshots, existing_fossils, local_repo, repo_name)``.
    :param log_prefix: Prefix for log messages (e.g. ``"Processing"``, ``"Checking survivor for"``).
    :return: ``True`` if any errors occurred.
    """
    data_path = Path(data_dir) / "raw"
    had_failures = False

    for json_file in sorted(data_path.glob("*.json")):
        if json_file.name == "manifest.json":
            continue

        repo_name = json_file.stem.replace("_data", "")
        repo_url = repo_urls.get(repo_name)
        if not repo_url:
            logger.warning("No URL found for '%s', skipping.", repo_name)
            continue

        logger.info("━━━ %s: %s ━━━", log_prefix, repo_name)

        data = load_snapshot_data(str(json_file))
        snapshots = data["snapshots"]
        existing_fossils = data.get("fossils", {})

        if not snapshots:
            logger.warning("  No snapshots found in %s, skipping.", json_file.name)
            continue

        base_temp = Path("./temp_fossil_repos")
        base_temp.mkdir(exist_ok=True)
        local_repo = base_temp / repo_name

        if not local_repo.exists():
            logger.info("  Cloning %s...", repo_url)
            run_command(["git", "clone", repo_url, str(local_repo)])
        else:
            logger.info("  Repo already cloned — fetching latest...")
            try:
                run_command(["git", "fetch", "--all"], cwd=str(local_repo))
            except RuntimeError as e:
                logger.warning("  Fetch failed (continuing with local): %s", e)

        try:
            error = process_fn(json_file, snapshots, existing_fossils, local_repo, repo_name)
            if error:
                logger.error("  ✗ %s", error)
                had_failures = True
            else:
                logger.info("  ✓ %s", repo_name)
        except Exception as e:  # noqa: BLE001
            logger.error("  ✗ Error processing %s: %s", repo_name, e)
            had_failures = True

        if local_repo.exists():
            remove_path(str(local_repo))

    return had_failures


# ---------------------------------------------------------------------------
# Full backfill driver
# ---------------------------------------------------------------------------


def _backfill_one(
    json_file: Path, snapshots: list, _fossils: dict, local_repo: Path, repo_name: str
) -> str | None:
    """Compute both fossils for a single repo; return error string or ``None``."""
    genesis = get_genesis_fossil(local_repo)
    survivor = get_survivor_fossil(local_repo)
    new_fossils = {"genesis": genesis, "survivor": survivor}

    if not genesis.get("file"):
        logger.warning("  ⚠ Genesis fossil is empty for %s", repo_name)
    if not survivor.get("file"):
        logger.warning("  ⚠ Survivor fossil is empty for %s", repo_name)
    if genesis.get("commit") == survivor.get("commit") and genesis.get("file"):
        logger.warning(
            "  ⚠ Genesis and Survivor share the same commit (%s) "
            "— may indicate the repo was never fully rewritten.",
            genesis["commit"],
        )

    logger.info(
        "  Genesis  → %s | %s:%s | %s",
        genesis.get("year"), genesis.get("file"), genesis.get("line"), genesis.get("commit"),
    )
    logger.info(
        "  Survivor → %s | %s:%s | %s",
        survivor.get("year"), survivor.get("file"), survivor.get("line"), survivor.get("commit"),
    )

    save_snapshot_data(str(json_file), snapshots, new_fossils)


def backfill_fossils(data_dir: str, repo_urls: dict[str, str]) -> bool:
    """
    For every repo JSON in ``data_dir``, recompute both fossils without
    touching snapshot data.

    Always forces a fresh recompute of both genesis and survivor for every
    repository.

    :param data_dir: Path to the ``data/`` directory.
    :param repo_urls: ``{repo_name: clone_url}`` mapping.
    :return: ``True`` if any errors occurred, ``False`` otherwise.
    """
    return _process_each_repo(data_dir, repo_urls, _backfill_one, log_prefix="Processing")


# ---------------------------------------------------------------------------
# Incremental survivor-only update (used by GitHub Actions)
# ---------------------------------------------------------------------------


def _update_survivor_one(
    json_file: Path, snapshots: list, existing_fossils: dict,
    local_repo: Path, repo_name: str,
) -> str | None:
    """Update survivor fossil for a single repo; return error string or ``None``."""
    existing_survivor = existing_fossils.get("survivor", {})
    new_survivor = get_survivor_fossil(local_repo)

    old_identity = _fossil_identity(existing_survivor)
    new_identity = _fossil_identity(new_survivor)
    metadata_changed = existing_survivor.get("view_commit") != new_survivor.get("view_commit")

    if old_identity == new_identity and not metadata_changed:
        logger.info(
            "  ✓ Survivor unchanged: %s:%s (commit %s) — skipping write.",
            new_survivor.get("file"),
            new_survivor.get("line"),
            new_survivor.get("commit"),
        )
        return None

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

    updated_fossils = {**existing_fossils, "survivor": new_survivor}
    save_snapshot_data(str(json_file), snapshots, updated_fossils)
    return None


def update_survivor_fossils(data_dir: str, repo_urls: dict[str, str]) -> bool:
    """
    Refresh only the Survivor (Living) fossil for each repo.

    Skips writing to disk if the fossil's ``file:line:commit`` has not changed.
    Designed to run on every monthly cron tick so the living fossil stays
    current even when no new snapshots are being added.

    :param data_dir: Path to the ``data/`` directory.
    :param repo_urls: ``{repo_name: clone_url}`` mapping.
    :return: ``True`` if any errors occurred, ``False`` otherwise.
    """
    return _process_each_repo(data_dir, repo_urls, _update_survivor_one, log_prefix="Checking survivor for")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """
    Entry point for fossil backfill and incremental survivor checking.

    CLI flags
    ---------
    --only REPO       Process only a single repository (by config name).
    --update-survivor Incremental mode: refresh only the Survivor fossil.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    config = load_config()
    data_dir = config.get("dataDir", "./data")

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
        help="Incremental mode: only refresh the Survivor fossil. Skips write "
        "if unchanged. Genesis is left untouched.",
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
