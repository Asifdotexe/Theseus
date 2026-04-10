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

import json
import os
import logging
import subprocess
import concurrent.futures
from pathlib import Path
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_command(cmd, cwd=None):
    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
            errors="replace",
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Command failed: {' '.join(str(c) for c in cmd)} — {e.stderr}") from e


def _blank_fossil():
    return {"timestamp": 2_147_483_647, "file": "", "content": "", "year": "", "commit": "", "view_commit": "", "line": 0}


def _blame_file(repo_path, file_path, view_commit=""):
    """Run git blame --line-porcelain on a single file and return the oldest fossil found."""
    try:
        blame_output = _run_command(
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
            if timestamp and timestamp < fossil["timestamp"] and content:
                fossil["timestamp"] = timestamp
                fossil["file"] = file_path
                fossil["content"] = content
                fossil["year"] = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y")
                fossil["commit"] = current_commit_data.get("commit", "")[:7]
                fossil["view_commit"] = view_commit  # the checkout commit — file is guaranteed to exist here
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


def _blame_files_parallel(repo_path, files, view_commit="", max_workers=20):
    """Blame a list of files in parallel and return the single oldest fossil found."""
    global_oldest = _blank_fossil()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_blame_file, repo_path, f, view_commit): f
            for f in files
        }
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result["timestamp"] < global_oldest["timestamp"] and result["file"]:
                global_oldest = result

    return global_oldest


def _get_tracked_files(repo_path):
    """Return a list of files that are tracked by git and exist on disk."""
    files_output = _run_command(["git", "ls-files"], cwd=repo_path)
    return [
        f for f in files_output.splitlines()
        if os.path.isfile(os.path.join(str(repo_path), f))
    ]


def _get_default_branch(repo_path):
    """Figure out the default branch name (main vs master vs something else)."""
    # Try the symref approach first (works with a full clone)
    for strategy in [
        ["git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD"],
        ["git", "rev-parse", "--abbrev-ref", "origin/HEAD"],
    ]:
        try:
            result = _run_command(strategy, cwd=repo_path)
            # Strip the "origin/" prefix if present
            branch = result.split("/")[-1]
            if branch:
                return branch
        except RuntimeError:
            continue

    # Fall back to checking which of the usual suspects exists
    for branch in ("main", "master", "develop"):
        try:
            _run_command(["git", "rev-parse", "--verify", f"origin/{branch}"], cwd=repo_path)
            return branch
        except RuntimeError:
            continue

    return "HEAD"


def _fossil_identity(fossil: dict) -> tuple:
    """Return a hashable key that identifies which line this fossil refers to.
    We use file + line-number + blame commit (the actual authoring commit).
    This detects when the living fossil moves to a different line or file.
    """
    return (fossil.get("file", ""), fossil.get("line", 0), fossil.get("commit", ""))


# ---------------------------------------------------------------------------
# Genesis — Historical Fossil
# ---------------------------------------------------------------------------

def get_genesis_fossil(repo_path, genesis_depth=50):
    """
    Historical Fossil: the oldest line **ever authored** in this repo.

    Strategy: Sort ALL commits by author-time (not committer-time), take the
    oldest genesis_depth ones, and blame them. This correctly handles repos
    migrated from SVN/Mercurial where old authored lines may appear in commits
    with much later committer timestamps.
    """
    logger.info("Computing Genesis (Historical) fossil...")

    # Get every commit with its author-time so we can sort by actual authorship date
    log_output = _run_command(
        ["git", "log", "--all", "--pretty=format:%H %at"],
        cwd=repo_path,
    )

    commit_pairs = []
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

    for i, (commit, author_ts) in enumerate(oldest_commits):
        logger.info(f"  Genesis scan: commit {i+1}/{len(oldest_commits)} ({commit[:7]}, at={author_ts})")
        try:
            _run_command(["git", "checkout", "--force", commit], cwd=repo_path)
        except RuntimeError as e:
            logger.warning(f"  Could not checkout {commit[:7]}: {e}")
            continue

        files = _get_tracked_files(repo_path)
        if not files:
            continue

        fossil = _blame_files_parallel(repo_path, files, view_commit=commit)

        if fossil["file"] and fossil["timestamp"] < global_oldest["timestamp"]:
            global_oldest = fossil

    return global_oldest


# ---------------------------------------------------------------------------
# Survivor — Living Fossil
# ---------------------------------------------------------------------------

def get_survivor_fossil(repo_path):
    """
    Living Fossil: the oldest line that is **still alive** in the codebase today.

    Strategy: checkout the current default branch HEAD, then blame every file.
    """
    logger.info("Computing Survivor (Living) fossil...")

    default_branch = _get_default_branch(repo_path)
    logger.info(f"  Checking out default branch: {default_branch}")

    try:
        _run_command(["git", "checkout", "--force", default_branch], cwd=repo_path)
    except RuntimeError:
        # Detached HEAD fallback
        _run_command(["git", "checkout", "--force", f"origin/{default_branch}"], cwd=repo_path)

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

def backfill_fossils(data_dir, repo_urls):
    """
    For every repo JSON in data_dir, recompute both fossils without touching snapshots.
    Always forces a fresh recompute of both genesis and survivor.
    """
    data_path = Path(data_dir)
    temp_dir = Path("./temp_fossil_repos")
    temp_dir.mkdir(exist_ok=True)

    for json_file in sorted(data_path.glob("*.json")):
        if json_file.name == "manifest.json":
            continue

        repo_name = json_file.stem.replace("_data", "")
        repo_url = repo_urls.get(repo_name)

        if not repo_url:
            logger.warning(f"No URL found for '{repo_name}', skipping.")
            continue

        logger.info(f"━━━ Processing: {repo_name} ━━━")

        # 1. Load existing data (snapshots untouched)
        with open(json_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        if isinstance(raw_data, list):
            snapshots = raw_data
        else:
            snapshots = raw_data.get("snapshots", [])

        if not snapshots:
            logger.warning(f"  No snapshots found in {json_file.name}, skipping.")
            continue

        # 2. Clone the repo if we don't have it locally already
        local_repo = temp_dir / repo_name
        if not local_repo.exists():
            logger.info(f"  Cloning {repo_url}...")
            _run_command(["git", "clone", repo_url, str(local_repo)])
        else:
            logger.info(f"  Repo already cloned — fetching latest...")
            try:
                _run_command(["git", "fetch", "--all"], cwd=local_repo)
            except RuntimeError as e:
                logger.warning(f"  Fetch failed (continuing with local): {e}")

        # 3. Compute fossils
        try:
            genesis = get_genesis_fossil(local_repo)
            survivor = get_survivor_fossil(local_repo)

            fossils = {"genesis": genesis, "survivor": survivor}

            # Validate — warn if something looks wrong
            if not genesis.get("file"):
                logger.warning(f"  ⚠ Genesis fossil is empty for {repo_name}")
            if not survivor.get("file"):
                logger.warning(f"  ⚠ Survivor fossil is empty for {repo_name}")
            if genesis.get("commit") == survivor.get("commit") and genesis.get("file"):
                logger.warning(
                    f"  ⚠ Genesis and Survivor share the same commit ({genesis['commit']}) "
                    f"— this may indicate the repo was never fully rewritten, which is valid, "
                    f"or there may be a data issue."
                )

            logger.info(
                f"  Genesis  → {genesis.get('year')} | {genesis.get('file')}:{genesis.get('line')} | {genesis.get('commit')}"
            )
            logger.info(
                f"  Survivor → {survivor.get('year')} | {survivor.get('file')}:{survivor.get('line')} | {survivor.get('commit')}"
            )

            # 4. Write back — snapshots are preserved as-is
            tmp_file = json_file.with_suffix(f"{json_file.suffix}.tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump({"snapshots": snapshots, "fossils": fossils}, f, separators=(",", ":"))
            os.replace(tmp_file, json_file)

            logger.info(f"  ✓ Successfully wrote fossils for {repo_name}")

        except Exception as e:
            logger.error(f"  ✗ Error computing fossils for {repo_name}: {e}")


# ---------------------------------------------------------------------------
# Incremental survivor-only update (used by GitHub Actions)
# ---------------------------------------------------------------------------

def update_survivor_fossils(data_dir, repo_urls):
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

    for json_file in sorted(data_path.glob("*.json")):
        if json_file.name == "manifest.json":
            continue

        repo_name = json_file.stem.replace("_data", "")
        repo_url = repo_urls.get(repo_name)

        if not repo_url:
            logger.warning(f"No URL found for '{repo_name}', skipping.")
            continue

        logger.info(f"━━━ Checking survivor for: {repo_name} ━━━")

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
            logger.warning(f"  No snapshots found in {json_file.name}, skipping.")
            continue

        existing_survivor = existing_fossils.get("survivor", {})

        # 2. Clone or fetch the repo
        local_repo = temp_dir / repo_name
        if not local_repo.exists():
            logger.info(f"  Cloning {repo_url}...")
            _run_command(["git", "clone", repo_url, str(local_repo)])
        else:
            logger.info("  Fetching latest...")
            try:
                _run_command(["git", "fetch", "--all"], cwd=local_repo)
            except RuntimeError as e:
                logger.warning(f"  Fetch failed (continuing with local): {e}")

        # 3. Compute new survivor
        try:
            new_survivor = get_survivor_fossil(local_repo)

            old_identity = _fossil_identity(existing_survivor)
            new_identity = _fossil_identity(new_survivor)
            metadata_changed = (
                existing_survivor.get("view_commit") != new_survivor.get("view_commit")
            )

            if old_identity == new_identity and not metadata_changed:
                logger.info(
                    f"  ✓ Survivor unchanged: {new_survivor.get('file')}:{new_survivor.get('line')} "
                    f"(commit {new_survivor.get('commit')}) — skipping write."
                )
                continue

            # Something changed — log the diff clearly
            logger.info(f"  ↻ Survivor updated for {repo_name}:")
            logger.info(f"    OLD: {existing_survivor.get('file')}:{existing_survivor.get('line')} @ {existing_survivor.get('commit')}")
            logger.info(f"    NEW: {new_survivor.get('file')}:{new_survivor.get('line')} @ {new_survivor.get('commit')}")

            # 4. Write back — genesis is preserved, only survivor is replaced
            updated_fossils = {**existing_fossils, "survivor": new_survivor}
            tmp_file = json_file.with_suffix(f"{json_file.suffix}.tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump({"snapshots": snapshots, "fossils": updated_fossils}, f, separators=(",", ":"))
            os.replace(tmp_file, json_file)

            logger.info(f"  ✓ Wrote updated survivor for {repo_name}")
            updated_count += 1

        except Exception as e:
            logger.error(f"  ✗ Error updating survivor for {repo_name}: {e}")

    logger.info(f"\nSurvivor update complete. {updated_count} repo(s) updated.")
    return updated_count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    REPO_URLS = {
        "react":       "https://github.com/facebook/react.git",
        "numpy":       "https://github.com/numpy/numpy.git",
        "langchain":   "https://github.com/langchain-ai/langchain.git",
        "zed":         "https://github.com/zed-industries/zed.git",
        "claude-code": "https://github.com/anthropics/claude-code.git",
    }

    parser = argparse.ArgumentParser(description="Manage fossil data for Theseus repos.")
    parser.add_argument(
        "--only",
        metavar="REPO",
        help=f"Process only this repo. Choices: {', '.join(REPO_URLS)}",
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
        if args.only not in REPO_URLS:
            parser.error(f"Unknown repo '{args.only}'. Valid options: {', '.join(REPO_URLS)}")
        selected = {args.only: REPO_URLS[args.only]}
        logger.info(f"Running for single repo: {args.only}")
    else:
        selected = REPO_URLS

    if args.update_survivor:
        logger.info("Mode: incremental survivor update")
        update_survivor_fossils("./data", selected)
    else:
        logger.info("Mode: full backfill (genesis + survivor)")
        backfill_fossils("./data", selected)
