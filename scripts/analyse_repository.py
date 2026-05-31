"""
This script is responsible for doing the heavy lifting.
Processes repository snapshots incrementally to track code age distribution.
Uses quarterly resolution for historical data (pre-2025) and monthly for recent data (2025+).

Fossil computation is handled separately by add_fossils.py.
"""

import argparse
import concurrent.futures
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from itertools import groupby

# Ensure sibling imports from _utils work in all invocation contexts
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from _utils import run_command, get_default_branch, load_config, remove_path

logger = logging.getLogger(__name__)


def clone_repository(repo_slug: str, clone_dir: str) -> None:
    """
    Dynamically clone a GitHub repository given its owner/name slug.

    :param repo_slug: The GitHub repository identifier (e.g., 'facebook/react').
    :param clone_dir: The local directory where the repository should be cloned.
    """
    logger.info("Cloning %s into %s...", repo_slug, clone_dir)
    repo_url = f"https://github.com/{repo_slug}.git"
    run_command(["git", "clone", repo_url, clone_dir])


def get_snapshots(repo_path: str) -> list[tuple[str, str]]:
    """
    Identify commits for snapshots: quarterly for pre-2025, monthly for 2025+.

    Quarterly uses the last month of each quarter: 03, 06, 09, 12.

    :param repo_path: Path to the git repository.
    :return: A list of tuples, each containing a 'YYYY-MM' period and the corresponding commit hash.
    """
    log_output = run_command(
        cmd=["git", "log", "--pretty=format:%H|%cI"], cwd=repo_path
    )

    snapshots: dict[str, str] = {}
    for line in log_output.splitlines():
        if not line:
            continue
        commit_hash, commit_date = line.split("|")
        period = commit_date[:7]
        # Keep the first (newest) commit per period
        if period not in snapshots:
            snapshots[period] = commit_hash

    quarterly_months = {"03", "06", "09", "12"}
    filtered_snapshots: dict[str, str] = {}

    for period, commit_hash in snapshots.items():
        year = period[:4]
        month = period[5:7]

        if int(year) >= 2025:
            filtered_snapshots[period] = commit_hash
        elif month in quarterly_months:
            filtered_snapshots[period] = commit_hash

    return sorted(filtered_snapshots.items(), key=lambda x: x[0])


def _parse_blame_output(blame_output: str) -> dict[str, int]:
    """
    Parse git blame --line-porcelain output, returning a year -> line count mapping.

    :param blame_output: The raw output from git blame --line-porcelain
    :return: A dictionary mapping years to the number of lines changed in that year
    """
    file_distribution = defaultdict(int)
    commit_to_year = {}
    current_commit = None

    for line in blame_output.splitlines():
        if line.startswith("\t"):
            if current_commit and current_commit in commit_to_year:
                year = commit_to_year[current_commit]
                file_distribution[year] += 1
        else:
            parts = line.split(" ")
            if len(parts[0]) in (40, 64) and all(c in "0123456789abcdef" for c in parts[0].lower()):
                current_commit = parts[0]
            elif parts[0] == "author-time":
                try:
                    timestamp = int(parts[1])
                    year = datetime.fromtimestamp(timestamp, timezone.utc).strftime(
                        "%Y"
                    )
                    commit_to_year[current_commit] = year
                except (ValueError, IndexError):
                    pass

    return dict(file_distribution)


def _blame_single_file(repo_path: str, file: str) -> dict[str, int]:
    """
    Worker function to run git blame on a single file.
    Designed to be run concurrently in a ThreadPool.
    """
    try:
        blame_output = run_command(
            ["git", "blame", "--line-porcelain", file], cwd=repo_path
        )
        return _parse_blame_output(blame_output)
    except RuntimeError:
        return {}


def analyze_snapshots(repo_path: str, commit_hash: str) -> dict[str, int]:
    """
    Analyze the snapshots collected from the repository.

    :param repo_path: Path to the repository
    :param commit_hash: Hash of the commit to analyze
    :return: Dictionary mapping birth year to line count
    """
    run_command(["git", "checkout", commit_hash], cwd=repo_path)
    files_output = run_command(["git", "ls-files"], cwd=repo_path)
    files = files_output.splitlines()

    age_distribution = defaultdict(int)

    valid_files = [f for f in files if os.path.isfile(os.path.join(repo_path, f))]

    # Safe BLAME_WORKERS parsing with fallback.
    # Default caps at 8 to avoid I/O contention on HDDs (git blame is
    # I/O-bound, not CPU-bound, so the CPU-count multiplier doesn't apply).
    # Override via BLAME_WORKERS env var (clamped 1-100).
    max_workers = min(8, (os.cpu_count() or 1) * 2)
    try:
        if "BLAME_WORKERS" in os.environ:
            max_workers = max(1, min(int(os.environ["BLAME_WORKERS"]), 100))
    except ValueError:
        pass

    logger.info("  Blaming %d valid files (%d workers)...", len(valid_files), max_workers)

    total_files = len(valid_files)
    completed = 0
    next_log_pct = 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(_blame_single_file, repo_path, file): file
            for file in valid_files
        }

        for future in concurrent.futures.as_completed(future_to_file):
            file_dist = future.result()
            for year, count in file_dist.items():
                age_distribution[year] += count

            completed += 1
            pct = completed / total_files * 100
            if pct >= next_log_pct:
                logger.info("  Blame progress: %d/%d (%.0f%%)", completed, total_files, pct)
                next_log_pct += 10

    return dict(age_distribution)


def load_existing_state(json_fname: str) -> dict:
    """
    Load the existing historical data supporting both old list and new object schemas.

    :param json_fname: Path to the existing JSON file containing the historical data.
    :return: A dictionary with 'snapshots' and 'fossils'.
    """
    if os.path.exists(json_fname):
        try:
            with open(json_fname, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return {"snapshots": data, "fossils": {}}
                return data
        except json.JSONDecodeError:
            logger.warning("%s is corrupted, starting fresh.", json_fname)
            return {"snapshots": [], "fossils": {}}
    return {"snapshots": [], "fossils": {}}


def _atomic_write_json(
    json_path: str, snapshots: list[dict], fossils: dict | None = None
) -> None:
    """Write JSON data atomically and minified to prevent corruption and save space."""
    tmp_path = json_path + ".tmp"
    data = {"snapshots": snapshots, "fossils": fossils or {}}
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    os.replace(tmp_path, json_path)


def _filter_snapshots(
    all_snapshots: list[tuple[str, str]],
    processed_periods: set[str],
    reprocess: str | None = None,
) -> list[tuple[str, str]]:
    """
    Filter a list of (period, commit) snapshots down to unprocessed entries.

    When *reprocess* is provided (``YYYY-MM``), that specific period is
    included regardless of whether it exists in *processed_periods*.

    :param all_snapshots: Full list of (period, commit) tuples.
    :param processed_periods: Set of period strings that have already been processed.
    :param reprocess: Optional period to re-run (e.g. ``"2023-06"``).
    :return: List of (period, commit) tuples that need processing.
    """
    result: list[tuple[str, str]] = []
    for period, commit in all_snapshots:
        if period not in processed_periods or (reprocess and period == reprocess):
            result.append((period, commit))
    return result


def process_repository(repo_slug: str, data_dir: str, reprocess: str | None = None) -> None:
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """
    Orchestrate the extraction of Ship of Theseus code persistence data
    using an incremental load strategy by just processing the delta.

    Processes year-by-year and writes to disk after each year completes
    to prevent data loss on crash.

    Fossil data is NOT touched here — that is handled by add_fossils.py.

    :param repo_slug: The GitHub repository identifier (e.g., 'facebook/react').
    :param data_dir: Path where the resulting JSON data will be saved.
    """
    repo_name = repo_slug.split("/")[-1]
    # Use the full slug (org/repo) in the temp dir name to avoid collisions
    # when two different orgs have repos with the same name.
    temp_repo_path = f"./temp_workdir_{repo_slug.replace('/', '__')}"
    output_json_path = os.path.join(data_dir, f"{repo_name}_data.json")

    try:
        if not os.path.exists(temp_repo_path):
            clone_repository(repo_slug, temp_repo_path)
        else:
            logger.info(
                "Repository %s already exists locally. Fetching latest...", repo_name
            )
            run_command(["git", "fetch", "--all"], cwd=temp_repo_path)
            default_branch = get_default_branch(temp_repo_path)
            if default_branch == "HEAD":
                raise RuntimeError(
                    f"[{repo_name}] Cannot determine default branch after fetch. "
                    "Tried: main, master, develop, origin/HEAD."
                )
            run_command(
                ["git", "checkout", "-B", default_branch, f"origin/{default_branch}"],
                cwd=temp_repo_path,
            )
            run_command(["git", "pull"], cwd=temp_repo_path)

        state = load_existing_state(output_json_path)
        historical_snapshots = state["snapshots"]
        # Preserve any existing fossil data — do not touch it
        existing_fossils = state.get("fossils", {})
        processed_periods = set(item["snapshot_date"] for item in historical_snapshots)

        all_snapshots = get_snapshots(temp_repo_path)
        new_snapshots = _filter_snapshots(all_snapshots, processed_periods, reprocess)

        if not new_snapshots:
            logger.info(
                "[%s] No new periods to process. Data is already up to date!", repo_name
            )
            return

        logger.info(
            "[%s] Processing %d new snapshots with hybrid resolution (quarterly pre-2025, monthly 2025+)",
            repo_name,
            len(new_snapshots),
        )

        snapshots_by_year = groupby(new_snapshots, key=lambda x: x[0][:4])
        total_new_data = []

        for year, year_snapshots in snapshots_by_year:
            year_snapshots_list = list(year_snapshots)
            year_data = []
            year_start = time.perf_counter()

            logger.info(
                "[%s] Processing year %s: %d snapshots",
                repo_name,
                year,
                len(year_snapshots_list),
            )

            for idx, (period, commit) in enumerate(year_snapshots_list, 1):
                logger.info(
                    "[%s] [%s] Processing %s (%d/%d) - Commit: %s",
                    repo_name,
                    year,
                    period,
                    idx,
                    len(year_snapshots_list),
                    commit[:7],
                )

                snapshot_start = time.perf_counter()
                distribution = analyze_snapshots(temp_repo_path, commit)
                snapshot_elapsed = time.perf_counter() - snapshot_start

                logger.info(
                    "[%s] [%s] Completed %s in %.2f seconds (%d total lines)",
                    repo_name,
                    year,
                    period,
                    snapshot_elapsed,
                    sum(distribution.values()),
                )

                year_data.append(
                    {
                        "snapshot_date": period,
                        "composition": distribution,
                    }
                )

            total_new_data.extend(year_data)
            year_elapsed = time.perf_counter() - year_start

            final_snapshots = historical_snapshots + total_new_data
            final_snapshots.sort(key=lambda x: x["snapshot_date"])

            # Write snapshot data, preserving existing fossil data untouched
            _atomic_write_json(output_json_path, final_snapshots, existing_fossils)

            logger.info(
                "[%s] Completed year %s in %.2f seconds. Wrote %d snapshots to disk.",
                repo_name,
                year,
                year_elapsed,
                len(final_snapshots),
            )

    finally:
        remove_path(temp_repo_path)


def main() -> None:
    """
    Main entry point. Loads configuration, creates output directory,
    and runs the repository analysis pipeline for all specified targets.

    CLI flags
    ---------
    --repo NAME    Process only the given repository (by config name).
    --reprocess YYYY-MM
                   Re-process a specific snapshot period even if it already exists in the data.
    """
    parser = argparse.ArgumentParser(
        description="Analyse repository git history for the Ship of Theseus pipeline."
    )
    parser.add_argument(
        "--repo",
        metavar="NAME",
        default=None,
        help="Only process this repository (e.g. 'react'). If omitted, all repos are processed.",
    )
    parser.add_argument(
        "--reprocess",
        metavar="YYYY-MM",
        default=None,
        help="Re-process a specific snapshot period (e.g. '2023-06').",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = load_config()
    DATA_OUTPUT_DIR = config.get("dataDir", "./data")
    os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)

    # Build from config: name -> repo slug
    all_targets: dict[str, str] = {
        repo["name"]: repo["repo"]
        for repo in config.get("repositories", [])
        if "name" in repo and "repo" in repo
    }
    if not all_targets:
        logger.error("No valid repositories found in configuration.")
        sys.exit(1)

    if args.repo:
        if args.repo not in all_targets:
            logger.error(
                "Unknown repository '%s'. Valid options: %s",
                args.repo,
                ", ".join(all_targets),
            )
            sys.exit(1)
        selected_targets = {args.repo: all_targets[args.repo]}
        logger.info("Processing single repository: %s", args.repo)
    else:
        selected_targets = all_targets
        logger.info("Processing %d repositories", len(selected_targets))

    if args.reprocess:
        logger.info("Re-processing period: %s", args.reprocess)

    # Bound top-level workers by CPU count
    max_top_level_workers = min(
        len(selected_targets),
        int(os.getenv("MAX_TOP_LEVEL_WORKERS", os.cpu_count() or 1)),
    )

    overall_start = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_top_level_workers
    ) as executor:
        futures = {
            executor.submit(
                process_repository, slug, DATA_OUTPUT_DIR, args.reprocess
            ): name
            for name, slug in selected_targets.items()
        }
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                future.result()
                logger.info("✓ %s completed successfully.", name)
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to process %s: %s", name, e)

    overall_elapsed = time.perf_counter() - overall_start
    logger.info("TOTAL PIPELINE EXECUTION TIME: %.2f seconds", overall_elapsed)


if __name__ == "__main__":
    main()
