"""
This script is responsible for doing the heavy lifting.
Processes repository snapshots incrementally to track code age distribution.
Uses quarterly resolution for historical data (pre-2025) and monthly for recent data (2025+).

Fossil computation is handled separately by add_fossils.py.
"""

import concurrent.futures
import json
import logging
import os
import shutil
import stat
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from itertools import groupby

logger = logging.getLogger(__name__)


def _run_command(cmd: list[str], cwd: str | None = None) -> str:
    """
    Execute a shell command and return its standard output

    :param cmd: List of arguments forming the command.
    :param cwd: Directory path where the command should be executed.
    :return: Decoded standard output of the command.
    """
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command '{' '.join(cmd)}' failed with exit code {e.returncode}"
        ) from e


def clone_repository(repo_slug: str, clone_dir: str) -> None:
    """
    Dynamically clone a GitHub repository given its owner/name slug.

    :param repo_slug: The GitHub repository identifier (e.g., 'facebook/react').
    :param clone_dir: The local directory where the repository should be cloned.
    """
    logger.info("Cloning %s into %s...", repo_slug, clone_dir)
    repo_url = f"https://github.com/{repo_slug}.git"
    _run_command(["git", "clone", repo_url, clone_dir])


def get_snapshots(repo_path: str) -> list[tuple[str, str]]:
    """
    Identify commits for snapshots: quarterly for pre-2025, monthly for 2025+.

    Quarterly uses the last month of each quarter: 03, 06, 09, 12.

    :param repo_path: Path to the git repository.
    :return: A list of tuples, each containing a 'YYYY-MM' period and the corresponding commit hash.
    """
    log_output = _run_command(
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
            if len(parts[0]) in (40, 64):
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
        blame_output = _run_command(
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
    _run_command(["git", "checkout", commit_hash], cwd=repo_path)
    files_output = _run_command(["git", "ls-files"], cwd=repo_path)
    files = files_output.splitlines()

    age_distribution = defaultdict(int)

    valid_files = [f for f in files if os.path.isfile(os.path.join(repo_path, f))]

    # Safe BLAME_WORKERS parsing with fallback
    max_workers = min(20, (os.cpu_count() or 1) * 2)
    try:
        if "BLAME_WORKERS" in os.environ:
            max_workers = max(1, min(int(os.environ["BLAME_WORKERS"]), 100))
    except ValueError:
        pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(_blame_single_file, repo_path, file): file
            for file in valid_files
        }

        for future in concurrent.futures.as_completed(future_to_file):
            file_dist = future.result()
            for year, count in file_dist.items():
                age_distribution[year] += count

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


def process_repository(repo_slug: str, data_dir: str) -> None:
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
    temp_repo_path = f"./temp_workdir_{repo_name}"
    output_json_path = os.path.join(data_dir, f"{repo_name}_data.json")

    try:
        if not os.path.exists(temp_repo_path):
            clone_repository(repo_slug, temp_repo_path)
        else:
            logger.info(
                "Repository %s already exists locally. Fetching latest...", repo_name
            )
            _run_command(["git", "fetch", "--all"], cwd=temp_repo_path)
            for branch in ["main", "master"]:
                try:
                    _run_command(["git", "checkout", branch], cwd=temp_repo_path)
                    break
                except RuntimeError:
                    continue
            _run_command(["git", "pull"], cwd=temp_repo_path)

        state = load_existing_state(output_json_path)
        historical_snapshots = state["snapshots"]
        # Preserve any existing fossil data — do not touch it
        existing_fossils = state.get("fossils", {})
        processed_periods = set(item["snapshot_date"] for item in historical_snapshots)

        all_snapshots = get_snapshots(temp_repo_path)
        new_snapshots = [
            (period, commit)
            for period, commit in all_snapshots
            if period not in processed_periods
        ]

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
        if os.path.exists(temp_repo_path):
            logger.info("Cleaning up temporary directory: %s", temp_repo_path)
            time.sleep(1)

            def handle_remove_readonly(func, path, _exc_info):
                """Handle permission errors on Windows/Unix by adding write permission."""
                try:
                    current_mode = os.stat(path).st_mode
                    os.chmod(
                        path, current_mode | stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
                    )
                    func(path)
                except PermissionError as e:
                    logger.warning("Permission error cleaning up %s: %s", path, e)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.warning("Error cleaning up %s: %s", path, e)

            for attempt in range(3):
                try:
                    shutil.rmtree(temp_repo_path, onexc=handle_remove_readonly)
                    break
                except Exception as e:  # pylint: disable=broad-exception-caught
                    if attempt < 2:
                        time.sleep(1)
                        logger.warning("Cleanup attempt %d failed: %s", attempt + 1, e)
                    else:
                        logger.error(
                            "Failed to clean up temporary directory after 3 attempts: %s",
                            e,
                        )


def main():
    """
    Main entry point. Loads configuration, creates output directory,
    and runs the repository analysis pipeline for all specified targets.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config_path = "theseus.config.json"
    if not os.path.exists(config_path):
        logger.error("Configuration file not found: %s", config_path)
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    DATA_OUTPUT_DIR = config.get("dataDir", "./data")
    os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)

    TARGETS = [
        repo["repo"] for repo in config.get("repositories", []) if "repo" in repo
    ]
    if not TARGETS:
        logger.error("No valid repositories found in configuration.")
        sys.exit(1)

    # Bound top-level workers by CPU count
    max_top_level_workers = min(
        len(TARGETS), int(os.getenv("MAX_TOP_LEVEL_WORKERS", os.cpu_count() or 1))
    )

    overall_start = time.perf_counter()
    logger.info("Starting analysis pipeline for %d repositories", len(TARGETS))

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_top_level_workers
    ) as executor:
        futures = {
            executor.submit(process_repository, target, DATA_OUTPUT_DIR): target
            for target in TARGETS
        }
        for future in concurrent.futures.as_completed(futures):
            target = futures[future]
            try:
                future.result()
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Failed to process %s: %s", target, e)

    overall_elapsed = time.perf_counter() - overall_start
    logger.info("TOTAL PIPELINE EXECUTION TIME: %.2f seconds", overall_elapsed)


if __name__ == "__main__":
    main()
