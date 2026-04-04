"""
This script is responsible for doing the heavy lifting.
Processes monthly snapshots incrementally to track code age distribution.
"""

import concurrent.futures
import json
import os
import shutil
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from functools import wraps


def timer(func):
    """
    A decorator that prints the execution time of the function it wraps.
    Used for benchmarking sequential vs. concurrent execution optimizations.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        print(f"⏱️  [TIMER] '{func.__name__}' executed in {execution_time:.4f} seconds")
        return result

    return wrapper


def _run_command(cmd: list[str], cwd: str | None = None) -> str:
    """
    Execute a shell command and return it's standard output

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
    print(f"Cloning {repo_slug} into {clone_dir}...")
    repo_url = f"https://github.com/{repo_slug}.git"
    _run_command(["git", "clone", repo_url, clone_dir])


def get_monthly_snapshots(repo_path: str) -> list[tuple[str, str]]:
    """
    Identify one commit per month to act as a historical snapshot.

    :param repo_path: Path to the git repository.
    :return: A list of tuples, each containing a 'YYYY-MM' period and the corresponding commit hash.
             i.e., [(period, commit_hash), ...]
    """
    log_output = _run_command(
        cmd=["git", "log", "--pretty=format:%H|%cI"], cwd=repo_path
    )

    snapshots: dict = {}
    for line in log_output.splitlines():
        if not line:
            continue
        commit_hash, commit_date = line.split("|")

        # We slice the first 7 characters of the ISO to get the 'YYYY-MM' period
        period = commit_date[:7]

        # Git log outputs newest commit first. By assigning to the dictionary,
        # the last commit processed for a month overwrites earlier ones,
        # leaving us with the very first commit of that specific month
        snapshots[period] = commit_hash

    return sorted(snapshots.items(), key=lambda x: x[0])


def _parse_blame_output(blame_output: str) -> dict[str, int]:
    """
    Parse git blame --line-porcelain output, returning a year -> line count mapping.
    Extracting this logic reduces nesting and properly handles Git's porcelain format,
    where 'author-time' is only printed once per commit block, but actual code lines
    begin with a tab character.

    :param blame_output: The raw output from git blame --line-porcelain
    :return: A dictionary mapping years to the number of lines changed in that year
    """
    file_distribution = defaultdict(int)
    commit_to_year = {}
    current_commit = None

    for line in blame_output.splitlines():
        if line.startswith("\t"):
            # This is an actual line of code. Attribute it to the year of the current commit.
            if current_commit and current_commit in commit_to_year:
                year = commit_to_year[current_commit]
                file_distribution[year] += 1
        else:
            parts = line.split(" ")
            # Check if the line starts with a 40-char (SHA-1) or 64-char (SHA-256) commit hash
            if len(parts[0]) in (40, 64):
                current_commit = parts[0]
            elif parts[0] == "author-time":
                try:
                    timestamp = int(parts[1])
                    year = datetime.fromtimestamp(timestamp).strftime("%Y")
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
        # Skip files that git blame cannot process (like binaries)
        return {}


@timer
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

    # Use ThreadPoolExecutor to bypass the O(N) sequential subprocess bottleneck.
    # Subprocess calls release the GIL, making threading highly effective here.
    max_threads = 20
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_file = {
            executor.submit(_blame_single_file, repo_path, file): file
            for file in valid_files
        }

        for future in concurrent.futures.as_completed(future_to_file):
            file_dist = future.result()
            for year, count in file_dist.items():
                age_distribution[year] += count

    return dict(age_distribution)


def load_existing_state(json_fname: str) -> list[dict]:
    """
    Load the existing historical data to prevent redundant re-calculations.

    :param json_fname: Path to the existing JSON file containing the historical data.
    :return: A list of dictionaries with the historical data.
    """
    if os.path.exists(json_fname):
        try:
            with open(json_fname, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: {json_fname} is corrupted, Start fresh.")
            return []
    return []


# TODO: Make the main function to tie everything together
@timer
def process_repository(repo_slug: str, data_dir: str) -> None:
    """
    Orchestrate the extraction of Ship of Theseus code persistence data
    using an incremental load strategy by just processing the delta

    :param repo_slug: The GitHub repository identifier (e.g., 'facebook/react').
    :param data_dir: Path where the resulting JSON data will be saved.
    """
    repo_name = repo_slug.split("/")[-1]
    temp_repo_path = f"./temp_workdir_{repo_name}"
    output_json_path = os.path.join(data_dir, f"{repo_name}_data.json")
    # System design thinking is that we don't want to load existing state and recalculate redundantly
    #
    # We clone the repository dynamically just to read it. By pulling the codebase
    # ourselves instead of relying on GitHub Actions checkout steps, we can iterate
    # through 10, 50, or 100 repositories entirely within Python.
    #
    # Let's say we have a 10-year old repository. Running git blame on every file for every single month
    # of it's 120 month long history would take hours and it would blow past the GitHub Action's free tier limit.
    # By loading the existing state and only processing the delta, we can avoid this and run much faster.
    #
    # This reduces a 30-minute monthly compute job down to about 5 seconds,
    # ensuring that I don't have to pay for keeping this project alive lmao.
    try:
        if not os.path.exists(temp_repo_path):
            clone_repository(repo_slug, temp_repo_path)
        else:
            print(f"Repository {repo_name} already exists locally. Fetching latest...")
            _run_command(["git", "fetch", "--all"], cwd=temp_repo_path)
            for branch in ["main", "master"]:
                try:
                    _run_command(["git", "checkout", branch], cwd=temp_repo_path)
                    break
                except RuntimeError:
                    continue
            _run_command(["git", "pull"], cwd=temp_repo_path)

        historical_data = load_existing_state(output_json_path)
        processed_periods = set(item["snapshot_date"] for item in historical_data)

        all_snapshots = get_monthly_snapshots(temp_repo_path)
        new_data = []

        for period, commit in all_snapshots:
            if period in processed_periods:
                # We already know what the repository looked like in this month. Skip it.
                continue

            print(
                f"[{repo_name}] Calculating DELTA for new period: {period} (Commit: {commit[:7]})..."
            )
            distribution = analyze_snapshots(temp_repo_path, commit)

            new_data.append(
                {
                    "snapshot_date": period,
                    "total_lines": sum(distribution.values()),
                    "composition": distribution,
                }
            )

        if not new_data:
            print(
                f"[{repo_name}] No new months to process. Data is already up to date!"
            )
        else:
            final_dataset = historical_data + new_data
            final_dataset.sort(key=lambda x: x["snapshot_date"])

            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(final_dataset, f, indent=4)

            print(
                f"[{repo_name}] Delta analysis complete. Appended {len(new_data)} new months."
            )

    finally:
        # Polite cleanup: Remove the gigantic source code folders we downloaded.
        # We only want to keep the JSON data!
        if os.path.exists(temp_repo_path):
            print(f"Cleaning up temporary directory: {temp_repo_path}")
            # Note: Windows might need special handling for git files, but this works on Linux/Mac (GitHub Actions)
            shutil.rmtree(temp_repo_path, ignore_errors=True)


if __name__ == "__main__":
    DATA_OUTPUT_DIR = "./data"
    os.makedirs(DATA_OUTPUT_DIR, exist_ok=True)

    # The Case Studies: Start with these one to benchmark.
    TARGETS = [
        "anthropics/claude-code",
    ]

    overall_start = time.perf_counter()
    for target in TARGETS:
        print(f"\n{'=' * 50}\nStarting analysis pipeline for: {target}\n{'=' * 50}")
        process_repository(target, DATA_OUTPUT_DIR)
    overall_end = time.perf_counter()
    print(
        f"\n{'=' * 50}\nTOTAL PIPELINE EXECUTION TIME: {overall_end - overall_start:.2f} seconds\n{'=' * 50}"
    )
