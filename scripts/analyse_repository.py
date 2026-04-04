"""
This script is reposible for doing the heavy lifting.
Collects the 12 blames per year for target repository
"""

import json
import os
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime


def _run_command(cmd: list[str], cwd: str = None) -> str:
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


# FIXME: Optimisation opportunity: use git blame directly on the file list or if the file is larger than 32KB,
#        batch it into chunks as that the N would be reduced
def analyze_snapshots(repo_path: str, commit_hash: str) -> dict[str, int]:
    """
    Analyze the snapshots collected from the repository.

    :param repo_path: Path to the repository
    :param commit_hash: Hash of the commit to analyze
    :return: Dictionary of file age distribution in months
    """
    try:
        files_output = _run_command(
            cmd=["git", "ls-tree", "-r", "-z", "--name-only", commit_hash],
            cwd=repo_path,
        )
    except RuntimeError as e:
        print(f"Failed to list files in repository: {str(e)}")
        return {}

    # Split by the null character (because of -z) to handle spaces accurately
    files = [f for f in files_output.split("\0") if f]
    age_distribution = defaultdict(int)

    for file in files:
        try:
            # Blame the file directly at the specific commit in history
            # The '--' ensure git doesn;t confuse filename with flas
            blame_output = _run_command(
                ["git", "blame", "--line-porcelain", commit_hash, "--", file],
                cwd=repo_path,
            )

            commit_to_year = {}
            current_commit = None

            # A robust state machine to parse Git porcelain format
            # Porcelain format only prints the 'author-time' once per commit block
            # so we must remember the year for each commit has we encounter.
            for line in blame_output.splitlines():
                if line.startswith("\t"):
                    if current_commit in commit_to_year:
                        age_distribution[commit_to_year[current_commit]] += 1
                else:
                    parts = line.split(" ")
                    # A 40 (or 64 for SHA-256) character hash marks the start of a new blame block
                    if len(parts[0]) in (40, 64):
                        current_commit = parts[0]
                    elif parts[0] == "author-time":
                        try:
                            timestamp = int(parts[1])
                            commit_to_year[current_commit] = datetime.fromtimestamp(
                                timestamp
                            ).strftime("%Y")
                        except (ValueError, OverflowError, IndexError):
                            pass
        except RuntimeError:
            # Skip files that git blame cannot process (e.g., binary files)
            continue

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
            _run_command(["git", "checkout", "main"], cwd=temp_repo_path)
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

            with open(output_json_path, "w") as f:
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

    # ---------------------------------------------------------
    # The Case Studies: Add any public repository you want here!
    # ---------------------------------------------------------
    TARGETS = [
        "facebook/react",  # The modern web standard
        "vuejs/vue",  # A fantastic comparison to React
        "d3/d3",  # The data-viz giant
    ]

    for target in TARGETS:
        print(f"\n{'=' * 50}\nStarting analysis pipeline for: {target}\n{'=' * 50}")
        process_repository(target, DATA_OUTPUT_DIR)
