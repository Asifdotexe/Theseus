"""
This script is reposible for doing the heavy lifting.
Collects the 12 blames per year for target repository
"""

import json
import os
import subprocess
from collections import defaultdict
from datetime import datetime


def _run_command(cmd: list[str], cwd: str) -> str:
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
def generate_theseus_data(target_repo_path: str, output_json_fname: str) -> None:
    """
    Orchestrate the extract of Ship of Theseus code persistence data
    using an incremental load strategy by just processing the delta
    """
    print(f"Starting analysis on {target_repo_path}...")
    # System design thinking is that we don't want to load existing state and recalculate redundantly
    #
    # Let's say we have a 10-year old repository. Running git blame on every file for every single month
    # of it's 120 month long history would take hours and it would blow past the GitHub Action's free tier limit.
    # By loading the existing state and only processing the delta, we can avoid this and run much faster.
    #
    # This reduces a 30-minute monthly compute job down to about 5 seconds,
    # ensuring that I don't have to pay for keeping this project alive lmao.
    historical_data = load_existing_state(output_json_fname)
    processed_periods = set(item["snapshot_date"] for item in historical_data)

    all_snapshots = get_monthly_snapshots(target_repo_path)
    new_snapshots = []

    for period, commit in all_snapshots:
        if period in processed_periods:
            # We already know that the repository looked like this month. Skip it.
            continue

        print(f"Calculating DELTA for new period: {period} (Commit: {commit[:7]})...")
        distribution = analyze_snapshots(target_repo_path, commit)

        new_snapshots.append(
            {
                "snapshot_date": period,
                "total_lines": sum(distribution.values()),
                "composition": distribution,
            }
        )

    if not new_snapshots:
        print("No new months to process. Repository data is already up-to-date.")
        return

    # Combine the historical data with the newly processed delta
    final_dataset = historical_data + new_snapshots

    # Ensure chronological order to prevent rendering glitches on the frontend
    final_dataset.sort(key=lambda x: x["snapshot_date"])

    # Polite cleanup: return the repo to its intial state
    _run_command(["git", "checkout", "-"], cwd=target_repo_path)

    with open(output_json_fname, "w", encoding="utf-8") as f:
        json.dump(final_dataset, f, indent=4)

    print(
        f"Delta analysis completed. Appended {len(new_snapshots)} new months to the dataset."
    )


# FIXME: Make this into argparse or list for scalability
if __name__ == "__main__":
    TARGET_REPO_PATH = "C:\\Users\\sayye\\OneDrive\\Documents\\GitHub\\portfolio"
    OUTPUT_JSON_FNAME = "./data/theseus_data.json"

    os.makedirs(os.path.dirname(OUTPUT_JSON_FNAME), exist_ok=True)

    if os.path.exists(TARGET_REPO_PATH):
        generate_theseus_data(TARGET_REPO_PATH, OUTPUT_JSON_FNAME)
    else:
        print(f"Target repository not found: {TARGET_REPO_PATH}")
