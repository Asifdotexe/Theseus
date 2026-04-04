"""
This script is reposible for doing the heavy lifting.
Collects the 12 blames per year for target repository
"""

import subprocess


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

    snapshot: dict = {}
    for line in log_output.splitlines():
        if not line:
            continue
        commit_hash, commit_date = line.split("|")

        # We slice the first 7 characters of the ISO to get the 'YYYY-MM' period
        period = commit_date[:7]

        # Git log outputs newest commit first. By assigning to the dictionary,
        # the last commit processed for a month overwrites earlier ones,
        # leaving us with the very first commit of that specific month
        snapshot[period] = commit_hash

    return sorted(snapshot.items(), key=lambda x: x[0])
