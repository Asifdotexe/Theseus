"""
Shared utilities for Theseus data pipeline scripts.

Consolidates helpers that were previously duplicated across
``analyse_repository.py`` and ``add_fossils.py``:

* ``run_command`` — safe subprocess wrapper with utf-8 handling
* ``get_default_branch`` — determine a repo's default git branch
"""

import subprocess
import logging

logger = logging.getLogger(__name__)


def run_command(cmd: list[str], cwd: str | None = None) -> str:
    """
    Execute a shell command and return its standard output.

    :param cmd: List of arguments forming the command.
    :param cwd: Directory path where the command should be executed.
    :return: Decoded standard output of the command, stripped.
    """
    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
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
            f"Command failed: {' '.join(str(c) for c in cmd)} "
            f"(exit {e.returncode}) — {e.stderr.strip()}"
        ) from e


def get_default_branch(repo_path: str | None = None) -> str:
    """
    Determine the default branch name for a git repository.

    Tries, in order:

    1. ``git symbolic-ref --short refs/remotes/origin/HEAD``
    2. ``git rev-parse --abbrev-ref origin/HEAD``
    3. ``git rev-parse --verify origin/main``
    4. ``git rev-parse --verify origin/master``
    5. ``git rev-parse --verify origin/develop``
    6. Falls back to ``"HEAD"``

    :param repo_path: Path to the git repository (or ``None`` for CWD).
    :return: Default branch name (e.g. ``"main"``, ``"master"``).
    """
    for strategy in [
        ["git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD"],
        ["git", "rev-parse", "--abbrev-ref", "origin/HEAD"],
    ]:
        try:
            result = run_command(strategy, cwd=repo_path)
            branch = (
                result[len("origin/"):] if result.startswith("origin/") else result
            )
            if branch:
                return branch
        except RuntimeError:
            continue

    for branch in ("main", "master", "develop"):
        try:
            run_command(
                ["git", "rev-parse", "--verify", f"origin/{branch}"],
                cwd=repo_path,
            )
            return branch
        except RuntimeError:
            continue

    return "HEAD"
