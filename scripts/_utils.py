"""
Shared utilities for Theseus data pipeline scripts.

Consolidates helpers that were previously duplicated across
``analyse_repository.py`` and ``add_fossils.py``:

* ``run_command`` — safe subprocess wrapper with utf-8 handling
* ``get_default_branch`` — determine a repo's default git branch
"""

import json
import logging
import os
import shutil
import subprocess
import sys
import time

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


def load_config(config_path: str = "theseus.config.json") -> dict:
    """
    Load and return the project configuration file (``theseus.config.json``).

    Exits with status 1 if the file is missing or malformed.

    :param config_path: Path to the JSON configuration file.
    :return: Parsed configuration dictionary.
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Configuration file not found: %s", config_path)
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error("Configuration file %s is malformed: %s", config_path, e)
        sys.exit(1)


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


def remove_path(path: str) -> None:
    """
    Remove a file or directory using OS-native fast deletion.

    Uses ``cmd /c rd /s /q`` on Windows and ``rm -rf`` on Unix,
    falling back to ``shutil.rmtree`` on failure.

    :param path: Path to the file or directory to remove.
    """
    if not os.path.exists(path):
        return

    if os.name == "nt":
        try:
            subprocess.run(
                ["cmd", "/c", "rd", "/s", "/q", path],
                capture_output=True,
                timeout=30,
            )
            if not os.path.exists(path):
                return
        except (subprocess.SubprocessError, OSError):
            pass
    else:
        try:
            subprocess.run(
                ["rm", "-rf", path],
                capture_output=True,
                timeout=30,
            )
            if not os.path.exists(path):
                return
        except (subprocess.SubprocessError, OSError):
            pass

    # Fallback: retry with shutil.rmtree
    for attempt in range(3):
        try:
            shutil.rmtree(path, ignore_errors=False)

            def handle_remove_readonly(func, path, _exc_info):
                try:
                    current_mode = os.stat(path).st_mode
                    os.chmod(
                        path,
                        current_mode | stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH,
                    )
                    func(path)
                except PermissionError:
                    pass
                except Exception:  # noqa: BLE001
                    pass

            shutil.rmtree(path, onexc=handle_remove_readonly)
            break
        except Exception:  # noqa: BLE001
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                logger.warning("Failed to clean up %s after 3 attempts", path)
