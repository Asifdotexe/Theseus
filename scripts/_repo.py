"""
Repository management utilities for the Theseus pipeline.
"""

import logging
import os

from scripts._utils import get_default_branch, run_command

logger = logging.getLogger(__name__)


def clone_repository(repo_slug: str, clone_dir: str) -> None:
    """Clone a GitHub repository into a local directory."""
    logger.info("Cloning %s into %s...", repo_slug, clone_dir)
    repo_url = f"https://github.com/{repo_slug}.git"
    run_command(["git", "clone", repo_url, clone_dir])


def ensure_repo_ready(repo_slug: str, repo_name: str, temp_repo_path: str) -> None:
    """Clone or fetch the repository so it's ready for analysis."""
    if not os.path.exists(temp_repo_path):
        clone_repository(repo_slug, temp_repo_path)
        return

    logger.info("Repository %s already exists locally. Fetching latest...", repo_name)
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
