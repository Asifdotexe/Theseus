"""
Processes repository snapshots incrementally to track code age distribution.

This script delegates the compute-heavy snapshot git blame traversal to the
compiled Rust engine (engine/src/main.rs), managing repository preparation
and thread orchestration.
"""

import argparse
import concurrent.futures
import logging
import os
import platform
import subprocess
import sys
import tempfile
import time

from scripts._utils import (get_default_branch, load_config, remove_path,
                            run_command)

logger = logging.getLogger(__name__)


def ensure_repo_ready(repo_slug: str, repo_name: str, temp_repo_path: str) -> None:
    """Clone or fetch the repository so it's ready for analysis."""
    git_head = os.path.join(temp_repo_path, ".git", "HEAD")
    if not os.path.isfile(git_head):
        if os.path.exists(temp_repo_path):
            remove_path(temp_repo_path)
        logger.info("Cloning %s into %s...", repo_slug, temp_repo_path)
        repo_url = f"https://github.com/{repo_slug}.git"
        run_command(
            [
                "git",
                "-c",
                "filter.lfs.smudge=",
                "-c",
                "filter.lfs.process=",
                "-c",
                "filter.lfs.required=false",
                "clone",
                repo_url,
                temp_repo_path,
            ]
        )
        return

    logger.info("Repository %s already exists locally. Fetching latest...", repo_name)
    lock_file = os.path.join(temp_repo_path, ".git", "index.lock")
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
        except OSError:
            pass
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


def process_repository(
    repo_slug: str, data_dir: str, reprocess: str | None = None
) -> None:
    """
    Process a single repository end-to-end.

    Clones or updates the repo, then delegates snapshot analysis to the compiled
    Rust engine.

    :param repo_slug: GitHub ``owner/name`` slug.
    :param data_dir: Path to the ``data/`` output directory.
    :param reprocess: Optional target ('all', 'last', or 'YYYY-MM') to force re-processing.
    """
    repo_name = repo_slug.split("/")[-1]
    temp_repo_path = os.path.join(
        tempfile.gettempdir(), f"theseus_workdir_{repo_slug.replace('/', '__')}"
    )
    output_json_path = os.path.abspath(os.path.join(data_dir, "raw", f"{repo_name}_history.jsonl"))
    state_json_path = os.path.abspath(os.path.join(data_dir, "state", f"{repo_name}_state.json"))
    fossil_json_path = os.path.abspath(os.path.join(data_dir, "raw", f"{repo_name}_fossils.json"))

    try:
        ensure_repo_ready(repo_slug, repo_name, temp_repo_path)

        if platform.system() == "Windows":
            cmd = [
                "cmd.exe",
                "/c",
                "run_engine.bat",
                "--repo-path",
                temp_repo_path,
                "--output",
                output_json_path,
                "--state",
                state_json_path,
                "--fossils",
                fossil_json_path,
            ]
        else:
            cmd = [
                "./engine/target/release/engine",
                "--repo-path",
                temp_repo_path,
                "--output",
                output_json_path,
                "--state",
                state_json_path,
                "--fossils",
                fossil_json_path,
            ]

        if reprocess:
            cmd.extend(["--reprocess", reprocess])

        logger.info("[%s] Delegating snapshot analysis to Rust engine...", repo_name)
        subprocess.run(cmd, check=True)

    finally:
        remove_path(temp_repo_path)


def main() -> None:
    """
    Entry point for the snapshot-analysis pipeline.

    CLI flags
    ---------
    --repo NAME         Process only the given repository (by config name).
    --reprocess TARGET  Re-process 'all', 'last', or a specific snapshot period even if it already
                        exists on disk.
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
        metavar="TARGET",
        default=None,
        help=(
            "Re-process a target. Pass 'all' to wipe and rebuild, 'last' for the latest period, "
            "or a specific period like '2023-06'."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = load_config()
    data_output_dir = config.get("dataDir", "./data")
    os.makedirs(data_output_dir, exist_ok=True)

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
        logger.info("Re-processing target: %s", args.reprocess)

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
                process_repository, slug, data_output_dir, args.reprocess
            ): name
            for name, slug in selected_targets.items()
        }
        for future in concurrent.futures.as_completed(futures):
            name = futures[future]
            try:
                future.result()
                logger.info("✓ %s completed successfully.", name)
            except Exception as e:  # noqa: BLE001
                logger.error("Failed to process %s: %s", name, e)

    overall_elapsed = time.perf_counter() - overall_start
    logger.info("TOTAL PIPELINE EXECUTION TIME: %.2f seconds", overall_elapsed)


if __name__ == "__main__":
    main()
