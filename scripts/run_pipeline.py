"""
Unified orchestration script for the Theseus data pipeline.

Runs all three stages in sequence on one or more repositories:

1. **Analyse** (snapshot generation via ``analyse_repository``)
2. **Fossils** (genesis + survivor via ``add_fossils``)
3. **Cleanup** (future-year filtering + minification via ``cleanup_data``)

Fossil data model
-----------------
The pipeline discovers two fossil types per repository:

**Genesis** (Historical Fossil)
    The single oldest-authored line ever written in the repository.  Found by
    blaming only files added in each of the earliest commits (sorted by
    author-time), scanning until *stale_limit* consecutive commits fail to
    improve the oldest-yet result.

**Survivor** (Living Fossil)
    The single oldest-authored line that still exists at the current HEAD.
    Found by blaming every tracked file on the default branch and returning
    the line with the smallest author-timestamp.

Each fossil stores: ``{timestamp, file, content, year, commit, view_commit, line}``.
"""

import logging
import os
import sys
import time

# Ensure sibling imports work in all invocation contexts
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from _utils import load_config
from cleanup_data import cleanup_data as run_cleanup

logger = logging.getLogger(__name__)


def run_pipeline(
    repo: str | None = None,
    reprocess: str | None = None,
    update_survivor: bool = False,
) -> bool:
    """
    Run the full pipeline (analyse → fossils → cleanup) for all repositories.

    :param repo: Optional repository name to process (None = all repos).
    :param reprocess: Optional ``YYYY-MM`` period to force re-process.
    :param update_survivor: If ``True``, skip genesis scan and only refresh the
        survivor (living) fossil. Designed for monthly cron ticks.
    :return: ``True`` if any stage failed, ``False`` otherwise.
    """
    config = load_config()
    data_dir = config.get("dataDir", "./data")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(os.path.join(data_dir, "raw"), exist_ok=True)
    os.makedirs(os.path.join(data_dir, "processed"), exist_ok=True)

    # Build target lists from config
    all_repos: list[dict] = config.get("repositories", [])
    if not all_repos:
        logger.error("No repositories found in configuration.")
        return True

    if repo:
        selected = [r for r in all_repos if r.get("name") == repo]
        if not selected:
            logger.error("Unknown repository '%s'.", repo)
            return True
        logger.info("Pipeline running for single repository: %s", repo)
    else:
        selected = all_repos
        logger.info("Pipeline running for %d repositories.", len(selected))

    had_failures = False

    # ── Stage 1: Analyse ──────────────────────────────────────────────
    logger.info("═══ STAGE 1: Snapshot analysis ═══")
    from analyse_repository import (
        process_repository,
    )

    for repo_info in selected:
        repo_slug = repo_info.get("repo", "")
        repo_name = repo_info.get("name", "")
        if not repo_slug or not repo_name:
            logger.warning("Skipping repo entry with missing slug/name: %s", repo_info)
            continue

        logger.info("  Analysing %s (%s)...", repo_name, repo_slug)
        try:
            process_repository(repo_slug, data_dir, reprocess)
            logger.info("  ✓ %s — snapshot analysis complete.", repo_name)
        except Exception as e:  # noqa: BLE001
            logger.error("  ✗ %s — snapshot analysis failed: %s", repo_name, e)
            had_failures = True

    # ── Stage 2: Fossils ───────────────────────────────────────────────
    from add_fossils import backfill_fossils, update_survivor_fossils

    repo_urls = {
        r["name"]: f"https://github.com/{r['repo']}.git"
        for r in selected
        if "name" in r and "repo" in r
    }

    if update_survivor:
        logger.info("═══ STAGE 2: Survivor-only refresh ═══")
        if repo_urls:
            try:
                fossil_errors = update_survivor_fossils(data_dir, repo_urls)
                if fossil_errors:
                    had_failures = True
                else:
                    logger.info("  ✓ All survivors refreshed.")
            except Exception as e:  # noqa: BLE001
                logger.error("  ✗ Survivor stage failed: %s", e)
                had_failures = True
        else:
            logger.warning("  Skipping survivor stage — no valid repos.")
    else:
        logger.info("═══ STAGE 2: Full fossil discovery ═══")
        if repo_urls:
            logger.info("  Computing fossils for %d repos...", len(repo_urls))
            try:
                fossil_errors = backfill_fossils(data_dir, repo_urls)
                if fossil_errors:
                    had_failures = True
                else:
                    logger.info("  ✓ All fossils computed.")
            except Exception as e:  # noqa: BLE001
                logger.error("  ✗ Fossil stage failed: %s", e)
                had_failures = True
        else:
            logger.warning("  Skipping fossil stage — no valid repos.")

    # ── Stage 3: Cleanup ───────────────────────────────────────────────
    logger.info("═══ STAGE 3: Data cleanup ═══")
    try:
        cleanup_errors = run_cleanup(data_dir)
        if cleanup_errors:
            had_failures = True
        else:
            logger.info("  ✓ Cleanup complete.")
    except Exception as e:  # noqa: BLE001
        logger.error("  ✗ Cleanup stage failed: %s", e)
        had_failures = True

    return had_failures


def main() -> None:
    """
    Entry point for the unified pipeline runner.

    CLI flags
    ---------
    --repo NAME          Process only this repository (by config name).
    --reprocess YYYY-MM  Re-process a specific snapshot period.
    --update-survivor    Skip genesis scan; refresh only the survivor fossil
                         (designed for monthly cron ticks).
    """
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Run the full Theseus pipeline: analyse → fossils → cleanup."
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
    parser.add_argument(
        "--update-survivor",
        action="store_true",
        help="Skip genesis scan; refresh only the survivor (living) fossil.",
    )
    args = parser.parse_args()

    overall_start = time.perf_counter()
    had_errors = run_pipeline(
        repo=args.repo,
        reprocess=args.reprocess,
        update_survivor=args.update_survivor,
    )
    elapsed = time.perf_counter() - overall_start

    if had_errors:
        logger.error("Pipeline finished with errors (%.2f seconds).", elapsed)
        sys.exit(1)

    logger.info("Pipeline completed successfully (%.2f seconds).", elapsed)


if __name__ == "__main__":
    main()
