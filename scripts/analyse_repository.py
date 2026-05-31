"""
Processes repository snapshots incrementally to track code age distribution.

This script is responsible for the **snapshot generation** step of the Theseus
data pipeline.  It clones (or fetches) a git repository, walks its commit
history at quarterly resolution (pre-2025) / monthly resolution (2025+), runs
``git blame --line-porcelain`` on all tracked files at each snapshot commit,
and aggregates the results into year-to-line-count distributions.

The output JSON has the standard ``{snapshots, fossils}`` shape where
``fossils`` is left untouched (preserving any previously computed fossil data).
Fossil computation is handled separately by ``add_fossils.py``.

Fossil data model
-----------------
Scripts in this pipeline use two fossil types:

  **Genesis** — the single oldest-authored line ever written in the repository.
  **Survivor** — the single oldest-authored line still alive at current HEAD.

Each fossil stores ``{timestamp, file, content, year, commit, view_commit, line}``.
See ``_blame.py`` for the full data-model definition and the algorithms used
to discover each fossil type.
"""

import argparse
import logging
import os
import sys
import time
from collections import defaultdict
from itertools import groupby

# Ensure sibling imports work in all invocation contexts
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from _blame import blame_files_to_file_compositions, blame_files_year_counts
from _data_io import load_snapshot_data, save_snapshot_data
from _utils import (
    count_repo_lines,
    get_changed_files,
    get_default_branch,
    get_tracked_files,
    load_config,
    run_command,
)

logger = logging.getLogger(__name__)


def clone_repository(repo_slug: str, clone_dir: str) -> None:
    """
    Clone a GitHub repository into a local directory.

    :param repo_slug: GitHub ``owner/name`` slug (e.g. ``'facebook/react'``).
    :param clone_dir: Local path to clone into.
    """
    logger.info("Cloning %s into %s...", repo_slug, clone_dir)
    repo_url = f"https://github.com/{repo_slug}.git"
    run_command(["git", "clone", repo_url, clone_dir])


def get_snapshot_periods(repo_path: str) -> list[tuple[str, str]]:
    """
    Identify (period, commit) snapshots from the repository's log.

    Resolution is quarterly (last month of each quarter: 03, 06, 09, 12) for
    pre-2025 history and monthly for 2025+.

    :param repo_path: Path to the git repository.
    :return: List of ``(YYYY-MM, commit_hash)`` tuples sorted chronologically.
    """
    log_output = run_command(
        cmd=["git", "log", "--pretty=format:%H|%cI"], cwd=repo_path
    )

    periods: dict[str, str] = {}
    for line in log_output.splitlines():
        if not line:
            continue
        commit_hash, commit_date = line.split("|")
        period = commit_date[:7]
        if period not in periods:
            periods[period] = commit_hash

    quarterly_months = {"03", "06", "09", "12"}
    filtered: dict[str, str] = {}

    for period, commit_hash in periods.items():
        year = period[:4]
        month = period[5:7]
        if int(year) >= 2025:
            filtered[period] = commit_hash
        elif month in quarterly_months:
            filtered[period] = commit_hash

    return sorted(filtered.items(), key=lambda x: x[0])


def _resolve_worker_count() -> int:
    """
    Determine the number of parallel blame workers.

    Default is ``min(8, cpu_count * 2)``.  Override via ``BLAME_WORKERS``
    environment variable (clamped 1-100).

    :return: Worker count (int).
    """
    max_workers = min(8, (os.cpu_count() or 1) * 2)
    try:
        if "BLAME_WORKERS" in os.environ:
            max_workers = max(1, min(int(os.environ["BLAME_WORKERS"]), 100))
    except ValueError:
        pass
    return max_workers


def analyze_single_snapshot(
    repo_path: str,
    commit_hash: str,
    prev_file_data: tuple[str, dict[str, dict[str, int]]] | None = None,
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """
    Analyse a single snapshot commit and return its year-to-line-count distribution.

    When *prev_file_data* ``(prev_commit, {file: {year: count}})`` is provided,
    uses an incremental strategy: carries forward blame results for files
    that have not changed between the two commits, and blames only files
    that differ (via ``git diff-tree``).  When *prev_file_data* is ``None``,
    blames every tracked file (baseline).

    A ``wc -l`` sanity check runs after every snapshot; if the blame total
    deviates more than 1 % from the real line count on disk, the snapshot
    is re-processed with a full blame to guard against incremental bugs.

    :param repo_path: Path to the git repository.
    :param commit_hash: The commit (tag, branch, or hash) to analyze.
    :param prev_file_data: Optional ``(prev_commit, {file: {year: count}})``
        from the previous snapshot for incremental blame.
    :return: ``(age_distribution, file_compositions)`` where
        ``age_distribution`` is ``{year: line_count}`` and
        ``file_compositions`` is ``{file_path: {year: count}}``.
    """
    run_command(["git", "checkout", commit_hash], cwd=repo_path)
    max_workers = _resolve_worker_count()

    # --- Blame phase ---
    # OPTIMIZATION (incremental blame): Between consecutive snapshot commits,
    # typically <10% of files change. Instead of blaming every tracked file
    # (>5000 per snapshot for large repos), we use git diff-tree to find
    # only the files that differ between the two commit trees. Unchanged
    # files carry forward their previous blame results verbatim (blame is
    # deterministic for identical file content). This reduces total blame
    # operations by ~90% over a full pipeline run.
    if prev_file_data is None:
        tracked_files = get_tracked_files(repo_path)
        file_compositions = blame_files_to_file_compositions(
            repo_path, tracked_files, max_workers
        )
    else:
        prev_commit, prev_compositions = prev_file_data
        changed_files = get_changed_files(repo_path, prev_commit, commit_hash)
        if not changed_files:
            file_compositions = {
                k: dict(v) for k, v in prev_compositions.items()
            }
        else:
            # Carry forward unchanged files, blame only changed
            file_compositions = {
                k: dict(v)
                for k, v in prev_compositions.items()
                if k not in changed_files
            }
            new_compositions = blame_files_to_file_compositions(
                repo_path, changed_files, max_workers
            )
            file_compositions.update(new_compositions)

    # --- Aggregation ---
    age_distribution: dict[str, int] = defaultdict(int)
    for f_data in file_compositions.values():
        for year, count in f_data.items():
            age_distribution[year] += count
    blame_total = sum(age_distribution.values())

    # --- Sanity check via disk line count ---
    # OPTIMIZATION: Verifies the blame total against a fast disk-only line
    # count (no git history traversal). If the incremental blame missed a
    # changed file (git diff-tree edge case) or carried forward stale data
    # incorrectly, the totals will diverge >1% and we fall back to a full
    # blame — ensuring correctness even if the incremental logic has a bug.
    disk_total = count_repo_lines(repo_path)
    if disk_total > 0:
        diff_pct = abs(blame_total - disk_total) / disk_total * 100
        if diff_pct > 1:
            logger.warning(
                "Line count mismatch: blame=%d vs disk=%d (%.1f%%). "
                "Falling back to full blame.",
                blame_total,
                disk_total,
                diff_pct,
            )
            tracked_files = get_tracked_files(repo_path)
            file_compositions = blame_files_to_file_compositions(
                repo_path, tracked_files, max_workers
            )
            age_distribution = defaultdict(int)
            for f_data in file_compositions.values():
                for year, count in f_data.items():
                    age_distribution[year] += count

    return dict(age_distribution), file_compositions


def _filter_snapshots(
    all_periods: list[tuple[str, str]],
    processed_periods: set[str],
    reprocess: str | None = None,
) -> list[tuple[str, str]]:
    """
    Filter (period, commit) pairs down to those that need processing.

    When *reprocess* is provided, that specific period is included even if it
    was already processed.

    :param all_periods: Full list of (period, commit) tuples.
    :param processed_periods: Set of period strings already on disk.
    :param reprocess: Optional ``YYYY-MM`` period to force re-processing.
    :return: List of (period, commit) tuples that need processing.
    """
    result: list[tuple[str, str]] = []
    for period, commit in all_periods:
        if period not in processed_periods or (reprocess and period == reprocess):
            result.append((period, commit))
    return result


def process_repository(
    repo_slug: str, data_dir: str, reprocess: str | None = None
) -> None:
    """
    Process a single repository end-to-end.

    Clones or updates the repo, then processes each new snapshot year-by-year,
    writing intermediate results to disk after each year to prevent data loss
    on crash.  Existing fossil data is preserved untouched.

    :param repo_slug: GitHub ``owner/name`` slug.
    :param data_dir: Path to the ``data/`` output directory.
    :param reprocess: Optional ``YYYY-MM`` period to force re-processing.
    """
    repo_name = repo_slug.split("/")[-1]
    temp_repo_path = f"./temp_workdir_{repo_slug.replace('/', '__')}"
    output_json_path = os.path.join(data_dir, f"{repo_name}_data.json")

    try:
        if not os.path.exists(temp_repo_path):
            clone_repository(repo_slug, temp_repo_path)
        else:
            logger.info(
                "Repository %s already exists locally. Fetching latest...", repo_name
            )
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

        state = load_snapshot_data(output_json_path)
        historical_snapshots = state["snapshots"]
        existing_fossils = state.get("fossils", {})
        processed_periods = set(item["snapshot_date"] for item in historical_snapshots)

        all_periods = get_snapshot_periods(temp_repo_path)
        new_snapshots = _filter_snapshots(all_periods, processed_periods, reprocess)

        if not new_snapshots:
            logger.info(
                "[%s] No new periods to process. Data is already up to date!",
                repo_name,
            )
            return

        logger.info(
            "[%s] Processing %d new snapshots (quarterly pre-2025, monthly 2025+)",
            repo_name,
            len(new_snapshots),
        )

        snapshots_by_year = groupby(new_snapshots, key=lambda x: x[0][:4])
        total_new_data = []

        # Find the previous snapshot for incremental blame baseline
        prev_file_data: tuple[str, dict[str, dict[str, int]]] | None = None
        if historical_snapshots:
            last_hist = historical_snapshots[-1]
            hist_commit = last_hist.get("commit_hash", "")
            hist_compositions = last_hist.get("file_compositions")
            if hist_commit and hist_compositions:
                prev_file_data = (hist_commit, hist_compositions)
                logger.info(
                    "[%s] Using incremental blame from %s",
                    repo_name,
                    last_hist["snapshot_date"],
                )

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
                    "[%s] [%s] Processing %s (%d/%d) — Commit: %s",
                    repo_name,
                    year,
                    period,
                    idx,
                    len(year_snapshots_list),
                    commit[:7],
                )

                snapshot_start = time.perf_counter()
                distribution, file_compositions = analyze_single_snapshot(
                    temp_repo_path, commit, prev_file_data
                )
                snapshot_elapsed = time.perf_counter() - snapshot_start

                # Prepare prev_file_data for the next iteration
                prev_file_data = (commit, file_compositions)

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
                        "commit_hash": commit,
                        "composition": distribution,
                        "file_compositions": file_compositions,
                    }
                )

            total_new_data.extend(year_data)
            year_elapsed = time.perf_counter() - year_start

            final_snapshots = historical_snapshots + total_new_data
            final_snapshots.sort(key=lambda x: x["snapshot_date"])

            save_snapshot_data(output_json_path, final_snapshots, existing_fossils)

            logger.info(
                "[%s] Completed year %s in %.2f seconds. Wrote %d snapshots.",
                repo_name,
                year,
                year_elapsed,
                len(final_snapshots),
            )

    finally:
        from _utils import remove_path

        remove_path(temp_repo_path)


def main() -> None:
    """
    Entry point for the snapshot-analysis pipeline.

    CLI flags
    ---------
    --repo NAME         Process only the given repository (by config name).
    --reprocess YYYY-MM Re-process a specific snapshot period even if it already
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
        metavar="YYYY-MM",
        default=None,
        help="Re-process a specific snapshot period (e.g. '2023-06').",
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
        logger.info("Re-processing period: %s", args.reprocess)

    import concurrent.futures

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
