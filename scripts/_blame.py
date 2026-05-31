"""
Shared git blame infrastructure for the Theseus pipeline.

Parses ``git blame --line-porcelain`` output and dispatches parallel blame
across file lists.  Two post-processing modes are exposed:

* ``parse_blame_year_counts``: aggregate lines per author-year for snapshot
  analysis (used by ``analyse_repository.py``).
* ``find_oldest_fossil_in_blame``: find the single oldest-authored line in a
  file's blame output (used by ``add_fossils.py``).

Fossil data model
-----------------
A **fossil** is a single source-code line whose author-timestamp is the
oldest ever found in a given scope.  Each fossil records:

``timestamp``
    Unix-epoch author-time.
``file``
    Relative file path.
``content``
    The actual source line text.
``year``
    4-digit year derived from ``timestamp``.
``commit``
    First 7 characters of the commit hash that last modified this line.
``view_commit``
    The git ref (commit hash or branch name) at which the file is checked out.
``line``
    1-based line number within the file.

Two concrete fossil types are discovered by the pipeline:

  **Genesis** — the single oldest-authored line *ever* to exist in the repo.
  Found by blaming only the files added in each of the earliest commits
  (sorted by author-time), scanning until ``stale_limit`` consecutive commits
  fail to improve the oldest-yet result.

  **Survivor** — the single oldest-authored line that *still exists* at the
  current HEAD.  Found by blaming every tracked file on the default branch.
"""

import concurrent.futures
import logging
import os
import sys
import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Ensure sibling imports work in all invocation contexts
_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from _utils import run_command

logger = logging.getLogger(__name__)

_HEX = frozenset("0123456789abcdef")

# OPTIMIZATION: _is_hash replaces `all(c in hex for c in s.lower())`.
# Profiling revealed that all() + generator expression is ~4.8M calls per parse
# run on a 15K-line blame output. Each call creates a generator object and
# iterates every character via Python bytecode. A manual for-loop over a
# frozenset avoids generator overhead and lets CPython's built-in set
# membership (C-level hash table lookup) handle the check. Also skips
# .lower() since git blame porcelain always emits lowercase hex hashes.
def _is_hash(s: str) -> bool:
    """Fast check if *s* is a 40- or 64-character lowercase hex string."""
    n = len(s)
    if n == 40:
        for c in s:
            if c not in _HEX:
                return False
        return True
    if n == 64:
        for c in s:
            if c not in _HEX:
                return False
        return True
    return False


# Fossil helper
def _blank_fossil() -> dict:
    """Return a blank fossil dict with the maximum possible timestamp."""
    return {
        "timestamp": 2_147_483_647,
        "file": "",
        "content": "",
        "year": "",
        "commit": "",
        "view_commit": "",
        "line": 0,
    }


# Single-file blame
def blame_single_file(repo_path: str | Path, file_path: str) -> str:
    """
    Run ``git blame --line-porcelain`` on a single file and return raw output.

    :param repo_path: Path to the git repository.
    :param file_path: Relative path of the file to blame.
    :return: Raw ``--line-porcelain`` output, or empty string on failure.
    """
    try:
        return run_command(
            ["git", "blame", "--line-porcelain", file_path],
            cwd=str(repo_path),
        )
    except RuntimeError:
        return ""


# Post-processing: year-count mode (for snapshot analysis)
# OPTIMIZATION: Three changes vs the original implementation.
#
# 1. Check "author-time" BEFORE the hash check. In blame porcelain, commit
#    header lines are ordered: hash first, then author-info, then filename, etc.
#    "author-time" appears far more often than non-hash keywords on non-hash
#    lines, so checking it first short-circuits the hash check for the bulk of
#    non-content lines.
#
# 2. Use _is_hash() instead of `all(c in hex for c in s.lower())`. The all()
#    + generator expression was ~4.8M calls per parse on a 15K-line blame
#    output, accounting for ~30% of total parse time.
#
# 3. Use str(dt.year) instead of dt.strftime("%Y"). strftime parses a format
#    string every call (C-level overhead), while .year is a direct struct
#    member access + str() conversion. Also caches the dict.get() path to
#    avoid an extra __getitem__ lookup per content line.
def parse_blame_year_counts(raw_output: str) -> dict[str, int]:
    """
    Parse ``git blame --line-porcelain`` output into a year-to-line-count map.

    :param raw_output: The raw porcelain output.
    :return: Dictionary mapping 4-digit year strings to line counts.
    """
    distribution = defaultdict(int)
    commit_to_year: dict[str, str] = {}
    current_commit: str | None = None

    for line in raw_output.splitlines():
        if line.startswith("\t"):
            if current_commit is not None:
                year = commit_to_year.get(current_commit)
                if year is not None:
                    distribution[year] += 1
        else:
            parts = line.split(" ")
            p0 = parts[0]
            if p0 == "author-time":
                try:
                    ts = int(parts[1])
                    year = str(datetime.fromtimestamp(ts, timezone.utc).year)
                    commit_to_year[current_commit] = year
                except (ValueError, IndexError):
                    pass
            elif _is_hash(p0):
                current_commit = p0

    return dict(distribution)


# Post-processing: oldest-fossil mode (for fossil discovery)
# OPTIMIZATION: Same three changes as parse_blame_year_counts.
# 1. Check "author-time" before hash (short-circuits the hash check for
#    the most common non-content header line type).
# 2. Use _is_hash() instead of all(genexpr) — removes ~4.8M generator
#    evaluations and the .lower() call (git porcelain uses lowercase hashes).
# 3. Use str(dt.year) instead of strftime("%Y") — faster C path.
def find_oldest_fossil_in_blame(
    raw_output: str, file_path: str, view_commit: str = ""
) -> dict:
    """
    Find the oldest-authored line in a single file's blame output.

    :param raw_output: Raw ``--line-porcelain`` output.
    :param file_path: Path of the blamed file (stored in the result).
    :param view_commit: Git ref to store as ``view_commit`` in the result.
    :return: A fossil dict for the oldest line found, or a blank fossil if no
             lines could be blamed.
    """
    fossil = _blank_fossil()
    current_commit_data: dict[str, str | int] = {}
    line_num = 0

    for line in raw_output.splitlines():
        if line.startswith("\t"):
            line_num += 1
            ts = current_commit_data.get("author-time")
            content = line.lstrip("\t").strip()
            if ts is not None and ts < fossil["timestamp"] and content:
                fossil["timestamp"] = ts
                fossil["file"] = file_path
                fossil["content"] = content
                fossil["year"] = str(
                    datetime.fromtimestamp(ts, timezone.utc).year
                )
                commit_hash = current_commit_data.get("commit", "")
                fossil["commit"] = (commit_hash[:7] if isinstance(commit_hash, str)
                                    else str(commit_hash)[:7])
                fossil["view_commit"] = view_commit
                fossil["line"] = line_num
        else:
            parts = line.split(" ")
            p0 = parts[0]
            if p0 == "author-time" and len(parts) >= 2:
                try:
                    current_commit_data["author-time"] = int(parts[1])
                except ValueError:
                    pass
            elif _is_hash(p0):
                current_commit_data = {"commit": p0}

    return fossil


# Parallel blame runner (internal)
def _blame_files_internal(
    repo_path: str | Path,
    files: list[str],
    max_workers: int,
    process_result,
    total_files_hint: int | None = None,
) -> None:
    """
    Blame files in parallel and call ``process_result(file, raw_output)`` for each.

    Logs 10 % progress steps so the user sees the script is making progress.

    :param repo_path: Path to the git repository.
    :param files: List of relative file paths to blame.
    :param max_workers: Maximum number of parallel blame processes.
    :param process_result: Callback ``(file_path: str, raw_output: str) -> None``.
    :param total_files_hint: For display purposes only; overrides the log count.
    """
    total = total_files_hint or len(files)
    completed = 0
    next_log_pct = 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(blame_single_file, repo_path, f): f for f in files
        }

        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            raw_output = future.result()
            if raw_output:
                process_result(file_path, raw_output)

            completed += 1
            pct = completed / total * 100
            if pct >= next_log_pct:
                logger.info("  Blame progress: %d/%d (%.0f%%)", completed, total, pct)
                next_log_pct += 10


# Single-file year-count (for incremental blame)
def blame_single_file_year_counts(
    repo_path: str | Path, file_path: str
) -> dict[str, int]:
    """
    Run ``git blame --line-porcelain`` on a single file and return its
    year-to-line-count map.

    :param repo_path: Path to the git repository.
    :param file_path: Relative path of the file to blame.
    :return: ``{year: line_count}`` for this file, or empty dict on failure.
    """
    raw = blame_single_file(repo_path, file_path)
    if raw:
        return parse_blame_year_counts(raw)
    return {}


def blame_files_to_file_compositions(
    repo_path: str | Path,
    files: list[str],
    max_workers: int = 8,
) -> dict[str, dict[str, int]]:
    """
    Blame a list of files in parallel and return per-file year-count maps.

    :param repo_path: Path to the git repository.
    :param files: List of relative file paths to blame.
    :param max_workers: Maximum parallel blame processes (default 8).
    :return: ``{file_path: {year: count}}`` for each blamed file.
    """
    if not files:
        return {}
    logger.info("  Blaming %d changed files (%d workers)...", len(files), max_workers)
    result: dict[str, dict[str, int]] = {}
    lock = threading.Lock()

    def _store(file_path: str, raw_output: str) -> None:
        counts = parse_blame_year_counts(raw_output)
        with lock:
            result[file_path] = counts

    _blame_files_internal(repo_path, files, max_workers, _store)
    return result


# Public parallel-blame helpers
def blame_files_year_counts(
    repo_path: str | Path, files: list[str], max_workers: int = 8
) -> dict[str, int]:
    """
    Blame a list of files in parallel and return an aggregated year-to-line-count map.

    :param repo_path: Path to the git repository.
    :param files: List of relative file paths to blame.
    :param max_workers: Maximum parallel blame processes (default 8).
    :return: ``{year: line_count}`` aggregated across all files.
    """
    logger.info("  Blaming %d files (%d workers)...", len(files), max_workers)
    age_distribution: dict[str, int] = defaultdict(int)

    def _accumulate(file_path: str, raw_output: str) -> None:
        for year, count in parse_blame_year_counts(raw_output).items():
            age_distribution[year] += count

    _blame_files_internal(repo_path, files, max_workers, _accumulate)
    return dict(age_distribution)


def blame_files_oldest_fossil(
    repo_path: str | Path,
    files: list[str],
    max_workers: int = 20,
    view_commit: str = "",
) -> dict:
    """
    Blame a list of files in parallel and return the single oldest fossil found.

    :param repo_path: Path to the git repository.
    :param files: List of relative file paths to blame.
    :param max_workers: Maximum parallel blame processes (default 20).
    :param view_commit: Git ref to store as ``view_commit`` in the result.
    :return: Fossil dict for the oldest line across all files, or a blank
             fossil if no lines could be blamed.
    """
    global_oldest = _blank_fossil()

    def _find(file_path: str, raw_output: str) -> None:
        nonlocal global_oldest
        fossil = find_oldest_fossil_in_blame(raw_output, file_path, view_commit)
        if fossil["timestamp"] < global_oldest["timestamp"] and fossil["file"]:
            global_oldest = fossil

    _blame_files_internal(repo_path, files, max_workers, _find)
    return global_oldest
