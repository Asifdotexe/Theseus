import json
import os
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from scripts.analyse_repository import (analyze_single_snapshot,
                                        ensure_repo_ready,
                                        get_snapshot_periods)


# Helper utility to convert raw byte counts into human-readable formats.
# This is needed to clearly visualize the data storage bloat caused by
# storing full file compositions vs aggregated compositions in the benchmark output.
def format_bytes(size):
    """
    Format a byte size into a human-readable string representation.

    :param size: The size in bytes to be formatted.
    :type size: float or int
    """
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0


# Core benchmark routine designed to establish the unit speed of the data pipeline.
# This test is needed to measure the parsing speed (lines per second) of full vs incremental
# git blame operations, and to quantify the disk storage bloat factor. It provides a baseline
# to ensure upstream architectural changes improve efficiency without causing regressions.
def run_benchmark():
    """
    Execute the pipeline benchmark against a predefined repository.

    This function sets up the target repository, extracts the snapshot periods,
    and profiles the first few snapshots to measure execution time, processing speed,
    and data storage size differences between aggregated and raw file compositions.
    """
    repo_slug = "anthropics/claude-code"
    repo_name = "claude-code"
    temp_repo_path = "../temp_workdir_benchmark"

    print(f"--- Benchmarking pipeline on {repo_name} ---")
    ensure_repo_ready(repo_slug, repo_name, temp_repo_path)

    periods = get_snapshot_periods(temp_repo_path)
    if not periods:
        print("No periods found.")
        return

    print(
        f"Found {len(periods)} snapshots. Benchmarking the first 3 (1 full, 2 incremental)...\n"
    )

    prev_file_data = None

    for i in range(3):
        period, commit = periods[i]
        print(f"[{i+1}/3] Profiling Snapshot: {period} (Commit: {commit[:7]})")

        mode = "FULL BLAME" if prev_file_data is None else "INCREMENTAL BLAME"

        t0 = time.perf_counter()
        distribution, file_compositions = analyze_single_snapshot(
            temp_repo_path, commit, prev_file_data
        )
        t1 = time.perf_counter()

        elapsed = t1 - t0
        total_lines = sum(distribution.values())
        print(f"  Mode: {mode}")
        print(f"  Time: {elapsed:.2f} seconds")
        print(f"  Lines processed: {total_lines}")
        print(f"  Speed: {(total_lines / elapsed) if elapsed > 0 else 0:.0f} lines/sec")

        # Profile Data Storage Size
        # 1. Size of just the composition (what we need historically)
        comp_size = len(json.dumps(distribution).encode("utf-8"))
        # 2. Size of full file_compositions (what we currently store historically)
        file_comp_size = len(json.dumps(file_compositions).encode("utf-8"))

        print(f"  Data Storage:")
        print(f"    Aggregated Composition Size: {format_bytes(comp_size)}")
        print(f"    File Compositions Size:      {format_bytes(file_comp_size)}")
        print(f"    (Bloat factor: {file_comp_size/comp_size:.1f}x)\n")

        prev_file_data = (commit, file_compositions)


if __name__ == "__main__":
    run_benchmark()
