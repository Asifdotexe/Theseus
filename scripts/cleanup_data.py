"""
Clean up raw snapshot data and generate processed graph data for the frontend.

This script acts as the custodian of the data layer. As the analysis pipeline runs,
the raw JSON files accumulate metadata, such as legacy file compositions and future-year
composition entries, which are no longer required for frontend rendering.

Why do we need this script?
Serving unoptimized, bloated JSON files degrades the client-side user experience.
This script strips out all pipeline-internal fields (like `commit_hash`),
leaving only the essential `snapshot_date` and `composition` data. It ensures the frontend
payload is strictly minimized to what is necessary for chart rendering.
"""

import json
import logging
import sys
from pathlib import Path

from scripts._data_io import load_fossils, load_history, save_history
from scripts._utils import load_config

logger = logging.getLogger(__name__)

# We use a frozenset here because it provides O(1) constant-time membership lookups
# which is slightly faster than a list or tuple, and its immutability guarantees
# that these fields cannot be accidentally modified at runtime.
GRAPH_FIELDS = frozenset({"snapshot_date", "composition"})


def _clean_snapshots(snapshots: list[dict]) -> list[dict]:
    """
    Remove future-year composition keys and legacy file_compositions from snapshots.

    :param snapshots: A list of snapshot dictionaries to be cleaned.
    """
    for snapshot in snapshots:
        # Strip legacy file_compositions if present
        if "file_compositions" in snapshot:
            del snapshot["file_compositions"]

        snapshot_date = snapshot.get("snapshot_date")
        if snapshot_date:
            max_year = int(snapshot_date[:4])
            composition = snapshot.get("composition", {})
            for key in list(composition.keys()):
                if int(key) > max_year:
                    del composition[key]
    return snapshots


def cleanup_raw_history_data(data_dir: str) -> bool:
    """
    Clean and minify raw data files in ``data_dir/raw/``.

    Under the hood, this function reads every `{repo}_history.jsonl` file,
    runs `_clean_snapshots` to strip out any future-year compositions or legacy fields,
    and then rewrites the cleaned snapshots back into the exact same file. This step
    ensures our raw data store is normalized before graph generation.

    :param data_dir: Path to the ``data/`` directory.
    :return: ``True`` if any errors occurred during the cleanup process.
    """
    raw_path = Path(data_dir) / "raw"
    if not raw_path.exists():
        return False

    had_failures = False
    for jsonl_file in sorted(raw_path.glob("*_history.jsonl")):
        print(f"Cleaning raw: {jsonl_file.name}...")
        try:
            snapshots = load_history(jsonl_file)
            snapshots = _clean_snapshots(snapshots)
            save_history(jsonl_file, snapshots)
        except OSError as e:
            print(f"  Error: {e}")
            had_failures = True

    return had_failures


def generate_frontend_graph_data(data_dir: str) -> bool:
    """
    Generate processed graph data from raw data.

    Reads ``data/raw/{name}_history.jsonl``, strips pipeline-internal fields
    (``commit_hash``), and writes
    ``data/processed/{name}_graph.json`` with only ``snapshot_date`` +
    ``composition`` per entry plus the fossil block from ``data/raw/{name}_fossils.json``.

    :param data_dir: Path to the ``data/`` directory.
    :return: ``True`` if any errors occurred.
    """
    raw_path = Path(data_dir) / "raw"
    processed_path = Path(data_dir) / "processed"
    processed_path.mkdir(exist_ok=True)

    if not raw_path.exists():
        return False

    had_failures = False
    for jsonl_file in sorted(raw_path.glob("*_history.jsonl")):
        repo_name = jsonl_file.stem.replace("_history", "")
        out_name = f"{repo_name}_graph.json"
        fossil_file = raw_path / f"{repo_name}_fossils.json"
        print(f"Generating graph: {out_name}...")

        try:
            snapshots = load_history(jsonl_file)
            fossils = load_fossils(fossil_file)

            graph_snapshots = [
                {k: v for k, v in snap.items() if k in GRAPH_FIELDS}
                for snap in snapshots
                if any(k in GRAPH_FIELDS for k in snap)
            ]

            out_path = processed_path / out_name
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"snapshots": graph_snapshots, "fossils": fossils},
                    f,
                    separators=(",", ":"),
                )

        except OSError as e:
            print(f"  Error: {e}")
            had_failures = True

    return had_failures


def execute_full_cleanup_and_graph_generation(data_dir: str) -> bool:
    """
    Run both raw history cleanup and frontend graph data generation.

    This function serves as the primary orchestrator for the cleanup stage, sequentially
    triggering `cleanup_raw_history_data` and then `generate_frontend_graph_data`.
    It aggregates failure states from both steps.

    :param data_dir: Path to the ``data/`` directory.
    :return: ``True`` if any errors occurred across either step.
    """
    had_errors = False
    if cleanup_raw_history_data(data_dir):
        had_errors = True
    if generate_frontend_graph_data(data_dir):
        had_errors = True
    return had_errors


def main() -> None:
    """
    Entry point for data cleanup.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    config = load_config()
    data_dir = config.get("dataDir", "./data")
    if execute_full_cleanup_and_graph_generation(data_dir):
        print("One or more files failed to clean up. Exiting non-zero.")
        sys.exit(1)


if __name__ == "__main__":
    main()
