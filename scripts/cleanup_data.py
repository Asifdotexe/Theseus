"""
Clean up raw snapshot data and generate processed graph data for the frontend.

Raw data (``data/raw/{name}_data.json``) is cleaned of future-year composition
entries and minified.  Processed graph data (``data/processed/{name}.json``)
is stripped of pipeline-internal fields (``commit_hash``, ``file_compositions``)
so the frontend only sees ``snapshot_date`` + ``composition`` per entry.
"""

import json
import logging
import sys
from pathlib import Path

from _data_io import load_snapshot_data, save_snapshot_data
from _utils import load_config

logger = logging.getLogger(__name__)

GRAPH_FIELDS = frozenset({"snapshot_date", "composition"})


def _clean_snapshots(snapshots: list[dict]) -> list[dict]:
    """Remove future-year composition keys and total_lines from snapshots."""
    for snapshot in snapshots:
        snapshot.pop("total_lines", None)
        snapshot_date = snapshot.get("snapshot_date")
        if snapshot_date:
            max_year = int(snapshot_date[:4])
            composition = snapshot.get("composition", {})
            for key in list(composition.keys()):
                if int(key) > max_year:
                    del composition[key]
    return snapshots


def cleanup_raw(data_dir: str) -> bool:
    """
    Clean and minify raw data files in ``data_dir/raw/``.

    Removes future-year composition entries and ``total_lines`` fields.
    Writes back minified to the same location.

    :param data_dir: Path to the ``data/`` directory.
    :return: ``True`` if any errors occurred.
    """
    raw_path = Path(data_dir) / "raw"
    if not raw_path.exists():
        return False

    had_failures = False
    for json_file in sorted(raw_path.glob("*.json")):
        if json_file.name == "manifest.json":
            continue
        print(f"Cleaning raw: {json_file.name}...")
        try:
            data = load_snapshot_data(str(json_file))
            snapshots = _clean_snapshots(data["snapshots"])
            fossils = data.get("fossils", {})
            save_snapshot_data(str(json_file), snapshots, fossils)
        except Exception as e:  # noqa: BLE001
            print(f"  Error: {e}")
            had_failures = True

    return had_failures


def generate_graph_data(data_dir: str) -> bool:
    """
    Generate processed graph data from raw data.

    Reads ``data/raw/{name}_data.json``, strips pipeline-internal fields
    (``commit_hash``, ``file_compositions``), and writes
    ``data/processed/{name}.json`` with only ``snapshot_date`` +
    ``composition`` per entry plus the fossil block.

    :param data_dir: Path to the ``data/`` directory.
    :return: ``True`` if any errors occurred.
    """
    raw_path = Path(data_dir) / "raw"
    processed_path = Path(data_dir) / "processed"
    processed_path.mkdir(exist_ok=True)

    if not raw_path.exists():
        return False

    had_failures = False
    for json_file in sorted(raw_path.glob("*.json")):
        if json_file.name == "manifest.json":
            continue

        repo_name = json_file.stem.replace("_data", "")
        out_name = f"{repo_name}_graph.json"
        print(f"Generating graph: {out_name}...")

        try:
            data = load_snapshot_data(str(json_file))
            snapshots = data["snapshots"]
            fossils = data.get("fossils", {})

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

        except Exception as e:  # noqa: BLE001
            print(f"  Error: {e}")
            had_failures = True

    return had_failures


def cleanup_data(data_dir: str) -> bool:
    """
    Run both raw cleanup and graph generation.

    Kept as the public entry point for backward compatibility with
    ``run_pipeline.py``.

    :param data_dir: Path to the ``data/`` directory.
    :return: ``True`` if any errors occurred.
    """
    had_errors = False
    if cleanup_raw(data_dir):
        had_errors = True
    if generate_graph_data(data_dir):
        had_errors = True
    return had_errors


def main() -> None:
    """
    Entry point for data cleanup.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    config = load_config()
    data_dir = config.get("dataDir", "./data")
    if cleanup_data(data_dir):
        print("One or more files failed to clean up. Exiting non-zero.")
        sys.exit(1)


if __name__ == "__main__":
    main()
