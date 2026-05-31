"""
Clean up and minify past snapshot data JSONs for the Theseus pipeline.

Per-file transformations (no logic changes):

1. Removes the redundant ``total_lines`` field from every snapshot.
2. Removes future-year keys from every snapshot's ``composition`` dict
   (e.g. a ``2023-06`` snapshot cannot contain ``2026`` entries).
3. Minifies the output JSON (no whitespace) to save disk space.

Fossil data is left untouched — only snapshot content is cleaned.
"""

import logging
import sys
from pathlib import Path

# Ensure sibling imports work in all invocation contexts
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from _data_io import load_snapshot_data, save_snapshot_data
from _utils import load_config

logger = logging.getLogger(__name__)


def cleanup_data(data_dir: str) -> bool:
    """
    Clean and minify all JSON data files in the specified directory.

    For each file, snapshots are cleaned (remove ``total_lines``, remove
    future-year composition keys) and the entire file is written back
    minified.  Fossil data is preserved unchanged.

    :param data_dir: Path to the ``data/`` directory.
    :return: ``True`` if any errors occurred, ``False`` otherwise.
    """
    data_path = Path(data_dir)
    if not data_path.exists() or not data_path.is_dir():
        print(f"Data directory not found or not a directory: {data_dir}")
        return True

    json_files = list(data_path.glob("*.json"))
    had_failures = False

    if not json_files:
        print(f"No JSON files found in {data_dir}")
        return had_failures

    for json_file in json_files:
        if json_file.name == "manifest.json":
            continue

        print(f"Processing {json_file.name}...")
        try:
            data = load_snapshot_data(str(json_file))
            snapshots = data["snapshots"]
            fossils = data.get("fossils", {})

            for snapshot in snapshots:
                if "total_lines" in snapshot:
                    del snapshot["total_lines"]

                snapshot_date = snapshot.get("snapshot_date")
                if snapshot_date:
                    max_year = int(snapshot_date[:4])
                    composition = snapshot.get("composition", {})
                    keys_to_remove = [
                        year_key
                        for year_key in composition.keys()
                        if int(year_key) > max_year
                    ]
                    for key in keys_to_remove:
                        del composition[key]

            save_snapshot_data(str(json_file), snapshots, fossils)
            print(f"  Successfully optimized and minified {json_file.name}")

        except Exception as e:  # noqa: BLE001
            print(f"  Error processing {json_file.name}: {e}")
            had_failures = True

    return had_failures


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
