"""
Module for cleaning up and minifying past snapshot data JSONs.
"""

import json
from pathlib import Path


def cleanup_data(data_dir: str) -> bool:
    """
    Cleans up all JSON data files in the specified directory.
    - Removes 'total_lines' (redundant)
    - Removes future-year keys in 'composition'
    - Minifies output
    Returns True if an error occurred, False otherwise.
    """
    data_path = Path(data_dir)
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
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Handle both list and object schemas
            snapshots = data.get("snapshots", data) if isinstance(data, dict) else data

            for snapshot in snapshots:
                # 1. Remove redundant total_lines
                if "total_lines" in snapshot:
                    del snapshot["total_lines"]

                # 2. Filter future years
                snapshot_date = snapshot.get("snapshot_date")
                if snapshot_date:
                    max_year = int(snapshot_date[:4])
                    composition = snapshot.get("composition", {})
                    keys_to_remove = [
                         year for year in composition.keys() if int(year) > max_year
                    ]
                    for key in keys_to_remove:
                        del composition[key]

            # Write back with original schema
            if isinstance(data, dict):
                data["snapshots"] = snapshots
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, separators=(",", ":"))
            else:
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(snapshots, f, separators=(",", ":"))
            print(f"  Successfully optimized and minified {json_file.name}")

        except Exception as e:
            print(f"  Error processing {json_file.name}: {e}")
            had_failures = True

    return had_failures

def main():
    import sys
    config_path = "theseus.config.json"
    if not Path(config_path).exists():
        print(f"Configuration file not found: {config_path}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    data_dir = config.get("dataDir", "./data")
    if cleanup_data(data_dir):
        print("One or more files failed to clean up. Exiting non-zero.")
        sys.exit(1)

if __name__ == "__main__":
    main()
