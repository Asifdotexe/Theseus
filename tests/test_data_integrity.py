"""
Tests data integrity and schema validation for Theseus generated JSON files.
"""

import json
from pathlib import Path


def test_data_integrity_optimized_schema():
    """
    Test that the data follows the optimized schema:
    1. No 'total_lines' field (it's redundant)
    2. No future-year keys in 'composition'
    3. Supports both list and object schemas (backwards compatibility)
    """
    data_dir = Path("./data")
    json_files = list(data_dir.glob("*.json"))

    json_files = [f for f in json_files if f.name != "manifest.json"]

    assert len(json_files) > 0, "No data files found in ./data"

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Support both list schema (legacy) and object schema (new)
        if isinstance(data, dict):
            snapshots = data.get("snapshots", [])
            fossils = data.get("fossils", {})
        else:
            snapshots = data
            fossils = {}

        for snapshot in snapshots:
            # 1. total_lines MUST be absent
            assert "total_lines" not in snapshot, (
                f"Error in {json_file.name}: 'total_lines' field should be "
                f"stripped for optimization but was found in {snapshot.get('snapshot_date')}"
            )

            # 2. Composition year check
            snapshot_date = snapshot.get("snapshot_date")
            if not snapshot_date:
                continue

            snapshot_year = int(snapshot_date[:4])
            composition = snapshot.get("composition", {})

            for year_key in composition.keys():
                year = int(year_key)
                assert year <= snapshot_year, (
                    f"Error in {json_file.name}: Snapshot {snapshot_date} "
                    f"contains impossible future year {year} in composition."
                )

        # 3. Validate fossil structure if present (only if not empty)
        for fossil_type, fossil_data in fossils.items():
            if not isinstance(fossil_data, dict):
                continue
            if not fossil_data:  # Empty fossil object is OK
                continue
            assert (
                "year" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'year' field"
            assert (
                "content" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'content' field"
            # New required fields: commit, line, file
            assert (
                "file" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'file' field"
            assert (
                "commit" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'commit' field"
            assert (
                "line" in fossil_data
            ), f"Error in {json_file.name}: Fossil '{fossil_type}' missing 'line' field"


if __name__ == "__main__":
    test_data_integrity_optimized_schema()
    print("All optimized data integrity checks passed!")
