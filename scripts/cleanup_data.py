import json
import os
from pathlib import Path

def cleanup_data(data_dir: str):
    """
    Cleans up all JSON data files in the specified directory.
    - Removes 'total_lines' (redundant)
    - Removes future-year keys in 'composition'
    - Minifies output
    """
    data_path = Path(data_dir)
    json_files = list(data_path.glob("*.json"))
    
    if not json_files:
        print(f"No JSON files found in {data_dir}")
        return

    for json_file in json_files:
        if json_file.name == "manifest.json":
            continue
            
        print(f"Processing {json_file.name}...")
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            for snapshot in data:
                # 1. Remove redundant total_lines
                if "total_lines" in snapshot:
                    del snapshot["total_lines"]
                
                # 2. Filter future years
                snapshot_date = snapshot.get("snapshot_date")
                if snapshot_date:
                    max_year = int(snapshot_date[:4])
                    composition = snapshot.get("composition", {})
                    keys_to_remove = [year for year in composition.keys() if int(year) > max_year]
                    for key in keys_to_remove:
                        del composition[key]
            
            # 3. Write MINIFIED
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(data, f, separators=(",", ":"))
            print(f"  Successfully optimized and minified {json_file.name}")
                
        except Exception as e:
            print(f"  Error processing {json_file.name}: {e}")

if __name__ == "__main__":
    DATA_DIR = "./data"
    cleanup_data(DATA_DIR)
