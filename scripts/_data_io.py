"""
Shared JSON I/O for Theseus data pipeline scripts.

This module provides utilities for reading and writing snapshot and state data
for the Ship of Theseus pipeline. It manages historical snapshots in an append-only
JSON Lines format and keeps track of the latest state to allow for incremental processing.

Fossils (the oldest lines of code) are managed in their own separate JSON files.
"""

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def _ensure_parent_dir(path: Path) -> None:
    """
    Ensure the parent directory of a path exists.
    
    :param path: The file path whose parent directory should exist.
    """
    path.parent.mkdir(parents=True, exist_ok=True)


def _atomic_replace(tmp_path: Path, target_path: Path) -> None:
    """
    Atomically replace the target file with a temporary file.
    
    :param tmp_path: The temporary file containing the new data.
    :param target_path: The final destination path.
    """
    tmp_path.replace(target_path)


def get_last_history_snapshot(file_path: str | Path) -> dict | None:
    """
    Read the last snapshot from a JSON Lines history file.

    We use this function to quickly peek at the most recent snapshot without having to 
    load the entire repository history into memory. This is especially helpful for large 
    codebases where the history file can grow to hundreds of megabytes. By just reading 
    the last line of the JSONL file, we can figure out our current baseline and seamlessly 
    resume incremental blame analysis.

    :param file_path: Path to the JSON Lines history file.
    :return: The last snapshot dictionary, or None if the file is empty or does not exist.
    """
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            last_line = None
            for line in f:
                if line.strip():
                    last_line = line
            if last_line:
                return json.loads(last_line)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not parse the last line of %s: %s", file_path, e)
    return None


def append_history_snapshot(file_path: str | Path, snapshot: dict) -> None:
    """
    Append a snapshot to a JSON Lines history file.

    :param file_path: Path to the JSON Lines history file.
    :param snapshot: The snapshot dictionary to append.
    """
    path = Path(file_path)
    # Make sure the target directory exists before trying to create a file within it,
    # otherwise open() will raise a FileNotFoundError.
    _ensure_parent_dir(path)
    
    with open(path, "a", encoding="utf-8") as f:
        # We serialize the dictionary into a compact, single-line JSON string without spaces,
        # followed by a newline. This strictly adheres to the JSON Lines (JSONL) format requirement
        # where each line must be a valid independent JSON object.
        f.write(json.dumps(snapshot, separators=(",", ":")) + "\n")


def load_history(file_path: str | Path) -> list[dict]:
    """
    Load all snapshots from a JSON Lines history file.

    :param file_path: Path to the JSON Lines history file.
    :return: A list of snapshot dictionaries.
    """
    path = Path(file_path)
    if not path.exists():
        return []
    snapshots = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    snapshots.append(json.loads(line))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not read history from %s: %s", file_path, e)
    return snapshots


def save_history(file_path: str | Path, snapshots: list[dict]) -> None:
    """
    Save all snapshots to a JSON Lines history file.

    :param file_path: Path to the JSON Lines history file.
    :param snapshots: A list of snapshot dictionaries to save.
    """
    path = Path(file_path)
    _ensure_parent_dir(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        for snapshot in snapshots:
            f.write(json.dumps(snapshot, separators=(",", ":")) + "\n")
    _atomic_replace(tmp_path, path)


def load_fossils(file_path: str | Path) -> dict:
    """
    Load fossils from a JSON file.

    :param file_path: Path to the fossils JSON file.
    :return: Dictionary containing fossil records.
    """
    path = Path(file_path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Could not load fossils from %s: %s", file_path, e)
        return {}


def save_fossils(file_path: str | Path, fossils: dict) -> None:
    """
    Save fossils to a JSON file atomically.

    :param file_path: Path to the fossils JSON file.
    :param fossils: Dictionary containing fossil records.
    """
    path = Path(file_path)
    _ensure_parent_dir(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(fossils, separators=(",", ":")), encoding="utf-8")
    _atomic_replace(tmp_path, path)


def load_latest_state(file_path: str | Path) -> tuple[str | None, dict | None]:
    """
    Load the latest file_compositions state for incremental blame.
    
    :param file_path: Path to the state JSON file.
    :return: A tuple containing the commit hash and file compositions, or (None, None) if not found.
    """
    path = Path(file_path)
    if not path.exists():
        return None, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("commit_hash"), data.get("file_compositions")
    except (json.JSONDecodeError, OSError):
        logger.warning("%s is corrupted, ignoring state.", file_path)
        return None, None


def save_latest_state(file_path: str | Path, commit_hash: str, file_compositions: dict) -> None:
    """
    Save the latest file_compositions state atomically.
    
    :param file_path: Destination path for the state JSON file.
    :param commit_hash: The commit hash of the state.
    :param file_compositions: The file compositions dict.
    """
    path = Path(file_path)
    _ensure_parent_dir(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    data = {"commit_hash": commit_hash, "file_compositions": file_compositions}
    tmp_path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    _atomic_replace(tmp_path, path)
