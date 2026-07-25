"""
Shared JSON I/O for Theseus data pipeline scripts.

All repository data files share a common top-level structure:

.. code-block:: json

    {
      "snapshots": [ { "snapshot_date": "YYYY-MM", "composition": {"YYYY": count, ...} }, ... ],
      "fossils":   { "genesis": { ... }, "survivor": { ... } }
    }

The ``fossils`` object stores the two fossil types:

  **Genesis** (``fossils.genesis``)
      The single oldest-authored line *ever* written in the repository.
      Discovered by blaming only files added in each of the earliest commits
      (sorted by author-time) and returning the line with the smallest
      author-timestamp.

  **Survivor** (``fossils.survivor``)
      The single oldest-authored line that *still exists* at the current HEAD.
      Discovered by blaming every tracked file on the default branch and
      returning the line with the smallest author-timestamp.

  Each fossil stores:

  ``timestamp``
      Unix-epoch author-time of the oldest line.
  ``file``
      Relative file path.
  ``content``
      The actual source line.
  ``year``
      4-digit year derived from ``timestamp``.
  ``commit``
      First 7 characters of the commit hash.
  ``view_commit``
      The git ref (commit hash or branch name) used to view this file.
  ``line``
      Line number within the file.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_snapshot_data(file_path: str | Path) -> dict:
    """
    Load snapshot data from a JSON file, normalising to ``{snapshots, fossils}``.

    Supports both the new dict schema (``{"snapshots": [...], "fossils": {...}}``)
    and the legacy list schema (``[{...}, ...]``).

    :param file_path: Path to the JSON data file.
    :return: Dictionary with ``snapshots`` (list) and ``fossils`` (dict) keys.
    """
    path = Path(file_path)
    if not path.exists():
        return {"snapshots": [], "fossils": {}}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return {"snapshots": data, "fossils": {}}
        if isinstance(data, dict):
            snapshots = data.get("snapshots")
            if not isinstance(snapshots, list):
                snapshots = []
            fossils = data.get("fossils")
            if not isinstance(fossils, dict):
                fossils = {}
            return {"snapshots": snapshots, "fossils": fossils}
        return {"snapshots": [], "fossils": {}}
    except json.JSONDecodeError:
        logger.warning("%s is corrupted, starting fresh.", file_path)
        return {"snapshots": [], "fossils": {}}


def save_snapshot_data(file_path: str | Path, snapshots: list[dict], fossils: dict) -> None:
    """
    Atomically write snapshot data to a minified JSON file.

    Writes to a ``.tmp`` sibling first, then atomically replaces the target
    to prevent file corruption on crash.

    :param file_path: Destination path.
    :param snapshots: List of snapshot objects.
    :param fossils: Fossil dictionary (``genesis`` + ``survivor`` keys).
    """
    path = Path(file_path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    data = {"snapshots": snapshots, "fossils": fossils}
    tmp_path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    tmp_path.replace(path)


def load_latest_state(file_path: str | Path) -> tuple[str | None, dict | None]:
    """
    Load the latest file_compositions state for incremental blame.
    
    :param file_path: Path to the state JSON file.
    :return: (commit_hash, file_compositions) or (None, None) if not found.
    """
    path = Path(file_path)
    if not path.exists():
        return None, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("commit_hash"), data.get("file_compositions")
    except json.JSONDecodeError:
        logger.warning("%s is corrupted, ignoring state.", file_path)
        return None, None


def save_latest_state(file_path: str | Path, commit_hash: str, file_compositions: dict) -> None:
    """
    Save the latest file_compositions state atomically.
    
    :param file_path: Destination path.
    :param commit_hash: The commit hash of the state.
    :param file_compositions: The file compositions dict.
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    data = {"commit_hash": commit_hash, "file_compositions": file_compositions}
    tmp_path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    tmp_path.replace(path)
