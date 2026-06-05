"""
Ensure the scripts directory is on sys.path for sibling imports.

When ``scripts`` is imported as a package (e.g. ``from scripts._blame import ...``
from tests), this adds the package directory to ``sys.path`` so that subsequent
``import _path_guard`` calls from sibling modules resolve correctly.
"""

import sys
from pathlib import Path

_SCRIPTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)
