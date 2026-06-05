"""
Ensure the scripts directory is on sys.path for sibling imports.

Every script in this directory should ``import _path_guard`` (with a
``# noqa: F401`` comment if the linter complains) before importing any
sibling module.  This is a no-op when the script's directory is already
on ``sys.path`` (the normal case for ``python scripts/foo.py``), but
guarantees correctness for ``python -m scripts.foo`` and test-runner
invocations.
"""

import sys
from pathlib import Path

_SCRIPTS_DIR = str(Path(__file__).resolve().parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)
