"""
Truthset and latency benchmark regression tests.

Uses the static 77-commit repository Asifdotexe/SentimentScoringModel as a real-world
truthset to verify snapshot calculation, fossil discovery, state preservation,
and execution latency.
"""

import os
import platform
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=wrong-import-position,too-many-statements
import socket
import pytest

from scripts._data_io import load_fossils, load_history, load_latest_state
from scripts._utils import remove_path
from scripts.analyse_repository import ensure_engine_ready, process_repository

TRUTHSET_REPO = "Asifdotexe/SentimentScoringModel"
TRUTHSET_NAME = "SentimentScoringModel"

EXPECTED_SNAPSHOT = {
    "snapshot_date": "2024-06",
    "commit_hash": "a9842e14ad073730d88d9cdd5603e857b56be74e",
    "composition": {"2024": 258279},
}

EXPECTED_GENESIS = {
    "commit": "9c7ade5",
    "file": "README.md",
    "content": "# nlp-sentimental-analysis-on-amazon-reviews",
    "year": "2024",
    "line": 1,
}

EXPECTED_SURVIVOR = {
    "commit": "c7cc4e0",
    "file": ".gitattributes",
    "content": "Office_Products_5.json filter=lfs diff=lfs merge=lfs -text",
    "year": "2024",
    "line": 1,
}


def test_sentiment_scoring_model_truthset_and_benchmark():
    """
    Execute full engine analysis on SentimentScoringModel and verify output
    against the truthset while monitoring execution latency.
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if platform.system() == "Windows":
        binary_path = os.path.join(repo_root, "engine", "target", "release", "engine.exe")
    else:
        binary_path = os.path.join(repo_root, "engine", "target", "release", "engine")

    if not os.path.isfile(binary_path):
        pytest.skip(f"Release engine binary not found at {binary_path}. Run 'cargo build --release' first.")

    try:
        with socket.create_connection(("github.com", 443), timeout=3):
            pass
    except OSError:
        pytest.skip("Network connection to github.com unavailable; skipping truthset benchmark.")

    ensure_engine_ready()
    temp_dir = tempfile.mkdtemp(prefix="theseus_benchmark_")

    try:
        start_time = time.perf_counter()

        # Run snapshot analysis and fossil extraction via compiled engine
        process_repository(TRUTHSET_REPO, temp_dir)

        elapsed = time.perf_counter() - start_time
        print(f"\n[BENCHMARK] SentimentScoringModel analyzed in {elapsed:.2f}s")

        # 1. Verify history snapshot truthset
        history_file = os.path.join(temp_dir, "raw", f"{TRUTHSET_NAME}_history.jsonl")
        assert os.path.exists(history_file), f"Missing history file at {history_file}"
        snapshots = load_history(history_file)
        assert len(snapshots) == 1, f"Expected 1 snapshot, got {len(snapshots)}"

        actual_snap = snapshots[0]
        assert actual_snap["snapshot_date"] == EXPECTED_SNAPSHOT["snapshot_date"]
        assert actual_snap["commit_hash"] == EXPECTED_SNAPSHOT["commit_hash"]
        assert actual_snap["composition"] == EXPECTED_SNAPSHOT["composition"]

        # 2. Verify fossils truthset
        fossil_file = os.path.join(temp_dir, "raw", f"{TRUTHSET_NAME}_fossils.json")
        assert os.path.exists(fossil_file), f"Missing fossils file at {fossil_file}"
        fossils = load_fossils(fossil_file)

        genesis = fossils.get("genesis", {})
        assert genesis["commit"] == EXPECTED_GENESIS["commit"]
        assert genesis["file"] == EXPECTED_GENESIS["file"]
        assert genesis["content"] == EXPECTED_GENESIS["content"]
        assert genesis["year"] == EXPECTED_GENESIS["year"]
        assert genesis["line"] == EXPECTED_GENESIS["line"]

        survivor = fossils.get("survivor", {})
        assert survivor["commit"] == EXPECTED_SURVIVOR["commit"]
        assert survivor["file"] == EXPECTED_SURVIVOR["file"]
        assert survivor["content"] == EXPECTED_SURVIVOR["content"]
        assert survivor["year"] == EXPECTED_SURVIVOR["year"]
        assert survivor["line"] == EXPECTED_SURVIVOR["line"]

        # 3. Verify state serialization
        state_file = os.path.join(temp_dir, "state", f"{TRUTHSET_NAME}_state.json")
        assert os.path.exists(state_file), f"Missing state file at {state_file}"
        commit_hash, file_comps = load_latest_state(state_file)
        assert commit_hash == EXPECTED_SNAPSHOT["commit_hash"]
        assert isinstance(file_comps, dict)
        assert len(file_comps) == 19

        # 4. Latency threshold: 77 commits and clone should finish well within 60 seconds
        assert elapsed < 60.0, f"Benchmark latency regression: took {elapsed:.2f}s (>60s)"

    finally:
        remove_path(temp_dir)
