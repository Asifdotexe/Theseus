"""
Tests for the unified pipeline runner (scripts/run_pipeline.py).
"""

# pylint: disable=wrong-import-position,unused-argument
import json
import os
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.run_pipeline import run_pipeline


def test_run_pipeline_no_repositories():
    """Verify run_pipeline returns True (error) when config has no repositories."""
    with patch("scripts.run_pipeline.load_config", return_value={"repositories": []}):
        failed = run_pipeline()
        assert failed is True


def test_run_pipeline_unknown_repository():
    """Verify run_pipeline returns True (error) when an unknown repo name is provided."""
    config = {
        "repositories": [{"name": "known-repo", "repo": "owner/known-repo"}]
    }
    with patch("scripts.run_pipeline.load_config", return_value=config):
        failed = run_pipeline(repo="nonexistent-repo")
        assert failed is True


def test_run_pipeline_success_flow():
    """Verify run_pipeline completes successfully and verifies engine fossils without calling add_fossils."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config = {
            "dataDir": temp_dir,
            "repositories": [{"name": "test-repo", "repo": "owner/test-repo"}],
        }

        # Mock process_repository to simulate engine output
        def fake_process_repository(_slug, data_dir, _reprocess=None):
            fossils_data = {
                "genesis": {
                    "commit": "abc1234",
                    "file": "README.md",
                    "content": "# Test Repo",
                    "year": "2020",
                    "line": 1,
                },
                "survivor": {
                    "commit": "def5678",
                    "file": "main.py",
                    "content": "print('hello')",
                    "year": "2022",
                    "line": 1,
                },
            }
            raw_dir = os.path.join(data_dir, "raw")
            os.makedirs(raw_dir, exist_ok=True)
            with open(os.path.join(raw_dir, "test-repo_fossils.json"), "w", encoding="utf-8") as f:
                json.dump(fossils_data, f)

        with patch("scripts.run_pipeline.load_config", return_value=config), \
             patch("scripts.run_pipeline.process_repository", side_effect=fake_process_repository), \
             patch("scripts.run_pipeline.run_cleanup", return_value=[]):

            failed = run_pipeline(repo="test-repo")
            assert failed is False


def test_run_pipeline_missing_fossils_fails():
    """Verify run_pipeline marks failure when engine output is missing valid fossils."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config = {
            "dataDir": temp_dir,
            "repositories": [{"name": "test-repo", "repo": "owner/test-repo"}],
        }

        # Simulate engine running without producing a valid fossils file
        with patch("scripts.run_pipeline.load_config", return_value=config), \
             patch("scripts.run_pipeline.process_repository", return_value=None), \
             patch("scripts.run_pipeline.run_cleanup", return_value=[]):

            failed = run_pipeline(repo="test-repo")
            assert failed is True
