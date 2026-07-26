import unittest
import tempfile
import os
import json
from pathlib import Path
from scripts._data_io import load_snapshot_data, save_snapshot_data, load_latest_state, save_latest_state

class TestDataIO(unittest.TestCase):
    """
    Test suite for data persistence layer handling snapshot histories and repository states.
    
    This test suite ensures that the decoupled data model functions flawlessly. The goal 
    is to guarantee that the massive historical footprint is correctly split between
    a lightweight timeline of snapshots (history) and a heavyweight latest representation (state). 
    This separation is essential for the serverless deployment model, as parsing a large JSON 
    object into memory just to append a small commit is computationally unfeasible.
    """

    def setUp(self):
        """
        Prepare a safe, isolated directory for test files.
        A temporary directory is used so that these tests can run cleanly on any environment 
        without mutating the actual data folders or encountering permission conflicts.
        """
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_file = Path(self.temp_dir.name) / "test_data.json"
        self.state_file = Path(self.temp_dir.name) / "test_state.json"
    
    def tearDown(self):
        """
        Clean up the temporary directory after the tests finish to prevent disk bloat.
        """
        self.temp_dir.cleanup()
        
    def test_save_and_load_snapshot_data(self):
        """
        Verify that core snapshot and fossil metrics are reliably saved and retrieved.
        
        Why this test is needed:
        The snapshot data drives the frontend graph visualization. If this structure becomes corrupted, 
        the visualization will break. This test ensures that writing the structure to disk and reading it 
        back yields the exact same structure without data loss.
        """
        snapshots = [{"snapshot_date": "2025-01", "composition": {"2025": 10}}]
        fossils = {"genesis": {"timestamp": 123}}
        
        # Simulate saving a repository's timeline.
        save_snapshot_data(self.data_file, snapshots, fossils)
        self.assertTrue(self.data_file.exists(), "The data file should be created on disk.")
        
        # Read back to guarantee fidelity.
        loaded = load_snapshot_data(self.data_file)
        self.assertEqual(loaded["snapshots"], snapshots, "Snapshots altered during save/load.")
        self.assertEqual(loaded["fossils"], fossils, "Fossils altered during save/load.")
        
    def test_save_and_load_latest_state(self):
        """
        Validate the persistence of the file composition state.
        
        Why this test is needed:
        The state file contains the granular per-file breakdown of line ownership. It is decoupled 
        from the main timeline so that the incremental Git blame process can resume exactly where it 
        left off without loading the entire historical timeline into memory. If this persistence fails, 
        the incremental caching mechanism breaks, requiring a full computational blame on each run.
        """
        file_compositions = {"fileA.py": {"2025": 10}}
        commit_hash = "abcdef1"
        
        save_latest_state(self.state_file, commit_hash, file_compositions)
        self.assertTrue(self.state_file.exists(), "The state file should be created on disk.")
        
        loaded_commit, loaded_comps = load_latest_state(self.state_file)
        self.assertEqual(loaded_commit, commit_hash, "Commit hash altered during save/load.")
        self.assertEqual(loaded_comps, file_compositions, "File compositions altered during save/load.")
        
    def test_load_latest_state_missing(self):
        """
        Ensure graceful handling when no prior state file exists.
        
        Why this test is needed:
        During the first run of a repository analysis, there is no state file to resume from. 
        Instead of throwing an exception, the system must handle this gracefully and return None, 
        signaling to the orchestrator that a full initial baseline blame is required.
        """
        loaded_commit, loaded_comps = load_latest_state(self.state_file)
        self.assertIsNone(loaded_commit, "Missing state should return None for the commit.")
        self.assertIsNone(loaded_comps, "Missing state should return None for the compositions.")

if __name__ == "__main__":
    unittest.main()
