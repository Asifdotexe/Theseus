import unittest
import tempfile
import os
import json
from pathlib import Path
from scripts._data_io import load_snapshot_data, save_snapshot_data, load_latest_state, save_latest_state

class TestDataIO(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_file = Path(self.temp_dir.name) / "test_data.json"
        self.state_file = Path(self.temp_dir.name) / "test_state.json"
    
    def tearDown(self):
        self.temp_dir.cleanup()
        
    def test_save_and_load_snapshot_data(self):
        snapshots = [{"snapshot_date": "2025-01", "composition": {"2025": 10}}]
        fossils = {"genesis": {"timestamp": 123}}
        
        save_snapshot_data(self.data_file, snapshots, fossils)
        self.assertTrue(self.data_file.exists())
        
        loaded = load_snapshot_data(self.data_file)
        self.assertEqual(loaded["snapshots"], snapshots)
        self.assertEqual(loaded["fossils"], fossils)
        
    def test_save_and_load_latest_state(self):
        file_compositions = {"fileA.py": {"2025": 10}}
        commit_hash = "abcdef1"
        
        save_latest_state(self.state_file, commit_hash, file_compositions)
        self.assertTrue(self.state_file.exists())
        
        loaded_commit, loaded_comps = load_latest_state(self.state_file)
        self.assertEqual(loaded_commit, commit_hash)
        self.assertEqual(loaded_comps, file_compositions)
        
    def test_load_latest_state_missing(self):
        loaded_commit, loaded_comps = load_latest_state(self.state_file)
        self.assertIsNone(loaded_commit)
        self.assertIsNone(loaded_comps)

if __name__ == "__main__":
    unittest.main()
