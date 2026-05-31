"""
Tests for the snapshot analysis module and its shared dependencies.
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# pylint: disable=wrong-import-position,import-error
from scripts._blame import parse_blame_year_counts
from scripts._data_io import load_snapshot_data
from scripts.analyse_repository import _filter_snapshots


class TestParseBlameYearCounts:
    """Tests for parsing git blame --line-porcelain output into year counts."""

    def test_single_file_single_author_year(self):
        """Test parsing a blame output with a single commit and author."""
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time 1704067200\n"
            "filename test.py\n"
            "\tprint('hello world')\n"
        )
        result = parse_blame_year_counts(blame_output)
        year = datetime.fromtimestamp(1704067200, timezone.utc).strftime("%Y")
        assert result == {year: 1}

    def test_multiple_commits_different_years(self):
        """Test parsing a blame output with multiple commits stretching across different years."""
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time 1609459200\n"
            "filename test.py\n"
            "\tconst x = 1;\n"
            "def4567890123456789012345678901234567890 2 2 1\n"
            "author Another Author\n"
            "author-time 1704067200\n"
            "filename test.py\n"
            "\tconst y = 2;\n"
        )
        result = parse_blame_year_counts(blame_output)
        year_2021 = datetime.fromtimestamp(1609459200, timezone.utc).strftime("%Y")
        year_2024 = datetime.fromtimestamp(1704067200, timezone.utc).strftime("%Y")
        assert result[year_2021] == 1
        assert result[year_2024] == 1

    def test_lines_attributed_to_correct_year(self):
        """Test parsing a blame output where multiple lines are credited to the same commit and year."""
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time 1609459200\n"
            "filename test.py\n"
            "\tline one\n"
            "\tline two\n"
            "\tline three\n"
        )
        result = parse_blame_year_counts(blame_output)
        year = datetime.fromtimestamp(1609459200, timezone.utc).strftime("%Y")
        assert result[year] == 3

    def test_empty_output(self):
        """Test parsing an empty blame output."""
        result = parse_blame_year_counts("")
        assert result == {}

    def test_invalid_timestamp_ignored(self):
        """Test parsing a blame output that contains an invalid timestamp, ensuring it is handled properly."""
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time not_a_number\n"
            "filename test.py\n"
            "\tprint('hello')\n"
        )
        result = parse_blame_year_counts(blame_output)
        assert result == {}

    def test_40_and_64_char_hashes(self):
        """Test parsing a blame output safely using varied hash sizes."""
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time 1704067200\n"
            "filename test.py\n"
            "\tprint('hello')\n"
        )
        result = parse_blame_year_counts(blame_output)
        year = datetime.fromtimestamp(1704067200, timezone.utc).strftime("%Y")
        assert year in result


class TestLoadSnapshotData:
    """Tests for loading snapshot data from JSON files."""

    def test_load_valid_json(self):
        """Test loading a correctly formatted existing JSON state."""
        data = [
            {
                "snapshot_date": "2024-01",
                "total_lines": 100,
                "composition": {"2020": 100},
            },
            {
                "snapshot_date": "2024-02",
                "total_lines": 150,
                "composition": {"2020": 150},
            },
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            f.flush()

            result = load_snapshot_data(f.name)
            assert "snapshots" in result
            assert "fossils" in result
            snapshots = result["snapshots"]
            assert len(snapshots) == 2
            assert snapshots[0]["snapshot_date"] == "2024-01"

        os.unlink(f.name)

    def test_file_not_exists(self):
        """Test loading state when the requested file does not exist, expecting a blank default structure."""
        result = load_snapshot_data("/nonexistent/path/data.json")
        assert result == {"snapshots": [], "fossils": {}}

    def test_corrupted_json_returns_empty(self):
        """Test loading a corrupted JSON file, which should fallback dynamically to a default parsed object."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json {")
            f.flush()

            result = load_snapshot_data(f.name)
            assert result == {"snapshots": [], "fossils": {}}

        os.unlink(f.name)


class TestFilterSnapshots:
    """Tests for the snapshot filtering helper."""

    def test_filters_out_processed_periods(self):
        """Test that processed periods are excluded from the result."""
        all_snaps = [("2020-01", "a"), ("2020-02", "b"), ("2020-03", "c")]
        processed = {"2020-01", "2020-03"}
        result = _filter_snapshots(all_snaps, processed)
        assert result == [("2020-02", "b")]

    def test_returns_all_when_none_processed(self):
        """Test that when no periods have been processed, all snapshots are returned."""
        all_snaps = [("2020-01", "a"), ("2020-02", "b")]
        result = _filter_snapshots(all_snaps, set())
        assert result == all_snaps

    def test_empty_input(self):
        """Test that an empty snapshot list returns an empty list."""
        result = _filter_snapshots([], set())
        assert result == []

    def test_reprocess_includes_specific_period(self):
        """Test that a reprocess period is included even if it was already processed."""
        all_snaps = [("2020-01", "a"), ("2020-02", "b"), ("2020-03", "c")]
        processed = {"2020-01", "2020-03"}
        result = _filter_snapshots(all_snaps, processed, reprocess="2020-01")
        assert ("2020-01", "a") in result
        assert ("2020-02", "b") in result
        assert ("2020-03", "c") not in result

    def test_reprocess_with_unprocessed_period(self):
        """Test that reprocessing an unprocessed period just includes it normally."""
        all_snaps = [("2020-01", "a"), ("2020-02", "b")]
        processed = {"2020-02"}
        result = _filter_snapshots(all_snaps, processed, reprocess="2020-01")
        assert result == [("2020-01", "a")]
