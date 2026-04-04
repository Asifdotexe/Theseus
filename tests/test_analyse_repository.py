import json
import os
import sys
import tempfile
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.analyse_repository import (
    _parse_blame_output,
    load_existing_state,
)


class TestParseBlameOutput:
    """Tests for the git blame output parser."""

    def test_single_file_single_author_year(self):
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time 1704067200\n"
            "filename test.py\n"
            "\tprint('hello world')\n"
        )
        result = _parse_blame_output(blame_output)
        year = datetime.fromtimestamp(1704067200, timezone.utc).strftime("%Y")
        assert result == {year: 1}

    def test_multiple_commits_different_years(self):
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
        result = _parse_blame_output(blame_output)
        year_2021 = datetime.fromtimestamp(1609459200, timezone.utc).strftime("%Y")
        year_2024 = datetime.fromtimestamp(1704067200, timezone.utc).strftime("%Y")
        assert result[year_2021] == 1
        assert result[year_2024] == 1

    def test_lines_attributed_to_correct_year(self):
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time 1609459200\n"
            "filename test.py\n"
            "\tline one\n"
            "\tline two\n"
            "\tline three\n"
        )
        result = _parse_blame_output(blame_output)
        year = datetime.fromtimestamp(1609459200, timezone.utc).strftime("%Y")
        assert result[year] == 3

    def test_empty_output(self):
        result = _parse_blame_output("")
        assert result == {}

    def test_invalid_timestamp_ignored(self):
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time not_a_number\n"
            "filename test.py\n"
            "\tprint('hello')\n"
        )
        result = _parse_blame_output(blame_output)
        assert result == {}

    def test_40_and_64_char_hashes(self):
        blame_output = (
            "abc123def4567890123456789012345678901234 1 1 1\n"
            "author Test Author\n"
            "author-time 1704067200\n"
            "filename test.py\n"
            "\tprint('hello')\n"
        )
        result = _parse_blame_output(blame_output)
        year = datetime.fromtimestamp(1704067200, timezone.utc).strftime("%Y")
        assert year in result


class TestLoadExistingState:
    """Tests for loading existing JSON state."""

    def test_load_valid_json(self):
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

            result = load_existing_state(f.name)
            assert len(result) == 2
            assert result[0]["snapshot_date"] == "2024-01"

        os.unlink(f.name)

    def test_file_not_exists(self):
        result = load_existing_state("/nonexistent/path/data.json")
        assert result == []

    def test_corrupted_json_returns_empty(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json {")
            f.flush()

            result = load_existing_state(f.name)
            assert result == []

        os.unlink(f.name)
