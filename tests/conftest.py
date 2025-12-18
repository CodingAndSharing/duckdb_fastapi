"""Test fixtures for duckdb_fastapi tests."""

import json
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_data_dir():
    """Create a temporary directory with test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create test JSON file
        json_file = tmpdir_path / "test_data.json"
        json_file.write_text(
            json.dumps(
                [
                    {"id": 1, "name": "Alice", "age": 30},
                    {"id": 2, "name": "Bob", "age": 25},
                ]
            )
        )

        # Create test CSV file
        csv_file = tmpdir_path / "test_data.csv"
        csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")

        # Create test subdirectory
        subdir = tmpdir_path / "subdir"
        subdir.mkdir()
        (subdir / "nested.json").write_text(json.dumps({"nested": True}))

        yield tmpdir_path


@pytest.fixture
def sample_json_file(tmp_path):
    """Create a sample JSON file."""
    json_file = tmp_path / "sample.json"
    json_file.write_text(json.dumps([{"id": 1, "value": "test"}]))
    return json_file


@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file."""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("id,value\n1,test\n")
    return csv_file
