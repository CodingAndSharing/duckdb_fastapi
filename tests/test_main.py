"""Tests for the main duckdb_fastapi module."""

import json
from pathlib import Path

import pytest

from duckdb_fastapi.main import (
    _create_endpoints,
    _get_data_path,
    _get_items_to_process,
)


class TestGetDataPath:
    """Test the _get_data_path function."""

    def test_get_data_path_with_valid_path(self, temp_data_dir):
        """Test with a valid path."""
        result = _get_data_path(str(temp_data_dir))
        assert result == temp_data_dir
        assert result.is_dir()

    def test_get_data_path_with_invalid_path(self):
        """Test with an invalid path."""
        with pytest.raises(ValueError, match="Data path does not exist"):
            _get_data_path("/nonexistent/path")

    def test_get_data_path_with_file_path(self, sample_json_file):
        """Test with a file path instead of directory."""
        with pytest.raises(ValueError, match="Data path must be a directory"):
            _get_data_path(str(sample_json_file))

    def test_get_data_path_with_datasample_keyword(self):
        """Test with duckdb_fastapi_datasample keyword."""
        result = _get_data_path("duckdb_fastapi_datasample")
        assert result.is_dir()
        assert result.name == "datasample"


class TestGetItemsToProcess:
    """Test the _get_items_to_process function."""

    def test_get_all_items(self, temp_data_dir):
        """Test getting all items when specific_items is None."""
        items = _get_items_to_process(temp_data_dir)
        assert len(items) > 0
        names = [item.name for item in items]
        assert "test_data.json" in names
        assert "test_data.csv" in names
        assert "subdir" in names

    def test_get_specific_items(self, temp_data_dir):
        """Test getting specific items."""
        specific = ["test_data.json", "subdir"]
        items = _get_items_to_process(temp_data_dir, specific)
        assert len(items) == 2
        names = [item.name for item in items]
        assert "test_data.json" in names
        assert "subdir" in names
        assert "test_data.csv" not in names

    def test_get_nonexistent_specific_items(self, temp_data_dir):
        """Test with nonexistent specific items."""
        specific = ["nonexistent.json"]
        items = _get_items_to_process(temp_data_dir, specific)
        assert len(items) == 0

    def test_get_partial_specific_items(self, temp_data_dir):
        """Test with partial existing specific items."""
        specific = ["test_data.json", "nonexistent.txt"]
        items = _get_items_to_process(temp_data_dir, specific)
        assert len(items) == 1
        assert items[0].name == "test_data.json"


class TestCreateEndpoints:
    """Test the _create_endpoints function."""

    def test_create_endpoints_structure(self, temp_data_dir):
        """Test that endpoints are created for items."""
        from fastapi import FastAPI

        app = FastAPI()
        items = _get_items_to_process(temp_data_dir)
        _create_endpoints(app, temp_data_dir, items)

        # Check that routes were added
        routes = [route.path for route in app.routes]
        assert len(routes) > 0


class TestDataPathResolution:
    """Test data path resolution with different inputs."""

    def test_relative_path_resolution(self, temp_data_dir):
        """Test resolution of relative paths."""
        result = _get_data_path(str(temp_data_dir))
        assert result.is_absolute()


class TestItemFiltering:
    """Test item filtering functionality."""

    def test_filter_by_extension(self, temp_data_dir):
        """Test filtering items by file extension."""
        all_items = _get_items_to_process(temp_data_dir)
        json_items = [item for item in all_items if item.suffix == ".json"]
        assert len(json_items) > 0

    def test_filter_directories(self, temp_data_dir):
        """Test filtering only directories."""
        all_items = _get_items_to_process(temp_data_dir)
        dirs = [item for item in all_items if item.is_dir()]
        assert len(dirs) > 0
        assert all(item.is_dir() for item in dirs)
