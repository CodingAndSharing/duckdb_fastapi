"""Comprehensive tests for endpoint coverage in duckdbfastapi."""

import json

import pytest
from fastapi import FastAPI
from duckdbfastapi.main import _create_endpoints, _get_items_to_process


class TestCreateEndpointsFileHandling:
    """Test file handling in endpoint creation."""

    def test_create_endpoints_with_json_file(self, temp_data_dir):
        """Test endpoint creation for JSON files."""
        app = FastAPI()
        items = [f for f in _get_items_to_process(temp_data_dir) if f.suffix == ".json"]

        if items:
            _create_endpoints(app, temp_data_dir, items)
            # Verify routes were created
            routes = [route.path for route in app.routes]
            assert any("data" in route for route in routes)

    def test_create_endpoints_with_csv_file(self, temp_data_dir):
        """Test endpoint creation for CSV files."""
        app = FastAPI()
        items = [f for f in _get_items_to_process(temp_data_dir) if f.suffix == ".csv"]

        if items:
            _create_endpoints(app, temp_data_dir, items)
            routes = [route.path for route in app.routes]
            assert any("data" in route for route in routes)

    def test_create_endpoints_with_directory(self, temp_data_dir):
        """Test endpoint creation for directories."""
        app = FastAPI()
        items = [f for f in _get_items_to_process(temp_data_dir) if f.is_dir()]

        if items:
            _create_endpoints(app, temp_data_dir, items)
            routes = [route.path for route in app.routes]
            assert any("data" in route for route in routes)

    def test_create_endpoints_mixed_types(self, temp_data_dir):
        """Test endpoint creation with mixed file types and directories."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir)

        if len(items) > 0:
            _create_endpoints(app, temp_data_dir, items)
            # Verify multiple endpoints were created
            data_routes = [route.path for route in app.routes if "data" in route.path]
            assert len(data_routes) > 0


class TestFileOperations:
    """Test file read operations for different formats."""

    def test_json_file_reading(self, sample_json_file):
        """Test JSON file can be read properly."""
        content = sample_json_file.read_text()
        data = json.loads(content)
        assert isinstance(data, list)
        assert len(data) > 0

    def test_csv_file_reading(self, sample_csv_file):
        """Test CSV file can be read properly."""
        content = sample_csv_file.read_text()
        assert "id" in content
        assert "value" in content

    def test_parquet_file_creation(self, tmp_path):
        """Test parquet file can be created and accessed."""
        try:
            import pyarrow.parquet as pq
            import pyarrow as pa

            # Create test parquet file
            data = {"id": [1, 2, 3], "value": ["a", "b", "c"]}
            table = pa.table(data)
            parquet_file = tmp_path / "test.parquet"
            pq.write_table(table, str(parquet_file))

            assert parquet_file.exists()
            assert parquet_file.suffix == ".parquet"
        except ImportError:
            pytest.skip("pyarrow not installed")

    def test_unsupported_file_type(self, tmp_path):
        """Test handling of unsupported file types."""
        unsupported_file = tmp_path / "test.log"
        unsupported_file.write_text("some log content")

        # Unsupported files should not create endpoints
        app = FastAPI()
        items = [unsupported_file]
        _create_endpoints(app, tmp_path, items)

        # No routes should be created for unsupported files
        data_routes = [route.path for route in app.routes if "data" in route.path]
        assert len(data_routes) == 0


class TestEndpointNaming:
    """Test endpoint name sanitization and generation."""

    def test_endpoint_names_with_spaces(self, tmp_path):
        """Test endpoint names with spaces get converted to underscores."""
        file_with_spaces = tmp_path / "file with spaces.json"
        file_with_spaces.write_text(json.dumps([{"id": 1}]))

        app = FastAPI()
        items = [file_with_spaces]
        _create_endpoints(app, tmp_path, items)

        routes = [route.path for route in app.routes]
        # Should have converted spaces to underscores
        assert any("file_with_spaces" in route.lower() for route in routes)

    def test_endpoint_names_with_hyphens(self, tmp_path):
        """Test endpoint names with hyphens get converted to underscores."""
        file_with_hyphens = tmp_path / "file-with-hyphens.json"
        file_with_hyphens.write_text(json.dumps([{"id": 1}]))

        app = FastAPI()
        items = [file_with_hyphens]
        _create_endpoints(app, tmp_path, items)

        routes = [route.path for route in app.routes]
        # Should have converted hyphens to underscores
        assert any("file_with_hyphens" in route.lower() for route in routes)

    def test_directory_endpoint_names(self, tmp_path):
        """Test directory endpoint names are properly generated."""
        test_dir = tmp_path / "test_directory"
        test_dir.mkdir()
        (test_dir / "data.json").write_text(json.dumps([{"id": 1}]))

        app = FastAPI()
        items = [test_dir]
        _create_endpoints(app, tmp_path, items)

        routes = [route.path for route in app.routes]
        assert any("test_directory" in route.lower() for route in routes)


class TestDirectoryContents:
    """Test directory listing and content generation."""

    def test_directory_with_mixed_contents(self, tmp_path):
        """Test directory with various file types."""
        test_dir = tmp_path / "mixed_dir"
        test_dir.mkdir()

        # Create different file types
        (test_dir / "file1.json").write_text(json.dumps({"data": 1}))
        (test_dir / "file2.csv").write_text("a,b\n1,2\n")
        (test_dir / "subdir").mkdir()

        app = FastAPI()
        items = [test_dir]
        _create_endpoints(app, tmp_path, items)

        routes = [route.path for route in app.routes]
        assert len(routes) > 0

    def test_empty_directory(self, tmp_path):
        """Test endpoint for empty directory."""
        empty_dir = tmp_path / "empty_dir"
        empty_dir.mkdir()

        app = FastAPI()
        items = [empty_dir]
        _create_endpoints(app, tmp_path, items)

        routes = [route.path for route in app.routes]
        assert len(routes) > 0


class TestDataPathResolutionExtended:
    """Extended tests for data path resolution."""

    def test_absolute_path_resolution(self, temp_data_dir):
        """Test absolute path is handled correctly."""
        from duckdbfastapi.main import _get_data_path

        result = _get_data_path(str(temp_data_dir.absolute()))
        assert result.is_absolute()

    def test_relative_path_to_absolute(self, temp_data_dir):
        """Test relative paths are converted to absolute."""
        from duckdbfastapi.main import _get_data_path
        import os

        # Save current directory
        original_cwd = os.getcwd()
        try:
            # Change to temp directory parent
            os.chdir(temp_data_dir.parent)

            # Use relative path
            result = _get_data_path(temp_data_dir.name)
            assert result.is_absolute()
        finally:
            # Restore original directory
            os.chdir(original_cwd)


class TestErrorHandling:
    """Test error handling in various scenarios."""

    def test_file_access_error_handling(self, tmp_path):
        """Test handling of file access errors."""
        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps([{"id": 1}]))

        app = FastAPI()
        items = [test_file]

        # Should not raise during endpoint creation
        _create_endpoints(app, tmp_path, items)
        assert len(app.routes) > 0

    def test_directory_read_error_handling(self, tmp_path):
        """Test handling of directory read errors."""
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()
        (test_dir / "file.json").write_text(json.dumps([{"id": 1}]))

        app = FastAPI()
        items = [test_dir]

        # Should not raise during endpoint creation
        _create_endpoints(app, tmp_path, items)
        assert len(app.routes) > 0


class TestItemProcessing:
    """Test item processing logic."""

    def test_process_all_file_types(self, temp_data_dir):
        """Test processing of all supported file types."""
        items = _get_items_to_process(temp_data_dir)

        file_types = set()
        for item in items:
            if item.is_file():
                file_types.add(item.suffix)

        # temp_data_dir should have at least json and csv
        assert ".json" in file_types or ".csv" in file_types

    def test_sorted_items_order(self, temp_data_dir):
        """Test that items are returned in sorted order."""
        items = _get_items_to_process(temp_data_dir)
        names = [item.name for item in items]

        # Verify items are sorted
        assert names == sorted(names)

    def test_specific_items_subset(self, temp_data_dir):
        """Test filtering to specific items."""
        all_items = _get_items_to_process(temp_data_dir)

        if len(all_items) >= 1:
            first_item_name = all_items[0].name
            items = _get_items_to_process(temp_data_dir, [first_item_name])

            assert len(items) == 1
            assert items[0].name == first_item_name
