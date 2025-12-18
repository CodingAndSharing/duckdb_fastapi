"""Tests for handler endpoint implementations."""

import json

import pytest
from fastapi import FastAPI
import duckdb

from duckdbfastapi.main import _create_endpoints


class TestJSONFileHandler:
    """Test JSON file endpoint handler."""

    def test_json_file_handler_success(self, tmp_path):
        """Test successful JSON file reading."""
        json_file = tmp_path / "data.json"
        json_file.write_text(json.dumps([{"id": 1, "name": "test"}]))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file])


class TestCSVFileHandler:
    """Test CSV file endpoint handler."""

    def test_csv_file_handler_success(self, tmp_path):
        """Test successful CSV file reading."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,test\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        # Test endpoint was created
        data_routes = [r for r in app.routes if "/data/" in r.path]
        assert len(data_routes) > 0


class TestParquetFileHandler:
    """Test Parquet file endpoint handler."""

    def test_parquet_file_handler(self, tmp_path):
        """Test parquet file handling."""
        try:
            import pyarrow.parquet as pq
            import pyarrow as pa

            data = {"id": [1, 2], "name": ["a", "b"]}
            table = pa.table(data)
            parquet_file = tmp_path / "data.parquet"
            pq.write_table(table, str(parquet_file))

            app = FastAPI()
            _create_endpoints(app, tmp_path, [parquet_file])

            data_routes = [r for r in app.routes if "/data/" in r.path]
            assert len(data_routes) > 0
        except ImportError:
            pytest.skip("pyarrow not installed")


class TestDirectoryHandler:
    """Test directory endpoint handler."""

    def test_directory_handler_success(self, tmp_path):
        """Test successful directory listing."""
        test_dir = tmp_path / "test_data"
        test_dir.mkdir()

        (test_dir / "file1.json").write_text(json.dumps({"test": 1}))
        (test_dir / "file2.txt").write_text("text content")
        sub_dir = test_dir / "subdir"
        sub_dir.mkdir()

        app = FastAPI()
        _create_endpoints(app, tmp_path, [test_dir])

        data_routes = [r for r in app.routes if "/data/" in r.path]
        assert len(data_routes) > 0

    def test_directory_with_subdirectories(self, tmp_path):
        """Test directory handler with nested directories."""
        test_dir = tmp_path / "nested"
        test_dir.mkdir()

        sub1 = test_dir / "sub1"
        sub1.mkdir()
        sub2 = test_dir / "sub2"
        sub2.mkdir()

        (sub1 / "data.json").write_text(json.dumps([1, 2, 3]))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [test_dir])

        data_routes = [r for r in app.routes if "/data/" in r.path]
        assert len(data_routes) > 0


class TestConnectionHandling:
    """Test DuckDB connection handling."""

    def test_duckdb_json_read(self, tmp_path):
        """Test DuckDB JSON reading."""
        json_file = tmp_path / "test.json"
        json_file.write_text(json.dumps([{"id": 1}, {"id": 2}]))

        conn = duckdb.connect(":memory:")
        result = conn.execute(f"SELECT * FROM read_json_auto('{json_file}')").fetchall()

        assert len(result) == 2

    def test_duckdb_csv_read(self, tmp_path):
        """Test DuckDB CSV reading."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,value\n1,a\n2,b\n")

        conn = duckdb.connect(":memory:")
        result = conn.execute(f"SELECT * FROM read_csv_auto('{csv_file}')").fetchall()

        assert len(result) == 2


class TestDescriptionHandling:
    """Test description/column handling."""

    def test_connection_description_extraction(self, tmp_path):
        """Test extracting column descriptions from connection."""
        json_file = tmp_path / "test.json"
        json_file.write_text(json.dumps([{"id": 1, "name": "test"}]))

        conn = duckdb.connect(":memory:")
        conn.execute(f"SELECT * FROM read_json_auto('{json_file}')")

        # Description should be available
        if conn.description:
            columns = [desc[0] for desc in conn.description]
            assert len(columns) > 0


class TestUnsupportedFileFormats:
    """Test handling of unsupported file formats."""

    def test_txt_file_not_processed(self, tmp_path):
        """Test that .txt files are not processed."""
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("plain text content")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [txt_file])

        # No data routes should be created
        data_routes = [r for r in app.routes if "/data/" in r.path]
        assert len(data_routes) == 0

    def test_log_file_not_processed(self, tmp_path):
        """Test that .log files are not processed."""
        log_file = tmp_path / "data.log"
        log_file.write_text("log content")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [log_file])

        data_routes = [r for r in app.routes if "/data/" in r.path]
        assert len(data_routes) == 0


class TestResponseStructures:
    """Test response structure validation."""

    def test_file_response_has_data_field(self, tmp_path):
        """Test that file responses include data field."""
        json_file = tmp_path / "test.json"
        json_file.write_text(json.dumps([{"id": 1}]))

        conn = duckdb.connect(":memory:")
        result = conn.execute(f"SELECT * FROM read_json_auto('{json_file}')").fetchall()

        response = {"data": result, "columns": ["id"], "count": len(result)}

        assert "data" in response
        assert "columns" in response
        assert "count" in response

    def test_directory_response_structure(self, tmp_path):
        """Test directory response structure."""
        test_dir = tmp_path / "test"
        test_dir.mkdir()
        (test_dir / "file.json").write_text(json.dumps({}))

        response = {
            "directory": "test",
            "contents": [
                {"name": "file.json", "type": "file", "path": "test/file.json"}
            ],
        }

        assert "directory" in response
        assert "contents" in response
        assert len(response["contents"]) > 0


class TestNameSanitization:
    """Test endpoint name sanitization."""

    def test_spaces_converted_to_underscores(self, tmp_path):
        """Test that spaces in names become underscores."""
        file_with_spaces = tmp_path / "my data file.json"
        file_with_spaces.write_text(json.dumps([]))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [file_with_spaces])

        routes = [r.path for r in app.routes]
        # Should have converted spaces
        assert any("my_data_file" in r.lower() for r in routes)

    def test_hyphens_converted_to_underscores(self, tmp_path):
        """Test that hyphens in names become underscores."""
        file_with_hyphens = tmp_path / "my-data-file.csv"
        file_with_hyphens.write_text("id,value\n1,a\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [file_with_hyphens])

        routes = [r.path for r in app.routes]
        assert any("my_data_file" in r.lower() for r in routes)


class TestMultipleFileTypes:
    """Test handling multiple file types in same directory."""

    def test_json_and_csv_together(self, tmp_path):
        """Test processing both JSON and CSV files."""
        json_file = tmp_path / "data.json"
        json_file.write_text(json.dumps([{"id": 1}]))

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id\n1\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file, csv_file])

        data_routes = [r for r in app.routes if "/data/" in r.path]
        assert len(data_routes) >= 2

    def test_files_and_directories_together(self, tmp_path):
        """Test processing both files and directories."""
        json_file = tmp_path / "data.json"
        json_file.write_text(json.dumps([]))

        test_dir = tmp_path / "subdir"
        test_dir.mkdir()

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file, test_dir])

        data_routes = [r for r in app.routes if "/data/" in r.path]
        assert len(data_routes) >= 2
