"""Tests for parquet folder handling functionality."""

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from duckdbfastapi.main import _create_endpoints, _has_parquet_files
from fastapi import FastAPI


@pytest.fixture
def parquet_folder_setup():
    """Create a temporary directory with multiple parquet files."""
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a subfolder with multiple parquet files (chunks of the same table)
        parquet_folder = tmpdir_path / "parquet_chunks"
        parquet_folder.mkdir()

        # Create multiple parquet files as chunks
        df1 = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )
        df1.to_parquet(parquet_folder / "chunk_1.parquet", index=False)

        df2 = pd.DataFrame(
            {"id": [4, 5, 6], "name": ["David", "Eve", "Frank"], "age": [28, 32, 29]}
        )
        df2.to_parquet(parquet_folder / "chunk_2.parquet", index=False)

        df3 = pd.DataFrame({"id": [7, 8], "name": ["Grace", "Henry"], "age": [26, 31]})
        df3.to_parquet(parquet_folder / "chunk_3.parquet", index=False)

        yield tmpdir_path, parquet_folder


class TestParquetFolderDetection:
    """Test parquet folder detection functionality."""

    def test_has_parquet_files_true(self, parquet_folder_setup):
        """Test detection of parquet files in folder."""
        tmpdir_path, parquet_folder = parquet_folder_setup
        assert _has_parquet_files(parquet_folder) is True

    def test_has_parquet_files_false_empty(self):
        """Test detection returns False for empty folder."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            assert _has_parquet_files(tmpdir_path) is False

    def test_has_parquet_files_false_no_parquets(self):
        """Test detection returns False for folder without parquet files."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            # Create a CSV file instead
            csv_file = tmpdir_path / "test.csv"
            csv_file.write_text("col1,col2\n1,2\n")
            assert _has_parquet_files(tmpdir_path) is False

    def test_has_parquet_files_false_for_file(self, parquet_folder_setup):
        """Test detection returns False when given a file path."""
        tmpdir_path, parquet_folder = parquet_folder_setup
        parquet_file = list(parquet_folder.glob("*.parquet"))[0]
        assert _has_parquet_files(parquet_file) is False


class TestParquetFolderEndpoints:
    """Test parquet folder endpoint creation and functionality."""

    def test_parquet_folder_data_endpoint_created(self, parquet_folder_setup):
        """Test that data endpoint is created for parquet folders."""
        tmpdir_path, parquet_folder = parquet_folder_setup

        app = FastAPI()
        items = [parquet_folder]
        _create_endpoints(app, tmpdir_path, items, include_schema_cols=False)

        client = TestClient(app)
        response = client.get("/data/parquet_chunks")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "columns" in data
        assert "count" in data
        assert "pagination" in data

    def test_parquet_folder_data_endpoint_content(self, parquet_folder_setup):
        """Test that parquet folder data endpoint returns combined data."""
        tmpdir_path, parquet_folder = parquet_folder_setup

        app = FastAPI()
        items = [parquet_folder]
        _create_endpoints(app, tmpdir_path, items, include_schema_cols=False)

        client = TestClient(app)
        response = client.get("/data/parquet_chunks")

        data = response.json()
        assert len(data["data"]) == 5  # Default page size
        assert data["columns"] == ["id", "name", "age"]
        assert data["pagination"]["total"] == 8  # 3 + 3 + 2 rows

    def test_parquet_folder_data_endpoint_pagination(self, parquet_folder_setup):
        """Test pagination on parquet folder data endpoint."""
        tmpdir_path, parquet_folder = parquet_folder_setup

        app = FastAPI()
        items = [parquet_folder]
        _create_endpoints(app, tmpdir_path, items, include_schema_cols=False)

        client = TestClient(app)

        # First page
        response1 = client.get("/data/parquet_chunks?page=1&page_size=3")
        data1 = response1.json()
        assert len(data1["data"]) == 3
        assert data1["pagination"]["has_next"] is True
        assert data1["pagination"]["page"] == 1
        assert data1["pagination"]["total"] == 8

        # Second page
        response2 = client.get("/data/parquet_chunks?page=2&page_size=3")
        data2 = response2.json()
        assert len(data2["data"]) == 3
        assert data2["pagination"]["has_next"] is True
        assert data2["pagination"]["page"] == 2

        # Last page
        response3 = client.get("/data/parquet_chunks?page=3&page_size=3")
        data3 = response3.json()
        assert len(data3["data"]) == 2
        assert data3["pagination"]["has_next"] is False
        assert data3["pagination"]["page"] == 3

    def test_parquet_folder_schema_endpoint_created(self, parquet_folder_setup):
        """Test that schema endpoint is created for parquet folders when enabled."""
        tmpdir_path, parquet_folder = parquet_folder_setup

        app = FastAPI()
        items = [parquet_folder]
        _create_endpoints(app, tmpdir_path, items, include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/parquet_chunks_columnnames")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert "count" in data
        assert "pagination" in data

    def test_parquet_folder_schema_endpoint_columns(self, parquet_folder_setup):
        """Test that schema endpoint returns correct column information."""
        tmpdir_path, parquet_folder = parquet_folder_setup

        app = FastAPI()
        items = [parquet_folder]
        _create_endpoints(app, tmpdir_path, items, include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/parquet_chunks_columnnames")

        data = response.json()
        assert len(data["columns"]) == 3

        # Check column details
        col_names = [col["column_name"] for col in data["columns"]]
        assert "id" in col_names
        assert "name" in col_names
        assert "age" in col_names

        # Check that example values are populated
        for col in data["columns"]:
            assert col["example_value"] is not None
            assert col["example_value"] != "NULL"
            assert col["data_type"] is not None

    def test_parquet_folder_schema_endpoint_not_created_when_disabled(
        self, parquet_folder_setup
    ):
        """Test that schema endpoint is not created when include_schema_cols is False."""
        tmpdir_path, parquet_folder = parquet_folder_setup

        app = FastAPI()
        items = [parquet_folder]
        _create_endpoints(app, tmpdir_path, items, include_schema_cols=False)

        client = TestClient(app)
        response = client.get("/data/parquet_chunks_columnnames")

        assert response.status_code == 404

    def test_parquet_folder_schema_endpoint_pagination(self, parquet_folder_setup):
        """Test pagination on schema endpoint for parquet folders."""
        tmpdir_path, parquet_folder = parquet_folder_setup

        app = FastAPI()
        items = [parquet_folder]
        _create_endpoints(app, tmpdir_path, items, include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/parquet_chunks_columnnames?page_size=2")

        data = response.json()
        assert data["pagination"]["total"] == 3  # 3 columns
        assert len(data["columns"]) == 2  # First page with page_size=2
        assert data["pagination"]["has_next"] is True


class TestParquetFolderErrorHandling:
    """Test error handling for parquet folder endpoints."""

    def test_parquet_folder_empty_folder_handling(self):
        """Test handling of empty parquet-less folder."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            empty_folder = tmpdir_path / "empty"
            empty_folder.mkdir()

            app = FastAPI()
            items = [empty_folder]
            _create_endpoints(app, tmpdir_path, items, include_schema_cols=True)

            client = TestClient(app)
            response = client.get("/data/empty")

            # Should return directory listing, not parquet data
            assert response.status_code == 200
            data = response.json()
            assert "directory" in data or "contents" in data

    def test_parquet_folder_mixed_files(self, parquet_folder_setup):
        """Test folder with both parquet and other files."""
        tmpdir_path, parquet_folder = parquet_folder_setup

        # Add a CSV file to the parquet folder
        csv_file = parquet_folder / "extra.csv"
        csv_file.write_text("a,b\n1,2\n")

        app = FastAPI()
        items = [parquet_folder]
        _create_endpoints(app, tmpdir_path, items, include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/parquet_chunks")

        # Should still work with mixed files (DuckDB ignores non-parquet files)
        assert response.status_code == 200
        data = response.json()
        assert "data" in data


class TestParquetFolderWithDifferentDataTypes:
    """Test parquet folders with various data types."""

    def test_parquet_folder_various_types(self):
        """Test parquet folder with various data types."""
        with TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            parquet_folder = tmpdir_path / "mixed_types"
            parquet_folder.mkdir()

            # Create parquet files with various data types
            df1 = pd.DataFrame(
                {
                    "int_col": [1, 2],
                    "str_col": ["a", "b"],
                    "float_col": [1.1, 2.2],
                    "bool_col": [True, False],
                }
            )
            df1.to_parquet(parquet_folder / "part1.parquet", index=False)

            df2 = pd.DataFrame(
                {
                    "int_col": [3, 4],
                    "str_col": ["c", "d"],
                    "float_col": [3.3, 4.4],
                    "bool_col": [False, True],
                }
            )
            df2.to_parquet(parquet_folder / "part2.parquet", index=False)

            app = FastAPI()
            items = [parquet_folder]
            _create_endpoints(app, tmpdir_path, items, include_schema_cols=True)

            client = TestClient(app)

            # Test data endpoint
            response = client.get("/data/mixed_types")
            assert response.status_code == 200
            data = response.json()
            assert data["pagination"]["total"] == 4

            # Test schema endpoint
            response = client.get("/data/mixed_types_columnnames")
            assert response.status_code == 200
            schema_data = response.json()
            assert len(schema_data["columns"]) == 4
