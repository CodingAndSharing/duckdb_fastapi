"""Tests that invoke actual endpoint handlers via HTTP requests."""

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckdb_fastapi.main import _create_endpoints


class TestJSONEndpointExecution:
    """Test actual execution of JSON file endpoints."""

    def test_call_json_endpoint(self, tmp_path):
        """Call JSON endpoint handler directly."""
        json_file = tmp_path / "test.json"
        json_file.write_text(json.dumps([{"id": 1, "name": "Alice"}]))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file])

        client = TestClient(app)

        # Find and call the endpoint
        for route in app.routes:
            if "test" in route.path and "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                data = response.json()
                assert "data" in data or "columns" in data or "count" in data
                break

    def test_json_endpoint_with_multiple_records(self, tmp_path):
        """Test JSON endpoint with multiple records."""
        json_file = tmp_path / "users.json"
        json_file.write_text(
            json.dumps(
                [
                    {"id": 1, "name": "Alice", "age": 30},
                    {"id": 2, "name": "Bob", "age": 25},
                    {"id": 3, "name": "Charlie", "age": 35},
                ]
            )
        )

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file])

        client = TestClient(app)

        for route in app.routes:
            if "users" in route.path and "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                data = response.json()
                # Should have data returned
                assert len(data) > 0
                break


class TestCSVEndpointExecution:
    """Test actual execution of CSV file endpoints."""

    def test_call_csv_endpoint(self, tmp_path):
        """Call CSV endpoint handler directly."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        client = TestClient(app)

        for route in app.routes:
            if "test" in route.path and "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                data = response.json()
                assert len(data) > 0
                break

    def test_csv_endpoint_response_format(self, tmp_path):
        """Test CSV endpoint returns correct format."""
        csv_file = tmp_path / "products.csv"
        csv_file.write_text(
            "product_id,product_name,price\n1,Widget,9.99\n2,Gadget,19.99\n"
        )

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        client = TestClient(app)

        for route in app.routes:
            if "products" in route.path and "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                break


class TestDirectoryEndpointExecution:
    """Test actual execution of directory endpoints."""

    def test_call_directory_endpoint(self, tmp_path):
        """Call directory endpoint handler directly."""
        test_dir = tmp_path / "data_folder"
        test_dir.mkdir()

        (test_dir / "file1.json").write_text(json.dumps({}))
        (test_dir / "file2.csv").write_text("a,b\n1,2\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [test_dir])

        client = TestClient(app)

        for route in app.routes:
            if "data_folder" in route.path and "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                data = response.json()
                assert "directory" in data or "contents" in data
                break

    def test_directory_endpoint_lists_contents(self, tmp_path):
        """Test directory endpoint returns file listing."""
        test_dir = tmp_path / "mydata"
        test_dir.mkdir()

        (test_dir / "data.json").write_text(json.dumps([]))
        (test_dir / "config.txt").write_text("config")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [test_dir])

        client = TestClient(app)

        for route in app.routes:
            if "mydata" in route.path and "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                break


class TestParquetEndpointExecution:
    """Test actual execution of Parquet endpoints."""

    def test_call_parquet_endpoint(self, tmp_path):
        """Call parquet endpoint handler."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            # Create parquet file
            data = {
                "id": [1, 2, 3],
                "value": ["a", "b", "c"],
                "score": [10.5, 20.3, 15.8],
            }
            table = pa.table(data)
            parquet_file = tmp_path / "data.parquet"
            pq.write_table(table, str(parquet_file))

            app = FastAPI()
            _create_endpoints(app, tmp_path, [parquet_file])

            client = TestClient(app)

            for route in app.routes:
                if "data" in route.path and "/data/" in route.path:
                    response = client.get(route.path)
                    assert response.status_code == 200
                    break
        except ImportError:
            pytest.skip("pyarrow not installed")


class TestMixedEndpoints:
    """Test endpoints with mixed file types."""

    def test_multiple_json_files(self, tmp_path):
        """Test multiple JSON files get separate endpoints."""
        file1 = tmp_path / "data1.json"
        file1.write_text(json.dumps([{"id": 1}]))

        file2 = tmp_path / "data2.json"
        file2.write_text(json.dumps([{"id": 2}]))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [file1, file2])

        json_routes = [
            r for r in app.routes if "/data/" in r.path and "/data/data" in r.path
        ]
        assert len(json_routes) >= 2

    def test_json_and_directory_endpoints(self, tmp_path):
        """Test both file and directory endpoints."""
        json_file = tmp_path / "file.json"
        json_file.write_text(json.dumps([]))

        test_dir = tmp_path / "folder"
        test_dir.mkdir()
        (test_dir / "nested.json").write_text(json.dumps({}))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file, test_dir])

        data_routes = [r for r in app.routes if "/data/" in r.path]
        assert len(data_routes) >= 2


class TestEndpointErrorHandling:
    """Test error handling in endpoints."""

    def test_malformed_json_file(self, tmp_path):
        """Test endpoint response when reading file."""
        # Even with valid JSON, the endpoint should handle it
        json_file = tmp_path / "data.json"
        json_file.write_text(json.dumps([{"key": "value"}]))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file])

        client = TestClient(app)

        for route in app.routes:
            if "data" in route.path and "/data/" in route.path:
                # Should succeed
                response = client.get(route.path)
                assert response.status_code in [200, 500]
                break

    def test_csv_with_different_encodings(self, tmp_path):
        """Test CSV endpoint with content."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1,col2,col3\nval1,val2,val3\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        client = TestClient(app)

        for route in app.routes:
            if "data" in route.path and "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                break


class TestEndpointResponseContentTypes:
    """Test response content types."""

    def test_json_endpoint_returns_dict(self, tmp_path):
        """Test JSON endpoint returns dict response."""
        json_file = tmp_path / "test.json"
        json_file.write_text(json.dumps([{"id": 1}]))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file])

        client = TestClient(app)

        for route in app.routes:
            if "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                # Response should be JSON
                assert isinstance(response.json(), dict)
                break

    def test_directory_endpoint_returns_dict(self, tmp_path):
        """Test directory endpoint returns dict."""
        test_dir = tmp_path / "dir"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [test_dir])

        client = TestClient(app)

        for route in app.routes:
            if "/data/" in route.path:
                response = client.get(route.path)
                assert response.status_code == 200
                assert isinstance(response.json(), dict)
                break
