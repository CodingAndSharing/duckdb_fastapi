"""Tests for schema/column information endpoints."""

import json
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckdbfastapi.main import _create_endpoints, _get_items_to_process


class TestSchemaEndpoints:
    """Test schema endpoints for different file types."""

    def test_csv_schema_endpoint(self, temp_data_dir):
        """Test schema endpoint for CSV files."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.csv"])
        _create_endpoints(app, temp_data_dir, items, include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/test_data.csv_columnnames")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert "pagination" in data
        assert len(data["columns"]) > 0
        assert "column_name" in data["columns"][0]
        assert "data_type" in data["columns"][0]
        assert "example_value" in data["columns"][0]

    def test_json_schema_endpoint(self, temp_data_dir):
        """Test schema endpoint for JSON files."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.json"])
        _create_endpoints(app, temp_data_dir, items, include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/test_data.json_columnnames")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert "pagination" in data
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["page_size"] == 5

    def test_schema_endpoint_pagination(self, temp_data_dir):
        """Test pagination on schema endpoint."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.csv"])
        _create_endpoints(app, temp_data_dir, items, include_schema_cols=True)

        client = TestClient(app)

        # First page
        response1 = client.get("/data/test_data.csv_columnnames?page=1&page_size=1")
        assert response1.status_code == 200
        data1 = response1.json()
        assert len(data1["columns"]) == 1

        # Second page
        response2 = client.get("/data/test_data.csv_columnnames?page=2&page_size=1")
        assert response2.status_code == 200
        data2 = response2.json()
        assert len(data2["columns"]) == 1

        # Verify different columns
        assert data1["columns"][0]["column_name"] != data2["columns"][0]["column_name"]

    def test_schema_endpoint_with_large_page_size(self, temp_data_dir):
        """Test schema endpoint with large page size."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.csv"])
        _create_endpoints(app, temp_data_dir, items, include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/test_data.csv_columnnames?page=1&page_size=1000")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert data["pagination"]["page_size"] <= 1000

    def test_schema_endpoint_example_values(self, temp_data_dir):
        """Test that schema endpoint provides example values."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.csv"])
        _create_endpoints(app, temp_data_dir, items, include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/test_data.csv_columnnames")

        assert response.status_code == 200
        data = response.json()

        for col in data["columns"]:
            assert col["example_value"] is not None
            assert isinstance(col["example_value"], str)


class TestSchemaEndpointDisabled:
    """Test endpoints when schema is disabled."""

    def test_no_schema_endpoint_when_disabled(self, temp_data_dir):
        """Test that schema endpoint is not created when disabled."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.csv"])
        _create_endpoints(app, temp_data_dir, items, include_schema_cols=False)

        client = TestClient(app)
        response = client.get("/data/test_data.csv_columnnames")

        # Should return 404 since endpoint doesn't exist
        assert response.status_code == 404

    def test_data_endpoint_exists_when_schema_disabled(self, temp_data_dir):
        """Test that data endpoint still exists when schema is disabled."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.csv"])
        _create_endpoints(app, temp_data_dir, items, include_schema_cols=False)

        client = TestClient(app)
        response = client.get("/data/test_data.csv")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data


class TestDirectoryEndpoint:
    """Test directory listing endpoints."""

    def test_directory_endpoint_returns_contents(self, temp_data_dir):
        """Test that directory endpoint returns file listing."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["subdir"])
        _create_endpoints(app, temp_data_dir, items)

        client = TestClient(app)
        response = client.get("/data/subdir")

        assert response.status_code == 200
        data = response.json()
        assert "directory" in data
        assert "contents" in data
        assert len(data["contents"]) > 0

    def test_directory_endpoint_file_info(self, temp_data_dir):
        """Test that directory endpoint provides file information."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["subdir"])
        _create_endpoints(app, temp_data_dir, items)

        client = TestClient(app)
        response = client.get("/data/subdir")

        assert response.status_code == 200
        data = response.json()

        for item in data["contents"]:
            assert "name" in item
            assert "type" in item
            assert "path" in item
            assert item["type"] in ["file", "directory"]


class TestJsonNestedStructures:
    """Test JSON handling with nested structures."""

    def test_json_with_nested_arrays(self, tmp_path):
        """Test JSON file with nested arrays."""
        json_file = tmp_path / "nested.json"
        nested_data = {
            "users": [
                {"id": 1, "name": "Alice", "emails": ["alice@example.com"]},
                {
                    "id": 2,
                    "name": "Bob",
                    "emails": ["bob@example.com", "bob2@example.com"],
                },
            ]
        }
        json_file.write_text(json.dumps(nested_data))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file], include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/nested.json")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "pagination" in data

    def test_json_array_pagination(self, tmp_path):
        """Test pagination of JSON arrays."""
        json_file = tmp_path / "array.json"
        data_array = [{"id": i} for i in range(200)]
        json_file.write_text(json.dumps(data_array))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file])

        client = TestClient(app)
        response = client.get("/data/array.json?page=1&page_size=10")

        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) == 10
        assert data["pagination"]["has_next"] is True


class TestErrorHandling:
    """Test error handling in endpoints."""

    def test_schema_endpoint_error_handling(self, tmp_path):
        """Test that schema endpoint handles errors gracefully."""
        # Create a file with invalid content
        csv_file = tmp_path / "invalid.csv"
        csv_file.write_text("")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file], include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/invalid.csv_columnnames")

        # Should return error or 500 for empty file
        assert response.status_code in [200, 500]

    def test_data_endpoint_error_handling(self, tmp_path):
        """Test that data endpoint handles errors gracefully."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col1,col2\n1,2\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        client = TestClient(app)

        # Test with invalid page number
        response = client.get("/data/test.csv?page=0")
        assert response.status_code == 422  # Validation error

        # Test with invalid page size
        response = client.get("/data/test.csv?page_size=0")
        assert response.status_code == 422


class TestPaginationMetadata:
    """Test pagination metadata in responses."""

    def test_pagination_metadata_structure(self, temp_data_dir):
        """Test that pagination metadata is correctly structured."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.csv"])
        _create_endpoints(app, temp_data_dir, items)

        client = TestClient(app)
        response = client.get("/data/test_data.csv?page=1&page_size=5")

        assert response.status_code == 200
        data = response.json()
        pagination = data["pagination"]

        assert pagination["page"] == 1
        assert pagination["page_size"] == 5
        assert "total" in pagination
        assert "has_next" in pagination
        assert isinstance(pagination["has_next"], bool)

    def test_has_next_false_on_last_page(self, temp_data_dir):
        """Test that has_next is False on last page."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir, ["test_data.csv"])
        _create_endpoints(app, temp_data_dir, items)

        client = TestClient(app)

        # Get total count first
        response1 = client.get("/data/test_data.csv?page=1&page_size=100")
        total = response1.json()["pagination"]["total"]

        # Request last page
        last_page = (total // 5) + 1
        response2 = client.get(f"/data/test_data.csv?page={last_page}&page_size=5")

        assert response2.status_code == 200
        data = response2.json()
        assert data["pagination"]["has_next"] is False
