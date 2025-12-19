"""Additional tests for remaining coverage gaps."""

import json
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckdbfastapi.main import _create_endpoints, _get_data_path, _get_items_to_process


class TestTsvFileHandling:
    """Test TSV file handling specifically."""

    def test_tsv_schema_endpoint(self, tmp_path):
        """Test schema endpoint for TSV files."""
        tsv_file = tmp_path / "data.tsv"
        tsv_file.write_text("col1\tcol2\tcol3\n1\t2\t3\n4\t5\t6\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [tsv_file], include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/data.tsv_columnnames")

        assert response.status_code == 200
        data = response.json()
        assert len(data["columns"]) == 3
        assert data["columns"][0]["column_name"] == "col1"

    def test_txt_file_with_schema(self, tmp_path):
        """Test TXT file schema endpoint."""
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("id\tname\tvalue\n1\ttest\t100\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [txt_file], include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/data.txt_columnnames")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data


class TestParquetFileHandling:
    """Test Parquet file specific functionality."""

    def test_parquet_data_endpoint(self, tmp_path):
        """Test that parquet endpoint works correctly."""
        # Use the actual parquet file from datasample
        data_path = _get_data_path("duckdbfastapi_datasample")
        items = _get_items_to_process(data_path, ["evidence_intogen"])

        app = FastAPI()
        _create_endpoints(app, data_path, items, include_schema_cols=False)

        client = TestClient(app)
        response = client.get("/data/evidence_intogen")

        # Should return parquet folder data (evidence_intogen contains parquet files)
        assert response.status_code == 200
        data = response.json()
        # Either parquet data or directory listing
        assert "data" in data or "directory" in data or "contents" in data


class TestJsonEdgeCases:
    """Test JSON file edge cases."""

    def test_json_small_file_no_pagination(self, tmp_path):
        """Test that small JSON files return full data without pagination need."""
        json_file = tmp_path / "small.json"
        small_data = {"items": [{"id": i} for i in range(50)]}
        json_file.write_text(json.dumps(small_data))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file])

        client = TestClient(app)
        response = client.get("/data/small.json")

        assert response.status_code == 200
        data = response.json()
        # Should still have pagination info
        assert "pagination" in data

    def test_json_list_format(self, tmp_path):
        """Test JSON file as a list."""
        json_file = tmp_path / "list.json"
        list_data = [{"id": 1}, {"id": 2}, {"id": 3}]
        json_file.write_text(json.dumps(list_data))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [json_file])

        client = TestClient(app)
        response = client.get("/data/list.json")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data["data"], list)


class TestEndpointResponseStructure:
    """Test response structure for all endpoints."""

    def test_csv_response_has_columns(self, tmp_path):
        """Test that CSV response includes columns."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name,value\n1,test,100\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        client = TestClient(app)
        response = client.get("/data/data.csv")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert data["columns"] == ["id", "name", "value"]

    def test_tsv_response_has_columns(self, tmp_path):
        """Test that TSV response includes columns."""
        tsv_file = tmp_path / "data.tsv"
        tsv_file.write_text("id\tname\tvalue\n1\ttest\t100\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [tsv_file])

        client = TestClient(app)
        response = client.get("/data/data.tsv")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert data["columns"] == ["id", "name", "value"]

    def test_response_has_count(self, tmp_path):
        """Test that response includes row count."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,test\n2,test2\n3,test3\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        client = TestClient(app)
        response = client.get("/data/data.csv?page=1&page_size=2")

        assert response.status_code == 200
        data = response.json()
        assert "count" in data
        assert data["count"] == 2


class TestSchemaColumnTypes:
    """Test that schema endpoint returns correct data types."""

    def test_schema_includes_data_types(self, tmp_path):
        """Test that schema endpoint includes data types."""
        csv_file = tmp_path / "typed.csv"
        csv_file.write_text("id,name,score\n1,Alice,95.5\n2,Bob,87.3\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file], include_schema_cols=True)

        client = TestClient(app)
        response = client.get("/data/typed.csv_columnnames")

        assert response.status_code == 200
        data = response.json()
        for col in data["columns"]:
            assert "data_type" in col
            assert col["data_type"] != ""


class TestMultipleFiles:
    """Test handling of multiple files."""

    def test_create_endpoints_for_multiple_files(self, tmp_path):
        """Test creating endpoints for multiple files."""
        csv_file = tmp_path / "data1.csv"
        csv_file.write_text("id,name\n1,test\n")

        json_file = tmp_path / "data2.json"
        json_file.write_text(json.dumps({"items": [{"id": 1}]}))

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file, json_file])

        client = TestClient(app)

        # Test both endpoints exist
        response1 = client.get("/data/data1.csv")
        response2 = client.get("/data/data2.json")

        assert response1.status_code == 200
        assert response2.status_code == 200


class TestPaginationEdgeCases:
    """Test pagination edge cases."""

    def test_pagination_first_page(self, tmp_path):
        """Test that pagination correctly identifies first page."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(
            "id,name\n" + "\n".join([f"{i},test{i}" for i in range(20)])
        )

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        client = TestClient(app)
        response = client.get("/data/data.csv?page=1&page_size=5")

        assert response.status_code == 200
        data = response.json()
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["has_next"] is True

    def test_pagination_middle_page(self, tmp_path):
        """Test pagination on middle page."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(
            "id,name\n" + "\n".join([f"{i},test{i}" for i in range(20)])
        )

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file])

        client = TestClient(app)
        response = client.get("/data/data.csv?page=2&page_size=5")

        assert response.status_code == 200
        data = response.json()
        assert data["pagination"]["page"] == 2


class TestSchemaWithLargeFiles:
    """Test schema endpoints with large files."""

    def test_schema_pagination_with_many_columns(self, tmp_path):
        """Test schema pagination with many columns."""
        csv_file = tmp_path / "many_cols.csv"
        # Create file with 20 columns
        headers = ",".join([f"col{i}" for i in range(20)])
        row = ",".join([str(i) for i in range(20)])
        csv_file.write_text(f"{headers}\n{row}\n")

        app = FastAPI()
        _create_endpoints(app, tmp_path, [csv_file], include_schema_cols=True)

        client = TestClient(app)

        # First page
        response1 = client.get("/data/many_cols.csv_columnnames?page=1&page_size=5")
        assert response1.status_code == 200
        data1 = response1.json()
        assert data1["pagination"]["total"] == 20
        assert data1["pagination"]["has_next"] is True

        # Second page
        response2 = client.get("/data/many_cols.csv_columnnames?page=2&page_size=5")
        assert response2.status_code == 200
        data2 = response2.json()
        assert len(data2["columns"]) == 5
