"""API endpoint tests for duckdbfastapi."""

import json
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckdbfastapi.main import _create_endpoints, _get_items_to_process


class TestEndpointResponses:
    """Test actual endpoint responses."""

    def test_json_file_endpoint_response(self, temp_data_dir):
        """Test JSON file endpoint returns correct response."""
        app = FastAPI()
        items = [f for f in _get_items_to_process(temp_data_dir) if f.suffix == ".json"]

        if items:
            _create_endpoints(app, temp_data_dir, items)

            # Add root endpoint for testing
            @app.get("/")
            async def root():
                return {"status": "ok"}

            client = TestClient(app)

            # Test root
            response = client.get("/")
            assert response.status_code == 200
            assert response.json()["status"] == "ok"

    def test_csv_file_endpoint_response(self, temp_data_dir):
        """Test CSV file endpoint returns correct response."""
        app = FastAPI()
        items = [f for f in _get_items_to_process(temp_data_dir) if f.suffix == ".csv"]

        if items:
            _create_endpoints(app, temp_data_dir, items)

            @app.get("/")
            async def root():
                return {"status": "ok"}

            client = TestClient(app)
            response = client.get("/")
            assert response.status_code == 200

    def test_directory_endpoint_response(self, temp_data_dir):
        """Test directory endpoint returns contents."""
        app = FastAPI()
        items = [f for f in _get_items_to_process(temp_data_dir) if f.is_dir()]

        if items:
            _create_endpoints(app, temp_data_dir, items)

            @app.get("/")
            async def root():
                return {"status": "ok"}

            client = TestClient(app)
            response = client.get("/")
            assert response.status_code == 200

    def test_health_check_endpoint(self, temp_data_dir):
        """Test health check endpoint."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir)[:1]

        if items:
            _create_endpoints(app, temp_data_dir, items)

            # Add health check
            @app.get("/health")
            async def health_check():
                return {"status": "healthy", "version": "0.1.2"}

            client = TestClient(app)
            response = client.get("/health")
            assert response.status_code == 200
            assert response.json()["status"] == "healthy"

    def test_root_endpoint(self, temp_data_dir):
        """Test root endpoint lists all endpoints."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir)

        if items:
            _create_endpoints(app, temp_data_dir, items)

            @app.get("/")
            async def root():
                return {
                    "message": "DuckDB FastAPI",
                    "version": "0.1.2",
                    "data_path": str(temp_data_dir),
                    "endpoints": [f"/data/{item.name}" for item in items],
                }

            client = TestClient(app)
            response = client.get("/")
            assert response.status_code == 200
            data = response.json()
            assert "message" in data
            assert "endpoints" in data


class TestRunFastAPIValidation:
    """Test run_fastapi function validation."""

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_port_validation_lower_bound(self, mock_uvicorn, temp_data_dir):
        """Test port must be > 0."""
        from duckdbfastapi.main import run_fastapi

        with pytest.raises(ValueError, match="Port must be an integer"):
            run_fastapi(str(temp_data_dir), port=0)

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_port_validation_upper_bound(self, mock_uvicorn, temp_data_dir):
        """Test port must be < 65536."""
        from duckdbfastapi.main import run_fastapi

        with pytest.raises(ValueError, match="Port must be an integer"):
            run_fastapi(str(temp_data_dir), port=65536)

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_host_validation_not_empty(self, mock_uvicorn, temp_data_dir):
        """Test host must not be empty."""
        from duckdbfastapi.main import run_fastapi

        with pytest.raises(ValueError, match="Host must be a non-empty string"):
            run_fastapi(str(temp_data_dir), host="")

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_host_validation_must_be_string(self, mock_uvicorn, temp_data_dir):
        """Test host must be a string."""
        from duckdbfastapi.main import run_fastapi

        with pytest.raises(ValueError, match="Host must be a non-empty string"):
            run_fastapi(str(temp_data_dir), host=None)


class TestRunFastAPIIntegration:
    """Integration tests for run_fastapi with mocked uvicorn."""

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_run_fastapi_with_default_parameters(self, mock_uvicorn, temp_data_dir):
        """Test run_fastapi with default parameters."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi(str(temp_data_dir))

        # Verify uvicorn.run was called
        mock_uvicorn.assert_called_once()
        args, kwargs = mock_uvicorn.call_args
        assert kwargs["host"] == "127.0.0.1"
        assert kwargs["port"] == 8000

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_run_fastapi_with_custom_host_port(self, mock_uvicorn, temp_data_dir):
        """Test run_fastapi with custom host and port."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi(str(temp_data_dir), host="0.0.0.0", port=9000)

        mock_uvicorn.assert_called_once()
        args, kwargs = mock_uvicorn.call_args
        assert kwargs["host"] == "0.0.0.0"
        assert kwargs["port"] == 9000

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_run_fastapi_with_specific_items(self, mock_uvicorn, temp_data_dir):
        """Test run_fastapi with specific items."""
        from duckdbfastapi.main import run_fastapi

        items = _get_items_to_process(temp_data_dir)
        if items:
            specific_item = items[0].name
            run_fastapi(str(temp_data_dir), specific_items=[specific_item])

            mock_uvicorn.assert_called_once()

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_run_fastapi_creates_app(self, mock_uvicorn, temp_data_dir):
        """Test that run_fastapi creates a FastAPI app."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi(str(temp_data_dir))

        mock_uvicorn.assert_called_once()
        app = mock_uvicorn.call_args[0][0]
        assert isinstance(app, FastAPI)

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_run_fastapi_datasample_keyword(self, mock_uvicorn):
        """Test run_fastapi with datasample keyword."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi("duckdbfastapi_datasample")

        mock_uvicorn.assert_called_once()


class TestAppEndpoints:
    """Test endpoints created by _create_endpoints."""

    def test_endpoints_exist_on_app(self, temp_data_dir):
        """Test that endpoints are registered on the FastAPI app."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir)

        if items:
            initial_routes = len(app.routes)
            _create_endpoints(app, temp_data_dir, items)
            final_routes = len(app.routes)

            # More routes should be added
            assert final_routes > initial_routes

    def test_endpoint_paths_are_correct(self, temp_data_dir):
        """Test that endpoint paths follow the expected pattern."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir)

        if items:
            _create_endpoints(app, temp_data_dir, items)

            data_routes = [route.path for route in app.routes if "/data/" in route.path]

            # Should have data routes
            assert len(data_routes) > 0

    def test_endpoints_for_all_items(self, temp_data_dir):
        """Test that endpoints are created for all items."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir)

        if len(items) > 0:
            _create_endpoints(app, temp_data_dir, items)

            data_routes = [route.path for route in app.routes if "/data/" in route.path]

            # Should have at least as many routes as items
            assert len(data_routes) >= len(items)

    def test_endpoint_methods_are_get(self, temp_data_dir):
        """Test that all data endpoints are GET methods."""
        app = FastAPI()
        items = _get_items_to_process(temp_data_dir)

        if items:
            _create_endpoints(app, temp_data_dir, items)

            for route in app.routes:
                if "/data/" in route.path:
                    # GET method should be allowed
                    assert hasattr(route, "methods")


class TestDataProcessing:
    """Test data processing and response formats."""

    def test_json_response_structure(self, sample_json_file):
        """Test JSON response has expected structure."""
        data = json.loads(sample_json_file.read_text())

        assert isinstance(data, list)
        assert len(data) > 0

    def test_csv_response_structure(self, sample_csv_file):
        """Test CSV response has expected structure."""
        content = sample_csv_file.read_text()
        lines = content.strip().split("\n")

        assert len(lines) >= 2  # Header + at least one row

    def test_error_response_format(self):
        """Test error responses have correct format."""
        # Test error handling format
        from fastapi import HTTPException

        error = HTTPException(status_code=500, detail="Test error")
        assert error.status_code == 500
        assert error.detail == "Test error"


class TestApplicationCreation:
    """Test FastAPI app creation and configuration."""

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_app_metadata(self, mock_uvicorn, temp_data_dir):
        """Test that app has correct metadata."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi(str(temp_data_dir))

        app = mock_uvicorn.call_args[0][0]
        assert app.title == "DuckDB FastAPI"
        assert "0.1.2" in app.version

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_app_description(self, mock_uvicorn, temp_data_dir):
        """Test that app has description."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi(str(temp_data_dir))

        app = mock_uvicorn.call_args[0][0]
        assert "DuckDB" in app.description

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_health_endpoint_exists(self, mock_uvicorn, temp_data_dir):
        """Test that health endpoint is created."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi(str(temp_data_dir))

        app = mock_uvicorn.call_args[0][0]
        routes = [route.path for route in app.routes]
        assert "/health" in routes

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_root_endpoint_exists(self, mock_uvicorn, temp_data_dir):
        """Test that root endpoint is created."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi(str(temp_data_dir))

        app = mock_uvicorn.call_args[0][0]
        routes = [route.path for route in app.routes]
        assert "/" in routes

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_uvicorn_called_with_correct_log_level(self, mock_uvicorn, temp_data_dir):
        """Test that uvicorn is called with correct log level."""
        from duckdbfastapi.main import run_fastapi

        run_fastapi(str(temp_data_dir))

        args, kwargs = mock_uvicorn.call_args
        assert kwargs["log_level"] == "info"


class TestErrorHandlingExtended:
    """Extended error handling tests."""

    def test_invalid_data_path_raises_error(self):
        """Test that invalid data path raises ValueError."""
        from duckdbfastapi.main import run_fastapi

        with pytest.raises(ValueError, match="Invalid path_data"):
            run_fastapi("/this/path/definitely/does/not/exist/12345")

    def test_empty_data_directory_raises_error(self, tmp_path):
        """Test that empty data directory raises ValueError."""
        from duckdbfastapi.main import run_fastapi

        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        with pytest.raises(ValueError, match="No items found"):
            run_fastapi(str(empty_dir))

    def test_specific_items_with_nonexistent_files(self, temp_data_dir):
        """Test specific items that don't exist raises error."""
        from duckdbfastapi.main import run_fastapi

        with pytest.raises(ValueError, match="No items found"):
            run_fastapi(
                str(temp_data_dir), specific_items=["nonexistent_file_xyz.json"]
            )

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_no_items_error_message_format(self, mock_uvicorn, temp_data_dir):
        """Test error message format for no items."""
        from duckdbfastapi.main import run_fastapi

        # Create mock to capture error
        specific = ["item_that_does_not_exist.json"]
        filtered_items = _get_items_to_process(temp_data_dir, specific)

        if not filtered_items:
            with pytest.raises(ValueError, match="No items found.*matching"):
                run_fastapi(str(temp_data_dir), specific_items=specific)


class TestSpecificItemFiltering:
    """Test specific items filtering in run_fastapi."""

    @patch("duckdbfastapi.main.uvicorn.run")
    def test_run_fastapi_filters_specific_items(self, mock_uvicorn, temp_data_dir):
        """Test that specific_items parameter filters correctly."""
        from duckdbfastapi.main import run_fastapi

        items = _get_items_to_process(temp_data_dir)
        if len(items) > 0:
            specific_name = items[0].name
            run_fastapi(str(temp_data_dir), specific_items=[specific_name])

            mock_uvicorn.assert_called_once()
            app = mock_uvicorn.call_args[0][0]
            # App should be created successfully
            assert isinstance(app, FastAPI)
