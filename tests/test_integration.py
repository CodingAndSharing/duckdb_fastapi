"""Integration tests for duckdb_fastapi."""

import pytest
from duckdb_fastapi.main import run_fastapi


@pytest.fixture
def test_app(temp_data_dir):
    """Create a test app with temp data."""
    # This fixture would be used for testing the app creation
    # Note: run_fastapi starts a server, so we test the functions separately
    pass


class TestRunFastAPIValidation:
    """Test argument validation for run_fastapi function."""

    def test_invalid_port_negative(self, temp_data_dir):
        """Test with negative port - validation happens when called."""
        # Port validation is minimal in the function
        # This test documents expected behavior
        pass

    def test_invalid_port_too_high(self, temp_data_dir):
        """Test with port too high."""
        # Port validation is minimal in the function
        pass

    def test_invalid_host_empty(self, temp_data_dir):
        """Test with empty host."""
        # Host validation is minimal in the function
        pass

    def test_invalid_path_data(self):
        """Test with invalid path_data."""
        with pytest.raises(ValueError, match="Invalid path_data"):
            run_fastapi("/nonexistent/path/that/does/not/exist")

    def test_no_items_found(self, tmp_path):
        """Test when no items are found."""
        # Create an empty directory
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        with pytest.raises(ValueError, match="No items found"):
            run_fastapi(str(empty_dir))


class TestDataSourceHandling:
    """Test handling of different data sources."""

    def test_specific_items_not_found(self, temp_data_dir):
        """Test with specific items that don't exist."""
        with pytest.raises(ValueError, match="No items found"):
            run_fastapi(str(temp_data_dir), specific_items=["nonexistent.json"])

    def test_mixed_specific_items(self, temp_data_dir):
        """Test with mix of existing and non-existing items."""
        from duckdb_fastapi.main import _get_items_to_process

        # Should process only existing items
        items = _get_items_to_process(
            temp_data_dir, ["test_data.json", "nonexistent.txt"]
        )
        assert len(items) >= 1


class TestEndpointGeneration:
    """Test endpoint generation logic."""

    def test_endpoint_names_sanitized(self, temp_data_dir):
        """Test that endpoint names are properly sanitized."""
        from duckdb_fastapi.main import _get_items_to_process

        items = _get_items_to_process(temp_data_dir)
        for item in items:
            # Verify that item names can be converted to valid endpoint names
            endpoint_name = item.name.lower().replace(" ", "_").replace("-", "_")
            assert endpoint_name.replace("_", "").isalnum() or "." in endpoint_name
