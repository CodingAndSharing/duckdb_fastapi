"""DuckDBFastAPI - A FastAPI application for serving DuckDB data endpoints."""

__version__ = "0.1.2"

from .main import run_fastapi

__all__ = ["run_fastapi"]
