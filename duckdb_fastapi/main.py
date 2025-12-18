"""Main module for DuckDB FastAPI application."""

from pathlib import Path
from typing import List, Optional

import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn


class DataResponse(BaseModel):
    """Response model for data queries."""

    data: dict
    message: str


def _get_data_path(path_data: str) -> Path:
    """
    Resolve the data path.

    Args:
        path_data: Path to data or special keyword "duckdb_fastapi_datasample"

    Returns:
        Path: Resolved path to the data directory

    Raises:
        ValueError: If path does not exist
    """
    if path_data == "duckdb_fastapi_datasample":
        # Get the datasample folder relative to this package
        base_path = Path(__file__).parent.parent
        resolved_path = base_path / "datasample"
    else:
        resolved_path = Path(path_data).resolve()

    if not resolved_path.exists():
        raise ValueError(f"Data path does not exist: {resolved_path}")

    if not resolved_path.is_dir():
        raise ValueError(f"Data path must be a directory: {resolved_path}")

    return resolved_path


def _get_items_to_process(
    data_path: Path, specific_items: Optional[List[str]] = None
) -> List[Path]:
    """
    Get the list of files/folders to process.

    Args:
        data_path: Path to the data directory
        specific_items: Optional list of specific files/folders to process

    Returns:
        List[Path]: List of paths to process
    """
    items = []

    if specific_items:
        for item in specific_items:
            item_path = data_path / item
            if item_path.exists():
                items.append(item_path)
    else:
        # Get all items in the directory
        for item in sorted(data_path.iterdir()):
            if item.is_file() or item.is_dir():
                items.append(item)

    return items


def _create_endpoints(app: FastAPI, data_path: Path, items: List[Path]) -> None:
    """
    Create endpoints for each file/folder.

    Args:
        app: FastAPI application instance
        data_path: Base data directory path
        items: List of items to create endpoints for
    """
    for item in items:
        item_name = item.name.lower().replace(" ", "_").replace("-", "_")

        if item.is_file():
            # Create endpoint for file
            if item.suffix in [".json", ".csv", ".parquet"]:

                @app.get(f"/data/{item_name}", response_model=dict)
                async def read_file(item_path: Path = item):
                    """Read and return file data."""
                    try:
                        if item_path.suffix == ".json":
                            conn = duckdb.connect(":memory:")
                            result = conn.execute(
                                f"SELECT * FROM read_json_auto('{item_path}')"
                            ).fetchall()
                            columns = (
                                [desc[0] for desc in conn.description]
                                if conn.description
                                else []
                            )
                            return {
                                "data": result,
                                "columns": columns,
                                "count": len(result),
                            }
                        elif item_path.suffix == ".csv":
                            conn = duckdb.connect(":memory:")
                            result = conn.execute(
                                f"SELECT * FROM read_csv_auto('{item_path}')"
                            ).fetchall()
                            columns = (
                                [desc[0] for desc in conn.description]
                                if conn.description
                                else []
                            )
                            return {
                                "data": result,
                                "columns": columns,
                                "count": len(result),
                            }
                        elif item_path.suffix == ".parquet":
                            conn = duckdb.connect(":memory:")
                            result = conn.execute(
                                f"SELECT * FROM '{item_path}'"
                            ).fetchall()
                            columns = (
                                [desc[0] for desc in conn.description]
                                if conn.description
                                else []
                            )
                            return {
                                "data": result,
                                "columns": columns,
                                "count": len(result),
                            }
                    except Exception as e:
                        raise HTTPException(
                            status_code=500, detail=f"Error reading file: {str(e)}"
                        )

                    return {"error": "Unsupported file format"}

        elif item.is_dir():
            # Create endpoint for directory
            @app.get(f"/data/{item_name}", response_model=dict)
            async def read_directory(item_path: Path = item):
                """Return information about directory contents."""
                try:
                    contents = []
                    for file_item in sorted(item_path.iterdir()):
                        contents.append(
                            {
                                "name": file_item.name,
                                "type": "file" if file_item.is_file() else "directory",
                                "path": str(file_item.relative_to(data_path)),
                            }
                        )
                    return {"directory": item_name, "contents": contents}
                except Exception as e:
                    raise HTTPException(
                        status_code=500, detail=f"Error reading directory: {str(e)}"
                    )


def run_fastapi(
    path_data: str,
    specific_items: Optional[List[str]] = None,
    port: int = 8000,
    host: str = "127.0.0.1",
) -> None:
    """
    Run FastAPI application with data endpoints.

    Args:
        path_data: Path to data directory or "duckdb_fastapi_datasample"
        specific_items: Optional list of specific files/folders to create endpoints for
        port: Port to run the server on (default: 8000)
        host: Host to run the server on (default: 127.0.0.1)

    Raises:
        ValueError: If path_data is invalid
        ValueError: If host/port are invalid

    Example:
        >>> run_fastapi("./data")
        >>> run_fastapi("duckdb_fastapi_datasample", port=9000)
        >>> run_fastapi("./data", specific_items=["file1.json", "folder1"])
        >>> run_fastapi("./data", host="0.0.0.0", port=8080)
    """
    # Validate arguments
    if not isinstance(port, int) or port < 1 or port > 65535:
        raise ValueError("Port must be an integer between 1 and 65535")

    if not isinstance(host, str) or not host:
        raise ValueError("Host must be a non-empty string")

    # Create FastAPI app
    app = FastAPI(
        title="DuckDB FastAPI",
        description="FastAPI application for serving DuckDB data endpoints",
        version="0.1.0",
    )

    # Resolve data path
    try:
        data_path = _get_data_path(path_data)
    except ValueError as e:
        raise ValueError(f"Invalid path_data: {str(e)}")

    # Get items to process
    items = _get_items_to_process(data_path, specific_items)

    if not items:
        raise ValueError(
            f"No items found in {data_path}"
            + (f" matching {specific_items}" if specific_items else "")
        )

    # Create endpoints
    _create_endpoints(app, data_path, items)

    # Add health check endpoint
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {"status": "healthy", "version": "0.1.0"}

    # Add root endpoint
    @app.get("/")
    async def root():
        """Root endpoint."""
        return {
            "message": "DuckDB FastAPI",
            "version": "0.1.0",
            "data_path": str(data_path),
            "endpoints": [f"/data/{item.name}" for item in items],
        }

    # Run the application
    uvicorn.run(app, host=host, port=port, log_level="info")
