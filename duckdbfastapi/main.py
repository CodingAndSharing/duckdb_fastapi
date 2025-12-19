"""Main module for DuckDB FastAPI application."""

import json
from pathlib import Path
from typing import List, Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import uvicorn


class PaginationResponse(BaseModel):
    """Response model for paginated data."""

    data: List[dict]
    columns: List[str]
    count: int
    total: int
    page: int
    page_size: int
    has_next: bool


class DataResponse(BaseModel):
    """Response model for data queries."""

    data: dict
    message: str


def _count_json_items(obj: dict) -> tuple[int, int]:
    """
    Count total items in JSON object (parent + children).

    Args:
        obj: JSON object to count

    Returns:
        tuple: (total_count, max_nested_count)
    """

    def count_nested(o, depth=0):
        if isinstance(o, dict):
            return sum(count_nested(v, depth + 1) for v in o.values())
        elif isinstance(o, list):
            return len(o) + sum(count_nested(item, depth + 1) for item in o)
        return 1

    total = count_nested(obj)
    max_nested = 0

    if isinstance(obj, dict):
        for v in obj.values():
            if isinstance(v, (dict, list)):
                max_nested = max(max_nested, count_nested(v))
    elif isinstance(obj, list):
        max_nested = len(obj)

    return total, max_nested


def _get_data_path(path_data: str) -> Path:
    """
    Resolve the data path.

    Args:
        path_data: Path to data or special keyword "duckdbfastapi_datasample"

    Returns:
        Path: Resolved path to the data directory

    Raises:
        ValueError: If path does not exist
    """
    if path_data == "duckdbfastapi_datasample":
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


def _has_parquet_files(folder_path: Path) -> bool:
    """
    Check if a folder contains parquet files.

    Args:
        folder_path: Path to the folder

    Returns:
        bool: True if folder contains .parquet files
    """
    if not folder_path.is_dir():
        return False
    return any(f.suffix == ".parquet" for f in folder_path.iterdir() if f.is_file())


def _create_endpoints(
    app: FastAPI,
    data_path: Path,
    items: List[Path] = [],
    include_schema_cols: bool = True,
) -> None:
    """
    Create endpoints for each file/folder.

    Args:
        app: FastAPI application instance
        data_path: Base data directory path
        items: List of items to create endpoints for
        include_schema_cols: If True, create additional _columnnames endpoints with schema info
    """
    for item in items:
        item_name = item.name.lower().replace(" ", "_").replace("-", "_")

        if item.is_file():
            # Create endpoint for file
            if item.suffix in [".json", ".csv", ".tsv", ".txt", ".parquet"]:

                @app.get(f"/data/{item_name}", response_model=dict)
                async def read_file(
                    item_path: Path = item,
                    page: int = Query(
                        1, ge=1, description="Page number for pagination"
                    ),
                    page_size: int = Query(
                        5, ge=1, le=1000, description="Items per page"
                    ),
                ):
                    """Read and return file data."""
                    try:
                        if item_path.suffix == ".json":
                            # Read JSON file
                            with open(item_path, "r") as f:
                                json_data = json.load(f)

                            # Count items to determine if pagination is needed
                            total_count, max_nested = _count_json_items(json_data)

                            # If small file (< 100 items), return full JSON
                            if total_count <= 100 and max_nested <= 100:
                                return {
                                    "data": json_data,
                                    "count": total_count,
                                    "pagination": {
                                        "page": 1,
                                        "page_size": total_count,
                                        "total": total_count,
                                        "has_next": False,
                                    },
                                }

                            # For larger files, convert to list format for pagination
                            if isinstance(json_data, list):
                                data_list = json_data
                            elif isinstance(json_data, dict):
                                # Convert dict to list of items
                                data_list = [json_data]
                            else:
                                data_list = [json_data]

                            # Apply pagination
                            total = len(data_list)
                            offset = (page - 1) * page_size
                            paginated_data = data_list[offset : offset + page_size]

                            return {
                                "data": paginated_data,
                                "count": len(paginated_data),
                                "pagination": {
                                    "page": page,
                                    "page_size": page_size,
                                    "total": total,
                                    "has_next": offset + page_size < total,
                                },
                            }

                        elif item_path.suffix == ".csv":
                            # Read CSV with DuckDB and apply limit
                            conn = duckdb.connect(":memory:")

                            # Get total count first
                            total_result = conn.execute(
                                f"SELECT COUNT(*) as cnt FROM read_csv_auto('{item_path}')"
                            ).fetchall()
                            total_count = total_result[0][0] if total_result else 0

                            # Apply pagination with LIMIT and OFFSET
                            offset = (page - 1) * page_size
                            result = conn.execute(
                                f"SELECT * FROM read_csv_auto('{item_path}') LIMIT {page_size} OFFSET {offset}"
                            ).fetchall()

                            columns = (
                                [desc[0] for desc in conn.description]
                                if conn.description
                                else []
                            )

                            # Convert rows to list of dicts
                            data_list = [dict(zip(columns, row)) for row in result]

                            return {
                                "data": data_list,
                                "columns": columns,
                                "count": len(data_list),
                                "pagination": {
                                    "page": page,
                                    "page_size": page_size,
                                    "total": total_count,
                                    "has_next": offset + page_size < total_count,
                                },
                            }

                        elif item_path.suffix in [".tsv", ".txt"]:
                            # Read TSV/TXT files with tab separator using DuckDB
                            conn = duckdb.connect(":memory:")

                            # Get total count first
                            total_result = conn.execute(
                                f"SELECT COUNT(*) as cnt FROM read_csv_auto('{item_path}', sep='\t')"
                            ).fetchall()
                            total_count = total_result[0][0] if total_result else 0

                            # Apply pagination with LIMIT and OFFSET
                            offset = (page - 1) * page_size
                            result = conn.execute(
                                f"SELECT * FROM read_csv_auto('{item_path}', sep='\t') LIMIT {page_size} OFFSET {offset}"
                            ).fetchall()

                            columns = (
                                [desc[0] for desc in conn.description]
                                if conn.description
                                else []
                            )

                            # Convert rows to list of dicts
                            data_list = [dict(zip(columns, row)) for row in result]

                            return {
                                "data": data_list,
                                "columns": columns,
                                "count": len(data_list),
                                "pagination": {
                                    "page": page,
                                    "page_size": page_size,
                                    "total": total_count,
                                    "has_next": offset + page_size < total_count,
                                },
                            }

                        elif item_path.suffix == ".parquet":
                            # Read Parquet file with DuckDB
                            conn = duckdb.connect(":memory:")

                            # Get total count
                            total_result = conn.execute(
                                f"SELECT COUNT(*) as cnt FROM '{item_path}'"
                            ).fetchall()
                            total_count = total_result[0][0] if total_result else 0

                            # Apply pagination
                            offset = (page - 1) * page_size
                            result = conn.execute(
                                f"SELECT * FROM '{item_path}' LIMIT {page_size} OFFSET {offset}"
                            ).fetchall()

                            columns = (
                                [desc[0] for desc in conn.description]
                                if conn.description
                                else []
                            )

                            # Convert rows to list of dicts
                            data_list = [dict(zip(columns, row)) for row in result]

                            return {
                                "data": data_list,
                                "columns": columns,
                                "count": len(data_list),
                                "pagination": {
                                    "page": page,
                                    "page_size": page_size,
                                    "total": total_count,
                                    "has_next": offset + page_size < total_count,
                                },
                            }
                    except Exception as e:
                        raise HTTPException(
                            status_code=500, detail=f"Error reading file: {str(e)}"
                        )

                    return {"error": "Unsupported file format"}

                # Create schema/columns endpoint if enabled
                if include_schema_cols:

                    @app.get(f"/data/{item_name}_columnnames", response_model=dict)
                    async def get_column_info(
                        item_path: Path = item,
                        page: int = Query(
                            1, ge=1, description="Page number for pagination"
                        ),
                        page_size: int = Query(
                            5, ge=1, le=1000, description="Items per page"
                        ),
                    ):
                        """Get column names, types, and example values with pagination."""
                        try:
                            conn = duckdb.connect(":memory:")

                            # Determine the query based on file type
                            if item_path.suffix == ".json":
                                query = f"SELECT * FROM read_json_auto('{item_path}') LIMIT 1"
                            elif item_path.suffix == ".csv":
                                query = f"SELECT * FROM read_csv_auto('{item_path}') LIMIT 1"
                            elif item_path.suffix in [".tsv", ".txt"]:
                                query = f"SELECT * FROM read_csv_auto('{item_path}', sep='\t') LIMIT 1"
                            elif item_path.suffix == ".parquet":
                                query = f"SELECT * FROM '{item_path}' LIMIT 1"
                            else:
                                raise ValueError(
                                    f"Unsupported file type: {item_path.suffix}"
                                )

                            # Execute query to get schema
                            conn.execute(query)

                            # Get column information
                            columns_info = []
                            for i, desc in enumerate(conn.description):
                                col_name = desc[0]
                                col_type = str(desc[1]) if desc[1] else "unknown"

                                # Get example value
                                result = conn.execute(
                                    f"SELECT {col_name} FROM ({query.replace('LIMIT 1', '')}) LIMIT 1"
                                ).fetchall()
                                example_value = (
                                    result[0][0] if result and result[0] else None
                                )

                                columns_info.append(
                                    {
                                        "column_name": col_name,
                                        "data_type": col_type,
                                        "example_value": str(example_value)
                                        if example_value is not None
                                        else "NULL",
                                    }
                                )

                            # Apply pagination
                            total = len(columns_info)
                            offset = (page - 1) * page_size
                            paginated_columns = columns_info[
                                offset : offset + page_size
                            ]

                            return {
                                "columns": paginated_columns,
                                "count": len(paginated_columns),
                                "pagination": {
                                    "page": page,
                                    "page_size": page_size,
                                    "total": total,
                                    "has_next": offset + page_size < total,
                                },
                            }
                        except Exception as e:
                            raise HTTPException(
                                status_code=500,
                                detail=f"Error getting column info: {str(e)}",
                            )

        elif item.is_dir():
            # Check if directory contains parquet files
            has_parquets = _has_parquet_files(item)

            if has_parquets:
                # Create endpoint for parquet folder (read all parquets as one dataset)
                @app.get(f"/data/{item_name}", response_model=dict)
                async def read_parquet_folder(
                    item_path: Path = item,
                    page: int = Query(
                        1, ge=1, description="Page number for pagination"
                    ),
                    page_size: int = Query(
                        5, ge=1, le=1000, description="Items per page"
                    ),
                ):
                    """Read all parquet files in folder as a single dataset."""
                    try:
                        conn = duckdb.connect(":memory:")

                        # Use glob pattern to read all parquet files in the folder
                        parquet_pattern = f"{item_path}/*.parquet"

                        # Get total count
                        total_result = conn.execute(
                            f"SELECT COUNT(*) as cnt FROM '{parquet_pattern}'"
                        ).fetchall()
                        total_count = total_result[0][0] if total_result else 0

                        # Apply pagination
                        offset = (page - 1) * page_size
                        result = conn.execute(
                            f"SELECT * FROM '{parquet_pattern}' LIMIT {page_size} OFFSET {offset}"
                        ).fetchall()

                        columns = (
                            [desc[0] for desc in conn.description]
                            if conn.description
                            else []
                        )

                        # Convert rows to list of dicts
                        data_list = [dict(zip(columns, row)) for row in result]

                        return {
                            "data": data_list,
                            "columns": columns,
                            "count": len(data_list),
                            "pagination": {
                                "page": page,
                                "page_size": page_size,
                                "total": total_count,
                                "has_next": offset + page_size < total_count,
                            },
                        }
                    except Exception as e:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Error reading parquet folder: {str(e)}",
                        )

                # Create schema endpoint for parquet folder if enabled
                if include_schema_cols:

                    @app.get(f"/data/{item_name}_columnnames", response_model=dict)
                    async def get_parquet_folder_column_info(
                        item_path: Path = item,
                        page: int = Query(
                            1, ge=1, description="Page number for pagination"
                        ),
                        page_size: int = Query(
                            5, ge=1, le=1000, description="Items per page"
                        ),
                    ):
                        """Get column names, types, and example values from parquet folder."""
                        try:
                            conn = duckdb.connect(":memory:")

                            # Use glob pattern to read all parquet files
                            parquet_pattern = f"{item_path}/*.parquet"

                            # Execute query to get schema
                            conn.execute(f"SELECT * FROM '{parquet_pattern}' LIMIT 1")

                            # Get column information
                            columns_info = []
                            for i, desc in enumerate(conn.description):
                                col_name = desc[0]
                                col_type = str(desc[1]) if desc[1] else "unknown"

                                # Get example value
                                result = conn.execute(
                                    f"SELECT {col_name} FROM '{parquet_pattern}' LIMIT 1"
                                ).fetchall()
                                example_value = (
                                    result[0][0] if result and result[0] else None
                                )

                                columns_info.append(
                                    {
                                        "column_name": col_name,
                                        "data_type": col_type,
                                        "example_value": str(example_value)
                                        if example_value is not None
                                        else "NULL",
                                    }
                                )

                            # Apply pagination
                            total = len(columns_info)
                            offset = (page - 1) * page_size
                            paginated_columns = columns_info[
                                offset : offset + page_size
                            ]

                            return {
                                "columns": paginated_columns,
                                "count": len(paginated_columns),
                                "pagination": {
                                    "page": page,
                                    "page_size": page_size,
                                    "total": total,
                                    "has_next": offset + page_size < total,
                                },
                            }
                        except Exception as e:
                            raise HTTPException(
                                status_code=500,
                                detail=f"Error getting parquet folder column info: {str(e)}",
                            )
            else:
                # Create endpoint for regular directory (non-parquet)
                @app.get(f"/data/{item_name}", response_model=dict)
                async def read_directory(item_path: Path = item):
                    """Return information about directory contents."""
                    try:
                        contents = []
                        for file_item in sorted(item_path.iterdir()):
                            contents.append(
                                {
                                    "name": file_item.name,
                                    "type": "file"
                                    if file_item.is_file()
                                    else "directory",
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
        path_data: Path to data directory or "duckdbfastapi_datasample"
        specific_items: Optional list of specific files/folders to create endpoints for
        port: Port to run the server on (default: 8000)
        host: Host to run the server on (default: 127.0.0.1)

    Raises:
        ValueError: If path_data is invalid
        ValueError: If host/port are invalid

    Example:
        >>> run_fastapi("./data")
        >>> run_fastapi("duckdbfastapi_datasample", port=9000)
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
