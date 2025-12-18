# DuckDB FastAPI

A FastAPI application for creating dynamic endpoints to serve DuckDB data from files and folders.

## Features

- 🚀 **Dynamic Endpoints**: Automatically create endpoints for JSON, CSV, and Parquet files
- 📁 **Folder Support**: Serve data from directory structures
- 🎯 **Flexible Configuration**: Specify custom data sources or use the default sample data
- 🔧 **Easy to Use**: Simple Python API with minimal configuration
- 📊 **DuckDB Integration**: Leverage DuckDB for efficient data querying
- ✅ **Well Tested**: Comprehensive test suite with coverage reporting

## Installation

```bash
pip install duckdb-fastapi
```

### Development Installation

```bash
git clone <repository-url>
cd duckdb_fastapi
pip install -e ".[dev]"
```

## Quick Start

### Basic Usage

```python
from duckdb_fastapi import run_fastapi

# Run with default datasample
run_fastapi("duckdb_fastapi_datasample")

# Run with custom data directory
run_fastapi("/path/to/data")

# Run on specific host and port
run_fastapi("/path/to/data", host="0.0.0.0", port=9000)

# Use specific files/folders only
run_fastapi("/path/to/data", specific_items=["file1.json", "folder1"])
```

### API Endpoints

Once running, the application provides:

- `GET /`: Root endpoint listing all available data endpoints
- `GET /health`: Health check endpoint
- `GET /data/{item_name}`: Data endpoint for each file/folder

## Configuration

### Arguments

`run_fastapi(path_data, specific_items=None, port=8000, host="127.0.0.1")`

- **path_data** (str, required): Path to data directory or `"duckdb_fastapi_datasample"`
- **specific_items** (List[str], optional): List of specific files/folders to expose
- **port** (int, optional): Port to run server on (default: 8000)
- **host** (str, optional): Host to bind to (default: "127.0.0.1")

## Supported File Formats

- JSON (`.json`)
- CSV (`.csv`)
- Parquet (`.parquet`)

## Example

```python
from duckdb_fastapi import run_fastapi

# Run with custom data
run_fastapi(
    path_data="/home/user/data",
    specific_items=["users.json", "products.csv"],
    host="0.0.0.0",
    port=8000
)

# Then visit:
# http://localhost:8000/ - List all endpoints
# http://localhost:8000/health - Health check
# http://localhost:8000/data/users_json - Query users.json
# http://localhost:8000/data/products_csv - Query products.csv
```

## Testing

Run tests with coverage:

```bash
pytest tests/ --cov=duckdb_fastapi --cov-report=html
```

Run specific test file:

```bash
pytest tests/test_main.py -v
```

Generate coverage report:

```bash
pytest --cov=duckdb_fastapi --cov-report=html
# Open htmlcov/index.html in browser
```

## CI/CD Pipeline

The project includes a GitHub Actions workflow that:

- ✅ Runs tests on Python 3.8, 3.9, 3.10, 3.11
- ✅ Generates coverage reports with pytest-cov
- ✅ Performs linting with flake8
- ✅ Checks code formatting with black
- ✅ Runs type checking with mypy
- ✅ Builds distribution packages
- ✅ Enforces quality gates (minimum 70% coverage)

## Project Structure

```
duckdb_fastapi/
├── .github/
│   └── workflows/
│       └── ci-cd.yml           # GitHub Actions workflow
├── duckdb_fastapi/
│   ├── __init__.py             # Package exports
│   └── main.py                 # Main application logic
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Pytest fixtures
│   ├── test_main.py            # Unit tests
│   └── test_integration.py      # Integration tests
├── datasample/                 # Sample data directory
├── pyproject.toml              # Project configuration
├── README.md                   # This file
└── .gitignore
```

## Development

### Code Quality Tools

- **Black**: Code formatting
- **isort**: Import sorting
- **Flake8**: Linting
- **mypy**: Type checking
- **pytest**: Testing
- **pytest-cov**: Coverage reporting

### Running Quality Checks

```bash
# Format code
black duckdb_fastapi tests

# Sort imports
isort duckdb_fastapi tests

# Lint
flake8 duckdb_fastapi tests

# Type check
mypy duckdb_fastapi

# Test with coverage
pytest --cov=duckdb_fastapi
```

## Requirements

- Python >= 3.8
- FastAPI >= 0.100.0
- Uvicorn >= 0.23.0
- DuckDB >= 0.8.0
- Pydantic >= 2.0.0

## License

MIT

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass
2. Code is formatted with black
3. Coverage is >= 70%
4. Type hints are included

## Support

For issues and questions, please open an issue on GitHub.
