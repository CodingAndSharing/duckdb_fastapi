[![CI/CD Pipeline](https://github.com/CodingAndSharing/duckdbfastapi/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/CodingAndSharing/duckdbfastapi/actions/workflows/ci-cd.yml) ![Static Badge](https://img.shields.io/badge/version-beta)

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
pip install duckdbfastapi
```

### Development Installation

Easy install:

```sh
pip install duckdbfastapi
```


Or latest development version at github:

```bash
git clone https://github.com/CodingAndSharing/duckdbfastapi.git
cd duckdbfastapi
pip install -e ".[dev]"
```

## Quick Start

### Basic Usage

```python
from duckdbfastapi import run_fastapi

# Run with default datasample
run_fastapi("duckdbfastapi_datasample")

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

- **path_data** (str, required): Path to data directory or `"duckdbfastapi_datasample"`
- **specific_items** (List[str], optional): List of specific files/folders to expose
- **port** (int, optional): Port to run server on (default: 8000)
- **host** (str, optional): Host to bind to (default: "127.0.0.1")

## Supported File Formats

- JSON (`.json`)
- CSV (`.csv`)
- Parquet (`.parquet`)

## Example

```python
from duckdbfastapi import run_fastapi

# Run with custom data. replace path_data value with the folder where you store your data
run_fastapi(
    path_data="./datasample",
    specific_items=[],
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
pytest tests/ --cov=duckdbfastapi --cov-report=html
```

Run specific test file:

```bash
pytest tests/test_main.py -v
```

Generate coverage report:

```bash
pytest --cov=duckdbfastapi --cov-report=html
# Open htmlcov/index.html in browser
```

## CI/CD Pipeline

The project includes a GitHub Actions workflow that:

- ✅ Runs tests on Python 3.9, 3.10, 3.11
- ✅ Generates coverage reports with pytest-cov
- ✅ Performs linting with ruff
- ✅ Checks code formatting with ruff
- ✅ Runs type checking with mypy
- ✅ Builds distribution packages
- ✅ Enforces quality gates (minimum 70% coverage)

## Project Structure

```
duckdbfastapi/
├── .github/
│   └── workflows/
│       ├── ci-cd.yml           # GitHub Actions workflow
│       └── publish.yml         # PyPI publish workflow
├── duckdbfastapi/
│   ├── __init__.py             # Package exports
│   └── main.py                 # Main application logic
├── tests/
│   ├── __init__.py
│   ├── conftest.py             # Pytest fixtures
│   ├── test_main.py            # Unit tests
│   ├── test_integration.py      # Integration tests
│   └── test_*.py               # Additional test modules
├── datasample/                 # Sample data directory
├── pyproject.toml              # Project configuration
├── ruff.toml                   # Ruff configuration
├── README.md                   # This file
└── .gitignore
```

## Development

### Code Quality Tools

- **Ruff**: Linting and formatting
- **mypy**: Type checking
- **pytest**: Testing
- **pytest-cov**: Coverage reporting

### Running Quality Checks

```bash
# Lint with ruff
ruff check duckdbfastapi tests

# Format with ruff
ruff format duckdbfastapi tests

# Type check
mypy duckdbfastapi

# Test with coverage
pytest --cov=duckdbfastapi
```

## Requirements

- Python >= 3.9
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
