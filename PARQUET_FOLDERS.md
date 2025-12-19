# Parquet Folder Handling Enhancement

## Overview
Added intelligent handling for folders containing multiple parquet files (treated as chunks of the same table).

## Changes Made

### 1. New Helper Function: `_has_parquet_files()`
- Detects if a folder contains any `.parquet` files
- Returns `True` if parquet files are found, `False` otherwise

### 2. Enhanced `_create_endpoints()` Function
Updated to differentiate between:
- **Parquet folders**: Folders containing `.parquet` files
- **Regular folders**: Folders without parquet files

### 3. Parquet Folder Data Endpoint
- **Route**: `/data/{folder_name}`
- **Behavior**: Reads all parquet files in the folder as a single combined dataset
- **DuckDB Query**: Uses glob pattern `folder/*.parquet` to read all files
- **Response**: Returns paginated data with columns and pagination metadata

```json
{
  "data": [...],
  "columns": ["col1", "col2", "col3"],
  "count": 5,
  "pagination": {
    "page": 1,
    "page_size": 5,
    "total": 1000,
    "has_next": true
  }
}
```

### 4. Parquet Folder Schema Endpoint (Optional)
- **Route**: `/data/{folder_name}_columnnames` (if `include_schema_cols=True`)
- **Behavior**: Returns column information from all parquet files in folder
- **Features**:
  - Column names and data types
  - Example values from first row
  - Paginated column listing (default: 5 columns per page)

```json
{
  "columns": [
    {
      "column_name": "id",
      "data_type": "int64",
      "example_value": "1"
    },
    {
      "column_name": "name",
      "data_type": "string",
      "example_value": "Alice"
    }
  ],
  "count": 2,
  "pagination": {
    "page": 1,
    "page_size": 5,
    "total": 3,
    "has_next": false
  }
}
```

## Usage Example

```python
from duckdbfastapi import run_fastapi

# Folder structure:
# data/
#   ├── chunk_1.parquet
#   ├── chunk_2.parquet
#   └── chunk_3.parquet

# Run the application
run_fastapi("./data")

# Available endpoints:
# GET /data/data              # Combined data from all parquets
# GET /data/data_columnnames  # Schema info (if include_schema_cols=True)
```

## Query Parameters

Both endpoints support:
- `page` (default: 1) - Page number for pagination
- `page_size` (default: 5, max: 1000) - Items per page

### Examples:
```bash
# Get first 5 rows
GET /data/data

# Get second page with 10 rows per page
GET /data/data?page=2&page_size=10

# Get schema info with 3 columns per page
GET /data/data_columnnames?page_size=3
```

## Test Coverage

Added comprehensive test suite: `tests/test_parquet_folders.py`

**Test Classes:**
1. `TestParquetFolderDetection` - Folder detection logic
2. `TestParquetFolderEndpoints` - Endpoint creation and functionality
3. `TestParquetFolderErrorHandling` - Error scenarios
4. `TestParquetFolderWithDifferentDataTypes` - Type handling

**New Tests**: 14 tests
**Total Tests**: 142 (14 new + 128 existing)
**Coverage**: 91% maintained

## Key Implementation Details

### DuckDB Glob Pattern
```sql
-- Read all parquet files in a folder
SELECT * FROM '/path/to/folder/*.parquet' LIMIT 5 OFFSET 0
```

### Endpoint Generation Logic
```python
if item.is_dir():
    if _has_parquet_files(item):
        # Create parquet folder data endpoint
        # Create parquet folder schema endpoint (if include_schema_cols=True)
    else:
        # Create regular directory listing endpoint
```

### Schema Extraction
For parquet folders, schema is extracted by:
1. Executing a query on the glob pattern with `LIMIT 1`
2. Iterating through `conn.description` for column metadata
3. Fetching example values from the first row

## Backward Compatibility

- ✅ Existing file-based endpoints unchanged
- ✅ Regular folders (without parquets) still return directory listings
- ✅ `include_schema_cols` parameter controls schema endpoint creation
- ✅ All 128 existing tests continue to pass

## Benefits

1. **Chunk Handling**: Transparently combines parquet chunks as a single table
2. **Schema Discovery**: Get column info across all chunks with one endpoint
3. **Efficient Querying**: DuckDB handles file concatenation natively
4. **Flexible Pagination**: Paginate large combined datasets
5. **Consistent API**: Same endpoint patterns as individual files
