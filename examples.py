#!/usr/bin/env python
"""
Example script demonstrating duckdb_fastapi usage.

This script shows different ways to use the duckdb_fastapi package.
"""

from duckdb_fastapi import run_fastapi


def main():
    """Run different examples of duckdb_fastapi."""

    # Example 1: Run with default datasample
    # Uncomment to run
    # print("Starting FastAPI with default datasample...")
    # run_fastapi("duckdb_fastapi_datasample")

    # Example 2: Run with custom data directory
    # Uncomment and update path to run
    # print("Starting FastAPI with custom data directory...")
    # run_fastapi("/path/to/your/data")

    # Example 3: Run with specific files only
    # Uncomment to run
    # print("Starting FastAPI with specific files...")
    # run_fastapi(
    #     "duckdb_fastapi_datasample",
    #     specific_items=["evidence_impc", "evidence_orphanet"]
    # )

    # Example 4: Run on custom host and port
    # Uncomment to run
    # print("Starting FastAPI on 0.0.0.0:9000...")
    # run_fastapi(
    #     "duckdb_fastapi_datasample",
    #     host="0.0.0.0",
    #     port=9000
    # )

    print("Example usage patterns shown above.")
    print("Uncomment the example you want to run.")
    print("\nAvailable endpoints once running:")
    print("  GET / - List all endpoints")
    print("  GET /health - Health check")
    print("  GET /data/{item_name} - Query data for specific file/folder")


if __name__ == "__main__":
    main()
