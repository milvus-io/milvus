#!/usr/bin/env python3
"""
Create an Iceberg table with test data on MinIO (local S3-compatible storage).
This data is used by the Iceberg external table E2E test.

Schema: pk (int64), label (string), vector (list<float32>[128])

Usage:
    python3 create_iceberg_table.py [--endpoint http://localhost:9000] [--num-rows 1000]
"""

import argparse
import json
import os
import random

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    FloatType,
    ListType,
    LongType,
    NestedField,
    StringType,
)


def create_test_data(num_rows: int, dim: int = 128) -> pa.Table:
    """Create test data as a PyArrow table."""
    random.seed(42)

    pks = list(range(num_rows))
    labels = [f"label_{i}" for i in range(num_rows)]
    vectors = [[random.uniform(-1.0, 1.0) for _ in range(dim)] for _ in range(num_rows)]

    schema = pa.schema(
        [
            pa.field("pk", pa.int64()),
            pa.field("label", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), dim)),
        ]
    )

    return pa.table(
        {
            "pk": pa.array(pks, type=pa.int64()),
            "label": pa.array(labels, type=pa.string()),
            "vector": pa.array(vectors, type=pa.list_(pa.float32(), dim)),
        },
        schema=schema,
    )


def main():
    parser = argparse.ArgumentParser(description="Create Iceberg test table on MinIO")
    parser.add_argument("--endpoint", default="http://localhost:9000", help="MinIO endpoint")
    parser.add_argument("--access-key", default="minioadmin", help="MinIO access key")
    parser.add_argument("--secret-key", default="minioadmin", help="MinIO secret key")
    parser.add_argument("--bucket", default="a-bucket", help="MinIO bucket name")
    parser.add_argument("--table-path", default="iceberg-test/e2e_test_table", help="Table path in bucket")
    parser.add_argument("--num-rows", type=int, default=1000, help="Number of rows to generate")
    parser.add_argument("--dim", type=int, default=128, help="Vector dimension")
    parser.add_argument("--output", default="", help="Output JSON path (default: testdata/iceberg_table_info.json)")
    args = parser.parse_args()

    warehouse_path = f"s3://{args.bucket}/{args.table_path}"

    # Create catalog using SQL (SQLite in-memory for one-time use)
    catalog = SqlCatalog(
        "test_catalog",
        **{
            "uri": "sqlite:///:memory:",
            "s3.endpoint": args.endpoint,
            "s3.access-key-id": args.access_key,
            "s3.secret-access-key": args.secret_key,
            "s3.region": "us-east-1",
            "warehouse": warehouse_path,
        },
    )

    # Create namespace
    try:
        catalog.create_namespace("default")
    except Exception:
        pass  # namespace may already exist

    # Define Iceberg schema
    iceberg_schema = Schema(
        NestedField(1, "pk", LongType(), required=False),
        NestedField(2, "label", StringType(), required=False),
        NestedField(
            3,
            "vector",
            ListType(4, FloatType(), element_required=False),
            required=False,
        ),
    )

    # Create table
    table_name = "default.e2e_test"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass

    table = catalog.create_table(table_name, schema=iceberg_schema)

    # Generate and append data
    data = create_test_data(args.num_rows, args.dim)
    table.append(data)

    # Refresh to get latest snapshot
    table = catalog.load_table(table_name)
    snapshot = table.current_snapshot()

    result = {
        "table_location": table.location(),
        "metadata_location": table.metadata_location,
        "snapshot_id": snapshot.snapshot_id,
        "num_rows": args.num_rows,
        "dim": args.dim,
    }

    print(json.dumps(result, indent=2))

    # Write to file for the E2E test to consume
    output_path = args.output if args.output else os.path.join(os.path.dirname(__file__), "iceberg_table_info.json")
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nTable info written to: {output_path}")


if __name__ == "__main__":
    main()
