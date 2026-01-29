#!/usr/bin/env python3
"""Test script for SnapshotRestoreChecker."""

import sys
import time
import threading

sys.path.insert(0, ".")

from pymilvus import connections, DataType, CollectionSchema, FieldSchema
from common.common_func import param_info
from chaos.checker import SnapshotRestoreChecker

# Configure Milvus connection (use snapshot-test instance)
MILVUS_HOST = "10.100.36.164"
MILVUS_PORT = 19530

param_info.param_host = MILVUS_HOST
param_info.param_port = MILVUS_PORT
param_info.param_user = ""
param_info.param_password = ""
param_info.param_token = ""
param_info.param_uri = ""

# Create default connection for MilvusSys
connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)


def create_simple_schema(dim=128):
    """Create a simple schema for testing."""
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=256),
    ]
    return CollectionSchema(fields=fields, description="Simple test schema")


def test_single_run():
    """Test a single snapshot restore cycle."""
    print("=" * 60)
    print("Testing single snapshot restore cycle...")
    print("=" * 60)

    # Use simple schema to speed up testing
    schema = create_simple_schema()
    checker = SnapshotRestoreChecker(schema=schema)
    print(f"Collection: {checker.c_name}")
    print(f"Initial pk_set size: {len(checker.pk_set)}")

    # Run a single task
    res, result = checker.run_task()

    print(f"Result: {result}")
    if not result:
        print(f"Error: {res}")

    print(f"Success: {checker._succ}, Fail: {checker._fail}")
    print(f"Final pk_set size: {len(checker.pk_set)}")

    # Cleanup
    checker.milvus_client.drop_collection(checker.c_name)
    print(f"Dropped collection {checker.c_name}")

    return result


def test_continuous_run(duration=60):
    """Test continuous running for specified duration."""
    print("=" * 60)
    print(f"Testing continuous run for {duration} seconds...")
    print("=" * 60)

    # Use simple schema to speed up testing
    schema = create_simple_schema()
    checker = SnapshotRestoreChecker(schema=schema)
    print(f"Collection: {checker.c_name}")

    # Start keep_running in a separate thread
    thread = threading.Thread(target=checker.keep_running, daemon=True)
    thread.start()

    start_time = time.time()
    while time.time() - start_time < duration:
        print(f"[{int(time.time() - start_time)}s] Success: {checker._succ}, Fail: {checker._fail}, "
              f"pk_set size: {len(checker.pk_set)}")
        time.sleep(10)

    # Save stats before terminate (which calls reset)
    total = checker.total()
    succ = checker._succ
    fail = checker._fail
    succ_rate = checker.succ_rate()
    errors = checker.error_messages.copy()

    # Stop the checker
    checker._keep_running = False
    thread.join(timeout=5)

    print("\n" + "=" * 60)
    print("Final Results:")
    print(f"  Total: {total}")
    print(f"  Success: {succ}")
    print(f"  Fail: {fail}")
    print(f"  Success Rate: {succ_rate:.2%}")
    if errors:
        print(f"  Errors: {errors}")
    print("=" * 60)

    # Cleanup
    checker.milvus_client.drop_collection(checker.c_name)
    print(f"Dropped collection {checker.c_name}")

    return succ_rate > 0


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test SnapshotRestoreChecker")
    parser.add_argument("--mode", choices=["single", "continuous"], default="single",
                        help="Test mode: single run or continuous")
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration for continuous test (seconds)")

    args = parser.parse_args()

    if args.mode == "single":
        success = test_single_run()
    else:
        success = test_continuous_run(args.duration)

    sys.exit(0 if success else 1)
