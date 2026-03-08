from __future__ import annotations

import argparse
import os
import random
import threading
import time

# from dotenv import load_dotenv
from pymilvus import DataType
from pymilvus.milvus_client import IndexParams

from pymilvus_pg import MilvusPGClient as MilvusClient
from pymilvus_pg import logger

# load_dotenv()

# ---------------------------- Default Configuration ---------------------------
DIMENSION = 128  # Vector dimension
INSERT_BATCH_SIZE = 1000
DELETE_BATCH_SIZE = 500
UPSERT_BATCH_SIZE = 300
COLLECTION_NAME_PREFIX = "data_correctness_checker"

# Global primary key counter and thread-safe lock
_global_id: int = 0
_id_lock = threading.Lock()

# Events for controlling thread pause/stop
pause_event = threading.Event()
stop_event = threading.Event()


def _next_id_batch(count: int) -> list[int]:
    """Return a list of consecutive IDs and safely increment the global counter."""
    global _global_id
    with _id_lock:
        start = _global_id
        _global_id += count
    return list(range(start, start + count))


def _generate_data(id_list: list[int], for_upsert: bool = False):
    """Generate records based on the ID list."""
    data = []
    for _id in id_list:
        record = {
            "id": _id,
            "name": f"name_{_id}{'_upserted' if for_upsert else ''}",
            "age": random.randint(18, 60) + (100 if for_upsert else 0),
            "json_field": {"attr1": _id, "attr2": f"val_{_id}"},
            "array_field": [_id, _id + 1, _id + 2, random.randint(0, 100)],
            "embedding": [random.random() for _ in range(DIMENSION)],
        }
        data.append(record)
    return data


def _insert_op(client: MilvusClient, collection: str):
    """Insert operation with exception handling to ensure thread stability."""
    try:
        ids = _next_id_batch(INSERT_BATCH_SIZE)
        client.insert(collection, _generate_data(ids))
        logger.info(f"[INSERT] {len(ids)} rows, start id {ids[0]}")
    except Exception as e:
        logger.error(f"[INSERT] Exception occurred: {e}")
        # Exception is caught to prevent thread exit


def _delete_op(client: MilvusClient, collection: str):
    """Delete operation with exception handling to ensure thread stability."""
    global _global_id
    try:
        # Only delete if there is existing data
        if _global_id == 0:
            return
        # Randomly select a range of ids
        start = random.randint(0, max(1, _global_id - DELETE_BATCH_SIZE))
        ids = list(range(start, start + DELETE_BATCH_SIZE))
        client.delete(collection, ids=ids)
        logger.info(f"[DELETE] {len(ids)} rows, start id {start}")
    except Exception as e:
        logger.error(f"[DELETE] Exception occurred: {e}")
        # Exception is caught to prevent thread exit


def _upsert_op(client: MilvusClient, collection: str):
    """Upsert operation with exception handling to ensure thread stability."""
    global _global_id
    try:
        if _global_id == 0:
            return
        start = random.randint(0, max(1, _global_id - UPSERT_BATCH_SIZE))
        ids = list(range(start, start + UPSERT_BATCH_SIZE))
        client.upsert(collection, _generate_data(ids, for_upsert=True))
        logger.info(f"[UPSERT] {len(ids)} rows, start id {start}")
    except Exception as e:
        logger.error(f"[UPSERT] Exception occurred: {e}")
        # Exception is caught to prevent thread exit


OPERATIONS = [_insert_op, _delete_op, _upsert_op]


def worker_loop(client: MilvusClient, collection: str):
    """Worker thread: Loop through random write operations."""
    while not stop_event.is_set():
        if pause_event.is_set():
            time.sleep(0.1)
            continue
        op = random.choice(OPERATIONS)
        try:
            op(client, collection)
        except Exception:  # noqa: BLE001
            logger.exception(f"Error during {op.__name__}")
        # Small sleep to reduce pressure
        time.sleep(random.uniform(0.05, 0.2))


def create_collection(client: MilvusClient, name: str):
    if client.has_collection(name):
        logger.warning(f"Collection {name} already exists, dropping")
        client.drop_collection(name)

    schema = client.create_schema()
    schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
    schema.add_field("name", DataType.VARCHAR, max_length=256)
    schema.add_field("age", DataType.INT64)
    schema.add_field("json_field", DataType.JSON)
    schema.add_field("array_field", DataType.ARRAY, element_type=DataType.INT64, max_capacity=20)
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=DIMENSION)
    client.create_collection(name, schema)

    index_params = IndexParams()
    index_params.add_index("embedding", metric_type="L2", index_type="IVF_FLAT", params={"nlist": 128})
    client.create_index(name, index_params)
    client.load_collection(name)
    logger.info(f"Collection {name} created and loaded")


def main():
    parser = argparse.ArgumentParser(description="Multi-thread write / verify checker for MilvusPGClient")
    parser.add_argument("--threads", type=int, default=4, help="Writer thread count (default 4)")
    parser.add_argument(
        "--compare_interval", type=int, default=60, help="Seconds between consistency checks (default 60)"
    )
    parser.add_argument("--duration", type=int, default=0, help="Total run time in seconds (0 means run indefinitely)")
    parser.add_argument(
        "--uri", type=str, default=os.getenv("MILVUS_URI", "http://localhost:19530"), help="Milvus server URI"
    )
    parser.add_argument("--token", type=str, default=os.getenv("MILVUS_TOKEN", ""), help="Milvus auth token")
    parser.add_argument(
        "--pg_conn",
        type=str,
        default=os.getenv("PG_CONN", "postgresql://postgres:admin@localhost:5432/default"),
        help="PostgreSQL DSN",
    )
    args = parser.parse_args()

    start_time = time.time()

    client = MilvusClient(
        uri=args.uri,
        token=args.token,
        pg_conn_str=args.pg_conn,
        ignore_vector=True
    )
    collection_name = f"{COLLECTION_NAME_PREFIX}_{int(time.time())}"
    logger.info(f"Using collection: {collection_name}")
    create_collection(client, collection_name)

    # Start writer threads
    threads: list[threading.Thread] = []
    for i in range(args.threads):
        t = threading.Thread(target=worker_loop, name=f"Writer-{i}", args=(client, collection_name), daemon=True)
        t.start()
        threads.append(t)

    last_compare = time.time()
    try:
        while True:
            time.sleep(1)
            if time.time() - last_compare >= args.compare_interval:
                logger.info("Pausing writers for entity compare …")
                pause_event.set()
                # Wait for in-flight operations to complete
                time.sleep(2)
                try:
                    client.entity_compare(collection_name)
                except Exception:
                    logger.exception("Error during entity_compare")
                last_compare = time.time()
                pause_event.clear()
                logger.info("Writers resumed")
            # Check duration
            if args.duration > 0 and time.time() - start_time >= args.duration:
                logger.info(f"Duration reached ({args.duration}s), stopping …")
                break
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, stopping …")
        stop_event.set()
        for t in threads:
            t.join(timeout=5)
    finally:
        logger.info("Finished. Final compare …")
        try:
            client.entity_compare(collection_name)
        except Exception:
            logger.exception("Final entity_compare failed")


if __name__ == "__main__":
    main()
