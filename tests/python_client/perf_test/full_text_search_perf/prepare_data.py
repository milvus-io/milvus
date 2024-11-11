from typing import Union
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection, BulkInsertState, utility, Function, FunctionType
)

import time
import argparse
from loguru import logger


def prepare_data(host="127.0.0.1", port=19530, data_size=1000_000):
    data_size = int(data_size)
    b_z = 100_000
    file_num = data_size // b_z
    batch_files = [f"wikipedia-22-12-en-text/parquet-train-{i:05d}-of-00252.parquet" for i in range(file_num)]
    connections.connect(
        host=host,
        port=port,
    )
    collection_name = "test_full_text_search_perf"

    if collection_name in list_collections():
        logger.info(f"collection {collection_name} exists, drop it")
        Collection(name=collection_name).drop()

    analyzer_params = {
        "tokenizer": "standard",
    }
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=25536,
                    enable_analyzer=True, analyzer_params=analyzer_params),
        FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR),
    ]
    schema = CollectionSchema(fields=fields, description="beir test collection")
    bm25_function = Function(
        name="text_bm25_emb",
        function_type=FunctionType.BM25,
        input_field_names=["text"],
        output_field_names=["sparse"],
        params={},
    )
    schema.add_function(bm25_function)
    logger.info(schema)
    collection = Collection(name=collection_name, schema=schema)
    logger.info(f"collection {collection_name} created")
    task_ids = []
    for files in batch_files:
        task_id = utility.do_bulk_insert(collection_name=collection_name, files=[files])
        task_ids.append(task_id)
        logger.info(f"Create a bulk inert task, task id: {task_id}")

    while len(task_ids) > 0:
        logger.info("Wait 1 second to check bulk insert tasks state...")
        time.sleep(1)
        for id in task_ids:
            state = utility.get_bulk_insert_state(task_id=id)
            if state.state == BulkInsertState.ImportFailed or state.state == BulkInsertState.ImportFailedAndCleaned:
                logger.info(f"The task {state.task_id} failed, reason: {state.failed_reason}")
                task_ids.remove(id)
            elif state.state == BulkInsertState.ImportCompleted:
                logger.info(f"The task {state.task_id} completed with state {state}")
                task_ids.remove(id)
    t0 = time.time()
    collection.create_index(
        "sparse",
        {
            "index_type": "SPARSE_INVERTED_INDEX",
            "metric_type": "BM25",
            "params": {
                "bm25_k1": 1.5,
                "bm25_b": 0.75,
            }
        }
    )
    index_list = utility.list_indexes(collection_name=collection_name)
    for index_name in index_list:
        progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
        while progress["pending_index_rows"] > 0:
            time.sleep(30)
            progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
            logger.info(f"collection {collection_name} index {index_name} progress: {progress}")
        logger.info(f"collection {collection_name} index {index_name} progress: {progress}")
    tt = time.time() - t0
    logger.info(f"collection {collection_name} index created, time: {tt}")
    t0 = time.time()
    collection.load()
    tt = time.time() - t0
    logger.info(f"collection {collection_name} loaded, time: {tt}")
    num = collection.num_entities
    logger.info(f"collection {collection_name} loaded, num_entities: {num}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="prepare data for perf test")
    parser.add_argument("--host", type=str, default="10.104.19.96")
    parser.add_argument("--port", type=int, default=19530)
    parser.add_argument("--data_size", type=str, default="1000_000")
    args = parser.parse_args()
    prepare_data(host=args.host, port=args.port, data_size=args.data_size)
