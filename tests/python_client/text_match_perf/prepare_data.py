import pandas as pd
from minio import Minio
import glob
import os
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection, BulkInsertState, utility
)

import time
import argparse
from loguru import logger
from faker import Faker
import random
def prepare_data(host="127.0.0.1", port=19530, minio_host="127.0.0.1", bucket_name="milvus-bucket", data_size=1000000):

    connections.connect(
        host=host,
        port=port,
    )
    collection_name = "test_text_match_perf"

    if collection_name in list_collections():
        logger.info(f"collection {collection_name} exists, drop it")
        Collection(name=collection_name).drop()
    dim = 128
    analyzer_params = {
        "tokenizer": "default",
    }
    fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="word", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="sentence", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="paragraph", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, analyzer_params=analyzer_params),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim)
        ]
    schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True)
    logger.info(schema)
    collection = Collection(name=collection_name, schema=schema)
    index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 48, "efConstruction": 500}}
    logger.info(f"collection {collection_name} created")
    # create dataset
    # clean all parquet
    files = glob.glob("./train*.parquet")
    # for file in files:
    #     try:
    #         os.remove(file)
    #     except Exception as e:
    #         logger.info(f"delete file failed with error {e}")
    # batch_size = 100000
    # epoch = data_size // batch_size
    # fake_en = Faker('en_US')
    # for e in range(epoch):
    #     data = [{
    #             "id": i,
    #             "word": fake_en.word(),
    #             "sentence": fake_en.sentence(),
    #             "paragraph": fake_en.paragraph(),
    #             "text": fake_en.text(),
    #             "emb": [random.random() for _ in range(dim)]
    #         } for i in range(e*batch_size, (e+1)*batch_size)
    #     ]
    #     df = pd.DataFrame(data)
    #     df.to_parquet(f"./train-{e}.parquet")
    #     logger.info(f"progress: {e+1}/{epoch}")
    batch_files = glob.glob("./train*.parquet")
    # logger.info(f"files {batch_files}")
    # # copy file to minio
    # client = Minio(
    #         f"{minio_host}:9000",
    #         access_key="minioadmin",
    #         secret_key="minioadmin",
    #         secure=False,
    #     )
    # for file in batch_files:
    #     f_name = file.split("/")[-1]
    #     client.fput_object(f"{bucket_name}", f_name, file)
    #     logger.info(f"upload file {file}")
    batch_files = [file.split("/")[-1] for file in batch_files]
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

    collection.create_index("emb", index_params=index_params)
    index_list = utility.list_indexes(collection_name=collection_name)
    for index_name in index_list:
        progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
        while progress["pending_index_rows"] > 0:
            time.sleep(30)
            progress = utility.index_building_progress(collection_name=collection_name, index_name=index_name)
            logger.info(f"collection {collection_name} index {index_name} progress: {progress}")
        logger.info(f"collection {collection_name} index {index_name} progress: {progress}")
    collection.load()
    num = collection.num_entities
    logger.info(f"collection {collection_name} loaded, num_entities: {num}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="prepare data for perf test")
    parser.add_argument("--host", type=str, default="10.104.1.10")
    parser.add_argument("--minio_host", type=str, default="10.104.23.30")
    parser.add_argument("--bucket_name", type=str, default="text-match-test-v12")
    parser.add_argument("--port", type=int, default=19530)
    parser.add_argument("--data_size", type=int, default=1000000)
    args = parser.parse_args()
    prepare_data(host=args.host, port=args.port, minio_host=args.minio_host, data_size=args.data_size, bucket_name=args.bucket_name)
