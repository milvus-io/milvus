import random
import pandas as pd
import glob
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection, RemoteBulkWriter, BulkInsertState, BulkFileType, utility
)
import time
import argparse
from loguru import logger
import faker

fake = faker.Faker()


def prepare_data(host="127.0.0.1", port=19530, minio_host="127.0.0.1"):

    connections.connect(
        host=host,
        port=port,
    )

    collection_name = "test_restful_perf"
    if collection_name in list_collections():
        logger.info(f"collection {collection_name} exists, drop it")
        Collection(name=collection_name).drop()
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="scalar_3", dtype=DataType.INT64),
        FieldSchema(name="scalar_6", dtype=DataType.INT64),
        FieldSchema(name="scalar_9", dtype=DataType.INT64),
        FieldSchema(name="scalar_12", dtype=DataType.INT64),
        FieldSchema(name="scalar_5_linear", dtype=DataType.INT64),
        FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=768)
    ]
    schema = CollectionSchema(fields=fields, description="test collection")
    collection = Collection(name=collection_name, schema=schema)
    index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 48, "efConstruction": 500}}
    logger.info(f"collection {collection_name} created")

    batch_files = glob.glob("data/train*.parquet")
    # copy data to minio






    with RemoteBulkWriter(
            schema=schema,
            file_type=BulkFileType.NUMPY,
            remote_path="bulk_data",
            connect_param=RemoteBulkWriter.S3ConnectParam(
                endpoint=f"{minio_host}:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                bucket_name="milvus-bucket"
            )
    ) as remote_writer:
        for file in batch_files:
            df = pd.read_parquet(file)

            for i in range(len(df)):
                data = df.iloc[i]
                columns = df.columns
                row = {
                    c: data[c] for c in columns
                }
                remote_writer.append_row(row)
            remote_writer.commit()
        batch_files = remote_writer.batch_files
    task_ids = []
    for files in batch_files:
        task_id = utility.do_bulk_insert(collection_name=collection_name, files=files)
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

    collection.create_index("text_emb", index_params=index_params)
    collection.create_index("image_emb", index_params=index_params)
    collection.create_index("text", index_params={"index_type": "INVERTED"})
    collection.create_index("doc_id", index_params={"index_type": "INVERTED"})
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
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--minio_host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19530)
    args = parser.parse_args()
    prepare_data(host=args.host, port=args.port, minio_host=args.minio_host)
