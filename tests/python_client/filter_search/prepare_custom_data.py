import random
from minio import Minio
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection, BulkInsertState, utility
)
import pandas as pd
import numpy as np
import time
import os
import argparse
from loguru import logger
import faker
import glob
fake = faker.Faker()


def prepare_data(host="127.0.0.1", port=19530, minio_host="127.0.0.1", data_size=1000000, partition_key="scalar_3", insert_mode="import", data_dir=".", bucket_name="milvus-bucket"):

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
        FieldSchema(name="scalar_3", dtype=DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_3")),
        FieldSchema(name="scalar_6", dtype=DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_6")),
        FieldSchema(name="scalar_9", dtype=DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_9")),
        FieldSchema(name="scalar_12", dtype=DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_12")),
        FieldSchema(name="scalar_5_linear", dtype=DataType.VARCHAR, max_length=1000, is_partition_key=bool(partition_key == "scalar_5_linear")),
        FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=768)
    ]
    schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True, num_partitions=1)
    collection = Collection(name=collection_name, schema=schema, num_partitions=1)
    logger.info(f"collection {collection_name} created: {collection.describe()}")
    index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 30, "efConstruction": 360}}
    logger.info(f"collection {collection_name} created")
    test_data_size = 1000
    test_emb_matrix = pd.read_parquet(f"{data_dir}/test.parquet")["emb"].values
    test_emb_matrix = test_emb_matrix[:test_data_size]

    train_emb_matrix = pd.read_parquet(f"{data_dir}/train-00-of-10.parquet")["emb"].values
    if data_size > len(train_emb_matrix):
        train_emb_matrix = np.concatenate([train_emb_matrix] * (data_size // len(train_emb_matrix) + 1), axis=0)
    train_emb_matrix = train_emb_matrix[:data_size]
    #
    # # generate data
    # np.random.seed(19530)
    # train_emb_file = 'train_emb_matrix.npy'
    # test_emb_file = 'test_emb_matrix.npy'
    # test_data_size = 1000
    # if not os.path.exists(train_emb_file):
    #     train_emb_matrix = np.random.random((data_size, 768))
    #     np.save(train_emb_file, train_emb_matrix)
    # else:
    #     train_emb_matrix = np.load(train_emb_file)
    #
    # if not os.path.exists(test_emb_file):
    #     test_emb_matrix = np.random.random((test_data_size, 768))
    #     np.save(test_emb_file, test_emb_matrix)
    # else:
    #     test_emb_matrix = np.load(test_emb_file)

    # create test data
    t0 = time.time()

    data = {
        "id": [i for i in range(test_data_size)],
        "emb": test_emb_matrix.tolist()
    }
    df = pd.DataFrame(data)
    logger.info(f"generate test data {test_data_size} cost time {time.time() - t0}")
    df.to_parquet(f"{data_dir}/test.parquet")
    logger.info(f"test data {df.head()}")
    t0 = time.time()
    batch_size = 1000000
    epoch = data_size // batch_size
    remainder = data_size % batch_size
    for i in range(epoch+1):
        if i == epoch:
            if remainder == 0:
                break
            batch_size = remainder
        start_idx = i * batch_size
        end_idx = (i+1) * batch_size
        data = {
            "id": [i for i in range(start_idx, end_idx)],
            "scalar_3": [str(i%3) for i in range(start_idx, end_idx)],
            "scalar_6": [str(i%6) for i in range(start_idx, end_idx)],
            "scalar_9": [str(i%9) for i in range(start_idx, end_idx)],
            "scalar_12": [str(i%12) for i in range(start_idx, end_idx)],
            "scalar_5_linear": [str(i%5) for i in range(start_idx, end_idx)],
            "emb": train_emb_matrix[start_idx:end_idx].tolist()
        }
        df = pd.DataFrame(data)
        df.to_parquet(f"{data_dir}/train_{i}.parquet")
    logger.info(f"train data {df.head()}")
    logger.info(f"generate data {data_size} cost time {time.time() - t0}")
    batch_files = glob.glob(f"{data_dir}/train_*.parquet")
    logger.info(f"files {batch_files}")
    if insert_mode == "import":
        # copy file to minio
        client = Minio(
                f"{minio_host}:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False,
        )
        for file in batch_files:
            f_name = file.split("/")[-1]
            client.fput_object(bucket_name, f_name, file)
            logger.info(f"upload file {file}")
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
    elif insert_mode == "insert":
        for file in batch_files:
            df = pd.read_parquet(file)
            batch_size = 5000
            for i in range(0, len(df), batch_size):
                data = df.iloc[i:i + batch_size]
                # data = data.to_dict(orient="records")
                t0 = time.time()
                collection.insert(data)
                logger.info(f"insert {len(data)} cost time {time.time() - t0}")
    else:
        raise ValueError(f"insert_mode {insert_mode} not supported")
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
    parser.add_argument("--host", type=str, default="10.104.5.119")
    parser.add_argument("--minio_host", type=str, default="10.104.32.34")
    parser.add_argument("--port", type=int, default=19530)
    parser.add_argument("--data_size", type=int, default=100000)
    parser.add_argument("--partition_key", type=str, default="scalar_3")
    parser.add_argument("--insert_mode", type=str, default="insert")
    parser.add_argument("--bucket_name", type=str, default="milvus-bucket")
    parser.add_argument("--data_dir", type=str, default=".")
    args = parser.parse_args()
    prepare_data(host=args.host, port=args.port, minio_host=args.minio_host, data_size=args.data_size, partition_key=args.partition_key, insert_mode=args.insert_mode, data_dir=args.data_dir, bucket_name=args.bucket_name)
