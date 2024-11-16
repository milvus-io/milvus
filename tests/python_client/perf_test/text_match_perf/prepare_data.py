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
import multiprocessing as mp

def clean_and_reinsert_tokens(df, token_probabilities):
    text_columns = ['word', 'sentence', 'paragraph', 'text']

    # Clean tokens
    for col in text_columns:
        df[col] = df[col].apply(lambda x: clean_tokens(x, token_probabilities.keys()))

    # Reinsert tokens
    for col in text_columns:
        df[col] = df[col].apply(lambda x: reinsert_tokens(x, token_probabilities))

    return df

def clean_tokens(text, tokens):
    for token in tokens:
        text = text.replace(token, '')
    return text

def reinsert_tokens(text, token_probabilities):
    words = text.split()
    insert_position = -1
    for token, prob in token_probabilities.items():
        insert_position += 1
        if random.random() < prob:
            words.insert(insert_position, token)
    return ' '.join(words)


def generate_and_process_batch(e, batch_size, dim, token_probabilities):
    fake_en = Faker('en_US')
    data = [{
            "id": i,
            "word": fake_en.word().lower(),
            "sentence": fake_en.sentence().lower(),
            "paragraph": fake_en.paragraph().lower(),
            "text": fake_en.text().lower(),
            "emb": [random.random() for _ in range(dim)]
        } for i in range(e*batch_size, (e+1)*batch_size)
    ]
    df = pd.DataFrame(data)
    df = clean_and_reinsert_tokens(df, token_probabilities)
    logger.info(f"dataframe\n {df}")
    df.to_parquet(f"./train_data/train-{e}.parquet")
    logger.info(f"progress: {e+1}")
    return f"./train_data/train-{e}.parquet"



def prepare_data(host="127.0.0.1", port=19530, minio_host="127.0.0.1", bucket_name="milvus-bucket", data_size=1000000):
    dim = 32
    # create dataset
    # clean all parquet
    os.makedirs("./train_data", exist_ok=True)
    files = glob.glob("./train_data/train*.parquet")
    for file in files:
        try:
            os.remove(file)
        except Exception as e:
            logger.info(f"delete file failed with error {e}")
    batch_size = 100000
    epoch = data_size // batch_size
    token_probabilities={
        "hello": 0.1,
        "milvus": 0.01,
        "vector": 0.001,
        "database": 0.0001,
    }
    # Prepare arguments for multiprocessing
    args_list = [(e, batch_size, dim, token_probabilities) for e in range(epoch)]

    # Use multiprocessing to generate and process data
    cpu_count = mp.cpu_count()
    logger.info(f"cpu count {cpu_count}")
    cpu_use_count = min(cpu_count, 8)
    t0 = time.time()
    with mp.Pool(processes=cpu_use_count) as pool:
        results = pool.starmap(generate_and_process_batch, args_list)
    logger.info(f"files {results}")
    tt = time.time() - t0
    batch_files = glob.glob("./train_data/train*.parquet")
    logger.info(f"files {batch_files}")
    logger.info(f"generate data time cost: {tt} s")

    connections.connect(
        host=host,
        port=port,
    )
    collection_name = "test_text_match_perf"

    if collection_name in list_collections():
        logger.info(f"collection {collection_name} exists, drop it")
        Collection(name=collection_name).drop()

    analyzer_params = {
        "tokenizer": "standard",
    }
    fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="word", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, enable_analyzer=True, analyzer_params=analyzer_params),
            FieldSchema(name="sentence", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, enable_analyzer=True, analyzer_params=analyzer_params),
            FieldSchema(name="paragraph", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, enable_analyzer=True, analyzer_params=analyzer_params),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535, enable_match=True, enable_analyzer=True, analyzer_params=analyzer_params),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim)
        ]
    schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=True)
    logger.info(schema)
    collection = Collection(name=collection_name, schema=schema)
    index_params = {"metric_type": "COSINE", "index_type": "HNSW", "params": {"M": 16, "efConstruction": 500}}
    logger.info(f"collection {collection_name} created")
    # copy file to minio
    client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
    for file in batch_files:
        f_name = file.split("/")[-1]
        client.fput_object(f"{bucket_name}", f_name, file)
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
    parser.add_argument("--host", type=str, default="10.104.1.205")
    parser.add_argument("--minio_host", type=str, default="10.104.30.236")
    parser.add_argument("--bucket_name", type=str, default="milvus-bucket")
    parser.add_argument("--port", type=int, default=19530)
    parser.add_argument("--data_size", type=int, default=1000_000)
    args = parser.parse_args()
    prepare_data(host=args.host, port=args.port, minio_host=args.minio_host, data_size=args.data_size, bucket_name=args.bucket_name)
