from minio import Minio
import glob
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection, BulkInsertState, utility
)

import time
import argparse
from loguru import logger
import faker
fake = faker.Faker()


def prepare_data(host="127.0.0.1", port=19530):

    connections.connect(
        host=host,
        port=port,
    )
    collection_name = "test_restful_perf"
    collection = Collection(name=collection_name)
    collection.release()

    for f in ["contains", "contains_any", "contains_all", "equals"]:
        collection.create_index(f, {"index_type": "INVERTED"})
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
    parser.add_argument("--host", type=str, default="10.104.15.106")
    parser.add_argument("--port", type=int, default=19530)
    args = parser.parse_args()
    prepare_data(host=args.host, port=args.port)
