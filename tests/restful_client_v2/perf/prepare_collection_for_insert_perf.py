from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection
)
from loguru import logger
import argparse


def main(host="127.0.0.1"):
    connections.connect(
        host=host,
        port=19530,
    )
    collection_name = "test_restful_insert_perf"
    if collection_name in list_collections():
        logger.info(f"collection {collection_name} exists, drop it")
        Collection(name=collection_name).drop()
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="doc_id", dtype=DataType.INT64),
        FieldSchema(name="text_no_index", dtype=DataType.VARCHAR, max_length=10000),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=10000),
        FieldSchema(name="text_emb", dtype=DataType.FLOAT_VECTOR, dim=768),
        FieldSchema(name="image_emb", dtype=DataType.FLOAT_VECTOR, dim=768)
    ]
    schema = CollectionSchema(fields=fields, description="test collection")
    collection = Collection(name=collection_name, schema=schema)
    logger.info(f"collection {collection_name} created, schema {collection.schema}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="prepare data for perf test")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="milvus server host")
    args = parser.parse_args()
    main(host=args.host)
