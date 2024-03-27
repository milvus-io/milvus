import random
from pymilvus import (
    connections, list_collections,
    FieldSchema, CollectionSchema, DataType,
    Collection
)
import argparse
from loguru import logger
import faker

fake = faker.Faker()


def prepare_data(host="127.0.0.1", port=19530, data_size=1000000):

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
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=10000),
        FieldSchema(name="text_emb", dtype=DataType.FLOAT_VECTOR, dim=768),
        FieldSchema(name="image_emb", dtype=DataType.FLOAT_VECTOR, dim=768)
    ]
    schema = CollectionSchema(fields=fields, description="test collection")
    collection = Collection(name=collection_name, schema=schema)
    index_params = {"metric_type": "L2", "index_type": "HNSW", "params": {"M": 48, "efConstruction": 500}}

    logger.info(f"collection {collection_name} created")
    batch_size = 5000
    batch = data_size // batch_size
    for i in range(batch):
        prefix = str(i%10)
        data = [
            [i for i in range(i * batch_size, (i + 1) * batch_size)],
            [prefix+fake.text(max_nb_chars=1000) for i in range(i * batch_size, (i + 1) * batch_size)],
            [[random.random() for _ in range(768)] for _ in range(batch_size)],
            [[random.random() for _ in range(768)] for _ in range(batch_size)]
        ]
        collection.insert(data)
        logger.info(f"inserted {(i+1)*batch_size} vectors")
    logger.info(f"inserted {data_size} vectors")
    collection.create_index("text_emb", index_params=index_params)
    collection.create_index("image_emb", index_params=index_params)
    collection.create_index("text", index_params={"index_type": "INVERTED"})
    collection.load()
    num = collection.num_entities
    logger.info(f"collection {collection_name} loaded, num_entities: {num}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="prepare data for perf test")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19530)
    parser.add_argument("--data_size", type=int, default=1000000)
    args = parser.parse_args()
    prepare_data(host=args.host, port=args.port, data_size=args.data_size)
