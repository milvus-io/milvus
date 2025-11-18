from pymilvus import MilvusClient, DataType, FunctionType, Function




client = MilvusClient(host="localhost", port=19530)

COLLECTION_NAME = "test_collection2"
NUM_ROW = 2000

import random

def random_floats(n):
    """随机生成n个float数，区间为[-2, 2]"""
    return [random.uniform(-2, 2) for _ in range(n)]

def random_sparse():
    sparse = {}
    dim = random.randint(20, 30)
    for i in range(dim):
        dim_off = random.randint(0, 10000)
        sparse[dim_off] = random.uniform(0, 2)
    return sparse

def random_int8(n):
    return [random.randint(-128, 127) for _ in range(n)]

def random_binary(n):
    return [random.randint(0, 1) for _ in range(n)]

def create_collection_2():
    client.drop_collection(COLLECTION_NAME)
    schema = client.create_schema()
    schema.add_field("sid", DataType.VARCHAR, is_primary=True, max_length=100)
    schema.add_field(field_name="float16_vector", datatype=DataType.FLOAT16_VECTOR, dim=64)
    schema.add_field(field_name="bfloat16_vector", datatype=DataType.BFLOAT16_VECTOR, dim=64)
    schema.add_field(field_name="int8_vector", datatype=DataType.INT8_VECTOR, dim=64)
    schema.add_field(field_name="binary_vector", datatype=DataType.BINARY_VECTOR, dim=64)

    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="float16_vector",
        index_type="AUTOINDEX",
        metric_type="L2",
    )
    index_params.add_index(
        field_name="bfloat16_vector",
        index_type="AUTOINDEX",
        metric_type="L2",
    )
    index_params.add_index(
        field_name="int8_vector",
        index_type="AUTOINDEX",
        metric_type="L2",
    )
    index_params.add_index(
        field_name="binary_vector",
        index_type="AUTOINDEX",
        metric_type="HAMMING",
    )
    client.create_collection(
        collection_name=COLLECTION_NAME,
        schema=schema,
        index_params=index_params
    )

def insert_data_2():
    import numpy as np
    import tensorflow as tf
    datas = []
    for i in range(NUM_ROW):
        data = {
            "sid": f"sid_{i}",
            "float16_vector": np.array(random_floats(64), dtype=np.float16),
            "bfloat16_vector": tf.cast(random_floats(64), dtype=tf.bfloat16).numpy(),
            "int8_vector": np.array(random_int8(64), dtype=np.int8),
            "binary_vector": bytes(np.packbits(random_binary(64), axis=-1).tolist()),
        }
        datas.append(data)
    client.insert(COLLECTION_NAME, datas)

def create_collection():
    schema = client.create_schema()

    # str primiary key, float vec, sparse vec, text field with bm25, arraystruct with vec
    schema.add_field("sid", DataType.VARCHAR, is_primary=True, max_length=100)
    schema.add_field(field_name="float_vector", datatype=DataType.FLOAT_VECTOR, dim=64)
    schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)
    schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=1000, enable_analyzer=True)
    schema.add_field(field_name="sparse", datatype=DataType.SPARSE_FLOAT_VECTOR)

    struct_schema = MilvusClient.create_struct_field_schema()
    struct_schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=64)
    struct_schema.add_field("float_vector2", DataType.FLOAT_VECTOR, dim=64)

    schema.add_field("struct_array", datatype=DataType.ARRAY, element_type=DataType.STRUCT, struct_schema=struct_schema, max_capacity=100)

    bm25_function = Function(
        name="text_bm25_emb", # Function name
        input_field_names=["text"], # Name of the VARCHAR field containing raw text data
        output_field_names=["sparse"], # Name of the SPARSE_FLOAT_VECTOR field reserved to store generated embeddings
        function_type=FunctionType.BM25, # Set to `BM25`
    )

    schema.add_function(bm25_function)


    index_params = client.prepare_index_params()

    index_params.add_index(
        field_name="float_vector",
        index_type="AUTOINDEX",
        metric_type="L2",
    )

    index_params.add_index(
        field_name="sparse_vector",
        index_type="SPARSE_INVERTED_INDEX",
        metric_type="IP",
        params={"inverted_index_algo": "DAAT_MAXSCORE"}, # or "DAAT_WAND" or "TAAT_NAIVE"
    )

    index_params.add_index(
        field_name="sparse",
        index_type="SPARSE_INVERTED_INDEX",
        metric_type="BM25",
        params={
            "inverted_index_algo": "DAAT_MAXSCORE",
            "bm25_k1": 1.2,
            "bm25_b": 0.75
        }

    )

    index_params.add_index(
        field_name="struct_array[float_vector]",
        index_type="AUTOINDEX",
        metric_type="MAX_SIM_COSINE",
    )

    index_params.add_index(
        field_name="struct_array[float_vector2]",
        index_type="AUTOINDEX",
        metric_type="MAX_SIM_COSINE",
    )

    client.create_collection(
        collection_name=COLLECTION_NAME,
        schema=schema,
        index_params=index_params
    )


texts = [
    "information retrieval is a field of study.",
    "information retrieval focuses on finding relevant information in large datasets.",
    "data mining and information retrieval overlap in research.",
    "you can perform full text searches using raw text queries.",
    "Milvus automatically converts your query into a sparse",
    "the sparse vectors generated by the BM25 function are not directly",
    "Full text search is a feature that retrieves documents containing",
    "specific terms or phrases in text datasets, then ranking the results",
    "based on relevance. This feature overcomes semantic search limitations",
    "which might overlook precise terms, ensuring you receive the most accurate",
    "and contextually relevant results. Additionally, it simplifies vector searches",
    "by accepting raw text input, automatically converting your text data into sparse",
    "embeddings without the need to manually generate vector embeddings.",
    "the BM25 function is a powerful tool for full text search.",
]

def random_text():
    return random.choice(texts)


def insert_data():
    datas = []
    for i in range(NUM_ROW):
        data = {
            "sid": f"sid_{i}",
            "float_vector": random_floats(64),
            "sparse_vector": random_sparse(),
            "text": random_text(),
            "struct_array": [
                {
                    "float_vector": random_floats(64),
                    "float_vector2": random_floats(64),
                },
                {
                    "float_vector": random_floats(64),
                    "float_vector2": random_floats(64),
                },
                {
                    "float_vector": random_floats(64),
                    "float_vector2": random_floats(64),
                },
            ]
        }
        datas.append(data)
    client.insert(COLLECTION_NAME, datas)

def search():
    # ret = client.query(
    #     collection_name=COLLECTION_NAME,
    #     filter = "sid == 'sid_0'",
    #     limit = 2
    # )

    ret = client.search(
        collection_name=COLLECTION_NAME,
        ids=["sid_0","sid_1","sid_2","sid_3"],
        anns_field="bfloat16_vector",
        limit = 4
    )

    print(ret)

search()