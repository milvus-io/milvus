# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.


import random
import numpy as np
import time
import argparse
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection,
    AnnSearchRequest, RRFRanker
)
import tensorflow as tf

TIMEOUT = 120


def gen_bf16_vectors(num, dim):
    raw_vectors = []
    bf16_vectors = []
    for _ in range(num):
        raw_vector = [random.random() for _ in range(dim)]
        raw_vectors.append(raw_vector)
        bf16_vector = tf.constant(raw_vector, dtype=tf.bfloat16)
        bf16_vectors.append(bytes(bf16_vector))
    return raw_vectors, bf16_vectors


def hello_milvus(host="127.0.0.1"):
    import time
    # create connection
    connections.connect(host=host, port="19530")

    # create collection
    dim = 2
    default_fields = [
        FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="float", dtype=DataType.FLOAT),
        FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=65535),
        # FieldSchema(name="array_int", dtype=DataType.ARRAY, element_type=DataType.INT64, max_capacity=200),
        FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
        # FieldSchema(name="float16_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
        FieldSchema(name="brain_float_vector", dtype=DataType.BFLOAT16_VECTOR, dim=dim),
    ]
    default_schema = CollectionSchema(fields=default_fields, description="test collection")

    print(f"\nCreate collection...")
    collection = Collection(name="hello_milvus_1118", schema=default_schema)

    #  insert data
    nb = 3
    float_query_vectors = [[random.random() for _ in range(dim)] for _ in range(10)]
    f16_query_vectors, bf16_query_vectors = gen_bf16_vectors(10, dim)
    f16_vectors, bf16_vectors = gen_bf16_vectors(nb, dim)

    t0 = time.time()
    data = [
            [i for i in range(nb)],
            [np.float32(i) for i in range(nb)],
            [str(i) for i in range(nb)],
            # [[1,2] for _ in range(nb)],
            [[random.random() for _ in range(dim)] for _ in range(nb)],
            # f16_vectors,
            bf16_vectors
        ]
    # columns = ["int64", "float", "varchar", "array_int", "float_vector", "float16_vector", "brain_float_vector"]
    # columns data to row data
    # data_rows = [
    #     {
    #         "int64": data[0][i],
    #         "float": data[1][i],
    #         "varchar": data[2][i],
    #         "array_int": data[3][i],
    #         "float_vector": data[4][i],
    #         "float16_vector": data[5][i],
    #         "brain_float_vector": data[6][i],
    #     } for i in range(nb)
    # ]

    collection.insert(data)
    t1 = time.time()
    print(f"\nInsert {nb} vectors cost {t1 - t0:.4f} seconds")

    t0 = time.time()
    print(f"\nGet collection entities...")
    collection.flush()
    print(collection.num_entities)
    t1 = time.time()
    print(f"\nGet collection entities cost {t1 - t0:.4f} seconds")

    print("\nGet replicas number")
    try:
        replicas_info = collection.get_replicas()
        replica_number = len(replicas_info.groups)
        print(f"\nReplicas number is {replica_number}")
    except Exception as e:
        print(str(e))
        replica_number = 1

    # create index and load table
    default_index = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
    collection.release()
    print(f"\nCreate index...")
    t0 = time.time()
    collection.create_index(field_name="int64", index_params={"index_type": "INVERTED"})
    collection.create_index(field_name="float", index_params={"index_type": "INVERTED"})
    collection.create_index(field_name="float_vector", index_params=default_index)
    collection.create_index(field_name="float16_vector", index_params=default_index)
    collection.create_index(field_name="brain_float_vector", index_params=default_index)
    t1 = time.time()
    print(f"\nCreate index cost {t1 - t0:.4f} seconds")
    print(f"\nload collection...")
    t0 = time.time()
    collection.load(replica_number=replica_number)
    t1 = time.time()
    print(f"\nload collection cost {t1 - t0:.4f} seconds")

    # load and search
    topK = 5
    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
    t0 = time.time()
    print(f"\nSearch...")
    # define output_fields of search result
    res = collection.search(
        float_query_vectors[-2:], "float_vector", search_params, topK,
        "int64 > 100", output_fields=["int64", "float"], timeout=TIMEOUT
    )
    t1 = time.time()
    print(f"search cost  {t1 - t0:.4f} seconds")
    # show result
    for hits in res:
        for hit in hits:
            # Get value of the random value field for search result
            print(hit, hit.entity.get("float"))
    # t0 = time.time()
    # print(f"\nSearch...")
    # # define output_fields of search result
    # res = collection.search(
    #     float_query_vectors[-2:], "brain_float_vector", search_params, topK,
    #     "int64 > 100", output_fields=["int64", "float"], timeout=TIMEOUT
    # )
    # t1 = time.time()
    # print(f"search cost  {t1 - t0:.4f} seconds")
    # # show result
    # for hits in res:
    #     for hit in hits:
    #         # Get value of the random value field for search result
    #         print(hit, hit.entity.get("float"))

    # query
    expr = "int64 in [2,4,6,8]"
    output_fields = ["int64", "float"]
    res = collection.query(expr, output_fields, timeout=TIMEOUT)
    sorted_res = sorted(res, key=lambda k: k['int64'])
    for r in sorted_res:
        print(r)

    reqs = [
        AnnSearchRequest(**{
            "data":[float_query_vectors[-1]],
            "anns_field":"float_vector",
            "param": {"metric_type": "L2"},
            "limit": 10,
        }),
        AnnSearchRequest(**{
            "data": [bf16_query_vectors[-1]],
            "anns_field": "brain_float_vector",
            "param": {"metric_type": "L2"},
            "limit": 10,
        })
    ]
    # search for each field
    for r in reqs:
        res = collection.search(
            data=r.data,
            anns_field=r.anns_field,
            param=r.param,
            limit=r.limit,
        )
        print(f"search result for {r.anns_field}: {res}")

    # hybrid search
    res = collection.hybrid_search(
        reqs=reqs,
        rerank=RRFRanker(),
        limit=10,
    )
    print(f"hybrid search result: {res}")


parser = argparse.ArgumentParser(description='host ip')
parser.add_argument('--host', type=str, default='10.104.20.97', help='host ip')
args = parser.parse_args()
# add time stamp
print(f"\nStart time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
hello_milvus(args.host)
