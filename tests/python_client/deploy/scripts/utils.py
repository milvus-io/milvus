# import docker
import copy
from pymilvus import (
    connections, FieldSchema, CollectionSchema, DataType,
    Collection, list_collections,
)

all_index_types = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW", "ANNOY", "RHNSW_FLAT", "RHNSW_PQ", "RHNSW_SQ",
                   "BIN_FLAT", "BIN_IVF_FLAT"]

default_index_params = [{"nlist": 128}, {"nlist": 128}, {"nlist": 128}, {"nlist": 128, "m": 16, "nbits": 8},
                        {"M": 48, "efConstruction": 500}, {"n_trees": 50}, {"M": 48, "efConstruction": 500},
                        {"M": 48, "efConstruction": 500, "PQM": 64}, {"M": 48, "efConstruction": 500}, {"nlist": 128},
                        {"nlist": 128}]

index_params_map = dict(zip(all_index_types, default_index_params))


def gen_search_param(index_type, metric_type="L2"):
    search_params = []
    if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_SQ8H", "IVF_PQ"]:
        for nprobe in [10]:
            ivf_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
            search_params.append(ivf_search_params)
    elif index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        for nprobe in [10]:
            bin_search_params = {"metric_type": "HAMMING", "params": {"nprobe": nprobe}}
            search_params.append(bin_search_params)
    elif index_type in ["HNSW", "RHNSW_FLAT", "RHNSW_PQ", "RHNSW_SQ"]:
        for ef in [64]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type in ["NSG", "RNSG"]:
        for search_length in [100]:
            nsg_search_param = {"metric_type": metric_type, "params": {"search_length": search_length}}
            search_params.append(nsg_search_param)
    elif index_type == "ANNOY":
        for search_k in [1000]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
    else:
        print("Invalid index_type.")
        raise Exception("Invalid index_type.")
    return search_params


# def list_containers():
#     client = docker.from_env()
#     containers = client.containers.list()
#     for c in containers:
#         if "milvus" in c.name:
#             print(c.image)


def get_collections():
    print(f"\nList collections...")
    col_list = list_collections()
    print(f"collections_nums: {len(col_list)}")
    # list entities if collections
    for name in col_list:
        c = Collection(name=name)
        print(f"{name}: {c.num_entities}")


def create_collections_and_insert_data():
    import random
    import time
    dim = 128
    default_fields = [
        FieldSchema(name="count", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="random_value", dtype=DataType.DOUBLE),
        FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]
    default_schema = CollectionSchema(fields=default_fields, description="test collection")
    print(f"\nList collections...")
    print(list_collections())
    for col_name in all_index_types:
        print(f"\nCreate collection...")
        collection = Collection(name=col_name, schema=default_schema) 
        print(f"collection name: {col_name}")
        count = 50000
        nb = 5000
        print(f"begin insert, count: {count} nb: {nb}")
        times = int(count / nb)
        total_time = 0.0
        vectors = [[random.random() for _ in range(dim)] for _ in range(count)]
        for j in range(times):
            start_time = time.time()
            collection.insert(
                [
                    [i for i in range(nb * j, nb * j + nb)],
                    [float(random.randrange(-20, -10)) for _ in range(nb)],
                    vectors[nb*j:nb*j+nb]
                ]
            )
            end_time = time.time()
            print(f"[{j+1}/{times}] insert {nb} data, time: {end_time - start_time}")
            total_time += end_time - start_time

        print("end insert, time:", total_time)
        print("Get collection entities")
        start_time = time.time()
        print(f"collection entities: {collection.num_entities}")
        end_time = time.time()
        print("Get collection entities time = %.4fs" % (end_time - start_time))
    print(f"\nList collections...")
    print(list_collections())


def create_index():
    # create index
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
    col_list = list_collections()
    print(f"\nCreate index...")
    for name in col_list:
        c = Collection(name=name)

        print(name)
        print(c)
        index = copy.deepcopy(default_index)
        index["index_type"] = name
        index["params"] = index_params_map[name]
        if name in ["BIN_FLAT", "BIN_IVF_FLAT"]:
            index["metric_type"] = "HAMMING"
        c.create_index(field_name="float_vector", index_params=index)


def load_and_search():
    print("search data starts")
    col_list = list_collections()
    for name in col_list:
        c = Collection(name=name)
        print(f"collection name: {name}")
        c.load()
        topK = 5
        vectors = [[0.0 for _ in range(128)] for _ in range(3000)]
        index_type = name
        search_params = gen_search_param(index_type)[0]
        print(search_params)
        # search_params = {"metric_type": "L2", "params": {"nprobe": 10}}

        import time
        start_time = time.time()
        print(f"\nSearch...")
        # define output_fields of search result
        res = c.search(
            vectors[:1], "float_vector", search_params, topK,
            "count > 500", output_fields=["count", "random_value"], timeout=20
        )
        end_time = time.time()
        # show result
        for hits in res:
            for hit in hits:
                # Get value of the random value field for search result
                print(hit, hit.entity.get("random_value"))
            ids = hits.ids
            print(ids)
        print("search latency = %.4fs" % (end_time - start_time))
        t0 = time.time()
        expr = "count in [2,4,6,8]"
        output_fields = ["count", "random_value"]
        res = c.query(expr, output_fields)
        sorted_res = sorted(res, key=lambda k: k['count'])
        for r in sorted_res:
            print(r)
        t1 = time.time()
        print("query latency = %.4fs" % (t1 -t0))
        # c.release()
        print("###########")
    print("search data ends")
