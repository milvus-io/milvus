import copy
import time
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection, list_collections,
)

all_index_types = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW", "ANNOY", "RHNSW_FLAT", "RHNSW_PQ", "RHNSW_SQ",
                   "BIN_FLAT", "BIN_IVF_FLAT"]

default_index_params = [{"nlist": 128}, {"nlist": 128}, {"nlist": 128}, {"nlist": 128, "m": 16, "nbits": 8},
                        {"M": 48, "efConstruction": 500}, {"n_trees": 50}, {"M": 48, "efConstruction": 500},
                        {"M": 48, "efConstruction": 500, "PQM": 8}, {"M": 48, "efConstruction": 500}, {"nlist": 128},
                        {"nlist": 128}]

index_params_map = dict(zip(all_index_types, default_index_params))

NUM_REPLICAS = 2


def filter_collections_by_prefix(prefix):
    col_list = list_collections()
    res = []
    for col in col_list:
        if col.startswith(prefix):
            res.append(col)
    return res


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


def get_collections(prefix, check=False):
    print("\nList collections...")
    col_list = filter_collections_by_prefix(prefix)
    print(f"collections_nums: {len(col_list)}")
    # list entities if collections
    for name in col_list:
        c = Collection(name=name)
        num_entities = c.num_entities
        print(f"{name}: {num_entities}")
        if check:
            assert num_entities >= 3000
    return col_list


def create_collections_and_insert_data(prefix, flush=True, count=3000, collection_cnt=11):
    import random
    dim = 128
    nb = count // 10
    default_fields = [
        FieldSchema(name="count", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="random_value", dtype=DataType.DOUBLE),
        FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]
    default_schema = CollectionSchema(fields=default_fields, description="test collection")
    for index_name in all_index_types[:collection_cnt]:
        print("\nCreate collection...")
        col_name = prefix + index_name
        collection = Collection(name=col_name, schema=default_schema) 
        print(f"collection name: {col_name}")
        print(f"begin insert, count: {count} nb: {nb}")
        times = int(count // nb)
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
            print(f"[{j+1}/{times}] insert {nb} data, time: {end_time - start_time:.4f}")
            total_time += end_time - start_time

        print(f"end insert, time: {total_time:.4f}")
        if flush:
            print("Get collection entities")
            start_time = time.time()
            print(f"collection entities: {collection.num_entities}")
            end_time = time.time()
            print("Get collection entities time = %.4fs" % (end_time - start_time))
    print("\nList collections...")
    print(get_collections(prefix))


def create_index(prefix):
    # create index
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
    col_list = get_collections(prefix)
    print("\nCreate index...")
    for col_name in col_list:
        c = Collection(name=col_name)
        index_name = col_name.replace(prefix, "")
        print(index_name)
        print(c)
        index = copy.deepcopy(default_index)
        index["index_type"] = index_name
        index["params"] = index_params_map[index_name]
        if index_name in ["BIN_FLAT", "BIN_IVF_FLAT"]:
            index["metric_type"] = "HAMMING"
        t0 = time.time()
        c.create_index(field_name="float_vector", index_params=index)
        print(f"create index time: {time.time() - t0:.4f}")


def load_and_search(prefix, replicas=1):
    print("search data starts")
    col_list = get_collections(prefix)
    for col_name in col_list:
        c = Collection(name=col_name)
        print(f"collection name: {col_name}")
        print("release collection")
        c.release()
        print("load collection")
        t0 = time.time()
        if replicas == 1:
            c.load()
        if replicas > 1:
            c.load(replica_number=replicas)
            print(c.get_replicas())
        print(f"load time: {time.time() - t0:.4f}")
        topK = 5
        vectors = [[1.0 for _ in range(128)] for _ in range(3000)]
        index_name = col_name.replace(prefix, "")
        search_params = gen_search_param(index_name)[0]
        print(search_params)
        # search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        start_time = time.time()
        print(f"\nSearch...")
        # define output_fields of search result
        res = c.search(
            vectors[:1], "float_vector", search_params, topK,
            "count > 500", output_fields=["count", "random_value"], timeout=120
        )
        end_time = time.time()
        # show result
        for hits in res:
            for hit in hits:
                # Get value of the random value field for search result
                print(hit, hit.entity.get("random_value"))
            ids = hits.ids
            print(ids)
        print("search latency: %.4fs" % (end_time - start_time))
        t0 = time.time()
        expr = "count in [2,4,6,8]"
        output_fields = ["count", "random_value"]
        res = c.query(expr, output_fields, timeout=20)
        sorted_res = sorted(res, key=lambda k: k['count'])
        for r in sorted_res:
            print(r)
        t1 = time.time()
        print("query latency: %.4fs" % (t1 - t0))
        # c.release()
        print("###########")
    print("search data ends")
