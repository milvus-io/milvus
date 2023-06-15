import sys
import copy
import time
from loguru import logger
import pymilvus
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection, list_collections,
)
logger.remove()
logger.add(sys.stderr, format= "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{thread.name}</cyan> |"
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>", 
    level="INFO")

pymilvus_version = pymilvus.__version__

all_index_types = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW"]

default_index_params = [{}, {"nlist": 128}, {"nlist": 128}, {"nlist": 128, "m": 16, "nbits": 8},
                        {"M": 48, "efConstruction": 500}]

index_params_map = dict(zip(all_index_types, default_index_params))

NUM_REPLICAS = 2


def filter_collections_by_prefix(prefix):
    col_list = list_collections()
    logger.info(f"all collections: {col_list}")
    res = []
    for col in col_list:
        if col.startswith(prefix):
            if any(index_name in col for index_name in all_index_types):
                res.append(col)
            else:
                logger.warning(f"collection {col} has no supported index, skip")
    logger.info(f"filtered collections with prefix {prefix}: {res}")
    return res


def gen_search_param(index_type, metric_type="L2"):
    search_params = []
    if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ"]:
        for nprobe in [10]:
            ivf_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
            search_params.append(ivf_search_params)
    elif index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
        for nprobe in [10]:
            bin_search_params = {"metric_type": "HAMMING", "params": {"nprobe": nprobe}}
            search_params.append(bin_search_params)
    elif index_type in ["HNSW"]:
        for ef in [64]:
            hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
            search_params.append(hnsw_search_param)
    elif index_type == "ANNOY":
        for search_k in [1000]:
            annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
            search_params.append(annoy_search_param)
    else:
        logger.info("Invalid index_type.")
        raise Exception("Invalid index_type.")
    return search_params


def get_collections(prefix, check=False):
    logger.info("\nList collections...")
    col_list = filter_collections_by_prefix(prefix)
    logger.info(f"collections_nums: {len(col_list)}")
    # list entities if collections
    for name in col_list:
        c = Collection(name=name)
        if pymilvus_version >= "2.2.0":
            c.flush()
        else:
            c.num_entities
        num_entities = c.num_entities
        logger.info(f"{name}: {num_entities}")
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
        logger.info("\nCreate collection...")
        col_name = prefix + index_name
        collection = Collection(name=col_name, schema=default_schema) 
        logger.info(f"collection name: {col_name}")
        logger.info(f"begin insert, count: {count} nb: {nb}")
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
            logger.info(f"[{j+1}/{times}] insert {nb} data, time: {end_time - start_time:.4f}")
            total_time += end_time - start_time
            if j <= times - 3:
                collection.flush()
                collection.num_entities
            if j == times - 3:
                collection.compact()
                

        logger.info(f"end insert, time: {total_time:.4f}")
        if flush:
            logger.info("Get collection entities")
            start_time = time.time()
            if pymilvus_version >= "2.2.0":
                collection.flush()
            else:
                collection.num_entities
            logger.info(f"collection entities: {collection.num_entities}")
            end_time = time.time()
            logger.info("Get collection entities time = %.4fs" % (end_time - start_time))
    logger.info("\nList collections...")
    logger.info(get_collections(prefix))


def create_index_flat():
    # create index
    default_flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "L2"}
    all_col_list = list_collections()
    col_list = []
    for col_name in all_col_list:
        if "FLAT" in col_name and "task" in col_name and "IVF" not in col_name:
            col_list.append(col_name)
    logger.info("\nCreate index for FLAT...")
    for col_name in col_list:
        c = Collection(name=col_name)
        logger.info(c)
        try:
            replicas = c.get_replicas()
            replica_number = len(replicas.groups)
            c.release()
        except Exception as e:
            replica_number = 0
            logger.info(e)
        t0 = time.time()
        c.create_index(field_name="float_vector", index_params=default_flat_index)
        logger.info(f"create index time: {time.time() - t0:.4f}")
        if replica_number > 0:
            c.load(replica_number=replica_number)


def create_index(prefix):
    # create index
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
    col_list = get_collections(prefix)
    logger.info("\nCreate index...")
    for col_name in col_list:
        c = Collection(name=col_name)
        try:
            replicas = c.get_replicas()
            replica_number = len(replicas.groups)
            c.release()
        except Exception as e:
            replica_number = 0
            logger.info(e)
        index_name = col_name.replace(prefix, "")
        logger.info(index_name)
        logger.info(c)
        index = copy.deepcopy(default_index)
        index["index_type"] = index_name
        index["params"] = index_params_map[index_name]
        if index_name in ["BIN_FLAT", "BIN_IVF_FLAT"]:
            index["metric_type"] = "HAMMING"
        t0 = time.time()
        c.create_index(field_name="float_vector", index_params=index)
        logger.info(f"create index time: {time.time() - t0:.4f}")
        if replica_number > 0:
            c.load(replica_number=replica_number)


def release_collection(prefix):
    col_list = get_collections(prefix)
    logger.info("release collection")
    for col_name in col_list:
        c = Collection(name=col_name)
        c.release()


def load_and_search(prefix, replicas=1):
    logger.info("search data starts")
    col_list = get_collections(prefix)
    for col_name in col_list:
        c = Collection(name=col_name)
        logger.info(f"collection name: {col_name}")
        logger.info("load collection")
        if replicas == 1:
            t0 = time.time()
            c.load()
            logger.info(f"load time: {time.time() - t0:.4f}")
        if replicas > 1:
            logger.info("release collection before load if replicas > 1")
            t0 = time.time()
            c.release()
            logger.info(f"release time: {time.time() - t0:.4f}")
            t0 = time.time()
            c.load(replica_number=replicas)
            logger.info(f"load time: {time.time() - t0:.4f}")
            logger.info(c.get_replicas())
        topK = 5
        vectors = [[1.0 for _ in range(128)] for _ in range(3000)]
        index_name = col_name.replace(prefix, "")
        search_params = gen_search_param(index_name)[0]
        logger.info(search_params)
        # search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        start_time = time.time()
        logger.info(f"\nSearch...")
        # define output_fields of search result
        v_search = vectors[:1]
        res = c.search(
            v_search, "float_vector", search_params, topK,
            "count > 500", output_fields=["count", "random_value"], timeout=120
        )
        end_time = time.time()
        # show result
        for hits in res:
            for hit in hits:
                logger.info(f"hit: {hit}")
            ids = hits.ids
            assert len(ids) == topK, f"get {len(ids)} results, but topK is {topK}"
            logger.info(ids)
        assert len(res) == len(v_search), f"get {len(res)} results, but search num is {len(v_search)}"
        logger.info("search latency: %.4fs" % (end_time - start_time))
        t0 = time.time()
        expr = "count in [2,4,6,8]"
        if "SQ" in col_name or "PQ" in col_name:
            output_fields = ["count", "random_value"]
        else:
            output_fields = ["count", "random_value", "float_vector"]
        res = c.query(expr, output_fields, timeout=120)
        sorted_res = sorted(res, key=lambda k: k['count'])
        for r in sorted_res:
            logger.info(r)
        t1 = time.time()
        assert len(res) == 4
        logger.info("query latency: %.4fs" % (t1 - t0))
        # c.release()
        logger.info("###########")
    logger.info("search data ends")
