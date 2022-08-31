import json
from utils.util_log import test_log as log

all_index_types = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW", "ANNOY", "BIN_FLAT", "BIN_IVF_FLAT"]

default_index_params = [{"nlist": 128}, {"nlist": 128}, {"nlist": 128}, {"nlist": 128, "m": 16, "nbits": 8},
                        {"M": 48, "efConstruction": 500}, {"n_trees": 50}, {"nlist": 128}, {"nlist": 128}]

index_params_map = dict(zip(all_index_types, default_index_params))

def gen_index_param(index_type):
    metric_type = "L2"
    if "BIN" in index_type:
        metric_type = "HAMMING"
    index_param = {
        "index_type": index_type,
        "params": index_params_map[index_type],
        "metric_type": metric_type
    }
    return index_param


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
        print("Invalid index_type.")
        raise Exception("Invalid index_type.")
    return search_params


def get_collections():
    try:
        with open("/tmp/ci_logs/all_collections.json", "r") as f:
            data = json.load(f)
            collections = data["all"]
    except Exception as e:
        log.error(f"get_all_collections error: {e}")
        return []
    return collections