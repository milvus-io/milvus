import pdb
import logging

logger = logging.getLogger("milvus_benchmark.parser")


def operations_parser(operations):
    if not operations:
        raise Exception("No operations in suite defined")
    for run_type, run_params in operations.items():
        logger.debug(run_type)
        return (run_type, run_params)


def collection_parser(collection_name):
    """
    Resolve the collection name to obtain the corresponding configuration
    e.g.:
    sift_1m_128_l2
    sift: type of data set
    1m: size of the data inserted in the collection
    128: vector dimension
    l2: metric type
    """
    tmp = collection_name.split("_")
    # if len(tmp) != 5:
    #     return None
    data_type = tmp[0]
    collection_size_unit = tmp[1][-1]
    collection_size = tmp[1][0:-1]
    if collection_size_unit == "w":
        collection_size = int(collection_size) * 10000
    elif collection_size_unit == "m":
        collection_size = int(collection_size) * 1000000
    elif collection_size_unit == "b":
        collection_size = int(collection_size) * 1000000000
    dimension = int(tmp[2])
    metric_type = str(tmp[3])
    return (data_type, collection_size, dimension, metric_type)


def parse_ann_collection_name(collection_name):
    """
    Analyze the collection name of the accuracy test and obtain the corresponding configuration
    e.g.:
    sift_128_euclidean
    """
    data_type = collection_name.split("_")[0]
    dimension = int(collection_name.split("_")[1])
    metric = collection_name.split("_")[2]
    # metric = collection_name.attrs['distance']
    # dimension = len(collection_name["train"][0])
    if metric == "euclidean":
        metric_type = "l2"
    elif metric  == "angular":
        metric_type = "ip"
    elif metric  == "jaccard":
        metric_type = "jaccard"
    elif metric == "hamming":
        metric_type = "hamming"
    return (data_type, dimension, metric_type)


def search_params_parser(param):
    # parse top-k, set default value if top-k not in param
    if "top_ks" not in param:
        top_ks = [10]
    else:
        top_ks = param["top_ks"]
    if isinstance(top_ks, int):
        top_ks = [top_ks]
    elif isinstance(top_ks, list):
        top_ks = list(top_ks)
    else:
        logger.warning("Invalid format top-ks: %s" % str(top_ks))

    # parse nqs, set default value if nq not in param
    if "nqs" not in param:
        nqs = [10]
    else:
        nqs = param["nqs"]
    if isinstance(nqs, int):
        nqs = [nqs]
    elif isinstance(nqs, list):
        nqs = list(nqs)
    else:
        logger.warning("Invalid format nqs: %s" % str(nqs))

    # parse nprobes
    if "nprobes" not in param:
        nprobes = [1]
    else:
        nprobes = param["nprobes"]
    if isinstance(nprobes, int):
        nprobes = [nprobes]
    elif isinstance(nprobes, list):
        nprobes = list(nprobes)
    else:
        logger.warning("Invalid format nprobes: %s" % str(nprobes))    

    return top_ks, nqs, nprobes
