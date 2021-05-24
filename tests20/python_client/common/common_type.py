
""" Initialized parameters """
port = 19530
epsilon = 0.000001
namespace = "milvus"
default_flush_interval = 1
big_flush_interval = 1000
default_drop_interval = 3
default_dim = 128
default_nb = 1200
default_top_k = 10
max_top_k = 16384
max_partition_num = 4096  # 256
default_segment_row_limit = 1000
default_server_segment_row_limit = 1024 * 512
default_alias = "default"
default_int64_field = "int64"
default_float_field = "float"
default_float_vec_field_name = "float_vector"
default_binary_vec_field_name = "binary_vector"
default_partition_name = "_default"
default_tag = "1970_01_01"
row_count = "row_count"
default_desc = ""
default_collection_desc = "default collection"
default_binary_desc = "default binary collection"
collection_desc = "collection"
int_field_desc = "int64 type field"
float_field_desc = "float type field"
float_vec_field_desc = "float vector type field"
binary_vec_field_desc = "binary vector type field"


"""" List of parameters used to pass """
get_invalid_strs = [
    [],
    1,
    [1, "2", 3],
    (1,),
    {1: 1},
    None,
    "12-s",
    "12 s",
    "(mn)",
    "中文",
    "%$#",
    "a".join("a" for i in range(256))]


""" Specially defined list """
all_index_types = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW", "ANNOY", "RHNSW_FLAT", "RHNSW_PQ", "RHNSW_SQ",
                   "BIN_FLAT", "BIN_IVF_FLAT"]

default_index_params = [{"nlist": 128}, {"nlist": 128}, {"nlist": 128}, {"nlist": 128, "m": 16, "nbits": 8},
                        {"M": 48, "efConstruction": 500}, {"n_trees": 50}, {"M": 48, "efConstruction": 500},
                        {"M": 48, "efConstruction": 500, "PQM": 64}, {"M": 48, "efConstruction": 500}, {"nlist": 128},
                        {"nlist": 128}]

Handler_type = ["GRPC", "HTTP"]
index_cpu_not_support = ["IVF_SQ8_HYBRID"]
binary_support = ["BIN_FLAT", "BIN_IVF_FLAT"]
delete_support = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_SQ8_HYBRID", "IVF_PQ"]
ivf = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_SQ8_HYBRID", "IVF_PQ"]
skip_pq = ["IVF_PQ", "RHNSW_PQ", "RHNSW_SQ"]
binary_metrics = ["JACCARD", "HAMMING", "TANIMOTO", "SUBSTRUCTURE", "SUPERSTRUCTURE"]
structure_metrics = ["SUBSTRUCTURE", "SUPERSTRUCTURE"]


class CheckParams:
    """ The name of the method used to check the result """
    cname_param_check = "collection_name_param_check"
    pname_param_check = "partition_name_param_check"
    list_count = "check_list_count"
    collection_property_check = "collection_property_check"


class CaseLabel:
    """
    Testcase Levels
    CI Regression:
        L0:
            part of CI Regression
            triggered by github commit
            optional used for dev to verify his fix before submitting a PR(like smoke)
            ~100 testcases and run in 3 mins
        L1:
            part of CI Regression
            triggered by github commit
            must pass before merge
            run in 15 mins
    Benchmark:
        L2:
            E2E tests and bug-fix verification
            Nightly run triggered by cron job
            run in 60 mins
        L3:
            Stability/Performance/reliability, etc. special tests
            Triggered by cron job or manually
            run duration depends on test configuration
    """
    L0 = "L0"
    L1 = "L1"
    L2 = "L2"
    L3 = "L3"
