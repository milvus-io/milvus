
""" Initialized parameters """
port = 19530
epsilon = 0.000001
namespace = "milvus"
default_flush_interval = 1
big_flush_interval = 1000
default_drop_interval = 3
default_dim = 128
default_nb = 3000
default_nb_medium = 5000
default_top_k = 10
default_nq = 2
default_limit = 10
default_search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
default_index = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
max_top_k = 16384
max_partition_num = 4096  # 256
default_segment_row_limit = 1000
default_server_segment_row_limit = 1024 * 512
default_alias = "default"
default_bool_field_name = "bool"
default_int8_field_name = "int8"
default_int16_field_name = "int16"
default_int32_field_name = "int32"
default_int64_field_name = "int64"
default_float_field_name = "float"
default_double_field_name = "double"
default_string_field_name = "string"
default_float_vec_field_name = "float_vector"
another_float_vec_field_name = "float_vector1"
default_binary_vec_field_name = "binary_vector"
default_partition_name = "_default"
default_tag = "1970_01_01"
row_count = "row_count"
default_desc = ""
default_collection_desc = "default collection"
default_index_name = "default_index_name"
default_binary_desc = "default binary collection"
collection_desc = "collection"
int_field_desc = "int64 type field"
float_field_desc = "float type field"
float_vec_field_desc = "float vector type field"
binary_vec_field_desc = "binary vector type field"
max_dim = 32768
gracefulTime = 1
default_nlist = 128

Not_Exist = "Not_Exist"
Connect_Object_Name = "Milvus"
list_content = "list_content"
dict_content = "dict_content"
value_content = "value_content"

err_code = "err_code"
err_msg = "err_msg"

"""" List of parameters used to pass """
get_invalid_strs = [
    [],
    1,
    [1, "2", 3],
    (1,),
    {1: 1},
    None,
    "",
    " ",
    "12-s",
    "12 s",
    "(mn)",
    "中文",
    "%$#",
    "a".join("a" for i in range(256))]


get_not_string = [
    [],
    {},
    None,
    (1, ),
    1,
    1.0,
    [1, "2", 3]
]

get_invalid_vectors = [
    "1*2",
    [1],
    [1, 2],
    [" "],
    ['a'],
    [None],
    None,
    (1, 2),
    {"a": 1},
    " ",
    "",
    "String",
    " siede ",
    "中文",
    "a".join("a" for i in range(256))
]

get_invalid_ints = [
    9999999999,
    1.0,
    None,
    [1, 2, 3],
    " ",
    "",
    -1,
    "String",
    "=c",
    "中文",
    "a".join("a" for i in range(256))
]

get_invalid_dict = [
    [],
    1,
    [1, "2", 3],
    (1,),
    None,
    "",
    " ",
    "12-s",
    {1: 1},
    {"中文": 1},
    {"%$#": ["a"]},
    {"a".join("a" for i in range(256)): "a"}
]

get_dict_without_host_port = [
    {"host": "host"},
    {"port": "port"},
    # ["host", "port"],
    # ("host", "port"),
    {"host": -1},
    {"port": ["192.168.1.1"]},
    {"": ""}
]

get_wrong_format_dict = [
    {"host": "string_host", "port": {}},
    {"host": 0, "port": 19520}
]


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


class CheckTasks:
    """ The name of the method used to check the result """
    check_nothing = "check_nothing"
    err_res = "error_response"
    ccr = "check_connection_result"
    check_collection_property = "check_collection_property"
    check_partition_property = "check_partition_property"
    check_search_results = "check_search_results"
    check_query_results = "check_query_results"
    check_distance = "check_distance"


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
