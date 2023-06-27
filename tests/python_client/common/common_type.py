""" Initialized parameters """
port = 19530
epsilon = 0.000001
namespace = "milvus"
default_flush_interval = 1
big_flush_interval = 1000
default_drop_interval = 3
default_dim = 128
default_nb = 2000
default_nb_medium = 5000
default_top_k = 10
default_nq = 2
default_limit = 10
default_search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}
default_search_ip_params = {"metric_type": "IP", "params": {"nprobe": 10}}
default_search_binary_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
default_index = {"index_type": "IVF_SQ8", "metric_type": "COSINE", "params": {"nlist": 64}}
default_binary_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
default_diskann_index = {"index_type": "DISKANN", "metric_type": "COSINE", "params": {}}
default_diskann_search_params = {"metric_type": "COSINE", "params": {"search_list": 30}}
max_top_k = 16384
max_partition_num = 4096  # 256
default_segment_row_limit = 1000
default_server_segment_row_limit = 1024 * 512
default_alias = "default"
default_user = "root"
default_password = "Milvus"
default_bool_field_name = "bool"
default_int8_field_name = "int8"
default_int16_field_name = "int16"
default_int32_field_name = "int32"
default_int64_field_name = "int64"
default_float_field_name = "float"
default_double_field_name = "double"
default_string_field_name = "varchar"
default_json_field_name = "json_field"
default_float_vec_field_name = "float_vector"
another_float_vec_field_name = "float_vector1"
default_binary_vec_field_name = "binary_vector"
default_partition_name = "_default"
default_resource_group_name = '__default_resource_group'
default_resource_group_capacity = 1000000
default_tag = "1970_01_01"
row_count = "row_count"
default_length = 65535
default_json_list_length = 1
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
min_dim = 1
gracefulTime = 1
default_nlist = 128
compact_segment_num_threshold = 3
compact_delta_ratio_reciprocal = 5  # compact_delta_binlog_ratio is 0.2
compact_retention_duration = 40  # compaction travel time retention range 20s
max_compaction_interval = 60  # the max time interval (s) from the last compaction
max_field_num = 64  # Maximum number of fields in a collection
max_name_length = 255  # Maximum length of name for a collection or alias
default_replica_num = 1
default_graceful_time = 5  #
default_shards_num = 1
max_shards_num = 16
IMAGE_REPOSITORY_MILVUS = "harbor.milvus.io/dockerhub/milvusdb/milvus"
NAMESPACE_CHAOS_TESTING = "chaos-testing"

Not_Exist = "Not_Exist"
Connect_Object_Name = True
list_content = "list_content"
dict_content = "dict_content"
value_content = "value_content"

err_code = "err_code"
err_msg = "err_msg"
in_cluster_env = "IN_CLUSTER"

default_flat_index = {"index_type": "FLAT", "params": {}, "metric_type": "COSINE"}
default_bin_flat_index = {"index_type": "BIN_FLAT", "params": {}, "metric_type": "JACCARD"}
default_count_output = "count(*)"

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
    "".join("a" for i in range(max_name_length + 1))]

get_invalid_type_fields = [
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
    "".join("a" for i in range(max_name_length + 1))]

get_not_string = [
    [],
    {},
    None,
    (1,),
    1,
    1.0,
    [1, "2", 3]
]

get_not_string_value = [
    " ",
    "12-s",
    "12 s",
    "(mn)",
    "中文",
    "%$#",
    "a".join("a" for i in range(256))
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

get_invalid_metric_type = [
    [],
    1,
    [1, "2", 3],
    (1,),
    {1: 1},
    " ",
    "12-s",
    "12 s",
    "(mn)",
    "中文",
    "%$#",
    "".join("a" for i in range(max_name_length + 1))]

get_dict_without_host_port = [
    {"host": "host"},
    {"": ""}
]

get_dict_invalid_host_port = [
    {"port": "port"},
    # ["host", "port"],
    # ("host", "port"),
    {"host": -1},
    {"port": ["192.168.1.1"]},
    {"port": "-1", "host": "hostlocal"},
]

get_wrong_format_dict = [
    {"host": "string_host", "port": {}},
    {"host": 0, "port": 19520}
]

""" Specially defined list """
all_index_types = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW", "DISKANN", "BIN_FLAT", "BIN_IVF_FLAT",
                   "GPU_IVF_FLAT", "GPU_IVF_PQ"]

default_index_params = [{"nlist": 128}, {"nlist": 128}, {"nlist": 128}, {"nlist": 128, "m": 16, "nbits": 8},
                        {"M": 48, "efConstruction": 500}, {}, {"nlist": 128}, {"nlist": 128},
                        {"nlist": 64}, {"nlist": 64, "m": 16, "nbits": 8}]

Handler_type = ["GRPC", "HTTP"]
binary_support = ["BIN_FLAT", "BIN_IVF_FLAT"]
delete_support = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ"]
ivf = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ"]
skip_pq = ["IVF_PQ"]
float_metrics = ["L2", "IP", "COSINE"]
binary_metrics = ["JACCARD", "HAMMING", "TANIMOTO", "SUBSTRUCTURE", "SUPERSTRUCTURE"]
structure_metrics = ["SUBSTRUCTURE", "SUPERSTRUCTURE"]
all_scalar_data_types = ['int8', 'int16', 'int32', 'int64', 'float', 'double', 'bool', 'varchar']


class CheckTasks:
    """ The name of the method used to check the result """
    check_nothing = "check_nothing"
    err_res = "error_response"
    ccr = "check_connection_result"
    check_collection_property = "check_collection_property"
    check_partition_property = "check_partition_property"
    check_search_results = "check_search_results"
    check_query_results = "check_query_results"
    check_query_empty = "check_query_empty"  # verify that query result is empty
    check_query_not_empty = "check_query_not_empty"
    check_distance = "check_distance"
    check_delete_compact = "check_delete_compact"
    check_merge_compact = "check_merge_compact"
    check_role_property = "check_role_property"
    check_permission_deny = "check_permission_deny"
    check_value_equal = "check_value_equal"
    check_rg_property = "check_resource_group_property"
    check_describe_collection_property = "check_describe_collection_property"


class BulkLoadStates:
    BulkLoadPersisted = "BulkLoadPersisted"
    BulkLoadFailed = "BulkLoadFailed"
    BulkLoadDataQueryable = "BulkLoadDataQueryable"
    BulkLoadDataIndexed = "BulkLoadDataIndexed"


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
        Loadbalance:
            loadbalance testcases which need to be run in multi query nodes
        ClusterOnly:
            For functions only suitable to cluster mode
        GPU:
            For GPU supported cases
    """
    L0 = "L0"
    L1 = "L1"
    L2 = "L2"
    L3 = "L3"
    RBAC = "RBAC"
    Loadbalance = "Loadbalance"  # loadbalance testcases which need to be run in multi query nodes
    ClusterOnly = "ClusterOnly"  # For functions only suitable to cluster mode
    MultiQueryNodes = "MultiQueryNodes"  # for 8 query nodes configs tests, such as resource group
    GPU = "GPU"
