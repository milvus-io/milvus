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
default_max_capacity = 100
default_top_k = 10
default_nq = 2
default_limit = 10
default_batch_size = 1000
max_limit = 16384
max_top_k = 16384
max_partition_num = 1024
max_role_num = 10
default_partition_num = 16   # default num_partitions for partition key feature
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
default_array_field_name = "int_array"
default_int32_array_field_name = "int32_array"
default_float_array_field_name = "float_array"
default_string_array_field_name = "string_array"
default_float_vec_field_name = "float_vector"
default_float16_vec_field_name = "float16_vector"
default_bfloat16_vec_field_name = "bfloat16_vector"
another_float_vec_field_name = "float_vector1"
default_binary_vec_field_name = "binary_vector"
float_type = "FLOAT_VECTOR"
float16_type = "FLOAT16_VECTOR"
bfloat16_type = "BFLOAT16_VECTOR"
sparse_vector = "SPARSE_FLOAT_VECTOR"
append_vector_type = [float16_type, bfloat16_type, sparse_vector]
all_dense_vector_types = [float_type, float16_type, bfloat16_type]
default_sparse_vec_field_name = "sparse_vector"
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
min_dim = 2
max_binary_vector_dim = 262144
max_sparse_vector_dim = 4294967294
min_sparse_vector_dim = 1
gracefulTime = 1
default_nlist = 128
compact_segment_num_threshold = 3
compact_delta_ratio_reciprocal = 5  # compact_delta_binlog_ratio is 0.2
compact_retention_duration = 40  # compaction travel time retention range 20s
max_compaction_interval = 60  # the max time interval (s) from the last compaction
max_field_num = 64  # Maximum number of fields in a collection
max_vector_field_num = 4  # Maximum number of vector fields in a collection
max_name_length = 255  # Maximum length of name for a collection or alias
default_replica_num = 1
default_graceful_time = 5  #
default_shards_num = 1
max_shards_num = 16
default_db = "default"
max_database_num = 64
max_collections_per_db = 65536
max_collection_num = 65536
max_hybrid_search_req_num = 1024


IMAGE_REPOSITORY_MILVUS = "harbor.milvus.io/dockerhub/milvusdb/milvus"
NAMESPACE_CHAOS_TESTING = "chaos-testing"

Not_Exist = "Not_Exist"
Connect_Object_Name = True
list_content = "list_content"
dict_content = "dict_content"
value_content = "value_content"

code = "code"
err_code = "err_code"
err_msg = "err_msg"
in_cluster_env = "IN_CLUSTER"
default_count_output = "count(*)"

rows_all_data_type_file_path = "/tmp/rows_all_data_type"

"""" List of parameters used to pass """
invalid_resource_names = [
    None,               # None
    " ",                # space
    "",                 # empty
    "12name",           # start with number
    "n12 ame",          # contain space
    "n-ame",            # contain hyphen
    "nam(e)",           # contain special character
    "name中文",          # contain Chinese character
    "name%$#",          # contain special character
    "".join("a" for i in range(max_name_length + 1))]           # exceed max length

valid_resource_names = [
    "name",             # valid name
    "_name",            # start with underline
    "_12name",          # start with underline and contains number
    "n12ame_",          # end with letter and contains number and underline
    "nam_e",             # contains underline
    "".join("a" for i in range(max_name_length))]       # max length

invalid_dims = [min_dim-1, 32.1, -32, "vii", "十六", max_dim+1]

get_not_string = [
    [],
    {},
    None,
    (1,),
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
L0_index_types = ["IVF_SQ8", "HNSW", "DISKANN"]
all_index_types = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ",
                   "HNSW", "SCANN", "DISKANN",
                   "BIN_FLAT", "BIN_IVF_FLAT",
                   "SPARSE_INVERTED_INDEX", "SPARSE_WAND",
                   "GPU_IVF_FLAT", "GPU_IVF_PQ"]

default_all_indexes_params = [{}, {"nlist": 128}, {"nlist": 128}, {"nlist": 128, "m": 16, "nbits": 8},
                              {"M": 32, "efConstruction": 360}, {"nlist": 128}, {},
                              {}, {"nlist": 64},
                              {"drop_ratio_build": 0.2}, {"drop_ratio_build": 0.2},
                              {"nlist": 64}, {"nlist": 64, "m": 16, "nbits": 8}]

default_all_search_params_params = [{}, {"nprobe": 32}, {"nprobe": 32}, {"nprobe": 32},
                                    {"ef": 100}, {"nprobe": 32, "reorder_k": 100}, {"search_list": 30},
                                    {}, {"nprobe": 32},
                                    {"drop_ratio_search": "0.2"}, {"drop_ratio_search": "0.2"},
                                    {}, {}]

Handler_type = ["GRPC", "HTTP"]
binary_support = ["BIN_FLAT", "BIN_IVF_FLAT"]
sparse_support = ["SPARSE_INVERTED_INDEX", "SPARSE_WAND"]
default_L0_metric = "COSINE"
float_metrics = ["L2", "IP", "COSINE"]
binary_metrics = ["JACCARD", "HAMMING", "SUBSTRUCTURE", "SUPERSTRUCTURE"]
structure_metrics = ["SUBSTRUCTURE", "SUPERSTRUCTURE"]
all_scalar_data_types = ['int8', 'int16', 'int32', 'int64', 'float', 'double', 'bool', 'varchar']


default_flat_index = {"index_type": "FLAT", "params": {}, "metric_type": default_L0_metric}
default_bin_flat_index = {"index_type": "BIN_FLAT", "params": {}, "metric_type": "JACCARD"}
default_sparse_inverted_index = {"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP",
                                 "params": {"drop_ratio_build": 0.2}}

default_search_params = {"params": default_all_search_params_params[2].copy()}
default_search_ip_params = {"metric_type": "IP", "params": default_all_search_params_params[2].copy()}
default_search_binary_params = {"metric_type": "JACCARD", "params": {"nprobe": 32}}
default_index = {"index_type": "IVF_SQ8", "metric_type": default_L0_metric, "params": default_all_indexes_params[2].copy()}
default_binary_index = {"index_type": "BIN_IVF_FLAT", "metric_type": "JACCARD", "params": default_all_indexes_params[8].copy()}
default_diskann_index = {"index_type": "DISKANN", "metric_type": default_L0_metric, "params": {}}
default_diskann_search_params = {"params": {"search_list": 30}}
default_sparse_search_params = {"metric_type": "IP", "params": {"drop_ratio_search": "0.2"}}


class CheckTasks:
    """ The name of the method used to check the result """
    check_nothing = "check_nothing"
    err_res = "error_response"
    ccr = "check_connection_result"
    check_collection_property = "check_collection_property"
    check_partition_property = "check_partition_property"
    check_search_results = "check_search_results"
    check_search_iterator = "check_search_iterator"
    check_query_results = "check_query_results"
    check_query_iterator = "check_query_iterator"
    check_query_empty = "check_query_empty"  # verify that query result is empty
    check_query_not_empty = "check_query_not_empty"
    check_distance = "check_distance"
    check_delete_compact = "check_delete_compact"
    check_merge_compact = "check_merge_compact"
    check_role_property = "check_role_property"
    check_permission_deny = "check_permission_deny"
    check_auth_failure = "check_auth_failure"
    check_value_equal = "check_value_equal"
    check_rg_property = "check_resource_group_property"
    check_describe_collection_property = "check_describe_collection_property"
    check_collection_fields_properties = "check_collection_fields_properties"


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
