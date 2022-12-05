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
default_search_ip_params = {"metric_type": "IP", "params": {"nprobe": 10}}
default_search_binary_params = {"metric_type": "JACCARD", "params": {"nprobe": 10}}
default_index_type = "HNSW"
default_index_params = {"index_type": "HNSW", "params": {"M": 48, "efConstruction": 500}, "metric_type": "L2"}
default_varchar_index = {}
default_binary_index = {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"}
default_diskann_index = {"index_type": "DISKANN", "metric_type": "L2", "params": {}}
default_diskann_search_params = {"metric_type": "L2", "params": {"search_list": 30}}
max_top_k = 16384
max_partition_num = 4096  # 256
default_segment_row_limit = 1000
default_server_segment_row_limit = 1024 * 512
default_alias = "default"
default_user = "root"
default_password = "Milvus"
default_bool_field_name = "Bool"
default_int8_field_name = "Int8"
default_int16_field_name = "Int16"
default_int32_field_name = "Int32"
default_int64_field_name = "Int64"
default_float_field_name = "Float"
default_double_field_name = "Double"
default_string_field_name = "Varchar"
default_float_vec_field_name = "FloatVector"
another_float_vec_field_name = "FloatVector1"
default_binary_vec_field_name = "BinaryVector"
default_partition_name = "_default"
default_tag = "1970_01_01"
row_count = "row_count"
default_length = 65535
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
compact_segment_num_threshold = 4
compact_delta_ratio_reciprocal = 5  # compact_delta_binlog_ratio is 0.2
compact_retention_duration = 40  # compaction travel time retention range 20s
max_compaction_interval = 60  # the max time interval (s) from the last compaction
max_field_num = 256  # Maximum number of fields in a collection

default_dsl = f"{default_int64_field_name} in [2,4,6,8]"
default_expr = f"{default_int64_field_name} in [2,4,6,8]"

metric_types = []
all_index_types = ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "HNSW", "ANNOY", "DISKANN", "BIN_FLAT", "BIN_IVF_FLAT"]
all_index_params_map = {"FLAT": {"index_type": "FLAT", "params": {"nlist": 128}, "metric_type": "L2"},
                        "IVF_FLAT": {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"},
                        "IVF_SQ8": {"index_type": "IVF_SQ8", "params": {"nlist": 128}, "metric_type": "L2"},
                        "IVF_PQ": {"index_type": "IVF_PQ", "params": {"nlist": 128, "m": 16, "nbits": 8},
                                   "metric_type": "L2"},
                        "HNSW": {"index_type": "HNSW", "params": {"M": 48, "efConstruction": 500}, "metric_type": "L2"},
                        "ANNOY": {"index_type": "ANNOY", "params": {"n_trees": 50}, "metric_type": "L2"},
                        "DISKANN": {"index_type": "DISKANN", "params": {}, "metric_type": "L2"},
                        "BIN_FLAT": {"index_type": "BIN_FLAT", "params": {"nlist": 128}, "metric_type": "JACCARD"},
                        "BIN_IVF_FLAT": {"index_type": "BIN_IVF_FLAT", "params": {"nlist": 128},
                                         "metric_type": "JACCARD"}
                        }
