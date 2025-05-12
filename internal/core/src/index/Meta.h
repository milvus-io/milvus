// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "knowhere/comp/index_param.h"

namespace milvus::index {
constexpr const char* OPERATOR_TYPE = "operator_type";
constexpr const char* RANGE_VALUE = "range_value";
constexpr const char* LOWER_BOUND_VALUE = "lower_bound_value";
constexpr const char* LOWER_BOUND_INCLUSIVE = "lower_bound_inclusive";
constexpr const char* UPPER_BOUND_VALUE = "upper_bound_value";
constexpr const char* UPPER_BOUND_INCLUSIVE = "upper_bound_inclusive";
constexpr const char* PREFIX_VALUE = "prefix_value";
// below configurations will be persistent, do not edit them.
constexpr const char* MARISA_TRIE_INDEX = "marisa_trie_index";
constexpr const char* MARISA_STR_IDS = "marisa_trie_str_ids";

// below meta key of store bitmap indexes
constexpr const char* BITMAP_INDEX_DATA = "bitmap_index_data";
constexpr const char* BITMAP_INDEX_META = "bitmap_index_meta";
constexpr const char* BITMAP_INDEX_LENGTH = "bitmap_index_length";
constexpr const char* BITMAP_INDEX_NUM_ROWS = "bitmap_index_num_rows";

constexpr const char* INDEX_TYPE = "index_type";
constexpr const char* METRIC_TYPE = "metric_type";

// scalar index type
constexpr const char* ASCENDING_SORT = "STL_SORT";
constexpr const char* MARISA_TRIE = "Trie";
constexpr const char* MARISA_TRIE_UPPER = "TRIE";
constexpr const char* INVERTED_INDEX_TYPE = "INVERTED";
constexpr const char* BITMAP_INDEX_TYPE = "BITMAP";
constexpr const char* HYBRID_INDEX_TYPE = "HYBRID";
constexpr const char* SCALAR_INDEX_ENGINE_VERSION =
    "scalar_index_engine_version";
constexpr const char* TANTIVY_INDEX_VERSION = "tantivy_index_version";
constexpr uint32_t TANTIVY_INDEX_LATEST_VERSION = 7;
constexpr const char* INDEX_NON_ENCODING = "index.nonEncoding";
constexpr const char* MIN_NGRAM = "min_gram";
constexpr const char* MAX_NGRAM = "max_gram";

// index meta
constexpr const char* COLLECTION_ID = "collection_id";
constexpr const char* PARTITION_ID = "partition_id";
constexpr const char* SEGMENT_ID = "segment_id";
constexpr const char* FIELD_ID = "field_id";
constexpr const char* INDEX_BUILD_ID = "index_build_id";
constexpr const char* INDEX_ID = "index_id";
constexpr const char* INDEX_VERSION = "index_version";
constexpr const char* INDEX_ENGINE_VERSION = "index_engine_version";
constexpr const char* BITMAP_INDEX_CARDINALITY_LIMIT =
    "bitmap_cardinality_limit";

// index config key
constexpr const char* MMAP_FILE_PATH = "mmap_filepath";
constexpr const char* ENABLE_MMAP = "enable_mmap";
constexpr const char* INDEX_FILES = "index_files";
constexpr const char* ENABLE_OFFSET_CACHE = "indexoffsetcache.enabled";

// VecIndex file metas
constexpr const char* DISK_ANN_PREFIX_PATH = "index_prefix";
constexpr const char* DISK_ANN_RAW_DATA_PATH = "data_path";

// VecIndex node filtering
constexpr const char* VEC_OPT_FIELDS_PATH = "opt_fields_path";

// DiskAnn build params
constexpr const char* DISK_ANN_MAX_DEGREE = "max_degree";
constexpr const char* DISK_ANN_SEARCH_LIST_SIZE = "search_list_size";
constexpr const char* DISK_ANN_PQ_CODE_BUDGET = "pq_code_budget_gb";
constexpr const char* DISK_ANN_BUILD_DRAM_BUDGET = "build_dram_budget_gb";
constexpr const char* DISK_ANN_BUILD_THREAD_NUM = "num_build_thread";
constexpr const char* DISK_ANN_PQ_DIMS = "disk_pq_dims";
constexpr const char* DISK_ANN_THREADS_NUM = "num_threads";

// DiskAnn prepare params
constexpr const char* DISK_ANN_LOAD_THREAD_NUM = "num_load_thread";
constexpr const char* DISK_ANN_SEARCH_CACHE_BUDGET = "search_cache_budget_gb";
constexpr const char* DISK_ANN_PREPARE_WARM_UP = "warm_up";
constexpr const char* DISK_ANN_PREPARE_USE_BFS_CACHE = "use_bfs_cache";

// DiskAnn query params
constexpr const char* DISK_ANN_QUERY_LIST = "search_list";
constexpr const char* DISK_ANN_QUERY_BEAMWIDTH = "beamwidth";
}  // namespace milvus::index
