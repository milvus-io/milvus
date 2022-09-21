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

#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

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

constexpr const char* INDEX_TYPE = "index_type";
constexpr const char* INDEX_MODE = "index_mode";
constexpr const char* METRIC_TYPE = "metric_type";

// scalar index type
constexpr const char* ASCENDING_SORT = "STL_SORT";
constexpr const char* MARISA_TRIE = "Trie";

// index meta
constexpr const char* COLLECTION_ID = "collection_id";
constexpr const char* PARTITION_ID = "partition_id";
constexpr const char* SEGMENT_ID = "segment_id";
constexpr const char* FIELD_ID = "field_id";
constexpr const char* INDEX_BUILD_ID = "index_build_id";
constexpr const char* INDEX_ID = "index_id";
constexpr const char* INDEX_VERSION = "index_version";

// DiskAnn build params
constexpr const char* DISK_ANN_RAW_DATA_PATH = "data_path";
constexpr const char* DISK_ANN_MAX_DEGREE = "max_degree";
constexpr const char* DISK_ANN_BUILD_LIST = "build_list";
constexpr const char* DISK_ANN_SEARCH_DRAM_BUDGET = "search_dram_budget";
constexpr const char* DISK_ANN_BUILD_DRAM_BUDGET = "build_dram_budget";
constexpr const char* DISK_ANN_BUILD_THREAD_NUM = "num_build_thread";
constexpr const char* DISK_ANN_PQ_BYTES = "ps_disk_bytes";

// DiskAnn prepare params
constexpr const char* DISK_ANN_PREPARE_THREAD_NUM = "num_prepare_thread";
constexpr const char* NUM_ROW_OF_RAW_DATA = "count";
constexpr const char* DISK_ANN_PREPARE_WARM_UP = "warm_up";
constexpr const char* DISK_ANN_PREPARE_USE_BFS_CACHE = "use_bfs_cache";

// DiskAnn query params
constexpr const char* DISK_ANN_QUERY_LIST = "search_list";
constexpr const char* DISK_ANN_QUERY_BEAMWIDTH = "beamwidth";

// DiskAnn config name
constexpr const char* Disk_ANN_Build_Config = "diskANN_build_config";
constexpr const char* Disk_ANN_Prepare_Config = "diskANN_prepare_config";
constexpr const char* Disk_ANN_Query_Config = "diskANN_query_config";

}  // namespace milvus::index
