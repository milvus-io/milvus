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

#include <stdint.h>
#include "Types.h"

#include "knowhere/comp/index_param.h"

const int64_t INVALID_FIELD_ID = -1;
const int64_t INVALID_SEG_OFFSET = -1;
const int64_t INVALID_ARRAY_INDEX = -1;
const milvus::PkType INVALID_PK;  // of std::monostate if not set.
// TODO: default field start id, could get from config.yaml
const int64_t START_USER_FIELDID = 100;
const char MAX_LENGTH[] = "max_length";

// const fieldID (rowID and timestamp)
const milvus::FieldId RowFieldID = milvus::FieldId(0);
const milvus::FieldId TimestampFieldID = milvus::FieldId(1);

// fill followed extra info to binlog file
const char ORIGIN_SIZE_KEY[] = "original_size";
const char INDEX_BUILD_ID_KEY[] = "indexBuildID";
const char NULLABLE[] = "nullable";
const char EDEK[] = "edek";
const char EZID[] = "encryption_zone";

const char INDEX_ROOT_PATH[] = "index_files";
const char RAWDATA_ROOT_PATH[] = "raw_datas";
const char ANALYZE_ROOT_PATH[] = "analyze_stats";
const char CENTROIDS_NAME[] = "centroids";
const char OFFSET_MAPPING_NAME[] = "offset_mapping";
const char NUM_CLUSTERS[] = "num_clusters";
const char KMEANS_CLUSTER[] = "KMEANS";
const char VEC_OPT_FIELDS[] = "opt_fields";
const char PAGE_RETAIN_ORDER[] = "page_retain_order";
const char TEXT_LOG_ROOT_PATH[] = "text_log";
const char ITERATIVE_FILTER[] = "iterative_filter";
const char HINTS[] = "hints";
// json stats related
const char JSON_KEY_INDEX_LOG_ROOT_PATH[] = "json_key_index_log";
const char NGRAM_LOG_ROOT_PATH[] = "ngram_log";
constexpr const char* JSON_STATS_ROOT_PATH = "json_stats";
constexpr const char* JSON_STATS_DATA_FORMAT_VERSION = "2";
constexpr const char* JSON_STATS_SHARED_INDEX_PATH = "shared_key_index";
constexpr const char* JSON_STATS_SHREDDING_DATA_PATH = "shredding_data";
constexpr const char* JSON_KEY_STATS_SHARED_FIELD_NAME = "__shared";
// store key layout type in parquet file metadata
inline constexpr const char* JSON_STATS_META_KEY_LAYOUT_TYPE_MAP =
    "key_layout_type_map";
// start json stats field id for mock column
// max user field id is 1024, so start json stats field id from 2000
// new field numbers will not exceed 10000 fields
const int64_t START_JSON_STATS_FIELD_ID = 2000;
const int64_t END_JSON_STATS_FIELD_ID = 12000;

const char DEFAULT_PLANNODE_ID[] = "0";
const char DEAFULT_QUERY_ID[] = "0";
const char DEFAULT_TASK_ID[] = "0";

const int64_t DEFAULT_FIELD_MAX_MEMORY_LIMIT = 128 << 20;  // bytes

const int64_t DEFAULT_INDEX_FILE_SLICE_SIZE = 16 << 20;  // bytes

const int64_t DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE = 8192;

const int64_t DEFAULT_DELETE_DUMP_BATCH_SIZE = 10000;

constexpr const char* RADIUS = knowhere::meta::RADIUS;
constexpr const char* RANGE_FILTER = knowhere::meta::RANGE_FILTER;

const int64_t DEFAULT_MAX_OUTPUT_SIZE = 67108864;  // bytes, 64MB

const int64_t DEFAULT_CHUNK_MANAGER_REQUEST_TIMEOUT_MS = 10000;

const int64_t DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND = 500;

const int64_t DEFAULT_HYBRID_INDEX_BITMAP_CARDINALITY_LIMIT = 100;

const size_t MARISA_NULL_KEY_ID = -1;

const std::string JSON_CAST_TYPE = "json_cast_type";
const std::string JSON_PATH = "json_path";
const std::string JSON_CAST_FUNCTION = "json_cast_function";
const bool DEFAULT_OPTIMIZE_EXPR_ENABLED = true;
const int64_t DEFAULT_CONVERT_OR_TO_IN_NUMERIC_LIMIT = 150;
const int64_t DEFAULT_JSON_INDEX_MEMORY_BUDGET = 16777216;  // bytes, 16MB
const bool DEFAULT_GROWING_JSON_KEY_STATS_ENABLED = false;
const bool DEFAULT_CONFIG_PARAM_TYPE_CHECK_ENABLED = true;

// index config related
const std::string SEGMENT_INSERT_FILES_KEY = "segment_insert_files";
const std::string INSERT_FILES_KEY = "insert_files";
const std::string PARTITION_KEY_ISOLATION_KEY = "partition_key_isolation";
const std::string STORAGE_VERSION_KEY = "storage_version";
const std::string DIM_KEY = "dim";
const std::string DATA_TYPE_KEY = "data_type";
const std::string ELEMENT_TYPE_KEY = "element_type";
const std::string INDEX_NUM_ROWS_KEY = "index_num_rows";

// storage version
const int64_t STORAGE_V1 = 1;
const int64_t STORAGE_V2 = 2;

const std::string UNKNOW_CAST_FUNCTION_NAME = "unknown";

const int64_t DEFAULT_SHORT_COLUMN_GROUP_ID = 0;

// VectorArray related, used for fetch metadata from Arrow schema
const std::string ELEMENT_TYPE_KEY_FOR_ARROW = "elementType";
