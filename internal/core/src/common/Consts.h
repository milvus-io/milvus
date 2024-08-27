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

const char INDEX_ROOT_PATH[] = "index_files";
const char RAWDATA_ROOT_PATH[] = "raw_datas";
const char ANALYZE_ROOT_PATH[] = "analyze_stats";
const char CENTROIDS_NAME[] = "centroids";
const char OFFSET_MAPPING_NAME[] = "offset_mapping";
const char NUM_CLUSTERS[] = "num_clusters";
const char KMEANS_CLUSTER[] = "KMEANS";
const char VEC_OPT_FIELDS[] = "opt_fields";
const char PAGE_RETAIN_ORDER[] = "page_retain_order";

const char DEFAULT_PLANNODE_ID[] = "0";
const char DEAFULT_QUERY_ID[] = "0";
const char DEFAULT_TASK_ID[] = "0";

const int64_t DEFAULT_FIELD_MAX_MEMORY_LIMIT = 128 << 20;  // bytes
const int64_t DEFAULT_HIGH_PRIORITY_THREAD_CORE_COEFFICIENT = 10;
const int64_t DEFAULT_MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT = 5;
const int64_t DEFAULT_LOW_PRIORITY_THREAD_CORE_COEFFICIENT = 1;

const int64_t DEFAULT_INDEX_FILE_SLICE_SIZE = 16 << 20;  // bytes

const int DEFAULT_CPU_NUM = 1;

const int64_t DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE = 8192;

constexpr const char* RADIUS = knowhere::meta::RADIUS;
constexpr const char* RANGE_FILTER = knowhere::meta::RANGE_FILTER;

const int64_t DEFAULT_MAX_OUTPUT_SIZE = 67108864;  // bytes, 64MB

const int64_t DEFAULT_CHUNK_MANAGER_REQUEST_TIMEOUT_MS = 10000;

const int64_t DEFAULT_BITMAP_INDEX_CARDINALITY_BOUND = 500;

const size_t MARISA_NULL_KEY_ID = -1;
