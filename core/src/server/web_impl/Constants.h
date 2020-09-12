// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "db/engine/ExecutionEngine.h"

#include <string>
#include <unordered_map>

namespace milvus {
namespace server {
namespace web {

extern const char* NAME_ENGINE_TYPE_FLAT;
extern const char* NAME_ENGINE_TYPE_IVFFLAT;
extern const char* NAME_ENGINE_TYPE_IVFSQ8;
extern const char* NAME_ENGINE_TYPE_IVFSQ8H;
extern const char* NAME_ENGINE_TYPE_RNSG;
extern const char* NAME_ENGINE_TYPE_IVFPQ;
extern const char* NAME_ENGINE_TYPE_HNSW;
extern const char* NAME_ENGINE_TYPE_ANNOY;

extern const char* NAME_METRIC_TYPE_L2;
extern const char* NAME_METRIC_TYPE_IP;
extern const char* NAME_METRIC_TYPE_HAMMING;
extern const char* NAME_METRIC_TYPE_JACCARD;
extern const char* NAME_METRIC_TYPE_TANIMOTO;
extern const char* NAME_METRIC_TYPE_SUBSTRUCTURE;
extern const char* NAME_METRIC_TYPE_SUPERSTRUCTURE;

////////////////////////////////////////////////////
extern const int64_t VALUE_COLLECTION_INDEX_FILE_SIZE_DEFAULT;
extern const char* VALUE_COLLECTION_METRIC_TYPE_DEFAULT;
extern const char* VALUE_PARTITION_TAG_DEFAULT;
extern const char* VALUE_INDEX_INDEX_TYPE_DEFAULT;
extern const int64_t VALUE_INDEX_NLIST_DEFAULT;
extern const int64_t VALUE_CONFIG_CPU_CACHE_CAPACITY_DEFAULT;
extern const bool VALUE_CONFIG_CACHE_INSERT_DATA_DEFAULT;

/////////////////////////////////////////////////////
extern const std::unordered_map<engine::EngineType, std::string> IndexMap;
extern const std::unordered_map<std::string, engine::EngineType> IndexNameMap;
extern const std::unordered_map<engine::MetricType, std::string> MetricMap;
extern const std::unordered_map<std::string, engine::MetricType> MetricNameMap;

}  // namespace web
}  // namespace server
}  // namespace milvus
