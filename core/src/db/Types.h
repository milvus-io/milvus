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

#include <faiss/Index.h>

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/engine/ExecutionEngine.h"
#include "segment/Types.h"
#include "utils/Json.h"

namespace milvus {
namespace engine {

typedef segment::doc_id_t IDNumber;
typedef IDNumber* IDNumberPtr;
typedef std::vector<IDNumber> IDNumbers;

typedef std::vector<faiss::Index::idx_t> ResultIds;
typedef std::vector<faiss::Index::distance_t> ResultDistances;

struct CollectionIndex {
    int32_t engine_type_ = (int)EngineType::FAISS_IDMAP;
    int32_t metric_type_ = (int)MetricType::L2;
    milvus::json extra_params_ = {{"nlist", 16384}};
};

struct VectorsData {
    uint64_t vector_count_ = 0;
    std::vector<float> float_data_;
    std::vector<uint8_t> binary_data_;
    IDNumbers id_array_;
};

struct Entity {
    uint64_t entity_count_ = 0;
    std::vector<uint8_t> attr_value_;
    std::unordered_map<std::string, std::vector<std::string>> attr_data_;
    std::unordered_map<std::string, VectorsData> vector_data_;
    IDNumbers id_array_;
};

using File2ErrArray = std::map<std::string, std::vector<std::string>>;
using Table2FileErr = std::map<std::string, File2ErrArray>;
using File2RefCount = std::map<std::string, int64_t>;
using Table2FileRef = std::map<std::string, File2RefCount>;

static const char* DEFAULT_PARTITON_TAG = "_default";

}  // namespace engine
}  // namespace milvus
