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

// #include <faiss/Index.h>

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// #include "utils/Json.h"

namespace milvus {
namespace engine {
///////////////////////////////////////////////////////////////////////////////////////////////////
using idx_t = int64_t;
using offset_t = int32_t;
using date_t = int32_t;
using distance_t = float;

using IDNumbers = std::vector<idx_t>;

// using VectorDistance = faiss::Index::distance_t;
// using VectorDistances = std::vector<VectorDistance>;

using ResultIds = std::vector<idx_t>;
using ResultDistances = std::vector<distance_t>;

///////////////////////////////////////////////////////////////////////////////////////////////////

enum class DataType {
    NONE = 0,
    BOOL = 1,
    INT8 = 2,
    INT16 = 3,
    INT32 = 4,
    INT64 = 5,

    FLOAT = 10,
    DOUBLE = 11,

    STRING = 20,

    VECTOR_BINARY = 100,
    VECTOR_FLOAT = 101,
};

///////////////////////////////////////////////////////////////////////////////////////////////////
enum class FieldElementType {
    FET_NONE = 0,
    FET_RAW = 1,
    FET_BLOOM_FILTER = 2,
    FET_DELETED_DOCS = 3,
    FET_INDEX = 4,
    FET_COMPRESS_SQ8 = 5,
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
// class BinaryData : public cache::DataObj {
// public:
//    int64_t
//    Size() {
//        return data_.size();
//    }
//
// public:
//    std::vector<uint8_t> data_;
//};
// using BinaryDataPtr = std::shared_ptr<BinaryData>;
//
/////////////////////////////////////////////////////////////////////////////////////////////////////
// class VaribleData : public cache::DataObj {
// public:
//    int64_t
//    Size() {
//        return data_.size() + offset_.size() * sizeof(int64_t);
//    }
//
// public:
//    std::vector<uint8_t> data_;
//    std::vector<int64_t> offset_;
//};
// using VaribleDataPtr = std::shared_ptr<VaribleData>;
//
/////////////////////////////////////////////////////////////////////////////////////////////////////
// using FIELD_TYPE_MAP = std::unordered_map<std::string, DataType>;
// using FIELD_WIDTH_MAP = std::unordered_map<std::string, int64_t>;
// using FIXEDX_FIELD_MAP = std::unordered_map<std::string, BinaryDataPtr>;
// using VARIABLE_FIELD_MAP = std::unordered_map<std::string, VaribleDataPtr>;
// using VECTOR_INDEX_MAP = std::unordered_map<std::string, knowhere::VecIndexPtr>;
// using STRUCTURED_INDEX_MAP = std::unordered_map<std::string, knowhere::IndexPtr>;

// ///////////////////////////////////////////////////////////////////////////////////////////////////
// struct DataChunk {
//     int64_t count_ = 0;
//     FIXEDX_FIELD_MAP fixed_fields_;
//     VARIABLE_FIELD_MAP variable_fields_;
// };
// using DataChunkPtr = std::shared_ptr<DataChunk>;

// ///////////////////////////////////////////////////////////////////////////////////////////////////
// struct CollectionIndex {
//     std::string index_name_;
//     std::string index_type_;
//     std::string metric_name_;
//     milvus::json extra_params_ = {{"nlist", 2048}};
// };

///////////////////////////////////////////////////////////////////////////////////////////////////
struct VectorsData {
    uint64_t vector_count_ = 0;
    std::vector<float> float_data_;
    std::vector<uint8_t> binary_data_;
    IDNumbers id_array_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////
struct AttrsData {
    uint64_t attr_count_ = 0;
    std::unordered_map<std::string, engine::DataType> attr_type_;
    std::unordered_map<std::string, std::vector<uint8_t>> attr_data_;
    IDNumbers id_array_;
};

}  // namespace engine
}  // namespace milvus
