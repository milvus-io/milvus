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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <faiss/utils/ConcurrentBitset.h>

#include "query/GeneralQuery.h"
#include "utils/Json.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

// TODO(linxj): replace with VecIndex::IndexType
enum class EngineType {
    INVALID = 0,
    FAISS_IDMAP = 1,
    FAISS_IVFFLAT,
    FAISS_IVFSQ8,
    NSG_MIX,
    FAISS_IVFSQ8H,
    FAISS_PQ,
    SPTAG_KDT,
    SPTAG_BKT,
    FAISS_BIN_IDMAP,
    FAISS_BIN_IVFFLAT,
    HNSW,
    ANNOY,
    MAX_VALUE = ANNOY,
};

enum class MetricType {
    L2 = 1,              // Euclidean Distance
    IP = 2,              // Cosine Similarity
    HAMMING = 3,         // Hamming Distance
    JACCARD = 4,         // Jaccard Distance
    TANIMOTO = 5,        // Tanimoto Distance
    SUBSTRUCTURE = 6,    // Substructure Distance
    SUPERSTRUCTURE = 7,  // Superstructure Distance
    MAX_VALUE = SUPERSTRUCTURE
};

enum class DataType {
    INT8 = 1,
    INT16 = 2,
    INT32 = 3,
    INT64 = 4,

    STRING = 20,

    BOOL = 30,

    FLOAT = 40,
    DOUBLE = 41,

    VECTOR = 100,
    UNKNOWN = 9999,
};

class ExecutionEngine {
 public:
    virtual Status
    AddWithIds(int64_t n, const float* xdata, const int64_t* xids) = 0;

    virtual Status
    AddWithIds(int64_t n, const uint8_t* xdata, const int64_t* xids) = 0;

    virtual size_t
    Count() const = 0;

    virtual size_t
    Dimension() const = 0;

    virtual size_t
    Size() const = 0;

    virtual Status
    Serialize() = 0;

    virtual Status
    Load(bool to_cache = true) = 0;

    virtual Status
    CopyToGpu(uint64_t device_id, bool hybrid) = 0;

    virtual Status
    CopyToIndexFileToGpu(uint64_t device_id) = 0;

    virtual Status
    CopyToCpu() = 0;

    //    virtual std::shared_ptr<ExecutionEngine>
    //    Clone() = 0;

    //    virtual Status
    //    Merge(const std::string& location) = 0;

#if 0
    virtual Status
    GetVectorByID(const int64_t id, float* vector, bool hybrid) = 0;

    virtual Status
    GetVectorByID(const int64_t id, uint8_t* vector, bool hybrid) = 0;
#endif

    virtual Status
    ExecBinaryQuery(query::GeneralQueryPtr general_query, faiss::ConcurrentBitsetPtr bitset,
                    std::unordered_map<std::string, DataType>& attr_type, uint64_t& nq, uint64_t& topk,
                    std::vector<float>& distances, std::vector<int64_t>& labels) = 0;

    virtual Status
    Search(int64_t n, const float* data, int64_t k, const milvus::json& extra_params, float* distances, int64_t* labels,
           bool hybrid) = 0;

    virtual Status
    Search(int64_t n, const uint8_t* data, int64_t k, const milvus::json& extra_params, float* distances,
           int64_t* labels, bool hybrid) = 0;

    virtual std::shared_ptr<ExecutionEngine>
    BuildIndex(const std::string& location, EngineType engine_type) = 0;

    virtual Status
    Cache() = 0;

    virtual Status
    Init() = 0;

    virtual EngineType
    IndexEngineType() const = 0;

    virtual MetricType
    IndexMetricType() const = 0;

    virtual std::string
    GetLocation() const = 0;
};

using ExecutionEnginePtr = std::shared_ptr<ExecutionEngine>;

}  // namespace engine
}  // namespace milvus
