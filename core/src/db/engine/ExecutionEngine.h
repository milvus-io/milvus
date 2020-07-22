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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <faiss/utils/ConcurrentBitset.h>

#include "db/meta/MetaTypes.h"
#include "query/GeneralQuery.h"
#include "utils/Json.h"
#include "utils/Status.h"

namespace milvus {

namespace scheduler {
class SearchJob;
using SearchJobPtr = std::shared_ptr<SearchJob>;
}  // namespace scheduler

namespace engine {

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
    LoadAttr(bool to_cache = true) = 0;

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
    ExecBinaryQuery(query::GeneralQueryPtr general_query, faiss::ConcurrentBitsetPtr& bitset,
                    std::unordered_map<std::string, meta::hybrid::DataType>& attr_type,
                    std::string& vector_placeholder) = 0;

    virtual Status
    HybridSearch(scheduler::SearchJobPtr job, std::unordered_map<std::string, meta::hybrid::DataType>& attr_type,
                 std::vector<float>& distances, std::vector<int64_t>& search_ids, bool hybrid) = 0;

    virtual Status
    Search(std::vector<int64_t>& ids, std::vector<float>& distances, scheduler::SearchJobPtr job, bool hybrid) = 0;

    virtual std::shared_ptr<ExecutionEngine>
    BuildIndex(const std::string& location, EngineType engine_type) = 0;

    virtual Status
    Cache() = 0;

    virtual Status
    AttrCache() = 0;

    virtual Status
    Init() = 0;

    virtual EngineType
    IndexEngineType() const = 0;

    virtual MetricType
    IndexMetricType() const = 0;

    virtual std::string
    GetLocation() const = 0;

    virtual std::string
    GetAttrLocation() const = 0;
};

using ExecutionEnginePtr = std::shared_ptr<ExecutionEngine>;

}  // namespace engine
}  // namespace milvus
