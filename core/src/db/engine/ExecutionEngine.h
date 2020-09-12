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
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Types.h"
#include "query/GeneralQuery.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

using TargetFields = std::set<std::string>;

struct ExecutionEngineContext {
    query::QueryPtr query_ptr_;
    QueryResultPtr query_result_;
    TargetFields target_fields_;  // for build index task, which field should be build
};
using ExecutionEngineContextPtr = std::shared_ptr<ExecutionEngineContext>;

class ExecutionEngine {
 public:
    virtual Status
    Load(ExecutionEngineContext& context) = 0;

    virtual Status
    CopyToGpu(uint64_t device_id) = 0;

    virtual Status
    Search(ExecutionEngineContext& context) = 0;

    virtual Status
<<<<<<< HEAD
    BuildIndex() = 0;
=======
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
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
};

using ExecutionEnginePtr = std::shared_ptr<ExecutionEngine>;

}  // namespace engine
}  // namespace milvus
