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

#include "segment/SegmentReader.h"
#include "utils/Json.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ExecutionEngine.h"
#include "db/attr/Attr.h"
#include "db/attr/AttrIndex.h"
#include "knowhere/index/vector_index/VecIndex.h"

namespace milvus {
namespace engine {

class ExecutionEngineImpl : public ExecutionEngine {
 public:
    ExecutionEngineImpl(uint16_t dimension, const std::string& location, EngineType index_type, MetricType metric_type,
                        const milvus::json& index_params);

    ExecutionEngineImpl(knowhere::VecIndexPtr index, const std::string& location, EngineType index_type,
                        MetricType metric_type, const milvus::json& index_params);

    Status
    AddWithIds(int64_t n, const float* xdata, const int64_t* xids) override;

    Status
    AddWithIds(int64_t n, const uint8_t* xdata, const int64_t* xids) override;

    size_t
    Count() const override;

    size_t
    Dimension() const override;

    size_t
    Size() const override;

    Status
    Serialize() override;

    Status
    Load(bool to_cache) override;

    Status
    LoadAttr(bool to_cache) override;

    Status
    CopyToGpu(uint64_t device_id, bool hybrid = false) override;

    Status
    CopyToIndexFileToGpu(uint64_t device_id) override;

    Status
    CopyToCpu() override;

#if 0
    Status
    GetVectorByID(const int64_t id, float* vector, bool hybrid) override;

    Status
    GetVectorByID(const int64_t id, uint8_t* vector, bool hybrid) override;
#endif

    Status
    ExecBinaryQuery(query::GeneralQueryPtr general_query, faiss::ConcurrentBitsetPtr& bitset,
                    std::unordered_map<std::string, meta::hybrid::DataType>& attr_type,
                    std::string& vector_placeholder) override;

    Status
    HybridSearch(scheduler::SearchJobPtr job, std::unordered_map<std::string, meta::hybrid::DataType>& attr_type,
                 std::vector<float>& distances, std::vector<int64_t>& search_ids, bool hybrid) override;

    Status
    Search(std::vector<int64_t>& ids, std::vector<float>& distances, scheduler::SearchJobPtr job, bool hybrid) override;

    ExecutionEnginePtr
    BuildIndex(const std::string& location, EngineType engine_type) override;

    Status
    Cache() override;

    Status
    AttrCache() override;

    Status
    Init() override;

    EngineType
    IndexEngineType() const override {
        return index_type_;
    }

    MetricType
    IndexMetricType() const override {
        return metric_type_;
    }

    std::string
    GetLocation() const override {
        return location_;
    }

    std::string
    GetAttrLocation() const override {
        return attr_location_;
    }

 private:
    knowhere::VecIndexPtr
    CreatetVecIndex(EngineType type);

    knowhere::VecIndexPtr
    Load(const std::string& location);

    Status
    ProcessTermQuery(faiss::ConcurrentBitsetPtr& bitset, query::TermQueryPtr term_query,
                     std::unordered_map<std::string, meta::hybrid::DataType>& attr_type);

    Status
    IndexedTermQuery(faiss::ConcurrentBitsetPtr& bitset, const std::string& field_name,
                     const meta::hybrid::DataType& data_type, milvus::json& term_values_json);

    Status
    ProcessRangeQuery(const std::unordered_map<std::string, meta::hybrid::DataType>& attr_type,
                      faiss::ConcurrentBitsetPtr& bitset, query::RangeQueryPtr range_query);

    Status
    IndexedRangeQuery(faiss::ConcurrentBitsetPtr& bitset, const meta::hybrid::DataType& data_type,
                      knowhere::IndexPtr& index_ptr, milvus::json& range_values_json);

    void
    HybridLoad() const;

    void
    HybridUnset() const;

 protected:
    knowhere::VecIndexPtr index_ = nullptr;
    std::string location_;
    int64_t dim_;
    EngineType index_type_;
    MetricType metric_type_;

    Attr::AttrIndexPtr attr_index_ = nullptr;

    Attr::AttrPtr attr_ = nullptr;

    std::string attr_location_;

    milvus::json index_params_;
    int64_t gpu_num_ = 0;
};

}  // namespace engine
}  // namespace milvus
