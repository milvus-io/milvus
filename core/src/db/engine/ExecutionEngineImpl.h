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
#include "knowhere/index/vector_index/VecIndex.h"

namespace milvus {
namespace engine {

class ExecutionEngineImpl : public ExecutionEngine {
 public:
    ExecutionEngineImpl(uint16_t dimension, const std::string& location, EngineType index_type, MetricType metric_type,
                        const milvus::json& index_params, int64_t time_stamp);

    ExecutionEngineImpl(knowhere::VecIndexPtr index, const std::string& location, EngineType index_type,
                        MetricType metric_type, const milvus::json& index_params, int64_t time_stamp);

    size_t
    Count() const override;

    size_t
    Dimension() const override;

    size_t
    Size() const override;

    Status
    Serialize() override;

    Status
    Load(bool load_blacklist, bool to_cache) override;

    Status
    CopyToGpu(uint64_t device_id, bool hybrid = false) override;

    Status
    CopyToIndexFileToGpu(uint64_t device_id) override;

    Status
    CopyToCpu() override;

    Status
    CopyToFpga() override;

    Status
    CopyToApu(uint32_t row_count, std::string collection_name) override;

#if 0
    Status
    GetVectorByID(const int64_t id, float* vector, bool hybrid) override;

    Status
    GetVectorByID(const int64_t id, uint8_t* vector, bool hybrid) override;

    Status
    ExecBinaryQuery(query::GeneralQueryPtr general_query, faiss::ConcurrentBitsetPtr bitset,
                    std::unordered_map<std::string, DataType>& attr_type, uint64_t& nq, uint64_t& topk,
                    std::vector<float>& distances, std::vector<int64_t>& labels) override;
#endif

    Status
    Search(int64_t n, const float* data, int64_t k, const milvus::json& extra_params, float* distances, int64_t* labels,
           bool hybrid = false) override;

    Status
    Search(int64_t n, const uint8_t* data, int64_t k, const milvus::json& extra_params, float* distances,
           int64_t* labels, bool hybrid = false) override;

    ExecutionEnginePtr
    BuildIndex(const std::string& location, EngineType engine_type) override;

    Status
    Cache() override;

    Status
    FpgaCache() override;

    Status
    ReleaseCache() override;

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

 private:
    knowhere::IndexMode
    GetModeFromConfig();

    knowhere::VecIndexPtr
    CreatetVecIndex(EngineType type, knowhere::IndexMode mode);

    void
    HybridLoad() const;

    void
    HybridUnset() const;

 protected:
    knowhere::BlacklistPtr blacklist_ = nullptr;
    knowhere::VecIndexPtr index_ = nullptr;
#ifdef MILVUS_GPU_VERSION
    knowhere::VecIndexPtr index_reserve_ = nullptr;  // reserve the cpu index before copying it to gpu
#endif
    std::string location_;
    int64_t dim_;
    EngineType index_type_;
    MetricType metric_type_;

    milvus::json index_params_;
    int64_t gpu_num_ = 0;

    int64_t time_stamp_;
};

}  // namespace engine
}  // namespace milvus
