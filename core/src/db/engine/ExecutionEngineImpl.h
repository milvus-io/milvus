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

#include "ExecutionEngine.h"
#include "db/SnapshotVisitor.h"
#include "db/snapshot/CompoundOperations.h"
#include "segment/SegmentReader.h"

namespace milvus {
namespace engine {

class ExecutionEngineImpl : public ExecutionEngine {
 public:
    ExecutionEngineImpl(const std::string& dir_root, const SegmentVisitorPtr& segment_visitor);

    Status
    Load(ExecutionEngineContext& context) override;

    Status
    CopyToGpu(uint64_t device_id) override;

    Status
    Search(ExecutionEngineContext& context) override;

    Status
    BuildIndex() override;

 private:
    Status
    VecSearch(ExecutionEngineContext& context, const query::VectorQueryPtr& vector_param,
              knowhere::VecIndexPtr& vec_index, bool hybrid = false);

    knowhere::VecIndexPtr
    CreateVecIndex(const std::string& index_name, knowhere::IndexMode mode);

    Status
    CreateStructuredIndex(const engine::DataType field_type, engine::BinaryDataPtr& raw_data,
                          knowhere::IndexPtr& index_ptr);

#if 0
    Status
<<<<<<< HEAD
    LoadForSearch(const query::QueryPtr& query_ptr);

    Status
    Load(const TargetFields& field_names);
=======
    GetVectorByID(const int64_t id, float* vector, bool hybrid) override;

    Status
    GetVectorByID(const int64_t id, uint8_t* vector, bool hybrid) override;
#endif
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    Status
    ExecBinaryQuery(const query::GeneralQueryPtr& general_query, faiss::ConcurrentBitsetPtr& bitset,
                    std::unordered_map<std::string, DataType>& attr_type, std::string& vector_placeholder);

    Status
    ProcessTermQuery(faiss::ConcurrentBitsetPtr& bitset, const query::TermQueryPtr& term_query,
                     std::unordered_map<std::string, DataType>& attr_type);

    Status
    IndexedTermQuery(faiss::ConcurrentBitsetPtr& bitset, const std::string& field_name, const DataType& data_type,
                     milvus::json& term_values_json);

    Status
    ProcessRangeQuery(const std::unordered_map<std::string, DataType>& attr_type, faiss::ConcurrentBitsetPtr& bitset,
                      const query::RangeQueryPtr& range_query);

    Status
    IndexedRangeQuery(faiss::ConcurrentBitsetPtr& bitset, const DataType& data_type, knowhere::IndexPtr& index_ptr,
                      milvus::json& range_values_json);

    using AddSegmentFileOperation = std::shared_ptr<snapshot::ChangeSegmentFileOperation>;
    Status
    CreateSnapshotIndexFile(AddSegmentFileOperation& operation, const std::string& field_name,
                            CollectionIndex& index_info);

    Status
    BuildKnowhereIndex(const std::string& field_name, const CollectionIndex& index_info,
                       knowhere::VecIndexPtr& new_index);

 private:
<<<<<<< HEAD
    segment::SegmentReaderPtr segment_reader_;
    TargetFields target_fields_;
    ExecutionEngineContext context_;

    int64_t entity_count_;

    int64_t gpu_num_ = 0;
    bool gpu_enable_ = false;
=======
    knowhere::VecIndexPtr
    CreatetVecIndex(EngineType type);

    knowhere::VecIndexPtr
    Load(const std::string& location);

    void
    HybridLoad() const;

    void
    HybridUnset() const;

 protected:
    knowhere::VecIndexPtr index_ = nullptr;
#ifdef MILVUS_GPU_VERSION
    knowhere::VecIndexPtr index_reserve_ = nullptr;  // reserve the cpu index before copying it to gpu
#endif
    std::string location_;
    int64_t dim_;
    EngineType index_type_;
    MetricType metric_type_;

    std::unordered_map<std::string, DataType> attr_types_;
    std::unordered_map<std::string, std::vector<uint8_t>> attr_data_;
    std::unordered_map<std::string, size_t> attr_size_;
    query::BinaryQueryPtr binary_query_;
    int64_t vector_count_;

    milvus::json index_params_;
    int64_t gpu_num_ = 0;

    bool gpu_cache_enable_ = false;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
};

}  // namespace engine
}  // namespace milvus
