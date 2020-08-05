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
    CreateVecIndex(const std::string& index_name);

    Status
    CreateStructuredIndex(const engine::DataType field_type, engine::BinaryDataPtr& raw_data,
                          knowhere::IndexPtr& index_ptr);

    Status
    LoadForSearch(const query::QueryPtr& query_ptr);

    Status
    Load(const TargetFields& field_names);

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
    segment::SegmentReaderPtr segment_reader_;
    TargetFields target_fields_;
    ExecutionEngineContext context_;

    int64_t entity_count_;

    int64_t gpu_num_ = 0;
    bool gpu_enable_ = false;
};

}  // namespace engine
}  // namespace milvus
