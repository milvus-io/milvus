// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "MvccNode.h"

namespace milvus {
namespace exec {

PhyMvccNode::PhyMvccNode(int32_t operator_id,
                         DriverContext* driverctx,
                         const std::shared_ptr<const plan::MvccNode>& mvcc_node)
    : Operator(
          driverctx, mvcc_node->output_type(), operator_id, mvcc_node->id()) {
    ExecContext* exec_context = operator_context_->get_exec_context();
    QueryContext* query_context = exec_context->get_query_context();
    segment_ = query_context->get_segment();
    query_timestamp_ = query_context->get_query_timestamp();
    active_count_ = query_context->get_active_count();
    is_source_node_ = mvcc_node->sources().size() == 0;
    collection_ttl_ = query_context->get_collection_ttl();
}

void
PhyMvccNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhyMvccNode::GetOutput() {
    if (is_finished_) {
        return nullptr;
    }

    if (!is_source_node_ && input_ == nullptr) {
        return nullptr;
    }

    if (active_count_ == 0) {
        is_finished_ = true;
        return nullptr;
    }
    // the first vector is filtering result and second bitset is a valid bitset
    // if valid_bitset[i]==false, means result[i] is null
    auto col_input = is_source_node_ ? std::make_shared<ColumnVector>(
                                           TargetBitmap(active_count_),
                                           TargetBitmap(active_count_))
                                     : GetColumnVector(input_);

    TargetBitmapView data(col_input->GetRawData(), col_input->size());
    // need to expose null?
    segment_->mask_with_timestamps(data, query_timestamp_, collection_ttl_);
    segment_->mask_with_delete(data, active_count_, query_timestamp_);
    is_finished_ = true;

    // input_ have already been updated
    return std::make_shared<RowVector>(std::vector<VectorPtr>{col_input});
}

bool
PhyMvccNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus