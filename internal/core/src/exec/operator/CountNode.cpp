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

#include "CountNode.h"

namespace milvus {
namespace exec {

static std::unique_ptr<milvus::RetrieveResult>
wrap_num_entities(int64_t cnt, int64_t size) {
    auto retrieve_result = std::make_unique<milvus::RetrieveResult>();
    DataArray arr;
    arr.set_type(milvus::proto::schema::Int64);
    auto scalar = arr.mutable_scalars();
    scalar->mutable_long_data()->mutable_data()->Add(cnt);
    retrieve_result->field_data_ = {arr};
    retrieve_result->total_data_cnt_ = size;
    return retrieve_result;
}

PhyCountNode::PhyCountNode(int32_t operator_id,
                           DriverContext* driverctx,
                           const std::shared_ptr<const plan::CountNode>& node)
    : Operator(driverctx,
               node->output_type(),
               operator_id,
               node->id(),
               "PhyCountNode") {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    segment_ = query_context_->get_segment();
    query_timestamp_ = query_context_->get_query_timestamp();
    active_count_ = query_context_->get_active_count();
}

void
PhyCountNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhyCountNode::GetOutput() {
    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

    auto col_input = GetColumnVector(input_);
    TargetBitmapView view(col_input->GetRawData(), col_input->size());
    auto cnt = view.size() - view.count();
    query_context_->set_retrieve_result(
        std::move(*(wrap_num_entities(cnt, view.size()))));
    is_finished_ = true;

    return input_;
}

bool
PhyCountNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus