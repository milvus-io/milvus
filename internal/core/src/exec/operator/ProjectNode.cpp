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

#include "ProjectNode.h"
#include "exec/expression/Utils.h"
#include "segcore/Utils.h"

namespace milvus {
namespace exec {
PhyProjectNode::PhyProjectNode(
    int32_t operator_id,
    milvus::exec::DriverContext* ctx,
    const std::shared_ptr<const plan::ProjectNode>& projectNode)
    : Operator(ctx,
               projectNode->output_type(),
               operator_id,
               projectNode->id(),
               "Project"),
      fields_to_project_(projectNode->FieldsToProject()) {
    auto exec_context = operator_context_->get_exec_context();
    segment_ = exec_context->get_query_context()->get_segment();
}

void
PhyProjectNode::AddInput(milvus::RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhyProjectNode::GetOutput() {
    if (is_finished_ || input_ == nullptr) {
        return nullptr;
    }
    auto col_input = GetColumnVector(input_);
    // raw data view
    TargetBitmapView raw_data_view(col_input->GetRawData(), col_input->size());
    TargetBitmap raw_data_bitset(raw_data_view);
    auto result_pair = segment_->find_first(-1, raw_data_bitset);
    auto selected_offsets = result_pair.first;
    auto selected_count = selected_offsets.size();
    auto row_type = OutputType();
    std::vector<VectorPtr> column_vectors;
    for (int i = 0; i < fields_to_project_.size(); i++) {
        auto column_type = row_type->column_type(i);
        auto field_id = fields_to_project_.at(i);

        TargetBitmap valid_map(selected_count);
        TargetBitmapView valid_view(valid_map.data(), selected_count);
        auto field_data = bulk_script_field_data(field_id,
                                                 column_type,
                                                 selected_offsets.data(),
                                                 selected_count,
                                                 segment_,
                                                 valid_view);
        auto column_vector = std::make_shared<ColumnVector>(
            std::move(field_data), std::move(valid_map));
        column_vectors.emplace_back(column_vector);
    }
    is_finished_ = true;
    auto row_vector = std::make_shared<RowVector>(std::move(column_vectors));
    return row_vector;
}

};  // namespace exec
};  // namespace milvus