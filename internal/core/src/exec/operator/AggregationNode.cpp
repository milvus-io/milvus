// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

//
// Created by hanchun on 24-10-18.
//

#include "AggregationNode.h"

#include <memory>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "exec/QueryContext.h"
#include "exec/VectorHasher.h"
#include "exec/expression/Utils.h"
#include "exec/operator/RawInput.h"
#include "exec/operator/query-agg/AggregateInfo.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {

PhyAggregationNode::PhyAggregationNode(
    int32_t operator_id,
    milvus::exec::DriverContext* ctx,
    const std::shared_ptr<const plan::AggregationNode>& node)
    : Operator(
          ctx, node->output_type(), operator_id, node->id(), "AggregationNode"),
      aggregationNode_(node),
      isGlobal_(node->GroupingKeys().empty()) {
}

void
PhyAggregationNode::initialize() {
    Operator::initialize();
    use_raw_input_ = aggregationNode_->UseRawInput();
    input_type_ = aggregationNode_->input_type() != nullptr
                      ? aggregationNode_->input_type()
                      : aggregationNode_->sources()[0]->output_type();
    raw_input_field_ids_ = aggregationNode_->RawInputFieldIds();
    if (use_raw_input_) {
        auto exec_context = operator_context_->get_exec_context();
        auto query_context = exec_context->get_query_context();
        segment_ = query_context->get_segment();
        op_context_ = query_context->get_op_context();
        AssertInfo(segment_, "segment cannot be nullptr for raw aggregation");
        AssertInfo(op_context_, "op context cannot be nullptr for raw aggregation");
        AssertInfo(input_type_ != nullptr,
                   "input type cannot be nullptr for raw aggregation");
        AssertInfo(input_type_->column_count() == raw_input_field_ids_.size(),
                   "raw aggregation input type and field id list must match");
    }
    auto hashers =
        createVectorHashers(input_type_, aggregationNode_->GroupingKeys());
    auto numHashers = hashers.size();
    std::vector<AggregateInfo> aggregateInfos =
        toAggregateInfo(*aggregationNode_, *operator_context_, numHashers);
    grouping_set_ = std::make_unique<GroupingSet>(
        input_type_, std::move(hashers), std::move(aggregateInfos));
    aggregationNode_.reset();
}

void
PhyAggregationNode::AddInput(RowVectorPtr& input) {
    if (use_raw_input_) {
        AddRawInput(input);
        return;
    }
    grouping_set_->addInput(input);
    numInputRows_ += input->size();
}

void
PhyAggregationNode::AddRawInput(RowVectorPtr& input) {
    auto filter_column = GetColumnVector(input);
    AssertInfo(filter_column->IsBitmap(),
               "raw aggregation expects bitmap input from upstream");
    TargetBitmapView filter_mask(filter_column->GetRawData(),
                                 filter_column->size());
    auto chunks = raw_input_field_ids_.empty()
                      ? SelectedChunkInput::FromChunkBounds(
                            {0, static_cast<int64_t>(filter_mask.size())},
                            filter_mask)
                      : SelectedChunkInput::FromSegment(
                            *segment_, raw_input_field_ids_.front(), filter_mask);
    for (const auto& chunk : chunks.chunks()) {
        RawInput raw_input(chunk);
        std::vector<std::unique_ptr<RawColumnPinHolderBase>> pins;
        pins.reserve(raw_input_field_ids_.size());
        for (size_t i = 0; i < raw_input_field_ids_.size(); ++i) {
            AddRawColumn(raw_input,
                         pins,
                         *segment_,
                         op_context_,
                         raw_input_field_ids_[i],
                         input_type_->column_type(i),
                         chunk);
        }
        grouping_set_->addRawInput(raw_input);
        numInputRows_ += raw_input.selected_count();
    }
}

RowVectorPtr
PhyAggregationNode::GetOutput() {
    if (finished_ || !no_more_input_) {
        input_ = nullptr;
        return nullptr;
    }
    DeferLambda([&]() { finished_ = true; });
    const auto outputRowCount = isGlobal_ ? 1 : grouping_set_->outputRowCount();
    output_ = std::make_shared<RowVector>(output_type_, outputRowCount);
    const bool hasData = grouping_set_->getOutput(output_);
    if (!hasData) {
        return nullptr;
    }
    numOutputRows_ += output_->size();
    return output_;
}

};  // namespace exec
};  // namespace milvus
