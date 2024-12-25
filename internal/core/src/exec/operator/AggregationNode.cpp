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
#include "common/Utils.h"

namespace milvus {
namespace exec {

PhyAggregationNode::PhyAggregationNode(
    int32_t operator_id,
    milvus::exec::DriverContext* ctx,
    const std::shared_ptr<const plan::AggregationNode>& node)
    : Operator(ctx, node->output_type(), operator_id, node->id()),
      aggregationNode_(node),
      isGlobal_(node->GroupingKeys().empty()),
      group_limit_(node->group_limit()) {
}

void
PhyAggregationNode::prepareOutput(vector_size_t size) {
    if (output_) {
        VectorPtr new_output = std::move(output_);
        BaseVector::prepareForReuse(new_output, size);
        output_ = std::static_pointer_cast<RowVector>(new_output);
    } else {
        output_ = std::make_shared<RowVector>(output_type_, size);
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
    prepareOutput(outputRowCount);
    const bool hasData = grouping_set_->getOutput(output_);
    if (!hasData) {
        return nullptr;
    }
    numOutputRows_ += output_->size();
    return output_;
}

void
PhyAggregationNode::initialize() {
    Operator::initialize();
    const auto& input_type = aggregationNode_->sources()[0]->output_type();
    auto hashers =
        createVectorHashers(input_type, aggregationNode_->GroupingKeys());
    auto numHashers = hashers.size();
    std::vector<AggregateInfo> aggregateInfos =
        toAggregateInfo(*aggregationNode_, *operator_context_, numHashers);
    grouping_set_ =
        std::make_unique<GroupingSet>(input_type,
                                      std::move(hashers),
                                      std::move(aggregateInfos),
                                      aggregationNode_->ignoreNullKeys(),
                                      aggregationNode_->group_limit());
    aggregationNode_.reset();
}

void
PhyAggregationNode::AddInput(milvus::RowVectorPtr& input) {
    grouping_set_->addInput(input);
    numInputRows_ += input->size();
}

};  // namespace exec
};  // namespace milvus
