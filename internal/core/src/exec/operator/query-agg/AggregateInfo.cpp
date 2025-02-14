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
// Created by hanchun on 24-10-22.
//
#include "AggregateInfo.h"
#include "common/Types.h"

namespace milvus {
namespace exec {

std::vector<AggregateInfo>
toAggregateInfo(const plan::AggregationNode& aggregationNode,
                const milvus::exec::OperatorContext& operatorCtx,
                uint32_t numKeys) {
    const auto numAggregates = aggregationNode.aggregates().size();
    std::vector<AggregateInfo> aggregates;
    aggregates.reserve(numAggregates);
    const auto& inputType = aggregationNode.sources()[0]->output_type();
    const auto& outputType = aggregationNode.output_type();
    const auto step = aggregationNode.step();

    for (auto i = 0; i < numAggregates; i++) {
        const auto& aggregate = aggregationNode.aggregates()[i];
        AggregateInfo info;
        auto& inputColumnIdxes = info.input_column_idxes_;
        for (const auto& inputExpr : aggregate.call_->inputs()) {
            if (auto fieldExpr = dynamic_cast<const expr::FieldAccessTypeExpr*>(
                    inputExpr.get())) {
                inputColumnIdxes.emplace_back(
                    inputType->GetChildIndex(fieldExpr->name()));
            } else if (inputExpr != nullptr) {
                PanicInfo(ExprInvalid,
                          "Only support aggregation towards column for now");
            }
        }
        auto index = numKeys + i;
        info.function_ = Aggregate::create(
            aggregate.call_->fun_name(),
            isPartialOutput(step) ? plan::AggregationNode::Step::kPartial
                                  : plan::AggregationNode::Step::kSingle,
            aggregate.rawInputTypes_,
            *(operatorCtx.get_exec_context()->get_query_config()));
        info.output_ = index;
        aggregates.emplace_back(std::move(info));
    }
    return aggregates;
}

}  // namespace exec
}  // namespace milvus