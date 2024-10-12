//
// Created by hanchun on 24-10-22.
//
#include "AggregateInfo.h"
#include "common/Types.h"

namespace milvus{
namespace exec{

std::vector<AggregateInfo> toAggregateInfo(
        const plan::AggregationNode& aggregationNode,
        const milvus::exec::OperatorContext& operatorCtx,
        uint32_t numKeys){
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
        for (const auto& inputExpr: aggregate.call_->inputs()) {
            if (auto fieldExpr = dynamic_cast<const expr::CallExpr*>(inputExpr.get())) {
                //inputColumnIdxes.emplace_back(inputType->GetChildIndex(fieldExpr->fun_name()));
            }
        }
        auto index = numKeys + i;
        const auto& aggResultType = outputType->column_type(index);
        info.function_ = Aggregate::create(
                aggregate.call_->fun_name(),
                isPartialOutput(step)? plan::AggregationNode::Step::kPartial:plan::AggregationNode::Step::kSingle,
                aggregate.rawInputTypes_,
                aggResultType);
        info.output_ = index;
        aggregates.emplace_back(std::move(info));
    }
    return aggregates;
}

}
}