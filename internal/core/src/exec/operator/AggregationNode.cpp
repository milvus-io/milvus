//
// Created by hanchun on 24-10-18.
//

#include "AggregationNode.h"

namespace milvus {
namespace exec {

PhyAggregationNode::PhyAggregationNode(int32_t operator_id,
                                       milvus::exec::DriverContext *ctx,
                                       const std::shared_ptr<const plan::AggregationNode> &node):
                                       Operator(ctx, node->output_type(), operator_id, node->id()),
                                       aggregationNode_(node),
                                       isGlobal_(node->GroupingKeys().empty()){

}

void PhyAggregationNode::prepareOutput(vector_size_t size){
    if (output_) {
        VectorPtr new_output = std::move(output_);
        BaseVector::prepareForReuse(new_output, size);
        output_ = std::static_pointer_cast<RowVector>(new_output);
    } else {
        output_ = std::make_shared<RowVector>(output_type_, size, 0);
    }
}

RowVectorPtr PhyAggregationNode::GetOutput() {
  if (finished_||(!no_more_input_ && !grouping_set_->hasOutput())) {
      input_ = nullptr;
      return nullptr;
  }

  const auto& queryConfig = operator_context_->get_driver_context()->GetQueryConfig();
  auto batch_size = queryConfig->get_expr_batch_size();
  const auto outputRowCount = isGlobal_? 1: batch_size;
  prepareOutput(outputRowCount);
  const bool hasData = grouping_set_->getOutput(output_);
  if (!hasData) {
      if (no_more_input_) {
        finished_ = true;
      }
      return nullptr;
  }
  numOutputRows_ += output_->size();
  return output_;
}

void PhyAggregationNode::initialize() {
    Operator::initialize();
    const auto& input_type = aggregationNode_->sources()[0]->output_type();
    auto hashers = createVectorHashers(input_type, aggregationNode_->GroupingKeys());
    auto numHashers = hashers.size();
    std::vector<AggregateInfo> aggregateInfos = toAggregateInfo(*aggregationNode_,
                                                                *operator_context_,
                                                                numHashers);

    // Check that aggregate result type match the output type.
    for (auto i = 0; i < aggregateInfos.size(); i++) {
        const auto aggResultType = aggregateInfos[i].function_->resultType();
        const auto expectedType = output_type_->column_type(numHashers + i);
        AssertInfo(aggResultType==expectedType,
                   "Unexpected result type for an aggregation: {}, expected {}, step {}",
                   aggResultType,
                   expectedType,
                   plan::AggregationNode::stepName(aggregationNode_->step()));
    }

    grouping_set_ = std::make_unique<GroupingSet>(
            input_type,
            std::move(hashers),
            std::move(aggregateInfos),
            !aggregationNode_->ignoreNullKeys(),
            isRawInput(aggregationNode_->step()));
    aggregationNode_.reset();
}

void PhyAggregationNode::AddInput(milvus::RowVectorPtr& input) {
    grouping_set_->addInput(input);
    numInputRows_ += input->size();
}

};
};

