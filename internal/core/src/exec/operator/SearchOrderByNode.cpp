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

#include "SearchOrderByNode.h"
#include "common/Tracer.h"
#include "exec/operator/search-orderby/SearchOrderByOperator.h"

namespace milvus {
namespace exec {

PhySearchOrderByNode::PhySearchOrderByNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::SearchOrderByNode>& node)
    : Operator(driverctx,
               node->output_type(),
               operator_id,
               node->id(),
               "PhySearchOrderByNode") {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    segment_ = query_context_->get_segment();
    order_by_fields_ = node->order_by_fields();
}

void
PhySearchOrderByNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhySearchOrderByNode::GetOutput() {
    milvus::exec::checkCancellation(query_context_);
    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

    tracer::AutoSpan span(
        "PhySearchOrderByNode::Execute", tracer::GetRootSpan(), true);

    DeferLambda([&]() { is_finished_ = true; });
    if (input_ == nullptr) {
        return nullptr;
    }

    std::chrono::high_resolution_clock::time_point start =
        std::chrono::high_resolution_clock::now();

    auto op_context = query_context_->get_op_context();
    auto search_result = query_context_->get_search_result();

    // Execute order by operation
    milvus::exec::SearchOrderBy(op_context,
                                order_by_fields_,
                                *segment_,
                                search_result.seg_offsets_,
                                search_result.distances_,
                                search_result.group_by_values_,
                                search_result.topk_per_nq_prefix_sum_,
                                query_context_->get_metric_type());

    query_context_->set_search_result(std::move(search_result));

    std::chrono::high_resolution_clock::time_point end =
        std::chrono::high_resolution_clock::now();
    double cost =
        std::chrono::duration<double, std::micro>(end - start).count();
    // TODO: Add monitor metric for order by latency
    (void)cost;

    return input_;
}

bool
PhySearchOrderByNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus
