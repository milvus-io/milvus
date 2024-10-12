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

#include "SearchGroupByNode.h"

#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include "monitor/prometheus_client.h"

namespace milvus {
namespace exec {

PhySearchGroupByNode::PhySearchGroupByNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::SearchGroupByNode>& node)
    : Operator(driverctx, node->output_type(), operator_id, node->id()) {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    segment_ = query_context_->get_segment();
    search_info_ = query_context_->get_search_info();
}

void
PhySearchGroupByNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhySearchGroupByNode::GetOutput() {
    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

    DeferLambda([&]() { is_finished_ = true; });
    if (input_ == nullptr) {
        return nullptr;
    }

    std::chrono::high_resolution_clock::time_point vector_start =
        std::chrono::high_resolution_clock::now();

    auto search_result = query_context_->get_search_result();
    if (search_result.vector_iterators_.has_value()) {
        AssertInfo(search_result.vector_iterators_.value().size() ==
                       search_result.total_nq_,
                   "Vector Iterators' count must be equal to total_nq_, Check "
                   "your code");
        std::vector<GroupByValueType> group_by_values;
        milvus::exec::SearchGroupBy(search_result.vector_iterators_.value(),
                                    search_info_,
                                    group_by_values,
                                    *segment_,
                                    search_result.seg_offsets_,
                                    search_result.distances_,
                                    search_result.topk_per_nq_prefix_sum_);
        search_result.group_by_values_ = std::move(group_by_values);
        search_result.group_size_ = search_info_.group_size_;
        AssertInfo(search_result.seg_offsets_.size() ==
                       search_result.group_by_values_.value().size(),
                   "Wrong state! search_result group_by_values_ size:{} is not "
                   "equal to search_result.seg_offsets.size:{}",
                   search_result.group_by_values_.value().size(),
                   search_result.seg_offsets_.size());
    }
    query_context_->set_search_result(std::move(search_result));
    std::chrono::high_resolution_clock::time_point vector_end =
        std::chrono::high_resolution_clock::now();
    double vector_cost =
        std::chrono::duration<double, std::micro>(vector_end - vector_start)
            .count();
    monitor::internal_core_search_latency_groupby.Observe(vector_cost / 1000);
    return input_;
}

bool
PhySearchGroupByNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus