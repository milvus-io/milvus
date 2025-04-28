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

#include "VectorSearchNode.h"

#include "monitor/prometheus_client.h"

namespace milvus {
namespace exec {

static milvus::SearchResult
empty_search_result(int64_t num_queries) {
    milvus::SearchResult final_result;
    final_result.total_nq_ = num_queries;
    final_result.unity_topK_ = 0;  // no result
    final_result.total_data_cnt_ = 0;
    return final_result;
}

PhyVectorSearchNode::PhyVectorSearchNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::VectorSearchNode>& search_node)
    : Operator(driverctx,
               search_node->output_type(),
               operator_id,
               search_node->id(),
               "PhyVectorSearchNode") {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    segment_ = query_context_->get_segment();
    query_timestamp_ = query_context_->get_query_timestamp();
    active_count_ = query_context_->get_active_count();
    placeholder_group_ = query_context_->get_placeholder_group();
    search_info_ = query_context_->get_search_info();
}

void
PhyVectorSearchNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhyVectorSearchNode::GetOutput() {
    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

    DeferLambda([&]() { is_finished_ = true; });
    if (input_ == nullptr) {
        return nullptr;
    }

    std::chrono::high_resolution_clock::time_point vector_start =
        std::chrono::high_resolution_clock::now();

    auto& ph = placeholder_group_->at(0);
    auto src_data = ph.get_blob();
    auto num_queries = ph.num_of_queries_;
    milvus::SearchResult search_result;

    auto col_input = GetColumnVector(input_);
    TargetBitmapView view(col_input->GetRawData(), col_input->size());
    if (view.all()) {
        query_context_->set_search_result(
            std::move(empty_search_result(num_queries)));
        return input_;
    }

    // TODO: uniform knowhere BitsetView and milvus BitsetView
    milvus::BitsetView final_view((uint8_t*)col_input->GetRawData(),
                                  col_input->size());
    segment_->vector_search(search_info_,
                            src_data,
                            num_queries,
                            query_timestamp_,
                            final_view,
                            search_result);

    search_result.total_data_cnt_ = final_view.size();
    query_context_->set_search_result(std::move(search_result));
    std::chrono::high_resolution_clock::time_point vector_end =
        std::chrono::high_resolution_clock::now();
    double vector_cost =
        std::chrono::duration<double, std::micro>(vector_end - vector_start)
            .count();
    monitor::internal_core_search_latency_vector.Observe(vector_cost / 1000);
    // for now, vector search store result in query_context
    // this node interface just return bitset
    return input_;
}

bool
PhyVectorSearchNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus