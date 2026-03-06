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

#include <algorithm>
#include <chrono>
#include <functional>
#include <ratio>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/ArrayOffsets.h"
#include "common/BitsetView.h"
#include "common/EasyAssert.h"
#include "common/QueryResult.h"
#include "common/Tracer.h"
#include "common/Utils.h"
#include "exec/QueryContext.h"
#include "exec/expression/Utils.h"
#include "exec/operator/Utils.h"
#include "monitor/Monitor.h"
#include "opentelemetry/trace/span.h"
#include "plan/PlanNode.h"
#include "prometheus/histogram.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentInterface.h"

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
    milvus::exec::checkCancellation(query_context_);

    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

    tracer::AutoSpan span(
        "PhyVectorSearchNode::Execute", tracer::GetRootSpan(), true);

    DeferLambda([&]() { is_finished_ = true; });
    if (input_ == nullptr) {
        return nullptr;
    }

    span.GetSpan()->SetAttribute("search_type", search_info_.metric_type_);
    span.GetSpan()->SetAttribute("topk", search_info_.topk_);

    std::chrono::high_resolution_clock::time_point vector_start =
        std::chrono::high_resolution_clock::now();

    auto& ph = placeholder_group_->at(0);
    auto src_data = ph.get_blob();
    auto src_offsets = ph.get_offsets();
    auto num_queries = ph.num_of_queries_;
    std::shared_ptr<const IArrayOffsets> array_offsets = nullptr;
    if (ph.element_level_) {
        array_offsets = segment_->GetArrayOffsets(search_info_.field_id_);
        AssertInfo(array_offsets != nullptr, "Array offsets not available");
        query_context_->set_array_offsets(array_offsets);
        search_info_.array_offsets_ = array_offsets;
    }

    // There are two types of execution: pre-filter and iterative filter
    // For **pre-filter**, we have execution path: FilterBitsNode -> MvccNode -> ElementFilterBitsNode -> VectorSearchNode -> ...
    // For **iterative filter**, we have execution path: MvccNode -> VectorSearchNode -> ElementFilterNode -> FilterNode -> ...
    //
    // When embedding search embedding on embedding list is used, which means element_level_ is true, we need to transform doc-level
    // bitset to element-level bitset. In pre-filter path, ElementFilterBitsNode already transforms the bitset. We need to transform it
    // in iterative filter path.
    if (milvus::exec::UseVectorIterator(search_info_) && ph.element_level_) {
        auto col_input = GetColumnVector(input_);
        TargetBitmapView view(col_input->GetRawData(), col_input->size());
        TargetBitmapView valid_view(col_input->GetValidRawData(),
                                    col_input->size());

        auto [element_bitset, valid_element_bitset] =
            array_offsets->RowBitsetToElementBitset(view, valid_view, 0);

        query_context_->set_active_element_count(element_bitset.size());

        std::vector<VectorPtr> col_res;
        col_res.push_back(std::make_shared<ColumnVector>(
            std::move(element_bitset), std::move(valid_element_bitset)));
        input_ = std::make_shared<RowVector>(col_res);
    }

    milvus::SearchResult search_result;

    auto col_input = GetColumnVector(input_);

    // Fast path: MvccNode set skip_filter flag on QueryContext, meaning
    // no filtering is needed (sealed segment, no deletes, no scalar filter,
    // no TTL). Pass an empty BitsetView to Knowhere so it uses
    // IDSelectorAll — the fastest search path.
    if (query_context_->get_skip_filter()) {
        milvus::BitsetView empty_view;
        auto op_context = query_context_->get_op_context();
        segment_->vector_search(search_info_,
                                src_data,
                                src_offsets,
                                num_queries,
                                query_timestamp_,
                                empty_view,
                                op_context,
                                search_result);
        search_result.total_data_cnt_ = active_count_;
        span.GetSpan()->SetAttribute(
            "result_count",
            static_cast<int>(search_result.seg_offsets_.size()));
        query_context_->set_search_result(std::move(search_result));
        std::chrono::high_resolution_clock::time_point vector_end =
            std::chrono::high_resolution_clock::now();
        double vector_cost =
            std::chrono::duration<double, std::micro>(
                vector_end - vector_start)
                .count();
        milvus::monitor::internal_core_search_latency_vector.Observe(
            vector_cost / 1000);
        return input_;
    }

    TargetBitmapView view(col_input->GetRawData(), col_input->size());

    if (view.all()) {
        query_context_->set_search_result(
            std::move(empty_search_result(num_queries)));
        return input_;
    }

    // TODO: uniform knowhere BitsetView and milvus BitsetView
    milvus::BitsetView final_view((uint8_t*)col_input->GetRawData(),
                                  col_input->size());
    auto op_context = query_context_->get_op_context();
    // todo(SpadeA): need to pass element_level to make check more rigorously?
    segment_->vector_search(search_info_,
                            src_data,
                            src_offsets,
                            num_queries,
                            query_timestamp_,
                            final_view,
                            op_context,
                            search_result);

    search_result.total_data_cnt_ = final_view.size();
    search_result.element_level_ = ph.element_level_;

    span.GetSpan()->SetAttribute(
        "result_count", static_cast<int>(search_result.seg_offsets_.size()));

    query_context_->set_search_result(std::move(search_result));
    std::chrono::high_resolution_clock::time_point vector_end =
        std::chrono::high_resolution_clock::now();
    double vector_cost =
        std::chrono::duration<double, std::micro>(vector_end - vector_start)
            .count();
    milvus::monitor::internal_core_search_latency_vector.Observe(vector_cost /
                                                                 1000);
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