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
#include <cmath>
#include <functional>
#include <ratio>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/ArrayOffsets.h"
#include "common/BitsetView.h"
#include "common/EasyAssert.h"
#include "common/DeferredFilter.h"
#include "common/QueryResult.h"
#include "common/Tracer.h"
#include "common/Utils.h"
#include "exec/QueryContext.h"
#include "exec/expression/Utils.h"
#include "exec/expression/OffsetExpressionCallback.h"
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
empty_search_result(int64_t num_queries, bool element_level = false) {
    milvus::SearchResult final_result;
    final_result.total_nq_ = num_queries;
    final_result.unity_topK_ = 0;  // no result
    final_result.total_data_cnt_ = 0;
    final_result.element_level_ = element_level;
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
    // Freeze the growing segment's visible-row bound at plan time and carry it
    // into the search kernels. They must not re-read it from the segment: a
    // concurrent insert can publish rows between this point and the kernel's
    // read, and those rows are neither acknowledged nor visible to this query.
    // Sealed indexes are immutable and keep their empty-bitset fast path.
    if (segment_->type() == SegmentType::Growing) {
        search_info_.active_count_ = active_count_;
    }
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

    WaitPrefetch();
    span.SetAttribute("search_type", search_info_.metric_type_);
    span.SetAttribute("topk", search_info_.topk_);

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

    // Prepare BitsetView for search.
    // Fast path: all_rows_visible + non-element-level → empty BitsetView
    //            (IDSelectorAll in Knowhere, skips per-vector bit test).
    // Normal path: build BitsetView from the bitmap produced upstream.
    milvus::BitsetView search_view;
    int64_t data_cnt = active_count_;

    if (!ph.element_level_ && query_context_->bitset_is_element_level()) {
        ThrowInfo(ExprInvalid,
                  "element-level filter bitset cannot be used for row-level "
                  "vector search; use MATCH_ANY/MATCH_* for row-level struct "
                  "array filtering");
    }

    if (query_context_->get_all_rows_visible() && !ph.element_level_) {
        // search_view stays default-constructed (empty)
    } else {
        // There are two types of execution: pre-filter and iterative filter
        // For **pre-filter**: FilterBitsNode -> MvccNode -> ElementFilterBitsNode -> VectorSearchNode -> ...
        // For **iterative filter**: MvccNode -> VectorSearchNode -> IterativeElementFilterNode -> IterativeFilterNode -> ...
        //
        // When element_level_ is true, we need to transform doc-level bitset
        // to element-level bitset.  In pre-filter path, ElementFilterBitsNode
        // already does this.  We only need to do it here for the iterative
        // path or when ElementFilterBitsNode is not present.
        if (ph.element_level_ && !query_context_->bitset_is_element_level()) {
            auto col_input = GetColumnVector(input_);
            TargetBitmapView view(col_input->GetRawData(), col_input->size());
            TargetBitmapView valid_view(col_input->GetValidRawData(),
                                        col_input->size());

            auto [element_bitset, valid_element_bitset] =
                array_offsets->RowBitsetToElementBitset(view, valid_view, 0);

            query_context_->set_active_element_count(element_bitset.size());
            if (element_bitset.empty()) {
                query_context_->set_search_result(
                    empty_search_result(num_queries, ph.element_level_));
                return input_;
            }

            std::vector<VectorPtr> col_res;
            col_res.push_back(std::make_shared<ColumnVector>(
                std::move(element_bitset), std::move(valid_element_bitset)));
            input_ = std::make_shared<RowVector>(col_res);
            query_context_->set_bitset_is_element_level(true);
        }

        auto col_input = GetColumnVector(input_);
        TargetBitmapView view(col_input->GetRawData(), col_input->size());

        if (view.all()) {
            auto search_result = empty_search_result(num_queries);
            search_result.total_data_cnt_ = data_cnt;
            search_result.element_level_ = ph.element_level_;
            query_context_->set_search_result(std::move(search_result));
            return input_;
        }

        // TODO: uniform knowhere BitsetView and milvus BitsetView
        search_view = milvus::BitsetView((uint8_t*)col_input->GetRawData(),
                                         col_input->size());
        data_cnt = search_view.size();
    }

    // Single search + metrics path
    // Descriptor and query-owned factory outlive the synchronous search and
    // all joined Cardinal workers. The view never owns expression state.
    knowhere::CandidateEvaluatorViewV1 callback_view;
    if (const auto& callback = query_context_->get_ann_fusing_callback()) {
        callback_view = callback->view();
        search_view.set_candidate_evaluator(&callback_view);
        auto& params = search_info_.search_params_;
        // Backend transport field is derived here, not a public strategy knob.
        params.erase("ann_fusing_filter_ratio");
        if (params.contains("debug_ann_fusing_filter_ratio")) {
            // Development override is the COMPLETE query filter ratio.
            // Only DiskANN expansion consumes it, never user-predicate sampling
            // or the exact mandatory bitmap count.
            const double ratio = params.at("debug_ann_fusing_filter_ratio").get<double>();
            AssertInfo(std::isfinite(ratio) && ratio >= 0 && ratio <= 1,
                       "debug_ann_fusing_filter_ratio must be in [0,1]");
            params["ann_fusing_filter_ratio"] = ratio;
        } else {
            if (const auto user_ratio =
                    query_context_->get_ann_fusing_user_filter_ratio()) {
                if (!search_view.has_known_count()) {
                    // This is the already materialized mandatory bitmap only.
                    // A freshly constructed BitsetView has no cached count.
                    search_view.count_filtered_bits(0, search_view.num_bits());
                }
                const double mandatory = search_view.size() == 0 ? 0.0 :
                    static_cast<double>(search_view.count()) / search_view.size();
                // Independence approximation, NOT an exact union count.
                // CAP acceptance injects the known full-query rate until
                // estimator precision/correlation is separately qualified.
                // DiskANN consumes this advisory. Do not set filter_level:
                // memory graph uses that name for a different connectivity
                // policy which must remain unchanged by scalar sampling.
                params["ann_fusing_filter_ratio"] =
                    mandatory + (1.0 - mandatory) * *user_ratio;
            }
        }
    }
    milvus::SearchResult search_result;
    auto op_context = query_context_->get_op_context();
    try {
        segment_->vector_search(search_info_,
                            src_data,
                            src_offsets,
                            num_queries,
                            query_timestamp_,
                            search_view,
                            op_context,
                                search_result);
    } catch (const DeferredFilterUnsupported&) {
        const auto callback = query_context_->get_ann_fusing_callback();
        AssertInfo(callback != nullptr, "bitmap fallback requires a deferred predicate");
        callback->RethrowIfFailed();
        auto baseline = callback->EvalBaseline();
        baseline.flip();  // Expr TRUE bitmap -> search exclusion bitmap.
        if (search_view.num_bits() != 0) {
            AssertInfo(search_view.num_bits() == baseline.size(), "mandatory bitmap size mismatch");
            TargetBitmapView mandatory(const_cast<uint8_t*>(search_view.mandatory_data()),
                                        search_view.num_bits());
            baseline.inplace_or(mandatory, baseline.size());
        }
        milvus::BitsetView baseline_view(
            reinterpret_cast<const uint8_t*>(baseline.data()), baseline.size());
        auto baseline_info = search_info_;
        baseline_info.search_params_.erase("ann_fusing_filter_ratio");
        // Only an explicit unsupported-before-search signal reaches here.
        // The new runtime snapshot may differ; the exact bitmap is safe for
        // either indexed or raw search. No index-type whitelist lives here.
        segment_->vector_search(baseline_info, src_data, src_offsets,
                                num_queries, query_timestamp_, baseline_view,
                                op_context, search_result);
        query_context_->set_ann_fusing_callback(nullptr);
        LOG_DEBUG("ann_fusing fallback reason=entry_requires_bitmap");
    } catch (...) {
        // Backends join their task futures before returning either results or
        // failure. Prefer the original Expr/storage/cancellation cause to a
        // generic cross-ABI callback-failed status.
        if (const auto& callback = query_context_->get_ann_fusing_callback()) {
            callback->RethrowIfFailed();
        }
        throw;
    }
    if (const auto& callback = query_context_->get_ann_fusing_callback()) {
        callback->RethrowIfFailed();
    }

    search_result.total_data_cnt_ = data_cnt;
    search_result.element_level_ = ph.element_level_;

    span.SetAttribute("result_count",
                      static_cast<int>(search_result.seg_offsets_.size()));
    query_context_->set_search_result(std::move(search_result));

    std::chrono::high_resolution_clock::time_point vector_end =
        std::chrono::high_resolution_clock::now();
    double vector_cost =
        std::chrono::duration<double, std::micro>(vector_end - vector_start)
            .count();
    milvus::monitor::internal_core_search_latency_vector.Observe(vector_cost /
                                                                 1000);
    // vector search stores result in query_context;
    // this node returns the bitset for downstream operators
    return input_;
}

bool
PhyVectorSearchNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus
