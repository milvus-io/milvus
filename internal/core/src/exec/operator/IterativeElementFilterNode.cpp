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

#include "IterativeElementFilterNode.h"
#include "exec/operator/Utils.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <optional>
#include <ratio>
#include <type_traits>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "common/EasyAssert.h"
#include "common/ElementFilterIterator.h"
#include "common/FieldMeta.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Utils.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "fmt/core.h"
#include "plan/PlanNode.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

PhyIterativeElementFilterNode::PhyIterativeElementFilterNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::IterativeElementFilterNode>&
        element_filter_node)
    : Operator(driverctx,
               element_filter_node->output_type(),
               operator_id,
               element_filter_node->id(),
               "PhyIterativeElementFilterNode"),
      struct_name_(element_filter_node->struct_name()),
      has_doc_predicate_(element_filter_node->has_doc_predicate()) {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    std::vector<expr::TypedExprPtr> exprs;
    exprs.emplace_back(element_filter_node->element_filter());
    element_exprs_ = std::make_unique<ExprSet>(exprs, exec_context);
}

void
PhyIterativeElementFilterNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhyIterativeElementFilterNode::GetOutput() {
    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

    tracer::AutoSpan span("PhyIterativeElementFilterNode::GetOutput",
                          tracer::GetRootSpan(),
                          true);

    DeferLambda([&]() { is_finished_ = true; });

    if (input_ == nullptr) {
        return nullptr;
    }

    std::chrono::high_resolution_clock::time_point start_time =
        std::chrono::high_resolution_clock::now();

    // Step 1: Get search result with iterators
    milvus::SearchResult search_result = query_context_->get_search_result();

    if (!search_result.element_level_) {
        ThrowInfo(ExprInvalid,
                  "PhyIterativeElementFilterNode expects element-level search "
                  "result");
    }

    if (!search_result.vector_iterators_.has_value()) {
        ThrowInfo(ExprInvalid,
                  "PhyIterativeElementFilterNode expects vector_iterators in "
                  "search result");
    }

    auto segment = query_context_->get_segment();
    auto& field_meta =
        segment->get_schema().GetFirstArrayFieldInStruct(struct_name_);
    auto field_id = field_meta.get_id();
    auto array_offsets = segment->GetArrayOffsets(field_id);
    if (array_offsets == nullptr) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "IArrayOffsets not found for field {}",
                  field_id.get());
    }
    query_context_->set_array_offsets(array_offsets);

    // Step 2: Wrap each iterator with ElementFilterIterator
    auto& base_iterators = search_result.vector_iterators_.value();
    std::vector<std::shared_ptr<VectorIterator>> wrapped_iterators;
    wrapped_iterators.reserve(base_iterators.size());

    ExecContext* exec_context = operator_context_->get_exec_context();

    for (auto& base_iter : base_iterators) {
        // Wrap each iterator with ElementFilterIterator
        auto wrapped_iter = std::make_shared<ElementFilterIterator>(
            base_iter, exec_context, element_exprs_.get());

        wrapped_iterators.push_back(std::move(wrapped_iter));
    }

    // Step 3: Update search result with wrapped iterators
    search_result.vector_iterators_ = std::move(wrapped_iterators);
    size_t num_iterators = search_result.vector_iterators_.has_value()
                               ? search_result.vector_iterators_->size()
                               : 0;

    // Step 4: If no doc-level predicate, collect results directly
    // (otherwise, downstream IterativeFilterNode will do this)
    if (!has_doc_predicate_) {
        CollectResults(search_result, array_offsets.get());
    }

    query_context_->set_search_result(std::move(search_result));

    // Step 5: Record metrics
    std::chrono::high_resolution_clock::time_point end_time =
        std::chrono::high_resolution_clock::now();
    double cost =
        std::chrono::duration<double, std::micro>(end_time - start_time)
            .count();

    tracer::AddEvent(fmt::format(
        "PhyIterativeElementFilterNode: wrapped {} iterators, struct_name: "
        "{}, has_doc_predicate: {}, cost_us: {}",
        num_iterators,
        struct_name_,
        has_doc_predicate_,
        cost));

    // Pass through input to downstream
    return input_;
}

void
PhyIterativeElementFilterNode::CollectResults(
    SearchResult& search_result, const IArrayOffsets* array_offsets) {
    // When there's no doc-level predicate, we need to consume the iterators
    // and collect the top-K results ourselves.
    //
    // Note: knowhere iterator doesn't guarantee strictly ordered output,
    // so we must use binary insertion to maintain sorted order.

    auto& iterators = search_result.vector_iterators_.value();
    int64_t nq = search_result.total_nq_;
    int64_t topk = search_result.unity_topK_;

    knowhere::MetricType metric_type = query_context_->get_metric_type();
    bool large_is_better = PositivelyRelated(metric_type);

    // Initialize result arrays
    search_result.seg_offsets_.resize(nq * topk, INVALID_SEG_OFFSET);
    search_result.distances_.resize(nq * topk, 0.0f);
    search_result.element_indices_.resize(nq * topk, -1);

    for (int64_t q = 0; q < nq; ++q) {
        auto& iterator = iterators[q];
        int64_t count = 0;
        int64_t base_idx = q * topk;

        while (iterator->HasNext() && count < topk) {
            auto result = iterator->Next();
            if (!result.has_value()) {
                break;
            }

            auto [element_id, distance] = result.value();
            auto [doc_id, elem_idx] =
                array_offsets->ElementIDToRowID(element_id);

            // Find insert position using binary search
            size_t pos =
                large_is_better
                    ? find_binsert_position<true>(search_result.distances_,
                                                  base_idx,
                                                  base_idx + count,
                                                  distance)
                    : find_binsert_position<false>(search_result.distances_,
                                                   base_idx,
                                                   base_idx + count,
                                                   distance);

            // Shift elements to make room for insertion
            if (count > 0 && pos < base_idx + count) {
                std::memmove(&search_result.distances_[pos + 1],
                             &search_result.distances_[pos],
                             (base_idx + count - pos) * sizeof(float));
                std::memmove(&search_result.seg_offsets_[pos + 1],
                             &search_result.seg_offsets_[pos],
                             (base_idx + count - pos) * sizeof(int64_t));
                std::memmove(&search_result.element_indices_[pos + 1],
                             &search_result.element_indices_[pos],
                             (base_idx + count - pos) * sizeof(int32_t));
            }

            // Insert the new result
            search_result.seg_offsets_[pos] = doc_id;
            search_result.element_indices_[pos] = elem_idx;
            search_result.distances_[pos] = distance;
            ++count;
        }
    }

    // Clear iterators to indicate results have been collected
    search_result.vector_iterators_.reset();

    tracer::AddEvent(fmt::format(
        "PhyIterativeElementFilterNode::CollectResults: nq={}, topk={}",
        nq,
        topk));
}

}  // namespace exec
}  // namespace milvus
