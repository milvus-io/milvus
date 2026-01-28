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

#include "ElementFilterBitsNode.h"
#include "common/Tracer.h"
#include "fmt/format.h"

#include "monitor/Monitor.h"

namespace milvus {
namespace exec {

const double DOC_HIT_RATIO_THRESHOLD_HIGH = 0.01;
const double DOC_HIT_RATIO_THRESHOLD_LOW = 0.002;

PhyElementFilterBitsNode::PhyElementFilterBitsNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::ElementFilterBitsNode>&
        element_filter_bits_node)
    : Operator(driverctx,
               element_filter_bits_node->output_type(),
               operator_id,
               element_filter_bits_node->id(),
               "PhyElementFilterBitsNode"),
      struct_name_(element_filter_bits_node->struct_name()) {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();

    // Build expression set from element-level expression
    std::vector<expr::TypedExprPtr> exprs;
    exprs.emplace_back(element_filter_bits_node->element_filter());
    element_exprs_ = std::make_unique<ExprSet>(exprs, exec_context);
}

void
PhyElementFilterBitsNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

RowVectorPtr
PhyElementFilterBitsNode::GetOutput() {
    if (is_finished_ || input_ == nullptr) {
        return nullptr;
    }

    DeferLambda([&]() { is_finished_ = true; });

    tracer::AutoSpan span(
        "PhyElementFilterBitsNode::GetOutput", tracer::GetRootSpan(), true);

    std::chrono::high_resolution_clock::time_point start_time =
        std::chrono::high_resolution_clock::now();
    std::chrono::high_resolution_clock::time_point step_time;

    // Step 1: Get array offsets
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
    auto [first_elem, _] =
        array_offsets->ElementIDRangeOfRow(query_context_->get_active_count());
    query_context_->set_active_element_count(first_elem);

    // Step 2: Prepare doc bitset
    auto col_input = GetColumnVector(input_);
    TargetBitmapView doc_bitset(col_input->GetRawData(), col_input->size());
    TargetBitmapView doc_bitset_valid(col_input->GetValidRawData(),
                                      col_input->size());
    doc_bitset.flip();

    // Step 3: Evaluate element expression
    // Use offset mode or full mode based on selectivity
    auto [expr_result, valid_expr_result] = EvaluateElementExpression(
        doc_bitset, doc_bitset_valid, array_offsets.get());

    // Step 4: Set query context
    query_context_->set_element_level_query(true);
    query_context_->set_struct_name(struct_name_);

    std::chrono::high_resolution_clock::time_point end_time =
        std::chrono::high_resolution_clock::now();
    double total_cost =
        std::chrono::duration<double, std::micro>(end_time - start_time)
            .count();
    milvus::monitor::internal_core_search_latency_scalar.Observe(total_cost /
                                                                 1000);

    auto filtered_count = expr_result.count();
    tracer::AddEvent(
        fmt::format("struct_name: {}, total_elements: {}, output_rows: {}, "
                    "filtered: {}, cost_us: {}",
                    struct_name_,
                    array_offsets->GetTotalElementCount(),
                    array_offsets->GetTotalElementCount() - filtered_count,
                    filtered_count,
                    total_cost));

    std::vector<VectorPtr> col_res;
    col_res.push_back(std::make_shared<ColumnVector>(
        std::move(expr_result), std::move(valid_expr_result)));
    return std::make_shared<RowVector>(col_res);
}

std::pair<TargetBitmap, TargetBitmap>
PhyElementFilterBitsNode::EvaluateElementExpression(
    const TargetBitmapView& doc_bitset,
    const TargetBitmapView& doc_bitset_valid,
    const IArrayOffsets* array_offsets) {
    tracer::AutoSpan span("PhyElementFilterBitsNode::EvaluateElementExpression",
                          tracer::GetRootSpan(),
                          true);
    int64_t total_elements = query_context_->get_active_element_count();

    auto count = doc_bitset.count();
    auto doc_hit_ratio = count * 1.0 / doc_bitset.size();

    // If hit_ratio is less than DOC_HIT_RATIO_THRESHOLD_LOW, we always use offset mode
    // If hit_ratio is between [DOC_HIT_RATIO_THRESHOLD_LOW, DOC_HIT_RATIO_THRESHOLD_HIGH], we check if all expressions can use nested index
    // Otherwise, we use full mode
    bool offset_mode;
    if (doc_hit_ratio < DOC_HIT_RATIO_THRESHOLD_LOW) {
        offset_mode = true;
    } else if (doc_hit_ratio < DOC_HIT_RATIO_THRESHOLD_HIGH) {
        offset_mode = !std::all_of(
            element_exprs_->exprs().begin(),
            element_exprs_->exprs().end(),
            [](const auto& expr) { return expr->CanUseNestedIndex(); });
    } else {
        offset_mode = false;
    }

    if (offset_mode) {
        // Offset mode: convert doc_bitset to element offsets and evaluate only on those
        // Offset mode processes all offsets in one pass
        FixedVector<int32_t> element_offsets =
            array_offsets->RowBitsetToElementOffsets(doc_bitset, 0);

        tracer::AddEvent(fmt::format("offset_mode, input_elements: {}",
                                     element_offsets.size()));

        EvalCtx eval_ctx(operator_context_->get_exec_context(),
                         &element_offsets);

        std::vector<VectorPtr> results;
        element_exprs_->Eval(0, 1, true, eval_ctx, results);

        AssertInfo(results.size() == 1 && results[0] != nullptr,
                   "ElementFilterBitsNode: expression evaluation should return "
                   "exactly one result");

        auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
        if (!col_vec) {
            ThrowInfo(ExprInvalid,
                      "ElementFilterBitsNode result should be ColumnVector");
        }
        if (!col_vec->IsBitmap()) {
            ThrowInfo(ExprInvalid,
                      "ElementFilterBitsNode result should be bitmap");
        }

        auto col_vec_size = col_vec->size();
        TargetBitmapView bitsetview(col_vec->GetRawData(), col_vec_size);

        AssertInfo(col_vec_size == element_offsets.size(),
                   "ElementFilterBitsNode result size mismatch: {} vs {}",
                   col_vec_size,
                   element_offsets.size());

        // Scatter results to full bitset
        TargetBitmap bitset(total_elements, false);
        TargetBitmap valid_bitset(total_elements, true);
        for (size_t i = 0; i < element_offsets.size(); ++i) {
            if (bitsetview[i]) {
                bitset[element_offsets[i]] = true;
            }
        }

        bitset.flip();

        tracer::AddEvent(
            fmt::format("evaluated_elements: {}, total_elements: {}",
                        element_offsets.size(),
                        total_elements));

        return std::make_pair(std::move(bitset), std::move(valid_bitset));
    } else {
        // Full mode: evaluate on all elements, then AND with doc_bitset
        tracer::AddEvent(
            fmt::format("full_mode, total_elements: {}", total_elements));

        EvalCtx eval_ctx(operator_context_->get_exec_context());

        TargetBitmap eval_bitset;
        int64_t num_processed_elements = 0;
        std::vector<VectorPtr> results;

        while (num_processed_elements < total_elements) {
            element_exprs_->Eval(0, 1, true, eval_ctx, results);

            AssertInfo(results.size() == 1 && results[0] != nullptr,
                       "ElementFilterBitsNode: expression evaluation "
                       "should return "
                       "exactly one result");

            auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
            if (!col_vec) {
                ThrowInfo(
                    ExprInvalid,
                    "ElementFilterBitsNode result should be ColumnVector");
            }
            if (!col_vec->IsBitmap()) {
                ThrowInfo(ExprInvalid,
                          "ElementFilterBitsNode result should be bitmap");
            }

            auto col_vec_size = col_vec->size();
            TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
            eval_bitset.append(view);
            num_processed_elements += col_vec_size;
        }

        AssertInfo(eval_bitset.size() == total_elements,
                   "ElementFilterBitsNode result size mismatch: {} vs {}",
                   eval_bitset.size(),
                   total_elements);

        // Convert doc_bitset to element bitset
        auto [elem_bitset, _] = array_offsets->RowBitsetToElementBitset(
            doc_bitset, doc_bitset_valid, 0);

        // AND expression result with element bitset from doc filter
        // eval_bitset: true means element matches expression
        // elem_bitset: true means element's doc passed predicate filter
        eval_bitset &= elem_bitset;

        eval_bitset.flip();

        tracer::AddEvent(
            fmt::format("evaluated_elements: {}, total_elements: {}",
                        total_elements,
                        total_elements));

        // Element filter targets individual elements within arrays.
        // While the array field itself can be nullable (handled by doc_bitset_valid),
        // individual elements inside an array do not support null values.
        // Therefore, the element-level valid_bitset is always all true.
        TargetBitmap valid_bitset(total_elements, true);
        return std::make_pair(std::move(eval_bitset), std::move(valid_bitset));
    }
}

}  // namespace exec
}  // namespace milvus