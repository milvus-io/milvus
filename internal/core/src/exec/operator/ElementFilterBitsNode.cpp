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

PhyElementFilterBitsNode::PhyElementFilterBitsNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::ElementFilterBitsNode>&
        element_filter_bits_node)
    : Operator(driverctx,
               DataType::NONE,
               operator_id,
               "element_filter_bits_plan_node",
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
    auto field_meta = milvus::FindFirstArrayFieldInStruct(segment->get_schema(),
                                                          struct_name_);
    auto field_id = field_meta.get_id();
    auto array_offsets = segment->GetArrayOffsets(field_id);
    if (array_offsets == nullptr) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "IArrayOffsets not found for field {}",
                  field_id.get());
    }
    query_context_->set_array_offsets(array_offsets);
    auto [first_elem, _] =
        array_offsets->DocIDToElementID(query_context_->get_active_count());
    query_context_->set_active_element_count(first_elem);

    // Step 2: Prepare doc bitset
    auto col_input = GetColumnVector(input_);
    TargetBitmapView doc_bitset(col_input->GetRawData(), col_input->size());
    TargetBitmapView doc_bitset_valid(col_input->GetValidRawData(),
                                      col_input->size());
    doc_bitset.flip();

    // Step 3: Convert doc bitset to element offsets
    FixedVector<int32_t> element_offsets =
        DocBitsetToElementOffsets(doc_bitset);

    // Step 4: Evaluate element expression
    auto [expr_result, valid_expr_result] =
        EvaluateElementExpression(element_offsets);

    // Step 5: Set query context
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

FixedVector<int32_t>
PhyElementFilterBitsNode::DocBitsetToElementOffsets(
    const TargetBitmapView& doc_bitset) {
    auto array_offsets = query_context_->get_array_offsets();
    AssertInfo(array_offsets != nullptr, "Array offsets not available");

    int64_t doc_count = array_offsets->GetDocCount();
    AssertInfo(doc_bitset.size() == doc_count,
               "Doc bitset size mismatch: {} vs {}",
               doc_bitset.size(),
               doc_count);

    FixedVector<int32_t> element_offsets;
    element_offsets.reserve(array_offsets->GetTotalElementCount());

    // For each document that passes the filter, get all its element offsets
    for (int64_t doc_id = 0; doc_id < doc_count; ++doc_id) {
        if (doc_bitset[doc_id]) {
            // Get element range for this document
            auto [first_elem, last_elem] =
                array_offsets->DocIDToElementID(doc_id);

            // Add all element IDs for this document
            for (int64_t elem_id = first_elem; elem_id < last_elem; ++elem_id) {
                element_offsets.push_back(static_cast<int32_t>(elem_id));
            }
        }
    }

    return element_offsets;
}

std::pair<TargetBitmap, TargetBitmap>
PhyElementFilterBitsNode::EvaluateElementExpression(
    FixedVector<int32_t>& element_offsets) {
    tracer::AutoSpan span("PhyElementFilterBitsNode::EvaluateElementExpression",
                          tracer::GetRootSpan(),
                          true);
    tracer::AddEvent(fmt::format("input_elements: {}", element_offsets.size()));

    // Use offset interface by passing element_offsets as third parameter
    EvalCtx eval_ctx(operator_context_->get_exec_context(),
                     element_exprs_.get(),
                     &element_offsets);

    std::vector<VectorPtr> results;
    element_exprs_->Eval(0, 1, true, eval_ctx, results);

    AssertInfo(results.size() == 1 && results[0] != nullptr,
               "ElementFilterBitsNode: expression evaluation should return "
               "exactly one result");

    TargetBitmap bitset;
    TargetBitmap valid_bitset;
    int64_t total_elements = query_context_->get_active_element_count();
    bitset = TargetBitmap(total_elements, false);
    valid_bitset = TargetBitmap(total_elements, true);

    if (auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0])) {
        if (col_vec->IsBitmap()) {
            auto col_vec_size = col_vec->size();
            TargetBitmapView bitsetview(col_vec->GetRawData(), col_vec_size);

            AssertInfo(col_vec_size == element_offsets.size(),
                       "ElementFilterBitsNode result size mismatch: {} vs {}",
                       col_vec_size,
                       element_offsets.size());

            for (size_t i = 0; i < element_offsets.size(); ++i) {
                if (bitsetview[i]) {
                    bitset[element_offsets[i]] = true;
                }
            }
        } else {
            ThrowInfo(ExprInvalid,
                      "ElementFilterBitsNode result should be bitmap");
        }
    } else {
        ThrowInfo(ExprInvalid,
                  "ElementFilterBitsNode result should be ColumnVector");
    }

    bitset.flip();

    tracer::AddEvent(fmt::format("evaluated_elements: {}, total_elements: {}",
                                 element_offsets.size(),
                                 total_elements));

    return std::make_pair(std::move(bitset), std::move(valid_bitset));
}

}  // namespace exec
}  // namespace milvus