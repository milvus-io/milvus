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

#include "FilterBitsNode.h"
#include "common/Tracer.h"
#include "fmt/format.h"

#include "monitor/Monitor.h"

namespace milvus {
namespace exec {
PhyFilterBitsNode::PhyFilterBitsNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::FilterBitsNode>& filter)
    : Operator(driverctx,
               filter->output_type(),
               operator_id,
               filter->id(),
               "PhyFilterBitsNode") {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    std::vector<expr::TypedExprPtr> filters;
    filters.emplace_back(filter->filter());
    exprs_ = std::make_unique<ExprSet>(filters, exec_context);
    need_process_rows_ = query_context_->get_active_count();
    num_processed_rows_ = 0;
}

void
PhyFilterBitsNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

bool
PhyFilterBitsNode::AllInputProcessed() {
    if (num_processed_rows_ == need_process_rows_) {
        input_ = nullptr;
        return true;
    }
    return false;
}

bool
PhyFilterBitsNode::IsFinished() {
    return AllInputProcessed();
}

RowVectorPtr
PhyFilterBitsNode::GetOutput() {
    milvus::exec::checkCancellation(query_context_);

    if (AllInputProcessed()) {
        return nullptr;
    }

    tracer::AutoSpan span(
        "PhyFilterBitsNode::Execute", tracer::GetRootSpan(), true);
    tracer::AddEvent(fmt::format("input_rows: {}", need_process_rows_));

    std::chrono::high_resolution_clock::time_point scalar_start =
        std::chrono::high_resolution_clock::now();

    EvalCtx eval_ctx(operator_context_->get_exec_context());

    TargetBitmap bitset;
    TargetBitmap valid_bitset;
    while (num_processed_rows_ < need_process_rows_) {
        // Evaluate all expressions (including TTL filter if added by CompileExpressions)
        exprs_->Eval(0, exprs_->size(), true, eval_ctx, results_);

        AssertInfo(
            results_.size() == exprs_->size(),
            "PhyFilterBitsNode result size should match expression count");
        AssertInfo(results_.size() > 0,
                   "PhyFilterBitsNode should have at least one expression");

        // Combine all expression results using AND logic
        // When CompileExpressions adds TTL filter, we have multiple expressions
        // that need to be combined
        TargetBitmap combined_bitmap;
        TargetBitmap combined_valid;
        bool first_expr = true;

        for (size_t i = 0; i < results_.size(); ++i) {
            AssertInfo(results_[i] != nullptr,
                       "PhyFilterBitsNode result[{}] should not be nullptr",
                       i);

            auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results_[i]);
            if (!col_vec) {
                ThrowInfo(ExprInvalid,
                          "PhyFilterBitsNode result[{}] should be ColumnVector",
                          i);
            }
            if (!col_vec->IsBitmap()) {
                ThrowInfo(ExprInvalid,
                          "PhyFilterBitsNode result[{}] should be bitmap",
                          i);
            }

            auto col_vec_size = col_vec->size();
            TargetBitmapView current_view(col_vec->GetRawData(), col_vec_size);
            TargetBitmapView current_valid_view(col_vec->GetValidRawData(),
                                                col_vec_size);

            if (first_expr) {
                // First expression: create copies as base
                combined_bitmap = TargetBitmap(current_view);
                combined_valid = TargetBitmap(current_valid_view);
                first_expr = false;
            } else {
                // Combine with previous results using AND logic
                AssertInfo(current_view.size() == combined_bitmap.size(),
                           "Expression result sizes must match: {} vs {}",
                           current_view.size(),
                           combined_bitmap.size());

                // In-place AND operation
                TargetBitmapView combined_view(combined_bitmap);
                combined_view.inplace_and(current_view, col_vec_size);
                TargetBitmapView combined_valid_view(combined_valid);
                combined_valid_view.inplace_and(current_valid_view,
                                                col_vec_size);
            }
        }

        auto col_vec_size = combined_bitmap.size();
        TargetBitmapView view(combined_bitmap);
        bitset.append(view);
        TargetBitmapView valid_view(combined_valid);
        valid_bitset.append(valid_view);
        num_processed_rows_ += col_vec_size;
    }
    bitset.flip();
    AssertInfo(bitset.size() == need_process_rows_,
               "bitset size: {}, need_process_rows_: {}",
               bitset.size(),
               need_process_rows_);
    Assert(valid_bitset.size() == need_process_rows_);

    auto filtered_count = bitset.count();
    auto filter_ratio =
        bitset.size() != 0 ? 1 - float(filtered_count) / bitset.size() : 0;
    milvus::monitor::internal_core_expr_filter_ratio.Observe(filter_ratio);
    // num_processed_rows_ = need_process_rows_;
    std::vector<VectorPtr> col_res;
    col_res.push_back(std::make_shared<ColumnVector>(std::move(bitset),
                                                     std::move(valid_bitset)));
    std::chrono::high_resolution_clock::time_point scalar_end =
        std::chrono::high_resolution_clock::now();
    double scalar_cost =
        std::chrono::duration<double, std::micro>(scalar_end - scalar_start)
            .count();
    milvus::monitor::internal_core_search_latency_scalar.Observe(scalar_cost /
                                                                 1000);

    tracer::AddEvent(fmt::format("output_rows: {}, filtered: {}",
                                 need_process_rows_ - filtered_count,
                                 filtered_count));

    return std::make_shared<RowVector>(col_res);
}

}  // namespace exec
}  // namespace milvus
