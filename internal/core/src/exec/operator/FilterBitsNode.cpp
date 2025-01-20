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
    is_source_node_ = filter->sources().size() == 0;
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
    if (!is_source_node_ && input_ == nullptr) {
        return nullptr;
    }

    if (AllInputProcessed()) {
        return nullptr;
    }

    std::chrono::high_resolution_clock::time_point scalar_start =
        std::chrono::high_resolution_clock::now();

    EvalCtx eval_ctx(operator_context_->get_exec_context(), exprs_.get());
    OffsetVector offsets{};
    if (input_ != nullptr) {
        // PhyFilterBitsNode is not the source node. Currently, only the PhyRandomSampleNode can be
        // the source node of PhyFilterBitsNode.
        AssertInfo(input_->size() == need_process_rows_,
                   "inconsistency between input size and need_process_rows_, "
                   "input size {}, need_process_rows_ {}",
                   input_->size(),
                   need_process_rows_);
        AssertInfo(input_->childrens().size() == 1,
                   "unexpected children size, expected 1, but get {}",
                   input_->childrens().size());
        auto& child = input_->childrens()[0];
        auto col_vec = std::dynamic_pointer_cast<ColumnVector>(child);
        AssertInfo(col_vec != nullptr, "unexpected");
        TargetBitmapView data(col_vec->GetRawData(), col_vec->size());
        for (int32_t i = 0; i < col_vec->size(); i++) {
            if (!data[i]) {
                offsets.push_back(i);
            }
        }
        eval_ctx.set_offset_input(&offsets);
        LOG_INFO(
            "debug_for_sample: GetOutput, input size {}, need_process_rows_ {}",
            input_->size(),
            need_process_rows_);
    }
    TargetBitmap bitset;
    TargetBitmap valid_bitset;
    while (num_processed_rows_ < need_process_rows_) {
        exprs_->Eval(0, 1, true, eval_ctx, results_);
        LOG_INFO(
            "debug_for_sample: evaluate expr, num_processed_rows_ {}, "
            "need_process_rows_ {}",
            num_processed_rows_,
            need_process_rows_);

        AssertInfo(results_.size() == 1 && results_[0] != nullptr,
                   "PhyFilterBitsNode result size should be size one and not "
                   "be nullptr");

        if (auto col_vec =
                std::dynamic_pointer_cast<ColumnVector>(results_[0])) {
            if (col_vec->IsBitmap()) {
                if (input_ == nullptr) {
                    auto col_vec_size = col_vec->size();
                    TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
                    bitset.append(view);
                    TargetBitmapView valid_view(col_vec->GetValidRawData(),
                                                col_vec_size);
                    valid_bitset.append(valid_view);
                    num_processed_rows_ += col_vec_size;
                } else {
                    // currently, process by providing offsets will handle all at once, so we can reassign the `bitset` and `valid_bitset`
                    // directly.
                    bitset = TargetBitmap{input_->size()};
                    valid_bitset = TargetBitmap{input_->size()};
                    auto col_vec_size = col_vec->size();
                    TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
                    TargetBitmapView valid_view(col_vec->GetValidRawData(),
                                                col_vec_size);
                    for (auto i = 0; i < offsets.size(); ++i) {
                        if (view[i] > 0) {
                            bitset[offsets[i]] = true;
                        }
                        if (valid_view[i] > 0) {
                            valid_bitset[offsets[i]] = true;
                        }
                    }
                    num_processed_rows_ += input_->size();
                }
            } else {
                PanicInfo(ExprInvalid,
                          "PhyFilterBitsNode result should be bitmap");
            }
        } else {
            PanicInfo(ExprInvalid,
                      "PhyFilterBitsNode result should be ColumnVector");
        }
    }
    bitset.flip();
    Assert(bitset.size() == need_process_rows_);
    Assert(valid_bitset.size() == need_process_rows_);
    // num_processed_rows_ = need_process_rows_;
    std::vector<VectorPtr> col_res;
    col_res.push_back(std::make_shared<ColumnVector>(std::move(bitset),
                                                     std::move(valid_bitset)));
    std::chrono::high_resolution_clock::time_point scalar_end =
        std::chrono::high_resolution_clock::now();
    double scalar_cost =
        std::chrono::duration<double, std::micro>(scalar_end - scalar_start)
            .count();
    monitor::internal_core_search_latency_scalar.Observe(scalar_cost / 1000);

    return std::make_shared<RowVector>(col_res);
}

}  // namespace exec
}  // namespace milvus
