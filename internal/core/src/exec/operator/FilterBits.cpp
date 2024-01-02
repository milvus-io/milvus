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

#include "FilterBits.h"

namespace milvus {
namespace exec {
FilterBits::FilterBits(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::FilterBitsNode>& filter)
    : Operator(driverctx,
               filter->output_type(),
               operator_id,
               filter->id(),
               "FilterBits") {
    ExecContext* exec_context = operator_context_->get_exec_context();
    QueryContext* query_context = exec_context->get_query_context();
    std::vector<expr::TypedExprPtr> filters;
    filters.emplace_back(filter->filter());
    exprs_ = std::make_unique<ExprSet>(filters, exec_context);
    need_process_rows_ = query_context->get_active_count();
    num_processed_rows_ = 0;
}

void
FilterBits::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

bool
FilterBits::AllInputProcessed() {
    if (num_processed_rows_ == need_process_rows_) {
        input_ = nullptr;
        return true;
    }
    return false;
}

bool
FilterBits::IsFinished() {
    return AllInputProcessed();
}

RowVectorPtr
FilterBits::GetOutput() {
    if (AllInputProcessed()) {
        return nullptr;
    }

    EvalCtx eval_ctx(
        operator_context_->get_exec_context(), exprs_.get(), input_.get());

    exprs_->Eval(0, 1, true, eval_ctx, results_);

    AssertInfo(results_.size() == 1 && results_[0] != nullptr,
               "FilterBits result size should be one and not be nullptr");

    if (results_[0]->type() == DataType::ROW) {
        auto row_vec = std::dynamic_pointer_cast<RowVector>(results_[0]);
        num_processed_rows_ += row_vec->child(0)->size();
    } else {
        num_processed_rows_ += results_[0]->size();
    }
    return std::make_shared<RowVector>(results_);
}

}  // namespace exec
}  // namespace milvus