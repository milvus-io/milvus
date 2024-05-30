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
    query_context_ = exec_context->get_query_context();
    std::vector<expr::TypedExprPtr> filters;
    filters.emplace_back(filter->filter());
    exprs_ = std::make_unique<ExprSet>(filters, exec_context);
    need_process_rows_ = query_context_->get_active_count();
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

    TargetBitmap bitset;
    while (num_processed_rows_ < need_process_rows_) {
        exprs_->Eval(0, 1, true, eval_ctx, results_);

        AssertInfo(results_.size() == 1 && results_[0] != nullptr,
                   "FilterBits result size should be one and not be nullptr");

        if (results_[0]->type() == DataType::ROW) {
            auto row_vec = std::dynamic_pointer_cast<RowVector>(results_[0]);
            auto col_res_vec =
                std::dynamic_pointer_cast<ColumnVector>(row_vec->child(0));
            auto col_res_vec_size = col_res_vec->size();
            TargetBitmapView view(col_res_vec->GetRawData(), col_res_vec_size);
            bitset.append(view);
            num_processed_rows_ += col_res_vec_size;

            // check whether can use pk term optimization,
            // store info to query context.
            if (!query_context_->get_pk_term_offset_cache_initialized()) {
                auto cache_offset_vec =
                    std::dynamic_pointer_cast<ColumnVector>(row_vec->child(1));

                // if get empty cache offset, means that no row heated all the segment,
                // so no need to get next batch.
                if (cache_offset_vec->size() == 0) {
                    bitset.resize(need_process_rows_);
                    break;
                }

                // cached pk term offset to query context
                // ensure query context is safe-thread
                auto cache_offset_data =
                    (int64_t*)cache_offset_vec->GetRawData();
                std::vector<int64_t> cached_offset;
                for (size_t i = 0; i < cache_offset_vec->size(); i++) {
                    cached_offset.push_back(cache_offset_data[i]);
                }
                query_context_->set_pk_term_offset_cache(
                    std::move(cached_offset));
                query_context_->set_pk_term_offset_cache_initialized(true);
            }
        } else {
            auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results_[0]);
            auto col_vec_size = col_vec->size();
            TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
            bitset.append(view);
            num_processed_rows_ += col_vec_size;
        }
    }
    bitset.flip();
    Assert(bitset.size() == need_process_rows_);
    num_processed_rows_ = need_process_rows_;
    std::vector<VectorPtr> col_res;
    col_res.push_back(std::make_shared<ColumnVector>(std::move(bitset)));
    return std::make_shared<RowVector>(col_res);
}

}  // namespace exec
}  // namespace milvus