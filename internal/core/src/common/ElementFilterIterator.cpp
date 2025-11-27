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

#include "ElementFilterIterator.h"

#include "common/EasyAssert.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"

namespace milvus {

ElementFilterIterator::ElementFilterIterator(
    std::shared_ptr<VectorIterator> base_iterator,
    exec::ExecContext* exec_context,
    exec::ExprSet* expr_set)
    : base_iterator_(std::move(base_iterator)),
      exec_context_(exec_context),
      expr_set_(expr_set),
      exhausted_(false) {
    AssertInfo(base_iterator_ != nullptr, "Base iterator cannot be null");
    AssertInfo(exec_context_ != nullptr, "Exec context cannot be null");
    AssertInfo(expr_set_ != nullptr, "ExprSet cannot be null");
}

bool
ElementFilterIterator::HasNext() {
    // If cache is empty and base iterator not exhausted, fetch more
    if (filtered_buffer_.empty() && !exhausted_) {
        FetchAndFilterBatch();
    }
    return !filtered_buffer_.empty();
}

std::optional<std::pair<int64_t, float>>
ElementFilterIterator::Next() {
    if (!HasNext()) {
        return std::nullopt;
    }

    auto result = filtered_buffer_.front();
    filtered_buffer_.pop_front();
    return result;
}

void
ElementFilterIterator::FetchAndFilterBatch() {
    constexpr size_t kBatchSize = 1024;

    // Step 1: Fetch a batch from base iterator (up to kBatchSize elements)
    FixedVector<int32_t> element_ids;
    FixedVector<float> distances;

    element_ids.reserve(kBatchSize);
    distances.reserve(kBatchSize);

    while (base_iterator_->HasNext() && element_ids.size() < kBatchSize) {
        auto pair = base_iterator_->Next();
        if (pair.has_value()) {
            element_ids.push_back(static_cast<int32_t>(pair->first));
            distances.push_back(pair->second);
        }
    }

    // If no elements fetched, mark as exhausted
    if (element_ids.empty()) {
        exhausted_ = true;
        return;
    }

    // Full implementation would be:
    // Step 2: Batch evaluate element-level expression
    exec::EvalCtx eval_ctx(exec_context_, expr_set_, &element_ids);
    std::vector<VectorPtr> results;

    // Evaluate the expression set (should contain only element_expr)
    expr_set_->Eval(0, 1, true, eval_ctx, results);

    AssertInfo(results.size() == 1 && results[0] != nullptr,
               "ElementFilterIterator: expression evaluation should return "
               "exactly one result");

    // Step 3: Extract evaluation results as bitmap
    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
    AssertInfo(col_vec != nullptr,
               "ElementFilterIterator: result should be ColumnVector");
    AssertInfo(col_vec->IsBitmap(),
               "ElementFilterIterator: result should be bitmap");

    auto col_vec_size = col_vec->size();
    AssertInfo(col_vec_size == element_ids.size(),
               "ElementFilterIterator: evaluation result size mismatch");

    TargetBitmapView bitsetview(col_vec->GetRawData(), col_vec_size);

    // Step 4: Filter elements based on evaluation results and cache them
    for (size_t i = 0; i < element_ids.size(); ++i) {
        if (bitsetview[i] > 0) {
            // Element passes filter, cache it
            filtered_buffer_.emplace_back(element_ids[i], distances[i]);
        }
    }
}

}  // namespace milvus
