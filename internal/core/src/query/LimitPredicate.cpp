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

#include "query/LimitPredicate.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>

#include "common/EasyAssert.h"
#include "common/Tracer.h"
#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "segcore/InsertRecord.h"

namespace milvus::query {

namespace {
// Smallest first batch. A limit of 1 still evaluates a few rows per call so a
// low hit ratio does not turn into one expression call per row.
constexpr int64_t kMinBatchRows = 32;
}  // namespace

bool
EvalLimitPredicate(exec::QueryContext* query_context,
                   const expr::TypedExprPtr& predicate,
                   int64_t limit,
                   std::vector<int64_t>& offsets,
                   std::vector<std::vector<int32_t>>* element_indices) {
    AssertInfo(query_context != nullptr && predicate != nullptr,
               "limit predicate requires a query context and a predicate");
    AssertInfo(
        element_indices == nullptr || element_indices->size() == offsets.size(),
        "element index lists must align with candidate offsets");
    if (offsets.empty()) {
        return false;
    }
    tracer::AutoSpan span("EvalLimitPredicate", tracer::GetRootSpan(), true);

    exec::ExecContext exec_context(query_context);
    std::vector<expr::TypedExprPtr> filters{predicate};
    // UNKNOWN rows do not pass, exactly as in the filter node.
    exec::ExprSet expr_set(filters, &exec_context, /*null_rejecting=*/true);
    for (const auto& expr : expr_set.exprs()) {
        AssertInfo(expr->SupportOffsetInput(),
                   "limit predicate must support offset evaluation: {}",
                   predicate->ToString());
    }
    expr_set.WaitPrefetch();

    const int64_t quota =
        (limit == segcore::Unlimited || limit == segcore::NoLimit || limit < 0)
            ? std::numeric_limits<int64_t>::max()
            : limit;
    const int64_t max_batch = std::max<int64_t>(
        kMinBatchRows, query_context->query_config()->get_expr_batch_size());
    const auto total = static_cast<int64_t>(offsets.size());

    exec::OffsetVector batch_offsets;
    std::vector<VectorPtr> results;
    int64_t passed_units = 0;
    int64_t kept = 0;
    // Number of leading candidates whose verdict has been consumed.
    int64_t consumed = 0;
    int64_t batch_rows = 0;
    while (consumed < total && passed_units < quota) {
        milvus::exec::checkCancellation(query_context);
        // First batch: about as many rows as are still needed. A batch that
        // does not fill the quota doubles the next one.
        batch_rows = batch_rows == 0
                         ? std::clamp(quota, kMinBatchRows, max_batch)
                         : std::min(max_batch, batch_rows * 2);
        const auto count = std::min(batch_rows, total - consumed);
        batch_offsets.resize(count);
        for (int64_t i = 0; i < count; ++i) {
            const auto offset = offsets[consumed + i];
            AssertInfo(
                offset >= 0 && offset <= std::numeric_limits<int32_t>::max(),
                "candidate offset {} out of range",
                offset);
            batch_offsets[i] = static_cast<int32_t>(offset);
        }

        exec::EvalCtx eval_ctx(&exec_context);
        eval_ctx.set_offset_input(&batch_offsets);
        expr_set.Eval(0, 1, true, eval_ctx, results);
        AssertInfo(!results.empty() && results[0] != nullptr,
                   "limit predicate returned null result: {}",
                   predicate->ToString());
        auto column = std::dynamic_pointer_cast<ColumnVector>(results[0]);
        AssertInfo(column != nullptr && column->IsBitmap() &&
                       static_cast<int64_t>(column->size()) == count,
                   "limit predicate must return one bit per candidate");
        TargetBitmapView data(column->GetRawData(), count);
        TargetBitmapView valid(column->GetValidRawData(), count);

        int64_t i = 0;
        for (; i < count && passed_units < quota; ++i) {
            if (!data[i] || !valid[i]) {
                continue;
            }
            const auto source = consumed + i;
            offsets[kept] = offsets[source];
            if (element_indices != nullptr) {
                const auto units =
                    static_cast<int64_t>((*element_indices)[source].size());
                passed_units += std::min(
                    units, std::numeric_limits<int64_t>::max() - passed_units);
                if (kept != source) {
                    (*element_indices)[kept] =
                        std::move((*element_indices)[source]);
                }
            } else {
                ++passed_units;
            }
            ++kept;
        }
        consumed += i;
    }

    offsets.resize(kept);
    if (element_indices != nullptr) {
        element_indices->resize(kept);
    }
    return consumed < total;
}

}  // namespace milvus::query
