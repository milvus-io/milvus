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

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "exec/QueryContext.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"

namespace milvus::test {

struct ExprBatchEvalResult {
    std::vector<int64_t> batch_sizes;
    ColumnVectorPtr result;
};

class ExprBatchSizeGuard {
 public:
    explicit ExprBatchSizeGuard(int64_t batch_size)
        : old_batch_size_(EXEC_EVAL_EXPR_BATCH_SIZE.exchange(batch_size)) {
    }

    ~ExprBatchSizeGuard() {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(old_batch_size_);
    }

 private:
    int64_t old_batch_size_;
};

inline ExprBatchEvalResult
EvalExprInBatches(const expr::TypedExprPtr& logical_expr,
                  const segcore::SegmentInternalInterface* segment,
                  int64_t active_count) {
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical_expr}, &exec_context, {}, false);
    AssertInfo(compiled.size() == 1,
               "expected one compiled expression, got {}",
               compiled.size());

    exec::EvalCtx eval_context(&exec_context);
    ExprBatchEvalResult evaluation;
    TargetBitmap combined_result;
    TargetBitmap combined_validity;
    int64_t processed_rows = 0;
    while (processed_rows < active_count) {
        VectorPtr result;
        compiled[0]->Eval(eval_context, result);
        AssertInfo(result != nullptr,
                   "expression evaluation stopped after {} of {} rows",
                   processed_rows,
                   active_count);
        auto column = std::dynamic_pointer_cast<ColumnVector>(result);
        AssertInfo(column != nullptr && column->IsBitmap(),
                   "expected bitmap column result");
        const auto batch_size = column->size();
        AssertInfo(
            batch_size > 0 && processed_rows + batch_size <= active_count,
            "invalid expression batch size {} after {} of {} rows",
            batch_size,
            processed_rows,
            active_count);
        evaluation.batch_sizes.push_back(batch_size);
        combined_result.append(
            TargetBitmapView(column->GetRawData(), batch_size));
        combined_validity.append(
            TargetBitmapView(column->GetValidRawData(), batch_size));
        processed_rows += batch_size;
    }
    evaluation.result = std::make_shared<ColumnVector>(
        std::move(combined_result), std::move(combined_validity));
    return evaluation;
}

inline std::vector<int64_t>
EvalExprBatchSizes(const expr::TypedExprPtr& logical_expr,
                   const segcore::SegmentInternalInterface* segment,
                   int64_t active_count) {
    return EvalExprInBatches(logical_expr, segment, active_count).batch_sizes;
}

inline bool
CanExprExecuteAllAtOnce(const expr::TypedExprPtr& logical_expr,
                        const segcore::SegmentInternalInterface* segment,
                        int64_t active_count) {
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical_expr}, &exec_context, {}, false);
    AssertInfo(compiled.size() == 1,
               "expected one compiled expression, got {}",
               compiled.size());
    return compiled[0]->CanExecuteAllAtOnce();
}

}  // namespace milvus::test
