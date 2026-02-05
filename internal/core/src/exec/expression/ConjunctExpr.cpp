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

#include "ConjunctExpr.h"
#include "common/ValueOp.h"

namespace milvus {
namespace exec {

DataType
PhyConjunctFilterExpr::ResolveType(const std::vector<DataType>& inputs) {
    AssertInfo(
        inputs.size() > 0,
        fmt::format(
            "Conjunct expressions expect at least one argument, received: {}",
            inputs.size()));

    for (const auto& type : inputs) {
        AssertInfo(
            type == DataType::BOOL,
            fmt::format("Conjunct expressions expect BOOLEAN, received: {}",
                        type));
    }
    return DataType::BOOL;
}

int64_t
PhyConjunctFilterExpr::UpdateResult(ColumnVectorPtr& input_result,
                                    EvalCtx& ctx,
                                    ColumnVectorPtr& result) {
    if (is_and_) {
        common::ThreeValuedLogicOp::And(result, input_result);
    } else {
        common::ThreeValuedLogicOp::Or(result, input_result);
    }

    // Return the count of active rows for short-circuit optimization
    // For AND: return count of true (if 0, all false, can skip)
    // For OR: return count of false (if 0, all true, can skip)
    TargetBitmapView res_data(result->GetRawData(), result->size());
    if (is_and_) {
        return static_cast<int64_t>(res_data.count());
    } else {
        return static_cast<int64_t>(result->size() - res_data.count());
    }
}

bool
PhyConjunctFilterExpr::CanSkipFollowingExprs(ColumnVectorPtr& vec) {
    // For AND: can only skip if ALL rows are definitely FALSE (valid=1, data=0)
    //   - If any row is TRUE, we need to continue to determine final result
    //   - If any row is NULL, we need to continue because NULL AND FALSE = FALSE
    //     but NULL AND TRUE = NULL, so the result depends on following exprs
    //
    // For OR: can only skip if ALL rows are definitely TRUE (valid=1, data=1)
    //   - If any row is FALSE, we need to continue to determine final result
    //   - If any row is NULL, we need to continue because NULL OR TRUE = TRUE
    //     but NULL OR FALSE = NULL, so the result depends on following exprs
    return is_and_ ? vec->AllFalse() : vec->AllTrue();
}

void
PhyConjunctFilterExpr::SkipFollowingExprs(int start) {
    for (int i = start; i < input_order_.size(); ++i) {
        inputs_[input_order_[i]]->MoveCursor();
    }
}

void
PhyConjunctFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span(
        "PhyConjunctFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("is_and", is_and_);

    if (input_order_.empty()) {
        input_order_.resize(inputs_.size());
        for (size_t i = 0; i < inputs_.size(); i++) {
            input_order_[i] = i;
        }
    }
    for (int i = 0; i < input_order_.size(); ++i) {
        VectorPtr input_result;
        inputs_[input_order_[i]]->Eval(context, input_result);
        if (i == 0) {
            result = input_result;
            auto all_flat_result = GetColumnVector(result);
            if (CanSkipFollowingExprs(all_flat_result)) {
                SkipFollowingExprs(i + 1);
                ClearBitmapInput(context);
                return;
            }
            SetNextExprBitmapInput(all_flat_result, context);
            continue;
        }
        auto input_flat_result = GetColumnVector(input_result);
        auto all_flat_result = GetColumnVector(result);
        auto active_rows =
            UpdateResult(input_flat_result, context, all_flat_result);
        if (active_rows == 0) {
            SkipFollowingExprs(i + 1);
            ClearBitmapInput(context);
            return;
        }
        SetNextExprBitmapInput(all_flat_result, context);
    }
    ClearBitmapInput(context);
}

}  //namespace exec
}  // namespace milvus
