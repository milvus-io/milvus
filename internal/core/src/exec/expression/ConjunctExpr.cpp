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
#include "UnaryExpr.h"
#include "LikeConjunctExpr.h"

#include <algorithm>
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

static bool
AllTrue(ColumnVectorPtr& vec) {
    TargetBitmapView data(vec->GetRawData(), vec->size());
    return data.all();
}

static void
AllSet(ColumnVectorPtr& vec) {
    TargetBitmapView data(vec->GetRawData(), vec->size());
    data.set();
}

static void
AllReset(ColumnVectorPtr& vec) {
    TargetBitmapView data(vec->GetRawData(), vec->size());
    data.reset();
}

static bool
AllFalse(ColumnVectorPtr& vec) {
    TargetBitmapView data(vec->GetRawData(), vec->size());
    return data.none();
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
    if ((is_and_ && AllFalse(vec)) || (!is_and_ && AllTrue(vec))) {
        return true;
    }
    return false;
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

    auto has_input_offset = context.get_offset_input() != nullptr;
    if (!has_input_offset && !like_batch_initialized_ && is_and_ &&
        like_indices_.size() > 1) {
        like_batch_initialized_ = true;
        // Collect LIKE expressions that can use ngram index at runtime
        std::vector<std::shared_ptr<PhyUnaryRangeFilterExpr>> ngram_exprs;
        for (size_t idx : like_indices_) {
            auto unary_expr =
                std::dynamic_pointer_cast<PhyUnaryRangeFilterExpr>(
                    inputs_[idx]);
            if (unary_expr && unary_expr->CanUseNgramIndex()) {
                ngram_exprs.push_back(unary_expr);
                batch_ngram_indices_.insert(idx);
            }
        }

        // Create PhyLikeConjunctExpr and add to inputs_ if we have >= 2 eligible
        if (ngram_exprs.size() >= 2) {
            auto active_count = ngram_exprs[0]->GetActiveCount();
            auto like_conjunct = std::make_shared<PhyLikeConjunctExpr>(
                std::move(ngram_exprs),
                op_ctx_,
                active_count,
                context.get_query_config()->get_expr_batch_size());
            inputs_.push_back(like_conjunct);
        } else {
            batch_ngram_indices_.clear();
            // Remove the like_conjunct index from input_order_ since we're not
            // creating the batch expression. The index was reserved at compile
            // time but the PhyLikeConjunctExpr is not being created at runtime.
            auto original_size = inputs_.size();
            input_order_.erase(std::remove_if(input_order_.begin(),
                                              input_order_.end(),
                                              [original_size](size_t idx) {
                                                  return idx >= original_size;
                                              }),
                               input_order_.end());
        }
    }

    bool has_result = false;
    for (size_t i = 0; i < input_order_.size(); ++i) {
        size_t idx = input_order_[i];

        // Skip expressions already executed via batch ngram
        if (batch_ngram_indices_.count(idx)) {
            continue;
        }

        VectorPtr input_result;
        inputs_[idx]->Eval(context, input_result);

        if (!has_result) {
            result = input_result;
            has_result = true;
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
