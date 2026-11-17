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

#include <folly/ScopeGuard.h>
#include <algorithm>

#include "LikeConjunctExpr.h"
#include "RawExprCacheAdapter.h"
#include "UnaryExpr.h"
#include "common/EasyAssert.h"
#include "common/Tracer.h"
#include "common/ValueOp.h"
#include "exec/QueryContext.h"
#include "exec/expression/Utils.h"
#include "fmt/core.h"
#include "opentelemetry/trace/span.h"

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

TargetBitmap
PhyConjunctFilterExpr::BuildActiveBitmap(const ColumnVectorPtr& vec) {
    // Rows that still need the following expressions.
    //
    // For AND: TRUE or NULL rows still need evaluation; only definite FALSE
    //   can stop (NULL AND FALSE = FALSE, so a NULL row can still change).
    //   With a null-rejecting consumer, NULL is already equivalent to FALSE
    //   downstream, so NULL rows are dropped too and only TRUE rows stay
    //   active — restoring the pre-3VL early exit for UNKNOWN-heavy batches.
    // For OR: FALSE or NULL rows still need evaluation; only definite TRUE
    //   can stop. A NULL row can still become TRUE (NULL OR TRUE = TRUE),
    //   so null-rejection does not shrink the active set for OR.
    const size_t size = vec->size();
    TargetBitmapView data(vec->GetRawData(), size);
    TargetBitmapView valid(vec->GetValidRawData(), size);
    if (is_and_) {
        if (null_rejecting_) {
            TargetBitmap active_rows(data);
            active_rows.inplace_and(valid, size);  // data & valid
            return active_rows;
        }
        TargetBitmap active_rows(valid);
        active_rows.inplace_sub(data, size);  // valid & ~data
        active_rows.flip();                   // data | ~valid
        return active_rows;
    }
    TargetBitmap active_rows(data);
    active_rows.inplace_and(valid, size);  // data & valid
    active_rows.flip();                    // ~data | ~valid
    return active_rows;
}

void
PhyConjunctFilterExpr::ApplyPendingSkips(size_t index) {
    auto it = pending_skips_.find(index);
    if (it != pending_skips_.end()) {
        auto& input = inputs_[index];
        input->WaitPrefetch();
        for (size_t i = 0; i < it->second; ++i) {
            input->MoveCursor();
        }
        pending_skips_.erase(it);
    }
}

void
PhyConjunctFilterExpr::SkipInput(size_t index) {
    if (!inputs_[index]->PrefetchReady()) {
        ++pending_skips_[index];
        return;
    }
    ApplyPendingSkips(index);
    inputs_[index]->MoveCursor();
}

void
PhyConjunctFilterExpr::SkipFollowingExprs(int start) {
    for (int i = start; i < input_order_.size(); ++i) {
        SkipInput(input_order_[i]);
    }
}

void
PhyConjunctFilterExpr::UseIndividualLikes() {
    like_batch_initialized_ = true;
    batch_ngram_indices_.clear();
    // A skipped batch advances the original predicates. Keep their cursors
    // if a later batch reaches LIKE for the first time.
    std::erase(input_order_, inputs_.size());
}

void
PhyConjunctFilterExpr::PrepareLikeBatch(bool has_offsets) {
    if (has_offsets) {
        UseIndividualLikes();
        return;
    }
    std::vector<std::shared_ptr<PhyUnaryRangeFilterExpr>> ngrams;
    for (auto idx : like_indices_) {
        auto input = inputs_[idx];
        input->WaitPrefetch();
        if (auto adapter =
                std::dynamic_pointer_cast<RawExprCacheAdapter>(input)) {
            input = adapter->wrapped_expr();
        }
        auto unary = std::dynamic_pointer_cast<PhyUnaryRangeFilterExpr>(input);
        if (unary) {
            unary->EnsureExecPathDetermined();
        }
        if (unary && unary->CanUseNgramIndex()) {
            ngrams.push_back(unary);
            batch_ngram_indices_.insert(idx);
        }
    }
    if (ngrams.size() < 2) {
        UseIndividualLikes();
        return;
    }
    like_batch_initialized_ = true;
    const auto rows = ngrams[0]->GetActiveCount();
    const auto batch = ngrams[0]->GetNextBatchSize();
    inputs_.push_back(std::make_shared<PhyLikeConjunctExpr>(
        std::move(ngrams), op_ctx_, rows, batch));
}

void
PhyConjunctFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span(
        "PhyConjunctFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.SetAttribute("is_and", is_and_);

    TargetBitmap inherited_bitmap;
    bool has_result = false;
    auto restore_bitmap =
        folly::makeGuard([&context, &inherited_bitmap, &has_result] {
            if (has_result) {
                context.set_bitmap_input(std::move(inherited_bitmap));
            }
        });

    if (input_order_.empty()) {
        input_order_.resize(inputs_.size());
        for (size_t i = 0; i < inputs_.size(); i++) {
            input_order_[i] = i;
        }
    }

    auto last_evaluated = [&] {
        size_t i = input_order_.size();
        while (i > 0 && batch_ngram_indices_.count(input_order_[i - 1])) {
            --i;
        }
        return i;
    };
    size_t eval_end = last_evaluated();
    for (size_t i = 0; i < input_order_.size(); ++i) {
        if (HasPendingLikeBatch() &&
            (input_order_[i] >= inputs_.size() ||
             std::find(like_indices_.begin(),
                       like_indices_.end(),
                       input_order_[i]) != like_indices_.end())) {
            PrepareLikeBatch(context.get_offset_input() != nullptr);
            eval_end = last_evaluated();
            if (i >= eval_end) {
                break;
            }
        }
        const size_t idx = input_order_[i];

        // Skip expressions already executed via batch ngram
        if (batch_ngram_indices_.count(idx)) {
            continue;
        }

        VectorPtr input_result;
        if (!pending_skips_.empty()) {
            ApplyPendingSkips(idx);
        }
        inputs_[idx]->Eval(context, input_result);

        ColumnVectorPtr all_flat_result;
        if (!has_result) {
            // The first input needs the parent's mask in context. Take it only
            // after that input succeeds, then restore its storage on every exit.
            inherited_bitmap = context.take_bitmap_input();
            has_result = true;
            result = input_result;
            all_flat_result = GetColumnVector(result);
        } else {
            auto input_flat_result = GetColumnVector(input_result);
            all_flat_result = GetColumnVector(result);
            if (is_and_) {
                common::ThreeValuedLogicOp::And(all_flat_result,
                                                input_flat_result);
            } else {
                common::ThreeValuedLogicOp::Or(all_flat_result,
                                               input_flat_result);
            }
        }

        // The last evaluated expression needs neither a skip decision nor a
        // bitmap input for a successor.
        if (i + 1 == eval_end) {
            break;
        }

        // Build the active-row bitmap once per input: it decides the
        // batch-level early exit, and the same bitmap becomes the row-level
        // input of the next expression.
        auto active_rows = BuildActiveBitmap(all_flat_result);
        // Local results outside the parent's candidates are unspecified and
        // must neither reactivate rows nor prevent a batch-level early exit.
        if (!inherited_bitmap.empty()) {
            active_rows.inplace_and(inherited_bitmap, active_rows.size());
        }
        if (active_rows.none()) {
            if (HasPendingLikeBatch()) {
                UseIndividualLikes();
            }
            SkipFollowingExprs(i + 1);
            return;
        }
        context.set_bitmap_input(std::move(active_rows));
    }
}

}  //namespace exec
}  // namespace milvus
