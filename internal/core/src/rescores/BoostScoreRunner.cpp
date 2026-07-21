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

#include "rescores/BoostScoreRunner.h"

#include <memory>

#include "common/EasyAssert.h"
#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "rescores/Utils.h"

namespace milvus::rescores {

namespace {

void
CopyScoresToBuffers(const std::vector<std::optional<float>>& scores,
                    float* output_scores,
                    bool* output_has_score) {
    for (auto i = 0; i < scores.size(); i++) {
        output_has_score[i] = scores[i].has_value();
        output_scores[i] = scores[i].value_or(0.0F);
    }
}

std::unique_ptr<exec::ExprSet>
BuildFilterExprSet(const expr::TypedExprPtr& filter,
                   exec::ExecContext* exec_context) {
    std::vector<expr::TypedExprPtr> filters;
    filters.emplace_back(filter);
    return std::make_unique<exec::ExprSet>(filters, exec_context);
}

bool
AllSupportOffsetInput(const exec::ExprSet& expr_set) {
    for (const auto& expr : expr_set.exprs()) {
        if (!expr->SupportOffsetInput()) {
            return false;
        }
    }
    return true;
}

// Without offset-input support each Eval only advances the expression by one
// internal batch (DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE rows), while the scorer
// offsets may reference any row of the segment. Accumulate every batch so the
// bitset covers all active rows.
TargetBitmap
EvalFilterOverAllBatches(exec::ExecContext* exec_context,
                         exec::ExprSet& expr_set,
                         const expr::TypedExprPtr& filter) {
    std::vector<VectorPtr> results;
    exec::EvalCtx eval_ctx(exec_context);
    auto active_count = exec_context->get_query_context()->get_active_count();
    TargetBitmap bitset;
    TargetBitmap valid_bitset;
    while (static_cast<int64_t>(bitset.size()) < active_count) {
        expr_set.Eval(0, 1, true, eval_ctx, results);

        AssertInfo(!results.empty() && results[0] != nullptr,
                   "ComputeScorerScores: filter expr returned null result, "
                   "filter: {}",
                   filter->ToString());
        auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
        AssertInfo(col_vec != nullptr,
                   "ComputeScorerScores: failed to cast result to "
                   "ColumnVector, filter: {}",
                   filter->ToString());
        auto col_vec_size = col_vec->size();
        AssertInfo(col_vec_size > 0,
                   "ComputeScorerScores: filter expr returned empty batch "
                   "after {} of {} rows, filter: {}",
                   bitset.size(),
                   active_count,
                   filter->ToString());
        TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
        bitset.append(view);
        TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec_size);
        valid_bitset.append(valid_view);
    }
    AssertInfo(static_cast<int64_t>(bitset.size()) == active_count,
               "ComputeScorerScores: filter bitset size {} must match "
               "active count {}, filter: {}",
               bitset.size(),
               active_count,
               filter->ToString());
    // Fold UNKNOWN (NULL) into FALSE explicitly so null rows never
    // receive a boost, matching PhyIterativeFilterNode's handling.
    bitset.inplace_and(valid_bitset, bitset.size());
    return bitset;
}

}  // namespace

std::optional<TargetBitmap>
ComputeNonNativeFilterBitset(exec::ExecContext* exec_context,
                             const std::shared_ptr<Scorer>& scorer,
                             std::unique_ptr<exec::ExprSet>* out_expr_set) {
    auto filter = scorer->filter();
    if (!filter) {
        return std::nullopt;
    }
    auto expr_set = BuildFilterExprSet(filter, exec_context);
    if (AllSupportOffsetInput(*expr_set)) {
        // Native filter: no whole-segment bitset, but hand the compiled
        // expressions back so per-chunk scoring does not rebuild them.
        if (out_expr_set != nullptr) {
            *out_expr_set = std::move(expr_set);
        }
        return std::nullopt;
    }
    auto bitset = EvalFilterOverAllBatches(exec_context, *expr_set, filter);
    // The non-native path is fully answered by the bitset; the expressions
    // have been advanced to the end of the segment and must not be reused.
    if (out_expr_set != nullptr) {
        out_expr_set->reset();
    }
    return bitset;
}

void
ComputeScorerScores(exec::ExecContext* exec_context,
                    OpContext* op_context,
                    const segcore::SegmentInternalInterface* segment,
                    const std::shared_ptr<Scorer>& scorer,
                    FixedVector<int32_t>& offsets,
                    std::vector<std::optional<float>>& output_scores,
                    const TargetBitmap* filter_bitset,
                    exec::ExprSet* prepared_expr_set) {
    AssertInfo(output_scores.size() == offsets.size(),
               "scorer score output size {} must match offsets size {}",
               output_scores.size(),
               offsets.size());

    auto function_mode = proto::plan::FunctionModeSum;
    auto filter = scorer->filter();
    if (!filter) {
        scorer->batch_score(
            op_context, segment, function_mode, offsets, output_scores);
        return;
    }

    if (filter_bitset != nullptr) {
        scorer->batch_score(op_context,
                            segment,
                            function_mode,
                            offsets,
                            *filter_bitset,
                            output_scores);
        return;
    }

    std::unique_ptr<exec::ExprSet> owned_expr_set;
    if (prepared_expr_set == nullptr) {
        owned_expr_set = BuildFilterExprSet(filter, exec_context);
        prepared_expr_set = owned_expr_set.get();
    }
    auto& expr_set = prepared_expr_set;
    if (AllSupportOffsetInput(*expr_set)) {
        std::vector<VectorPtr> results;
        exec::EvalCtx eval_ctx(exec_context);
        eval_ctx.set_offset_input(&offsets);
        expr_set->Eval(0, 1, true, eval_ctx, results);

        AssertInfo(!results.empty() && results[0] != nullptr,
                   "ComputeScorerScores: filter expr returned null result, "
                   "offsets size: {}, filter: {}",
                   offsets.size(),
                   filter->ToString());
        auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
        AssertInfo(col_vec != nullptr,
                   "ComputeScorerScores: failed to cast result to "
                   "ColumnVector, filter: {}",
                   filter->ToString());
        auto col_vec_size = col_vec->size();
        TargetBitmapView bitsetview(col_vec->GetRawData(), col_vec_size);
        scorer->batch_score(op_context,
                            segment,
                            function_mode,
                            offsets,
                            bitsetview,
                            output_scores);
    } else {
        auto bitset = EvalFilterOverAllBatches(exec_context, *expr_set, filter);
        scorer->batch_score(
            op_context, segment, function_mode, offsets, bitset, output_scores);
    }
}

void
ComputeScorerScores(exec::ExecContext* exec_context,
                    OpContext* op_context,
                    const segcore::SegmentInternalInterface* segment,
                    const std::shared_ptr<Scorer>& scorer,
                    FixedVector<int32_t>& offsets,
                    float* output_scores,
                    bool* output_has_score,
                    const TargetBitmap* filter_bitset,
                    exec::ExprSet* prepared_expr_set) {
    std::vector<std::optional<float>> scores(offsets.size());
    ComputeScorerScores(exec_context,
                        op_context,
                        segment,
                        scorer,
                        offsets,
                        scores,
                        filter_bitset,
                        prepared_expr_set);
    CopyScoresToBuffers(scores, output_scores, output_has_score);
}

void
ComputeFunctionScores(exec::ExecContext* exec_context,
                      OpContext* op_context,
                      const segcore::SegmentInternalInterface* segment,
                      const std::vector<std::shared_ptr<Scorer>>& scorers,
                      proto::plan::FunctionMode function_mode,
                      FixedVector<int32_t>& offsets,
                      std::vector<std::optional<float>>& output_scores) {
    AssertInfo(output_scores.size() == offsets.size(),
               "function score output size {} must match offsets size {}",
               output_scores.size(),
               offsets.size());

    for (const auto& scorer : scorers) {
        std::vector<std::optional<float>> scorer_scores(offsets.size());
        ComputeScorerScores(
            exec_context, op_context, segment, scorer, offsets, scorer_scores);
        for (auto i = 0; i < offsets.size(); i++) {
            if (!scorer_scores[i].has_value()) {
                continue;
            }
            if (!output_scores[i].has_value()) {
                output_scores[i] = scorer_scores[i].value();
            } else {
                output_scores[i] =
                    function_score_merge(output_scores[i].value(),
                                         scorer_scores[i].value(),
                                         function_mode);
            }
        }
    }
}

void
ComputeFunctionScores(exec::ExecContext* exec_context,
                      OpContext* op_context,
                      const segcore::SegmentInternalInterface* segment,
                      const std::vector<std::shared_ptr<Scorer>>& scorers,
                      proto::plan::FunctionMode function_mode,
                      FixedVector<int32_t>& offsets,
                      float* output_scores,
                      bool* output_has_score) {
    std::vector<std::optional<float>> scores(offsets.size());
    ComputeFunctionScores(exec_context,
                          op_context,
                          segment,
                          scorers,
                          function_mode,
                          offsets,
                          scores);
    CopyScoresToBuffers(scores, output_scores, output_has_score);
}

}  // namespace milvus::rescores
