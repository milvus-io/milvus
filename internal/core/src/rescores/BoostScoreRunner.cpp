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

}  // namespace

void
ComputeScorerScores(exec::ExecContext* exec_context,
                    OpContext* op_context,
                    const segcore::SegmentInternalInterface* segment,
                    const std::shared_ptr<Scorer>& scorer,
                    FixedVector<int32_t>& offsets,
                    std::vector<std::optional<float>>& output_scores) {
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

    std::vector<expr::TypedExprPtr> filters;
    filters.emplace_back(filter);
    auto expr_set = std::make_unique<exec::ExprSet>(filters, exec_context);
    std::vector<VectorPtr> results;
    exec::EvalCtx eval_ctx(exec_context);

    const auto& exprs = expr_set->exprs();
    bool is_native_supported = true;
    for (const auto& expr : exprs) {
        is_native_supported =
            (is_native_supported && (expr->SupportOffsetInput()));
    }

    if (is_native_supported) {
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
        expr_set->Eval(0, 1, true, eval_ctx, results);

        AssertInfo(!results.empty() && results[0] != nullptr,
                   "ComputeScorerScores: filter expr returned null result, "
                   "filter: {}",
                   filter->ToString());
        auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
        AssertInfo(col_vec != nullptr,
                   "ComputeScorerScores: failed to cast result to "
                   "ColumnVector, filter: {}",
                   filter->ToString());
        TargetBitmap bitset;
        auto col_vec_size = col_vec->size();
        TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
        bitset.append(view);
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
                    bool* output_has_score) {
    std::vector<std::optional<float>> scores(offsets.size());
    ComputeScorerScores(
        exec_context, op_context, segment, scorer, offsets, scores);
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
