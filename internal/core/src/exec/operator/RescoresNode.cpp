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

#include "RescoresNode.h"
#include <cstddef>
#include "exec/operator/Utils.h"
#include "log/Log.h"
#include "monitor/Monitor.h"
#include "pb/plan.pb.h"

namespace milvus::exec {

PhyRescoresNode::PhyRescoresNode(
    int32_t operator_id,
    DriverContext* ctx,
    const std::shared_ptr<const plan::RescoresNode>& scorer)
    : Operator(ctx,
               scorer->output_type(),
               operator_id,
               scorer->id(),
               "PhyRescoresNode") {
    scorers_ = scorer->scorers();
    option_ = scorer->option();
};

void
PhyRescoresNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

bool
PhyRescoresNode::IsFinished() {
    return is_finished_;
}

RowVectorPtr
PhyRescoresNode::GetOutput() {
    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

    DeferLambda([&]() { is_finished_ = true; });

    if (input_ == nullptr) {
        return nullptr;
    }

    std::chrono::high_resolution_clock::time_point scalar_start =
        std::chrono::high_resolution_clock::now();

    ExecContext* exec_context = operator_context_->get_exec_context();
    auto query_context_ = exec_context->get_query_context();
    auto query_info = exec_context->get_query_config();
    milvus::SearchResult search_result = query_context_->get_search_result();
    auto segment = query_context_->get_segment();
    auto op_ctx = query_context_->get_op_context();

    // prepare segment offset
    FixedVector<int32_t> offsets;
    std::vector<size_t> offset_idx;

    for (size_t i = 0; i < search_result.seg_offsets_.size(); i++) {
        // remain offset will be placeholder(-1) if result count not enough (less than topk)
        // skip placeholder offset
        if (search_result.seg_offsets_[i] >= 0) {
            offsets.push_back(
                static_cast<int32_t>(search_result.seg_offsets_[i]));
            offset_idx.push_back(i);
        }
    }

    // skip rescore if result was empty
    if (offsets.empty()) {
        query_context_->set_search_result(std::move(search_result));
        return input_;
    }

    std::vector<std::optional<float>> boost_scores(offsets.size());
    auto function_mode = option_->function_mode();

    for (auto& scorer : scorers_) {
        auto filter = scorer->filter();
        // boost for all result if no filter
        if (!filter) {
            scorer->batch_score(
                op_ctx, segment, function_mode, offsets, boost_scores);
            continue;
        }

        std::vector<expr::TypedExprPtr> filters;
        filters.emplace_back(filter);
        auto expr_set = std::make_unique<ExprSet>(filters, exec_context);
        std::vector<VectorPtr> results;
        EvalCtx eval_ctx(exec_context, expr_set.get());

        const auto& exprs = expr_set->exprs();
        bool is_native_supported = true;
        for (const auto& expr : exprs) {
            is_native_supported =
                (is_native_supported && (expr->SupportOffsetInput()));
        }

        if (is_native_supported) {
            // could set input offset if expr was native supported
            eval_ctx.set_offset_input(&offsets);
            expr_set->Eval(0, 1, true, eval_ctx, results);

            // filter result for offsets[i] was resut bitset[i]
            auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
            auto col_vec_size = col_vec->size();
            TargetBitmapView bitsetview(col_vec->GetRawData(), col_vec_size);
            scorer->batch_score(op_ctx,
                                segment,
                                function_mode,
                                offsets,
                                bitsetview,
                                boost_scores);
        } else {
            // query all segment if expr not native
            expr_set->Eval(0, 1, true, eval_ctx, results);

            // filter result for offsets[i] was bitset[offset[i]]
            TargetBitmap bitset;
            auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
            auto col_vec_size = col_vec->size();
            TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
            bitset.append(view);
            scorer->batch_score(
                op_ctx, segment, function_mode, offsets, bitset, boost_scores);
        }
    }

    // calculate final score
    auto boost_mode = option_->boost_mode();
    switch (boost_mode) {
        case proto::plan::BoostModeMultiply:
            for (auto i = 0; i < offsets.size(); i++) {
                if (boost_scores[i].has_value()) {
                    search_result.distances_[offset_idx[i]] *=
                        boost_scores[i].value();
                }
            }
            break;
        case proto::plan::BoostModeSum:
            for (auto i = 0; i < offsets.size(); i++) {
                if (boost_scores[i].has_value()) {
                    search_result.distances_[offset_idx[i]] +=
                        boost_scores[i].value();
                }
            }

            break;
        default:
            ThrowInfo(ErrorCode::UnexpectedError,
                      fmt::format("unknown boost boost mode: {}", boost_mode));
    }

    knowhere::MetricType metric_type = query_context_->get_metric_type();
    bool large_is_better = PositivelyRelated(metric_type);
    sort_search_result(search_result, large_is_better);
    query_context_->set_search_result(std::move(search_result));

    std::chrono::high_resolution_clock::time_point scalar_end =
        std::chrono::high_resolution_clock::now();
    double scalar_cost =
        std::chrono::duration<double, std::micro>(scalar_end - scalar_start)
            .count();
    milvus::monitor::internal_core_search_latency_rescore.Observe(scalar_cost /
                                                                  1000);
    return input_;
};

}  // namespace milvus::exec