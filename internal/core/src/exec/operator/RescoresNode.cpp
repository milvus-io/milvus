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
#include "monitor/Monitor.h"

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
    ExecContext* exec_context = operator_context_->get_exec_context();
    auto query_context_ = exec_context->get_query_context();
    auto query_info = exec_context->get_query_config();
    milvus::SearchResult search_result = query_context_->get_search_result();

    // prepare segment offset
    FixedVector<int32_t> offsets;
    std::vector<size_t> offset_idx;

    for (size_t i = 0; i < search_result.seg_offsets_.size(); i++) {
        // remain offset will be -1 if result count not enough (less than topk)
        // skip placeholder offset
        if (search_result.seg_offsets_[i] >= 0){
            offsets.push_back(static_cast<int32_t>(search_result.seg_offsets_[i]));
            offset_idx.push_back(i);
        }
    }

    for (auto& scorer : scorers_) {
        auto filter = scorer->filter();
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
            Assert(bitsetview.size() == offsets.size());
            for (auto i = 0; i < offsets.size(); i++) {
                if (bitsetview[i] > 0) {
                    search_result.distances_[offset_idx[i]] =
                        scorer->rescore(search_result.distances_[offset_idx[i]]);
                }
            }
        } else {
            // query all segment if expr not native
            expr_set->Eval(0, 1, true, eval_ctx, results);

            // filter result for offsets[i] was bitset[offset[i]]
            TargetBitmap bitset;
            auto col_vec = std::dynamic_pointer_cast<ColumnVector>(results[0]);
            auto col_vec_size = col_vec->size();
            TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
            bitset.append(view);
            for (auto i = 0; i < offsets.size(); i++) {
                if (bitset[offsets[i]] > 0) {
                    search_result.distances_[offset_idx[i]] =
                        scorer->rescore(search_result.distances_[offset_idx[i]]);
                }
            }
        }
    }

    knowhere::MetricType metric_type = query_context_->get_metric_type();
    bool large_is_better = PositivelyRelated(metric_type);
    sort_search_result(search_result, large_is_better);
    query_context_->set_search_result(std::move(search_result));
    return input_;
};

}  // namespace milvus::exec