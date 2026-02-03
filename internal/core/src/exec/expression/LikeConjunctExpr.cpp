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

#include "exec/expression/LikeConjunctExpr.h"

#include <utility>

#include "bitset/bitset.h"
#include "common/EasyAssert.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/UnaryExpr.h"

namespace milvus {

namespace exec {

PhyLikeConjunctExpr::PhyLikeConjunctExpr(
    std::vector<std::shared_ptr<PhyUnaryRangeFilterExpr>> ngram_exprs,
    milvus::OpContext* op_ctx,
    int64_t active_count,
    int64_t batch_size)
    : Expr(DataType::BOOL, {}, "PhyLikeConjunctExpr", op_ctx),
      ngram_exprs_(std::move(ngram_exprs)),
      active_count_(active_count),
      batch_size_(batch_size) {
}

int64_t
PhyLikeConjunctExpr::GetNextBatchSize() {
    return current_pos_ + batch_size_ >= active_count_
               ? active_count_ - current_pos_
               : batch_size_;
}

void
PhyLikeConjunctExpr::Eval(EvalCtx& context, VectorPtr& result) {
    AssertInfo(context.get_offset_input() == nullptr,
               "Offset input is not supported for PhyLikeConjunctExpr");
    AssertInfo(!ngram_exprs_.empty(),
               "PhyLikeConjunctExpr requires at least one expression");

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        result = nullptr;
        return;
    }

    // Phase 1: Execute once for entire segment (no bitmap_input needed)
    if (cached_phase1_res_ == nullptr) {
        TargetBitmap candidates(active_count_, true);

        // Execute all ngram index queries, AND merge results
        for (auto& expr : ngram_exprs_) {
            expr->ExecuteNgramPhase1(candidates);
            if (candidates.none()) {
                break;
            }
        }

        cached_phase1_res_ =
            std::make_shared<TargetBitmap>(std::move(candidates));
    }

    // Phase 2: Execute per batch with batch-level bitmap_input
    // Extract current batch from Phase1 result
    TargetBitmap batch_candidates;
    batch_candidates.append(*cached_phase1_res_, current_pos_, real_batch_size);

    // Apply batch-level pre_filter from previous expressions in conjunction
    const auto& bitmap_input = context.get_bitmap_input();
    if (!bitmap_input.empty()) {
        AssertInfo(static_cast<int64_t>(bitmap_input.size()) == real_batch_size,
                   "bitmap_input size {} != real_batch_size {}",
                   bitmap_input.size(),
                   real_batch_size);
        batch_candidates &= bitmap_input;
    }

    // Execute Phase2 (post-filter) on this batch
    if (!batch_candidates.none()) {
        for (auto& expr : ngram_exprs_) {
            expr->ExecuteNgramPhase2(
                batch_candidates, current_pos_, real_batch_size);
            if (batch_candidates.none()) {
                break;
            }
        }
    }

    // For ngram like expression, the valid result is always true as result has all information
    TargetBitmap valid_result(real_batch_size, true);
    current_pos_ += real_batch_size;
    result = std::make_shared<ColumnVector>(std::move(batch_candidates),
                                            std::move(valid_result));
}
}  // namespace exec
}  // namespace milvus
