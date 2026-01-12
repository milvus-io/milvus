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

#include <memory>
#include <vector>

#include "exec/expression/Expr.h"

namespace milvus {
namespace exec {

class PhyUnaryRangeFilterExpr;

// PhyLikeConjunctExpr optimizes multiple LIKE expressions with ngram index.
// Phase1 (ngram index query) executes once for the entire segment.
// Phase2 (post-filter) executes per batch with batch-level pre_filter.
class PhyLikeConjunctExpr : public Expr {
 public:
    PhyLikeConjunctExpr(
        std::vector<std::shared_ptr<PhyUnaryRangeFilterExpr>> ngram_exprs,
        milvus::OpContext* op_ctx,
        int64_t active_count,
        int64_t batch_size);

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::string
    ToString() const override {
        return "PhyLikeConjunctExpr";
    }

 private:
    int64_t
    GetNextBatchSize();

    std::vector<std::shared_ptr<PhyUnaryRangeFilterExpr>> ngram_exprs_;
    // Cached Phase1 result (segment-level ngram index query result)
    std::shared_ptr<TargetBitmap> cached_phase1_res_{nullptr};

    int64_t active_count_{0};
    int64_t batch_size_{0};
    int64_t current_pos_{0};
};

}  // namespace exec
}  // namespace milvus
