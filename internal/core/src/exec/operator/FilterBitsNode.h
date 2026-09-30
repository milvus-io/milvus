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

#include <cstddef>
#include <memory>
#include <string>

#include "common/Types.h"
#include "exec/Driver.h"
#include "exec/FilterDiagnostics.h"
#include "exec/expression/Expr.h"
#include "exec/operator/Operator.h"
#include "exec/QueryContext.h"

namespace milvus {
namespace exec {

bool
ConvertPredicateToFilteredBitset(TargetBitmapView data,
                                 TargetBitmapView valid,
                                 size_t size);

class PhyFilterBitsNode : public Operator {
 public:
    PhyFilterBitsNode(
        int32_t operator_id,
        DriverContext* ctx,
        const std::shared_ptr<const plan::FilterBitsNode>& filter);

    ~PhyFilterBitsNode() override;

    bool
    IsFilter() const override {
        return true;
    }

    bool
    NeedInput() const override {
        return !input_;
    }

    void
    AddInput(RowVectorPtr& input) override;

    RowVectorPtr
    GetOutput() override;

    bool
    IsFinished() override;

    void
    Close() override {
        Operator::Close();
        exprs_->Clear();
    }

    BlockingReason
    IsBlocked(ContinueFuture* /* unused */) override {
        return BlockingReason::kNotBlocked;
    }

    bool
    AllInputProcessed();

    virtual std::string
    ToString() const override {
        return "PhyFilterBitsNode";
    }

    void
    PrefetchAsync(const std::shared_ptr<folly::CPUThreadPoolExecutor>
                      prefetch_pool) override {
        // Fusing clears the user ExprSet: only baseline prefetches whole columns.
        exprs_->PrefetchAsync(prefetch_pool);
    }

    void
    WaitPrefetch() override {
        FilterDiagnosticTimer timer(profile_ ? &profile_->wait_ns : nullptr);
        FilterDiagnosticFailure failure(profile_.get());
        exprs_->WaitPrefetch();
    }

 private:
    std::unique_ptr<ExprSet> exprs_;
    QueryContext* query_context_;
    int64_t num_processed_rows_;
    int64_t need_process_rows_;
    bool skip_user_bitmap_{false};
    std::unique_ptr<FilterDiagnostics> profile_;
    int64_t profile_segment_id_{0};
    uint64_t profile_timestamp_{0};
    bool profile_cache_hit_{false};
    // Expression filter cache for two-stage search.
    // Cache backend is the process-level ExprResCacheManager.
    bool enable_expr_cache_ = false;
    std::string expr_cache_key_;
};
}  // namespace exec
}  // namespace milvus
