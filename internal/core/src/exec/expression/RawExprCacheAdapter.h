// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "exec/expression/Expr.h"

namespace milvus {
namespace exec {

// Composition wrapper for passively caching batched RawData expression
// results. It is attached only while ExprResCacheManager is enabled, keeping
// the disabled physical expression graph and execution path unchanged.
class RawExprCacheAdapter final : public Expr {
 public:
    RawExprCacheAdapter(std::shared_ptr<SegmentExpr> input,
                        milvus::OpContext* op_ctx,
                        bool enable_cache_write);

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override;

    bool
    SupportOffsetInput() override;

    std::string
    ToString() const override;

    bool
    CanExecuteAllAtOnce() const override;

    void
    SetExecuteAllAtOnce() override;

    bool
    IsSource() const override;

    void
    MarkNullRejecting() override;

    bool
    IsCacheable() const override;

    bool
    CanUseNestedIndex() const override;

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override;

    void
    PrefetchAsync(const std::shared_ptr<folly::CPUThreadPoolExecutor>
                      prefetch_pool) override;

    void
    WaitPrefetch() override;

    const std::shared_ptr<SegmentExpr>&
    wrapped_expr() const {
        return input_;
    }

 private:
    enum class LookupState { Unchecked, Ineligible, Miss, Hit };

    struct State {
        LookupState lookup_state{LookupState::Unchecked};
        std::optional<ExprResCacheManager::Key> key;

        std::shared_ptr<TargetBitmap> result;
        std::shared_ptr<TargetBitmap> valid_result;
        int64_t sequential_pos{0};

        bool admission_checked{false};
        std::optional<ExprResCacheManager::AdmissionTicket> admission_ticket;

        bool capturing{false};
        bool full_coverage_possible{true};
        bool put_attempted{false};
        TargetBitmap captured_result;
        TargetBitmap captured_valid_result;
        int64_t eval_duration_us{0};
    };

    void
    EnsureLookup();

    bool
    TryServeCacheHit(EvalCtx& context, VectorPtr& result);

    void
    EnsureAdmission(EvalCtx& context);

    void
    AbortCapture();

    void
    CaptureResult(EvalCtx& context,
                  int64_t start_pos,
                  bool input_was_unmasked,
                  size_t bitmap_input_size,
                  int64_t elapsed_us,
                  const VectorPtr& result);

    void
    TryPutCapturedResult();

 private:
    std::shared_ptr<SegmentExpr> input_;
    const bool enable_cache_write_;
    State state_;
    std::once_flag lookup_once_;
    std::optional<folly::Future<folly::Unit>> prefetch_future_;
};

// Called once after the existing expression optimization pass while the
// process cache is enabled. Read lookup remains available when cache writes
// are disabled for this request.
void
DecorateRawExprCache(std::vector<ExprPtr>& exprs,
                     milvus::OpContext* op_ctx,
                     bool enable_cache_write);

}  // namespace exec
}  // namespace milvus
