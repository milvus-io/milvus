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

#include "exec/expression/RawExprCacheAdapter.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus {
namespace exec {

namespace {

ExprPtr
DecorateOne(const ExprPtr& expr,
            milvus::OpContext* op_ctx,
            bool enable_cache_write) {
    if (expr == nullptr) {
        return expr;
    }

    // Decoration happens after the normal optimizer, so replacing children no
    // longer interferes with its concrete-expression dynamic casts.
    for (auto& input : expr->GetInputsRef()) {
        input = DecorateOne(input, op_ctx, enable_cache_write);
    }

    auto segment_expr = std::dynamic_pointer_cast<SegmentExpr>(expr);
    if (segment_expr == nullptr || !segment_expr->SupportsRawExprCache() ||
        !segment_expr->IsCacheable()) {
        return expr;
    }
    const auto* segment = segment_expr->GetSegmentForRawExprCache();
    if (segment == nullptr ||
        !ExprResCacheManager::Instance().CanCacheSegment(segment->type())) {
        return expr;
    }
    return std::make_shared<RawExprCacheAdapter>(
        std::move(segment_expr), op_ctx, enable_cache_write);
}

}  // namespace

RawExprCacheAdapter::RawExprCacheAdapter(std::shared_ptr<SegmentExpr> input,
                                         milvus::OpContext* op_ctx,
                                         bool enable_cache_write)
    : Expr(DataType::BOOL, {}, "RawExprCacheAdapter", op_ctx),
      input_(std::move(input)),
      enable_cache_write_(enable_cache_write) {
    AssertInfo(input_ != nullptr,
               "RawExprCacheAdapter requires a SegmentExpr input");
}

void
RawExprCacheAdapter::EnsureLookup() {
    std::call_once(lookup_once_, [this]() {
        state_.lookup_state = LookupState::Ineligible;
        try {
            if (!ExprResCacheManager::IsEnabled() ||
                !input_->SupportsRawExprCache() || !input_->IsCacheable() ||
                input_->GetActiveCountForRawExprCache() <= 0) {
                return;
            }

            const auto* segment = input_->GetSegmentForRawExprCache();
            auto& manager = ExprResCacheManager::Instance();
            if (segment == nullptr ||
                !manager.CanCacheSegment(segment->type()) ||
                input_->GetExecPathForRawExprCache() != ExprExecPath::RawData) {
                return;
            }

            state_.key.emplace(ExprResCacheManager::Key{
                segment->get_segment_id(), input_->ToString()});
            ExprResCacheManager::Value cached;
            cached.active_count = input_->GetActiveCountForRawExprCache();
            if (manager.Get(*state_.key, cached) && cached.result != nullptr &&
                cached.valid_result != nullptr &&
                cached.result->size() ==
                    static_cast<size_t>(cached.active_count) &&
                cached.valid_result->size() ==
                    static_cast<size_t>(cached.active_count)) {
                state_.result = std::move(cached.result);
                state_.valid_result = std::move(cached.valid_result);
                state_.lookup_state = LookupState::Hit;
                return;
            }
            state_.lookup_state = LookupState::Miss;
        } catch (...) {
            // Expression caching is best-effort. The wrapped evaluator remains
            // the source of truth and will surface any real evaluation error.
            state_.key.reset();
            state_.result.reset();
            state_.valid_result.reset();
            state_.lookup_state = LookupState::Ineligible;
        }
    });
}

bool
RawExprCacheAdapter::TryServeCacheHit(EvalCtx& context, VectorPtr& result) {
    if (state_.lookup_state != LookupState::Hit) {
        return false;
    }

    const auto abandon_hit = [this]() {
        state_.result.reset();
        state_.valid_result.reset();
        state_.lookup_state = LookupState::Ineligible;
    };

    auto* offsets = context.get_offset_input();
    if (offsets != nullptr) {
        if (offsets->empty()) {
            result = nullptr;
            return true;
        }
        for (const auto offset : *offsets) {
            AssertInfo(offset >= 0 &&
                           static_cast<size_t>(offset) < state_.result->size(),
                       "offset {} is outside cached result size {}",
                       offset,
                       state_.result->size());
        }
        try {
            TargetBitmap gathered(offsets->size(), false);
            TargetBitmap gathered_valid(offsets->size(), false);
            for (size_t i = 0; i < offsets->size(); ++i) {
                const auto offset = (*offsets)[i];
                gathered[i] = (*state_.result)[offset];
                gathered_valid[i] = (*state_.valid_result)[offset];
            }
            result = std::make_shared<ColumnVector>(std::move(gathered),
                                                    std::move(gathered_valid));
            return true;
        } catch (...) {
            abandon_hit();
            return false;
        }
    }

    const auto active_count = input_->GetActiveCountForRawExprCache();
    const auto actual_rows =
        std::min(input_->GetNextBatchSizeForRawExprCache(),
                 std::max<int64_t>(active_count - state_.sequential_pos, 0));
    if (actual_rows == 0) {
        result = nullptr;
        return true;
    }

    ColumnVectorPtr sliced_result;
    try {
        TargetBitmap sliced;
        TargetBitmap sliced_valid;
        sliced.append(*state_.result, state_.sequential_pos, actual_rows);
        sliced_valid.append(
            *state_.valid_result, state_.sequential_pos, actual_rows);
        sliced_result = std::make_shared<ColumnVector>(std::move(sliced),
                                                       std::move(sliced_valid));
    } catch (...) {
        abandon_hit();
        return false;
    }

    state_.sequential_pos += actual_rows;
    input_->MoveCursor();
    result = std::move(sliced_result);
    return true;
}

void
RawExprCacheAdapter::EnsureAdmission(EvalCtx& context) {
    if (state_.admission_checked || state_.lookup_state != LookupState::Miss ||
        !state_.key.has_value()) {
        return;
    }

    state_.admission_checked = true;
    if (!enable_cache_write_) {
        return;
    }
    try {
        const auto* segment = input_->GetSegmentForRawExprCache();
        auto& manager = ExprResCacheManager::Instance();
        if (segment == nullptr || !manager.CanCacheSegment(segment->type())) {
            AbortCapture();
            return;
        }

        state_.admission_ticket = manager.ObserveMiss(*state_.key);
        if (!state_.admission_ticket->admitted) {
            return;
        }

        const auto& bitmap_input = context.get_bitmap_input();
        const bool unmasked = bitmap_input.empty() || bitmap_input.all();
        if (!state_.full_coverage_possible || state_.sequential_pos != 0 ||
            context.get_offset_input() != nullptr || !unmasked) {
            AbortCapture();
            return;
        }

        const auto active_count = input_->GetActiveCountForRawExprCache();
        state_.captured_result.reserve(active_count);
        state_.captured_valid_result.reserve(active_count);
        state_.capturing = true;
    } catch (...) {
        AbortCapture();
    }
}

void
RawExprCacheAdapter::AbortCapture() {
    state_.capturing = false;
    state_.full_coverage_possible = false;
    state_.captured_result = TargetBitmap{};
    state_.captured_valid_result = TargetBitmap{};
}

void
RawExprCacheAdapter::CaptureResult(EvalCtx& context,
                                   int64_t start_pos,
                                   bool input_was_unmasked,
                                   size_t bitmap_input_size,
                                   int64_t elapsed_us,
                                   const VectorPtr& result) {
    const bool has_offset_input = context.get_offset_input() != nullptr;
    if (result == nullptr) {
        if (!has_offset_input &&
            state_.sequential_pos < input_->GetActiveCountForRawExprCache()) {
            AbortCapture();
        }
        return;
    }

    auto column = std::dynamic_pointer_cast<ColumnVector>(result);
    if (column == nullptr || !column->IsBitmap()) {
        AbortCapture();
        return;
    }

    const auto result_size = column->size();
    if (!has_offset_input) {
        state_.sequential_pos += result_size;
    }

    if (!state_.capturing) {
        return;
    }

    const auto active_count = input_->GetActiveCountForRawExprCache();
    if (has_offset_input || !input_was_unmasked ||
        (bitmap_input_size != 0 &&
         bitmap_input_size != static_cast<size_t>(result_size)) ||
        start_pos != static_cast<int64_t>(state_.captured_result.size()) ||
        start_pos + result_size > active_count) {
        AbortCapture();
        return;
    }

    try {
        TargetBitmapView result_view(column->GetRawData(), result_size);
        TargetBitmapView valid_view(column->GetValidRawData(), result_size);
        state_.captured_result.append(result_view);
        state_.captured_valid_result.append(valid_view);
        state_.eval_duration_us += std::max<int64_t>(elapsed_us, 1);
        TryPutCapturedResult();
    } catch (...) {
        // Capturing is an optional side effect. Allocation or bitmap-copy
        // failures must not replace the result already produced by input_.
        AbortCapture();
    }
}

void
RawExprCacheAdapter::TryPutCapturedResult() {
    if (!enable_cache_write_ || !state_.capturing || state_.put_attempted ||
        !state_.admission_ticket.has_value() || !state_.key.has_value()) {
        return;
    }

    const auto active_count = input_->GetActiveCountForRawExprCache();
    if (state_.captured_result.size() != static_cast<size_t>(active_count) ||
        state_.captured_valid_result.size() !=
            static_cast<size_t>(active_count)) {
        return;
    }

    state_.put_attempted = true;
    state_.capturing = false;
    state_.full_coverage_possible = false;

    try {
        ExprResCacheManager::Value value;
        value.result =
            std::make_shared<TargetBitmap>(std::move(state_.captured_result));
        value.valid_result = std::make_shared<TargetBitmap>(
            std::move(state_.captured_valid_result));
        value.active_count = active_count;
        value.eval_duration_us = state_.eval_duration_us;
        ExprResCacheManager::Instance().PutAdmitted(
            *state_.key, value, *state_.admission_ticket);
    } catch (...) {
        // Cache writes must not affect the query result already produced by the
        // wrapped expression.
    }
}

void
RawExprCacheAdapter::Eval(EvalCtx& context, VectorPtr& result) {
    const bool has_offset_input = context.get_offset_input() != nullptr;
    SetHasOffsetInput(has_offset_input);
    // A cache hit bypasses input_->Eval(), so mirror the offset mode that the
    // wrapped evaluator would otherwise record for its MoveCursor() guard.
    input_->SetHasOffsetInput(has_offset_input);

    WaitPrefetch();
    EnsureLookup();
    if (TryServeCacheHit(context, result)) {
        return;
    }

    if (state_.lookup_state != LookupState::Miss) {
        input_->Eval(context, result);
        return;
    }

    EnsureAdmission(context);
    const auto start_pos = state_.sequential_pos;
    bool input_was_unmasked = true;
    size_t bitmap_input_size = 0;
    if (state_.capturing) {
        const auto& bitmap_input = context.get_bitmap_input();
        input_was_unmasked = bitmap_input.empty() || bitmap_input.all();
        bitmap_input_size = bitmap_input.size();
        if (context.get_offset_input() != nullptr || !input_was_unmasked ||
            start_pos != static_cast<int64_t>(state_.captured_result.size())) {
            AbortCapture();
        }
    }

    const bool measure = state_.capturing;
    std::chrono::steady_clock::time_point eval_start;
    if (measure) {
        eval_start = std::chrono::steady_clock::now();
    }
    try {
        input_->Eval(context, result);
    } catch (...) {
        AbortCapture();
        throw;
    }
    const auto elapsed_us =
        measure ? std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - eval_start)
                      .count()
                : 0;
    if (measure) {
        CaptureResult(context,
                      start_pos,
                      input_was_unmasked,
                      bitmap_input_size,
                      elapsed_us,
                      result);
    }
}

void
RawExprCacheAdapter::MoveCursor() {
    if (!has_offset_input_) {
        const auto rows = input_->GetNextBatchSizeForRawExprCache();
        if (rows > 0) {
            const auto active_count = input_->GetActiveCountForRawExprCache();
            state_.sequential_pos =
                std::min(active_count, state_.sequential_pos + rows);
            AbortCapture();
        }
    }
    input_->MoveCursor();
}

bool
RawExprCacheAdapter::SupportOffsetInput() {
    return input_->SupportOffsetInput();
}

std::string
RawExprCacheAdapter::ToString() const {
    return input_->ToString();
}

bool
RawExprCacheAdapter::CanExecuteAllAtOnce() const {
    return input_->CanExecuteAllAtOnce();
}

void
RawExprCacheAdapter::SetExecuteAllAtOnce() {
    input_->SetExecuteAllAtOnce();
}

bool
RawExprCacheAdapter::IsSource() const {
    return input_->IsSource();
}

void
RawExprCacheAdapter::MarkNullRejecting() {
    input_->MarkNullRejecting();
}

bool
RawExprCacheAdapter::IsCacheable() const {
    return input_->IsCacheable();
}

bool
RawExprCacheAdapter::CanUseNestedIndex() const {
    return input_->CanUseNestedIndex();
}

std::optional<milvus::expr::ColumnInfo>
RawExprCacheAdapter::GetColumnInfo() const {
    return input_->GetColumnInfo();
}

void
RawExprCacheAdapter::PrefetchAsync(
    const std::shared_ptr<folly::CPUThreadPoolExecutor> prefetch_pool) {
    auto self =
        std::static_pointer_cast<RawExprCacheAdapter>(shared_from_this());
    prefetch_future_.emplace(folly::via(prefetch_pool.get(), [self]() {
        if (self->op_ctx_ != nullptr &&
            self->op_ctx_->cancellation_token.isCancellationRequested()) {
            return;
        }
        self->EnsureLookup();
        if (self->state_.lookup_state != LookupState::Hit) {
            self->input_->PrefetchOnCurrentThread();
        }
    }));
}

void
RawExprCacheAdapter::WaitPrefetch() {
    if (prefetch_future_.has_value()) {
        auto future = std::move(*prefetch_future_);
        prefetch_future_.reset();
        std::move(future).get();
    } else {
        EnsureLookup();
    }

    if (state_.lookup_state != LookupState::Hit) {
        input_->WaitPrefetch();
    }
}

void
DecorateRawExprCache(std::vector<ExprPtr>& exprs,
                     milvus::OpContext* op_ctx,
                     bool enable_cache_write) {
    for (auto& expr : exprs) {
        expr = DecorateOne(expr, op_ctx, enable_cache_write);
    }
}

}  // namespace exec
}  // namespace milvus
