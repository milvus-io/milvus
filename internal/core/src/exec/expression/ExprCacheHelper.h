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

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include "common/Types.h"
#include "exec/expression/ExprCache.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

// Wrapper around ExprResCacheManager to simplify the get→compute→put pattern
// for expression implementations. Each expression only needs to:
//   1. Provide a stable ToString() signature
//   2. Wrap its compute logic in a lambda
//
// The helper handles: cache lookup, miss path timing, put, and the
// sealed-segment / enabled checks.
//
// Example usage in an expression:
//
//   auto cached = exec::ExprCacheHelper::GetOrCompute(
//       segment_, this->ToString(), active_count_,
//       [&]() -> exec::ExprCacheHelper::ComputeResult {
//           TargetBitmap res = do_actual_computation();
//           TargetBitmap valid = compute_valid();
//           return {std::move(res), std::move(valid)};
//       });
//   result_bitmap = cached.result;
//   valid_bitmap = cached.valid;

// Forward decl
class BatchedCachedMixin;

class ExprCacheHelper {
 public:
    struct CachedBitmaps {
        std::shared_ptr<TargetBitmap> result;
        std::shared_ptr<TargetBitmap> valid;
    };

    // Return type of the compute lambda: (result_bitmap, valid_bitmap).
    // Valid bitmap may be all-ones for expressions that don't produce
    // nullability (e.g. unary comparisons on non-nullable fields).
    struct ComputeResult {
        TargetBitmap result;
        TargetBitmap valid;
    };

    // Try cache; on miss, call `compute`, put result into cache, return.
    // Backend semantics:
    //   - Memory mode supports sealed and growing segments. `active_count`
    //     participates in the memory cache key, so growing-segment snapshots
    //     with different row counts are isolated from each other.
    //   - Disk mode is sealed-segment only. DiskSlotFile uses fixed-size slots
    //     derived from row_count; if a segment's row_count changes, the manager
    //     drops that disk file and skips disk caching for the segment.
    //
    // Skips the cache entirely if ExprResCacheManager is disabled. Disk mode
    // also skips growing segments because DiskSlotFile has fixed row_count slots.
    //
    // Correctness requirements for the caller:
    //   - `expr_signature` MUST uniquely identify the expression and its
    //     parameters. Same parameters → same signature. Field order in the
    //     string must be fixed (don't rely on protobuf DebugString).
    //   - `active_count` MUST be the current segment row count. Used to
    //     detect staleness after insert/compaction.
    //   - `compute` MUST be deterministic: same segment + same signature
    //     + same active_count must always produce the same bitmaps.
    template <typename ComputeFn>
    static CachedBitmaps
    GetOrCompute(const segcore::SegmentInternalInterface* segment,
                 const std::string& expr_signature,
                 int64_t active_count,
                 ComputeFn&& compute) {
        bool cache_eligible =
            segment != nullptr && ExprResCacheManager::IsEnabled();
        if (cache_eligible &&
            ExprResCacheManager::Instance().GetMode() == CacheMode::Disk &&
            segment->type() != SegmentType::Sealed) {
            cache_eligible = false;
        }

        if (cache_eligible) {
            // Try Get
            ExprResCacheManager::Key key{segment->get_segment_id(),
                                         expr_signature};
            ExprResCacheManager::Value got;
            got.active_count = active_count;
            if (ExprResCacheManager::Instance().Get(key, got)) {
                return {got.result, got.valid_result};
            }
        }

        // Miss (or cache ineligible): run compute with timing
        auto t0 = std::chrono::steady_clock::now();
        ComputeResult out = compute();
        auto eval_us = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - t0)
                           .count();

        auto result = std::make_shared<TargetBitmap>(std::move(out.result));
        auto valid = std::make_shared<TargetBitmap>(std::move(out.valid));

        if (cache_eligible) {
            ExprResCacheManager::Key key{segment->get_segment_id(),
                                         expr_signature};
            ExprResCacheManager::Value v;
            v.result = result;
            v.valid_result = valid;
            v.active_count = active_count;
            v.eval_duration_us = eval_us;
            ExprResCacheManager::Instance().Put(key, v);
        }

        return {result, valid};
    }
};

// ============================================================
//  BatchedCachedMixin — eliminate boilerplate for cached expressions
// ============================================================
//
// Many filter expressions use the Volcano execution model: each Eval()
// returns one batch_size slice. When caching, we want:
//   1. First Eval: compute the FULL segment bitset (or load from cache),
//      store it in a member, then slice the current batch.
//   2. Later Evals: just slice from the stored full bitset.
//
// This mixin encapsulates that pattern. A derived expression inherits
// it and implements `ComputeFullBitset()`. Call sites only need:
//
//   auto [result, valid, advance] = EnsureFullAndSlice(batch_size);
//
// The mixin handles:
//   - Cache lookup on first access
//   - Compute + timing + cache put on miss
//   - Slicing the current batch from the full bitset
//   - Cursor management (via `batch_offset_`)
//
// Expressions holding this mixin must also provide:
//   - A valid `segment_` pointer
//   - `active_count_` (current row count)
//   - `ToString()` returning a stable signature
//   - `ComputeFullBitset()` implementation in the derived class
//
// Example:
//
//   class PhyJsonContainsExpr : public SegmentExpr,
//                                public BatchedCachedMixin {
//       VectorPtr Eval() override {
//           auto [result, valid, has_more] =
//               this->NextBatchViaCache(GetNextBatchSize(), this);
//           if (!has_more) return nullptr;
//           return std::make_shared<ColumnVector>(std::move(result),
//                                                   std::move(valid));
//       }
//
//       ExprCacheHelper::ComputeResult ComputeFullBitset() {
//           TargetBitmap r(active_count_);
//           TargetBitmap v(active_count_, true);
//           for (int64_t i = 0; i < active_count_; ++i) {
//               r[i] = DoJsonContains(i);
//           }
//           return {std::move(r), std::move(v)};
//       }
//   };

class BatchedCachedMixin {
 public:
    struct BatchSlice {
        TargetBitmap result;   // size = actual_batch_size
        TargetBitmap valid;    // size = actual_batch_size
        bool has_data{false};  // false when cursor is past the end
    };

    // Reset cache state between queries (call from the expression's reset/init)
    void
    ResetCache() {
        full_result_.reset();
        full_valid_.reset();
        batch_offset_ = 0;
    }

    // Derived expression calls this from Eval() to get the next batch slice.
    // On first call, runs cache lookup / compute via ExprCacheHelper.
    // On subsequent calls, slices from the stored full bitset.
    //
    // `derived` should be `this` (the derived expression), used to access
    // segment/signature/active_count/ComputeFullBitset via CRTP-like dispatch.
    template <typename Derived>
    BatchSlice
    NextBatchViaCache(int64_t batch_size, Derived* derived) {
        EnsureFullLoaded(derived);

        BatchSlice out;
        const int64_t total = static_cast<int64_t>(full_result_->size());
        if (batch_offset_ >= total || batch_size <= 0) {
            return out;  // has_data=false → end of stream
        }
        const int64_t actual =
            std::min<int64_t>(batch_size, total - batch_offset_);
        out.result = TargetBitmap(actual);
        out.valid = TargetBitmap(actual);
        out.result.append(*full_result_, batch_offset_, actual);
        out.valid.append(*full_valid_, batch_offset_, actual);
        out.has_data = true;
        batch_offset_ += actual;
        return out;
    }

    // Take exclusive ownership of the full bitsets (use_count == 1 after this
    // call, enabling std::move of the underlying TargetBitmap by the caller).
    // After this call the mixin's internal state is cleared; subsequent
    // NextBatchViaCache / TakeFullBitset calls will reload from cache/recompute.
    //
    // Use this in `execute_all_at_once_` optimization paths where you want to
    // avoid the batch-slice append copy:
    //
    //   if (execute_all_at_once_) {
    //       auto full = TakeFullBitset(this);
    //       MoveCursor();
    //       return std::make_shared<ColumnVector>(
    //           std::move(*full.result), std::move(*full.valid));
    //   }
    //   // else: normal batch loop
    //   auto slice = NextBatchViaCache(batch_size, this);
    //   ...
    template <typename Derived>
    ExprCacheHelper::CachedBitmaps
    TakeFullBitset(Derived* derived) {
        EnsureFullLoaded(derived);
        ExprCacheHelper::CachedBitmaps out{std::move(full_result_),
                                           std::move(full_valid_)};
        // Reset so a subsequent call reloads (mixin no longer owns the data).
        // Note: the underlying cache entry is still on disk/memory, so reload
        // is cheap (single Get).
        batch_offset_ = 0;
        return out;
    }

    // True if the full bitset has been loaded into the mixin already.
    bool
    IsFullLoaded() const {
        return full_result_ != nullptr;
    }

 private:
    template <typename Derived>
    void
    EnsureFullLoaded(Derived* derived) {
        if (full_result_ != nullptr) {
            return;
        }
        auto cached = ExprCacheHelper::GetOrCompute(
            derived->cache_segment(),
            derived->cache_signature(),
            derived->cache_active_count(),
            [derived]() { return derived->ComputeFullBitset(); });
        full_result_ = cached.result;
        full_valid_ = cached.valid;
    }

 public:
    // Advance cursor without computing (used for cache-skip optimizations)
    void
    AdvanceBatchCursor(int64_t step) {
        batch_offset_ += step;
    }

 protected:
    std::shared_ptr<TargetBitmap> full_result_;
    std::shared_ptr<TargetBitmap> full_valid_;
    int64_t batch_offset_{0};
};

}  // namespace exec
}  // namespace milvus
