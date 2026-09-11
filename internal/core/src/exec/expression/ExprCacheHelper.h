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
    //   - Memory mode always supports sealed segments. Growing segments are
    //     supported only when explicitly enabled.
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
                 ComputeFn&& compute,
                 bool enable_cache_write = true) {
        auto& manager = ExprResCacheManager::Instance();
        bool cache_eligible = segment != nullptr &&
                              ExprResCacheManager::IsEnabled() &&
                              manager.CanCacheSegment(segment->type());

        if (cache_eligible) {
            // Try Get
            ExprResCacheManager::Key key{segment->get_segment_id(),
                                         expr_signature};
            ExprResCacheManager::Value got;
            got.active_count = active_count;
            if (manager.Get(key, got)) {
                return {got.result, got.valid_result};
            }
        }

        const bool cache_can_write = cache_eligible && enable_cache_write;
        if (!cache_can_write) {
            ComputeResult out = compute();
            auto result = std::make_shared<TargetBitmap>(std::move(out.result));
            auto valid = std::make_shared<TargetBitmap>(std::move(out.valid));
            return {result, valid};
        }

        // Miss: run compute with timing for latency admission.
        auto t0 = std::chrono::steady_clock::now();
        ComputeResult out = compute();
        auto eval_us = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - t0)
                           .count();

        auto result = std::make_shared<TargetBitmap>(std::move(out.result));
        auto valid = std::make_shared<TargetBitmap>(std::move(out.valid));

        ExprResCacheManager::Key key{segment->get_segment_id(), expr_signature};
        ExprResCacheManager::Value v;
        v.result = result;
        v.valid_result = valid;
        v.active_count = active_count;
        v.eval_duration_us = eval_us;
        manager.Put(key, v);

        return {result, valid};
    }

    // Cache one immutable bitmap artifact using the same backend, admission,
    // staleness, and segment-invalidation rules as full expression results.
    // The all-ones companion satisfies the existing two-bitmap cache value
    // contract and is compressed efficiently by the memory backend.
    template <typename ComputeFn>
    static std::shared_ptr<TargetBitmap>
    GetOrComputeBitmap(const segcore::SegmentInternalInterface* segment,
                       const std::string& artifact_signature,
                       int64_t active_count,
                       ComputeFn&& compute,
                       bool enable_cache_write = true) {
        auto cached = GetOrCompute(
            segment,
            artifact_signature,
            active_count,
            [&]() -> ComputeResult {
                auto result = compute();
                TargetBitmap valid(result.size(), true);
                return {std::move(result), std::move(valid)};
            },
            enable_cache_write);
        return cached.result;
    }
};

}  // namespace exec
}  // namespace milvus
