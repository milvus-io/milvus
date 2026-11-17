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
#include <folly/futures/Future.h>

#include "common/FeatureBits.h"
#include "common/Types.h"
#include "exec/expression/ExprCache.h"
#include "exec/expression/ExprCacheFlight.h"
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
//       segment_, snapshot_, [this]() { return this->ToString(); }, active_count_,
//       [&]() -> exec::ExprCacheHelper::ComputeResult {
//           TargetBitmap res = do_actual_computation();
//           TargetBitmap valid = compute_valid();
//           return {std::move(res), std::move(valid)};
//       });
//   result_bitmap = cached.result;
//   valid_bitmap = cached.valid;

class ExprCacheHelper {
 public:
    static bool
    HasCacheIdentity(const segcore::SegmentInternalInterface* segment,
                     const segcore::SegmentReadSnapshot* snapshot) {
        return segment != nullptr &&
               (segment->type() != SegmentType::Sealed ||
                (snapshot != nullptr && snapshot->GetExprCacheGeneration()));
    }

    static ExprResCacheManager::Key
    MakeKey(const segcore::SegmentInternalInterface* segment,
            const segcore::SegmentReadSnapshot* snapshot,
            std::string signature) {
        return {segment->get_segment_id(),
                std::move(signature),
                snapshot ? snapshot->GetExprCacheGeneration() : nullptr};
    }

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
    //   - `make_signature` MUST return a string that uniquely identifies the
    //     expression and its parameters. Same parameters → same signature.
    //     Field order in the string must be fixed (don't rely on protobuf
    //     DebugString).
    //   - `active_count` MUST be the current segment row count. Used to
    //     detect staleness after insert/compaction.
    //   - `snapshot` MUST be the snapshot used by `compute`; sealed calls without
    //     a snapshot bypass the cache. Never fetch the latest generation on Put.
    //   - `compute` MUST be deterministic: same segment generation + same signature
    //     + same active_count must always produce the same bitmaps.
    template <typename SignatureFn, typename ComputeFn>
    static CachedBitmaps
    GetOrCompute(const segcore::SegmentInternalInterface* segment,
                 const segcore::SegmentReadSnapshot* snapshot,
                 SignatureFn&& make_signature,
                 int64_t active_count,
                 ComputeFn&& compute,
                 bool enable_cache_write = true,
                 FeatureRecorder* recorder = nullptr,
                 const ExprCacheFlightOptions* flight_options = nullptr) {
        ExprResCacheManager* manager = nullptr;
        bool cache_eligible = false;
        std::string expr_signature;
        if (ExprResCacheManager::IsEnabled() &&
            HasCacheIdentity(segment, snapshot)) {
            RunExprCacheBestEffort([&]() {
                manager = &ExprResCacheManager::Instance();
                if (!manager->CanCacheSegment(segment->type())) {
                    return;
                }
                expr_signature = std::forward<SignatureFn>(make_signature)();
                cache_eligible = true;
            });
        }

        bool genuine_miss = false;
        if (cache_eligible) {
            ExprResCacheManager::Value got;
            bool cache_hit = false;
            RunExprCacheBestEffort([&]() {
                auto key = MakeKey(segment, snapshot, expr_signature);
                got.active_count = active_count;
                const auto status = manager->GetWithStatus(key, got);
                cache_hit = status == ExprResCacheManager::LookupResult::Hit;
                genuine_miss =
                    status == ExprResCacheManager::LookupResult::Miss;
            });
            if (cache_hit) {
                MarkFeature(recorder, FeatureBit::ExprCacheHit);
                return {got.result, got.valid_result};
            }
        }

        // Explicit opt-in: other helpers (including internal validity artifacts)
        // and all disabled/hit paths keep their original execution behavior.
        if (flight_options && genuine_miss && manager->Flights().IsEnabled() &&
            !ExprCacheFlightCoordinator::IsComputing() &&
            segment->type() == SegmentType::Sealed) {
            std::optional<ExprResCacheManager::Key> key;
            RunExprCacheBestEffort(
                [&] { key = MakeKey(segment, snapshot, expr_signature); });
            if (key) {
                return GetOrComputeFlight(*manager,
                                          *key,
                                          active_count,
                                          compute,
                                          enable_cache_write,
                                          recorder,
                                          *flight_options);
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

        RunExprCacheBestEffort([&]() {
            auto key = MakeKey(segment, snapshot, expr_signature);
            ExprResCacheManager::Value v;
            v.result = result;
            v.valid_result = valid;
            v.active_count = active_count;
            v.eval_duration_us = eval_us;
            manager->Put(key, v);
        });

        return {result, valid};
    }

    // Cache one immutable bitmap artifact using the same backend, admission,
    // staleness, and segment-invalidation rules as full expression results.
    // The all-ones companion satisfies the existing two-bitmap cache value
    // contract and is compressed efficiently by the memory backend.
    template <typename SignatureFn, typename ComputeFn>
    static std::shared_ptr<TargetBitmap>
    GetOrComputeBitmap(const segcore::SegmentInternalInterface* segment,
                       const segcore::SegmentReadSnapshot* snapshot,
                       SignatureFn&& make_signature,
                       int64_t active_count,
                       ComputeFn&& compute,
                       bool enable_cache_write = true) {
        auto cached = GetOrCompute(
            segment,
            snapshot,
            std::forward<SignatureFn>(make_signature),
            active_count,
            [&]() -> ComputeResult {
                auto result = compute();
                TargetBitmap valid(result.size(), true);
                return {std::move(result), std::move(valid)};
            },
            enable_cache_write);
        return cached.result;
    }

 private:
    static void
    CheckFlightCancellation(const ExprCacheFlightOptions& options) {
        if (options.cancel && options.cancel->isCancellationRequested()) {
            throw folly::FutureCancellation();
        }
    }

    static CachedBitmaps
    OwnResult(ComputeResult out) {
        return {std::make_shared<TargetBitmap>(std::move(out.result)),
                std::make_shared<TargetBitmap>(std::move(out.valid))};
    }

    template <typename ComputeFn>
    static CachedBitmaps
    GetOrComputeFlight(ExprResCacheManager& manager,
                       const ExprResCacheManager::Key& key,
                       int64_t active_count,
                       ComputeFn& compute,
                       bool enable_cache_write,
                       FeatureRecorder* recorder,
                       const ExprCacheFlightOptions& options) {
        using Coordinator = ExprCacheFlightCoordinator;
        auto& coordinator = manager.Flights();
        CheckFlightCancellation(options);
        ExprResCacheManager::AdmissionTicket ticket;
        if (enable_cache_write) {
            RunExprCacheBestEffort([&] {
                ticket =
                    manager.ObserveMiss(key, active_count, SegmentType::Sealed);
            });
        }
        const auto epoch = ticket.config_epoch != 0 ? ticket.config_epoch
                                                    : manager.ConfigEpoch();
        const auto current = [&] {
            return ExprResCacheManager::IsEnabled() &&
                   manager.ConfigEpoch() == epoch && key.generation &&
                   !key.generation->IsRetired();
        };
        Coordinator::Handle handle;
        RunExprCacheBestEffort([&] {
            if (current())
                handle = coordinator.TryJoin({key, active_count, epoch});
        });
        // A handle immediately owns cleanup, including every exceptional exit.
        if (handle)
            coordinator.OfferAdmission(handle, ticket);

        if (handle && !handle.IsLeader()) {
            Coordinator::WaitResult waited;
            RunExprCacheBestEffort([&] {
                waited = coordinator.Wait(handle, options.cancel, current);
            });
            CheckFlightCancellation(options);
            std::shared_ptr<Coordinator::Bitmaps> copy;
            if (waited.bitmaps && current()) {
                RunExprCacheBestEffort([&] {
                    auto lease =
                        manager.TryAcquireMaterialization(active_count);
                    if (lease)
                        copy = std::make_shared<Coordinator::Bitmaps>(
                            std::move(*lease),
                            waited.bitmaps->result,
                            waited.bitmaps->valid);
                });
            }
            waited.bitmaps.reset();
            handle.Reset();
            CheckFlightCancellation(options);
            if (copy && current()) {
                return {std::shared_ptr<TargetBitmap>(copy, &copy->result),
                        std::shared_ptr<TargetBitmap>(copy, &copy->valid)};
            }
            copy.reset();
            // No recursive joining or duplicate Put on a follower fallback.
            return OwnResult(compute());
        }

        // Also suppress nested flights for bypass computation in this path.
        Coordinator::ComputeScope compute_scope;
        bool can_share = static_cast<bool>(handle);
        if (handle) {
            ExprResCacheManager::Value got;
            got.active_count = active_count;
            auto status = ExprResCacheManager::LookupResult::ResourceLimit;
            RunExprCacheBestEffort(
                [&] { status = manager.GetWithStatus(key, got); });
            CheckFlightCancellation(options);
            if (status == ExprResCacheManager::LookupResult::Hit) {
                RunExprCacheBestEffort([&] {
                    auto lease =
                        manager.TryAcquireMaterialization(active_count);
                    if (lease && current()) {
                        coordinator.Complete(
                            handle,
                            std::make_shared<Coordinator::Bitmaps>(
                                std::move(*lease),
                                *got.result,
                                *got.valid_result));
                    }
                });
                MarkFeature(recorder, FeatureBit::ExprCacheHit);
                return {got.result, got.valid_result};
            }
            if (status != ExprResCacheManager::LookupResult::Miss) {
                coordinator.Abort(handle);
                can_share = false;
            }
        }

        std::optional<ExprResCacheManager::MaterializationLease> shared_lease;
        if (can_share) {
            RunExprCacheBestEffort([&] {
                shared_lease = manager.TryAcquireMaterialization(active_count);
            });
            if (!shared_lease)
                coordinator.Abort(handle);
        }
        CheckFlightCancellation(options);
        const auto start = std::chrono::steady_clock::now();
        // Deliberately outside best-effort handling: preserve compute errors.
        auto out = compute();
        const auto eval_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start)
                .count();
        CheckFlightCancellation(options);
        auto owned = OwnResult(std::move(out));
        if (handle) {
            RunExprCacheBestEffort([&] {
                if (shared_lease && current() &&
                    owned.result->size() == static_cast<size_t>(active_count) &&
                    owned.valid->size() == static_cast<size_t>(active_count)) {
                    coordinator.Complete(handle,
                                         std::make_shared<Coordinator::Bitmaps>(
                                             std::move(*shared_lease),
                                             *owned.result,
                                             *owned.valid));
                }
            });
            // If sharing failed, wake followers before any potentially slow Put.
            coordinator.Abort(handle);
            ticket = coordinator.TakeAdmission(handle);
        }
        shared_lease.reset();
        if (enable_cache_write && current()) {
            ExprResCacheManager::Value value;
            value.result = owned.result;
            value.valid_result = owned.valid;
            value.active_count = active_count;
            value.eval_duration_us = eval_us;
            manager.PutAdmitted(key, value, ticket);
        }
        return owned;
    }
};

}  // namespace exec
}  // namespace milvus
