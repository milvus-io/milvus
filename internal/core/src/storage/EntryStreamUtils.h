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

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/Common.h"
#include "common/EasyAssert.h"
#include "folly/CancellationToken.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

constexpr size_t kMinStreamSliceSize = 64 * 1024;
constexpr size_t kStreamSliceAlignment = 4 * 1024;
constexpr size_t kTailMergeGrace = 1 * 1024 * 1024;
constexpr size_t kFileStreamBufferMultiplier = 2;
// Encrypted reads may simultaneously retain ciphertext, decrypted plaintext,
// and the returned plaintext buffer.
constexpr size_t kEncryptedStreamBufferMultiplier = 3;

inline bool
IsStreamSliceSizeAligned(size_t slice_size) {
    return slice_size > 0 && slice_size % kStreamSliceAlignment == 0;
}

inline size_t
DefaultStreamSliceSize() {
    return DEFAULT_INDEX_FILE_SLICE_SIZE;
}

inline void
ThrowIfCancelled(const folly::CancellationToken& cancellation_token,
                 const std::string& operation) {
    if (cancellation_token.isCancellationRequested()) {
        ThrowInfo(ErrorCode::FollyCancel, "{} cancelled", operation);
    }
}

/// A slice read from a V3 entry. `error` carries an exception captured in
/// the producer task so the consumer can rethrow instead of hanging.
struct StreamSliceResult {
    size_t slice_transient_bytes{0};
    std::vector<uint8_t> data;
    std::exception_ptr error = nullptr;
};

/// Byte budget for transient data that has been submitted for async work but
/// has not been consumed yet. Capacity 0 means unlimited.
///
/// Usage:
///   - Call Acquire(bytes) to block until budget is available.
///   - Call AcquireUntil(bytes, cancellation_token) to block until budget is
///     available or cancellation is requested.
///   - Call TryAcquire(bytes) for non-blocking replenish in refill loops.
///   - Call Release(bytes) after the transient data has been consumed.
///   - Oversized requests are allowed to run exclusively to guarantee progress.
class TransientMemoryBudget {
 public:
    static TransientMemoryBudget&
    GetLoadTransientBudget() {
        static TransientMemoryBudget instance;
        return instance;
    }

    static TransientMemoryBudget&
    GetJsonStatsBuildBudget() {
        static TransientMemoryBudget instance;
        return instance;
    }

    static void
    SetLoadTransientBudgetBytes(size_t bytes) {
        GetLoadTransientBudget().SetCapacityBytes(bytes);
    }

    static void
    SetJsonStatsBuildBudgetBytes(size_t bytes) {
        GetJsonStatsBuildBudget().SetCapacityBytes(bytes);
    }

    /// Block until enough budget is available. Safe to call when the calling
    /// thread has no inflight tasks (no risk of deadlock with channel pop).
    void
    Acquire(size_t bytes) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this, bytes] { return CanAcquireLocked(bytes); });
        inflight_bytes_ += bytes;
    }

    /// Block until enough budget is available, or cancellation is requested.
    /// Returning false means no budget was acquired and the caller should stop
    /// its work.
    bool
    AcquireUntil(size_t bytes,
                 const folly::CancellationToken& cancellation_token) {
        folly::CancellationCallback cancel_callback(
            cancellation_token, [this]() noexcept {
                // Pair with wait(lock, predicate) to avoid losing a cancel
                // notification between predicate check and wait.
                std::lock_guard<std::mutex> lock(mu_);
                cv_.notify_all();
            });

        bool acquired = false;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait(lock, [this, bytes, &cancellation_token] {
                return cancellation_token.isCancellationRequested() ||
                       CanAcquireLocked(bytes);
            });
            if (!cancellation_token.isCancellationRequested()) {
                inflight_bytes_ += bytes;
                acquired = true;
            }
        }

        return acquired;
    }

    /// Try to claim budget. Returns true if under budget.
    /// Used in the refill loop where blocking could cause deadlock.
    bool
    TryAcquire(size_t bytes) {
        std::lock_guard<std::mutex> lock(mu_);
        if (CanAcquireLocked(bytes)) {
            inflight_bytes_ += bytes;
            return true;
        }
        return false;
    }

    void
    Release(size_t bytes) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            AssertInfo(bytes <= inflight_bytes_,
                       "Transient memory budget over-release: release {}, "
                       "inflight {}",
                       bytes,
                       inflight_bytes_);
            inflight_bytes_ -= bytes;
        }
        cv_.notify_all();
    }

    /// Replace a task's pre-dispatch reservation with its measured result
    /// size. Growth is intentionally non-blocking: completed workers must not
    /// wait for consumers that are ordered behind other active tasks. Any
    /// temporary overage prevents new TryAcquire calls until consumers release
    /// enough bytes.
    void
    ReconcileReservation(size_t reserved_bytes, size_t actual_bytes) {
        bool released_bytes = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            AssertInfo(reserved_bytes <= inflight_bytes_,
                       "Transient memory budget reconcile exceeds inflight: "
                       "reserved {}, inflight {}",
                       reserved_bytes,
                       inflight_bytes_);
            auto other_inflight_bytes = inflight_bytes_ - reserved_bytes;
            AssertInfo(actual_bytes <= std::numeric_limits<size_t>::max() -
                                           other_inflight_bytes,
                       "Transient memory budget reconcile overflow: actual "
                       "{}, other inflight {}",
                       actual_bytes,
                       other_inflight_bytes);
            inflight_bytes_ = other_inflight_bytes + actual_bytes;
            released_bytes = actual_bytes < reserved_bytes;
        }
        if (released_bytes) {
            cv_.notify_all();
        }
    }

    size_t
    CapacityBytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return CapacityBytesLocked();
    }

    size_t
    InflightBytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return inflight_bytes_;
    }

    void
    SetCapacityBytes(size_t bytes) {
        std::lock_guard<std::mutex> update_lock(capacity_update_mutex_);
        auto old_capacity = CapacityBytes();
        auto expanding =
            old_capacity != 0 && (bytes == 0 || bytes > old_capacity);
        auto& overhead_controller = LoadMemoryOverheadController::GetInstance();
        if (expanding && !overhead_controller.UpdateBudgetBytes(bytes)) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            capacity_bytes_ = bytes;
        }
        if (!expanding) {
            overhead_controller.UpdateBudgetBytes(bytes);
        }
        cv_.notify_all();
    }

    void
    NotifyCapacityUpdated() {
        cv_.notify_all();
    }

 private:
    TransientMemoryBudget() = default;

    explicit TransientMemoryBudget(size_t capacity_bytes)
        : capacity_bytes_(capacity_bytes) {
    }

    size_t
    CapacityBytesLocked() const {
        return capacity_bytes_;
    }

    bool
    CanAcquireLocked(size_t bytes) const {
        auto capacity_bytes = CapacityBytesLocked();
        if (capacity_bytes == 0) {
            return true;
        }
        if (bytes > capacity_bytes) {
            return inflight_bytes_ == 0;
        }
        return inflight_bytes_ <= capacity_bytes &&
               bytes <= capacity_bytes - inflight_bytes_;
    }

    std::mutex capacity_update_mutex_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    size_t inflight_bytes_{0};
    size_t capacity_bytes_{0};
};

inline size_t
SaturatingMultiply(size_t value, size_t multiplier) {
    if (value == 0 || multiplier == 0) {
        return 0;
    }
    if (value > std::numeric_limits<size_t>::max() / multiplier) {
        return std::numeric_limits<size_t>::max();
    }
    return value * multiplier;
}

inline size_t
MaxEntryStreamTaskBytes() {
    return DefaultStreamSliceSize() + kTailMergeGrace;
}

inline size_t
EntryStreamTransientBytes(size_t stream_bytes, bool encrypted) {
    // This is the compatibility fallback for callers that cannot inspect a
    // concrete encrypted V3 directory. File-aware planning uses persisted
    // ciphertext slice sizes instead.
    auto buffer_multiplier =
        encrypted ? kEncryptedStreamBufferMultiplier : size_t{1};
    return SaturatingMultiply(stream_bytes, buffer_multiplier);
}

inline size_t
EntryStreamMaxTransientBytes(size_t total_transient_bytes,
                             size_t max_task_transient_bytes,
                             size_t live_worker_count = 0) {
    if (total_transient_bytes == 0 || max_task_transient_bytes == 0) {
        return 0;
    }

    auto configured_threads =
        std::max(milvus::ComputeThreadPoolMaxThreads(
                     milvus::HIGH_PRIORITY_THREAD_CORE_COEFFICIENT.load()),
                 milvus::ComputeThreadPoolMaxThreads(
                     milvus::LOW_PRIORITY_THREAD_CORE_COEFFICIENT.load()));
    auto& high_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& low_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    auto max_tasks =
        std::max({static_cast<size_t>(configured_threads),
                  std::max(high_pool.GetThreadNum(), low_pool.GetThreadNum()),
                  live_worker_count});
    auto pool_bound = SaturatingMultiply(max_task_transient_bytes, max_tasks);
    auto capacity =
        TransientMemoryBudget::GetLoadTransientBudget().CapacityBytes();
    auto budget_bound =
        capacity == 0 ? pool_bound
                      : std::min(std::max(capacity, max_task_transient_bytes),
                                 pool_bound);
    return std::min(total_transient_bytes, budget_bound);
}

}  // namespace milvus::storage
