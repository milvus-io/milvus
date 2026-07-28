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
#include "storage/ThreadPools.h"

namespace milvus::storage {

constexpr size_t kMinStreamSliceSize = 64 * 1024;
constexpr size_t kStreamSliceAlignment = 4 * 1024;
constexpr size_t kTailMergeGrace = 1 * 1024 * 1024;
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

    static void
    SetLoadTransientBudgetBytes(size_t bytes) {
        GetLoadTransientBudget().SetCapacityBytes(bytes);
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

    size_t
    CapacityBytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return CapacityBytesLocked();
    }

    void
    SetCapacityBytes(size_t bytes) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            capacity_bytes_ = bytes;
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

    mutable std::mutex mu_;
    std::condition_variable cv_;
    size_t inflight_bytes_{0};
    size_t capacity_bytes_{0};
};

inline size_t
EntryStreamDataTransientBytes(size_t stream_bytes, bool encrypted) {
    if (!encrypted) {
        return stream_bytes;
    }
    if (stream_bytes >
        std::numeric_limits<size_t>::max() / kEncryptedStreamBufferMultiplier) {
        return std::numeric_limits<size_t>::max();
    }
    return stream_bytes * kEncryptedStreamBufferMultiplier;
}

inline size_t
PlainEntryFileStreamTransientBytes(size_t stream_bytes) {
    constexpr size_t kFileStreamBufferMultiplier = 2;
    if (stream_bytes >
        std::numeric_limits<size_t>::max() / kFileStreamBufferMultiplier) {
        return std::numeric_limits<size_t>::max();
    }
    return stream_bytes * kFileStreamBufferMultiplier;
}

inline size_t
EncryptedEntryStreamTaskTransientBytes() {
    // Compatibility fallback for callers that cannot inspect a concrete V3
    // directory. File-aware planning uses persisted ciphertext slice sizes.
    return EntryStreamDataTransientBytes(
        DefaultStreamSliceSize() + kTailMergeGrace, true);
}

inline size_t
PlainEntryStreamTaskTransientBytes() {
    return DefaultStreamSliceSize() + kTailMergeGrace;
}

inline size_t
EntryStreamPoolBoundTransientBytesForTask(size_t task_bound,
                                          size_t live_worker_count) {
    auto configured_threads =
        std::max(milvus::ComputeThreadPoolMaxThreads(
                     milvus::HIGH_PRIORITY_THREAD_CORE_COEFFICIENT.load()),
                 milvus::ComputeThreadPoolMaxThreads(
                     milvus::LOW_PRIORITY_THREAD_CORE_COEFFICIENT.load()));
    auto max_tasks =
        std::max(static_cast<size_t>(configured_threads), live_worker_count);
    if (task_bound > std::numeric_limits<size_t>::max() / max_tasks) {
        return std::numeric_limits<size_t>::max();
    }
    return max_tasks * task_bound;
}

inline size_t
EntryStreamPoolBoundTransientBytes(bool encrypted = false,
                                   size_t live_worker_count = 0) {
    auto task_bound = encrypted ? EncryptedEntryStreamTaskTransientBytes()
                                : PlainEntryStreamTaskTransientBytes();
    return EntryStreamPoolBoundTransientBytesForTask(task_bound,
                                                     live_worker_count);
}

inline size_t
PlainEntryFileStreamTaskTransientBytes() {
    return PlainEntryFileStreamTransientBytes(
        PlainEntryStreamTaskTransientBytes());
}

inline size_t
PlainEntryFileStreamPoolBoundTransientBytes(size_t live_worker_count = 0) {
    return EntryStreamPoolBoundTransientBytesForTask(
        PlainEntryFileStreamTaskTransientBytes(), live_worker_count);
}

inline size_t
EntryStreamMaxTransientBytes(bool encrypted = false) {
    auto capacity =
        TransientMemoryBudget::GetLoadTransientBudget().CapacityBytes();
    auto& high_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& low_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    auto pool_bound = EntryStreamPoolBoundTransientBytes(
        encrypted, std::max(high_pool.GetThreadNum(), low_pool.GetThreadNum()));
    if (capacity == 0) {
        // The runtime reservation for encrypted streams includes the actual
        // ciphertext length, which has no static upper bound in ICipherPlugin.
        // Let the caller cap the estimate by its concrete index size instead.
        return encrypted ? std::numeric_limits<size_t>::max() : pool_bound;
    }
    if (encrypted) {
        return std::min(
            std::max(capacity, EncryptedEntryStreamTaskTransientBytes()),
            pool_bound);
    }

    return std::min(std::max(capacity, PlainEntryStreamTaskTransientBytes()),
                    pool_bound);
}

inline size_t
EncryptedEntryStreamMaxTransientBytes(size_t total_transient_bytes,
                                      size_t max_task_transient_bytes) {
    if (total_transient_bytes == 0 || max_task_transient_bytes == 0) {
        return 0;
    }

    auto capacity =
        TransientMemoryBudget::GetLoadTransientBudget().CapacityBytes();
    auto& high_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& low_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    auto pool_bound = EntryStreamPoolBoundTransientBytesForTask(
        max_task_transient_bytes,
        std::max(high_pool.GetThreadNum(), low_pool.GetThreadNum()));
    auto budget_bound =
        capacity == 0 ? pool_bound
                      : std::min(std::max(capacity, max_task_transient_bytes),
                                 pool_bound);
    return std::min(total_transient_bytes, budget_bound);
}

inline size_t
PlainEntryFileStreamMaxTransientBytes() {
    auto capacity =
        TransientMemoryBudget::GetLoadTransientBudget().CapacityBytes();
    auto& high_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& low_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    auto pool_bound = PlainEntryFileStreamPoolBoundTransientBytes(
        std::max(high_pool.GetThreadNum(), low_pool.GetThreadNum()));
    auto task_bound = PlainEntryFileStreamTaskTransientBytes();
    if (capacity == 0) {
        return pool_bound;
    }
    return std::min(std::max(capacity, task_bound), pool_bound);
}

}  // namespace milvus::storage
