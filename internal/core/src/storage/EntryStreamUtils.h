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
#include <deque>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/Common.h"
#include "common/EasyAssert.h"
#include "folly/CancellationToken.h"
#include "folly/OperationCancelled.h"
#include "folly/coro/Promise.h"
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

enum class TransientBudgetPriority {
    High,
    Low,
};

class TransientMemoryBudget;

class TransientBudgetLease {
 public:
    TransientBudgetLease() = default;
    TransientBudgetLease(const TransientBudgetLease&) = delete;
    TransientBudgetLease&
    operator=(const TransientBudgetLease&) = delete;
    TransientBudgetLease(TransientBudgetLease&& other) noexcept;
    TransientBudgetLease&
    operator=(TransientBudgetLease&& other) noexcept;
    ~TransientBudgetLease();

    void
    Release();

 private:
    friend class TransientMemoryBudget;

    TransientBudgetLease(TransientMemoryBudget* budget, size_t bytes)
        : budget_(budget), bytes_(bytes) {
    }

    TransientMemoryBudget* budget_{nullptr};
    size_t bytes_{0};
};

/// Byte budget for transient data that has been submitted for async work but
/// has not been consumed yet. Capacity 0 means unlimited.
///
/// Usage:
///   - Call AcquireAsync(bytes, priority, token) for a cancellable RAII lease.
///   - Call Acquire(bytes) to block until budget is available.
///   - Call AcquireUntil(bytes, cancellation_token) to block until budget is
///     available or cancellation is requested.
///   - Call TryAcquire(bytes) for non-blocking replenish in refill loops.
///   - Call Release(bytes) after the transient data has been consumed.
///   - Oversized requests are allowed to run exclusively to guarantee progress.
/// Async waiters are admitted high-priority first without reserved capacity.
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

    folly::coro::Future<TransientBudgetLease>
    AcquireAsync(size_t bytes,
                 TransientBudgetPriority priority,
                 const folly::CancellationToken& cancellation_token = {}) {
        auto [promise, future] =
            folly::coro::makePromiseContract<TransientBudgetLease>();
        auto pending = std::make_shared<PendingAdmission>();
        pending->promise = std::move(promise);
        pending->bytes = bytes;

        auto merged_cancellation_token = folly::cancellation_token_merge(
            cancellation_token, pending->promise.getCancellationToken());
        if (merged_cancellation_token.canBeCancelled()) {
            std::weak_ptr<PendingAdmission> weak_pending = pending;
            pending->cancellation_callback =
                std::make_unique<folly::CancellationCallback>(
                    merged_cancellation_token, [this, weak_pending]() {
                        if (auto admission = weak_pending.lock()) {
                            CancelPending(std::move(admission));
                        }
                    });
        }

        bool admitted = false;
        std::vector<std::shared_ptr<PendingAdmission>> cleanup;
        {
            std::lock_guard<std::mutex> lock(mu_);
            CleanupCancelledLocked(cleanup);
            if (pending->state == PendingAdmission::State::Cancelled) {
                cleanup.push_back(pending);
            } else if (CanAdmitImmediatelyLocked(priority, bytes)) {
                MarkAdmittedLocked(pending);
                admitted = true;
            } else {
                auto& queue = priority == TransientBudgetPriority::High
                                  ? high_pending_
                                  : low_pending_;
                queue.push_back(pending);
            }
        }

        if (admitted) {
            FulfillAdmission(std::move(pending));
        }
        return future;
    }

    /// Block until enough budget is available. Safe to call when the calling
    /// thread has no inflight tasks (no risk of deadlock with channel pop).
    void
    Acquire(size_t bytes) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this, bytes] { return CanLegacyAcquireLocked(bytes); });
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
                       CanLegacyAcquireLocked(bytes);
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
        if (CanLegacyAcquireLocked(bytes)) {
            inflight_bytes_ += bytes;
            return true;
        }
        return false;
    }

    void
    Release(size_t bytes) {
        PendingResolution resolution;
        {
            std::lock_guard<std::mutex> lock(mu_);
            AssertInfo(bytes <= inflight_bytes_,
                       "Transient memory budget over-release: release {}, "
                       "inflight {}",
                       bytes,
                       inflight_bytes_);
            inflight_bytes_ -= bytes;
            resolution = TakeAdmittedLocked();
        }
        ResolvePending(std::move(resolution));
        cv_.notify_all();
    }

    size_t
    CapacityBytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return CapacityBytesLocked();
    }

    void
    SetCapacityBytes(size_t bytes) {
        PendingResolution resolution;
        {
            std::lock_guard<std::mutex> update_lock(capacity_update_mutex_);
            auto old_capacity = CapacityBytes();
            auto expanding =
                old_capacity != 0 && (bytes == 0 || bytes > old_capacity);
            auto& overhead_controller =
                LoadMemoryOverheadController::GetInstance();
            if (expanding && !overhead_controller.UpdateBudgetBytes(bytes)) {
                return;
            }
            {
                std::lock_guard<std::mutex> lock(mu_);
                capacity_bytes_ = bytes;
                resolution = TakeAdmittedLocked();
            }
            if (!expanding) {
                overhead_controller.UpdateBudgetBytes(bytes);
            }
        }
        ResolvePending(std::move(resolution));
        cv_.notify_all();
    }

    void
    NotifyCapacityUpdated() {
        PendingResolution resolution;
        {
            std::lock_guard<std::mutex> lock(mu_);
            resolution = TakeAdmittedLocked();
        }
        ResolvePending(std::move(resolution));
        cv_.notify_all();
    }

 private:
    struct PendingAdmission {
        enum class State {
            Pending,
            Admitted,
            Cancelled,
        };

        size_t bytes{0};
        folly::coro::Promise<TransientBudgetLease> promise;
        std::unique_ptr<folly::CancellationCallback> cancellation_callback;
        State state{State::Pending};
    };

    struct PendingResolution {
        std::vector<std::shared_ptr<PendingAdmission>> admitted;
        std::vector<std::shared_ptr<PendingAdmission>> cleanup;
    };

    TransientMemoryBudget() = default;

    explicit TransientMemoryBudget(size_t capacity_bytes)
        : capacity_bytes_(capacity_bytes) {
    }

    size_t
    CapacityBytesLocked() const {
        return capacity_bytes_;
    }

    bool
    CanAcquireCapacityLocked(size_t bytes) const {
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

    bool
    HasPendingLocked() const {
        auto has_pending = [](const auto& queue) {
            return std::any_of(
                queue.begin(), queue.end(), [](const auto& admission) {
                    return admission->state == PendingAdmission::State::Pending;
                });
        };
        return has_pending(high_pending_) || has_pending(low_pending_);
    }

    bool
    CanLegacyAcquireLocked(size_t bytes) const {
        return !HasPendingLocked() && CanAcquireCapacityLocked(bytes);
    }

    bool
    CanAdmitImmediatelyLocked(TransientBudgetPriority priority,
                              size_t bytes) const {
        if (!CanAcquireCapacityLocked(bytes)) {
            return false;
        }
        if (priority == TransientBudgetPriority::High) {
            return high_pending_.empty();
        }
        return high_pending_.empty() && low_pending_.empty();
    }

    void
    MarkAdmittedLocked(const std::shared_ptr<PendingAdmission>& pending) {
        pending->state = PendingAdmission::State::Admitted;
        inflight_bytes_ += pending->bytes;
    }

    void
    CleanupCancelledLocked(
        std::vector<std::shared_ptr<PendingAdmission>>& cleanup) {
        auto cleanup_queue = [&cleanup](auto& queue) {
            auto it = queue.begin();
            while (it != queue.end()) {
                if ((*it)->state == PendingAdmission::State::Cancelled) {
                    cleanup.push_back(std::move(*it));
                    it = queue.erase(it);
                } else {
                    ++it;
                }
            }
        };
        cleanup_queue(high_pending_);
        cleanup_queue(low_pending_);
    }

    PendingResolution
    TakeAdmittedLocked() {
        PendingResolution resolution;
        CleanupCancelledLocked(resolution.cleanup);

        auto admit_queue = [this, &resolution](auto& queue) {
            while (!queue.empty()) {
                auto& pending = queue.front();
                if (!CanAcquireCapacityLocked(pending->bytes)) {
                    break;
                }
                auto admitted = std::move(pending);
                queue.pop_front();
                MarkAdmittedLocked(admitted);
                resolution.admitted.push_back(std::move(admitted));
            }
        };
        admit_queue(high_pending_);
        if (high_pending_.empty()) {
            admit_queue(low_pending_);
        }
        return resolution;
    }

    void
    FulfillAdmission(std::shared_ptr<PendingAdmission> pending) {
        auto lease = TransientBudgetLease(this, pending->bytes);
        pending->cancellation_callback.reset();
        pending->promise.trySetValue(std::move(lease));
    }

    void
    ResolvePending(PendingResolution resolution) {
        for (auto& pending : resolution.admitted) {
            FulfillAdmission(std::move(pending));
        }
        resolution.cleanup.clear();
    }

    void
    CancelPending(std::shared_ptr<PendingAdmission> pending) {
        PendingResolution resolution;
        bool cancelled = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (pending->state == PendingAdmission::State::Pending) {
                pending->state = PendingAdmission::State::Cancelled;
                cancelled = true;
                resolution = TakeAdmittedLocked();
            }
        }
        if (cancelled) {
            pending->promise.trySetException(folly::OperationCancelled{});
            ResolvePending(std::move(resolution));
            cv_.notify_all();
        }
    }

    std::mutex capacity_update_mutex_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    size_t inflight_bytes_{0};
    size_t capacity_bytes_{0};
    std::deque<std::shared_ptr<PendingAdmission>> high_pending_;
    std::deque<std::shared_ptr<PendingAdmission>> low_pending_;
};

inline TransientBudgetLease::TransientBudgetLease(
    TransientBudgetLease&& other) noexcept
    : budget_(other.budget_), bytes_(other.bytes_) {
    other.budget_ = nullptr;
    other.bytes_ = 0;
}

inline TransientBudgetLease&
TransientBudgetLease::operator=(TransientBudgetLease&& other) noexcept {
    if (this != &other) {
        Release();
        budget_ = other.budget_;
        bytes_ = other.bytes_;
        other.budget_ = nullptr;
        other.bytes_ = 0;
    }
    return *this;
}

inline TransientBudgetLease::~TransientBudgetLease() {
    Release();
}

inline void
TransientBudgetLease::Release() {
    if (budget_ == nullptr || bytes_ == 0) {
        return;
    }
    auto* budget = budget_;
    auto bytes = bytes_;
    budget_ = nullptr;
    bytes_ = 0;
    budget->Release(bytes);
}

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
