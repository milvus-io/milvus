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

#include <condition_variable>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>

#include "folly/CancellationToken.h"
#include "folly/coro/Promise.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

enum class TransientBudgetPriority {
    High,
    Low,
};

// Maps a load thread-pool priority to the transient-budget admission class.
[[nodiscard]] constexpr TransientBudgetPriority
TransientPriorityForThreadPool(milvus::ThreadPoolPriority priority) noexcept {
    return priority == milvus::ThreadPoolPriority::LOW
               ? TransientBudgetPriority::Low
               : TransientBudgetPriority::High;
}

class TransientMemoryBudget;

// Owns a transient-memory reservation and returns it to the budget on
// destruction. A moved-from or default-constructed lease owns no reservation.
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

    // Releases the reservation early; repeated calls are no-ops.
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

// Coordinates transient bytes held by submitted load work. Capacity zero is
// unlimited, while an oversized request is admitted exclusively to preserve
// progress. Pending asynchronous work is admitted high-priority first.
class TransientMemoryBudget {
 public:
    // Returns the process-wide budget shared by all load paths.
    [[nodiscard]] static TransientMemoryBudget&
    GetLoadTransientBudget();

    // Updates the process-wide load budget and wakes newly admissible waiters.
    static void
    SetLoadTransientBudgetBytes(size_t bytes);

    // Asynchronously waits for a cancellable RAII reservation.
    [[nodiscard]] folly::coro::Future<TransientBudgetLease>
    AcquireAsync(size_t bytes,
                 TransientBudgetPriority priority,
                 const folly::CancellationToken& cancellation_token = {});

    // Blocks until the requested reservation is admitted. The caller must not
    // hold inflight work whose completion is needed to release budget.
    void
    Acquire(size_t bytes, TransientBudgetPriority priority);

    // Blocks until admitted or cancelled. False means no bytes were reserved.
    [[nodiscard]] bool
    AcquireUntil(size_t bytes,
                 TransientBudgetPriority priority,
                 const folly::CancellationToken& cancellation_token);

    // Attempts immediate admission without waiting; refill loops use this to
    // avoid blocking while they still own inflight work.
    [[nodiscard]] bool
    TryAcquire(size_t bytes, TransientBudgetPriority priority);

    // Releases exactly the bytes associated with a prior successful admission.
    void
    Release(size_t bytes);

    // Returns the configured capacity in bytes; zero means unlimited.
    [[nodiscard]] size_t
    CapacityBytes() const;

    // Updates this budget and its load-overhead controller consistently. An
    // expansion rejected by the controller leaves both capacities unchanged.
    void
    SetCapacityBytes(size_t bytes);

    // Re-evaluates pending admissions after an external capacity change.
    void
    NotifyCapacityUpdated();

 private:
    struct PendingAdmission;
    using PendingQueue = std::list<std::shared_ptr<PendingAdmission>>;

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
        bool is_blocking_waiter{false};
        // Queue membership and this iterator are protected by mu_.
        PendingQueue* queue{nullptr};
        PendingQueue::iterator queue_position{};
    };

    struct PendingResolution {
        PendingQueue admitted;
    };

    TransientMemoryBudget() = default;

    // Methods suffixed with Locked require mu_ to be held by the caller.
    [[nodiscard]] size_t
    CapacityBytesLocked() const;

    [[nodiscard]] bool
    CanAcquireCapacityLocked(size_t bytes) const;

    [[nodiscard]] bool
    CanAdmitImmediatelyLocked(TransientBudgetPriority priority,
                              size_t bytes) const;

    void
    EnqueuePendingLocked(const std::shared_ptr<PendingAdmission>& pending,
                         TransientBudgetPriority priority);

    void
    MarkAdmittedLocked(const std::shared_ptr<PendingAdmission>& pending);

    [[nodiscard]] PendingResolution
    TakeAdmittedLocked();

    // Resolves one admission after it is removed from a protected queue.
    void
    FulfillAdmission(std::shared_ptr<PendingAdmission> pending);

    // Resolves admitted asynchronous waiters without holding mu_.
    void
    ResolvePending(PendingResolution resolution);

    // Cancels a pending waiter and then admits any newly unblocked work.
    void
    CancelPending(std::shared_ptr<PendingAdmission> pending);

    std::mutex capacity_update_mutex_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    size_t inflight_bytes_{0};
    size_t capacity_bytes_{0};
    PendingQueue high_pending_;
    PendingQueue low_pending_;
};

}  // namespace milvus::storage
