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
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>

#include "folly/CancellationToken.h"
#include "folly/coro/Promise.h"
#include "folly/synchronization/Baton.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

enum class LoadAdmissionPriority {
    High,
    Low,
};

// Maps a load thread-pool priority to the load admission class.
[[nodiscard]] constexpr LoadAdmissionPriority
LoadAdmissionPriorityForThreadPool(
    milvus::ThreadPoolPriority priority) noexcept {
    return priority == milvus::ThreadPoolPriority::LOW
               ? LoadAdmissionPriority::Low
               : LoadAdmissionPriority::High;
}

// Resources reserved together for one load operation. All current production
// callers request one slot per window, batch, or stream slice.
struct LoadAdmissionRequest {
    size_t transient_bytes{0};
    size_t slots{0};
};

class LoadAdmissionController;

// Owns both resource reservations and returns them to the controller on
// destruction. A moved-from or default-constructed lease owns no reservation.
class LoadAdmissionLease {
 public:
    LoadAdmissionLease() = default;
    LoadAdmissionLease(const LoadAdmissionLease&) = delete;
    LoadAdmissionLease&
    operator=(const LoadAdmissionLease&) = delete;
    LoadAdmissionLease(LoadAdmissionLease&& other) noexcept;
    LoadAdmissionLease&
    operator=(LoadAdmissionLease&& other) noexcept;
    ~LoadAdmissionLease();

    // Releases the reservation early; repeated calls are no-ops.
    void
    Release();

 private:
    friend class LoadAdmissionController;

    LoadAdmissionLease(LoadAdmissionController* controller,
                       LoadAdmissionRequest request)
        : controller_(controller), request_(request) {
    }

    LoadAdmissionController* controller_{nullptr};
    LoadAdmissionRequest request_;
};

// Jointly admits transient bytes and slots held by submitted load work. Zero
// capacity disables that dimension's limit. Oversized byte requests may run
// when no other bytes are reserved, but must still satisfy the slot limit.
// High priority precedes low priority; each class is FIFO. Sustained high
// priority traffic may starve low priority work. Waiters reserve neither resource.
// A weighted request above the total slot capacity waits for a capacity increase
// or cancellation; callers must size requests to permit progress.
class LoadAdmissionController {
 public:
    // Returns the process-wide controller shared by all load paths.
    [[nodiscard]] static LoadAdmissionController&
    GetInstance();

    // Updates the process-wide load budget and wakes newly admissible waiters.
    static void
    SetLoadTransientBudgetBytes(size_t bytes);

    // Asynchronously waits for a cancellable RAII reservation.
    [[nodiscard]] folly::coro::Future<LoadAdmissionLease>
    AcquireAsync(LoadAdmissionRequest request,
                 LoadAdmissionPriority priority,
                 const folly::CancellationToken& cancellation_token = {});

    // Blocks until the requested reservation is admitted. The caller must not
    // hold inflight work whose completion is needed to release budget.
    void
    Acquire(LoadAdmissionRequest request, LoadAdmissionPriority priority);

    // Blocks until admitted or cancelled. False means neither resource was reserved.
    [[nodiscard]] bool
    AcquireUntil(LoadAdmissionRequest request,
                 LoadAdmissionPriority priority,
                 const folly::CancellationToken& cancellation_token);

    // Attempts immediate admission without waiting; refill loops use this to
    // avoid blocking while they still own inflight work.
    [[nodiscard]] bool
    TryAcquire(LoadAdmissionRequest request, LoadAdmissionPriority priority);

    // Releases exactly the request associated with a successful blocking or
    // TryAcquire admission. Never release a reservation also owned by a lease.
    void
    Release(LoadAdmissionRequest request);

    // Returns the configured capacity in bytes; zero means unlimited.
    [[nodiscard]] size_t
    CapacityBytes() const;

    // Updates this budget and its load-overhead controller consistently. An
    // expansion rejected by the controller leaves both capacities unchanged.
    void
    SetCapacityBytes(size_t bytes);

    // Returns the configured slot capacity; zero means unlimited.
    [[nodiscard]] size_t
    CapacitySlots() const;

    // Re-evaluates waiters after updating slots. Shrinking does not revoke
    // admitted work; new requests wait until they fit the new capacity.
    void
    SetCapacitySlots(size_t slots);

    // Re-evaluates pending admissions after an external capacity change.
    void
    NotifyCapacityUpdated();

    // Publishes one resource/queue snapshot without holding mu_ during metric
    // updates. The scrape boundary must serialize publication and collection.
    void
    UpdateMetrics() const;

 private:
    using Clock = std::chrono::steady_clock;
    struct PendingAdmission;
    using PendingQueue = std::list<std::shared_ptr<PendingAdmission>>;

    struct PendingAdmission {
        enum class State {
            Pending,
            Admitted,
            Cancelled,
        };

        LoadAdmissionRequest request;
        folly::coro::Promise<LoadAdmissionLease> promise;
        std::unique_ptr<folly::CancellationCallback> cancellation_callback;
        State state{State::Pending};
        bool is_blocking_waiter{false};
        // The terminal-state winner posts once, even if wait has not started.
        folly::Baton<> ready;
        // Queue membership and this iterator are protected by mu_.
        PendingQueue* queue{nullptr};
        PendingQueue::iterator queue_position{};
        // Initialized only on enqueue, then immutable until resolution.
        Clock::time_point queued_at{};
        LoadAdmissionPriority priority{LoadAdmissionPriority::High};
    };

    struct PendingResolution {
        PendingQueue admitted;
        Clock::time_point admitted_at{};
    };

    LoadAdmissionController() = default;

    // Methods suffixed with Locked require mu_ to be held by the caller.
    [[nodiscard]] size_t
    CapacityBytesLocked() const;

    [[nodiscard]] bool
    CanAcquireCapacityLocked(LoadAdmissionRequest request) const;

    [[nodiscard]] bool
    CanAdmitImmediatelyLocked(LoadAdmissionPriority priority,
                              LoadAdmissionRequest request) const;

    // Transfers the single node prepared outside mu_ into its priority queue.
    void
    EnqueuePendingLocked(PendingQueue& prepared,
                         LoadAdmissionPriority priority);

    // Accounts for resources after the caller has checked joint capacity.
    void
    ReserveLocked(LoadAdmissionRequest request) noexcept;

    void
    MarkAdmittedLocked(PendingAdmission& pending);

    [[nodiscard]] PendingResolution
    TakeAdmittedLocked();

    // Resolves one admission after it is removed from a protected queue.
    void
    FulfillAdmission(std::shared_ptr<PendingAdmission> pending);

    // Completes promises and posts blocking waiters without holding mu_.
    void
    ResolvePending(PendingResolution resolution);

    // Only for a waiter that actually entered a queue and reached a terminal
    // state. Histogram updates must run outside mu_.
    static void
    ObserveQueueWait(const PendingAdmission& pending,
                     Clock::time_point finished_at);

    // Cancels a pending waiter and then admits any newly unblocked work.
    void
    CancelPending(std::shared_ptr<PendingAdmission> pending);

    std::mutex capacity_update_mutex_;
    mutable std::mutex mu_;
    size_t inflight_bytes_{0};
    size_t inflight_slots_{0};
    size_t capacity_bytes_{0};
    size_t capacity_slots_{0};
    PendingQueue high_pending_;
    PendingQueue low_pending_;
};

}  // namespace milvus::storage
