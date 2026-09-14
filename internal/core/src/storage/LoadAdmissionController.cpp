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

#include "storage/LoadAdmissionController.h"

#include <utility>

#include "common/EasyAssert.h"
#include "folly/OperationCancelled.h"
#include "monitor/Monitor.h"
#include "storage/LoadOverheadController.h"

namespace milvus::storage {

LoadAdmissionLease::LoadAdmissionLease(LoadAdmissionLease&& other) noexcept
    : controller_(std::exchange(other.controller_, nullptr)),
      request_(std::exchange(other.request_, {})) {
}

LoadAdmissionLease&
LoadAdmissionLease::operator=(LoadAdmissionLease&& other) noexcept {
    if (this != &other) {
        Release();
        controller_ = std::exchange(other.controller_, nullptr);
        request_ = std::exchange(other.request_, {});
    }
    return *this;
}

LoadAdmissionLease::~LoadAdmissionLease() {
    Release();
}

void
LoadAdmissionLease::Release() {
    if (controller_ == nullptr) {
        return;
    }
    auto* const controller = std::exchange(controller_, nullptr);
    const auto request = std::exchange(request_, {});
    controller->Release(request);
}

LoadAdmissionController&
LoadAdmissionController::GetInstance() {
    static LoadAdmissionController instance;
    return instance;
}

void
LoadAdmissionController::SetLoadTransientBudgetBytes(const size_t bytes) {
    GetInstance().SetCapacityBytes(bytes);
}

folly::coro::Future<LoadAdmissionLease>
LoadAdmissionController::AcquireAsync(
    const LoadAdmissionRequest request,
    const LoadAdmissionPriority priority,
    const folly::CancellationToken& cancellation_token) {
    auto [promise, future] =
        folly::coro::makePromiseContract<LoadAdmissionLease>();
    bool admitted = false;
    bool cancelled = false;
    {
        std::lock_guard lock(mu_);
        cancelled = cancellation_token.isCancellationRequested();
        if (!cancelled && CanAdmitImmediatelyLocked(priority, request)) {
            ReserveLocked(request);
            admitted = true;
        }
    }
    if (cancelled) {
        promise.trySetException(folly::OperationCancelled{});
        return std::move(future);
    }
    if (admitted) {
        promise.trySetValue(LoadAdmissionLease(this, request));
        return std::move(future);
    }

    // The returned future has not escaped yet, so only the explicit token
    // can cancel the fast path. Register both sources before queueing below.
    auto pending = std::make_shared<PendingAdmission>();
    pending->promise = std::move(promise);
    pending->request = request;
    PendingQueue prepared{pending};

    const auto merged_cancellation_token = folly::cancellation_token_merge(
        cancellation_token, pending->promise.getCancellationToken());
    if (merged_cancellation_token.canBeCancelled()) {
        const std::weak_ptr<PendingAdmission> weak_pending = pending;
        pending->cancellation_callback =
            std::make_unique<folly::CancellationCallback>(
                merged_cancellation_token, [this, weak_pending]() {
                    if (auto admission = weak_pending.lock()) {
                        CancelPending(std::move(admission));
                    }
                });
    }

    {
        std::lock_guard lock(mu_);
        if (pending->state != PendingAdmission::State::Cancelled) {
            // Capacity, queue order and cancellation may have changed while
            // the waiter and callback were being prepared outside the lock.
            if (CanAdmitImmediatelyLocked(priority, request)) {
                MarkAdmittedLocked(*pending);
                admitted = true;
            } else {
                EnqueuePendingLocked(prepared, priority);
            }
        }
    }

    if (admitted) {
        FulfillAdmission(std::move(pending));
    }
    return std::move(future);
}

void
LoadAdmissionController::Acquire(const LoadAdmissionRequest request,
                                 const LoadAdmissionPriority priority) {
    const bool admitted = AcquireUntil(request, priority, {});
    AssertInfo(admitted, "Blocking load admission was not admitted");
}

bool
LoadAdmissionController::AcquireUntil(
    const LoadAdmissionRequest request,
    const LoadAdmissionPriority priority,
    const folly::CancellationToken& cancellation_token) {
    {
        std::lock_guard lock(mu_);
        if (cancellation_token.isCancellationRequested()) {
            return false;
        }
        if (CanAdmitImmediatelyLocked(priority, request)) {
            ReserveLocked(request);
            return true;
        }
    }

    auto pending = std::make_shared<PendingAdmission>();
    pending->request = request;
    pending->is_blocking_waiter = true;
    PendingQueue prepared{pending};
    if (cancellation_token.canBeCancelled()) {
        const std::weak_ptr<PendingAdmission> weak_pending = pending;
        pending->cancellation_callback =
            std::make_unique<folly::CancellationCallback>(
                cancellation_token, [this, weak_pending]() {
                    if (auto admission = weak_pending.lock()) {
                        CancelPending(std::move(admission));
                    }
                });
    }

    bool admitted = false;
    {
        std::lock_guard lock(mu_);
        if (pending->state != PendingAdmission::State::Cancelled) {
            if (CanAdmitImmediatelyLocked(priority, request)) {
                MarkAdmittedLocked(*pending);
                admitted = true;
            } else {
                EnqueuePendingLocked(prepared, priority);
            }
        }
    }

    if (admitted) {
        FulfillAdmission(pending);
    }
    // Always pair the terminal-state winner's post with one wait, including
    // cancellation during callback construction and admission before wait.
    pending->ready.wait();
    pending->cancellation_callback.reset();
    // The terminal state is immutable and published by the Baton handoff.
    return pending->state == PendingAdmission::State::Admitted;
}

bool
LoadAdmissionController::TryAcquire(const LoadAdmissionRequest request,
                                    const LoadAdmissionPriority priority) {
    std::lock_guard lock(mu_);
    if (CanAdmitImmediatelyLocked(priority, request)) {
        ReserveLocked(request);
        return true;
    }
    return false;
}

void
LoadAdmissionController::Release(const LoadAdmissionRequest request) {
    PendingResolution resolution;
    {
        std::lock_guard lock(mu_);
        AssertInfo(
            request.transient_bytes <= inflight_bytes_ &&
                request.slots <= inflight_slots_,
            "Load admission over-release: release ({}, {}), inflight ({}, {})",
            request.transient_bytes,
            request.slots,
            inflight_bytes_,
            inflight_slots_);
        inflight_bytes_ -= request.transient_bytes;
        inflight_slots_ -= request.slots;
        resolution = TakeAdmittedLocked();
    }
    ResolvePending(std::move(resolution));
}

size_t
LoadAdmissionController::CapacityBytes() const {
    std::lock_guard lock(mu_);
    return CapacityBytesLocked();
}

void
LoadAdmissionController::SetCapacityBytes(const size_t bytes) {
    PendingResolution resolution;
    {
        std::lock_guard update_lock(capacity_update_mutex_);
        const auto old_capacity = CapacityBytes();
        const bool expanding =
            old_capacity != 0 && (bytes == 0 || bytes > old_capacity);
        auto& overhead_controller = LoadMemoryOverheadController::GetInstance();
        if (expanding && !overhead_controller.UpdateBudgetBytes(bytes)) {
            return;
        }
        {
            std::lock_guard lock(mu_);
            capacity_bytes_ = bytes;
            resolution = TakeAdmittedLocked();
        }
        if (!expanding) {
            overhead_controller.UpdateBudgetBytes(bytes);
        }
    }
    ResolvePending(std::move(resolution));
}

size_t
LoadAdmissionController::CapacitySlots() const {
    std::lock_guard lock(mu_);
    return capacity_slots_;
}

void
LoadAdmissionController::SetCapacitySlots(const size_t slots) {
    PendingResolution resolution;
    {
        std::lock_guard lock(mu_);
        capacity_slots_ = slots;
        resolution = TakeAdmittedLocked();
    }
    ResolvePending(std::move(resolution));
}

void
LoadAdmissionController::NotifyCapacityUpdated() {
    PendingResolution resolution;
    {
        std::lock_guard lock(mu_);
        resolution = TakeAdmittedLocked();
    }
    ResolvePending(std::move(resolution));
}

void
LoadAdmissionController::UpdateMetrics() const {
    struct Snapshot {
        size_t reserved_bytes;
        size_t capacity_bytes;
        size_t reserved_slots;
        size_t capacity_slots;
        size_t high_pending;
        size_t low_pending;
        double high_oldest_wait;
        double low_oldest_wait;
    } snapshot;
    {
        std::lock_guard lock(mu_);
        const auto now = Clock::now();
        const auto oldest_wait = [now](const PendingQueue& queue) {
            return queue.empty() ? 0.0
                                 : std::chrono::duration<double>(
                                       now - queue.front()->queued_at)
                                       .count();
        };
        snapshot = {inflight_bytes_,
                    capacity_bytes_,
                    inflight_slots_,
                    capacity_slots_,
                    high_pending_.size(),
                    low_pending_.size(),
                    oldest_wait(high_pending_),
                    oldest_wait(low_pending_)};
    }
    using namespace milvus::monitor;
    internal_load_admission_reserved_bytes.Set(snapshot.reserved_bytes);
    internal_load_admission_capacity_bytes.Set(snapshot.capacity_bytes);
    internal_load_admission_reserved_slots.Set(snapshot.reserved_slots);
    internal_load_admission_capacity_slots.Set(snapshot.capacity_slots);
    internal_load_admission_pending_requests_high.Set(snapshot.high_pending);
    internal_load_admission_pending_requests_low.Set(snapshot.low_pending);
    internal_load_admission_oldest_wait_seconds_high.Set(
        snapshot.high_oldest_wait);
    internal_load_admission_oldest_wait_seconds_low.Set(
        snapshot.low_oldest_wait);
}

size_t
LoadAdmissionController::CapacityBytesLocked() const {
    return capacity_bytes_;
}

bool
LoadAdmissionController::CanAcquireCapacityLocked(
    const LoadAdmissionRequest request) const {
    if (capacity_slots_ != 0 &&
        (inflight_slots_ > capacity_slots_ ||
         request.slots > capacity_slots_ - inflight_slots_)) {
        return false;
    }
    const auto bytes = request.transient_bytes;
    const auto capacity_bytes = CapacityBytesLocked();
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
LoadAdmissionController::CanAdmitImmediatelyLocked(
    const LoadAdmissionPriority priority,
    const LoadAdmissionRequest request) const {
    if (!CanAcquireCapacityLocked(request)) {
        return false;
    }
    if (priority == LoadAdmissionPriority::High) {
        return high_pending_.empty();
    }
    return high_pending_.empty() && low_pending_.empty();
}

void
LoadAdmissionController::EnqueuePendingLocked(
    PendingQueue& prepared, const LoadAdmissionPriority priority) {
    auto& queue =
        priority == LoadAdmissionPriority::High ? high_pending_ : low_pending_;
    const auto position = prepared.begin();
    const auto& pending = *position;
    pending->queued_at = Clock::now();
    pending->priority = priority;
    queue.splice(queue.end(), prepared, position);
    pending->queue = &queue;
    pending->queue_position = position;
}

void
LoadAdmissionController::ReserveLocked(
    const LoadAdmissionRequest request) noexcept {
    inflight_bytes_ += request.transient_bytes;
    inflight_slots_ += request.slots;
}

void
LoadAdmissionController::MarkAdmittedLocked(PendingAdmission& pending) {
    pending.state = PendingAdmission::State::Admitted;
    ReserveLocked(pending.request);
}

LoadAdmissionController::PendingResolution
LoadAdmissionController::TakeAdmittedLocked() {
    PendingResolution resolution;

    const auto admit_queue = [this, &resolution](auto& queue) {
        while (!queue.empty()) {
            auto& pending = *queue.front();
            if (!CanAcquireCapacityLocked(pending.request)) {
                break;
            }
            pending.queue = nullptr;
            MarkAdmittedLocked(pending);
            resolution.admitted.splice(
                resolution.admitted.end(), queue, queue.begin());
        }
    };
    admit_queue(high_pending_);
    if (high_pending_.empty()) {
        admit_queue(low_pending_);
    }
    if (!resolution.admitted.empty()) {
        // Timestamp the batch decision under mu_, excluding notification and
        // coroutine resumption. Empty releases do not read the clock.
        resolution.admitted_at = Clock::now();
    }
    return resolution;
}

void
LoadAdmissionController::FulfillAdmission(
    std::shared_ptr<PendingAdmission> pending) {
    if (pending->is_blocking_waiter) {
        // AcquireUntil owns and clears the callback after its wait completes.
        pending->ready.post();
        return;
    }
    pending->cancellation_callback.reset();
    auto lease = LoadAdmissionLease(this, pending->request);
    pending->promise.trySetValue(std::move(lease));
}

void
LoadAdmissionController::ResolvePending(PendingResolution resolution) {
    for (auto& pending : resolution.admitted) {
        ObserveQueueWait(*pending, resolution.admitted_at);
        FulfillAdmission(std::move(pending));
    }
}

void
LoadAdmissionController::ObserveQueueWait(const PendingAdmission& pending,
                                          const Clock::time_point finished_at) {
    using namespace milvus::monitor;
    const bool high = pending.priority == LoadAdmissionPriority::High;
    const bool admitted = pending.state == PendingAdmission::State::Admitted;
    auto& histogram =
        high ? (admitted
                    ? internal_load_admission_queue_wait_seconds_high_admitted
                    : internal_load_admission_queue_wait_seconds_high_cancelled)
             : (admitted
                    ? internal_load_admission_queue_wait_seconds_low_admitted
                    : internal_load_admission_queue_wait_seconds_low_cancelled);
    histogram.Observe(
        std::chrono::duration<double>(finished_at - pending.queued_at).count());
}

void
LoadAdmissionController::CancelPending(
    std::shared_ptr<PendingAdmission> pending) {
    PendingResolution resolution;
    PendingQueue removed;
    Clock::time_point cancelled_at{};
    bool cancelled = false;
    {
        std::lock_guard lock(mu_);
        if (pending->state == PendingAdmission::State::Pending) {
            pending->state = PendingAdmission::State::Cancelled;
            if (pending->queue != nullptr) {
                cancelled_at = Clock::now();
                removed.splice(
                    removed.end(), *pending->queue, pending->queue_position);
                pending->queue = nullptr;
            }
            cancelled = true;
            resolution = TakeAdmittedLocked();
        }
    }
    if (cancelled) {
        // Cancellation before enqueue is not a queue-wait sample.
        if (!removed.empty()) {
            ObserveQueueWait(*pending, cancelled_at);
        }
        if (pending->is_blocking_waiter) {
            pending->ready.post();
        } else {
            pending->promise.trySetException(folly::OperationCancelled{});
        }
        ResolvePending(std::move(resolution));
    }
}

}  // namespace milvus::storage
