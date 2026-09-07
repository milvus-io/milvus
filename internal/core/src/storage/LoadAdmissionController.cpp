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
    auto pending = std::make_shared<PendingAdmission>();
    pending->promise = std::move(promise);
    pending->request = request;

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

    bool admitted = false;
    {
        std::lock_guard lock(mu_);
        if (pending->state != PendingAdmission::State::Cancelled) {
            if (CanAdmitImmediatelyLocked(priority, request)) {
                MarkAdmittedLocked(pending);
                admitted = true;
            } else {
                EnqueuePendingLocked(pending, priority);
            }
        }
    }

    if (admitted) {
        FulfillAdmission(std::move(pending));
    }
    return future;
}

void
LoadAdmissionController::Acquire(const LoadAdmissionRequest request,
                                 const LoadAdmissionPriority priority) {
    auto pending = std::make_shared<PendingAdmission>();
    pending->request = request;
    pending->is_blocking_waiter = true;

    std::unique_lock lock(mu_);
    if (CanAdmitImmediatelyLocked(priority, request)) {
        MarkAdmittedLocked(pending);
    } else {
        EnqueuePendingLocked(pending, priority);
        cv_.wait(lock, [&pending] {
            return pending->state != PendingAdmission::State::Pending;
        });
    }
    AssertInfo(pending->state == PendingAdmission::State::Admitted,
               "Blocking load admission was not admitted");
}

bool
LoadAdmissionController::AcquireUntil(
    const LoadAdmissionRequest request,
    const LoadAdmissionPriority priority,
    const folly::CancellationToken& cancellation_token) {
    auto pending = std::make_shared<PendingAdmission>();
    pending->request = request;
    pending->is_blocking_waiter = true;
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

    bool acquired = false;
    {
        std::unique_lock lock(mu_);
        if (pending->state != PendingAdmission::State::Cancelled) {
            if (CanAdmitImmediatelyLocked(priority, request)) {
                MarkAdmittedLocked(pending);
            } else {
                EnqueuePendingLocked(pending, priority);
                cv_.wait(lock, [&pending] {
                    return pending->state != PendingAdmission::State::Pending;
                });
            }
        }
        acquired = pending->state == PendingAdmission::State::Admitted;
    }

    pending->cancellation_callback.reset();
    return acquired;
}

bool
LoadAdmissionController::TryAcquire(const LoadAdmissionRequest request,
                                    const LoadAdmissionPriority priority) {
    std::lock_guard lock(mu_);
    if (CanAdmitImmediatelyLocked(priority, request)) {
        inflight_bytes_ += request.transient_bytes;
        inflight_slots_ += request.slots;
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
    cv_.notify_all();
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
    cv_.notify_all();
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
    cv_.notify_all();
}

void
LoadAdmissionController::NotifyCapacityUpdated() {
    PendingResolution resolution;
    {
        std::lock_guard lock(mu_);
        resolution = TakeAdmittedLocked();
    }
    ResolvePending(std::move(resolution));
    cv_.notify_all();
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
    const std::shared_ptr<PendingAdmission>& pending,
    const LoadAdmissionPriority priority) {
    auto& queue =
        priority == LoadAdmissionPriority::High ? high_pending_ : low_pending_;
    const auto position = queue.insert(queue.end(), pending);
    pending->queue = &queue;
    pending->queue_position = position;
}

void
LoadAdmissionController::MarkAdmittedLocked(
    const std::shared_ptr<PendingAdmission>& pending) {
    pending->state = PendingAdmission::State::Admitted;
    inflight_bytes_ += pending->request.transient_bytes;
    inflight_slots_ += pending->request.slots;
}

LoadAdmissionController::PendingResolution
LoadAdmissionController::TakeAdmittedLocked() {
    PendingResolution resolution;

    const auto admit_queue = [this, &resolution](auto& queue) {
        while (!queue.empty()) {
            const auto& pending = queue.front();
            if (!CanAcquireCapacityLocked(pending->request)) {
                break;
            }
            const auto admitted = pending;
            resolution.admitted.splice(
                resolution.admitted.end(), queue, queue.begin());
            admitted->queue = nullptr;
            MarkAdmittedLocked(admitted);
        }
    };
    admit_queue(high_pending_);
    if (high_pending_.empty()) {
        admit_queue(low_pending_);
    }
    return resolution;
}

void
LoadAdmissionController::FulfillAdmission(
    std::shared_ptr<PendingAdmission> pending) {
    if (pending->is_blocking_waiter) {
        // AcquireUntil owns and clears the callback after its wait completes.
        return;
    }
    pending->cancellation_callback.reset();
    auto lease = LoadAdmissionLease(this, pending->request);
    pending->promise.trySetValue(std::move(lease));
}

void
LoadAdmissionController::ResolvePending(PendingResolution resolution) {
    for (auto& pending : resolution.admitted) {
        FulfillAdmission(std::move(pending));
    }
}

void
LoadAdmissionController::CancelPending(
    std::shared_ptr<PendingAdmission> pending) {
    PendingResolution resolution;
    bool cancelled = false;
    {
        std::lock_guard lock(mu_);
        if (pending->state == PendingAdmission::State::Pending) {
            pending->state = PendingAdmission::State::Cancelled;
            if (pending->queue != nullptr) {
                pending->queue->erase(pending->queue_position);
                pending->queue = nullptr;
            }
            cancelled = true;
            resolution = TakeAdmittedLocked();
        }
    }
    if (cancelled) {
        if (!pending->is_blocking_waiter) {
            pending->promise.trySetException(folly::OperationCancelled{});
        }
        ResolvePending(std::move(resolution));
        cv_.notify_all();
    }
}

}  // namespace milvus::storage
