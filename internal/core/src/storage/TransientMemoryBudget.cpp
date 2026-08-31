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

#include "storage/TransientMemoryBudget.h"

#include <utility>

#include "common/EasyAssert.h"
#include "folly/OperationCancelled.h"
#include "storage/LoadOverheadController.h"

namespace milvus::storage {

TransientBudgetLease::TransientBudgetLease(
    TransientBudgetLease&& other) noexcept
    : budget_(std::exchange(other.budget_, nullptr)),
      bytes_(std::exchange(other.bytes_, 0)) {
}

TransientBudgetLease&
TransientBudgetLease::operator=(TransientBudgetLease&& other) noexcept {
    if (this != &other) {
        Release();
        budget_ = std::exchange(other.budget_, nullptr);
        bytes_ = std::exchange(other.bytes_, 0);
    }
    return *this;
}

TransientBudgetLease::~TransientBudgetLease() {
    Release();
}

void
TransientBudgetLease::Release() {
    if (budget_ == nullptr || bytes_ == 0) {
        return;
    }
    auto* const budget = std::exchange(budget_, nullptr);
    const auto bytes = std::exchange(bytes_, 0);
    budget->Release(bytes);
}

TransientMemoryBudget&
TransientMemoryBudget::GetLoadTransientBudget() {
    static TransientMemoryBudget instance;
    return instance;
}

void
TransientMemoryBudget::SetLoadTransientBudgetBytes(const size_t bytes) {
    GetLoadTransientBudget().SetCapacityBytes(bytes);
}

folly::coro::Future<TransientBudgetLease>
TransientMemoryBudget::AcquireAsync(
    const size_t bytes,
    const TransientBudgetPriority priority,
    const folly::CancellationToken& cancellation_token) {
    auto [promise, future] =
        folly::coro::makePromiseContract<TransientBudgetLease>();
    auto pending = std::make_shared<PendingAdmission>();
    pending->promise = std::move(promise);
    pending->bytes = bytes;

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
            if (CanAdmitImmediatelyLocked(priority, bytes)) {
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
TransientMemoryBudget::Acquire(const size_t bytes,
                               const TransientBudgetPriority priority) {
    auto pending = std::make_shared<PendingAdmission>();
    pending->bytes = bytes;
    pending->is_blocking_waiter = true;

    std::unique_lock lock(mu_);
    if (CanAdmitImmediatelyLocked(priority, bytes)) {
        MarkAdmittedLocked(pending);
    } else {
        EnqueuePendingLocked(pending, priority);
        cv_.wait(lock, [&pending] {
            return pending->state != PendingAdmission::State::Pending;
        });
    }
    AssertInfo(pending->state == PendingAdmission::State::Admitted,
               "Blocking budget admission was not admitted");
}

bool
TransientMemoryBudget::AcquireUntil(
    const size_t bytes,
    const TransientBudgetPriority priority,
    const folly::CancellationToken& cancellation_token) {
    auto pending = std::make_shared<PendingAdmission>();
    pending->bytes = bytes;
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
            if (CanAdmitImmediatelyLocked(priority, bytes)) {
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
TransientMemoryBudget::TryAcquire(const size_t bytes,
                                  const TransientBudgetPriority priority) {
    std::lock_guard lock(mu_);
    if (CanAdmitImmediatelyLocked(priority, bytes)) {
        inflight_bytes_ += bytes;
        return true;
    }
    return false;
}

void
TransientMemoryBudget::Release(const size_t bytes) {
    PendingResolution resolution;
    {
        std::lock_guard lock(mu_);
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
TransientMemoryBudget::CapacityBytes() const {
    std::lock_guard lock(mu_);
    return CapacityBytesLocked();
}

void
TransientMemoryBudget::SetCapacityBytes(const size_t bytes) {
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

void
TransientMemoryBudget::NotifyCapacityUpdated() {
    PendingResolution resolution;
    {
        std::lock_guard lock(mu_);
        resolution = TakeAdmittedLocked();
    }
    ResolvePending(std::move(resolution));
    cv_.notify_all();
}

size_t
TransientMemoryBudget::CapacityBytesLocked() const {
    return capacity_bytes_;
}

bool
TransientMemoryBudget::CanAcquireCapacityLocked(const size_t bytes) const {
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
TransientMemoryBudget::CanAdmitImmediatelyLocked(
    const TransientBudgetPriority priority, const size_t bytes) const {
    if (!CanAcquireCapacityLocked(bytes)) {
        return false;
    }
    if (priority == TransientBudgetPriority::High) {
        return high_pending_.empty();
    }
    return high_pending_.empty() && low_pending_.empty();
}

void
TransientMemoryBudget::EnqueuePendingLocked(
    const std::shared_ptr<PendingAdmission>& pending,
    const TransientBudgetPriority priority) {
    auto& queue = priority == TransientBudgetPriority::High ? high_pending_
                                                            : low_pending_;
    const auto position = queue.insert(queue.end(), pending);
    pending->queue = &queue;
    pending->queue_position = position;
}

void
TransientMemoryBudget::MarkAdmittedLocked(
    const std::shared_ptr<PendingAdmission>& pending) {
    pending->state = PendingAdmission::State::Admitted;
    inflight_bytes_ += pending->bytes;
}

TransientMemoryBudget::PendingResolution
TransientMemoryBudget::TakeAdmittedLocked() {
    PendingResolution resolution;

    const auto admit_queue = [this, &resolution](auto& queue) {
        while (!queue.empty()) {
            const auto& pending = queue.front();
            if (!CanAcquireCapacityLocked(pending->bytes)) {
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
TransientMemoryBudget::FulfillAdmission(
    std::shared_ptr<PendingAdmission> pending) {
    if (pending->is_blocking_waiter) {
        // AcquireUntil owns and clears the callback after its wait completes.
        return;
    }
    pending->cancellation_callback.reset();
    auto lease = TransientBudgetLease(this, pending->bytes);
    pending->promise.trySetValue(std::move(lease));
}

void
TransientMemoryBudget::ResolvePending(PendingResolution resolution) {
    for (auto& pending : resolution.admitted) {
        FulfillAdmission(std::move(pending));
    }
}

void
TransientMemoryBudget::CancelPending(
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
