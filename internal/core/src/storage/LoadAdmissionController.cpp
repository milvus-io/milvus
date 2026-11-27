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

#include <algorithm>
#include <utility>

#include "common/EasyAssert.h"
#include "folly/OperationCancelled.h"
#include "monitor/Monitor.h"
#include "storage/LoadOverheadController.h"
#include "storage/LoadConfig.h"

namespace milvus::storage {

namespace {

bool
UpdateLoadOverheadControllers(const size_t slots) {
    if (!milvus::storage::LoadAsyncReadEnabled()) {
        return true;
    }
    // All current Group bindings provide max_runtime_unit, so both policy
    // updates should succeed. If that invariant is violated and only one
    // update succeeds, SetCapacitySlots's ordering keeps admission
    // conservative: expansion stops before admitting more work, while
    // shrinking restricts admission before updating the policies.
    const auto memory_updated =
        storage::LoadMemoryOverheadController::GetInstance()
            .UpdateConcurrencyLimit(slots);
    const auto file_updated = storage::LoadFileOverheadController::GetInstance()
                                  .UpdateConcurrencyLimit(slots);
    if (memory_updated != file_updated) {
        LOG_ERROR(
            "Load overhead controllers were updated partially, "
            "memory_updated:{}, file_updated:{}, slots:{}",
            memory_updated,
            file_updated,
            slots);
    }
    return memory_updated && file_updated;
}

}  // namespace

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
    const folly::CancellationToken& cancellation_token,
    uint64_t sequence_id) {
    if (sequence_id == 0) {
        sequence_id = std::numeric_limits<uint64_t>::max();
    }
    auto [promise, future] =
        folly::coro::makePromiseContract<LoadAdmissionLease>();
    PendingResolution resolution;
    bool admitted = false;
    bool cancelled = false;
    {
        std::lock_guard lock(mu_);
        cancelled = cancellation_token.isCancellationRequested();
        if (!cancelled &&
            CanAdmitImmediatelyLocked(priority, request, sequence_id)) {
            ReserveLocked(request);
            resolution = TakeAdmittedLocked();
            admitted = true;
        }
    }
    if (cancelled) {
        promise.trySetException(folly::OperationCancelled{});
        return std::move(future);
    }
    if (admitted) {
        promise.trySetValue(LoadAdmissionLease(this, request));
        ResolvePending(std::move(resolution));
        return std::move(future);
    }

    // The returned future has not escaped yet, so only the explicit token
    // can cancel the fast path. Register both sources before queueing below.
    auto pending = std::make_shared<PendingAdmission>();
    pending->promise = std::move(promise);
    pending->request = request;
    PendingQueue prepared{{sequence_id, pending}};

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
            if (CanAdmitImmediatelyLocked(priority, request, sequence_id)) {
                MarkAdmittedLocked(*pending);
                resolution = TakeAdmittedLocked();
                admitted = true;
            } else {
                EnqueuePendingLocked(prepared, priority);
            }
        }
    }

    if (admitted) {
        FulfillAdmission(std::move(pending));
        ResolvePending(std::move(resolution));
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
    PendingResolution resolution;
    bool admitted = false;
    {
        std::lock_guard lock(mu_);
        if (cancellation_token.isCancellationRequested()) {
            return false;
        }
        if (CanAdmitImmediatelyLocked(priority, request)) {
            ReserveLocked(request);
            resolution = TakeAdmittedLocked();
            admitted = true;
        }
    }

    if (admitted) {
        ResolvePending(std::move(resolution));
        return true;
    }

    auto pending = std::make_shared<PendingAdmission>();
    pending->request = request;
    pending->is_blocking_waiter = true;
    PendingQueue prepared{{std::numeric_limits<uint64_t>::max(), pending}};
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

    {
        std::lock_guard lock(mu_);
        if (pending->state != PendingAdmission::State::Cancelled) {
            if (CanAdmitImmediatelyLocked(priority, request)) {
                MarkAdmittedLocked(*pending);
                resolution = TakeAdmittedLocked();
                admitted = true;
            } else {
                EnqueuePendingLocked(prepared, priority);
            }
        }
    }

    if (admitted) {
        FulfillAdmission(pending);
        ResolvePending(std::move(resolution));
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
    PendingResolution resolution;
    {
        std::lock_guard lock(mu_);
        if (!CanAdmitImmediatelyLocked(priority, request)) {
            return false;
        }
        ReserveLocked(request);
        resolution = TakeAdmittedLocked();
    }
    ResolvePending(std::move(resolution));
    return true;
}

void
LoadAdmissionController::Release(const LoadAdmissionRequest request) {
    PendingResolution resolution;
    bool refresh_slot_policy;
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
        refresh_slot_policy =
            slot_policy_update_pending_ && inflight_slots_ <= capacity_slots_;
    }
    if (refresh_slot_policy) {
        RefreshSlotPolicy();
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
        const bool async = milvus::storage::LoadAsyncReadEnabled();
        if (async && expanding &&
            !overhead_controller.UpdateBudgetBytes(bytes)) {
            return;
        }
        {
            std::lock_guard lock(mu_);
            capacity_bytes_ = bytes;
            resolution = TakeAdmittedLocked();
        }
        if (async && !expanding) {
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
        std::lock_guard update_lock(capacity_update_mutex_);
        const auto old_capacity = CapacitySlots();
        const bool expanding =
            old_capacity != 0 && (slots == 0 || slots > old_capacity);
        size_t effective_slots;
        {
            std::lock_guard lock(mu_);
            effective_slots = slots == 0 ? 0 : std::max(slots, inflight_slots_);
        }
        if (expanding && !UpdateLoadOverheadControllers(effective_slots)) {
            return;
        }
        {
            std::lock_guard lock(mu_);
            capacity_slots_ = slots;
            // For a reduction from unlimited admission, sample inflight only
            // after restricting new work under the same lock.
            if (!expanding) {
                effective_slots =
                    slots == 0 ? 0 : std::max(slots, inflight_slots_);
            }
            slot_policy_update_pending_ = effective_slots > slots;
            resolution = TakeAdmittedLocked();
        }
        if (!expanding && !UpdateLoadOverheadControllers(effective_slots)) {
            std::lock_guard lock(mu_);
            slot_policy_update_pending_ = true;
        }
    }
    // Work may have drained between the snapshot and policy publication.
    RefreshSlotPolicy();
    ResolvePending(std::move(resolution));
}

void
LoadAdmissionController::RefreshSlotPolicy() {
    std::lock_guard update_lock(capacity_update_mutex_);
    size_t slots;
    {
        std::lock_guard lock(mu_);
        if (!slot_policy_update_pending_ || inflight_slots_ > capacity_slots_) {
            return;
        }
        slots = capacity_slots_;
    }
    if (UpdateLoadOverheadControllers(slots)) {
        std::lock_guard lock(mu_);
        slot_policy_update_pending_ = false;
    }
}

void
LoadAdmissionController::SetSchedulingWeights(std::array<size_t, 3> weights) {
    std::lock_guard lock(mu_);
    weights_ =
        std::all_of(
            weights.begin(), weights.end(), [](size_t w) { return w > 0; })
            ? weights
            : std::array<size_t, 3>{32, 8, 1};
    next_priority_ = 0;
    remaining_ = weights_[0];
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
        size_t urgent_pending;
        size_t high_pending;
        size_t low_pending;
        double urgent_oldest_wait;
        double high_oldest_wait;
        double low_oldest_wait;
    } snapshot;
    {
        std::lock_guard lock(mu_);
        const auto now = Clock::now();
        const auto oldest_wait = [now](const PendingQueue& queue) {
            auto oldest = now;
            for (const auto& [sequence, pending] : queue) {
                oldest = std::min(oldest, pending->queued_at);
            }
            return std::chrono::duration<double>(now - oldest).count();
        };
        snapshot = {inflight_bytes_,
                    capacity_bytes_,
                    inflight_slots_,
                    capacity_slots_,
                    pending_[0].size(),
                    pending_[1].size(),
                    pending_[2].size(),
                    oldest_wait(pending_[0]),
                    oldest_wait(pending_[1]),
                    oldest_wait(pending_[2])};
    }
    using namespace milvus::monitor;
    internal_load_admission_reserved_bytes.Set(snapshot.reserved_bytes);
    internal_load_admission_capacity_bytes.Set(snapshot.capacity_bytes);
    internal_load_admission_reserved_slots.Set(snapshot.reserved_slots);
    internal_load_admission_capacity_slots.Set(snapshot.capacity_slots);
    internal_load_admission_pending_requests_urgent.Set(
        snapshot.urgent_pending);
    internal_load_admission_oldest_wait_seconds_urgent.Set(
        snapshot.urgent_oldest_wait);
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

void
LoadAdmissionController::ResetRoundIfIdleLocked() {
    if (pending_[0].empty() && pending_[1].empty() && pending_[2].empty()) {
        next_priority_ = 0;
        remaining_ = weights_[0];
    }
}

size_t
LoadAdmissionController::SelectPriorityLocked(size_t candidate) {
    for (size_t i = 0; i < pending_.size() + 1; ++i) {
        if (remaining_ != 0 && (!pending_[next_priority_].empty() ||
                                next_priority_ == candidate)) {
            return next_priority_;
        }
        next_priority_ = (next_priority_ + 1) % pending_.size();
        remaining_ = weights_[next_priority_];
    }
    return pending_.size();
}

bool
LoadAdmissionController::CanAdmitImmediatelyLocked(
    const LoadAdmissionPriority priority,
    const LoadAdmissionRequest request,
    const uint64_t sequence_id) {
    if (!CanAcquireCapacityLocked(request)) {
        return false;
    }
    ResetRoundIfIdleLocked();
    const auto index = static_cast<size_t>(priority);
    auto& queue = pending_[index];
    if (SelectPriorityLocked(index) != index ||
        (!queue.empty() && queue.begin()->first <= sequence_id)) {
        return false;
    }
    --remaining_;
    ResetRoundIfIdleLocked();
    return true;
}

void
LoadAdmissionController::EnqueuePendingLocked(
    PendingQueue& prepared, const LoadAdmissionPriority priority) {
    auto& queue = pending_[static_cast<size_t>(priority)];
    const auto position = prepared.begin();
    const auto pending = position->second;
    pending->queued_at = Clock::now();
    pending->priority = priority;
    const auto inserted = queue.insert(prepared.extract(position));
    pending->queue = &queue;
    pending->queue_position = inserted;
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

    while (true) {
        const auto priority = SelectPriorityLocked();
        if (priority == pending_.size()) {
            ResetRoundIfIdleLocked();
            break;
        }
        auto& queue = pending_[priority];
        auto& pending = *queue.begin()->second;
        // Keep the selected head's turn until capacity is released, increased,
        // or the waiter is cancelled. Do not spend quota on a failed admission.
        if (!CanAcquireCapacityLocked(pending.request)) {
            break;
        }
        --remaining_;
        pending.queue = nullptr;
        MarkAdmittedLocked(pending);
        auto node = queue.extract(queue.begin());
        node.key() = resolution.admitted.size();
        resolution.admitted.insert(std::move(node));
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
    for (auto& [sequence, pending] : resolution.admitted) {
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
        pending.priority == LoadAdmissionPriority::Urgent
            ? (admitted
                   ? internal_load_admission_queue_wait_seconds_urgent_admitted
                   : internal_load_admission_queue_wait_seconds_urgent_cancelled)
        : high
            ? (admitted
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
                removed.insert(
                    pending->queue->extract(pending->queue_position));
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
