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

#include "exec/expression/ExprCacheFlight.h"

#include <algorithm>
#include <utility>

namespace milvus::exec {
namespace {
thread_local size_t expr_flight_compute_depth = 0;
constexpr auto kCancellationPoll = std::chrono::milliseconds(5);
}  // namespace

struct ExprCacheFlightCoordinator::Counters {
    std::atomic<size_t> flights{0};
    std::atomic<size_t> waiters{0};
};

struct ExprCacheFlightCoordinator::Flight {
    struct Charge {
        explicit Charge(std::shared_ptr<Counters> counters)
            : counters(std::move(counters)) {
            this->counters->flights.fetch_add(1, std::memory_order_relaxed);
        }
        ~Charge() {
            counters->flights.fetch_sub(1, std::memory_order_relaxed);
        }
        std::shared_ptr<Counters> counters;
    };
    Flight(const Key& key,
           uint64_t policy_epoch,
           std::shared_ptr<Counters> counters)
        : charge(std::move(counters)), key(key), policy_epoch(policy_epoch) {
    }

    // Last member destroyed: detached, reader-held flights remain counted.
    Charge charge;
    const Key key;
    const uint64_t policy_epoch;
    std::atomic<size_t> waiters{0};
    std::mutex mutex;
    std::condition_variable cv;
    enum class State { Computing, Ready, Aborted };
    State state{State::Computing};
    std::shared_ptr<const Bitmaps> bitmaps;
    ExprResCacheManager::AdmissionTicket admission;
    bool admission_closed{false};
};

ExprCacheFlightCoordinator::ExprCacheFlightCoordinator()
    : counters_(std::make_shared<Counters>()) {
}
ExprCacheFlightCoordinator::~ExprCacheFlightCoordinator() = default;

ExprCacheFlightCoordinator::ComputeScope::ComputeScope() {
    ++expr_flight_compute_depth;
}
ExprCacheFlightCoordinator::ComputeScope::~ComputeScope() {
    --expr_flight_compute_depth;
}
bool
ExprCacheFlightCoordinator::IsComputing() {
    return expr_flight_compute_depth != 0;
}

bool
ExprCacheFlightCoordinator::Configure(const ExprCacheFlightConfig& config) {
    // Bound the duration before constructing a chrono deadline, including for
    // direct C++ callers. Invalid settings disable only this optimization.
    const bool valid = config.max_wait_ms > 0 && config.max_wait_ms <= 60000 &&
                       config.max_flights > 0 && config.max_waiters > 0 &&
                       config.max_waiters_per_key > 0;
    std::lock_guard lock(map_mutex_);
    config_ = config;
    policy_epoch_.fetch_add(1, std::memory_order_release);
    enabled_.store(valid && config.enabled, std::memory_order_release);
    // No map clearing or cache backend lock. Waiters notice the policy change
    // at their next timed check; existing leaders retain ownership and clean up.
    return valid;
}

ExprCacheFlightCoordinator::Stats
ExprCacheFlightCoordinator::GetStats() const {
    return {counters_->flights.load(std::memory_order_relaxed),
            counters_->waiters.load(std::memory_order_relaxed)};
}

size_t
ExprCacheFlightCoordinator::KeyHasher::operator()(
    const Key& key) const noexcept {
    auto hash = ExprResCacheManager::KeyHasher{}(key.cache_key);
    hash ^= std::hash<int64_t>{}(key.active_count) + (hash << 6) + (hash >> 2);
    return hash ^ (std::hash<uint64_t>{}(key.config_epoch) + (hash << 6) +
                   (hash >> 2));
}

ExprCacheFlightCoordinator::Handle::Handle(ExprCacheFlightCoordinator* owner,
                                           std::shared_ptr<Flight> flight,
                                           bool leader,
                                           int64_t max_wait_ms)
    : owner_(owner),
      flight_(std::move(flight)),
      leader_(leader),
      deadline_(std::chrono::steady_clock::now() +
                std::chrono::milliseconds(max_wait_ms)) {
}
ExprCacheFlightCoordinator::Handle::Handle(Handle&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      flight_(std::move(other.flight_)),
      leader_(other.leader_),
      deadline_(other.deadline_) {
}
ExprCacheFlightCoordinator::Handle&
ExprCacheFlightCoordinator::Handle::operator=(Handle&& other) noexcept {
    if (this != &other) {
        Reset();
        owner_ = std::exchange(other.owner_, nullptr);
        flight_ = std::move(other.flight_);
        leader_ = other.leader_;
        deadline_ = other.deadline_;
    }
    return *this;
}
ExprCacheFlightCoordinator::Handle::~Handle() {
    Reset();
}
void
ExprCacheFlightCoordinator::Handle::Reset() noexcept {
    if (owner_) {
        owner_->Finish(*this);
        owner_ = nullptr;
        // Destruction of results and key storage is outside map/state locks.
        flight_.reset();
    }
}

ExprCacheFlightCoordinator::Handle
ExprCacheFlightCoordinator::TryJoin(const Key& key) {
    if (!IsEnabled() || IsComputing() || key.active_count < 0 ||
        !key.cache_key.generation || key.cache_key.generation->IsRetired()) {
        return {};
    }
    std::lock_guard lock(map_mutex_);
    if (!IsEnabled())
        return {};
    const auto policy = policy_epoch_.load(std::memory_order_acquire);
    auto it = flights_.find(key);
    if (it != flights_.end()) {
        auto& flight = it->second;
        if (flight->policy_epoch != policy ||
            counters_->waiters.load(std::memory_order_relaxed) >=
                config_.max_waiters ||
            flight->waiters.load(std::memory_order_relaxed) >=
                config_.max_waiters_per_key) {
            return {};
        }
        counters_->waiters.fetch_add(1, std::memory_order_relaxed);
        flight->waiters.fetch_add(1, std::memory_order_relaxed);
        return Handle(this, flight, false, config_.max_wait_ms);
    }
    if (counters_->flights.load(std::memory_order_relaxed) >=
        config_.max_flights) {
        return {};
    }
    auto flight = std::make_shared<Flight>(key, policy, counters_);
    flights_.emplace(key, flight);
    return Handle(this, std::move(flight), true, config_.max_wait_ms);
}

void
ExprCacheFlightCoordinator::OfferAdmission(
    const Handle& handle, const ExprResCacheManager::AdmissionTicket& ticket) {
    if (!handle || !ticket.admitted ||
        ticket.config_epoch != handle.flight_->key.config_epoch)
        return;
    std::lock_guard lock(handle.flight_->mutex);
    if (!handle.flight_->admission_closed)
        handle.flight_->admission = ticket;
}

ExprResCacheManager::AdmissionTicket
ExprCacheFlightCoordinator::TakeAdmission(const Handle& handle) {
    std::lock_guard lock(handle.flight_->mutex);
    handle.flight_->admission_closed = true;
    return handle.flight_->admission;
}

ExprCacheFlightCoordinator::WaitResult
ExprCacheFlightCoordinator::Wait(const Handle& handle,
                                 const folly::CancellationToken* cancel,
                                 const std::function<bool()>& cache_current) {
    const auto& flight = handle.flight_;
    for (;;) {
        if (cancel && cancel->isCancellationRequested())
            return {WaitStatus::Cancelled, {}};
        if (!IsEnabled() ||
            policy_epoch_.load(std::memory_order_acquire) !=
                flight->policy_epoch ||
            !cache_current())
            return {};
        std::unique_lock lock(flight->mutex);
        if (flight->state == Flight::State::Ready)
            return {WaitStatus::Ready, flight->bitmaps};
        if (flight->state == Flight::State::Aborted)
            return {};
        const auto now = std::chrono::steady_clock::now();
        if (now >= handle.deadline_)
            return {};
        // The final deadline never moves, even after spurious wakeups.
        flight->cv.wait_until(
            lock, std::min(handle.deadline_, now + kCancellationPoll), [&] {
                return flight->state != Flight::State::Computing;
            });
    }
}

void
ExprCacheFlightCoordinator::Complete(const Handle& handle,
                                     std::shared_ptr<const Bitmaps> bitmaps) {
    const auto& flight = handle.flight_;
    if (!bitmaps ||
        bitmaps->result.size() !=
            static_cast<size_t>(flight->key.active_count) ||
        bitmaps->valid.size() !=
            static_cast<size_t>(flight->key.active_count)) {
        Abort(handle);
        return;
    }
    {
        std::lock_guard lock(flight->mutex);
        if (flight->state != Flight::State::Computing)
            return;
        flight->bitmaps = std::move(bitmaps);
        flight->state = Flight::State::Ready;
    }
    flight->cv.notify_all();
}

void
ExprCacheFlightCoordinator::Abort(const Handle& handle) noexcept {
    const auto& flight = handle.flight_;
    {
        std::lock_guard lock(flight->mutex);
        if (flight->state != Flight::State::Computing)
            return;
        flight->state = Flight::State::Aborted;
    }
    flight->cv.notify_all();
}

void
ExprCacheFlightCoordinator::Finish(Handle& handle) noexcept {
    if (!handle.leader_) {
        handle.flight_->waiters.fetch_sub(1, std::memory_order_relaxed);
        counters_->waiters.fetch_sub(1, std::memory_order_relaxed);
        return;
    }
    Abort(handle);
    std::lock_guard lock(map_mutex_);
    const auto it = flights_.find(handle.flight_->key);
    if (it != flights_.end() && it->second == handle.flight_)
        flights_.erase(it);
}

}  // namespace milvus::exec
