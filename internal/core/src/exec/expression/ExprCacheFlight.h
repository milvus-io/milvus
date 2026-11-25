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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <folly/CancellationToken.h>

#include "exec/expression/ExprCache.h"

namespace milvus::exec {

struct ExprCacheFlightConfig {
    bool enabled{false};
    int64_t max_wait_ms{1000};
    size_t max_flights{1024};
    size_t max_waiters{64};
    size_t max_waiters_per_key{8};
};

// Passing these options explicitly opts a call site into the independent,
// full-row bitmap contract. Never use for passive/batched miss capture.
struct ExprCacheFlightOptions {
    const folly::CancellationToken* cancel{nullptr};
};

class ExprCacheFlightCoordinator {
    struct Flight;
    struct Counters;

 public:
    struct Key {
        ExprResCacheManager::Key cache_key;
        int64_t active_count;
        uint64_t config_epoch;

        bool
        operator==(const Key& other) const {
            return cache_key == other.cache_key &&
                   active_count == other.active_count &&
                   config_epoch == other.config_epoch;
        }
    };

    struct Bitmaps {
        Bitmaps(ExprResCacheManager::MaterializationLease lease,
                const TargetBitmap& result,
                const TargetBitmap& valid)
            : lease(std::move(lease)),
              result(result.view()),
              valid(valid.view()) {
        }

        // Destroy both buffers before returning their reservation.
        ExprResCacheManager::MaterializationLease lease;
        TargetBitmap result;
        TargetBitmap valid;
    };

    // The coordinator must outlive its handles. The cache manager owns the
    // process-wide coordinator; tests may use a scoped coordinator.
    class Handle {
     public:
        Handle() = default;
        Handle(const Handle&) = delete;
        Handle&
        operator=(const Handle&) = delete;
        Handle(Handle&& other) noexcept;
        Handle&
        operator=(Handle&& other) noexcept;
        ~Handle();

        explicit operator bool() const {
            return flight_ != nullptr;
        }
        bool
        IsLeader() const {
            return leader_;
        }
        void
        Reset() noexcept;

     private:
        friend class ExprCacheFlightCoordinator;
        friend class ExprCacheFlightTestPeer;
        Handle(ExprCacheFlightCoordinator* owner,
               std::shared_ptr<Flight> flight,
               bool leader,
               int64_t max_wait_ms);

        ExprCacheFlightCoordinator* owner_{nullptr};
        std::shared_ptr<Flight> flight_;
        bool leader_{false};
        std::chrono::steady_clock::time_point deadline_;
    };

    enum class WaitStatus { Ready, Bypass, Cancelled };
    struct WaitResult {
        WaitStatus status{WaitStatus::Bypass};
        std::shared_ptr<const Bitmaps> bitmaps;
    };
    struct Stats {
        size_t flights;
        size_t waiters;
    };

    // Prevent a synchronous producer from joining another flight recursively.
    class ComputeScope {
     public:
        ComputeScope();
        ~ComputeScope();
        ComputeScope(const ComputeScope&) = delete;
        ComputeScope&
        operator=(const ComputeScope&) = delete;
    };

    ExprCacheFlightCoordinator();
    ~ExprCacheFlightCoordinator();
    bool
    Configure(const ExprCacheFlightConfig& config);
    bool
    IsEnabled() const {
        return enabled_.load(std::memory_order_acquire);
    }
    static bool
    IsComputing();
    Stats
    GetStats() const;

    Handle
    TryJoin(const Key& key);
    // A snapshot of live follower handles, not a guarantee they will stay.
    // If false, the caller must still Abort to release racing late joiners.
    bool
    HasWaiters(const Handle& handle) const noexcept;
    void
    OfferAdmission(const Handle& handle,
                   const ExprResCacheManager::AdmissionTicket& ticket);
    ExprResCacheManager::AdmissionTicket
    TakeAdmission(const Handle& handle);
    WaitResult
    Wait(const Handle& handle,
         const folly::CancellationToken* cancel,
         const std::function<bool()>& cache_current);
    void
    Complete(const Handle& handle, std::shared_ptr<const Bitmaps> bitmaps);
    void
    Abort(const Handle& handle) noexcept;

 private:
    friend class ExprCacheFlightTestPeer;
    struct KeyHasher {
        size_t
        operator()(const Key& key) const noexcept;
    };
    void
    Finish(Handle& handle) noexcept;

    std::shared_ptr<Counters> counters_;
    std::atomic<bool> enabled_{false};
    std::atomic<uint64_t> policy_epoch_{0};
    mutable std::mutex map_mutex_;
    ExprCacheFlightConfig config_;
    std::unordered_map<Key, std::shared_ptr<Flight>, KeyHasher> flights_;
};

}  // namespace milvus::exec
