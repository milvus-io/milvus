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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <vector>

#include "storage/ThreadPools.h"

namespace milvus::storage {

/// Default chunk size for ReadEntryStream (2 MB).
constexpr size_t kDefaultStreamChunkSize = 2 * 1024 * 1024;

/// Budget multiplier relative to thread pool size.
constexpr double kScalarIndexChunkBudgetMultiplier = 1.5;

/// A chunk downloaded from a V3 entry, carrying a sequence number for
/// reordering on the consumer side. `error` carries an exception captured in
/// the producer task so the consumer can rethrow instead of hanging on pop.
struct ChunkResult {
    size_t seq{0};
    size_t budget_bytes{0};
    std::vector<uint8_t> data;
    std::exception_ptr error = nullptr;
};

/// Byte budget for transient data that has been submitted for async work but
/// has not been consumed yet.
///
/// Usage:
///   - Call Acquire(bytes) to block until budget is available.
///   - Call TryAcquire(bytes) for non-blocking replenish in slide loops.
///   - Call Release(bytes) after the transient data has been consumed.
///   - Oversized requests are allowed to run exclusively to guarantee progress.
class TransientMemoryBudget {
 public:
    static TransientMemoryBudget&
    GetScalarIndexChunkBudget() {
        static TransientMemoryBudget instance(
            DefaultScalarIndexChunkBudgetBytes());
        return instance;
    }

    /// Block until enough budget is available. Safe to call when the calling
    /// thread has no inflight tasks (no risk of deadlock with channel pop).
    void
    Acquire(size_t bytes) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this, bytes] { return CanAcquireLocked(bytes); });
        inflight_bytes_ += bytes;
    }

    /// Try to claim budget. Returns true if under budget.
    /// Used in the slide loop where blocking could cause deadlock.
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
            if (bytes >= inflight_bytes_) {
                inflight_bytes_ = 0;
            } else {
                inflight_bytes_ -= bytes;
            }
        }
        cv_.notify_all();
    }

    size_t
    CapacityBytes() const {
        return capacity_bytes_;
    }

 private:
    explicit TransientMemoryBudget(size_t capacity_bytes)
        : capacity_bytes_(std::max<size_t>(capacity_bytes, 1)) {
    }

    static size_t
    DefaultScalarIndexChunkBudgetBytes() {
        auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::HIGH);
        auto capacity = static_cast<size_t>(pool.GetMaxThreadNum() *
                                            kScalarIndexChunkBudgetMultiplier) *
                        kDefaultStreamChunkSize;
        return std::max<size_t>(capacity, kDefaultStreamChunkSize);
    }

    bool
    CanAcquireLocked(size_t bytes) const {
        if (bytes > capacity_bytes_) {
            return inflight_bytes_ == 0;
        }
        return inflight_bytes_ <= capacity_bytes_ &&
               bytes <= capacity_bytes_ - inflight_bytes_;
    }

    std::mutex mu_;
    std::condition_variable cv_;
    size_t inflight_bytes_{0};
    size_t capacity_bytes_;
};

}  // namespace milvus::storage
