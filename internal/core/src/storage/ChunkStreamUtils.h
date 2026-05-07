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

/// Capacity multiplier relative to thread pool size.
/// Matches the kChannelCapacityMultiplier used by ManifestGroupTranslator.
constexpr double kStreamChannelCapacityMultiplier = 1.5;

/// A chunk downloaded from a V3 entry, carrying a sequence number for
/// reordering on the consumer side. `error` carries an exception captured in
/// the producer task so the consumer can rethrow instead of hanging on pop.
struct ChunkResult {
    size_t seq;
    std::vector<uint8_t> data;
    std::exception_ptr error = nullptr;
};

/// Global inflight chunk counter shared by all scalar index ReadEntryStream
/// calls. Limits total memory used by in-flight chunks across all concurrent
/// index loads.
///
/// Usage:
///   - Call Acquire() to block until a slot is available (for initial fill).
///   - Call TryAcquire() for non-blocking attempt (for replenish in slide loop).
///   - Call Release() after the chunk has been consumed (callback done).
///   - Inflight slots are bounded by pool_size * kStreamChannelCapacityMultiplier.
///
/// Capacity is derived once from the HIGH-priority pool on first use. Streams
/// that submit to a smaller pool (LOW/MIDDLE) share the same budget; they may
/// therefore keep fewer slots in flight than the budget allows, which only
/// under-utilizes the bound rather than violating it. The singleton is
/// process-wide, so concurrent streams at any priority contend on the same
/// counter — the practical ceiling is HIGH pool size regardless of the pool
/// actually used by a given caller.
class ChunkInflightBudget {
 public:
    static ChunkInflightBudget&
    GetInstance() {
        static ChunkInflightBudget instance;
        return instance;
    }

    /// Block until a slot is available. Uses condition_variable for efficient
    /// waiting. Safe to call when the calling thread has no inflight tasks
    /// (no risk of deadlock with channel pop).
    void
    Acquire() {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return inflight_ < max_inflight_; });
        inflight_++;
    }

    /// Try to claim a slot. Returns true if under budget.
    /// Used in the slide loop where blocking could cause deadlock.
    bool
    TryAcquire() {
        std::lock_guard<std::mutex> lock(mu_);
        if (inflight_ < max_inflight_) {
            inflight_++;
            return true;
        }
        return false;
    }

    void
    Release() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            inflight_--;
        }
        cv_.notify_one();
    }

    size_t
    MaxInflight() const {
        return max_inflight_;
    }

 private:
    ChunkInflightBudget() {
        auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::HIGH);
        max_inflight_ = static_cast<size_t>(pool.GetMaxThreadNum() *
                                            kStreamChannelCapacityMultiplier);
        if (max_inflight_ == 0) {
            max_inflight_ = 1;
        }
    }

    std::mutex mu_;
    std::condition_variable cv_;
    size_t inflight_{0};
    size_t max_inflight_;
};

}  // namespace milvus::storage
