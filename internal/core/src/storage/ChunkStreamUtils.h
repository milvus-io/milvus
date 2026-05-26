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

#include "common/Common.h"

namespace milvus::storage {

inline size_t
DefaultStreamChunkSize() {
    auto chunk_size = milvus::INDEX_ENTRY_STREAM_CHUNK_SIZE.load();
    return chunk_size > 0 ? static_cast<size_t>(chunk_size) : 1;
}

inline double
ScalarIndexChunkBudgetRatio() {
    auto ratio = milvus::SCALAR_INDEX_ENTRY_STREAM_BUDGET_RATIO.load();
    return ratio > 0 ? ratio : 1.0;
}

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
        auto core_num = std::max(1, milvus::CPU_NUM);
        auto capacity =
            static_cast<size_t>(core_num * ScalarIndexChunkBudgetRatio()) *
            DefaultStreamChunkSize();
        return std::max<size_t>(capacity, DefaultStreamChunkSize());
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
