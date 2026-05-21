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
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

#include "common/Common.h"
#include "common/EasyAssert.h"

namespace milvus::storage {

constexpr size_t kMinStreamSliceSize = 64 * 1024;
constexpr size_t kStreamSliceAlignment = 4 * 1024;
constexpr size_t kTailMergeGrace = 1 * 1024 * 1024;

inline bool
IsStreamSliceSizeAligned(size_t slice_size) {
    return slice_size > 0 && slice_size % kStreamSliceAlignment == 0;
}

inline size_t
DefaultStreamSliceSize() {
    return DEFAULT_INDEX_FILE_SLICE_SIZE;
}

inline double
StreamBudgetRatio() {
    auto ratio = milvus::ENTRY_STREAM_BUDGET_RATIO.load();
    return ratio > 0 ? ratio : 1.0;
}

/// A slice read from a V3 entry. `error` carries an exception captured in
/// the producer task so the consumer can rethrow instead of hanging.
struct StreamSliceResult {
    size_t budget_bytes{0};
    std::vector<uint8_t> data;
    std::exception_ptr error = nullptr;
};

/// Byte budget for transient data that has been submitted for async work but
/// has not been consumed yet.
///
/// Usage:
///   - Call Acquire(bytes) to block until budget is available.
///   - Call TryAcquire(bytes) for non-blocking replenish in refill loops.
///   - Call Release(bytes) after the transient data has been consumed.
///   - Oversized requests are allowed to run exclusively to guarantee progress.
class TransientMemoryBudget {
 public:
    static TransientMemoryBudget&
    GetEntryStreamBudget() {
        static TransientMemoryBudget instance;
        return instance;
    }

    static TransientMemoryBudget&
    GetFieldDataLoadBudget() {
        static TransientMemoryBudget instance(DEFAULT_FIELD_MAX_MEMORY_LIMIT);
        return instance;
    }

    static void
    SetFieldDataLoadBudgetBytes(size_t bytes) {
        GetFieldDataLoadBudget().SetCapacityBytes(bytes);
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
    /// Used in the refill loop where blocking could cause deadlock.
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
            AssertInfo(bytes <= inflight_bytes_,
                       "Transient memory budget over-release: release {}, "
                       "inflight {}",
                       bytes,
                       inflight_bytes_);
            inflight_bytes_ -= bytes;
        }
        cv_.notify_all();
    }

    size_t
    CapacityBytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return CapacityBytesLocked();
    }

    void
    SetCapacityBytes(size_t bytes) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            capacity_bytes_ = std::max<size_t>(bytes, 1);
        }
        cv_.notify_all();
    }

    void
    NotifyCapacityUpdated() {
        cv_.notify_all();
    }

 private:
    TransientMemoryBudget() = default;

    explicit TransientMemoryBudget(size_t capacity_bytes)
        : capacity_bytes_(std::max<size_t>(capacity_bytes, 1)) {
    }

    static size_t
    EntryStreamBudgetBytes() {
        auto core_num = std::max(1, milvus::CPU_NUM);
        auto capacity = static_cast<size_t>(core_num * StreamBudgetRatio()) *
                        DefaultStreamSliceSize();
        return std::max<size_t>(capacity, DefaultStreamSliceSize());
    }

    size_t
    CapacityBytesLocked() const {
        if (capacity_bytes_ > 0) {
            return capacity_bytes_;
        }
        return EntryStreamBudgetBytes();
    }

    bool
    CanAcquireLocked(size_t bytes) const {
        auto capacity_bytes = CapacityBytesLocked();
        if (bytes > capacity_bytes) {
            return inflight_bytes_ == 0;
        }
        return inflight_bytes_ <= capacity_bytes &&
               bytes <= capacity_bytes - inflight_bytes_;
    }

    mutable std::mutex mu_;
    std::condition_variable cv_;
    size_t inflight_bytes_{0};
    size_t capacity_bytes_{0};
};

inline size_t
EntryStreamMaxTransientBytes() {
    auto capacity =
        TransientMemoryBudget::GetEntryStreamBudget().CapacityBytes();
    if (capacity > std::numeric_limits<size_t>::max() - kTailMergeGrace) {
        return std::numeric_limits<size_t>::max();
    }
    return capacity + kTailMergeGrace;
}

}  // namespace milvus::storage
