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

#include <condition_variable>
#include <cstddef>
#include <limits>
#include <mutex>

#include "common/EasyAssert.h"

namespace milvus::storage {

/// Byte budget for transient data submitted for asynchronous work but not yet
/// consumed. Capacity 0 means unlimited. An oversized request may run
/// exclusively to guarantee progress.
class TransientMemoryBudget {
 public:
    static TransientMemoryBudget&
    GetJsonStatsBuildBudget() {
        static TransientMemoryBudget instance;
        return instance;
    }

    static void
    SetJsonStatsBuildBudgetBytes(size_t bytes) {
        GetJsonStatsBuildBudget().SetCapacityBytes(bytes);
    }

    /// Block until enough budget is available. The caller must not hold work
    /// whose completion is needed to release budget.
    void
    Acquire(size_t bytes) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this, bytes] { return CanAcquireLocked(bytes); });
        inflight_bytes_ += bytes;
    }

    /// Try to claim budget without blocking. Refill loops use this while they
    /// still own inflight work.
    bool
    TryAcquire(size_t bytes) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!CanAcquireLocked(bytes)) {
            return false;
        }
        inflight_bytes_ += bytes;
        return true;
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

    /// Replace a task's pre-dispatch reservation with its measured result
    /// size. Growth is intentionally non-blocking: completed workers must not
    /// wait for consumers ordered behind other active tasks. A temporary
    /// overage prevents new acquisitions until consumers release enough bytes.
    void
    ReconcileReservation(size_t reserved_bytes, size_t actual_bytes) {
        bool released_bytes = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            AssertInfo(reserved_bytes <= inflight_bytes_,
                       "Transient memory budget reconcile exceeds inflight: "
                       "reserved {}, inflight {}",
                       reserved_bytes,
                       inflight_bytes_);
            const auto other_inflight_bytes = inflight_bytes_ - reserved_bytes;
            AssertInfo(actual_bytes <= std::numeric_limits<size_t>::max() -
                                           other_inflight_bytes,
                       "Transient memory budget reconcile overflow: actual "
                       "{}, other inflight {}",
                       actual_bytes,
                       other_inflight_bytes);
            inflight_bytes_ = other_inflight_bytes + actual_bytes;
            released_bytes = actual_bytes < reserved_bytes;
        }
        if (released_bytes) {
            cv_.notify_all();
        }
    }

    size_t
    CapacityBytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return capacity_bytes_;
    }

    size_t
    InflightBytes() const {
        std::lock_guard<std::mutex> lock(mu_);
        return inflight_bytes_;
    }

    void
    SetCapacityBytes(size_t bytes) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            capacity_bytes_ = bytes;
        }
        cv_.notify_all();
    }

 private:
    TransientMemoryBudget() = default;

    bool
    CanAcquireLocked(size_t bytes) const {
        if (capacity_bytes_ == 0) {
            return true;
        }
        if (bytes > capacity_bytes_) {
            return inflight_bytes_ == 0;
        }
        return inflight_bytes_ <= capacity_bytes_ &&
               bytes <= capacity_bytes_ - inflight_bytes_;
    }

    mutable std::mutex mu_;
    std::condition_variable cv_;
    size_t inflight_bytes_{0};
    size_t capacity_bytes_{0};
};

}  // namespace milvus::storage
