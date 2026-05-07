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
#include <cstdint>
#include <deque>
#include <future>
#include <mutex>
#include <string>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "storage/DataCodec.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"

namespace milvus::index {

// Counting semaphore for bounding in-flight download slices.
// Each slot represents one FILE_SLICE_SIZE (~16MB) in transient memory.
class DownloadSemaphore {
 public:
    explicit DownloadSemaphore(int capacity) : count_(capacity) {
    }

    void
    Acquire() {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this] { return count_ > 0; });
        --count_;
    }

    void
    Release() {
        std::lock_guard<std::mutex> lock(mtx_);
        ++count_;
        cv_.notify_one();
    }

    // RAII guard for automatic release
    class Guard {
     public:
        explicit Guard(DownloadSemaphore& sem) : sem_(&sem), active_(true) {
            sem_->Acquire();
        }
        ~Guard() {
            if (active_) {
                sem_->Release();
            }
        }
        Guard(const Guard&) = delete;
        Guard&
        operator=(const Guard&) = delete;
        Guard(Guard&& other) noexcept
            : sem_(other.sem_), active_(other.active_) {
            other.active_ = false;
        }
        Guard&
        operator=(Guard&& other) noexcept {
            if (this != &other) {
                if (active_) {
                    sem_->Release();
                }
                sem_ = other.sem_;
                active_ = other.active_;
                other.active_ = false;
            }
            return *this;
        }

     private:
        DownloadSemaphore* sem_;
        bool active_;
    };

 private:
    std::mutex mtx_;
    std::condition_variable cv_;
    int count_;
};

// Per-priority download semaphores for scalar index streaming loads.
// Slot count is derived from the corresponding thread pool size to avoid
// having more in-flight downloads than threads available, while also
// capping total transient memory.
//
// Total budget = 512MB, split 3:1 between HIGH and LOW priority.
// Formula: slots = min(pool.MaxThreads, memory_budget / FILE_SLICE_SIZE)
//   HIGH: 384MB / 16MB = 24 slots (capped by pool threads if fewer)
//   LOW:  128MB / 16MB = 8 slots  (capped by pool threads if fewer)
class DownloadSemaphores {
 public:
    static constexpr int64_t kTotalBudget = 512 << 20;  // 512MB

    static DownloadSemaphore&
    Get(milvus::ThreadPoolPriority priority) {
        static DownloadSemaphores instance;
        if (priority == milvus::ThreadPoolPriority::LOW) {
            return instance.low_;
        }
        return instance.high_;
    }

 private:
    static int
    ComputeSlots(milvus::ThreadPoolPriority priority, int64_t memory_budget) {
        auto& pool = ::milvus::ThreadPools::GetThreadPool(priority);
        int pool_threads = pool.GetMaxThreadNum();
        int64_t slice_size = FILE_SLICE_SIZE.load();
        int mem_slots =
            std::max(1, static_cast<int>(memory_budget / slice_size));
        return std::min(pool_threads, mem_slots);
    }

    DownloadSemaphores()
        : high_(ComputeSlots(milvus::ThreadPoolPriority::HIGH,
                             kTotalBudget * 3 / 4)),
          low_(ComputeSlots(milvus::ThreadPoolPriority::LOW,
                            kTotalBudget * 1 / 4)) {
    }
    DownloadSemaphore high_;
    DownloadSemaphore low_;
};

// Prefetch and process slices with a sliding window.
// Maintains up to prefetch_depth in-flight downloads, each holding a
// semaphore slot. Downloads are submitted to the thread pool via
// GetObjectData (single file per call); while the current slice is
// being processed, the next ones are already downloading.
//
// Memory control: each in-flight slice holds one DownloadSemaphore slot
// (= one FILE_SLICE_SIZE ≈ 16MB). Global cap = semaphore capacity × 16MB.
//
// Usage:
//   PrefetchAndProcess(cm, files, prio, prefetch_depth,
//       [&](const uint8_t* data, size_t size) {
//           file_writer.Write(data, size);
//       });
template <typename ProcessFn>
void
PrefetchAndProcess(storage::ChunkManager* cm,
                   const std::vector<std::string>& files,
                   milvus::ThreadPoolPriority prio,
                   size_t prefetch_depth,
                   ProcessFn&& process_fn) {
    if (files.empty()) {
        return;
    }
    AssertInfo(prefetch_depth > 0,
               "PrefetchAndProcess: prefetch_depth must be > 0");

    using Future = std::future<std::unique_ptr<storage::DataCodec>>;

    struct PrefetchEntry {
        DownloadSemaphore::Guard guard;
        Future future;
    };

    auto& sem = DownloadSemaphores::Get(prio);
    std::deque<PrefetchEntry> window;
    size_t next = 0;

    auto submit_one = [&]() {
        if (next >= files.size()) {
            return;
        }
        DownloadSemaphore::Guard guard(sem);
        auto futures = storage::GetObjectData(cm, {files[next]}, prio);
        window.push_back({std::move(guard), std::move(futures[0])});
        next++;
    };

    // Drain all remaining futures to avoid background tasks writing to
    // destroyed promises. Guards are released as entries are destroyed.
    auto drain_window = [&window]() {
        while (!window.empty()) {
            auto entry = std::move(window.front());
            window.pop_front();
            try {
                entry.future.get();
            } catch (...) {
            }
        }
    };

    // Fill initial prefetch window
    for (size_t i = 0; i < std::min(prefetch_depth, files.size()); i++) {
        submit_one();
    }

    // Process in order: pop front, submit next, process
    try {
        while (!window.empty()) {
            auto entry = std::move(window.front());
            window.pop_front();

            // Submit next before waiting, so download overlaps with
            // wait+process
            submit_one();

            auto codec = entry.future.get();
            process_fn(codec->PayloadData(), codec->PayloadSize());
            // entry destroyed → Guard released → semaphore slot freed
        }
    } catch (...) {
        drain_window();
        throw;
    }
}

}  // namespace milvus::index
