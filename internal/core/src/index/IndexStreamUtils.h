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

#include "storage/DataCodec.h"
#include "storage/Util.h"

namespace milvus::index {

// Process-wide semaphore that limits the number of in-flight index data
// slices across all concurrent scalar index loads. This bounds the total
// transient memory used for streaming downloads regardless of how many
// indexes are loading simultaneously.
//
// Default capacity = 8 slots. Each slot holds one FILE_SLICE_SIZE (16MB)
// download, so the global cap is 8 * 16MB = 128MB.
class DownloadSemaphore {
 public:
    static DownloadSemaphore&
    GetInstance() {
        static DownloadSemaphore instance(8);
        return instance;
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
        Guard() : active_(true) {
            DownloadSemaphore::GetInstance().Acquire();
        }
        ~Guard() {
            if (active_) {
                DownloadSemaphore::GetInstance().Release();
            }
        }
        Guard(const Guard&) = delete;
        Guard&
        operator=(const Guard&) = delete;
        Guard(Guard&& other) noexcept : active_(other.active_) {
            other.active_ = false;
        }
        Guard&
        operator=(Guard&& other) noexcept {
            if (this != &other) {
                if (active_) {
                    DownloadSemaphore::GetInstance().Release();
                }
                active_ = other.active_;
                other.active_ = false;
            }
            return *this;
        }

     private:
        bool active_;
    };

 private:
    explicit DownloadSemaphore(int capacity) : count_(capacity) {
    }
    std::mutex mtx_;
    std::condition_variable cv_;
    int count_;
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

    using Future = std::future<std::unique_ptr<storage::DataCodec>>;

    struct PrefetchEntry {
        DownloadSemaphore::Guard guard;
        Future future;
    };

    std::deque<PrefetchEntry> window;
    size_t next = 0;

    auto submit_one = [&]() {
        if (next >= files.size()) {
            return;
        }
        DownloadSemaphore::Guard guard;
        auto futures = storage::GetObjectData(cm, {files[next]}, prio);
        window.push_back({std::move(guard), std::move(futures[0])});
        next++;
    };

    // Fill initial prefetch window
    for (size_t i = 0; i < std::min(prefetch_depth, files.size()); i++) {
        submit_one();
    }

    // Process in order: pop front, submit next, process
    while (!window.empty()) {
        auto entry = std::move(window.front());
        window.pop_front();

        // Submit next before waiting, so download overlaps with wait+process
        submit_one();

        auto codec = entry.future.get();
        process_fn(codec->PayloadData(), codec->PayloadSize());
        // entry destroyed → Guard released → semaphore slot freed
    }
}

}  // namespace milvus::index
