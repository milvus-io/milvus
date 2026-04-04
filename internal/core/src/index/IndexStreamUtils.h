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
#include <cstdint>
#include <mutex>

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
        Guard() {
            DownloadSemaphore::GetInstance().Acquire();
        }
        ~Guard() {
            DownloadSemaphore::GetInstance().Release();
        }
        Guard(const Guard&) = delete;
        Guard&
        operator=(const Guard&) = delete;
    };

 private:
    explicit DownloadSemaphore(int capacity) : count_(capacity) {
    }
    std::mutex mtx_;
    std::condition_variable cv_;
    int count_;
};

}  // namespace milvus::index
