// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <cstddef>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "folly/Executor.h"
#include "folly/executors/CPUThreadPoolExecutor.h"

namespace milvus::storage {

// Owns the dedicated executor used for blocking local-file finalization.
// A CPUThreadPoolExecutor is used intentionally because these tasks perform
// blocking syscalls instead of EventBase-driven asynchronous I/O. FileWriter
// remains synchronous and uses WritePermit to preserve the configured global
// disk-write concurrency limit across all callers.
class LocalFileIOPool {
 public:
    // Move-only token that releases one disk-write slot on destruction.
    class WritePermit {
     public:
        WritePermit() = default;

        WritePermit(const WritePermit&) = delete;
        WritePermit&
        operator=(const WritePermit&) = delete;

        WritePermit(WritePermit&& other) noexcept;
        WritePermit&
        operator=(WritePermit&& other) noexcept;

        ~WritePermit();

     private:
        friend class LocalFileIOPool;

        explicit WritePermit(LocalFileIOPool* owner) : owner_(owner) {
        }

        // Releases the owned slot early; repeated calls are no-ops.
        void
        Release() noexcept;

        LocalFileIOPool* owner_{nullptr};
    };

    // Returns the process-wide local-file executor and write limiter.
    [[nodiscard]] static LocalFileIOPool&
    GetInstance();

    // Sets worker and write concurrency. Zero disables the executor and limit.
    void
    Configure(int worker_count);

    // Returns an empty token when the dedicated executor is disabled.
    [[nodiscard]] folly::Executor::KeepAlive<>
    GetExecutor() const;

    // Blocks until a configured disk-write slot is available. An empty permit
    // represents the unlimited configuration and requires no release work.
    [[nodiscard]] WritePermit
    AcquireWritePermit();

    LocalFileIOPool(const LocalFileIOPool&) = delete;
    LocalFileIOPool&
    operator=(const LocalFileIOPool&) = delete;

    ~LocalFileIOPool();

 private:
    LocalFileIOPool() = default;

    // Returns one configured disk-write slot and wakes a waiter.
    void
    ReleaseWritePermit() noexcept;

    using Executor = folly::CPUThreadPoolExecutor;

    std::mutex configure_mutex_;
    mutable std::mutex executor_mutex_;
    std::shared_ptr<Executor> executor_;

    std::mutex permit_mutex_;
    std::condition_variable permit_cv_;
    std::atomic<size_t> write_concurrency_limit_{0};
    size_t active_write_permits_{0};
};

}  // namespace milvus::storage
