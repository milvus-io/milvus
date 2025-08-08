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

#include <cassert>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "common/EasyAssert.h"
#include "storage/PayloadWriter.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

/**
 * FileWriter is a class that sequentially writes data to new files, designed specifically for saving temporary data downloaded from remote storage.
 * It supports both buffered and direct I/O, and can use an additional thread pool to write data to files.
 * FileWriter is not thread-safe, so you should take care of the thread safety when using the same FileWriter object in multiple threads.
 * For now, only QueryNode uses FileWriter to write data to files. If you want to use it in DataNode, you need to add it to the configuration.
 *
 * The basic usage is:
 *
 * auto file_writer = FileWriter("path/to/file");
 * file_writer.Write(data, size);
 * ...
 * file_writer.Write(data, size);
 * file_writer.Finish();
 */
class FileWriter {
 public:
    enum class WriteMode : uint8_t { BUFFERED = 0, DIRECT = 1 };

    static constexpr size_t ALIGNMENT_BYTES = 4096;
    static constexpr size_t ALIGNMENT_MASK = ALIGNMENT_BYTES - 1;
    static constexpr size_t MAX_BUFFER_SIZE = 64 * 1024 * 1024;  // 64MB
    static constexpr size_t MIN_BUFFER_SIZE = 4 * 1024;          // 4KB
    static constexpr size_t DEFAULT_BUFFER_SIZE = 64 * 1024;     // 64KB

    explicit FileWriter(std::string filename);

    ~FileWriter();

    void
    Write(const void* data, size_t size);

    size_t
    Finish();

    // static functions for global configuration
    static void
    SetMode(WriteMode mode);

    static void
    SetBufferSize(size_t buffer_size);

    static WriteMode
    GetMode();

    static size_t
    GetBufferSize();

 private:
    void
    WriteWithDirectIO(const void* data, size_t nbyte);

    void
    WriteWithBufferedIO(const void* data, size_t nbyte);

    void
    FlushWithDirectIO();

    void
    FlushWithBufferedIO();

    bool
    PositionedWrite(const void* data, size_t nbyte, size_t file_offset);

    void
    PositionedWriteWithCheck(const void* data,
                             size_t nbyte,
                             size_t file_offset);

    void
    Cleanup() noexcept;

    int fd_{-1};
    std::string filename_{""};
    size_t file_size_{0};

    // for direct io
    bool use_direct_io_{false};
    void* aligned_buf_{nullptr};
    size_t capacity_{0};
    size_t offset_{0};

    // for global configuration
    static WriteMode
        mode_;  // The write mode, which can be 'buffered' (default) or 'direct'.
    static size_t
        buffer_size_;  // The buffer size used for direct I/O, which is only used when the write mode is 'direct'.
};

class FileWriteWorkerPool {
 public:
    FileWriteWorkerPool() = default;

    static FileWriteWorkerPool&
    GetInstance() {
        static FileWriteWorkerPool instance;
        return instance;
    }

    static void
    Configure(int nr_worker) {
        auto& instance = GetInstance();
        instance.SetWorker(nr_worker);
    }

    void
    SetWorker(int nr_worker) {
        if (nr_worker < 0) {
            LOG_WARN(
                "Invalid number of worker, expected: > 0, got: {}, "
                "set to 0",
                nr_worker);
            nr_worker = 0;
        } else if (nr_worker > std::thread::hardware_concurrency()) {
            LOG_WARN(
                "Invalid number of worker, expected: <= {}, got: {}, "
                "set to {}",
                std::thread::hardware_concurrency(),
                nr_worker,
                std::thread::hardware_concurrency());
            nr_worker = std::thread::hardware_concurrency();
        }
        std::shared_ptr<folly::CPUThreadPoolExecutor> old_executor = nullptr;
        {
            std::lock_guard<std::mutex> lock(executor_mutex_);
            old_executor = executor_;
            if (nr_worker > 0) {
                executor_ =
                    std::make_shared<folly::CPUThreadPoolExecutor>(nr_worker);
            } else {
                executor_ = nullptr;
            }
        }
        if (old_executor != nullptr) {
            old_executor->stop();
            old_executor->join();
        }
        LOG_INFO("Set the number of write worker to {}", nr_worker);
    }

    bool
    AddTask(std::function<void()> task) {
        if (executor_ == nullptr) {
            return false;
        }
        std::lock_guard<std::mutex> lock(executor_mutex_);
        executor_->add(std::move(task));
        return true;
    }

    ~FileWriteWorkerPool() {
        if (executor_ != nullptr) {
            executor_->stop();
            executor_->join();
            executor_ = nullptr;
        }
    }

 private:
    std::shared_ptr<folly::CPUThreadPoolExecutor> executor_{nullptr};
    std::mutex executor_mutex_{};
};

}  // namespace milvus::storage
