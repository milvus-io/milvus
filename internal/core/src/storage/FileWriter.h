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
#include <folly/executors/SerialExecutor.h>

#include "common/EasyAssert.h"
#include "storage/PayloadWriter.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

/**
 * FileWriter is a class that sequentially writes data to new files, designed specifically for saving temporary data downloaded from remote storage.
 * It supports both buffered and direct I/O, and can use an additional thread pool to write data to files.
 * FileWriter is not thread-safe, so you should take care of the thread safety when using the same FileWriter object in multiple threads.
 * The configuration of FileWriter is managed by FileWriterConfig, which is a singleton initialized at startup.
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
    explicit FileWriter(std::string filename);

    ~FileWriter();

    void
    Write(const void* data, size_t size);

    size_t
    Finish();

 private:
    static constexpr size_t ALIGNMENT_BYTES = 4096;
    static constexpr size_t ALIGNMENT_MASK = ALIGNMENT_BYTES - 1;

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
    Cleanup() noexcept;

    int fd_{-1};
    std::string filename_{""};
    size_t file_size_{0};

    // for direct io
    bool use_direct_io_{false};
    void* aligned_buf_{nullptr};
    size_t capacity_{0};
    size_t offset_{0};
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
    Configure(int nr_executor) {
        auto& instance = GetInstance();
        instance.SetExecutor(nr_executor);
    }

    void
    SetExecutor(int nr_executor) {
        if (nr_executor < 0) {
            LOG_WARN(
                "Invalid number of executor, expected: > 0, got: {}, "
                "set to 0",
                nr_executor);
            nr_executor = 0;
        } else if (nr_executor > std::thread::hardware_concurrency()) {
            LOG_WARN(
                "Invalid number of executor, expected: <= {}, got: {}, "
                "set to {}",
                std::thread::hardware_concurrency(),
                nr_executor,
                std::thread::hardware_concurrency());
            nr_executor = std::thread::hardware_concurrency();
        }
        std::shared_ptr<folly::CPUThreadPoolExecutor> old_executor = nullptr;
        {
            std::lock_guard<std::mutex> lock(executor_mutex_);
            old_executor = executor_;
            if (nr_executor > 0) {
                executor_ =
                    std::make_shared<folly::CPUThreadPoolExecutor>(nr_executor);
            } else {
                executor_ = nullptr;
            }
        }
        if (old_executor != nullptr) {
            old_executor->stop();
            old_executor->join();
        }
        LOG_INFO("Set write executor to {}", nr_executor);
    }

    bool
    AddTask(std::function<void()> task) {
        if (executor_ == nullptr) {
            return false;
        }
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

/**
 * FileWriterConfig is a singleton used to configure the FileWriter.
 * Three parameters are configurable:
 * 1. The number of writer threads in the extra thread pool, which is used to write data to files.
 * 2. The write mode, which can be 'buffered' (default) or 'direct'.
 * 3. The buffer size used for direct I/O, which is only used when the write mode is 'direct'.
 *
 * FileWriterConfig is thread-safe, so you can modify the configuration from multiple threads.
 * Note: The write mode and buffer size are not protected by a lock, but this is acceptable since changes to the write mode and buffer size will not affect existing FileWriter instances.
 */
class FileWriterConfig {
 public:
    enum class WriteMode : uint8_t { BUFFERED = 0, DIRECT = 1 };

    static FileWriterConfig&
    GetInstance() {
        static FileWriterConfig instance;
        return instance;
    }

    static void
    Configure(int nr_executor, WriteMode mode, size_t buffer_size) {
        auto& instance = GetInstance();
        instance.SetWriteExecutor(nr_executor);
        instance.SetMode(mode);
        instance.SetBufferSize(buffer_size);
    }

    ~FileWriterConfig() = default;

    void
    SetWriteExecutor(int nr_executor) {
        FileWriteWorkerPool::Configure(nr_executor);
    }

    void
    SetMode(WriteMode mode) {
        if (mode != WriteMode::BUFFERED && mode != WriteMode::DIRECT) {
            LOG_WARN(
                "Invalid write mode: {}, expected: BUFFERED or DIRECT, "
                "set to BUFFERED",
                static_cast<int>(mode));
            mode = WriteMode::BUFFERED;
        }
        mode_ = mode;
        LOG_INFO("Set write mode to {}", static_cast<uint8_t>(mode));
    }

    void
    SetBufferSize(size_t buffer_size) {
        if (buffer_size > MAX_BUFFER_SIZE) {
            LOG_WARN("Invalid buffer size: {}, expected: <= {}, set to {}",
                     buffer_size,
                     MAX_BUFFER_SIZE,
                     MAX_BUFFER_SIZE);
            buffer_size = MAX_BUFFER_SIZE;
        } else if (buffer_size < MIN_BUFFER_SIZE) {
            LOG_WARN("Invalid buffer size: {}, expected: >= {}, set to {}",
                     buffer_size,
                     MIN_BUFFER_SIZE,
                     MIN_BUFFER_SIZE);
            buffer_size = MIN_BUFFER_SIZE;
        } else {
            buffer_size = (buffer_size + ALIGNMENT_MASK) & ~ALIGNMENT_MASK;
        }
        buffer_size_ = buffer_size;
        LOG_INFO("Set buffer size to {}", buffer_size);
    }

    WriteMode
    GetMode() const {
        return mode_;
    }

    size_t
    GetBufferSize() const {
        return buffer_size_;
    }

    FileWriteWorkerPool&
    GetWriteExecutor() {
        return FileWriteWorkerPool::GetInstance();
    }

    FileWriterConfig(const FileWriterConfig&) = delete;
    FileWriterConfig&
    operator=(const FileWriterConfig&) = delete;
    FileWriterConfig(FileWriterConfig&&) = delete;
    FileWriterConfig&
    operator=(FileWriterConfig&&) = delete;

 private:
    static constexpr size_t ALIGNMENT_BYTES = 4096;
    static constexpr size_t ALIGNMENT_MASK = ALIGNMENT_BYTES - 1;
    static constexpr size_t MAX_BUFFER_SIZE = 64 * 1024 * 1024;  // 64MB
    static constexpr size_t MIN_BUFFER_SIZE = 4 * 1024;          // 4KB

    FileWriterConfig() = default;
    WriteMode mode_{WriteMode::BUFFERED};
    size_t buffer_size_{0};
};

}  // namespace milvus::storage
