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
#include <array>
#include <cassert>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "common/EasyAssert.h"
#include "log/Log.h"
#include "pb/common.pb.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

namespace io {
enum class Priority { HIGH = 0, MIDDLE = 1, LOW = 2, NR_PRIORITY = 3 };

inline Priority
GetPriorityFromLoadPriority(milvus::proto::common::LoadPriority priority) {
    return priority == milvus::proto::common::LoadPriority::HIGH
               ? io::Priority::HIGH
               : io::Priority::LOW;
}

class WriteRateLimiter {
 public:
    static WriteRateLimiter&
    GetInstance() {
        static WriteRateLimiter instance;
        return instance;
    }

    void
    Configure(int64_t refill_period_us,
              int64_t avg_bps,
              int64_t max_burst_bps,
              int32_t high_priority_ratio,
              int32_t middle_priority_ratio,
              int32_t low_priority_ratio) {
        if (refill_period_us <= 0 || avg_bps <= 0 || max_burst_bps <= 0 ||
            avg_bps > max_burst_bps) {
            ThrowInfo(ErrorCode::InvalidParameter,
                      "All parameters must be positive, but got: "
                      "refill_period_us: {}, "
                      "avg_bps: {}, max_burst_bps: {}",
                      refill_period_us,
                      avg_bps,
                      max_burst_bps);
        }
        std::unique_lock<std::mutex> lock(mutex_);
        // avoid too small refill period, 1ms is used as the minimum refill period
        refill_period_us_ = std::max<int64_t>(1000, refill_period_us);
        refill_bytes_per_period_ = avg_bps * refill_period_us_ / 1000000;
        if (refill_bytes_per_period_ <= 0) {
            refill_bytes_per_period_ = 1;
        }
        expire_periods_ = max_burst_bps * refill_period_us_ / 1000000 /
                          refill_bytes_per_period_;
        if (expire_periods_ <= 0) {
            expire_periods_ = 1;
        }
        available_bytes_ = 0;
        last_refill_time_ = std::chrono::steady_clock::now();
        priority_ratio_ = {
            high_priority_ratio, middle_priority_ratio, low_priority_ratio};
        LOG_INFO(
            "Disk rate limiter configured with refill_period_us: {}, "
            "refill_bytes_per_period: {},avg_bps: {}, max_burst_bps: {}, "
            "expire_periods: {}, high_priority_ratio: {}, "
            "middle_priority_ratio: {}, low_priority_ratio: {}",
            refill_period_us_,
            refill_bytes_per_period_,
            avg_bps,
            max_burst_bps,
            expire_periods_,
            high_priority_ratio,
            middle_priority_ratio,
            low_priority_ratio);
    }

    size_t
    Acquire(size_t bytes,
            size_t alignment_bytes = 1,
            Priority priority = Priority::MIDDLE) {
        if (static_cast<int>(priority) >=
            static_cast<int>(Priority::NR_PRIORITY)) {
            ThrowInfo(ErrorCode::InvalidParameter,
                      "Invalid priority value: {}",
                      static_cast<int>(priority));
        }
        // if priority ratio is <= 0, no rate limit is applied, return the original bytes
        if (priority_ratio_[static_cast<int>(priority)] <= 0) {
            return bytes;
        }
        AssertInfo(alignment_bytes > 0 && bytes >= alignment_bytes &&
                       (bytes % alignment_bytes == 0),
                   "alignment_bytes must be positive and bytes must be "
                   "divisible by alignment_bytes");

        std::unique_lock<std::mutex> lock(mutex_);
        // recheck the amplification ratio after taking the lock
        auto amplification_ratio = priority_ratio_[static_cast<int>(priority)];
        if (amplification_ratio <= 0) {
            return bytes;
        }

        // calculate the available bytes by delta periods
        std::chrono::steady_clock::time_point now =
            std::chrono::steady_clock::now();
        // steady_clock is monotonic, so the time delta is always >= 0
        auto delta_periods = static_cast<int>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                now - last_refill_time_)
                .count() /
            refill_period_us_);
        // early return if the time delta is less than the refill period and
        // the available bytes is less than the alignment bytes
        if (delta_periods == 0 && available_bytes_ < alignment_bytes) {
            return 0;
        }
        if (delta_periods > expire_periods_) {
            available_bytes_ += expire_periods_ * refill_bytes_per_period_;
        } else {
            available_bytes_ += delta_periods * refill_bytes_per_period_;
        }
        // keep the available bytes in the range of [0, refill_bytes_per_period_ * expire_periods_]
        available_bytes_ = std::min(
            available_bytes_,
            static_cast<size_t>(refill_bytes_per_period_ * expire_periods_));

        // calculate the allowed bytes with amplification ratio
        auto ret = std::min(bytes, available_bytes_ * amplification_ratio);
        // align the allowed bytes to the alignment bytes
        ret = (ret / alignment_bytes) * alignment_bytes;
        // update available_bytes_ by removing the amplification ratio, the updated value is always >= 0
        available_bytes_ -= ret / amplification_ratio;

        // update the last refill time only if delta_periods > 0
        if (delta_periods > 0) {
            last_refill_time_ = now;
        }

        return ret;
    }

    size_t
    GetRateLimitPeriod() const {
        return refill_period_us_;
    }

    size_t
    GetBytesPerPeriod() const {
        return refill_bytes_per_period_;
    }

    void
    Reset() {
        std::unique_lock<std::mutex> lock(mutex_);
        available_bytes_ = refill_bytes_per_period_;
        last_refill_time_ = std::chrono::steady_clock::now();
    }

    WriteRateLimiter(const WriteRateLimiter&) = delete;
    WriteRateLimiter&
    operator=(const WriteRateLimiter&) = delete;

    ~WriteRateLimiter() = default;

 private:
    WriteRateLimiter() = default;

    // Set the default rate limit to a valid, reasonable value.
    // These values should always be overridden by the yaml configuration, but
    // if not, the default can still serve as a reasonable "no-limit" fallback.
    int64_t refill_period_us_ = 100000;                       // 100ms
    int64_t refill_bytes_per_period_ = 1024ll * 1024 * 1024;  // 1GB
    int32_t expire_periods_ = 10;                             // 10 periods
    std::chrono::steady_clock::time_point last_refill_time_ =
        std::chrono::steady_clock::now();
    size_t available_bytes_ = 0;
    std::array<int32_t, 3> priority_ratio_ = {-1, -1, -1};
    std::mutex mutex_;
};

}  // namespace io

/**
 * FileWriter is a class that sequentially writes data to new files, designed specifically for saving temporary data downloaded from remote storage.
 * It supports both buffered and direct I/O, and can use an additional thread pool to write data to files.
 * FileWriter is not thread-safe, so you should take care of the thread safety when using the same FileWriter object in multiple threads.
 * For now, only QueryNode uses FileWriter to write data to files. If you want to use it in DataNode, you need to add it to the configuration.
 *
 * The basic usage is:
 *
 * auto file_writer = FileWriter("path/to/file", io::Priority::MIDDLE);
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
    // for rate limiter
    static constexpr int MAX_EMPTY_LOOPS = 20;
    static constexpr int64_t MAX_WAIT_US = 5000000;  // 5s

    explicit FileWriter(std::string filename,
                        io::Priority priority = io::Priority::MIDDLE);

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

    // for rate limiter
    io::Priority priority_;
    io::WriteRateLimiter& rate_limiter_;
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
