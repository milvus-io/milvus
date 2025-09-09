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

#include <utility>
#include "folly/futures/Future.h"
#include "monitor/Monitor.h"
#include "storage/FileWriter.h"

namespace milvus::storage {

FileWriter::FileWriter(std::string filename, io::Priority priority)
    : filename_(std::move(filename)),
      priority_(priority),
      rate_limiter_(io::WriteRateLimiter::GetInstance()) {
    auto mode = GetMode();
    use_direct_io_ = mode == WriteMode::DIRECT;
    auto open_flags = O_CREAT | O_RDWR | O_TRUNC;
    if (use_direct_io_) {
        // check if the file is aligned to the alignment size
        size_t buf_size = GetBufferSize();
        AssertInfo(
            buf_size != 0 && (buf_size % ALIGNMENT_BYTES) == 0,
            "Buffer size must be greater than 0 and aligned to the alignment "
            "size, buf_size: {}, alignment size: {}, error: {}",
            buf_size,
            ALIGNMENT_BYTES,
            strerror(errno));
        capacity_ = buf_size;
        auto err = posix_memalign(&aligned_buf_, ALIGNMENT_BYTES, capacity_);
        if (err != 0) {
            aligned_buf_ = nullptr;
            ThrowInfo(
                ErrorCode::MemAllocateFailed,
                "Failed to allocate aligned buffer for direct io, error: {}",
                strerror(err));
        }
#ifndef __APPLE__
        open_flags |= O_DIRECT;
#endif
    }

    fd_ = open(filename_.c_str(), open_flags, S_IRUSR | S_IWUSR);
    if (fd_ == -1) {
        Cleanup();
        ThrowInfo(ErrorCode::FileCreateFailed,
                  "Failed to open file: {}, error: {}",
                  filename_,
                  strerror(errno));
    }

#ifdef __APPLE__
    if (use_direct_io_) {
        auto ret = fcntl(fd_, F_NOCACHE, 1);
        if (ret == -1) {
            Cleanup();
            ThrowInfo(ErrorCode::FileCreateFailed,
                      "Failed to set F_NOCACHE on file: {}, error: {}",
                      filename_,
                      strerror(errno));
        }
    }
#endif
}

FileWriter::~FileWriter() {
    Cleanup();
}

void
FileWriter::Cleanup() noexcept {
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
    if (use_direct_io_) {
        free(aligned_buf_);
        aligned_buf_ = nullptr;
    }
}

bool
FileWriter::PositionedWrite(const void* data,
                            size_t nbyte,
                            size_t file_offset) {
    const char* src = static_cast<const char*>(data);
    size_t left = nbyte;

    while (left != 0) {
        ssize_t done = pwrite(fd_, src, left, file_offset);
        if (done < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        left -= done;
        file_offset += done;
        src += done;
    }

    return true;
}

void
FileWriter::PositionedWriteWithCheck(const void* data,
                                     size_t nbyte,
                                     size_t file_offset) {
    size_t bytes_to_write = nbyte;
    int32_t empty_loops = 0;
    int64_t total_wait_us = 0;
    size_t alignment_bytes = use_direct_io_ ? ALIGNMENT_BYTES : 1;
    while (bytes_to_write != 0) {
        auto allowed_bytes =
            rate_limiter_.Acquire(bytes_to_write, alignment_bytes, priority_);
        if (allowed_bytes == 0) {
            ++empty_loops;
            // if the empty loops is too large or the total wait time is too long, we should write the data directly
            if (empty_loops > MAX_EMPTY_LOOPS || total_wait_us > MAX_WAIT_US) {
                allowed_bytes = rate_limiter_.GetBytesPerPeriod();
                empty_loops = 0;
                total_wait_us = 0;
            } else {
                int64_t wait_us = (1 << (empty_loops / 10)) *
                                  rate_limiter_.GetRateLimitPeriod();
                std::this_thread::sleep_for(std::chrono::microseconds(wait_us));
                total_wait_us += wait_us;
                continue;
            }
        }
        if (!PositionedWrite(data, allowed_bytes, file_offset)) {
            Cleanup();
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "Failed to write to file: {}, error: {}",
                      filename_,
                      strerror(errno));
        }
        file_offset += allowed_bytes;
        bytes_to_write -= allowed_bytes;
        data = static_cast<const char*>(data) + allowed_bytes;
    }
}

void
FileWriter::WriteWithDirectIO(const void* data, size_t nbyte) {
    const char* src = static_cast<const char*>(data);
    // if the data can fit in the aligned buffer, we can just copy it to the aligned buffer
    if (offset_ + nbyte <= capacity_) {
        memcpy(static_cast<char*>(aligned_buf_) + offset_, src, nbyte);
        offset_ += nbyte;
        return;
    }
    size_t left_size = nbyte;

    // we should fill and handle the cached aligned buffer first
    if (offset_ != 0) {
        size_t cpy_size = capacity_ - offset_;
        memcpy(static_cast<char*>(aligned_buf_) + offset_, src, cpy_size);
        PositionedWriteWithCheck(aligned_buf_, capacity_, file_size_);
        file_size_ += capacity_;
        left_size -= cpy_size;
        src += cpy_size;
        offset_ = 0;
    }

    // if the left data is aligned, we can just write it to the file and only save the tail to the aligned buffer
    // it will save the time of memcpy
    if (reinterpret_cast<uintptr_t>(src) % ALIGNMENT_BYTES == 0) {
        size_t aligned_left_size = left_size & ~ALIGNMENT_MASK;
        left_size -= aligned_left_size;
        while (aligned_left_size != 0) {
            size_t bytes_written = std::min(aligned_left_size, capacity_);
            PositionedWriteWithCheck(src, bytes_written, file_size_);
            file_size_ += bytes_written;
            aligned_left_size -= bytes_written;
            src += bytes_written;
        }
    }

    // finally, handle the left unaligned data by the aligned buffer
    while (left_size >= capacity_) {
        size_t copy_size = capacity_ - offset_;
        memcpy(static_cast<char*>(aligned_buf_) + offset_, src, copy_size);
        PositionedWriteWithCheck(aligned_buf_, capacity_, file_size_);
        file_size_ += capacity_;
        offset_ = 0;
        left_size -= copy_size;
        src += copy_size;
    }

    // save the tail to the aligned buffer
    if (left_size > 0) {
        memcpy(static_cast<char*>(aligned_buf_) + offset_, src, left_size);
        offset_ += left_size;
        src += left_size;
    }

    assert(src == static_cast<const char*>(data) + nbyte);

    milvus::monitor::disk_write_total_bytes_direct.Increment(nbyte);
}

void
FileWriter::WriteWithBufferedIO(const void* data, size_t nbyte) {
    PositionedWriteWithCheck(data, nbyte, file_size_);
    file_size_ += nbyte;
    milvus::monitor::disk_write_total_bytes_buffered.Increment(nbyte);
}

void
FileWriter::Write(const void* data, size_t nbyte) {
    AssertInfo(fd_ != -1, "FileWriter is not initialized or finished");
    if (nbyte == 0) {
        return;
    }

    auto promise = std::make_shared<folly::Promise<folly::Unit>>();
    auto future = promise->getFuture();
    auto task = [this, data, nbyte, promise]() {
        try {
            if (use_direct_io_) {
                WriteWithDirectIO(data, nbyte);
            } else {
                WriteWithBufferedIO(data, nbyte);
            }
            promise->setValue(folly::Unit{});
        } catch (...) {
            promise->setException(
                folly::exception_wrapper(std::current_exception()));
        }
    };

    if (FileWriteWorkerPool::GetInstance().AddTask(task)) {
        try {
            future.wait();
        } catch (const std::exception& e) {
            Cleanup();
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "Failed to write to file: {}, error: {}",
                      filename_,
                      e.what());
        }
    } else {
        if (use_direct_io_) {
            WriteWithDirectIO(data, nbyte);
        } else {
            WriteWithBufferedIO(data, nbyte);
        }
    }
}

void
FileWriter::FlushWithDirectIO() {
    size_t nearest_aligned_offset =
        (offset_ + ALIGNMENT_MASK) & ~ALIGNMENT_MASK;
    memset(static_cast<char*>(aligned_buf_) + offset_,
           0,
           nearest_aligned_offset - offset_);
    PositionedWriteWithCheck(aligned_buf_, nearest_aligned_offset, file_size_);
    file_size_ += offset_;
    milvus::monitor::disk_write_total_bytes_direct.Increment(offset_);
    // truncate the file to the actual size since the file written by the aligned buffer may be larger than the actual size
    if (ftruncate(fd_, file_size_) != 0) {
        Cleanup();
        ThrowInfo(ErrorCode::FileWriteFailed,
                  "Failed to truncate file: {}, error: {}",
                  filename_,
                  strerror(errno));
    }
    offset_ = 0;
}

size_t
FileWriter::Finish() {
    AssertInfo(fd_ != -1, "FileWriter is not initialized or finished");

    // if the aligned buffer is not empty, we should flush it to the file
    if (offset_ != 0) {
        auto promise = std::make_shared<folly::Promise<folly::Unit>>();
        auto future = promise->getFuture();
        auto task = [this, promise]() {
            try {
                if (use_direct_io_) {
                    FlushWithDirectIO();
                }
                promise->setValue(folly::Unit{});
            } catch (...) {
                promise->setException(
                    folly::exception_wrapper(std::current_exception()));
            }
        };

        if (FileWriteWorkerPool::GetInstance().AddTask(task)) {
            try {
                future.wait();
            } catch (const std::exception& e) {
                Cleanup();
                ThrowInfo(ErrorCode::FileWriteFailed,
                          "Failed to flush file: {}, error: {}",
                          filename_,
                          e.what());
            }
        } else {
            if (use_direct_io_) {
                FlushWithDirectIO();
            }
        }
    }

    // clean up the file writer
    Cleanup();

    // return the file size
    return file_size_;
}

FileWriter::WriteMode FileWriter::mode_ = FileWriter::WriteMode::BUFFERED;
size_t FileWriter::buffer_size_ = DEFAULT_BUFFER_SIZE;

void
FileWriter::SetMode(WriteMode mode) {
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
FileWriter::SetBufferSize(size_t buffer_size) {
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

FileWriter::WriteMode
FileWriter::GetMode() {
    return mode_;
}

size_t
FileWriter::GetBufferSize() {
    return buffer_size_;
}

}  // namespace milvus::storage
