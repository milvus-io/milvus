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

#include "common/EasyAssert.h"
#include "storage/ThreadPools.h"

namespace milvus::storage {

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

class FileWriterConfig {
 public:
    enum class WriteMode : uint8_t { BUFFERED = 0, DIRECT = 1 };

    static FileWriterConfig&
    GetInstance() {
        static FileWriterConfig instance;
        return instance;
    }

    ~FileWriterConfig() = default;

    void
    SetMode(WriteMode mode) {
        if (mode != WriteMode::BUFFERED && mode != WriteMode::DIRECT) {
            throw std::invalid_argument(
                fmt::format("Invalid write mode: {}", static_cast<int>(mode)));
        }
        mode_ = mode;
    }

    void
    SetBufferSize(size_t buffer_size) {
        if (buffer_size > MAX_BUFFER_SIZE) {
            buffer_size = MAX_BUFFER_SIZE;
        } else if (buffer_size < MIN_BUFFER_SIZE) {
            buffer_size = MIN_BUFFER_SIZE;
        } else {
            buffer_size = (buffer_size + ALIGNMENT_MASK) & ~ALIGNMENT_MASK;
        }
        buffer_size_ = buffer_size;
    }

    WriteMode
    GetMode() const {
        return mode_;
    }

    size_t
    GetBufferSize() const {
        return buffer_size_;
    }

    FileWriterConfig(const FileWriterConfig&) = delete;
    FileWriterConfig&
    operator=(const FileWriterConfig&) = delete;
    FileWriterConfig(FileWriterConfig&&) = default;
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
