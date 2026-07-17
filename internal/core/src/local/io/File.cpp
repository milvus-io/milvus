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

#include "local/io/File.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::local::io {
namespace {

off_t
CheckedOffset(uint64_t offset, const std::filesystem::path& path) {
    if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "local file offset {} exceeds the platform limit for {}",
                  offset,
                  path.string());
    }
    return static_cast<off_t>(offset);
}

off_t
CheckedOffset(uint64_t offset,
              size_t delta,
              const std::filesystem::path& path) {
    if (delta > std::numeric_limits<uint64_t>::max() - offset) {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "local file offset {} plus length {} overflows for {}",
                  offset,
                  delta,
                  path.string());
    }
    return CheckedOffset(offset + delta, path);
}

uint64_t
FileSize(int fd, const std::filesystem::path& path) {
    struct stat status {};
    if (fstat(fd, &status) != 0) {
        ThrowInfo(ErrorCode::FileReadFailed,
                  "failed to inspect local file {}, error: {}",
                  path.string(),
                  strerror(errno));
    }
    return static_cast<uint64_t>(status.st_size);
}

}  // namespace

RandomAccessFile::RandomAccessFile(int fd,
                                   std::filesystem::path debug_path) noexcept
    : fd_(fd), debug_path_(std::move(debug_path)) {
}

RandomAccessFile::RandomAccessFile(RandomAccessFile&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)),
      debug_path_(std::move(other.debug_path_)) {
}

RandomAccessFile&
RandomAccessFile::operator=(RandomAccessFile&& other) noexcept {
    if (this != &other) {
        Close();
        fd_ = std::exchange(other.fd_, -1);
        debug_path_ = std::move(other.debug_path_);
    }
    return *this;
}

RandomAccessFile::~RandomAccessFile() {
    Close();
}

void
RandomAccessFile::Close() noexcept {
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
}

size_t
RandomAccessFile::ReadAt(uint64_t offset, std::span<std::byte> output) const {
    size_t total = 0;
    while (total < output.size()) {
        auto current_offset = CheckedOffset(offset, total, debug_path_);
        auto read_size = pread(
            fd_, output.data() + total, output.size() - total, current_offset);
        if (read_size < 0) {
            if (errno == EINTR) {
                continue;
            }
            ThrowInfo(ErrorCode::FileReadFailed,
                      "failed to read local file {} at offset {}, error: {}",
                      debug_path_.string(),
                      static_cast<uint64_t>(current_offset),
                      strerror(errno));
        }
        if (read_size == 0) {
            break;
        }
        total += static_cast<size_t>(read_size);
    }
    return total;
}

uint64_t
RandomAccessFile::Size() const {
    return FileSize(fd_, debug_path_);
}

WritableFile::WritableFile(int fd,
                           std::filesystem::path debug_path,
                           bool direct_io) noexcept
    : fd_(fd), debug_path_(std::move(debug_path)), direct_io_(direct_io) {
}

WritableFile::WritableFile(WritableFile&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)),
      debug_path_(std::move(other.debug_path_)),
      direct_io_(std::exchange(other.direct_io_, false)) {
}

WritableFile&
WritableFile::operator=(WritableFile&& other) noexcept {
    if (this != &other) {
        Close();
        fd_ = std::exchange(other.fd_, -1);
        debug_path_ = std::move(other.debug_path_);
        direct_io_ = std::exchange(other.direct_io_, false);
    }
    return *this;
}

WritableFile::~WritableFile() {
    Close();
}

void
WritableFile::Close() noexcept {
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
}

size_t
WritableFile::Write(std::span<const std::byte> data) {
    size_t total = 0;
    while (total < data.size()) {
        auto written = write(fd_, data.data() + total, data.size() - total);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "failed to write local file {}, error: {}",
                      debug_path_.string(),
                      strerror(errno));
        }
        if (written == 0) {
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "write to local file {} made no progress",
                      debug_path_.string());
        }
        total += static_cast<size_t>(written);
    }
    return total;
}

size_t
WritableFile::WriteAt(uint64_t offset, std::span<const std::byte> data) {
    size_t total = 0;
    while (total < data.size()) {
        auto current_offset = CheckedOffset(offset, total, debug_path_);
        auto written = pwrite(
            fd_, data.data() + total, data.size() - total, current_offset);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "failed to write local file {} at offset {}, error: {}",
                      debug_path_.string(),
                      static_cast<uint64_t>(current_offset),
                      strerror(errno));
        }
        if (written == 0) {
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "write to local file {} at offset {} made no progress",
                      debug_path_.string(),
                      static_cast<uint64_t>(current_offset));
        }
        total += static_cast<size_t>(written);
    }
    return total;
}

uint64_t
WritableFile::Size() const {
    return FileSize(fd_, debug_path_);
}

void
WritableFile::Truncate(uint64_t size) {
    auto checked_size = CheckedOffset(size, debug_path_);
    if (ftruncate(fd_, checked_size) != 0) {
        ThrowInfo(ErrorCode::FileWriteFailed,
                  "failed to truncate local file {} to {}, error: {}",
                  debug_path_.string(),
                  size,
                  strerror(errno));
    }
}

void
WritableFile::Sync() {
    if (fsync(fd_) != 0) {
        ThrowInfo(ErrorCode::FileWriteFailed,
                  "failed to sync local file {}, error: {}",
                  debug_path_.string(),
                  strerror(errno));
    }
}

}  // namespace milvus::local::io
