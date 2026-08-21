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

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <limits>
#include <span>
#include <string>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::storage {

// Owns a pre-sized writable file mapping used as a direct remote-read target.
// Until Commit() succeeds at IndexFinalize, destruction removes the staging
// file. Keeping this object shared by all Slice tasks keeps the mapping alive
// while caller-owned async reads are outstanding.
class WritableMmapFile final {
 public:
    static std::shared_ptr<WritableMmapFile>
    Create(std::string path, size_t file_size) {
        auto target = std::shared_ptr<WritableMmapFile>(
            new WritableMmapFile(std::move(path), file_size));
        target->OpenAndMap();
        return target;
    }

    WritableMmapFile(const WritableMmapFile&) = delete;
    WritableMmapFile&
    operator=(const WritableMmapFile&) = delete;

    ~WritableMmapFile() {
        Finish();
        if (!committed_ && !path_.empty()) {
            ::unlink(path_.c_str());
        }
    }

    // Ends the writable materialization phase while retaining ownership of
    // the staging path. Commit() may still be called after IndexFinalize;
    // otherwise destruction removes the file.
    void
    Finish() noexcept {
        if (finished_) {
            return;
        }
        if (mapping_ != nullptr) {
            ::munmap(mapping_, file_size_);
            mapping_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        finished_ = true;
    }

    std::span<uint8_t>
    Region(size_t offset, size_t bytes) {
        AssertInfo(
            !finished_, "Writable mmap file '{}' is already finished", path_);
        AssertInfo(offset <= file_size_ && bytes <= file_size_ - offset,
                   "Writable mmap region [{}, {}) exceeds file '{}' size {}",
                   offset,
                   offset + bytes,
                   path_,
                   file_size_);
        if (bytes == 0) {
            return {};
        }
        return {mapping_ + offset, bytes};
    }

    const std::string&
    Path() const noexcept {
        return path_;
    }

    size_t
    Size() const noexcept {
        return file_size_;
    }

    void
    Commit() noexcept {
        committed_ = true;
    }

    bool
    Committed() const noexcept {
        return committed_;
    }

 private:
    WritableMmapFile(std::string path, size_t file_size)
        : path_(std::move(path)), file_size_(file_size) {
    }

    void
    OpenAndMap() {
        AssertInfo(file_size_ <=
                       static_cast<size_t>(std::numeric_limits<off_t>::max()),
                   "Writable mmap file '{}' size {} exceeds off_t range",
                   path_,
                   file_size_);
        fd_ =
            ::open(path_.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
        auto open_errno = errno;
        AssertInfo(fd_ >= 0,
                   "Failed to create writable mmap file '{}': {}",
                   path_,
                   std::strerror(open_errno));

        if (::ftruncate(fd_, static_cast<off_t>(file_size_)) != 0) {
            auto truncate_errno = errno;
            ::close(fd_);
            fd_ = -1;
            ::unlink(path_.c_str());
            ThrowInfo(ErrorCode::UnexpectedError,
                      "Failed to resize writable mmap file '{}': {}",
                      path_,
                      std::strerror(truncate_errno));
        }
        if (file_size_ == 0) {
            return;
        }

        void* mapping = ::mmap(
            nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (mapping == MAP_FAILED) {
            auto mmap_errno = errno;
            ::close(fd_);
            fd_ = -1;
            ::unlink(path_.c_str());
            ThrowInfo(ErrorCode::UnexpectedError,
                      "Failed to map writable mmap file '{}': {}",
                      path_,
                      std::strerror(mmap_errno));
        }
        mapping_ = static_cast<uint8_t*>(mapping);
    }

    std::string path_;
    size_t file_size_{0};
    int fd_{-1};
    uint8_t* mapping_{nullptr};
    bool finished_{false};
    bool committed_{false};
};

}  // namespace milvus::storage
