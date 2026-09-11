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
        CleanupNoexcept();
        if (owns_file_ && !committed_) {
            ::unlink(path_.c_str());
        }
    }

    // Ends the writable materialization phase. Call only after all mapping
    // writers have joined. Throws MmapError for mapping cleanup failures and
    // FileWriteFailed for descriptor close failures.
    void
    Finish() {
        if (state_ == State::Finished) {
            return;
        }
        AssertInfo(state_ == State::Writable,
                   "Writable mmap file '{}' cannot be finished after a "
                   "previous finish failure",
                   path_);

        auto error_code = ErrorCode::Success;
        int finish_errno = 0;
        const char* failed_operation = nullptr;
        const auto record_error = [&](ErrorCode code, const char* operation) {
            if (finish_errno == 0) {
                error_code = code;
                finish_errno = errno;
                failed_operation = operation;
            }
        };

        if (mapping_ != nullptr) {
            if (::munmap(mapping_, file_size_) != 0) {
                record_error(ErrorCode::MmapError, "unmap");
            } else {
                mapping_ = nullptr;
            }
        }
        if (fd_ >= 0) {
            if (::close(fd_) != 0) {
                record_error(ErrorCode::FileWriteFailed, "close");
            }
            // On Linux close() may release the descriptor even when it reports
            // EINTR, so retrying could close an unrelated reused descriptor.
            fd_ = -1;
        }
        if (finish_errno != 0) {
            state_ = State::Failed;
            ThrowInfo(error_code,
                      "Failed to {} writable mmap file '{}': {}",
                      failed_operation,
                      path_,
                      std::strerror(finish_errno));
        }
        state_ = State::Finished;
    }

    std::span<uint8_t>
    Region(size_t offset, size_t bytes) {
        AssertInfo(state_ == State::Writable,
                   "Writable mmap file '{}' is not writable",
                   path_);
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

    // Marks a successfully finished staging file as retained.
    void
    Commit() {
        AssertInfo(state_ == State::Finished,
                   "Writable mmap file '{}' must be finished before commit",
                   path_);
        committed_ = true;
    }

    bool
    Committed() const noexcept {
        return committed_;
    }

 private:
    enum class State { Writable, Finished, Failed };

    WritableMmapFile(std::string path, size_t file_size)
        : path_(std::move(path)), file_size_(file_size) {
    }

    // Reserves filesystem blocks before exposing the mapping so ENOSPC is
    // reported synchronously instead of arriving as SIGBUS in a writer.
    int
    Preallocate() noexcept {
#ifdef __APPLE__
        fstore_t store{
            F_ALLOCATECONTIG,
            F_PEOFPOSMODE,
            0,
            static_cast<off_t>(file_size_),
            0,
        };
        if (::fcntl(fd_, F_PREALLOCATE, &store) != 0) {
            store.fst_flags = F_ALLOCATEALL;
            if (::fcntl(fd_, F_PREALLOCATE, &store) != 0) {
                return errno;
            }
        }
        if (::ftruncate(fd_, static_cast<off_t>(file_size_)) != 0) {
            return errno;
        }
        return 0;
#elif defined(__linux__)
        int result;
        do {
            result = ::fallocate(fd_, 0, 0, static_cast<off_t>(file_size_));
        } while (result != 0 && errno == EINTR);
        if (result == 0) {
            return 0;
        }
        const int allocation_errno = errno;
        if (allocation_errno == EOPNOTSUPP || allocation_errno == ENOSYS) {
            return ::posix_fallocate(fd_, 0, static_cast<off_t>(file_size_));
        }
        return allocation_errno;
#else
        return ::posix_fallocate(fd_, 0, static_cast<off_t>(file_size_));
#endif
    }

    // Opens, preallocates, and maps the staging file. Partial construction is
    // cleaned by the owning shared_ptr if any operation throws.
    void
    OpenAndMap() {
        AssertInfo(file_size_ <=
                       static_cast<size_t>(std::numeric_limits<off_t>::max()),
                   "Writable mmap file '{}' size {} exceeds off_t range",
                   path_,
                   file_size_);
        fd_ =
            ::open(path_.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
        if (fd_ < 0) {
            const auto open_errno = errno;
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "Failed to create writable mmap file '{}': {}",
                      path_,
                      std::strerror(open_errno));
        }
        owns_file_ = true;
        if (file_size_ == 0) {
            return;
        }
        if (const auto preallocate_errno = Preallocate();
            preallocate_errno != 0) {
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "Failed to preallocate writable mmap file '{}': {}",
                      path_,
                      std::strerror(preallocate_errno));
        }

        void* mapping = ::mmap(
            nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (mapping == MAP_FAILED) {
            const auto mmap_errno = errno;
            ThrowInfo(ErrorCode::MmapError,
                      "Failed to map writable mmap file '{}': {}",
                      path_,
                      std::strerror(mmap_errno));
        }
        mapping_ = static_cast<uint8_t*>(mapping);
    }

    // Best-effort release for exception unwinding and destruction.
    void
    CleanupNoexcept() noexcept {
        if (mapping_ != nullptr) {
            ::munmap(mapping_, file_size_);
            mapping_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    std::string path_;
    size_t file_size_{0};
    int fd_{-1};
    uint8_t* mapping_{nullptr};
    State state_{State::Writable};
    bool owns_file_{false};
    bool committed_{false};
};

}  // namespace milvus::storage
