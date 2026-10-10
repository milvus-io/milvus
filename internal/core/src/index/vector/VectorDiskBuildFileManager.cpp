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

#include "index/vector/VectorDiskBuildFileManager.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/EasyAssert.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"

namespace milvus::index {
namespace {

class LocalInput final : public milvus::InputStream {
 public:
    LocalInput(std::string filename,
               std::shared_ptr<VectorDiskBuildFileManager> manager)
        : filename_(std::move(filename)),
          manager_(std::move(manager)),
          fd_(::open(filename_.c_str(), O_RDONLY)) {
        if (fd_.Get() < 0) {
            ThrowInfo(FileOpenFailed,
                      "failed to open local vector input {}: {}",
                      filename_,
                      std::strerror(errno));
        }
        struct stat stat {};
        if (::fstat(fd_.Get(), &stat) != 0 || stat.st_size < 0) {
            const auto error = errno;
            ThrowInfo(FileReadFailed,
                      "failed to stat local vector input {}: {}",
                      filename_,
                      std::strerror(error));
        }
        size_ = static_cast<size_t>(stat.st_size);
    }

    ~LocalInput() override = default;

    size_t
    Size() const override {
        return size_;
    }

    bool
    Seek(int64_t offset) override {
        if (offset < 0 || static_cast<uint64_t>(offset) > size_) {
            return false;
        }
        std::lock_guard lock(mutex_);
        offset_ = static_cast<size_t>(offset);
        return true;
    }

    size_t
    Tell() const override {
        std::lock_guard lock(mutex_);
        return offset_;
    }

    size_t
    Read(void* ptr, size_t size) override {
        try {
            std::lock_guard lock(mutex_);
            const auto read = ReadAtUnlocked(ptr, offset_, size);
            offset_ += read;
            return read;
        } catch (...) {
            manager_->RecordFailure(std::current_exception());
            throw;
        }
    }

    size_t
    ReadAt(void* ptr, size_t offset, size_t size) override {
        try {
            return ReadAtUnlocked(ptr, offset, size);
        } catch (...) {
            manager_->RecordFailure(std::current_exception());
            throw;
        }
    }

    size_t
    Read(int destination_fd, size_t size) override {
        try {
            constexpr size_t buffer_size = 1024 * 1024;
            std::vector<uint8_t> buffer(std::min(buffer_size, size));
            size_t copied = 0;
            while (copied < size) {
                const auto chunk = std::min(buffer.size(), size - copied);
                const auto read = Read(buffer.data(), chunk);
                if (read != chunk) {
                    ThrowInfo(FileReadFailed,
                              "local vector input {} ended before {} bytes",
                              filename_,
                              size);
                }
                storage::WriteAll(destination_fd,
                                  buffer.data(),
                                  read,
                                  filename_,
                                  "failed to copy local vector input");
                copied += read;
            }
            return copied;
        } catch (...) {
            manager_->RecordFailure(std::current_exception());
            throw;
        }
    }

    bool
    Eof() const override {
        std::lock_guard lock(mutex_);
        return offset_ == size_;
    }

 private:
    size_t
    ReadAtUnlocked(void* ptr, size_t offset, size_t size) const {
        if (size > 0 && ptr == nullptr) {
            ThrowInfo(FileReadFailed,
                      "local vector input {} has a null destination",
                      filename_);
        }
        if (offset > size_ || size > size_ - offset) {
            ThrowInfo(FileReadFailed,
                      "local vector input {} read is out of range",
                      filename_);
        }
        size_t total = 0;
        while (total < size) {
            const auto result = ::pread(fd_.Get(),
                                        static_cast<uint8_t*>(ptr) + total,
                                        size - total,
                                        static_cast<off_t>(offset + total));
            if (result < 0 && errno == EINTR) {
                continue;
            }
            if (result <= 0) {
                ThrowInfo(
                    FileReadFailed,
                    "failed to read local vector input {}: {}",
                    filename_,
                    result == 0 ? "unexpected EOF" : std::strerror(errno));
            }
            total += static_cast<size_t>(result);
        }
        return total;
    }

    std::string filename_;
    std::shared_ptr<VectorDiskBuildFileManager> manager_;
    storage::FileDescriptorGuard fd_;
    size_t size_{0};
    mutable std::mutex mutex_;
    size_t offset_{0};
};

class LocalOutput final : public milvus::OutputStream {
 public:
    LocalOutput(std::string filename,
                std::shared_ptr<VectorDiskBuildFileManager> manager)
        : filename_(std::move(filename)),
          manager_(std::move(manager)),
          fd_(::open(filename_.c_str(),
                     O_CREAT | O_TRUNC | O_WRONLY,
                     S_IRUSR | S_IWUSR)) {
        if (fd_ < 0) {
            ThrowInfo(FileOpenFailed,
                      "failed to open local vector output {}: {}",
                      filename_,
                      std::strerror(errno));
        }
    }

    ~LocalOutput() override {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        if (!closed_) {
            try {
                ThrowInfo(FileWriteFailed,
                          "local vector output {} was destroyed before Close",
                          filename_);
            } catch (...) {
                manager_->RecordFailure(std::current_exception());
            }
        }
    }

    size_t
    Tell() const override {
        return offset_;
    }

    size_t
    Write(const void* ptr, size_t size) override {
        try {
            if (closed_) {
                ThrowInfo(FileWriteFailed,
                          "local vector output {} is closed",
                          filename_);
            }
            if (size > 0 && ptr == nullptr) {
                ThrowInfo(FileWriteFailed,
                          "local vector output {} has a null source",
                          filename_);
            }
            storage::WriteAll(fd_,
                              ptr,
                              size,
                              filename_,
                              "failed to write local vector output");
            offset_ += size;
            return size;
        } catch (...) {
            manager_->RecordFailure(std::current_exception());
            throw;
        }
    }

    size_t
    Write(int source_fd, size_t size) override {
        try {
            constexpr size_t buffer_size = 1024 * 1024;
            std::vector<uint8_t> buffer(std::min(buffer_size, size));
            size_t copied = 0;
            while (copied < size) {
                const auto chunk = std::min(buffer.size(), size - copied);
                ssize_t read = 0;
                do {
                    read = ::read(source_fd, buffer.data(), chunk);
                } while (read < 0 && errno == EINTR);
                if (read <= 0) {
                    ThrowInfo(
                        FileReadFailed,
                        "failed to read source fd for local vector output "
                        "{}: {}",
                        filename_,
                        read == 0 ? "unexpected EOF" : std::strerror(errno));
                }
                Write(buffer.data(), static_cast<size_t>(read));
                copied += static_cast<size_t>(read);
            }
            return copied;
        } catch (...) {
            manager_->RecordFailure(std::current_exception());
            throw;
        }
    }

    void
    Close() override {
        try {
            if (closed_) {
                return;
            }
            if (::fsync(fd_) != 0) {
                ThrowInfo(FileWriteFailed,
                          "failed to flush local vector output {}: {}",
                          filename_,
                          std::strerror(errno));
            }
            if (::close(fd_) != 0) {
                fd_ = -1;
                ThrowInfo(FileWriteFailed,
                          "failed to close local vector output {}: {}",
                          filename_,
                          std::strerror(errno));
            }
            fd_ = -1;
            closed_ = true;
            manager_->CompleteOutput(filename_, offset_);
        } catch (...) {
            manager_->RecordFailure(std::current_exception());
            throw;
        }
    }

 private:
    std::string filename_;
    std::shared_ptr<VectorDiskBuildFileManager> manager_;
    int fd_{-1};
    size_t offset_{0};
    bool closed_{false};
};

}  // namespace

VectorDiskBuildFileManager::VectorDiskBuildFileManager(
    std::shared_ptr<storage::LocalDirectory> local_files)
    : local_files_(std::move(local_files)) {
    AssertInfo(local_files_ != nullptr,
               "vector disk build file owner is missing");
}

bool
VectorDiskBuildFileManager::LoadFile(const std::string& filename) {
    try {
        std::error_code error;
        const bool exists = std::filesystem::is_regular_file(filename, error);
        if (error || !exists) {
            ThrowInfo(FileReadFailed,
                      "local vector build input {} is not readable{}{}",
                      filename,
                      error ? ": " : "",
                      error ? error.message() : "");
        }
        return true;
    } catch (...) {
        RecordFailure(std::current_exception());
        return false;
    }
}

bool
VectorDiskBuildFileManager::AddFile(const std::string& filename) {
    try {
        RegisterOwnedFile(filename);
        return true;
    } catch (...) {
        RecordFailure(std::current_exception());
        return false;
    }
}

bool
VectorDiskBuildFileManager::AddFileMeta(const milvus::FileMeta& file_meta) {
    try {
        if (!local_files_->Owns(file_meta.file_path)) {
            ThrowInfo(FileWriteFailed,
                      "refusing to register unowned vector output metadata {}",
                      file_meta.file_path);
        }
        std::error_code error;
        const auto actual =
            std::filesystem::file_size(file_meta.file_path, error);
        if (error || actual != file_meta.file_size) {
            ThrowInfo(FileWriteFailed,
                      "local vector output {} size disagrees with registered "
                      "size {}",
                      file_meta.file_path,
                      file_meta.file_size);
        }
        std::lock_guard lock(mutex_);
        AssertInfo(
            open_outputs_.find(file_meta.file_path) != open_outputs_.end(),
            "local vector output metadata {} has no open stream",
            file_meta.file_path);
        AssertInfo(files_.find(file_meta.file_path) == files_.end(),
                   "local vector output {} is already complete",
                   file_meta.file_path);
        const auto [entry, inserted] =
            pending_meta_.emplace(file_meta.file_path, file_meta.file_size);
        if (!inserted && entry->second != file_meta.file_size) {
            ThrowInfo(FileWriteFailed,
                      "local vector output {} has conflicting metadata sizes",
                      file_meta.file_path);
        }
        return true;
    } catch (...) {
        RecordFailure(std::current_exception());
        return false;
    }
}

std::optional<bool>
VectorDiskBuildFileManager::IsExisted(const std::string& filename) {
    try {
        std::error_code error;
        const bool exists = std::filesystem::exists(filename, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect local vector file {}: {}",
                      filename,
                      error.message());
        }
        return exists;
    } catch (...) {
        RecordFailure(std::current_exception());
        return std::nullopt;
    }
}

bool
VectorDiskBuildFileManager::RemoveFile(const std::string& filename) {
    try {
        if (!local_files_->Owns(filename)) {
            ThrowInfo(FileWriteFailed,
                      "refusing to remove unowned vector file {}",
                      filename);
        }
        std::error_code error;
        std::filesystem::remove(filename, error);
        if (error) {
            ThrowInfo(FileWriteFailed,
                      "failed to remove local vector file {}: {}",
                      filename,
                      error.message());
        }
        std::lock_guard lock(mutex_);
        files_.erase(filename);
        return true;
    } catch (...) {
        RecordFailure(std::current_exception());
        return false;
    }
}

std::shared_ptr<milvus::InputStream>
VectorDiskBuildFileManager::OpenInputStream(const std::string& filename) {
    try {
        if (!local_files_->Owns(filename)) {
            ThrowInfo(FileReadFailed,
                      "refusing to open unowned local vector input {}",
                      filename);
        }
        return std::make_shared<LocalInput>(filename, shared_from_this());
    } catch (...) {
        RecordFailure(std::current_exception());
        throw;
    }
}

std::shared_ptr<milvus::OutputStream>
VectorDiskBuildFileManager::OpenOutputStream(const std::string& filename) {
    try {
        if (!local_files_->Owns(filename)) {
            ThrowInfo(FileWriteFailed,
                      "refusing to open unowned vector output {}",
                      filename);
        }
        std::error_code error;
        std::filesystem::create_directories(
            std::filesystem::path(filename).parent_path(), error);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to create local vector output parent for {}: {}",
                      filename,
                      error.message());
        }
        BeginOutput(filename);
        try {
            return std::make_shared<LocalOutput>(filename, shared_from_this());
        } catch (...) {
            AbortOutput(filename);
            throw;
        }
    } catch (...) {
        RecordFailure(std::current_exception());
        throw;
    }
}

const std::string&
VectorDiskBuildFileManager::Directory() const {
    return local_files_->Path();
}

std::string
VectorDiskBuildFileManager::IndexPrefix() const {
    return Directory() + std::filesystem::path::preferred_separator;
}

void
VectorDiskBuildFileManager::RegisterOwnedFile(const std::string& filename) {
    RegisterOwnedFile(filename, VectorDiskFileTransport::LegacySliced);
}

void
VectorDiskBuildFileManager::RegisterOwnedFile(
    const std::string& filename, VectorDiskFileTransport transport) {
    if (!local_files_->Owns(filename)) {
        ThrowInfo(FileWriteFailed,
                  "refusing to register unowned vector output {}",
                  filename);
    }
    std::error_code error;
    const auto regular = std::filesystem::is_regular_file(filename, error);
    if (error || !regular) {
        ThrowInfo(FileReadFailed,
                  "local vector output {} is not a completed regular file{}{}",
                  filename,
                  error ? ": " : "",
                  error ? error.message() : "");
    }
    const auto size = std::filesystem::file_size(filename, error);
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to stat local vector output {}: {}",
                  filename,
                  error.message());
    }
    switch (transport) {
        case VectorDiskFileTransport::LegacySliced:
        case VectorDiskFileTransport::RawUnsliced:
            break;
        default:
            AssertInfo(false,
                       "invalid vector disk artifact transport for {}",
                       filename);
    }
    std::lock_guard lock(mutex_);
    AssertInfo(open_outputs_.find(filename) == open_outputs_.end() &&
                   pending_meta_.find(filename) == pending_meta_.end(),
               "local vector output {} is incomplete",
               filename);
    RegisterCompletedFileLocked(filename, size, transport);
}

void
VectorDiskBuildFileManager::BeginOutput(const std::string& filename) {
    if (!local_files_->Owns(filename)) {
        ThrowInfo(FileWriteFailed,
                  "refusing to track unowned vector output {}",
                  filename);
    }
    std::lock_guard lock(mutex_);
    AssertInfo(files_.find(filename) == files_.end(),
               "local vector output {} is already complete",
               filename);
    const auto [_, inserted] = open_outputs_.emplace(filename, 0);
    if (!inserted) {
        ThrowInfo(FileWriteFailed,
                  "local vector output {} is already open",
                  filename);
    }
}

void
VectorDiskBuildFileManager::CompleteOutput(const std::string& filename,
                                           size_t size) {
    std::lock_guard lock(mutex_);
    const auto open = open_outputs_.find(filename);
    if (open == open_outputs_.end()) {
        ThrowInfo(FileWriteFailed,
                  "local vector output {} was not registered as open",
                  filename);
    }
    const auto pending = pending_meta_.find(filename);
    if (pending != pending_meta_.end() && pending->second != size) {
        ThrowInfo(FileWriteFailed,
                  "local vector output {} closed at size {}, metadata says {}",
                  filename,
                  size,
                  pending->second);
    }
    RegisterCompletedFileLocked(
        filename, size, VectorDiskFileTransport::RawUnsliced);
    open_outputs_.erase(open);
    if (pending != pending_meta_.end()) {
        pending_meta_.erase(pending);
    }
}

std::vector<VectorDiskArtifactFile>
VectorDiskBuildFileManager::Files() const {
    std::lock_guard lock(mutex_);
    if (!open_outputs_.empty() || !pending_meta_.empty()) {
        ThrowInfo(FileWriteFailed,
                  "vector disk build has incomplete local output streams");
    }
    std::vector<VectorDiskArtifactFile> files;
    files.reserve(files_.size());
    for (const auto& entry : files_) {
        files.push_back(entry.second);
    }
    std::sort(files.begin(), files.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.path < rhs.path;
    });
    return files;
}

void
VectorDiskBuildFileManager::RegisterCompletedFileLocked(
    const std::string& filename,
    size_t size,
    VectorDiskFileTransport transport) {
    auto [entry, inserted] = files_.emplace(
        filename, VectorDiskArtifactFile{filename, size, transport});
    if (!inserted &&
        (entry->second.size != size || entry->second.transport != transport)) {
        ThrowInfo(FileWriteFailed,
                  "local vector output {} has conflicting size or transport",
                  filename);
    }
}

void
VectorDiskBuildFileManager::RethrowFirstFailure() const {
    std::exception_ptr failure;
    {
        std::lock_guard lock(mutex_);
        failure = first_failure_;
    }
    if (failure != nullptr) {
        std::rethrow_exception(failure);
    }
}

void
VectorDiskBuildFileManager::RecordFailure(std::exception_ptr failure) noexcept {
    try {
        std::lock_guard lock(mutex_);
        if (first_failure_ == nullptr) {
            first_failure_ = std::move(failure);
        }
    } catch (...) {
    }
}

void
VectorDiskBuildFileManager::AbortOutput(const std::string& filename) noexcept {
    try {
        std::lock_guard lock(mutex_);
        open_outputs_.erase(filename);
        pending_meta_.erase(filename);
    } catch (...) {
    }
}

}  // namespace milvus::index
