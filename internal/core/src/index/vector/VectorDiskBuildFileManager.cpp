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
#include <set>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "arrow/io/memory.h"
#include "storage/StatusToErrorCode.h"
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

}  // namespace

// Close preserves the size for AddFileMeta. TakeOutput transfers the backing
// buffer without another copy, after the engine has finished serialization.
class VectorDiskBuildFileManager::BufferedOutput final
    : public milvus::OutputStream {
 public:
    BufferedOutput(size_t expected_size,
                   std::weak_ptr<VectorDiskBuildFileManager> manager)
        : manager_(std::move(manager)) {
        AssertInfo(expected_size <=
                       static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                   "artifact entry size exceeds the storage size limit");
        auto result = arrow::io::BufferOutputStream::Create(expected_size);
        if (!result.ok()) {
            ThrowInfo(storage::ArrowStatusToErrorCode(result.status()),
                      "failed to allocate artifact entry: {}",
                      result.status().ToString());
        }
        buffer_ = std::move(result).ValueOrDie();
    }

    size_t
    Tell() const override {
        try {
            if (closed_) {
                return size_;
            }
            auto result = buffer_->Tell();
            if (!result.ok()) {
                ThrowInfo(storage::ArrowStatusToErrorCode(result.status()),
                          "failed to tell artifact entry position: {}",
                          result.status().ToString());
            }
            return result.ValueOrDie();
        } catch (...) {
            RecordFailure();
            throw;
        }
    }

    size_t
    Write(const void* data, size_t size) override {
        try {
            auto status = buffer_->Write(data, size);
            if (!status.ok()) {
                ThrowInfo(storage::ArrowStatusToErrorCode(status),
                          "failed to write artifact entry: {}",
                          status.ToString());
            }
            return size;
        } catch (...) {
            RecordFailure();
            throw;
        }
    }

    size_t
    Write(int fd, size_t size) override {
        try {
            std::vector<uint8_t> buffer(std::min(
                size, static_cast<size_t>(DEFAULT_INDEX_FILE_SLICE_SIZE)));
            size_t copied = 0;
            while (copied < size) {
                const auto chunk = std::min(buffer.size(), size - copied);
                storage::ReadAll(fd,
                                 buffer.data(),
                                 chunk,
                                 std::to_string(fd),
                                 "artifact entry source fd");
                Write(buffer.data(), chunk);
                copied += chunk;
            }
            return copied;
        } catch (...) {
            RecordFailure();
            throw;
        }
    }

    void
    Close() override {
        try {
            if (!closed_) {
                const auto size = Tell();
                auto status = buffer_->Close();
                if (!status.ok()) {
                    ThrowInfo(storage::ArrowStatusToErrorCode(status),
                              "failed to close artifact entry: {}",
                              status.ToString());
                }
                size_ = size;
                closed_ = true;
            }
        } catch (...) {
            RecordFailure();
            throw;
        }
    }

    std::shared_ptr<arrow::Buffer>
    Finish() {
        Close();
        auto result = buffer_->Finish();
        if (!result.ok()) {
            ThrowInfo(storage::ArrowStatusToErrorCode(result.status()),
                      "failed to finish artifact entry: {}",
                      result.status().ToString());
        }
        return std::move(result).ValueOrDie();
    }

 private:
    void
    RecordFailure() const noexcept {
        if (auto manager = manager_.lock()) {
            manager->RecordFailure(std::current_exception());
        }
    }

    std::weak_ptr<VectorDiskBuildFileManager> manager_;
    std::shared_ptr<arrow::io::BufferOutputStream> buffer_;
    size_t size_{0};
    bool closed_{false};
};

VectorDiskBuildFileManager::VectorDiskBuildFileManager(
    std::shared_ptr<storage::LocalDirectory> local_files)
    : local_files_(std::move(local_files)) {
    AssertInfo(local_files_ != nullptr,
               "vector disk build file owner is missing");
}

void
VectorDiskBuildFileManager::SetFileLoader(
    std::function<void(const std::string&)> loader) {
    std::lock_guard lock(mutex_);
    file_loader_ = std::move(loader);
}

bool
VectorDiskBuildFileManager::LoadFile(const std::string& filename) {
    try {
        std::lock_guard lock(mutex_);
        if (file_loader_) {
            file_loader_(filename);
        }
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
        std::shared_ptr<BufferedOutput> stream;
        {
            std::lock_guard lock(mutex_);
            const auto entry = output_streams_.find(file_meta.file_path);
            if (entry != output_streams_.end()) {
                stream = entry->second;
            }
        }
        AssertInfo(stream && stream->Tell() == file_meta.file_size,
                   "vector output {} size disagrees with registered size {}",
                   file_meta.file_path,
                   file_meta.file_size);
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
VectorDiskBuildFileManager::OpenOutputStream(const std::string& filename,
                                             size_t file_size) {
    try {
        if (!local_files_->Owns(filename)) {
            ThrowInfo(FileWriteFailed,
                      "refusing to open unowned vector output {}",
                      filename);
        }
        std::lock_guard lock(mutex_);
        AssertInfo(
            !output_streams_.contains(filename) && !files_.contains(filename),
            "duplicate vector output {}",
            filename);
        auto stream =
            std::make_shared<BufferedOutput>(file_size, weak_from_this());
        output_streams_.emplace(filename, stream);
        return stream;
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
    const auto [entry, inserted] = files_.emplace(
        filename, VectorDiskArtifactFile{filename, size, transport});
    if (!inserted &&
        (entry->second.size != size || entry->second.transport != transport)) {
        ThrowInfo(FileWriteFailed,
                  "local vector output {} has conflicting size or transport",
                  filename);
    }
}

std::vector<VectorDiskArtifactFile>
VectorDiskBuildFileManager::LocalFiles() const {
    std::vector<VectorDiskArtifactFile> files;
    {
        std::lock_guard lock(mutex_);
        files.reserve(files_.size());
        for (const auto& entry : files_) {
            files.push_back(entry.second);
        }
    }
    std::sort(files.begin(), files.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.path < rhs.path;
    });

    // Recheck before publication: files may have changed since registration.
    std::set<std::string> names;
    for (const auto& file : files) {
        AssertInfo(file.transport == VectorDiskFileTransport::LegacySliced ||
                       file.transport == VectorDiskFileTransport::RawUnsliced,
                   "vector disk artifact file has an invalid transport");
        std::error_code error;
        if (!local_files_->Owns(file.path) ||
            !std::filesystem::is_regular_file(file.path, error) || error) {
            ThrowInfo(FileReadFailed,
                      "vector disk artifact file {} is unavailable{}{}",
                      file.path,
                      error ? ": " : "",
                      error ? error.message() : "");
        }
        const auto actual_size = std::filesystem::file_size(file.path, error);
        if (error || actual_size != file.size) {
            ThrowInfo(FileReadFailed,
                      "vector disk artifact file {} size changed from {}{}{}",
                      file.path,
                      file.size,
                      error ? ": " : " to ",
                      error ? error.message() : std::to_string(actual_size));
        }
        const auto name = std::filesystem::path(file.path).filename().string();
        AssertInfo(!name.empty() && names.insert(name).second,
                   "vector disk artifact has a duplicate or empty file name "
                   "{}",
                   name);
    }
    return files;
}

std::map<std::string, std::shared_ptr<arrow::Buffer>>
VectorDiskBuildFileManager::TakeOutput() {
    std::map<std::string, std::shared_ptr<BufferedOutput>> streams;
    {
        std::lock_guard lock(mutex_);
        streams.swap(output_streams_);
    }
    std::map<std::string, std::shared_ptr<arrow::Buffer>> output;
    for (const auto& [path, stream] : streams) {
        output.emplace(path, stream->Finish());
    }
    return output;
}

void
VectorDiskBuildFileManager::ResetOutput() {
    std::lock_guard lock(mutex_);
    output_streams_.clear();
    first_failure_ = nullptr;
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

}  // namespace milvus::index
