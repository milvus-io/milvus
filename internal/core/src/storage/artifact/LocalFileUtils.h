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
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/artifact/FileSink.h"

namespace milvus::storage {

enum class LocalEntrySelection {
    RegularFiles,
    NonDirectories,
};

// These selections intentionally differ for non-regular entries. Preserve
// each format's selection before sorting by path for deterministic output.
inline std::vector<std::filesystem::path>
ListLocalFiles(const std::string& directory,
               LocalEntrySelection selection,
               std::string_view context) {
    std::vector<std::filesystem::path> files;
    std::error_code error;
    for (std::filesystem::directory_iterator it(directory, error), end;
         !error && it != end;
         it.increment(error)) {
        if (selection == LocalEntrySelection::RegularFiles) {
            if (it->is_regular_file(error)) {
                files.push_back(it->path());
            } else if (error) {
                break;
            }
        } else if (!it->is_directory(error)) {
            if (error) {
                break;
            }
            files.push_back(it->path());
        }
    }
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to enumerate {} directory {}: {}",
                  context,
                  directory,
                  error.message());
    }
    std::sort(files.begin(), files.end());
    return files;
}

inline void
WriteEntriesFromLocalFiles(
    FileSink& sink,
    std::span<const std::filesystem::path> files) {
    for (const auto& file : files) {
        sink.WriteEntryFromLocalFile(file.filename().string(), file.string());
    }
}

// Cleanup during unwinding is best-effort; callers choose when failures from
// normal-path cleanup must be reported. Diagnostic context is not retained.
class LocalEntryGuard {
 public:
    explicit LocalEntryGuard(std::string path) : path_(std::move(path)) {
    }

    ~LocalEntryGuard() {
        Remove(false, {});
    }

    LocalEntryGuard(const LocalEntryGuard&) = delete;
    LocalEntryGuard&
    operator=(const LocalEntryGuard&) = delete;

    LocalEntryGuard(LocalEntryGuard&& other) noexcept
        : path_(other.Release()) {
    }

    LocalEntryGuard&
    operator=(LocalEntryGuard&& other) noexcept {
        if (this != &other) {
            Remove(false, {});
            path_ = other.Release();
        }
        return *this;
    }

    const std::string&
    Path() const {
        return path_;
    }

    char*
    MutablePath() {
        return path_.data();
    }

    std::string
    Release() noexcept {
        return std::exchange(path_, {});
    }

    void
    RemoveChecked(std::string_view context) {
        Remove(true, context);
    }

 private:
    void
    Remove(bool report_error, std::string_view context) {
        if (path_.empty()) {
            return;
        }
        const auto path = std::exchange(path_, {});
        if (::unlink(path.c_str()) != 0 && errno != ENOENT && report_error) {
            ThrowInfo(FileWriteFailed,
                      "failed to remove {} {}: {}",
                      context,
                      path,
                      std::strerror(errno));
        }
    }

    std::string path_;
};

class MappedRegionGuard {
 public:
    MappedRegionGuard() = default;

    MappedRegionGuard(char* data, size_t size) : data_(data), size_(size) {
    }

    ~MappedRegionGuard() {
        Reset();
    }

    MappedRegionGuard(const MappedRegionGuard&) = delete;
    MappedRegionGuard&
    operator=(const MappedRegionGuard&) = delete;

    MappedRegionGuard(MappedRegionGuard&& other) noexcept
        : data_(std::exchange(other.data_, nullptr)),
          size_(std::exchange(other.size_, 0)) {
    }

    MappedRegionGuard&
    operator=(MappedRegionGuard&& other) noexcept {
        if (this != &other) {
            Reset();
            data_ = std::exchange(other.data_, nullptr);
            size_ = std::exchange(other.size_, 0);
        }
        return *this;
    }

    char*
    Data() const {
        return data_;
    }

    size_t
    Size() const {
        return size_;
    }

    char*
    Release() noexcept {
        size_ = 0;
        return std::exchange(data_, nullptr);
    }

 private:
    void
    Reset() noexcept {
        if (data_ != nullptr) {
            ::munmap(data_, size_);
            data_ = nullptr;
            size_ = 0;
        }
    }

    char* data_{nullptr};
    size_t size_{0};
};

class FileDescriptorGuard {
 public:
    explicit FileDescriptorGuard(int fd) : fd_(fd) {
    }

    ~FileDescriptorGuard() {
        if (fd_ != -1) {
            ::close(fd_);
        }
    }

    FileDescriptorGuard(const FileDescriptorGuard&) = delete;
    FileDescriptorGuard&
    operator=(const FileDescriptorGuard&) = delete;

    int
    Get() const {
        return fd_;
    }

    void
    CloseChecked(const std::string& path, std::string_view context) {
        const auto fd = std::exchange(fd_, -1);
        if (::close(fd) != 0) {
            ThrowInfo(FileReadFailed,
                      "failed to close {} {}: {}",
                      context,
                      path,
                      std::strerror(errno));
        }
    }

 private:
    int fd_{-1};
};

inline void
WriteAll(int fd,
         const void* data,
         size_t size,
         const std::string& path,
         std::string_view error_prefix) {
    auto* cursor = static_cast<const uint8_t*>(data);
    while (size != 0) {
        const auto written = ::write(fd, cursor, size);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            ThrowInfo(FileWriteFailed,
                      "{} {}: {}",
                      error_prefix,
                      path,
                      std::strerror(errno));
        }
        cursor += written;
        size -= static_cast<size_t>(written);
    }
}

inline size_t
LocalFileSize(const std::string& path, std::string_view error_prefix) {
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (error || size > std::numeric_limits<size_t>::max()) {
        ThrowInfo(
            FileReadFailed, "{} {}: {}", error_prefix, path, error.message());
    }
    return static_cast<size_t>(size);
}

inline void
ReadAll(int fd,
        void* output,
        size_t size,
        const std::string& path,
        std::string_view context) {
    auto* cursor = static_cast<uint8_t*>(output);
    while (size != 0) {
        const auto request = std::min(
            size, static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const auto read_size = ::read(fd, cursor, request);
        if (read_size < 0 && errno == EINTR) {
            continue;
        }
        if (read_size < 0) {
            ThrowInfo(FileReadFailed,
                      "failed to read {} {}: {}",
                      context,
                      path,
                      std::strerror(errno));
        }
        if (read_size == 0) {
            ThrowInfo(FileReadFailed,
                      "unexpected EOF while reading {} {}",
                      context,
                      path);
        }
        cursor += read_size;
        size -= static_cast<size_t>(read_size);
    }
}

}  // namespace milvus::storage
