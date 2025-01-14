// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <cstdio>
#include <string>
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "fmt/core.h"
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

namespace milvus {

#define THROW_FILE_WRITE_ERROR(path)                                     \
    PanicInfo(ErrorCode::FileWriteFailed,                                \
              fmt::format("write data to file {} failed, error code {}", \
                          path,                                          \
                          strerror(errno)));

class File {
 public:
    File(const File& file) = delete;
    File(File&& file) noexcept {
        fd_ = file.fd_;
        file.fd_ = -1;
    }
    ~File() {
        if (fs_ != nullptr) {
            fclose(fs_);
        }
    }

    static File
    Open(const std::string_view filepath, int flags) {
        // using default buf size = 4096
        return Open(filepath, flags, 4096);
    }

    static File
    Open(const std::string_view filepath, int flags, size_t buf_size) {
        int fd = open(filepath.data(), flags, S_IRUSR | S_IWUSR);
        AssertInfo(fd != -1,
                   "failed to create mmap file {}: {}",
                   filepath,
                   strerror(errno));
        FILE* fs = fdopen(fd, "wb+");
        AssertInfo(fs != nullptr,
                   "failed to open file {}: {}",
                   filepath,
                   strerror(errno));
        auto f = File(fd, fs, std::string(filepath));
        // setup buffer size file stream will use
        setvbuf(f.fs_, nullptr, _IOFBF, buf_size);
        return f;
    }

    int
    Descriptor() const {
        return fd_;
    }

    std::string
    Path() const {
        return filepath_;
    }

    ssize_t
    Write(const void* buf, size_t size) {
        return write(fd_, buf, size);
    }

    template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
    ssize_t
    WriteInt(T value) {
        return write(fd_, &value, sizeof(value));
    }

    ssize_t
    FWrite(const void* buf, size_t size) {
        return fwrite(buf, sizeof(char), size, fs_);
    }

    template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
    ssize_t
    FWriteInt(T value) {
        return fwrite(&value, 1, sizeof(value), fs_);
    }

    int
    FFlush() {
        return fflush(fs_);
    }

    offset_t
    Seek(offset_t offset, int whence) {
        return lseek(fd_, offset, whence);
    }

    void
    Close() {
        fclose(fs_);
        fs_ = nullptr;
        fd_ = -1;
    }

 private:
    explicit File(int fd, FILE* fs, const std::string& filepath)
        : fd_(fd), filepath_(filepath), fs_(fs) {
    }
    int fd_{-1};
    FILE* fs_;
    std::string filepath_;
};

class BufferedWriter {
 public:
    // Constructor: Initialize with the file pointer and the buffer size (default 4KB).
    explicit BufferedWriter(File& file, size_t buffer_size = 4096)
        : file_(file),
          buffer_size_(buffer_size),
          buffer_(new char[buffer_size]) {
    }

    ~BufferedWriter() {
        // Ensure the buffer is flushed when the object is destroyed
        flush();
        delete[] buffer_;
    }

    // Write method to handle data larger than the buffer
    void
    Write(const void* data, size_t size) {
        if (size > buffer_size_) {
            flush();
            ssize_t written_data_size = file_.FWrite(data, size);
            if (written_data_size != size) {
                THROW_FILE_WRITE_ERROR(file_.Path())
            }
            return;
        }

        if (buffer_pos_ + size > buffer_size_) {
            flush();
        }

        std::memcpy(buffer_ + buffer_pos_, data, size);
        buffer_pos_ += size;
    }

    template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
    void
    WriteInt(T value) {
        Write(&value, sizeof(value));
    }

    // Flush method: Write the contents of the buffer to the file
    void
    flush() {
        if (buffer_pos_ > 0) {
            ssize_t written_data_size = file_.FWrite(buffer_, buffer_pos_);
            if (written_data_size != buffer_pos_) {
                THROW_FILE_WRITE_ERROR(file_.Path())
            }
            buffer_pos_ = 0;
        }
    }

 private:
    File& file_;            // File pointer
    size_t buffer_size_;    // Size of the internal buffer
    char* buffer_;          // The buffer itself
    size_t buffer_pos_{0};  // Current position in the buffer
};
}  // namespace milvus
