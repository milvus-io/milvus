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

#include <string>
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "fmt/core.h"
#include <fcntl.h>
#include <unistd.h>

namespace milvus {
class File {
 public:
    File(const File& file) = delete;
    File(File&& file) noexcept {
        fd_ = file.fd_;
        file.fd_ = -1;
    }
    ~File() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    static File
    Open(const std::string_view filepath, int flags) {
        int fd = open(filepath.data(), flags, S_IRUSR | S_IWUSR);
        AssertInfo(fd != -1,
                   "failed to create mmap file {}: {}",
                   filepath,
                   strerror(errno));
        return File(fd);
    }

    int
    Descriptor() const {
        return fd_;
    }

    ssize_t
    Write(const void* buf, size_t size) {
        return write(fd_, buf, size);
    }

    offset_t
    Seek(offset_t offset, int whence) {
        return lseek(fd_, offset, whence);
    }

    void
    Close() {
        close(fd_);
        fd_ = -1;
    }

 private:
    explicit File(int fd) : fd_(fd) {
    }
    int fd_{-1};
};
}  // namespace milvus
