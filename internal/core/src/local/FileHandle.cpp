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

#include "local/FileHandle.h"

#include <unistd.h>

#include <utility>

namespace milvus::local {

FileHandle
FileHandle::Adopt(int fd,
                  std::filesystem::path debug_path,
                  bool direct_io) noexcept {
    return FileHandle(fd, std::move(debug_path), direct_io);
}

FileHandle::FileHandle(int fd,
                       std::filesystem::path debug_path,
                       bool direct_io) noexcept
    : fd_(fd), debug_path_(std::move(debug_path)), direct_io_(direct_io) {
}

FileHandle::FileHandle(FileHandle&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)),
      debug_path_(std::move(other.debug_path_)),
      direct_io_(std::exchange(other.direct_io_, false)) {
}

FileHandle&
FileHandle::operator=(FileHandle&& other) noexcept {
    if (this != &other) {
        Close();
        fd_ = std::exchange(other.fd_, -1);
        debug_path_ = std::move(other.debug_path_);
        direct_io_ = std::exchange(other.direct_io_, false);
    }
    return *this;
}

FileHandle::~FileHandle() {
    Close();
}

int
FileHandle::Release() noexcept {
    return std::exchange(fd_, -1);
}

void
FileHandle::Close() noexcept {
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
}

}  // namespace milvus::local
