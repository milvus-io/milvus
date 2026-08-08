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

#include <filesystem>

namespace milvus::local {

class FileSystem;

struct FileHandle final {
    static FileHandle
    Adopt(int fd,
          std::filesystem::path debug_path,
          bool direct_io = false) noexcept;

    FileHandle(const FileHandle&) = delete;
    FileHandle&
    operator=(const FileHandle&) = delete;

    FileHandle(FileHandle&& other) noexcept;
    FileHandle&
    operator=(FileHandle&& other) noexcept;

    ~FileHandle();

    int
    Get() const noexcept {
        return fd_;
    }

    int
    Release() noexcept;

    const std::filesystem::path&
    DebugPath() const noexcept {
        return debug_path_;
    }

    bool
    DirectIOEnabled() const noexcept {
        return direct_io_;
    }

 private:
    friend class FileSystem;

    FileHandle(int fd,
               std::filesystem::path debug_path,
               bool direct_io) noexcept;

    void
    Close() noexcept;

    int fd_{-1};
    std::filesystem::path debug_path_;
    bool direct_io_{false};
};

}  // namespace milvus::local
