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

#include <span>

#include <cstddef>
#include <cstdint>
#include <filesystem>

namespace milvus::local {
class FileSystem;
}

namespace milvus::local::io {

// ReadAt does not mutate a shared file position and may be called concurrently
// while the handle remains alive.
class RandomAccessFile final {
 public:
    RandomAccessFile(const RandomAccessFile&) = delete;
    RandomAccessFile&
    operator=(const RandomAccessFile&) = delete;

    RandomAccessFile(RandomAccessFile&& other) noexcept;
    RandomAccessFile&
    operator=(RandomAccessFile&& other) noexcept;

    ~RandomAccessFile();

    size_t
    ReadAt(uint64_t offset, std::span<std::byte> output) const;

    uint64_t
    Size() const;

 private:
    friend class local::FileSystem;

    RandomAccessFile(int fd, std::filesystem::path debug_path) noexcept;

    void
    Close() noexcept;

    int fd_{-1};
    std::filesystem::path debug_path_;
};

// WritableFile does not serialize writes. Callers own ordering and must not
// issue overlapping concurrent writes.
class WritableFile final {
 public:
    WritableFile(const WritableFile&) = delete;
    WritableFile&
    operator=(const WritableFile&) = delete;

    WritableFile(WritableFile&& other) noexcept;
    WritableFile&
    operator=(WritableFile&& other) noexcept;

    ~WritableFile();

    size_t
    Write(std::span<const std::byte> data);

    size_t
    WriteAt(uint64_t offset, std::span<const std::byte> data);

    uint64_t
    Size() const;

    void
    Truncate(uint64_t size);

    void
    Sync();

    const std::filesystem::path&
    DebugPath() const noexcept {
        return debug_path_;
    }

    bool
    DirectIOEnabled() const noexcept {
        return direct_io_;
    }

 private:
    friend class local::FileSystem;

    WritableFile(int fd,
                 std::filesystem::path debug_path,
                 bool direct_io) noexcept;

    void
    Close() noexcept;

    int fd_{-1};
    std::filesystem::path debug_path_;
    bool direct_io_{false};
};

}  // namespace milvus::local::io
