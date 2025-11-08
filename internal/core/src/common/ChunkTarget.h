// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include <memory>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstddef>
#include <string>
#include <utility>
#include "common/EasyAssert.h"
#include "storage/FileWriter.h"

namespace milvus {
class ChunkTarget {
 public:
    static constexpr size_t ALIGNED_SIZE = 4096;  // 4KB

    virtual void
    write(const void* data, size_t size) = 0;

    virtual char*
    release() = 0;

    virtual ~ChunkTarget() = default;

    virtual size_t
    tell() = 0;
};

class MmapChunkTarget : public ChunkTarget {
 public:
    explicit MmapChunkTarget(std::string file_path, size_t cap)
        : file_path_(std::move(file_path)), cap_(cap) {
        file_writer_ = std::make_unique<storage::FileWriter>(file_path_);
    }

    void
    write(const void* data, size_t size) override;

    char*
    release() override;

    size_t
    tell() override;

 private:
    void
    flush();

    std::unique_ptr<storage::FileWriter> file_writer_{nullptr};
    std::string file_path_{};
    size_t cap_{0};
    size_t size_{0};
};

class MemChunkTarget : public ChunkTarget {
 public:
    explicit MemChunkTarget(size_t cap) : cap_(cap) {
        auto m = mmap(nullptr,
                      cap,
                      PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANON,
                      -1,
                      0);
        AssertInfo(m != MAP_FAILED,
                   "failed to map: {}, map_size={}",
                   strerror(errno),
                   size_);
        data_ = reinterpret_cast<char*>(m);
    }

    void
    write(const void* data, size_t size) override;

    char*
    release() override;

    size_t
    tell() override;

 private:
    char* data_;  // no need to delete in destructor, will be deleted by Chunk
    size_t cap_;
    size_t size_ = 0;
};

}  // namespace milvus