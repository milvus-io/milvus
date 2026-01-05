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

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif
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

    virtual ~ChunkTarget() = default;

    /**
     * @brief write data to the target at the current position
     * @param data the data to write
     * @param size the size of the data to write
     */
    virtual void
    write(const void* data, size_t size) = 0;

    /**
     * @brief release the data pointer to the caller
     * @note no write() should be called after release()
     * @return the data pointer
     */
    virtual char*
    release() = 0;

    /**
     * @brief get the current position of the target
     * @return the current position
     */
    virtual size_t
    tell() = 0;
};

class MmapChunkTarget : public ChunkTarget {
 public:
    explicit MmapChunkTarget(std::string file_path,
                             bool populate,
                             size_t cap,
                             storage::io::Priority io_prio)
        : file_path_(std::move(file_path)), cap_(cap), populate_(populate) {
        file_writer_ =
            std::make_unique<storage::FileWriter>(file_path_, io_prio);
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
    bool populate_{false};
};

class MemChunkTarget : public ChunkTarget {
 public:
    explicit MemChunkTarget(size_t cap, bool populate = true) : cap_(cap) {
        auto mmap_flag = MAP_PRIVATE | MAP_ANON;
        if (populate) {
            mmap_flag |= MAP_POPULATE;
        }
        auto m = mmap(nullptr,
                      cap,
                      PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANON | MAP_POPULATE,
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