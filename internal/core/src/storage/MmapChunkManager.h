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
#include <cstdint>
#include <cstring>
#include <vector>
#include <queue>
#include <atomic>
#include <unordered_map>
#include <shared_mutex>
#include <memory>
#include <shared_mutex>
#include "common/EasyAssert.h"
#include "log/Log.h"
#include <optional>
#include "common/type_c.h"
#include "storage/LocalChunkManagerSingleton.h"
namespace milvus::storage {
// use segment id and segment type to descripe a segment in mmap chunk manager, segment only in two type (growing or sealed) in mmap chunk manager
struct MmapChunkDescriptor {
    struct DescriptorHash {
        size_t
        operator()(const MmapChunkDescriptor& x) const {
            //SegmentType::Growing = 0x01,SegmentType::Sealed = 0x10
            size_t sign = ((size_t)x.segment_type) << (sizeof(size_t) * 8 - 1);
            return ((size_t)x.segment_id) | sign;
        }
    };
    bool
    operator==(const MmapChunkDescriptor& x) const {
        return segment_id == x.segment_id && segment_type == x.segment_type;
    }
    int64_t segment_id;
    SegmentType segment_type;
};
using MmapChunkDescriptorPtr = std::shared_ptr<MmapChunkDescriptor>;

/**
 * @brief MmapBlock is a basic unit of MmapChunkManager. It handle all memory mmaping in one tmp file.
 * static function(TotalBlocksSize) is used to get total files size of chunk mmap.
 */
struct MmapBlock {
 public:
    enum class BlockType {
        Fixed = 0,
        Variable = 1,
    };
    MmapBlock(const std::string& file_name,
              const uint64_t file_size,
              BlockType type = BlockType::Fixed);
    ~MmapBlock();
    void
    Init();
    void
    Close();
    void*
    Get(const uint64_t size);
    void
    Reset() {
        offset_.store(0);
    }
    BlockType
    GetType() {
        return block_type_;
    }
    uint64_t
    GetCapacity() {
        return file_size_;
    }
    static void
    ClearAllocSize() {
        allocated_size_.store(0);
    }
    static uint64_t
    TotalBlocksSize() {
        return allocated_size_.load();
    }

 private:
    const std::string file_name_;
    const uint64_t file_size_;
    char* addr_ = nullptr;
    std::atomic<uint64_t> offset_ = 0;
    const BlockType block_type_;
    std::atomic<bool> is_valid_ = false;
    static inline std::atomic<uint64_t> allocated_size_ =
        0;  //keeping the total size used in
    mutable std::mutex file_mutex_;
};
using MmapBlockPtr = std::unique_ptr<MmapBlock>;

/**
 * @brief MmapBlocksHandler is used to handle the creation and destruction of mmap blocks
 * MmapBlocksHandler is not thread safe,
 */
class MmapBlocksHandler {
 public:
    MmapBlocksHandler(const uint64_t disk_limit,
                      const uint64_t fix_file_size,
                      const std::string file_prefix)
        : max_disk_limit_(disk_limit),
          mmap_file_prefix_(file_prefix),
          fix_mmap_file_size_(fix_file_size) {
        mmmap_file_counter_.store(0);
        MmapBlock::ClearAllocSize();
    }
    ~MmapBlocksHandler() {
        ClearCache();
    }
    uint64_t
    GetDiskLimit() {
        return max_disk_limit_;
    }
    uint64_t
    GetFixFileSize() {
        return fix_mmap_file_size_;
    }
    uint64_t
    Capacity() {
        return MmapBlock::TotalBlocksSize();
    }
    uint64_t
    Size() {
        return Capacity() - fix_size_blocks_cache_.size() * fix_mmap_file_size_;
    }
    MmapBlockPtr
    AllocateFixSizeBlock();
    MmapBlockPtr
    AllocateLargeBlock(const uint64_t size);
    void
    Deallocate(MmapBlockPtr&& block);

 private:
    std::string
    GetFilePrefix() {
        return mmap_file_prefix_;
    }
    std::string
    GetMmapFilePath() {
        auto file_id = mmmap_file_counter_.fetch_add(1);
        return mmap_file_prefix_ + "/" + std::to_string(file_id);
    }
    void
    ClearCache();
    void
    FitCache(const uint64_t size);

 private:
    uint64_t max_disk_limit_;
    std::string mmap_file_prefix_;
    std::atomic<uint64_t> mmmap_file_counter_;
    uint64_t fix_mmap_file_size_;
    std::queue<MmapBlockPtr> fix_size_blocks_cache_;
    const float cache_threshold = 0.25;
};

/**
 * @brief MmapChunkManager
 * MmapChunkManager manages the memory-mapping space in mmap manager;
 * MmapChunkManager uses blocks_table_ to record the relationship of segments and the mapp space it uses.
 * The basic space unit of MmapChunkManager is MmapBlock, and is managed by MmapBlocksHandler.
 * todo(cqy): blocks_handler_ and blocks_table_ is not thread safe, we need use fine-grained locks for better performance.
 */
class MmapChunkManager {
 public:
    explicit MmapChunkManager(std::string root_path,
                              const uint64_t disk_limit,
                              const uint64_t file_size);
    ~MmapChunkManager();
    void
    Register(const MmapChunkDescriptorPtr descriptor);
    void
    UnRegister(const MmapChunkDescriptorPtr descriptor);
    bool
    HasRegister(const MmapChunkDescriptorPtr descriptor);
    void*
    Allocate(const MmapChunkDescriptorPtr descriptor, const uint64_t size);
    uint64_t
    GetDiskAllocSize() {
        std::shared_lock<std::shared_mutex> lck(mtx_);
        if (blocks_handler_ == nullptr) {
            return 0;
        } else {
            return blocks_handler_->Capacity();
        }
    }
    uint64_t
    GetDiskUsage() {
        std::shared_lock<std::shared_mutex> lck(mtx_);
        if (blocks_handler_ == nullptr) {
            return 0;
        } else {
            return blocks_handler_->Size();
        }
    }

 private:
    mutable std::shared_mutex mtx_;
    std::unordered_map<MmapChunkDescriptor,
                       std::vector<MmapBlockPtr>,
                       MmapChunkDescriptor::DescriptorHash>
        blocks_table_;
    std::unique_ptr<MmapBlocksHandler> blocks_handler_ = nullptr;
    std::string mmap_file_prefix_;
};
using MmapChunkManagerPtr = std::shared_ptr<MmapChunkManager>;
}  // namespace milvus::storage