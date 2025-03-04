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

#include "storage/MmapChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include <fstream>
#include <sys/mman.h>
#include <unistd.h>
#include "stdio.h"
#include <fcntl.h>
#include "log/Log.h"
#include "monitor/prometheus_client.h"

namespace milvus::storage {
namespace {
static constexpr int kMmapDefaultProt = PROT_WRITE | PROT_READ;
static constexpr int kMmapDefaultFlags = MAP_SHARED;
};  // namespace

// todo(cqy): After confirming the append parallelism of multiple fields, adjust the lock granularity.

MmapBlock::MmapBlock(const std::string& file_name,
                     const uint64_t file_size,
                     BlockType type)
    : file_name_(file_name),
      file_size_(file_size),
      block_type_(type),
      is_valid_(false) {
}

void
MmapBlock::Init() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (is_valid_ == true) {
        LOG_WARN("This mmap block has been init.");
        return;
    }
    // create tmp file
    int fd = open(file_name_.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        PanicInfo(ErrorCode::FileCreateFailed,
                  "Failed to open mmap tmp file:{}",
                  file_name_);
    }
    // append file size to 'file_size'
    if (lseek(fd, file_size_ - 1, SEEK_SET) == -1) {
        PanicInfo(ErrorCode::FileReadFailed,
                  "Failed to seek mmap tmp file:{}",
                  file_name_);
    }
    if (write(fd, "", 1) == -1) {
        PanicInfo(ErrorCode::FileWriteFailed,
                  "Failed to write mmap tmp file:{}",
                  file_name_);
    }
    // memory mmaping
    addr_ = static_cast<char*>(
        mmap(nullptr, file_size_, kMmapDefaultProt, kMmapDefaultFlags, fd, 0));
    if (addr_ == MAP_FAILED) {
        PanicInfo(ErrorCode::MmapError,
                  "Failed to mmap in mmap_block:{}",
                  file_name_);
    }
    offset_.store(0);
    close(fd);

    milvus::monitor::internal_mmap_allocated_space_bytes_file.Observe(
        file_size_);
    milvus::monitor::internal_mmap_in_used_space_bytes_file.Increment(
        file_size_);
    milvus::monitor::internal_mmap_in_used_count_file.Increment();
    is_valid_ = true;
    allocated_size_.fetch_add(file_size_);
}

void
MmapBlock::Close() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (is_valid_ == false) {
        LOG_WARN("This mmap block has been closed under file:{}", file_name_);
        return;
    }
    if (addr_ != nullptr) {
        if (munmap(addr_, file_size_) != 0) {
            PanicInfo(ErrorCode::MemAllocateSizeNotMatch,
                      "Failed to munmap in mmap_block under file:{}",
                      file_name_);
        }
    }
    if (access(file_name_.c_str(), F_OK) == 0) {
        if (remove(file_name_.c_str()) != 0) {
            PanicInfo(ErrorCode::MmapError,
                      "Failed to munmap in mmap_block under file:{}",
                      file_name_);
        }
    }
    allocated_size_.fetch_sub(file_size_);
    milvus::monitor::internal_mmap_in_used_space_bytes_file.Decrement(
        file_size_);
    milvus::monitor::internal_mmap_in_used_count_file.Decrement();
    is_valid_ = false;
}

MmapBlock::~MmapBlock() {
    if (is_valid_ == true) {
        try {
            Close();
        } catch (const std::exception& e) {
            LOG_ERROR(e.what());
        }
    }
}

void*
MmapBlock::Get(const uint64_t size) {
    AssertInfo(is_valid_,
               "Fail to get memory from invalid MmapBlock under file:{}.",
               file_name_);
    if (file_size_ - offset_.load() < size) {
        return nullptr;
    } else {
        return (void*)(addr_ + offset_.fetch_add(size));
    }
}

MmapBlockPtr
MmapBlocksHandler::AllocateFixSizeBlock() {
    if (fix_size_blocks_cache_.size() != 0) {
        // return a mmap_block in fix_size_blocks_cache_
        auto block = std::move(fix_size_blocks_cache_.front());
        fix_size_blocks_cache_.pop();
        return std::move(block);
    } else {
        // if space not enough for create a new block, clear cache and check again
        if (GetFixFileSize() + Size() > max_disk_limit_) {
            PanicInfo(ErrorCode::MemAllocateSizeNotMatch,
                      "Failed to create a new mmap_block, not enough disk for "
                      "create a new mmap block. Allocated size: {}, Max size: "
                      "{} under mmap file_prefix: {}",
                      Size(),
                      max_disk_limit_,
                      mmap_file_prefix_);
        }
        auto new_block = std::make_unique<MmapBlock>(
            GetMmapFilePath(), GetFixFileSize(), MmapBlock::BlockType::Fixed);
        new_block->Init();
        return std::move(new_block);
    }
}

MmapBlockPtr
MmapBlocksHandler::AllocateLargeBlock(const uint64_t size) {
    if (size + Capacity() > max_disk_limit_) {
        ClearCache();
    }
    if (size + Size() > max_disk_limit_) {
        PanicInfo(ErrorCode::MemAllocateSizeNotMatch,
                  "Failed to create a new mmap_block, not enough disk for "
                  "create a new mmap block. To Allocate:{} Allocated size: {}, "
                  "Max size: {} "
                  "under mmap file_prefix: {}",
                  size,
                  Size(),
                  max_disk_limit_,
                  mmap_file_prefix_);
    }
    auto new_block = std::make_unique<MmapBlock>(
        GetMmapFilePath(), size, MmapBlock::BlockType::Variable);
    new_block->Init();
    return std::move(new_block);
}

void
MmapBlocksHandler::Deallocate(MmapBlockPtr&& block) {
    if (block->GetType() == MmapBlock::BlockType::Fixed) {
        // store the mmap block in cache
        block->Reset();
        fix_size_blocks_cache_.push(std::move(block));
        uint64_t max_cache_size =
            uint64_t(cache_threshold * (float)max_disk_limit_);
        if (fix_size_blocks_cache_.size() * fix_mmap_file_size_ >
            max_cache_size) {
            FitCache(max_cache_size);
        }
    } else {
        // release the mmap block
        block->Close();
        block = nullptr;
    }
}

void
MmapBlocksHandler::ClearCache() {
    while (!fix_size_blocks_cache_.empty()) {
        auto block = std::move(fix_size_blocks_cache_.front());
        block->Close();
        fix_size_blocks_cache_.pop();
    }
}

void
MmapBlocksHandler::FitCache(const uint64_t size) {
    while (fix_size_blocks_cache_.size() * fix_mmap_file_size_ > size) {
        auto block = std::move(fix_size_blocks_cache_.front());
        block->Close();
        fix_size_blocks_cache_.pop();
    }
}

MmapChunkManager::~MmapChunkManager() {
    // munmap all mmap_blocks before remove dir
    for (auto it = blocks_table_.begin(); it != blocks_table_.end();) {
        it = blocks_table_.erase(it);
    }
    if (blocks_handler_ != nullptr) {
        blocks_handler_ = nullptr;
    }
    // clean the mmap dir
    auto cm =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    if (cm->Exist(mmap_file_prefix_)) {
        cm->RemoveDir(mmap_file_prefix_);
    }
}

void
MmapChunkManager::Register(const MmapChunkDescriptorPtr descriptor) {
    if (HasRegister(descriptor)) {
        LOG_WARN("descriptor has exist in MmapChunkManager");
        return;
    }
    AssertInfo(
        descriptor->segment_type == SegmentType::Growing ||
            descriptor->segment_type == SegmentType::Sealed,
        "only register for growing or sealed segment in MmapChunkManager");
    std::unique_lock<std::shared_mutex> lck(mtx_);
    blocks_table_.emplace(*descriptor.get(), std::vector<MmapBlockPtr>());
    return;
}

void
MmapChunkManager::UnRegister(const MmapChunkDescriptorPtr descriptor) {
    std::unique_lock<std::shared_mutex> lck(mtx_);
    MmapChunkDescriptor blocks_table_key = *descriptor.get();
    if (blocks_table_.find(blocks_table_key) != blocks_table_.end()) {
        auto& blocks = blocks_table_[blocks_table_key];
        for (auto i = 0; i < blocks.size(); i++) {
            blocks_handler_->Deallocate(std::move(blocks[i]));
        }
        blocks_table_.erase(blocks_table_key);
    }
}

bool
MmapChunkManager::HasRegister(const MmapChunkDescriptorPtr descriptor) {
    std::shared_lock<std::shared_mutex> lck(mtx_);
    return (blocks_table_.find(*descriptor.get()) != blocks_table_.end());
}

void*
MmapChunkManager::Allocate(const MmapChunkDescriptorPtr descriptor,
                           const uint64_t size) {
    AssertInfo(HasRegister(descriptor),
               "descriptor {} has not been register.",
               descriptor->segment_id);
    std::unique_lock<std::shared_mutex> lck(mtx_);
    auto blocks_table_key = *descriptor.get();
    if (size < blocks_handler_->GetFixFileSize()) {
        // find a place to fit in
        for (auto block_id = 0;
             block_id < blocks_table_[blocks_table_key].size();
             block_id++) {
            auto addr = blocks_table_[blocks_table_key][block_id]->Get(size);
            if (addr != nullptr) {
                return addr;
            }
        }
        // create a new block
        auto new_block = blocks_handler_->AllocateFixSizeBlock();
        AssertInfo(new_block != nullptr, "new mmap_block can't be nullptr");
        auto addr = new_block->Get(size);
        AssertInfo(addr != nullptr, "fail to allocate from mmap block.");
        blocks_table_[blocks_table_key].emplace_back(std::move(new_block));
        return addr;
    } else {
        auto new_block = blocks_handler_->AllocateLargeBlock(size);
        AssertInfo(new_block != nullptr, "new mmap_block can't be nullptr");
        auto addr = new_block->Get(size);
        AssertInfo(addr != nullptr, "fail to allocate from mmap block.");
        blocks_table_[blocks_table_key].emplace_back(std::move(new_block));
        return addr;
    }
}

MmapChunkManager::MmapChunkManager(std::string root_path,
                                   const uint64_t disk_limit,
                                   const uint64_t file_size) {
    blocks_handler_ =
        std::make_unique<MmapBlocksHandler>(disk_limit, file_size, root_path);
    mmap_file_prefix_ = root_path;
    auto cm =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    AssertInfo(cm != nullptr,
               "Fail to get LocalChunkManager, LocalChunkManagerPtr is null");
    if (cm->Exist(root_path)) {
        cm->RemoveDir(root_path);
    }
    cm->CreateDir(root_path);
    LOG_INFO(
        "Init MappChunkManager with: Path {}, MaxDiskSize {} MB, "
        "FixedFileSize {} MB.",
        root_path,
        disk_limit / (1024 * 1024),
        file_size / (1024 * 1024));
}
}  // namespace milvus::storage