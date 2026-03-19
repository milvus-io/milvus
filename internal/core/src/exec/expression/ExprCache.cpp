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

#include "exec/expression/ExprCache.h"

#include <filesystem>

#include "exec/expression/DiskSlotFile.h"
#include "exec/expression/EntryPool.h"

namespace milvus {
namespace exec {

std::atomic<bool> ExprResCacheManager::enabled_{false};

ExprResCacheManager&
ExprResCacheManager::Instance() {
    static ExprResCacheManager instance;
    return instance;
}

void
ExprResCacheManager::SetEnabled(bool enabled) {
    enabled_.store(enabled);
}

bool
ExprResCacheManager::IsEnabled() {
    return enabled_.load();
}

void
ExprResCacheManager::Init(size_t capacity_bytes, bool enabled) {
    SetEnabled(enabled);
    // capacity_bytes is kept for compatibility but not used directly
}

void
ExprResCacheManager::SetConfig(const CacheConfig& config) {
    config_ = config;
    if (config_.mode == CacheMode::Memory) {
        entry_pool_ = std::make_unique<EntryPool>(config_.mem_max_bytes);
        entry_pool_->Configure(config_.mem_max_bytes,
                               config_.compression_enabled,
                               config_.admission_threshold,
                               config_.mem_min_eval_duration_us);
        {
            std::unique_lock lock(disk_files_mutex_);
            disk_files_.clear();
        }
    } else {
        entry_pool_.reset();
        {
            std::unique_lock lock(disk_files_mutex_);
            disk_files_.clear();
        }
        if (!config_.disk_base_path.empty()) {
            std::filesystem::create_directories(config_.disk_base_path);
            // Clean old cache files
            for (auto& entry :
                 std::filesystem::directory_iterator(config_.disk_base_path)) {
                if (entry.path().extension() == ".cache") {
                    std::filesystem::remove(entry.path());
                }
            }
        }
    }
}

void
ExprResCacheManager::SetDiskConfig(const std::string& base_path,
                                   uint64_t max_total_size,
                                   uint64_t max_segment_file_size,
                                   bool compression_enabled,
                                   uint8_t admission_threshold,
                                   int64_t min_eval_duration_us,
                                   bool in_memory) {
    // Map old API to new CacheConfig.
    // in_memory=true → Memory mode; in_memory=false → still uses Memory mode
    // (to maintain backward compatibility: old callers that pass in_memory=false
    //  were using mmap files, but now we route them through Memory mode since
    //  SegmentCacheFile is removed. Disk mode is only via SetConfig.)
    CacheConfig cfg;
    cfg.mode = CacheMode::Memory;
    cfg.mem_max_bytes = max_total_size;
    cfg.compression_enabled = compression_enabled;
    cfg.admission_threshold = admission_threshold;
    cfg.mem_min_eval_duration_us = min_eval_duration_us;
    SetConfig(cfg);
}

void
ExprResCacheManager::SetCapacityBytes(size_t capacity_bytes) {
    // Backward compatibility: ensure memory-mode EntryPool exists.
    // Old callers used SetCapacityBytes to configure the cache size;
    // the V2 manager needs an EntryPool to actually store entries.
    // Use threshold=1 and min_eval_duration_us=0 (no admission control)
    // to match the old manager's unconditional caching behavior.
    config_.mode = CacheMode::Memory;
    config_.mem_max_bytes = capacity_bytes;
    config_.admission_threshold = 1;
    config_.mem_min_eval_duration_us = 0;
    if (!entry_pool_) {
        entry_pool_ = std::make_unique<EntryPool>(capacity_bytes);
    }
    entry_pool_->Configure(capacity_bytes,
                           config_.compression_enabled,
                           config_.admission_threshold,
                           config_.mem_min_eval_duration_us);
}

size_t
ExprResCacheManager::GetCapacityBytes() const {
    if (config_.mode == CacheMode::Memory) {
        return config_.mem_max_bytes;
    }
    return config_.disk_max_file_size;
}

size_t
ExprResCacheManager::GetCurrentBytes() const {
    if (config_.mode == CacheMode::Memory && entry_pool_) {
        return entry_pool_->GetCurrentBytes();
    }
    return 0;
}

size_t
ExprResCacheManager::GetEntryCount() const {
    if (config_.mode == CacheMode::Memory && entry_pool_) {
        return entry_pool_->GetEntryCount();
    }
    if (config_.mode == CacheMode::Disk) {
        std::shared_lock lock(disk_files_mutex_);
        return disk_files_.size();
    }
    return 0;
}

bool
ExprResCacheManager::Get(const Key& key, Value& out_value) {
    if (!IsEnabled()) {
        return false;
    }

    if (config_.mode == CacheMode::Memory) {
        if (!entry_pool_) {
            return false;
        }
        TargetBitmap result(0), valid(0);
        if (!entry_pool_->Get(key.segment_id,
                              key.signature,
                              out_value.active_count,
                              result,
                              valid)) {
            return false;
        }
        out_value.result = std::make_shared<TargetBitmap>(std::move(result));
        out_value.valid_result =
            std::make_shared<TargetBitmap>(std::move(valid));
        return true;
    } else {
        // Disk mode
        std::shared_lock lock(disk_files_mutex_);
        auto it = disk_files_.find(key.segment_id);
        if (it == disk_files_.end()) {
            return false;
        }
        TargetBitmap result(0), valid(0);
        if (!it->second->Get(
                key.signature, out_value.active_count, result, valid)) {
            return false;
        }
        out_value.result = std::make_shared<TargetBitmap>(std::move(result));
        out_value.valid_result =
            std::make_shared<TargetBitmap>(std::move(valid));
        return true;
    }
}

void
ExprResCacheManager::Put(const Key& key, const Value& value) {
    if (!IsEnabled()) {
        return;
    }

    if (config_.mode == CacheMode::Memory) {
        if (!entry_pool_) {
            return;
        }
        entry_pool_->Put(key.segment_id,
                         key.signature,
                         value.active_count,
                         *value.result,
                         *value.valid_result,
                         value.eval_duration_us);
    } else {
        // Disk mode
        if (config_.disk_base_path.empty()) {
            return;
        }

        // Latency admission (disk mode)
        if (config_.disk_min_eval_duration_us > 0 &&
            value.eval_duration_us > 0 &&
            value.eval_duration_us < config_.disk_min_eval_duration_us) {
            return;
        }

        auto* file = GetOrCreateDiskFile(key.segment_id, value.result->size());
        if (!file) {
            return;
        }
        file->Put(key.signature,
                  value.active_count,
                  *value.result,
                  *value.valid_result);
    }
}

void
ExprResCacheManager::Clear() {
    if (entry_pool_) {
        entry_pool_->Clear();
    }
    {
        std::unique_lock lock(disk_files_mutex_);
        disk_files_.clear();
    }
    if (!config_.disk_base_path.empty() &&
        std::filesystem::exists(config_.disk_base_path)) {
        for (auto& entry :
             std::filesystem::directory_iterator(config_.disk_base_path)) {
            if (entry.path().extension() == ".cache") {
                std::filesystem::remove(entry.path());
            }
        }
    }
}

size_t
ExprResCacheManager::EraseSegment(int64_t segment_id) {
    if (config_.mode == CacheMode::Memory) {
        return entry_pool_ ? entry_pool_->EraseSegment(segment_id) : 0;
    } else {
        std::unique_lock lock(disk_files_mutex_);
        auto it = disk_files_.find(segment_id);
        if (it == disk_files_.end()) {
            return 0;
        }
        it->second->Close();
        std::string path = config_.disk_base_path + "/seg_" +
                           std::to_string(segment_id) + ".cache";
        std::filesystem::remove(path);
        disk_files_.erase(it);
        return 1;
    }
}

DiskSlotFile*
ExprResCacheManager::GetOrCreateDiskFile(int64_t segment_id, size_t row_count) {
    {
        std::shared_lock lock(disk_files_mutex_);
        auto it = disk_files_.find(segment_id);
        if (it != disk_files_.end()) {
            return it->second.get();
        }
    }
    std::unique_lock lock(disk_files_mutex_);
    auto& ptr = disk_files_[segment_id];
    if (!ptr) {
        std::string path = config_.disk_base_path + "/seg_" +
                           std::to_string(segment_id) + ".cache";
        ptr = std::make_unique<DiskSlotFile>(segment_id,
                                             path,
                                             static_cast<int64_t>(row_count),
                                             config_.disk_max_file_size);
    }
    return ptr.get();
}

}  // namespace exec
}  // namespace milvus
