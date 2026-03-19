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

#include "cachinglayer/Metrics.h"
#include "exec/expression/DiskSlotFile.h"
#include "exec/expression/EntryPool.h"
#include "xxhash.h"

namespace milvus {
namespace exec {

namespace {

void
RemoveCacheFilesInDir(const std::string& base_path) {
    if (base_path.empty()) {
        return;
    }

    std::error_code ec;
    if (!std::filesystem::exists(base_path, ec) || ec) {
        if (ec) {
            LOG_WARN("ExprResCacheManager: failed to stat cache dir {}: {}",
                     base_path,
                     ec.message());
        }
        return;
    }

    std::filesystem::directory_iterator it(base_path, ec);
    std::filesystem::directory_iterator end;
    for (; !ec && it != end; it.increment(ec)) {
        auto& entry = *it;
        if (entry.path().extension() == ".cache") {
            std::filesystem::remove(entry.path(), ec);
            if (ec) {
                LOG_WARN(
                    "ExprResCacheManager: failed to remove cache file {}: {}",
                    entry.path().string(),
                    ec.message());
                ec.clear();
            }
        }
    }
    if (ec) {
        LOG_WARN("ExprResCacheManager: failed to iterate cache dir {}: {}",
                 base_path,
                 ec.message());
    }
}

}  // namespace

std::atomic<bool> ExprResCacheManager::enabled_{false};

namespace {

void
UpdateGauge(prometheus::Gauge& gauge, int64_t delta) {
    if (delta > 0) {
        gauge.Increment(delta);
    } else if (delta < 0) {
        gauge.Decrement(-delta);
    }
}

}  // namespace

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

bool
ExprResCacheManager::SetConfig(const CacheConfig& config) {
    std::unique_lock state_lock(state_mutex_);
    const auto old_mode = config_.mode;
    const auto old_disk_base_path = config_.disk_base_path;

    if (config.mode == CacheMode::Disk && !config.disk_base_path.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(config.disk_base_path, ec);
        if (ec) {
            LOG_WARN("ExprResCacheManager: failed to create cache dir {}: {}",
                     config.disk_base_path,
                     ec.message());
            SetEnabled(false);
            entry_pool_.reset();
            {
                std::unique_lock lock(disk_files_mutex_);
                disk_files_.clear();
                disk_ineligible_segments_.clear();
            }
            {
                std::lock_guard lock(disk_clock_mutex_);
                disk_clock_segments_.clear();
                disk_clock_index_.clear();
                disk_segment_usage_.clear();
                disk_clock_hand_ = 0;
            }
            SyncUsageMetrics(0, 0);
            return false;
        }
    }

    config_ = config;
    frequency_tracker_.Reset();
    if (config_.mode == CacheMode::Memory) {
        entry_pool_ = std::make_unique<EntryPool>(config_.mem_max_bytes);
        entry_pool_->Configure(config_.mem_max_bytes,
                               config_.compression_enabled,
                               config_.mem_min_eval_duration_us);
        {
            std::unique_lock lock(disk_files_mutex_);
            disk_files_.clear();
            disk_ineligible_segments_.clear();
        }
        {
            std::lock_guard lock(disk_clock_mutex_);
            disk_clock_segments_.clear();
            disk_clock_index_.clear();
            disk_segment_usage_.clear();
            disk_clock_hand_ = 0;
        }
        if (old_mode == CacheMode::Disk) {
            RemoveCacheFilesInDir(old_disk_base_path);
        }
        SyncUsageMetrics(0, 0);
        return true;
    } else {
        entry_pool_.reset();
        {
            std::unique_lock lock(disk_files_mutex_);
            disk_files_.clear();
            disk_ineligible_segments_.clear();
        }
        {
            std::lock_guard lock(disk_clock_mutex_);
            disk_clock_segments_.clear();
            disk_clock_index_.clear();
            disk_segment_usage_.clear();
            disk_clock_hand_ = 0;
        }
        // Disk cache metadata is process-local; old files are not reusable.
        if (!config_.disk_base_path.empty()) {
            RemoveCacheFilesInDir(config_.disk_base_path);
        }
        if (old_mode == CacheMode::Disk && !old_disk_base_path.empty() &&
            old_disk_base_path != config_.disk_base_path) {
            RemoveCacheFilesInDir(old_disk_base_path);
        }
        SyncUsageMetrics(0, 0);
        return true;
    }
}

CacheMode
ExprResCacheManager::GetMode() const {
    std::shared_lock state_lock(state_mutex_);
    return config_.mode;
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
    std::unique_lock state_lock(state_mutex_);
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
                           config_.mem_min_eval_duration_us);
}

size_t
ExprResCacheManager::GetCapacityBytes() const {
    std::shared_lock state_lock(state_mutex_);
    if (config_.mode == CacheMode::Memory) {
        return config_.mem_max_bytes;
    }
    return config_.disk_max_bytes;
}

size_t
ExprResCacheManager::GetCurrentBytes() const {
    std::shared_lock state_lock(state_mutex_);
    if (config_.mode == CacheMode::Memory && entry_pool_) {
        return entry_pool_->GetCurrentBytes();
    }
    if (config_.mode == CacheMode::Disk) {
        std::shared_lock lock(disk_files_mutex_);
        return GetDiskCurrentBytesLocked();
    }
    return 0;
}

size_t
ExprResCacheManager::GetEntryCount() const {
    std::shared_lock state_lock(state_mutex_);
    if (config_.mode == CacheMode::Memory && entry_pool_) {
        return entry_pool_->GetEntryCount();
    }
    if (config_.mode == CacheMode::Disk) {
        std::shared_lock lock(disk_files_mutex_);
        size_t total = 0;
        for (const auto& [_, file] : disk_files_) {
            if (file) {
                total += file->GetUsedCount();
            }
        }
        return total;
    }
    return 0;
}

bool
ExprResCacheManager::Get(const Key& key, Value& out_value) {
    if (!IsEnabled()) {
        return false;
    }

    std::shared_lock state_lock(state_mutex_);
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
        TryTouchDiskSegment(key.segment_id);
        return true;
    }
}

void
ExprResCacheManager::Put(const Key& key, const Value& value) {
    if (!IsEnabled()) {
        return;
    }
    if (!value.result || !value.valid_result) {
        return;
    }

    std::shared_lock state_lock(state_mutex_);
    if (!IsEnabled()) {
        return;
    }
    if (config_.mode == CacheMode::Memory) {
        if (!entry_pool_) {
            return;
        }
        const bool same_signature_cached =
            entry_pool_->HasSignature(key.segment_id, key.signature);
        if (!same_signature_cached && config_.mem_min_eval_duration_us > 0 &&
            value.eval_duration_us > 0 &&
            value.eval_duration_us < config_.mem_min_eval_duration_us) {
            return;
        }
        if (!same_signature_cached &&
            !frequency_tracker_.RecordAndCheck(
                XXH64(key.signature.data(), key.signature.size(), 0),
                config_.admission_threshold)) {
            return;
        }
        entry_pool_->Put(key.segment_id,
                         key.signature,
                         value.active_count,
                         *value.result,
                         *value.valid_result,
                         value.eval_duration_us);
        SyncUsageMetrics(entry_pool_->GetCurrentBytes(), 0);
    } else {
        // Disk mode
        if (config_.disk_base_path.empty()) {
            return;
        }

        bool replacing_existing = false;
        {
            std::shared_lock lock(disk_files_mutex_);
            if (disk_ineligible_segments_.find(key.segment_id) !=
                disk_ineligible_segments_.end()) {
                return;
            }
            auto it = disk_files_.find(key.segment_id);
            if (it != disk_files_.end()) {
                replacing_existing = it->second->HasSignature(key.signature);
            }
        }

        // Latency admission (disk mode)
        if (!replacing_existing && config_.disk_min_eval_duration_us > 0 &&
            value.eval_duration_us > 0 &&
            value.eval_duration_us < config_.disk_min_eval_duration_us) {
            return;
        }

        // Frequency admission is mode-independent. Applying it before opening
        // the segment file avoids one-off expressions consuming disk slots and
        // issuing unnecessary pwrite calls.
        if (!replacing_existing &&
            !frequency_tracker_.RecordAndCheck(
                XXH64(key.signature.data(), key.signature.size(), 0),
                config_.admission_threshold)) {
            return;
        }

        std::unique_lock lock(disk_files_mutex_);
        if (disk_ineligible_segments_.find(key.segment_id) !=
            disk_ineligible_segments_.end()) {
            return;
        }
        std::string path = config_.disk_base_path + "/seg_" +
                           std::to_string(key.segment_id) + ".cache";
        auto& file = disk_files_[key.segment_id];
        if (file &&
            file->GetRowCount() != static_cast<int64_t>(value.result->size())) {
            // Disk cache is sealed-only. A row-count change identifies a
            // growing/unstable segment for this backend, so drop the old fixed
            // file and skip future disk puts until the segment/config resets.
            RemoveDiskSegmentFile(key.segment_id);
            disk_ineligible_segments_.insert(key.segment_id);
            SyncUsageMetrics(0, GetDiskCurrentBytesLocked());
            return;
        }
        if (!file) {
            file = std::make_unique<DiskSlotFile>(
                key.segment_id,
                path,
                static_cast<int64_t>(value.result->size()),
                config_.disk_max_file_size);
        }
        file->Put(key.signature,
                  value.active_count,
                  *value.result,
                  *value.valid_result);
        TouchDiskSegment(key.segment_id);
        EvictDiskSegmentsUntilWithinBudget(key.segment_id);
        SyncUsageMetrics(0, GetDiskCurrentBytesLocked());
    }
}

void
ExprResCacheManager::Clear() {
    std::unique_lock state_lock(state_mutex_);
    if (entry_pool_) {
        entry_pool_->Clear();
    }
    frequency_tracker_.Reset();
    {
        std::unique_lock lock(disk_files_mutex_);
        disk_files_.clear();
        disk_ineligible_segments_.clear();
    }
    {
        std::lock_guard lock(disk_clock_mutex_);
        disk_clock_segments_.clear();
        disk_clock_index_.clear();
        disk_segment_usage_.clear();
        disk_clock_hand_ = 0;
    }
    if (!config_.disk_base_path.empty()) {
        RemoveCacheFilesInDir(config_.disk_base_path);
    }
    SyncUsageMetrics(0, 0);
}

size_t
ExprResCacheManager::EraseSegment(int64_t segment_id) {
    std::unique_lock state_lock(state_mutex_);
    if (config_.mode == CacheMode::Memory) {
        size_t erased = entry_pool_ ? entry_pool_->EraseSegment(segment_id) : 0;
        SyncUsageMetrics(entry_pool_ ? entry_pool_->GetCurrentBytes() : 0, 0);
        return erased;
    } else {
        std::unique_lock lock(disk_files_mutex_);
        if (disk_files_.find(segment_id) == disk_files_.end()) {
            disk_ineligible_segments_.erase(segment_id);
            RemoveDiskClockSegment(segment_id);
            return 0;
        }
        RemoveDiskSegmentFile(segment_id);
        SyncUsageMetrics(0, GetDiskCurrentBytesLocked());
        return 1;
    }
}

size_t
ExprResCacheManager::GetDiskCurrentBytesLocked() const {
    size_t total = 0;
    for (const auto& [_, file] : disk_files_) {
        if (file) {
            total += file->GetUsedBytes();
        }
    }
    return total;
}

size_t
ExprResCacheManager::EvictDiskSegmentsUntilWithinBudget(
    int64_t protected_segment_id) {
    if (config_.disk_max_bytes == 0) {
        return 0;
    }

    size_t total = GetDiskCurrentBytesLocked();
    size_t erased = 0;
    while (total > config_.disk_max_bytes) {
        int64_t victim_segment_id = 0;
        bool found_victim = false;
        {
            std::lock_guard lock(disk_clock_mutex_);
            if (disk_clock_segments_.empty()) {
                break;
            }

            bool has_evictable_segment = false;
            for (auto segment_id : disk_clock_segments_) {
                if (segment_id != protected_segment_id) {
                    has_evictable_segment = true;
                    break;
                }
            }
            if (!has_evictable_segment) {
                break;
            }

            if (disk_clock_hand_ >= disk_clock_segments_.size()) {
                disk_clock_hand_ = 0;
            }

            const auto segment_id = disk_clock_segments_[disk_clock_hand_];
            if (segment_id == protected_segment_id) {
                disk_clock_hand_ =
                    (disk_clock_hand_ + 1) % disk_clock_segments_.size();
                continue;
            }

            auto usage_it = disk_segment_usage_.find(segment_id);
            if (usage_it != disk_segment_usage_.end() && usage_it->second > 0) {
                --usage_it->second;
                disk_clock_hand_ =
                    (disk_clock_hand_ + 1) % disk_clock_segments_.size();
                continue;
            }

            victim_segment_id = segment_id;
            found_victim = true;
        }

        if (!found_victim) {
            continue;
        }

        const size_t file_size = RemoveDiskSegmentFile(victim_segment_id);
        total = file_size > total ? 0 : total - file_size;
        ++erased;
    }

    if (total > config_.disk_max_bytes) {
        LOG_WARN(
            "ExprResCacheManager: disk cache usage {} exceeds budget {}, "
            "protected segment {} is kept",
            total,
            config_.disk_max_bytes,
            protected_segment_id);
    }

    return erased;
}

void
ExprResCacheManager::TouchDiskSegment(int64_t segment_id) {
    static constexpr uint8_t kMaxSegmentUsage = 5;

    std::lock_guard lock(disk_clock_mutex_);
    if (disk_clock_index_.find(segment_id) == disk_clock_index_.end()) {
        disk_clock_index_[segment_id] = disk_clock_segments_.size();
        disk_clock_segments_.push_back(segment_id);
        disk_segment_usage_[segment_id] = 0;
    }

    auto& usage = disk_segment_usage_[segment_id];
    if (usage < kMaxSegmentUsage) {
        ++usage;
    }
}

void
ExprResCacheManager::TryTouchDiskSegment(int64_t segment_id) {
    static constexpr uint8_t kMaxSegmentUsage = 5;

    std::unique_lock lock(disk_clock_mutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
        return;
    }

    auto usage_it = disk_segment_usage_.find(segment_id);
    if (usage_it == disk_segment_usage_.end()) {
        return;
    }
    if (usage_it->second < kMaxSegmentUsage) {
        ++usage_it->second;
    }
}

void
ExprResCacheManager::RemoveDiskClockSegment(int64_t segment_id) {
    std::lock_guard lock(disk_clock_mutex_);
    auto index_it = disk_clock_index_.find(segment_id);
    if (index_it == disk_clock_index_.end()) {
        disk_segment_usage_.erase(segment_id);
        return;
    }

    const size_t index = index_it->second;
    const size_t last_index = disk_clock_segments_.size() - 1;
    if (index != last_index) {
        const auto moved_segment_id = disk_clock_segments_[last_index];
        disk_clock_segments_[index] = moved_segment_id;
        disk_clock_index_[moved_segment_id] = index;
    }

    disk_clock_segments_.pop_back();
    disk_clock_index_.erase(segment_id);
    disk_segment_usage_.erase(segment_id);

    if (disk_clock_segments_.empty()) {
        disk_clock_hand_ = 0;
    } else if (index < disk_clock_hand_) {
        --disk_clock_hand_;
    } else if (disk_clock_hand_ >= disk_clock_segments_.size()) {
        disk_clock_hand_ = 0;
    }
}

size_t
ExprResCacheManager::RemoveDiskSegmentFile(int64_t segment_id) {
    auto it = disk_files_.find(segment_id);
    if (it == disk_files_.end()) {
        RemoveDiskClockSegment(segment_id);
        disk_ineligible_segments_.erase(segment_id);
        return 0;
    }

    auto* file = it->second.get();
    const size_t file_size = file ? file->GetUsedBytes() : 0;
    if (file) {
        file->Close();
    }

    std::string path = config_.disk_base_path + "/seg_" +
                       std::to_string(segment_id) + ".cache";
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        LOG_WARN("ExprResCacheManager: failed to remove cache file {}: {}",
                 path,
                 ec.message());
    }

    disk_files_.erase(it);
    disk_ineligible_segments_.erase(segment_id);
    RemoveDiskClockSegment(segment_id);
    return file_size;
}

void
ExprResCacheManager::SyncUsageMetrics(size_t memory_bytes, size_t disk_bytes) {
    auto old_memory = reported_memory_bytes_.exchange(
        memory_bytes, std::memory_order_relaxed);
    auto old_disk =
        reported_disk_bytes_.exchange(disk_bytes, std::memory_order_relaxed);

    UpdateGauge(
        cachinglayer::monitor::cache_loaded_bytes(
            cachinglayer::CellDataType::OTHER,
            cachinglayer::StorageType::MEMORY),
        static_cast<int64_t>(memory_bytes) - static_cast<int64_t>(old_memory));
    UpdateGauge(
        cachinglayer::monitor::cache_loaded_bytes(
            cachinglayer::CellDataType::OTHER, cachinglayer::StorageType::DISK),
        static_cast<int64_t>(disk_bytes) - static_cast<int64_t>(old_disk));
}

}  // namespace exec
}  // namespace milvus
