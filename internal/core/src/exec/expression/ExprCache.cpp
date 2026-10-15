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

#include <folly/ScopeGuard.h>
#include <unistd.h>
#include <filesystem>
#include <functional>
#include <limits>
#include <utility>

#include "cachinglayer/Metrics.h"
#include "exec/expression/DiskSlotFile.h"
#include "exec/expression/EntryPool.h"
#include "monitor/Monitor.h"
#include "xxhash.h"

namespace milvus {
namespace exec {

class ExprCacheMaterializationBudgetState {
 public:
    explicit ExprCacheMaterializationBudgetState(size_t capacity_bytes)
        : capacity_bytes_(capacity_bytes),
          memory_gauge_(cachinglayer::monitor::cache_loaded_bytes(
              cachinglayer::CellDataType::OTHER,
              cachinglayer::StorageType::MEMORY)) {
    }

    bool
    TryAcquire(size_t bytes) {
        std::lock_guard lock(mutex_);
        if (capacity_bytes_ == 0 || bytes > capacity_bytes_ ||
            used_bytes_ > capacity_bytes_ - bytes) {
            return false;
        }
        used_bytes_ += bytes;
        memory_gauge_.Increment(bytes);
        return true;
    }

    void
    Release(size_t bytes) noexcept {
        std::lock_guard lock(mutex_);
        if (bytes > used_bytes_) {
            bytes = used_bytes_;
        }
        used_bytes_ -= bytes;
        memory_gauge_.Decrement(bytes);
    }

    void
    SetCapacity(size_t capacity_bytes) {
        std::lock_guard lock(mutex_);
        capacity_bytes_ = capacity_bytes;
    }

    size_t
    GetUsedBytes() const {
        std::lock_guard lock(mutex_);
        return used_bytes_;
    }

 private:
    mutable std::mutex mutex_;
    size_t capacity_bytes_{0};
    size_t used_bytes_{0};
    // Lease lifetime, including outstanding queries after clear/reconfigure,
    // determines this contribution to the shared cachinglayer memory gauge.
    prometheus::Gauge& memory_gauge_;
};

namespace {

std::optional<size_t>
FullBitmapPairBytes(int64_t active_count) {
    if (active_count < 0) {
        return std::nullopt;
    }
    const auto bits = static_cast<size_t>(active_count);
    constexpr size_t kBitsPerWord = 64;
    constexpr size_t kBytesPerWord = sizeof(uint64_t);
    if (bits > std::numeric_limits<size_t>::max() - (kBitsPerWord - 1)) {
        return std::nullopt;
    }
    const auto words = (bits + kBitsPerWord - 1) / kBitsPerWord;
    if (words > std::numeric_limits<size_t>::max() / (2 * kBytesPerWord)) {
        return std::nullopt;
    }
    return words * kBytesPerWord * 2;
}

struct MaterializedBitmapPair {
    explicit MaterializedBitmapPair(
        ExprResCacheManager::MaterializationLease lease)
        : lease(std::move(lease)) {
    }

    // Keep the reservation alive until after both bitmap buffers are freed.
    ExprResCacheManager::MaterializationLease lease;
    TargetBitmap result{0};
    TargetBitmap valid{0};
};

uint64_t
AdmissionKeyHash(const ExprResCacheManager::Key& key, int64_t active_count) {
    // Count reuse of the same expression snapshot. Different segments or
    // growing row counts must not make a one-off snapshot look recurrent.
    // Storage still keeps only one snapshot per (segment_id, signature).
    const auto key_hash = XXH64(key.signature.data(),
                                key.signature.size(),
                                static_cast<uint64_t>(key.segment_id));
    return XXH64(&active_count, sizeof(active_count), key_hash);
}

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

ExprResCacheManager::MaterializationLease::MaterializationLease(
    std::shared_ptr<ExprCacheMaterializationBudgetState> budget, size_t bytes)
    : budget_(std::move(budget)), bytes_(bytes) {
}

ExprResCacheManager::MaterializationLease::MaterializationLease(
    MaterializationLease&& other) noexcept
    : budget_(std::exchange(other.budget_, nullptr)),
      bytes_(std::exchange(other.bytes_, 0)) {
}

ExprResCacheManager::MaterializationLease&
ExprResCacheManager::MaterializationLease::operator=(
    MaterializationLease&& other) noexcept {
    if (this != &other) {
        Release();
        budget_ = std::exchange(other.budget_, nullptr);
        bytes_ = std::exchange(other.bytes_, 0);
    }
    return *this;
}

ExprResCacheManager::MaterializationLease::~MaterializationLease() {
    Release();
}

void
ExprResCacheManager::MaterializationLease::Release() {
    if (!budget_) {
        return;
    }
    auto budget = std::exchange(budget_, nullptr);
    const auto bytes = std::exchange(bytes_, 0);
    budget->Release(bytes);
}

ExprResCacheManager::ExprResCacheManager()
    : memory_budget_(std::make_shared<ExprCacheMemoryBudget>(
          config_.mem_max_bytes, /*report_metrics=*/true)),
      materialization_budget_(
          std::make_shared<ExprCacheMaterializationBudgetState>(
              config_.materialization_max_bytes)) {
}

ExprResCacheManager&
ExprResCacheManager::Instance() {
    static ExprResCacheManager instance;
    return instance;
}

void
ExprResCacheManager::SetEnabled(bool enabled) {
    auto current = enabled_.load(std::memory_order_acquire);
    while (current != enabled) {
        // Invalidate old tickets before publishing an enabled state. A Put
        // that observes enabled=true with acquire ordering must also observe
        // this epoch change.
        Instance().config_epoch_.fetch_add(1, std::memory_order_release);
        if (enabled_.compare_exchange_weak(current,
                                           enabled,
                                           std::memory_order_release,
                                           std::memory_order_acquire)) {
            return;
        }
    }
}

bool
ExprResCacheManager::IsEnabled() noexcept {
    return enabled_.load();
}

bool
ExprResCacheManager::SetConfig(const CacheConfig& config) {
    std::unique_lock state_lock(state_mutex_);
    config_epoch_.fetch_add(1, std::memory_order_relaxed);
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
            SyncDiskUsageMetrics(0);
            return false;
        }
    }

    config_ = config;
    materialization_budget_->SetCapacity(config_.materialization_max_bytes);
    frequency_tracker_.Reset();
    if (config_.mode == CacheMode::Memory) {
        entry_pool_ =
            std::make_unique<EntryPool>(config_.mem_max_bytes, memory_budget_);
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
        SyncDiskUsageMetrics(0);
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
        SyncDiskUsageMetrics(0);
        return true;
    }
}

CacheMode
ExprResCacheManager::GetMode() const {
    std::shared_lock state_lock(state_mutex_);
    return config_.mode;
}

bool
ExprResCacheManager::CanCacheSegment(SegmentType segment_type) const noexcept {
    bool can_cache = false;
    RunExprCacheBestEffort([&]() {
        std::shared_lock state_lock(state_mutex_);
        can_cache = CanCacheSegmentLocked(segment_type);
    });
    return can_cache;
}

bool
ExprResCacheManager::CanCacheSegmentLocked(
    SegmentType segment_type) const noexcept {
    return segment_type == SegmentType::Sealed ||
           (segment_type == SegmentType::Growing &&
            config_.mode == CacheMode::Memory && config_.mem_enable_growing);
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
    cfg.materialization_max_bytes = max_total_size;
    cfg.compression_enabled = compression_enabled;
    cfg.admission_threshold = admission_threshold;
    cfg.mem_min_eval_duration_us = min_eval_duration_us;
    SetConfig(cfg);
}

void
ExprResCacheManager::SetCapacityBytes(size_t capacity_bytes) {
    std::unique_lock state_lock(state_mutex_);
    config_epoch_.fetch_add(1, std::memory_order_relaxed);
    // Backward compatibility: ensure memory-mode EntryPool exists.
    // Old callers used SetCapacityBytes to configure the cache size;
    // the V2 manager needs an EntryPool to actually store entries.
    // Use threshold=1 and min_eval_duration_us=0 (no admission control)
    // to match the old manager's unconditional caching behavior.
    config_.mode = CacheMode::Memory;
    config_.mem_max_bytes = capacity_bytes;
    config_.materialization_max_bytes = capacity_bytes;
    config_.mem_enable_growing = false;
    config_.admission_threshold = 1;
    config_.mem_min_eval_duration_us = 0;
    if (!entry_pool_) {
        entry_pool_ =
            std::make_unique<EntryPool>(capacity_bytes, memory_budget_);
    }
    entry_pool_->Configure(capacity_bytes,
                           config_.compression_enabled,
                           config_.mem_min_eval_duration_us);
    materialization_budget_->SetCapacity(capacity_bytes);
}

size_t
ExprResCacheManager::GetCurrentBytes() const {
    std::shared_lock state_lock(state_mutex_);
    if (config_.mode == CacheMode::Memory) {
        return GetMemoryBytes();
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

size_t
ExprResCacheManager::GetMemoryBytes() const {
    return memory_budget_->GetUsedBytes();
}

size_t
ExprResCacheManager::GetMaterializationBytes() const {
    return materialization_budget_->GetUsedBytes();
}

std::optional<ExprResCacheManager::MaterializationLease>
ExprResCacheManager::TryAcquireMaterialization(int64_t active_count) {
    const auto bytes = FullBitmapPairBytes(active_count);
    if (!bytes.has_value() || !materialization_budget_->TryAcquire(*bytes)) {
        return std::nullopt;
    }
    return MaterializationLease(materialization_budget_, *bytes);
}

bool
ExprResCacheManager::Get(const Key& key, Value& out_value) noexcept {
    bool hit = false;
    if (!RunExprCacheBestEffort([&]() {
            hit = GetWithStatus(key, out_value) == LookupResult::Hit;
        })) {
        out_value.result.reset();
        out_value.valid_result.reset();
        out_value.bytes = 0;
        return false;
    }
    return hit;
}

ExprResCacheManager::LookupResult
ExprResCacheManager::GetWithStatus(const Key& key, Value& out_value) {
    return GetWithStatus(key, out_value, {});
}

ExprResCacheManager::LookupResult
ExprResCacheManager::GetWithStatus(const Key& key,
                                   Value& out_value,
                                   const std::function<void()>& before_decode) {
    out_value.result.reset();
    out_value.valid_result.reset();
    out_value.bytes = 0;
    if (!IsEnabled()) {
        return LookupResult::Miss;
    }

    std::shared_ptr<MaterializedBitmapPair> materialized;
    // Free partially decoded buffers before their reservation on every exit.
    TargetBitmap result{0};
    TargetBitmap valid{0};
    auto failure = LookupResult::Miss;
    auto before_materialize = [&]() {
        auto lease = TryAcquireMaterialization(out_value.active_count);
        if (!lease.has_value()) {
            failure = LookupResult::ResourceLimit;
            return false;
        }
        materialized =
            std::make_shared<MaterializedBitmapPair>(std::move(*lease));
        return true;
    };

    {
        std::shared_lock state_lock(state_mutex_);
        if (!IsEnabled()) {
            return LookupResult::Miss;
        }
        if (config_.mode == CacheMode::Memory) {
            if (!entry_pool_) {
                return LookupResult::Miss;
            }
            auto payload = entry_pool_->Lookup(
                key.segment_id, key.signature, out_value.active_count);
            if (!payload) {
                return LookupResult::Miss;
            }
            // Lookup has released the pool lock. The immutable payload owns
            // its bytes and charge independently of the pool/config lifetime.
            state_lock.unlock();
            if (!before_materialize()) {
                return failure;
            }
            if (before_decode) {
                before_decode();
            }
            if (!payload->Decode(result, valid)) {
                return LookupResult::Miss;
            }
        } else {
            // Disk slots still require their file/metadata locks during I/O.
            std::shared_lock lock(disk_files_mutex_);
            auto it = disk_files_.find(key.segment_id);
            if (it == disk_files_.end() || !it->second) {
                return LookupResult::Miss;
            }
            if (!it->second->Get(key.signature,
                                 out_value.active_count,
                                 result,
                                 valid,
                                 std::ref(before_materialize))) {
                return failure;
            }
            TryTouchDiskSegment(key.segment_id);
        }
    }

    materialized->result = std::move(result);
    materialized->valid = std::move(valid);
    out_value.result =
        std::shared_ptr<TargetBitmap>(materialized, &materialized->result);
    out_value.valid_result =
        std::shared_ptr<TargetBitmap>(materialized, &materialized->valid);
    out_value.bytes = FullBitmapPairBytes(out_value.active_count).value_or(0);
    ::milvus::monitor::internal_expr_cache_hit_total.Increment();
    return LookupResult::Hit;
}

void
ExprResCacheManager::Put(const Key& key, const Value& value) noexcept {
    RunExprCacheBestEffort([&]() { PutInternal(key, value, nullptr); });
}

ExprResCacheManager::AdmissionTicket
ExprResCacheManager::ObserveMiss(const Key& key,
                                 int64_t active_count,
                                 SegmentType segment_type) {
    AdmissionTicket ticket;
    if (!IsEnabled() || active_count < 0) {
        return ticket;
    }

    std::shared_lock state_lock(state_mutex_);
    if (!IsEnabled() || !CanCacheSegmentLocked(segment_type)) {
        return ticket;
    }

    ticket.config_epoch = config_epoch_.load(std::memory_order_relaxed);
    ticket.key_hash = AdmissionKeyHash(key, active_count);

    if (config_.mode == CacheMode::Memory) {
        if (!entry_pool_) {
            return ticket;
        }
    } else {
        if (config_.disk_base_path.empty()) {
            return ticket;
        }
        std::shared_lock lock(disk_files_mutex_);
        if (disk_ineligible_segments_.find(key.segment_id) !=
            disk_ineligible_segments_.end()) {
            return ticket;
        }
    }

    ticket.admitted = frequency_tracker_.RecordAndCheck(
        ticket.key_hash, config_.admission_threshold);
    return ticket;
}

void
ExprResCacheManager::PutAdmitted(const Key& key,
                                 const Value& value,
                                 const AdmissionTicket& ticket) noexcept {
    RunExprCacheBestEffort([&]() { PutInternal(key, value, &ticket); });
}

void
ExprResCacheManager::PutInternal(const Key& key,
                                 const Value& value,
                                 const AdmissionTicket* ticket,
                                 const DiskPutHook& hook) {
    if (!IsEnabled()) {
        return;
    }
    if (!value.result || !value.valid_result) {
        return;
    }
    if (value.active_count < 0 ||
        value.result->size() != static_cast<size_t>(value.active_count) ||
        value.valid_result->size() != static_cast<size_t>(value.active_count)) {
        return;
    }

    std::shared_lock state_lock(state_mutex_);
    if (!IsEnabled()) {
        return;
    }

    if (ticket != nullptr) {
        const auto key_hash = AdmissionKeyHash(key, value.active_count);
        if (!ticket->admitted ||
            ticket->config_epoch !=
                config_epoch_.load(std::memory_order_relaxed) ||
            ticket->key_hash != key_hash) {
            return;
        }
    }

    if (config_.mode == CacheMode::Memory) {
        if (!entry_pool_) {
            return;
        }
        if (config_.mem_min_eval_duration_us > 0 &&
            value.eval_duration_us > 0 &&
            value.eval_duration_us < config_.mem_min_eval_duration_us) {
            return;
        }
        if (ticket == nullptr && !frequency_tracker_.RecordAndCheck(
                                     AdmissionKeyHash(key, value.active_count),
                                     config_.admission_threshold)) {
            return;
        }
        entry_pool_->Put(key.segment_id,
                         key.signature,
                         value.active_count,
                         *value.result,
                         *value.valid_result,
                         value.eval_duration_us);
        SyncDiskUsageMetrics(0);
    } else {
        // Disk mode
        if (config_.disk_base_path.empty()) {
            return;
        }

        {
            std::shared_lock lock(disk_files_mutex_);
            if (disk_ineligible_segments_.find(key.segment_id) !=
                disk_ineligible_segments_.end()) {
                return;
            }
        }

        // Latency admission (disk mode)
        if (config_.disk_min_eval_duration_us > 0 &&
            value.eval_duration_us > 0 &&
            value.eval_duration_us < config_.disk_min_eval_duration_us) {
            return;
        }

        // Frequency admission is mode-independent. Applying it before opening
        // the segment file avoids one-off expressions consuming disk slots and
        // issuing unnecessary pwrite calls.
        if (ticket == nullptr && !frequency_tracker_.RecordAndCheck(
                                     AdmissionKeyHash(key, value.active_count),
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
        auto file_it = disk_files_.find(key.segment_id);
        if (file_it != disk_files_.end() && file_it->second &&
            file_it->second->GetRowCount() !=
                static_cast<int64_t>(value.result->size())) {
            // Disk cache is sealed-only. A row-count change identifies a
            // growing/unstable segment for this backend, so drop the old fixed
            // file and skip future disk puts until the segment/config resets.
            RemoveDiskSegmentFile(key.segment_id);
            disk_ineligible_segments_.insert(key.segment_id);
            SyncDiskUsageMetrics(GetDiskCurrentBytesLocked());
            return;
        }
        const bool create_file =
            file_it == disk_files_.end() || !file_it->second;
        // Finish the allocating Clock registration before creating a file or
        // committing a slot. A failed registration must leave no disk write.
        TouchDiskSegment(key.segment_id, hook);
        auto rollback_new_file = folly::makeGuard([&]() {
            if (create_file) {
                disk_files_.erase(key.segment_id);
                // Reuse the prepared path: exception cleanup must not allocate
                // another path string while handling an allocation failure.
                ::unlink(path.c_str());
                RemoveDiskClockSegment(key.segment_id);
            }
        });
        if (create_file) {
            if (hook) {
                hook(DiskPutStage::FileCreate);
            }
            auto new_file = std::make_unique<DiskSlotFile>(
                key.segment_id,
                path,
                static_cast<int64_t>(value.result->size()),
                config_.disk_max_file_size);
            if (hook) {
                hook(DiskPutStage::FilePublish);
            }
            if (file_it == disk_files_.end()) {
                file_it =
                    disk_files_.emplace(key.segment_id, std::move(new_file))
                        .first;
            } else {
                file_it->second = std::move(new_file);
            }
        }
        if (hook) {
            hook(DiskPutStage::SlotWrite);
        }
        file_it->second->Put(key.signature,
                             value.active_count,
                             *value.result,
                             *value.valid_result);
        if (hook) {
            hook(DiskPutStage::SlotWritten);
        }
        if (create_file && file_it->second->GetUsedCount() == 0) {
            // Open/pwrite failures are reported by leaving the file empty.
            // Roll back its registration and file before returning as well.
            return;
        }
        rollback_new_file.dismiss();
        EvictDiskSegmentsUntilWithinBudget(key.segment_id);
        SyncDiskUsageMetrics(GetDiskCurrentBytesLocked());
    }
}

void
ExprResCacheManager::Clear() {
    std::unique_lock state_lock(state_mutex_);
    config_epoch_.fetch_add(1, std::memory_order_relaxed);
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
    SyncDiskUsageMetrics(0);
}

size_t
ExprResCacheManager::EraseSegment(int64_t segment_id) {
    std::unique_lock state_lock(state_mutex_);
    if (config_.mode == CacheMode::Memory) {
        size_t erased = entry_pool_ ? entry_pool_->EraseSegment(segment_id) : 0;
        SyncDiskUsageMetrics(0);
        return erased;
    } else {
        std::unique_lock lock(disk_files_mutex_);
        if (disk_files_.find(segment_id) == disk_files_.end()) {
            disk_ineligible_segments_.erase(segment_id);
            RemoveDiskClockSegment(segment_id);
            return 0;
        }
        RemoveDiskSegmentFile(segment_id);
        SyncDiskUsageMetrics(GetDiskCurrentBytesLocked());
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
ExprResCacheManager::TouchDiskSegment(int64_t segment_id,
                                      const DiskPutHook& hook) {
    static constexpr uint8_t kMaxSegmentUsage = 5;

    std::lock_guard lock(disk_clock_mutex_);
    if (disk_clock_index_.find(segment_id) == disk_clock_index_.end()) {
        const auto index = disk_clock_segments_.size();
        if (hook) {
            hook(DiskPutStage::ClockAppend);
        }
        disk_clock_segments_.push_back(segment_id);
        try {
            if (hook) {
                hook(DiskPutStage::ClockIndexInsert);
            }
            disk_clock_index_.emplace(segment_id, index);
            if (hook) {
                hook(DiskPutStage::ClockUsageInsert);
            }
            disk_segment_usage_.try_emplace(segment_id, 0);
        } catch (...) {
            disk_clock_index_.erase(segment_id);
            disk_segment_usage_.erase(segment_id);
            disk_clock_segments_.pop_back();
            throw;
        }
    }

    auto& usage = disk_segment_usage_.try_emplace(segment_id, 0).first->second;
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
ExprResCacheManager::SyncDiskUsageMetrics(size_t disk_bytes) {
    const auto old_disk =
        reported_disk_bytes_.exchange(disk_bytes, std::memory_order_relaxed);
    UpdateGauge(
        cachinglayer::monitor::cache_loaded_bytes(
            cachinglayer::CellDataType::OTHER, cachinglayer::StorageType::DISK),
        static_cast<int64_t>(disk_bytes) - static_cast<int64_t>(old_disk));
}

}  // namespace exec
}  // namespace milvus
