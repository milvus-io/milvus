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
#include <chrono>
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
    // generations must not make a one-off snapshot look recurrent.
    // Storage still keeps only one snapshot per (segment_id, signature).
    const auto key_hash = XXH64(&key.signature,
                                sizeof(key.signature),
                                static_cast<uint64_t>(key.segment_id));
    const auto generation = key.SegmentKey().generation;
    const auto snapshot_hash = XXH64(&generation, sizeof(generation), key_hash);
    return XXH64(&active_count, sizeof(active_count), snapshot_hash);
}

const std::string&
DiskProcessTag() {
    static const std::string tag =
        "_p" + std::to_string(::getpid()) + "_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()) +
        "_";
    return tag;
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
        if (entry.path().filename().string().find(DiskProcessTag()) !=
            std::string::npos) {
            continue;
        }
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

ExprCacheGeneration::ExprCacheGeneration()
    : id([] {
          static std::atomic<uint64_t> next{1};
          const auto value = next.fetch_add(1, std::memory_order_relaxed);
          // Never recycle an identity, including on the theoretical wraparound.
          if (value == 0) {
              std::terminate();
          }
          return value;
      }()) {
}

ExprResCacheManager::RetiredGeneration::RetiredGeneration() = default;
ExprResCacheManager::RetiredGeneration::RetiredGeneration(
    RetiredGeneration&&) noexcept = default;
ExprResCacheManager::RetiredGeneration::~RetiredGeneration() = default;

ExprResCacheManager::RetiredGeneration
ExprResCacheManager::RetireGeneration(
    int64_t segment_id, const ExprCacheGenerationPtr& generation) {
    RetiredGeneration retired;
    if (!generation) {
        return retired;
    }
    generation->Retire();
    // Drain puts that passed the token check before retirement. Later puts
    // still check the token under this lock and cannot recreate the entries.
    std::unique_lock state_lock(state_mutex_);
    const ExprCacheSegmentKey key{segment_id, generation->id};
    if (entry_pool_) {
        retired.memory = entry_pool_->DetachSegment(key);
    }
    {
        std::unique_lock files_lock(disk_files_mutex_);
        auto it = disk_files_.find(key);
        if (it != disk_files_.end()) {
            retired.disk = std::move(it->second);
            disk_files_.erase(it);
        }
        disk_ineligible_segments_.erase(key);
        RemoveDiskClockSegment(key);
        if (retired.disk) {
            SyncDiskUsageMetrics(GetDiskCurrentBytesLocked());
        }
    }
    return retired;
}

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
    // Filesystem work happens before publication and never holds state_mutex_.
    std::error_code error;
    if (config.mode == CacheMode::Disk && !config.disk_base_path.empty()) {
        std::filesystem::create_directories(config.disk_base_path, error);
        if (!error) {
            RemoveCacheFilesInDir(config.disk_base_path);
        }
    }
    decltype(disk_files_) detached;
    std::unique_ptr<EntryPool> old_pool;
    std::unique_lock state_lock(state_mutex_);
    config_epoch_.fetch_add(1, std::memory_order_relaxed);
    old_pool = std::move(entry_pool_);
    {
        std::unique_lock files_lock(disk_files_mutex_);
        disk_files_.swap(detached);
        disk_ineligible_segments_.clear();
        std::lock_guard clock_lock(disk_clock_mutex_);
        disk_clock_segments_.clear();
        disk_clock_index_.clear();
        disk_segment_usage_.clear();
        disk_clock_hand_ = 0;
    }
    SyncDiskUsageMetrics(0);
    if (error) {
        SetEnabled(false);
        return false;
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
    }
    return true;
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
    return segment_type == SegmentType::Sealed;
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
    return ReadWithStatus(
        Lookup(key, out_value.active_count), out_value, nullptr, before_decode);
}

ExprResCacheManager::ReadHandle
ExprResCacheManager::Lookup(const Key& key, int64_t active_count) {
    ReadHandle handle;
    handle.signature = key.signature;
    handle.active_count = active_count;
    {
        std::shared_lock state_lock(state_mutex_);
        if (!IsEnabled() || (!key.generation || key.generation->IsRetired())) {
            return {};
        }
        if (config_.mode == CacheMode::Memory) {
            if (entry_pool_) {
                handle.memory = entry_pool_->Lookup(
                    key.SegmentKey(), key.signature, active_count);
            }
        } else {
            std::shared_lock files_lock(disk_files_mutex_);
            const auto it = disk_files_.find(key.SegmentKey());
            if (it != disk_files_.end()) {
                handle.disk = it->second;
            }
        }
    }
    // No manager lock may cover a file lock: even a metadata lookup can wait
    // for readers completing pread on that file.
    if (handle.disk) {
        if (!handle.disk->Contains(key.signature, active_count)) {
            return {};
        }
        TryTouchDiskSegment(key.SegmentKey());
    }
    return handle;
}

ExprResCacheManager::LookupResult
ExprResCacheManager::ReadWithStatus(
    const ReadHandle& handle,
    Value& out_value,
    const std::span<const int32_t>* offsets,
    const std::function<void()>& before_decode) {
    out_value.result.reset();
    out_value.valid_result.reset();
    out_value.bytes = 0;
    if (!handle) {
        return LookupResult::Miss;
    }
    if (offsets) {
        for (auto offset : *offsets) {
            if (offset < 0 || offset >= handle.active_count) {
                return LookupResult::Miss;
            }
        }
    }
    const auto output_rows = offsets ? offsets->size() : handle.active_count;
    std::shared_ptr<MaterializedBitmapPair> materialized;
    TargetBitmap result, valid;
    auto failure = LookupResult::Miss;
    const auto reserve = [&]() {
        auto lease = TryAcquireMaterialization(output_rows);
        if (!lease) {
            failure = LookupResult::ResourceLimit;
            return false;
        }
        materialized =
            std::make_shared<MaterializedBitmapPair>(std::move(*lease));
        if (before_decode) {
            before_decode();
        }
        return true;
    };
    if (handle.memory) {
        if (!reserve()) {
            return failure;
        }
        const auto& payload = *handle.memory;
        const bool decoded = offsets
                                 ? CacheCompressor::Gather(payload.data.data(),
                                                           payload.data.size(),
                                                           payload.comp_type,
                                                           handle.active_count,
                                                           *offsets,
                                                           result,
                                                           valid)
                                 : payload.Decode(result, valid);
        if (!decoded) {
            return LookupResult::Miss;
        }
    } else if (!handle.disk->Get(handle.signature,
                                 handle.active_count,
                                 result,
                                 valid,
                                 reserve,
                                 offsets)) {
        return failure;
    }
    if (result.size() != output_rows || valid.size() != output_rows) {
        return LookupResult::Miss;
    }
    materialized->result = std::move(result);
    materialized->valid = std::move(valid);
    out_value.active_count = handle.active_count;
    out_value.result =
        std::shared_ptr<TargetBitmap>(materialized, &materialized->result);
    out_value.valid_result =
        std::shared_ptr<TargetBitmap>(materialized, &materialized->valid);
    out_value.bytes = FullBitmapPairBytes(output_rows).value_or(0);
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
    if (!IsEnabled() || !CanCacheSegmentLocked(segment_type) ||
        (!key.generation || key.generation->IsRetired())) {
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
        if (disk_ineligible_segments_.find(key.SegmentKey()) !=
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

    // Declared before locks: dropping the last file reference may close/unlink.
    std::vector<std::shared_ptr<DiskSlotFile>> detached;
    std::shared_ptr<DiskSlotFile> file;
    std::shared_lock state_lock(state_mutex_);
    if (!IsEnabled() || (!key.generation || key.generation->IsRetired())) {
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
        entry_pool_->Put(key.SegmentKey(),
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
            if (disk_ineligible_segments_.find(key.SegmentKey()) !=
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

        const auto epoch = config_epoch_.load(std::memory_order_relaxed);
        const auto base_path = config_.disk_base_path;
        const auto max_file_size = config_.disk_max_file_size;
        {
            std::shared_lock files_lock(disk_files_mutex_);
            const auto it = disk_files_.find(key.SegmentKey());
            if (it != disk_files_.end()) {
                file = it->second;
            }
        }
        state_lock.unlock();
        if (!file) {
            if (hook) {
                hook(DiskPutStage::FileCreate);
            }
            static std::atomic<uint64_t> next_file{0};
            const auto path =
                base_path + "/seg_" + std::to_string(key.segment_id) + "_" +
                std::to_string(key.SegmentKey().generation) + DiskProcessTag() +
                std::to_string(next_file.fetch_add(1)) + ".cache";
            file = std::make_shared<DiskSlotFile>(
                key.segment_id, path, value.active_count, max_file_size);
        }
        bool registered = false;
        state_lock.lock();
        if (!IsEnabled() ||
            epoch != config_epoch_.load(std::memory_order_relaxed) ||
            (!key.generation || key.generation->IsRetired())) {
            return;
        }
        {
            std::unique_lock files_lock(disk_files_mutex_);
            if (disk_ineligible_segments_.count(key.SegmentKey())) {
                return;
            }
            const auto it = disk_files_.find(key.SegmentKey());
            if (it != disk_files_.end() && it->second != file) {
                detached.push_back(file);
                file = it->second;
            }
            if (file->GetRowCount() != value.active_count) {
                RemoveDiskSegmentFile(key.SegmentKey(), detached);
                disk_ineligible_segments_.insert(key.SegmentKey());
                SyncDiskUsageMetrics(GetDiskCurrentBytesLocked());
                return;
            }
            if (it == disk_files_.end()) {
                TouchDiskSegment(key.SegmentKey(), hook);
                auto rollback_clock = folly::makeGuard(
                    [&] { RemoveDiskClockSegment(key.SegmentKey()); });
                if (hook) {
                    hook(DiskPutStage::FilePublish);
                }
                disk_files_.emplace(key.SegmentKey(), file);
                registered = true;
                rollback_clock.dismiss();
            } else {
                TouchDiskSegment(key.SegmentKey(), hook);
            }
        }
        state_lock.unlock();
        bool completed = false;
        auto finish = [&] {
            // A detached/reconfigured file can finish its I/O, but cannot
            // republish itself or alter the current generation's accounting.
            std::shared_lock finish_state(state_mutex_);
            std::unique_lock files_lock(disk_files_mutex_);
            const auto it = disk_files_.find(key.SegmentKey());
            if (epoch != config_epoch_.load(std::memory_order_relaxed) ||
                it == disk_files_.end() || it->second != file) {
                return;
            }
            if (registered && (!completed || file->GetUsedCount() == 0)) {
                RemoveDiskSegmentFile(key.SegmentKey(), detached);
            } else {
                EvictDiskSegmentsUntilWithinBudget(key.SegmentKey(), detached);
            }
            SyncDiskUsageMetrics(GetDiskCurrentBytesLocked());
        };
        try {
            file->Put(key.signature,
                      value.active_count,
                      *value.result,
                      *value.valid_result,
                      [&] {
                          if (hook) {
                              hook(DiskPutStage::SlotWrite);
                          }
                      });
            if (hook) {
                hook(DiskPutStage::SlotWritten);
            }
            completed = true;
        } catch (...) {
            finish();
            throw;
        }
        finish();
    }
}

void
ExprResCacheManager::Clear() {
    decltype(disk_files_) detached;
    std::unique_lock state_lock(state_mutex_);
    config_epoch_.fetch_add(1, std::memory_order_relaxed);
    if (entry_pool_) {
        entry_pool_->Clear();
    }
    frequency_tracker_.Reset();
    {
        std::unique_lock lock(disk_files_mutex_);
        disk_files_.swap(detached);
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
    ExprCacheSegmentKey protected_segment_id,
    std::vector<std::shared_ptr<DiskSlotFile>>& detached) {
    if (config_.disk_max_bytes == 0) {
        return 0;
    }

    size_t total = GetDiskCurrentBytesLocked();
    size_t erased = 0;
    while (total > config_.disk_max_bytes) {
        ExprCacheSegmentKey victim_segment_id;
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

        const size_t file_size =
            RemoveDiskSegmentFile(victim_segment_id, detached);
        total = file_size > total ? 0 : total - file_size;
        ++erased;
    }

    if (total > config_.disk_max_bytes) {
        LOG_WARN(
            "ExprResCacheManager: disk cache usage {} exceeds budget {}, "
            "protected segment {} is kept",
            total,
            config_.disk_max_bytes,
            protected_segment_id.segment_id);
    }

    return erased;
}

void
ExprResCacheManager::TouchDiskSegment(ExprCacheSegmentKey segment_id,
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
ExprResCacheManager::TryTouchDiskSegment(ExprCacheSegmentKey segment_id) {
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
ExprResCacheManager::RemoveDiskClockSegment(ExprCacheSegmentKey segment_id) {
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
ExprResCacheManager::RemoveDiskSegmentFile(
    ExprCacheSegmentKey segment_id,
    std::vector<std::shared_ptr<DiskSlotFile>>& detached) {
    auto it = disk_files_.find(segment_id);
    if (it == disk_files_.end()) {
        RemoveDiskClockSegment(segment_id);
        disk_ineligible_segments_.erase(segment_id);
        return 0;
    }

    auto* file = it->second.get();
    const size_t file_size = file ? file->GetUsedBytes() : 0;
    detached.push_back(it->second);

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
