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

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "common/Types.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

// Forward declarations for backend types
class EntryPool;
class DiskSlotFile;

// Lightweight frequency tracker using direct-mapped counter array.
// Used for cache admission control: only cache expressions seen >= threshold times.
// Approximate — hash collisions cause shared counters, which is acceptable.
class FrequencyTracker {
 public:
    // Record a signature hash and return whether it has reached the threshold.
    bool
    RecordAndCheck(uint64_t sig_hash, uint8_t threshold) {
        if (threshold <= 1) {
            return true;  // no admission control
        }
        size_t slot = sig_hash % kNumSlots;
        uint8_t old = counters_[slot].fetch_add(1, std::memory_order_relaxed);
        // Cap at 255 to prevent overflow
        if (old >= 254) {
            counters_[slot].store(254, std::memory_order_relaxed);
        }

        // Periodic decay: every kDecayInterval records, halve all counters.
        uint64_t count = total_records_.fetch_add(1, std::memory_order_relaxed);
        if ((count + 1) % kDecayInterval == 0) {
            Decay();
        }

        return (old + 1) >= threshold;
    }

    void
    Reset() {
        for (auto& c : counters_) {
            c.store(0, std::memory_order_relaxed);
        }
        total_records_.store(0, std::memory_order_relaxed);
    }

 private:
    void
    Decay() {
        for (auto& c : counters_) {
            uint8_t val = c.load(std::memory_order_relaxed);
            c.store(val / 2, std::memory_order_relaxed);
        }
    }

    static constexpr size_t kNumSlots = 16384;  // 16K slots = 16KB memory
    static constexpr uint64_t kDecayInterval =
        10000;  // decay every 10K records

    std::atomic<uint8_t> counters_[kNumSlots]{};
    std::atomic<uint64_t> total_records_{0};
};

// Cache mode: Memory (EntryPool) or Disk (DiskSlotFile).
enum class CacheMode { Memory, Disk };

// Configuration for ExprResCacheManager.
struct CacheConfig {
    CacheMode mode{CacheMode::Disk};
    // Memory mode
    size_t mem_max_bytes{2ULL * 1024 * 1024 * 1024};
    bool compression_enabled{true};
    uint8_t admission_threshold{2};
    int64_t mem_min_eval_duration_us{100};
    // Disk mode
    std::string disk_base_path;
    uint64_t disk_max_file_size{256ULL * 1024 * 1024};
    int64_t disk_min_eval_duration_us{200};
};

// Process-level expression result cache with mode dispatch.
// Routes to EntryPool (memory mode) or per-segment DiskSlotFile (disk mode).
class ExprResCacheManager {
 public:
    struct Key {
        int64_t segment_id{0};
        std::string signature;  // expr signature including parameters

        bool
        operator==(const Key& other) const {
            return segment_id == other.segment_id &&
                   signature == other.signature;
        }
    };

    struct KeyHasher {
        size_t
        operator()(const Key& k) const noexcept {
            std::hash<int64_t> h1;
            std::hash<std::string> h2;
            return (h1(k.segment_id) * 1315423911u) ^ h2(k.signature);
        }
    };

    struct Value {
        std::shared_ptr<TargetBitmap> result;        // filter result bits
        std::shared_ptr<TargetBitmap> valid_result;  // valid bits
        int64_t active_count{0};                     // active count when cached
        size_t bytes{0};  // approximate size in bytes
        int64_t eval_duration_us{
            0};  // eval duration in us, 0 = skip cost check
    };

 public:
    static ExprResCacheManager&
    Instance();
    static void
    SetEnabled(bool enabled);
    static bool
    IsEnabled();
    static void
    Init(size_t capacity_bytes, bool enabled);

    // Configure the cache mode and parameters.
    void
    SetConfig(const CacheConfig& config);

    // Backward-compatible shim: maps old SetDiskConfig parameters to CacheConfig.
    void
    SetDiskConfig(const std::string& base_path,
                  uint64_t max_total_size,
                  uint64_t max_segment_file_size,
                  bool compression_enabled,
                  uint8_t admission_threshold = 1,
                  int64_t min_eval_duration_us = 0,
                  bool in_memory = false);

    void
    SetCapacityBytes(size_t capacity_bytes);
    size_t
    GetCapacityBytes() const;
    size_t
    GetCurrentBytes() const;
    size_t
    GetEntryCount() const;

    // Try to get cached value. If found, returns true and fills out_value.
    // NOTE: caller must pre-set out_value.active_count for staleness check.
    bool
    Get(const Key& key, Value& out_value);

    // Insert or update cache entry. The provided value.result must be non-null.
    void
    Put(const Key& key, const Value& value);

    void
    Clear();

    // Erase all cache entries of a specific segment. Returns number of erased entries.
    size_t
    EraseSegment(int64_t segment_id);

 private:
    ExprResCacheManager() = default;

    // Disk mode helper: get or create DiskSlotFile for a segment.
    DiskSlotFile*
    GetOrCreateDiskFile(int64_t segment_id, size_t row_count);

 private:
    static std::atomic<bool> enabled_;

    CacheConfig config_;

    // Memory mode backend
    std::unique_ptr<EntryPool> entry_pool_;

    // Disk mode backend
    mutable std::shared_mutex disk_files_mutex_;
    std::unordered_map<int64_t, std::unique_ptr<DiskSlotFile>> disk_files_;
};

// Helper API: erase all cache for a given segment id, returns erased entry count
inline size_t
EraseSegmentCache(int64_t segment_id) {
    if (!ExprResCacheManager::IsEnabled()) {
        LOG_INFO("expr res cache is disabled, skip erase segment cache");
        return 0;
    }
    return ExprResCacheManager::Instance().EraseSegment(segment_id);
}

}  // namespace exec
}  // namespace milvus
