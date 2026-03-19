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

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Types.h"
#include "exec/expression/CacheCompressor.h"
#include "exec/expression/ExprCache.h"

namespace milvus {
namespace exec {

// Pure in-memory expression result cache with Clock eviction.
//
// Stores compressed bitset entries in heap memory (malloc-managed).
// Uses Clock algorithm for eviction — near-LRU quality with zero
// Get-path lock overhead (only one atomic store per access).
//
// Thread safety:
//   - Get:  shared_lock(index) + atomic usage_count update (no write lock)
//   - Put:  unique_lock(index) + potential Clock eviction
//   - EraseSegment: unique_lock(index)
//
// Memory management:
//   - Each entry owns a std::vector<char> for compressed data
//   - Fragmentation handled by jemalloc/tcmalloc (Milvus's default allocator)
//   - Total memory tracked by current_bytes_ atomic counter

class EntryPool {
 public:
    struct Key {
        int64_t segment_id{0};
        uint64_t sig_hash{0};

        bool
        operator==(const Key& other) const {
            return segment_id == other.segment_id && sig_hash == other.sig_hash;
        }
    };

    struct KeyHasher {
        size_t
        operator()(const Key& k) const noexcept {
            return std::hash<int64_t>()(k.segment_id) * 1315423911u ^
                   std::hash<uint64_t>()(k.sig_hash);
        }
    };

    struct Entry {
        Key key;
        std::string signature;    // full expression string for exact match
        int64_t active_count{0};  // staleness check
        uint8_t comp_type{0};
        std::vector<char> data;  // compressed payload (header + body)
        std::atomic<uint8_t> usage_count{0};  // Clock: 0-5, accessed → ++

        size_t
        MemoryUsage() const {
            return sizeof(Entry) + data.capacity() + signature.capacity();
        }
    };

    explicit EntryPool(size_t max_bytes) : max_bytes_(max_bytes) {
    }

    ~EntryPool() = default;

    // Configure pool parameters. Can be called after construction to
    // update settings (e.g., from paramtable config reload).
    void
    Configure(size_t max_bytes,
              bool compression_enabled,
              uint8_t admission_threshold,
              int64_t min_eval_duration_us);

    // Try to get a cached entry. On hit, fills out_result.
    // If out_valid_all_ones is set to true, out_valid is NOT filled (caller
    // should treat valid as all-ones, saving ~30μs of bitmap construction).
    // Returns false on miss, signature mismatch, or staleness mismatch.
    bool
    Get(int64_t segment_id,
        const std::string& signature,
        int64_t active_count,
        TargetBitmap& out_result,
        TargetBitmap& out_valid);

    // Insert a compressed entry. Compression is done internally.
    // May trigger Clock eviction if over capacity.
    // Subject to frequency and latency admission control.
    void
    Put(int64_t segment_id,
        const std::string& signature,
        int64_t active_count,
        const TargetBitmap& result,
        const TargetBitmap& valid,
        int64_t eval_duration_us = 0);

    // Erase all entries belonging to a segment. Returns number erased.
    size_t
    EraseSegment(int64_t segment_id);

    // Clear all entries.
    void
    Clear();

    size_t
    GetCurrentBytes() const {
        return current_bytes_.load(std::memory_order_relaxed);
    }

    size_t
    GetEntryCount() const {
        std::shared_lock lock(mutex_);
        return entries_.size();
    }

 private:
    // Clock sweep: find and evict one entry with usage_count == 0.
    // Entries with usage_count > 0 get decremented (one "chance" per sweep).
    // Must be called under unique_lock.
    void
    EvictOne();

    size_t max_bytes_;
    std::atomic<size_t> current_bytes_{0};

    // Admission control
    FrequencyTracker frequency_tracker_;
    uint8_t admission_threshold_{1};
    int64_t min_eval_duration_us_{0};
    bool compression_enabled_{true};

    mutable std::shared_mutex mutex_;
    std::unordered_map<Key, std::unique_ptr<Entry>, KeyHasher> entries_;

    // Clock state: we iterate over entries_ using a persistent iterator
    // position. Since unordered_map iteration order is stable between
    // modifications, we track position as an index into a separate vector.
    // Rebuilt lazily when entries are added/removed.
    std::vector<Key> clock_keys_;  // snapshot of keys for clock sweep
    size_t clock_hand_{0};
    bool clock_dirty_{true};  // true when clock_keys_ needs rebuild

    void
    RebuildClockKeys() {
        clock_keys_.clear();
        clock_keys_.reserve(entries_.size());
        for (const auto& [k, _] : entries_) {
            clock_keys_.push_back(k);
        }
        clock_hand_ = 0;
        clock_dirty_ = false;
    }
};

}  // namespace exec
}  // namespace milvus
