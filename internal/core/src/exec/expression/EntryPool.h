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
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "exec/expression/CacheCompressor.h"

namespace prometheus {
class Gauge;
}

namespace milvus {
namespace exec {

// Shared by successive memory backends. A retired payload keeps its charge
// here until its last reader releases it, even after the pool is destroyed.
class ExprCacheMemoryBudget {
 public:
    explicit ExprCacheMemoryBudget(size_t capacity,
                                   bool report_metrics = false);

    bool
    TryAcquire(size_t bytes);
    bool
    CanFit(size_t bytes, size_t reclaimable = 0) const;
    void
    Release(size_t bytes) noexcept;
    void
    SetCapacity(size_t capacity);
    size_t
    GetUsedBytes() const;

 private:
    mutable std::mutex mutex_;
    size_t capacity_;
    size_t used_{0};
    prometheus::Gauge* memory_gauge_{nullptr};
};

// Pure in-memory expression result cache with Clock eviction.
//
// Stores compressed bitset entries in heap memory (malloc-managed).
// Uses Clock algorithm for eviction — near-LRU quality with concurrent reads.
//
// Thread safety:
//   - Lookup: shared_lock(index) + atomic usage_count update
//   - Decode: immutable payload; no manager or pool lock
//   - Put:  unique_lock(index) + potential Clock eviction
//   - EraseSegment: unique_lock(index)
//
// Memory management:
//   - Entries and active readers share immutable compressed payloads
//   - Fragmentation handled by jemalloc/tcmalloc (Milvus's default allocator)
//   - Memory remains charged through the last payload reference

class EntryPool {
 public:
    struct Key {
        int64_t segment_id{0};
        uint64_t sig_hash{0};
        std::string signature;

        bool
        operator==(const Key& other) const {
            return segment_id == other.segment_id &&
                   sig_hash == other.sig_hash && signature == other.signature;
        }
    };

    struct KeyHasher {
        size_t
        operator()(const Key& k) const noexcept {
            return std::hash<int64_t>()(k.segment_id) * 1315423911u ^
                   std::hash<uint64_t>()(k.sig_hash) ^
                   std::hash<std::string>()(k.signature);
        }
    };

    class Payload {
     private:
        friend class EntryPool;
        struct Charge {
            std::shared_ptr<ExprCacheMemoryBudget> budget;
            size_t bytes{0};
            ~Charge() {
                if (budget) {
                    budget->Release(bytes);
                }
            }
        };
        // Destroy the byte buffer before returning its reservation.
        Charge charge_;

     public:
        Payload(int64_t rows, uint8_t encoding, std::vector<char> bytes)
            : active_count(rows), comp_type(encoding), data(std::move(bytes)) {
        }
        Payload(const Payload&) = delete;
        Payload&
        operator=(const Payload&) = delete;

        bool
        Decode(TargetBitmap& result, TargetBitmap& valid) const;

        const int64_t active_count;
        const uint8_t comp_type;
        const std::vector<char> data;
    };
    using Handle = std::shared_ptr<const Payload>;

    struct Entry {
        Handle payload;
        std::atomic<uint8_t> usage_count{1};
        // The map owns the only Key. Rehash preserves its address; unlink this
        // entry from Clock before erasing the map node.
        const Key* key{nullptr};
        Entry* prev{nullptr};
        Entry* next{nullptr};
    };

    explicit EntryPool(size_t max_bytes,
                       std::shared_ptr<ExprCacheMemoryBudget> budget = {});

    ~EntryPool() = default;

    // Configure pool parameters. Can be called after construction to
    // update settings (e.g., from paramtable config reload).
    void
    Configure(size_t max_bytes,
              bool compression_enabled,
              int64_t min_eval_duration_us);

    // The returned handle survives erase, clear, replacement, and pool
    // destruction. No iterator, Entry reference, or pool lock escapes Lookup.
    Handle
    Lookup(int64_t segment_id,
           const std::string& signature,
           int64_t active_count);

    // Insert a compressed entry. Compression is done internally.
    // May trigger Clock eviction if over capacity.
    // Subject to latency admission here; the manager applies frequency
    // admission before calling this method.
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
        return budget_->GetUsedBytes();
    }

    size_t
    GetEntryCount() const {
        std::shared_lock lock(mutex_);
        return entries_.size();
    }

 private:
    friend class EntryPoolTestPeer;

    // Append before hand without changing the next victim. The same links
    // also hold provisional eviction candidates. Requires unique_lock.
    static void
    LinkClockEntry(Entry*& hand, Entry* entry) noexcept;
    static void
    UnlinkClockEntry(Entry*& hand, Entry* entry) noexcept;

    // Select Clock candidates before removing any entries. Return false
    // without erasing entries if their immediately reclaimable bytes cannot
    // admit the Put. Include the unheld replacement's charge in reclaimable,
    // but leave its mapping for the caller to replace. Requires unique_lock.
    bool
    EvictForPut(size_t bytes, size_t reclaimable, const Entry* protected_entry);

    size_t max_bytes_;
    std::shared_ptr<ExprCacheMemoryBudget> budget_;

    int64_t min_eval_duration_us_{0};
    bool compression_enabled_{true};

    mutable std::shared_mutex mutex_;
    std::unordered_map<Key, std::unique_ptr<Entry>, KeyHasher> entries_;

    // Intrusive circular list: membership changes only for inserted/erased
    // entries. Neither scanning nor membership maintenance copies keys.
    Entry* clock_hand_{nullptr};
};

}  // namespace exec
}  // namespace milvus
