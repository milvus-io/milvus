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

#include "exec/expression/EntryPool.h"

#include "log/Log.h"
#include "xxhash.h"

namespace milvus {
namespace exec {

void
EntryPool::Configure(size_t max_bytes,
                     bool compression_enabled,
                     uint8_t admission_threshold,
                     int64_t min_eval_duration_us) {
    std::unique_lock lock(mutex_);
    max_bytes_ = max_bytes;
    compression_enabled_ = compression_enabled;
    admission_threshold_ = admission_threshold;
    min_eval_duration_us_ = min_eval_duration_us;
}

bool
EntryPool::Get(int64_t segment_id,
               const std::string& signature,
               int64_t active_count,
               TargetBitmap& out_result,
               TargetBitmap& out_valid) {
    uint64_t sig_hash = XXH64(signature.data(), signature.size(), 0);
    Key key{segment_id, sig_hash, signature, active_count};

    std::shared_lock lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }

    auto& entry = *it->second;

    // Staleness check
    if (entry.active_count != active_count) {
        return false;
    }

    // Bump usage_count (atomic, no lock needed — Clock "续命")
    auto old = entry.usage_count.load(std::memory_order_relaxed);
    if (old < 5) {
        entry.usage_count.store(old + 1, std::memory_order_relaxed);
    }

    // Decompress (CacheCompressor handles valid_all_ones optimization internally)
    return CacheCompressor::Decompress(entry.data.data(),
                                       static_cast<uint32_t>(entry.data.size()),
                                       entry.comp_type,
                                       out_result,
                                       out_valid);
}

void
EntryPool::Put(int64_t segment_id,
               const std::string& signature,
               int64_t active_count,
               const TargetBitmap& result,
               const TargetBitmap& valid,
               int64_t eval_duration_us) {
    uint64_t sig_hash = XXH64(signature.data(), signature.size(), 0);
    Key key{segment_id, sig_hash, signature, active_count};

    bool replacing_existing = false;
    bool same_signature_cached = false;
    {
        std::shared_lock lock(mutex_);
        replacing_existing = entries_.find(key) != entries_.end();
        if (!replacing_existing) {
            for (const auto& [entry_key, _] : entries_) {
                if (entry_key.segment_id == segment_id &&
                    entry_key.sig_hash == sig_hash &&
                    entry_key.signature == signature) {
                    same_signature_cached = true;
                    break;
                }
            }
        }
    }

    // Latency admission: skip cheap expressions
    if (!replacing_existing && !same_signature_cached &&
        min_eval_duration_us_ > 0 && eval_duration_us > 0 &&
        eval_duration_us < min_eval_duration_us_) {
        return;
    }

    // Frequency admission: only cache expressions seen >= threshold times
    if (!replacing_existing && !same_signature_cached &&
        !frequency_tracker_.RecordAndCheck(sig_hash, admission_threshold_)) {
        return;
    }

    // Compress internally
    uint8_t comp_type = 0;
    std::vector<char> compressed_data = CacheCompressor::Compress(
        result, valid, compression_enabled_, comp_type);

    size_t entry_mem =
        sizeof(Entry) + compressed_data.capacity() + signature.capacity() * 2;

    std::unique_lock lock(mutex_);

    auto existing = entries_.find(key);
    if (existing != entries_.end()) {
        current_bytes_.fetch_sub(existing->second->MemoryUsage(),
                                 std::memory_order_relaxed);
        entries_.erase(existing);
        clock_dirty_ = true;
    }

    // Evict until enough space
    while (current_bytes_.load(std::memory_order_relaxed) + entry_mem >
           max_bytes_) {
        if (entries_.empty()) {
            break;
        }
        EvictOne();
    }

    // Insert
    auto entry = std::make_unique<Entry>();
    entry->key = key;
    entry->signature = signature;
    entry->active_count = active_count;
    entry->comp_type = comp_type;
    entry->data = std::move(compressed_data);
    entry->usage_count.store(1, std::memory_order_relaxed);

    current_bytes_.fetch_add(entry->MemoryUsage(), std::memory_order_relaxed);
    entries_[key] = std::move(entry);
    clock_dirty_ = true;  // clock_keys_ needs rebuild
}

size_t
EntryPool::EraseSegment(int64_t segment_id) {
    std::unique_lock lock(mutex_);
    size_t erased = 0;

    for (auto it = entries_.begin(); it != entries_.end();) {
        if (it->first.segment_id == segment_id) {
            current_bytes_.fetch_sub(it->second->MemoryUsage(),
                                     std::memory_order_relaxed);
            it = entries_.erase(it);
            ++erased;
        } else {
            ++it;
        }
    }

    if (erased > 0) {
        clock_dirty_ = true;
    }
    return erased;
}

void
EntryPool::Clear() {
    std::unique_lock lock(mutex_);
    entries_.clear();
    clock_keys_.clear();
    clock_hand_ = 0;
    clock_dirty_ = true;
    current_bytes_.store(0, std::memory_order_relaxed);
}

void
EntryPool::EvictOne() {
    // Must be called under unique_lock
    if (entries_.empty()) {
        return;
    }

    // Rebuild clock key snapshot if needed
    if (clock_dirty_) {
        RebuildClockKeys();
    }

    if (clock_keys_.empty()) {
        return;
    }

    // Clock sweep: scan entries, decrement usage_count, evict first with 0
    // Worst case: full scan + one more round (all entries accessed)
    size_t max_scan = clock_keys_.size() * 2;
    for (size_t i = 0; i < max_scan; ++i) {
        if (clock_hand_ >= clock_keys_.size()) {
            clock_hand_ = 0;
        }
        const auto& key = clock_keys_[clock_hand_];
        auto it = entries_.find(key);
        if (it == entries_.end()) {
            // Entry was removed (e.g. by EraseSegment) but clock_keys_ stale
            clock_hand_++;
            continue;
        }

        auto& entry = *it->second;
        auto count = entry.usage_count.load(std::memory_order_relaxed);
        if (count > 0) {
            // Give it a chance: decrement and move on
            entry.usage_count.store(count - 1, std::memory_order_relaxed);
            clock_hand_++;
        } else {
            // usage_count == 0: evict this entry
            current_bytes_.fetch_sub(entry.MemoryUsage(),
                                     std::memory_order_relaxed);
            LOG_DEBUG("EntryPool::EvictOne segment_id={}, sig_hash={}",
                      key.segment_id,
                      key.sig_hash);

            // Remove from clock_keys_ (swap with last for O(1))
            clock_keys_[clock_hand_] = clock_keys_.back();
            clock_keys_.pop_back();
            entries_.erase(it);
            // Don't increment clock_hand_ — the swapped-in key is now here
            return;
        }
    }

    // If we get here, all entries have high usage_count (all hot).
    // Force evict the one at current clock_hand_.
    if (!clock_keys_.empty()) {
        if (clock_hand_ >= clock_keys_.size()) {
            clock_hand_ = 0;
        }
        const auto& key = clock_keys_[clock_hand_];
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            current_bytes_.fetch_sub(it->second->MemoryUsage(),
                                     std::memory_order_relaxed);
            clock_keys_[clock_hand_] = clock_keys_.back();
            clock_keys_.pop_back();
            entries_.erase(it);
        }
    }
}

}  // namespace exec
}  // namespace milvus
