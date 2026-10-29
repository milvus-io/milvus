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

#include <cassert>
#include <folly/ScopeGuard.h>
#include <limits>

#include "cachinglayer/Metrics.h"
#include "log/Log.h"
#include "xxhash.h"

namespace milvus {
namespace exec {

ExprCacheMemoryBudget::ExprCacheMemoryBudget(size_t capacity,
                                             bool report_metrics)
    : capacity_(capacity) {
    if (report_metrics) {
        memory_gauge_ = &cachinglayer::monitor::cache_loaded_bytes(
            cachinglayer::CellDataType::OTHER,
            cachinglayer::StorageType::MEMORY);
    }
}

bool
ExprCacheMemoryBudget::TryAcquire(size_t bytes) {
    std::lock_guard lock(mutex_);
    if (bytes > capacity_ || used_ > capacity_ - bytes) {
        return false;
    }
    used_ += bytes;
    if (memory_gauge_) {
        memory_gauge_->Increment(bytes);
    }
    return true;
}

bool
ExprCacheMemoryBudget::CanFit(size_t bytes, size_t reclaimable) const {
    std::lock_guard lock(mutex_);
    assert(reclaimable <= used_);
    return bytes <= capacity_ && used_ - reclaimable <= capacity_ - bytes;
}

void
ExprCacheMemoryBudget::Release(size_t bytes) noexcept {
    std::lock_guard lock(mutex_);
    assert(bytes <= used_);
    used_ -= bytes;
    if (memory_gauge_) {
        memory_gauge_->Decrement(bytes);
    }
}

void
ExprCacheMemoryBudget::SetCapacity(size_t capacity) {
    std::lock_guard lock(mutex_);
    capacity_ = capacity;
}

size_t
ExprCacheMemoryBudget::GetUsedBytes() const {
    std::lock_guard lock(mutex_);
    return used_;
}

EntryPool::EntryPool(size_t max_bytes,
                     std::shared_ptr<ExprCacheMemoryBudget> budget)
    : max_bytes_(max_bytes),
      budget_(budget ? std::move(budget)
                     : std::make_shared<ExprCacheMemoryBudget>(max_bytes)) {
}

void
EntryPool::Configure(size_t max_bytes,
                     bool compression_enabled,
                     int64_t min_eval_duration_us) {
    std::unique_lock lock(mutex_);
    max_bytes_ = max_bytes;
    budget_->SetCapacity(max_bytes);
    compression_enabled_ = compression_enabled;
    min_eval_duration_us_ = min_eval_duration_us;
}

bool
EntryPool::Payload::Decode(TargetBitmap& result, TargetBitmap& valid) const {
    return CacheCompressor::Decompress(data.data(),
                                       static_cast<uint32_t>(data.size()),
                                       comp_type,
                                       result,
                                       valid);
}

EntryPool::Handle
EntryPool::Lookup(int64_t segment_id,
                  const std::string& signature,
                  int64_t active_count) {
    const uint64_t sig_hash = XXH64(signature.data(), signature.size(), 0);
    Key key{segment_id, sig_hash, signature};
    std::shared_lock lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end() ||
        it->second->payload->active_count != active_count) {
        return {};
    }
    auto& entry = *it->second;
    auto old = entry.usage_count.load(std::memory_order_relaxed);
    if (old < 5) {
        entry.usage_count.store(old + 1, std::memory_order_relaxed);
    }
    return entry.payload;
}

void
EntryPool::Put(int64_t segment_id,
               const std::string& signature,
               int64_t active_count,
               const TargetBitmap& result,
               const TargetBitmap& valid,
               int64_t eval_duration_us) {
    const uint64_t sig_hash = XXH64(signature.data(), signature.size(), 0);
    Key key{segment_id, sig_hash, signature};
    bool compression_enabled;
    {
        std::shared_lock lock(mutex_);
        auto existing = entries_.find(key);
        if (existing != entries_.end() &&
            active_count < existing->second->payload->active_count) {
            return;
        }
        if (min_eval_duration_us_ > 0 && eval_duration_us > 0 &&
            eval_duration_us < min_eval_duration_us_) {
            return;
        }
        compression_enabled = compression_enabled_;
    }

    uint8_t comp_type = 0;
    auto compressed = CacheCompressor::Compress(
        result, valid, compression_enabled, comp_type);
    if (compressed.size() > std::numeric_limits<uint32_t>::max()) {
        return;
    }
    auto payload = std::make_shared<Payload>(
        active_count, comp_type, std::move(compressed));
    auto entry = std::make_unique<Entry>();
    // Charge buffer capacity exactly; metadata and shared-control-block costs
    // are estimates. Retired payloads conservatively retain the whole entry's
    // estimate until their last reader releases them.
    std::unique_lock lock(mutex_);
    auto existing = entries_.find(key);
    const size_t key_capacity = existing == entries_.end()
                                    ? key.signature.capacity()
                                    : existing->first.signature.capacity();
    const size_t entry_mem = sizeof(Entry) + sizeof(Payload) +
                             2 * sizeof(void*) + payload->data.capacity() +
                             sizeof(Key) + key_capacity;
    if (entry_mem > max_bytes_) {
        return;
    }
    // Growing queries may complete out of order. Recheck after compression
    // so an older snapshot cannot overwrite a concurrently published one.
    if (existing != entries_.end() &&
        active_count < existing->second->payload->active_count) {
        return;
    }
    // Allocate the map node (and any rehash) before evicting. The provisional
    // node is invisible to readers under this lock and is not in Clock yet.
    const bool inserted = existing == entries_.end();
    if (inserted) {
        existing = entries_.try_emplace(std::move(key), std::move(entry)).first;
        existing->second->key = &existing->first;
    }
    auto* protected_entry = existing->second.get();
    auto rollback = folly::makeGuard([&]() {
        if (inserted || !protected_entry->payload) {
            if (protected_entry->next != nullptr) {
                UnlinkClockEntry(clock_hand_, protected_entry);
            }
            entries_.erase(existing);
        }
    });
    const size_t reclaimable =
        !inserted && protected_entry->payload.use_count() == 1
            ? protected_entry->payload->charge_.bytes
            : 0;
    if (!EvictForPut(entry_mem, reclaimable, protected_entry)) {
        return;
    }
    if (reclaimable != 0) {
        // Keep the map node, key address, and Clock position on replacement.
        // Return the unheld payload's charge before acquiring the new one.
        protected_entry->payload.reset();
    }
    if (!budget_->TryAcquire(entry_mem)) {
        return;
    }
    payload->charge_.budget = budget_;
    payload->charge_.bytes = entry_mem;
    protected_entry->payload = std::move(payload);
    protected_entry->usage_count.store(1, std::memory_order_relaxed);
    if (inserted) {
        LinkClockEntry(clock_hand_, protected_entry);
    }
    rollback.dismiss();
}

size_t
EntryPool::EraseSegment(int64_t segment_id) {
    std::unique_lock lock(mutex_);
    size_t erased = 0;
    for (auto it = entries_.begin(); it != entries_.end();) {
        if (it->first.segment_id == segment_id) {
            UnlinkClockEntry(clock_hand_, it->second.get());
            it = entries_.erase(it);
            ++erased;
        } else {
            ++it;
        }
    }
    return erased;
}

void
EntryPool::Clear() {
    std::unique_lock lock(mutex_);
    clock_hand_ = nullptr;
    entries_.clear();
}

void
EntryPool::LinkClockEntry(Entry*& hand, Entry* entry) noexcept {
    assert(entry->prev == nullptr && entry->next == nullptr);
    if (hand == nullptr) {
        entry->prev = entry->next = entry;
        hand = entry;
        return;
    }
    entry->prev = hand->prev;
    entry->next = hand;
    hand->prev->next = entry;
    hand->prev = entry;
}

void
EntryPool::UnlinkClockEntry(Entry*& hand, Entry* entry) noexcept {
    assert(hand != nullptr && entry->prev != nullptr && entry->next != nullptr);
    if (entry->next == entry) {
        hand = nullptr;
    } else {
        entry->prev->next = entry->next;
        entry->next->prev = entry->prev;
        if (hand == entry) {
            hand = entry->next;
        }
    }
    entry->prev = entry->next = nullptr;
}

bool
EntryPool::EvictForPut(size_t bytes,
                       size_t reclaimable,
                       const Entry* protected_entry) {
    if (budget_->CanFit(bytes, reclaimable)) {
        return true;
    }
    // Move candidates into a temporary ring using their existing links. No
    // keys are copied and no storage is allocated during the sweep. Rejoin
    // all surviving candidates on rejection or exceptional exit.
    Entry* candidates = nullptr;
    auto restore_candidates = folly::makeGuard([&]() {
        if (candidates == nullptr) {
            return;
        }
        if (clock_hand_ == nullptr) {
            clock_hand_ = candidates;
            return;
        }
        auto* tail = clock_hand_->prev;
        auto* candidate_tail = candidates->prev;
        tail->next = candidates;
        candidates->prev = tail;
        candidate_tail->next = clock_hand_;
        clock_hand_->prev = candidate_tail;
    });
    // A new key's provisional map node is not yet a Clock member.
    const size_t entry_count =
        entries_.size() - (protected_entry->next == nullptr ? 1 : 0);
    // Give entries two second-chance passes, then select any unheld entry.
    const size_t second_chance_scan = entry_count * 2;
    const size_t max_scan = second_chance_scan + entry_count;
    bool can_fit = false;
    for (size_t i = 0; i < max_scan && clock_hand_ != nullptr; ++i) {
        auto& entry = *clock_hand_;
        clock_hand_ = entry.next;
        if (&entry == protected_entry || entry.payload.use_count() > 1) {
            continue;
        }
        auto count = entry.usage_count.load(std::memory_order_relaxed);
        if (count > 0 && i < second_chance_scan) {
            entry.usage_count.store(count - 1, std::memory_order_relaxed);
            continue;
        }
        UnlinkClockEntry(clock_hand_, &entry);
        LinkClockEntry(candidates, &entry);
        reclaimable += entry.payload->charge_.bytes;
        can_fit = budget_->CanFit(bytes, reclaimable);
        if (can_fit) {
            break;
        }
    }
    if (!can_fit && !budget_->CanFit(bytes, reclaimable)) {
        return false;
    }
    // Lookup needs the pool lock to acquire a reference, so staged unheld
    // payloads stay reclaimable until removal under this same exclusive lock.
    while (candidates != nullptr) {
        auto it = entries_.find(*candidates->key);
        LOG_DEBUG("EntryPool::EvictForPut segment_id={}, sig_hash={}",
                  it->first.segment_id,
                  it->first.sig_hash);
        UnlinkClockEntry(candidates, it->second.get());
        entries_.erase(it);
    }
    return true;
}

}  // namespace exec
}  // namespace milvus
