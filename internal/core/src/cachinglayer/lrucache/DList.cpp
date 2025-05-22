// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License
#include "cachinglayer/lrucache/DList.h"

#include <mutex>
#include <vector>

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "cachinglayer/Utils.h"
#include "cachinglayer/lrucache/ListNode.h"
#include "monitor/prometheus_client.h"
#include "log/Log.h"

namespace milvus::cachinglayer::internal {

bool
DList::reserveMemory(const ResourceUsage& size) {
    std::unique_lock<std::mutex> list_lock(list_mtx_);
    auto used = used_memory_.load();
    if (max_memory_.CanHold(used + size)) {
        used_memory_ += size;
        return true;
    }

    // try to evict so that used + size <= low watermark, but if that is not possible,
    // evict enough for the current reservation.
    if (tryEvict(used + size - low_watermark_, used + size - max_memory_)) {
        used_memory_ += size;
        return true;
    }
    return false;
}

void
DList::evictionLoop() {
    while (true) {
        std::unique_lock<std::mutex> lock(list_mtx_);
        if (eviction_thread_cv_.wait_for(
                lock, eviction_config_.eviction_interval, [this] {
                    return stop_eviction_loop_.load();
                })) {
            break;
        }
        auto used = used_memory_.load();
        // if usage is above high watermark, evict until low watermark is reached.
        if (used.memory_bytes >= high_watermark_.memory_bytes ||
            used.file_bytes >= high_watermark_.file_bytes) {
            tryEvict(
                {
                    used.memory_bytes >= high_watermark_.memory_bytes
                        ? used.memory_bytes - low_watermark_.memory_bytes
                        : 0,
                    used.file_bytes >= high_watermark_.file_bytes
                        ? used.file_bytes - low_watermark_.file_bytes
                        : 0,
                },
                // in eviction loop, we always evict as much as possible until low watermark.
                {0, 0});
        }
    }
}

std::string
DList::usageInfo(const ResourceUsage& actively_pinned) const {
    auto used = used_memory_.load();
    static double precision = 100.0;
    return fmt::format(
        "low_watermark_: {}, "
        "high_watermark_: {} , "
        "max_memory_: {} , "
        "used_memory_: {} {:.2}% of max, {:.2}% of "
        "high_watermark memory, {:.2}% of max, {:.2}% of "
        "high_watermark disk, "
        "actively_pinned: {} {:.2}% of used memory, {:.2}% of used disk",
        low_watermark_.ToString(),
        high_watermark_.ToString(),
        max_memory_.ToString(),
        used.ToString(),
        static_cast<double>(used.memory_bytes) / max_memory_.memory_bytes *
            precision,
        static_cast<double>(used.memory_bytes) / high_watermark_.memory_bytes *
            precision,
        static_cast<double>(used.file_bytes) / max_memory_.file_bytes *
            precision,
        static_cast<double>(used.file_bytes) / high_watermark_.file_bytes *
            precision,
        actively_pinned.ToString(),
        static_cast<double>(actively_pinned.memory_bytes) / used.memory_bytes *
            precision,
        static_cast<double>(actively_pinned.file_bytes) / used.file_bytes *
            precision);
}

bool
DList::tryEvict(const ResourceUsage& expected_eviction,
                const ResourceUsage& min_eviction) {
    std::vector<ListNode*> to_evict;
    // items are evicted because they are not used for a while, thus it should be ok to lock them
    // a little bit longer.
    std::vector<std::unique_lock<std::shared_mutex>> item_locks;

    ResourceUsage size_to_evict;

    auto would_help = [&](const ResourceUsage& size) -> bool {
        auto need_memory =
            size_to_evict.memory_bytes < expected_eviction.memory_bytes;
        auto need_disk =
            size_to_evict.file_bytes < expected_eviction.file_bytes;
        return (need_memory && size.memory_bytes > 0) ||
               (need_disk && size.file_bytes > 0);
    };

    ResourceUsage actively_pinned{0, 0};

    // accumulate victims using expected_eviction.
    for (auto it = tail_; it != nullptr; it = it->next_) {
        if (!would_help(it->size())) {
            continue;
        }
        // use try_to_lock to avoid dead lock by failing immediately if the ListNode lock is already held.
        auto& lock = item_locks.emplace_back(it->mtx_, std::try_to_lock);
        // if lock failed, it means this ListNode will be used again, so we don't evict it anymore.
        if (lock.owns_lock() && it->pin_count_ == 0) {
            to_evict.push_back(it);
            size_to_evict += it->size();
            if (size_to_evict.CanHold(expected_eviction)) {
                break;
            }
        } else {
            // if we grabbed the lock only to find that the ListNode is pinned; or if we failed to lock
            // the ListNode, we do not evict this ListNode.
            item_locks.pop_back();
            actively_pinned += it->size();
        }
    }
    if (!size_to_evict.CanHold(expected_eviction)) {
        if (!size_to_evict.CanHold(min_eviction)) {
            LOG_WARN(
                "Milvus Caching Layer: cannot evict even min_eviction {}, "
                "giving up eviction. Current usage: {}",
                min_eviction.ToString(),
                usageInfo(actively_pinned));
            return false;
        }
        LOG_INFO(
            "Milvus Caching Layer: cannot evict expected_eviction {}, "
            "evicting as much({}) as possible. Current usage: {}",
            expected_eviction.ToString(),
            size_to_evict.ToString(),
            usageInfo(actively_pinned));
    }

    internal::cache_eviction_event_count().Increment();
    for (auto* list_node : to_evict) {
        auto size = list_node->size();
        internal::cache_cell_eviction_count(size.storage_type()).Increment();
        popItem(list_node);
        list_node->clear_data();
        used_memory_ -= size;
    }

    switch (size_to_evict.storage_type()) {
        case StorageType::MEMORY:
            milvus::monitor::internal_cache_evicted_bytes_memory.Increment(
                size_to_evict.memory_bytes);
            break;
        case StorageType::DISK:
            milvus::monitor::internal_cache_evicted_bytes_disk.Increment(
                size_to_evict.file_bytes);
            break;
        case StorageType::MIXED:
            milvus::monitor::internal_cache_evicted_bytes_memory.Increment(
                size_to_evict.memory_bytes);
            milvus::monitor::internal_cache_evicted_bytes_disk.Increment(
                size_to_evict.file_bytes);
            break;
        default:
            PanicInfo(ErrorCode::UnexpectedError, "Unknown StorageType");
    }
    return true;
}

bool
DList::UpdateLimit(const ResourceUsage& new_limit) {
    AssertInfo(new_limit.GEZero(),
               "Milvus Caching Layer: memory and disk usage limit must be "
               "greater than 0");
    std::unique_lock<std::mutex> list_lock(list_mtx_);
    auto used = used_memory_.load();
    if (!new_limit.CanHold(used)) {
        // positive means amount owed
        auto deficit = used - new_limit;
        // deficit is the hard limit of eviction, if we cannot evict deficit, we give
        // up the limit change.
        if (!tryEvict(deficit, deficit)) {
            return false;
        }
    }
    max_memory_ = new_limit;
    milvus::monitor::internal_cache_capacity_bytes_memory.Set(
        max_memory_.memory_bytes);
    milvus::monitor::internal_cache_capacity_bytes_disk.Set(
        max_memory_.file_bytes);
    return true;
}

void
DList::UpdateLowWatermark(const ResourceUsage& new_low_watermark) {
    std::unique_lock<std::mutex> list_lock(list_mtx_);
    AssertInfo(new_low_watermark.GEZero(),
               "Milvus Caching Layer: low watermark must be greater than 0");
    low_watermark_ = new_low_watermark;
}

void
DList::UpdateHighWatermark(const ResourceUsage& new_high_watermark) {
    std::unique_lock<std::mutex> list_lock(list_mtx_);
    AssertInfo(new_high_watermark.GEZero(),
               "Milvus Caching Layer: high watermark must be greater than 0");
    high_watermark_ = new_high_watermark;
}

void
DList::releaseMemory(const ResourceUsage& size) {
    // safe to substract on atomic without lock
    used_memory_ -= size;
}

void
DList::touchItem(ListNode* list_node, std::optional<ResourceUsage> size) {
    std::lock_guard<std::mutex> list_lock(list_mtx_);
    popItem(list_node);
    pushHead(list_node);
    if (size.has_value()) {
        used_memory_ += size.value();
    }
}

void
DList::removeItem(ListNode* list_node, ResourceUsage size) {
    std::lock_guard<std::mutex> list_lock(list_mtx_);
    if (popItem(list_node)) {
        used_memory_ -= size;
    }
}

void
DList::pushHead(ListNode* list_node) {
    if (head_ == nullptr) {
        head_ = list_node;
        tail_ = list_node;
    } else {
        list_node->prev_ = head_;
        head_->next_ = list_node;
        head_ = list_node;
    }
}

bool
DList::popItem(ListNode* list_node) {
    if (list_node->prev_ == nullptr && list_node->next_ == nullptr &&
        list_node != head_) {
        // list_node is not in the list
        return false;
    }
    if (head_ == tail_) {
        head_ = tail_ = nullptr;
        list_node->prev_ = list_node->next_ = nullptr;
    } else if (head_ == list_node) {
        head_ = list_node->prev_;
        head_->next_ = nullptr;
        list_node->prev_ = nullptr;
    } else if (tail_ == list_node) {
        tail_ = list_node->next_;
        tail_->prev_ = nullptr;
        list_node->next_ = nullptr;
    } else {
        list_node->prev_->next_ = list_node->next_;
        list_node->next_->prev_ = list_node->prev_;
        list_node->prev_ = list_node->next_ = nullptr;
    }
    return true;
}

bool
DList::IsEmpty() const {
    std::lock_guard<std::mutex> list_lock(list_mtx_);
    return head_ == nullptr;
}

}  // namespace milvus::cachinglayer::internal
