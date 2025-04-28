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
#pragma once

#include <atomic>
#include <mutex>

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "cachinglayer/lrucache/ListNode.h"

namespace milvus::cachinglayer::internal {

class DList {
 public:
    // Touch a node means to move it to the head of the list, which requires locking the entire list.
    // Use TouchConfig to reduce the frequency of touching and reduce contention.
    struct TouchConfig {
        std::chrono::seconds refresh_window = std::chrono::seconds(10);
    };

    DList(ResourceUsage max_memory, TouchConfig touch_config)
        : max_memory_(max_memory), touch_config_(touch_config) {
    }

    // If after evicting all unpinned items, the used_memory_ is still larger than new_limit, false will be returned
    // and no eviction will be done.
    // Will throw if new_limit is negative.
    bool
    UpdateLimit(const ResourceUsage& new_limit);

    // True if no nodes in the list.
    bool
    IsEmpty() const;

    // This method uses a global lock.
    bool
    reserveMemory(const ResourceUsage& size);

    // Used only when load failed. This will only cause used_memory_ to decrease, which will not affect the correctness
    // of concurrent reserveMemory() even without lock.
    void
    releaseMemory(const ResourceUsage& size);

    // Caller must guarantee that the current thread holds the lock of list_node->mtx_.
    // touchItem is used in 2 places:
    // 1. when a loaded cell is pinned/unpinned, we need to touch it to refresh the LRU order.
    //    we don't update used_memory_ here.
    // 2. when a cell is loaded as a bonus, we need to touch it to insert into the LRU and update
    //    used_memory_ to track the memory usage(usage of such cell is not counted during reservation).
    void
    touchItem(ListNode* list_node,
              std::optional<ResourceUsage> size = std::nullopt);

    // Caller must guarantee that the current thread holds the lock of list_node->mtx_.
    // Removes the node from the list and updates used_memory_.
    void
    removeItem(ListNode* list_node, ResourceUsage size);

    const TouchConfig&
    touch_config() const {
        return touch_config_;
    }

 private:
    friend class DListTestFriend;

    // Try to evict some items so that the evicted items are larger than expected_eviction.
    // If we cannot achieve the goal, nothing will be evicted and false will be returned.
    // Must be called under the lock of list_mtx_.
    bool
    tryEvict(const ResourceUsage& expected_eviction);

    // Must be called under the lock of list_mtx_ and list_node->mtx_.
    // ListNode is guaranteed to be not in the list.
    void
    pushHead(ListNode* list_node);

    // Must be called under the lock of list_mtx_ and list_node->mtx_.
    // If ListNode is not in the list, this function does nothing.
    // Returns true if ListNode is in the list and popped, false otherwise.
    bool
    popItem(ListNode* list_node);

    // head_ is the most recently used item, tail_ is the least recently used item.
    // tail_ -> next -> ... -> head_
    // tail_ <- prev <- ... <- head_
    ListNode* head_ = nullptr;
    ListNode* tail_ = nullptr;

    // TODO(tiered storage 3): benchmark folly::DistributedMutex for this usecase.
    mutable std::mutex list_mtx_;
    // access to used_memory_ and max_memory_ must be done under the lock of list_mtx_
    std::atomic<ResourceUsage> used_memory_{};
    ResourceUsage max_memory_;
    const TouchConfig touch_config_;
};

}  // namespace milvus::cachinglayer::internal
