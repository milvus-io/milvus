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
#include <condition_variable>
#include <mutex>
#include <chrono>
#include <queue>
#include <unordered_map>

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <folly/io/async/EventBase.h>
#include <folly/system/ThreadName.h>

#include "cachinglayer/lrucache/ListNode.h"
#include "cachinglayer/Utils.h"
#include "log/Log.h"

namespace milvus::cachinglayer::internal {

class DList {
 public:
    DList(ResourceUsage max_memory,
          ResourceUsage low_watermark,
          ResourceUsage high_watermark,
          EvictionConfig eviction_config)
        : max_memory_(max_memory),
          low_watermark_(low_watermark),
          high_watermark_(high_watermark),
          eviction_config_(eviction_config),
          next_request_id_(1) {
        AssertInfo(low_watermark.AllGEZero(),
                   "[MCL] low watermark must be greater than or equal to 0");
        AssertInfo((high_watermark - low_watermark).AllGEZero(),
                   "[MCL] high watermark must be greater than low watermark");
        AssertInfo((max_memory - high_watermark).AllGEZero(),
                   "[MCL] max memory must be greater than high watermark");

        // Initialize event base and thread
        event_base_ = std::make_unique<folly::EventBase>();
        event_base_thread_ = std::make_unique<std::thread>([this] {
            LOG_INFO("[MCL] Starting cache EventBase thread");
            folly::setThreadName("cache-eb");
            event_base_->loopForever();
        });

        eviction_thread_ = std::thread(&DList::evictionLoop, this);
    }

    ~DList() {
        // Stop event base first
        if (event_base_) {
            event_base_->terminateLoopSoon();
        }
        if (event_base_thread_ && event_base_thread_->joinable()) {
            event_base_thread_->join();
        }

        // Stop eviction loop
        stop_eviction_loop_ = true;
        eviction_thread_cv_.notify_all();
        if (eviction_thread_.joinable()) {
            eviction_thread_.join();
        }
        clearWaitingQueue();
    }

    // If after evicting all unpinned items, the used_resources_ is still larger than new_limit, false will be returned
    // and no eviction will be done.
    // Will throw if new_limit is negative.
    bool
    UpdateLimit(const ResourceUsage& new_limit);

    // Update low/high watermark does not trigger eviction, thus will not fail.
    void
    UpdateLowWatermark(const ResourceUsage& new_low_watermark);

    void
    UpdateHighWatermark(const ResourceUsage& new_high_watermark);

    // True if no nodes in the list.
    bool
    IsEmpty() const;

    folly::SemiFuture<bool>
    reserveMemoryWithTimeout(const ResourceUsage& size,
                             std::chrono::milliseconds timeout);

    // Called when a node becomes evictable (pin count drops to 0), or when a node is loaded as a bonus.
    void
    increaseEvictableSize(const ResourceUsage& size);

    // Called when a node is pinned(pin count increases from 0), or when a node is removed(evicted or released).
    void
    decreaseEvictableSize(const ResourceUsage& size);

    // Used only when load failed. This will only cause used_resources_ to decrease, which will not affect the correctness
    // of concurrent reserveMemoryWithTimeout() even without lock.
    void
    releaseMemory(const ResourceUsage& size);

    // Caller must guarantee that the current thread holds the lock of list_node->mtx_.
    // touchItem is used in 2 places:
    // 1. when a loaded cell is pinned/unpinned, we need to touch it to refresh the LRU order.
    //    we don't update used_resources_ here.
    // 2. when a cell is loaded as a bonus, we need to touch it to insert into the LRU and update
    //    used_resources_ to track the memory usage(usage of such cell is not counted during reservation).
    //
    // Returns the time point when the item was last touched. This methods always acquires the
    // global list_mtx_, thus the returned time point is guaranteed to be monotonically increasing.
    std::chrono::high_resolution_clock::time_point
    touchItem(ListNode* list_node,
              std::optional<ResourceUsage> size = std::nullopt);

    // Caller must guarantee that the current thread holds the lock of list_node->mtx_.
    // Removes the node from the list and updates used_resources_.
    void
    removeItem(ListNode* list_node, ResourceUsage size);

    void
    removeLoadingResource(const ResourceUsage& size);

    const EvictionConfig&
    eviction_config() const {
        return eviction_config_;
    }

 private:
    friend class DListTestFriend;

    // Waiting request for timeout-based memory reservation
    struct WaitingRequest {
        ResourceUsage required_size;
        std::chrono::steady_clock::time_point deadline;
        folly::Promise<bool> promise;
        folly::EventBase* event_base;
        uint64_t request_id;

        WaitingRequest(ResourceUsage size,
                       std::chrono::steady_clock::time_point dl,
                       folly::Promise<bool> p,
                       folly::EventBase* eb,
                       uint64_t id)
            : required_size(size),
              deadline(dl),
              promise(std::move(p)),
              event_base(eb),
              request_id(id) {
        }
    };

    // Comparator for priority queue (smaller size and earlier deadline have higher priority)
    struct WaitingRequestComparator {
        bool
        operator()(const std::unique_ptr<WaitingRequest>& a,
                   const std::unique_ptr<WaitingRequest>& b) {
            // First priority: deadline (earlier deadline has higher priority)
            if (a->deadline != b->deadline) {
                return a->deadline > b->deadline;
            }
            // Second priority: resource size (smaller size has higher priority)
            int64_t total_a =
                a->required_size.memory_bytes + a->required_size.file_bytes;
            int64_t total_b =
                b->required_size.memory_bytes + b->required_size.file_bytes;
            return total_a > total_b;
        }
    };

    // reserveMemory without taking lock, must be called with lock held.
    bool
    reserveMemoryInternal(const ResourceUsage& size);

    void
    evictionLoop();

    // Try to evict some items so that the resources of evicted items are larger than expected_eviction.
    // If we cannot achieve the goal, but we can evict min_eviction, we will still perform eviction.
    // If we cannot even evict min_eviction, nothing will be evicted and false will be returned.
    // Must be called under the lock of list_mtx_.
    // Returns the logical amount of resources that are evicted. 0 means no eviction happened.
    ResourceUsage
    tryEvict(const ResourceUsage& expected_eviction,
             const ResourceUsage& min_eviction,
             const bool evict_expired_items = false);

    // Notify waiting requests when resources are available.
    // This method should be called with list_mtx_ already held.
    void
    notifyWaitingRequests();

    // Clear all waiting requests (used in destructor)
    void
    clearWaitingQueue();

    // Must be called under the lock of list_mtx_ and list_node->mtx_.
    // ListNode is guaranteed to be not in the list.
    void
    pushHead(ListNode* list_node);

    // Must be called under the lock of list_mtx_ and list_node->mtx_.
    // If ListNode is not in the list, this function does nothing.
    // Returns true if ListNode is in the list and popped, false otherwise.
    bool
    popItem(ListNode* list_node);

    std::string
    usageInfo() const;

    // Physical resource protection methods
    // Returns the amount of memory/disk that needs to be evicted to satisfy physical resource limit
    // Returns 0 if no eviction needed, positive value if eviction needed.
    // For disk, it only checks whether the usage will exceed the disk capacity to avoid using up all disk space.
    // Does not obey the configured disk capacity limit. Reason is: we can't easily determine the amount of disk space
    // that is used by the cache(there may be other processes using the disk).
    ResourceUsage
    checkPhysicalResourceLimit(const ResourceUsage& size) const;

    // not thread safe, use for debug only
    std::string
    chainString() const;

    // head_ is the most recently used item, tail_ is the least recently used item.
    // tail_ -> next -> ... -> head_
    // tail_ <- prev <- ... <- head_
    ListNode* head_ = nullptr;
    ListNode* tail_ = nullptr;

    // TODO(tiered storage 3): benchmark folly::DistributedMutex for this usecase.
    mutable std::mutex list_mtx_;
    // access to used_resources_ and max_memory_ must be done under the lock of list_mtx_
    std::atomic<ResourceUsage> used_resources_{};
    // Track estimated resources currently being loaded
    std::atomic<ResourceUsage> loading_{};
    ResourceUsage low_watermark_;
    ResourceUsage high_watermark_;
    ResourceUsage max_memory_;
    const EvictionConfig eviction_config_;

    std::thread eviction_thread_;
    std::condition_variable eviction_thread_cv_;
    std::atomic<bool> stop_eviction_loop_{false};

    // Waiting queue for timeout-based memory reservation
    std::priority_queue<std::unique_ptr<WaitingRequest>,
                        std::vector<std::unique_ptr<WaitingRequest>>,
                        WaitingRequestComparator>
        waiting_queue_;
    // using a separate variable to avoid locking just to check if the queue is empty.
    std::atomic<bool> waiting_queue_empty_{true};

    // Quick lookup map for waiting requests (for timeout handling)
    std::unordered_map<uint64_t, WaitingRequest*> waiting_requests_map_;

    // Counter for generating unique request IDs
    std::atomic<uint64_t> next_request_id_;

    // Total size of nodes that are loaded and unpinned
    std::atomic<ResourceUsage> evictable_size_{};

    // EventBase and thread for handling timeout operations
    std::unique_ptr<folly::EventBase> event_base_;
    std::unique_ptr<std::thread> event_base_thread_;
};

}  // namespace milvus::cachinglayer::internal
