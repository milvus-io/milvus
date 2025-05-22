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

#include <any>
#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include <type_traits>
#include <vector>

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <folly/Synchronized.h>

#include "cachinglayer/lrucache/DList.h"
#include "cachinglayer/lrucache/ListNode.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/EasyAssert.h"
#include "common/type_c.h"
#include "log/Log.h"
#include "monitor/prometheus_client.h"
#include "storage/ThreadPools.h"

namespace milvus::cachinglayer {

template <typename CellT>
class CellAccessor;

// - The action of pinning cells is not started until the returned SemiFuture is scheduled on an executor.
// - Once the future is scheduled, CacheSlot must live until the future is ready.
// - The returned CellAccessor stores a shared_ptr of CacheSlot, thus will keep CacheSlot alive.
template <typename CellT>
class CacheSlot final : public std::enable_shared_from_this<CacheSlot<CellT>> {
 public:
    // TODO(tiered storage 1): the CellT should return its actual usage, once loaded. And we use this to report metrics.
    static_assert(
        std::is_same_v<size_t, decltype(std::declval<CellT>().CellByteSize())>,
        "CellT must have a CellByteSize() method that returns a size_t "
        "representing the memory consumption of the cell");

    CacheSlot(std::unique_ptr<Translator<CellT>> translator,
              internal::DList* dlist)
        : translator_(std::move(translator)),
          cells_(translator_->num_cells()),
          dlist_(dlist) {
        for (cid_t i = 0; i < translator_->num_cells(); ++i) {
            new (&cells_[i])
                CacheCell(this, i, translator_->estimated_byte_size_of_cell(i));
        }
        internal::cache_slot_count(translator_->meta()->storage_type)
            .Increment();
        internal::cache_cell_count(translator_->meta()->storage_type)
            .Increment(translator_->num_cells());
        internal::cache_memory_overhead_bytes(translator_->meta()->storage_type)
            .Increment(memory_overhead());
    }

    CacheSlot(const CacheSlot&) = delete;
    CacheSlot&
    operator=(const CacheSlot&) = delete;
    CacheSlot(CacheSlot&&) = delete;
    CacheSlot&
    operator=(CacheSlot&&) = delete;

    void
    Warmup() {
        auto warmup_policy = translator_->meta()->cache_warmup_policy;

        if (warmup_policy == CacheWarmupPolicy::CacheWarmupPolicy_Disable) {
            return;
        }

        std::vector<cid_t> cids;
        cids.reserve(translator_->num_cells());
        for (cid_t i = 0; i < translator_->num_cells(); ++i) {
            cids.push_back(i);
        }

        switch (warmup_policy) {
            case CacheWarmupPolicy::CacheWarmupPolicy_Sync:
                SemiInlineGet(PinCells(std::move(cids)));
                break;
            case CacheWarmupPolicy::CacheWarmupPolicy_Async:
                // PinCells submits tasks to middle priority thread pool, thus here we submit to
                // low priority thread pool to avoid dead lock.
                auto& pool = milvus::ThreadPools::GetThreadPool(
                    milvus::ThreadPoolPriority::LOW);
                pool.Submit([this, cids = std::move(cids)]() mutable {
                    SemiInlineGet(PinCells(std::move(cids)));
                });
                break;
        }
    }

    folly::SemiFuture<std::shared_ptr<CellAccessor<CellT>>>
    PinAllCells() {
        return folly::makeSemiFuture().deferValue([this](auto&&) {
            size_t index = 0;
            return PinInternal(
                [this, index]() mutable -> std::pair<cid_t, bool> {
                    if (index >= cells_.size()) {
                        return std::make_pair(cells_.size(), true);
                    }
                    return std::make_pair(index++, false);
                },
                cells_.size());
        });
    }

    folly::SemiFuture<std::shared_ptr<CellAccessor<CellT>>>
    PinCells(std::vector<uid_t> uids) {
        return folly::makeSemiFuture().deferValue([this,
                                                   uids = std::vector<uid_t>(
                                                       uids)](auto&&) {
            auto count = uids.size();
            std::unordered_set<cid_t> involved_cids;
            involved_cids.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                auto uid = uids[i];
                auto cid = translator_->cell_id_of(uid);
                if (cid >= cells_.size()) {
                    return folly::makeSemiFuture<
                        std::shared_ptr<CellAccessor<CellT>>>(
                        folly::make_exception_wrapper<std::invalid_argument>(
                            fmt::format(
                                "CacheSlot {}: translator returned cell_id {} "
                                "for uid {} which is out of range",
                                translator_->key(),
                                cid,
                                uid)));
                }
                involved_cids.insert(cid);
            }
            auto reserve_size = involved_cids.size();

            // must be captured by value.
            // theoretically, we can initialize it outside, and it will not be invalidated
            // even though we moved involved_cids afterwards, but for safety we initialize it
            // inside the lambda.
            decltype(involved_cids.begin()) it;
            bool initialized = false;

            return PinInternal(
                [this,
                 cids = std::move(involved_cids),
                 it,
                 initialized]() mutable -> std::pair<cid_t, bool> {
                    if (!initialized) {
                        it = cids.begin();
                        initialized = true;
                    }
                    if (it == cids.end()) {
                        return std::make_pair(0, true);
                    }
                    auto cid = *it++;
                    return std::make_pair(cid, false);
                },
                reserve_size);
        });
    }

    // Manually evicts the cell if it is not pinned.
    // Returns true if the cell ends up in a state other than LOADED.
    bool
    ManualEvict(cid_t cid) {
        return cells_[cid].manual_evict();
    }

    // Returns true if any cell is evicted.
    bool
    ManualEvictAll() {
        bool evicted = false;
        for (cid_t cid = 0; cid < cells_.size(); ++cid) {
            if (cells_[cid].manual_evict()) {
                evicted = true;
            }
        }
        return evicted;
    }

    size_t
    num_cells() const {
        return translator_->num_cells();
    }

    ResourceUsage
    size_of_cell(cid_t cid) const {
        return translator_->estimated_byte_size_of_cell(cid);
    }

    Meta*
    meta() {
        return translator_->meta();
    }

    ~CacheSlot() {
        internal::cache_slot_count(translator_->meta()->storage_type)
            .Decrement();
        internal::cache_cell_count(translator_->meta()->storage_type)
            .Decrement(translator_->num_cells());
        internal::cache_memory_overhead_bytes(translator_->meta()->storage_type)
            .Decrement(memory_overhead());
    }

 private:
    friend class CellAccessor<CellT>;

    template <typename Fn>
    folly::SemiFuture<std::shared_ptr<CellAccessor<CellT>>>
    PinInternal(Fn&& cid_iterator, size_t reserve_size) {
        std::vector<folly::SemiFuture<internal::ListNode::NodePin>> futures;
        std::unordered_set<cid_t> need_load_cids;
        futures.reserve(reserve_size);
        need_load_cids.reserve(reserve_size);
        auto [cid, end] = cid_iterator();
        while (!end) {
            auto [need_load, future] = cells_[cid].pin();
            futures.push_back(std::move(future));
            if (need_load) {
                need_load_cids.insert(cid);
            }
            std::tie(cid, end) = cid_iterator();
        }
        auto load_future = folly::makeSemiFuture();
        if (!need_load_cids.empty()) {
            load_future = RunLoad(std::move(need_load_cids));
        }
        return std::move(load_future)
            .deferValue(
                [this, futures = std::move(futures)](auto&&) mutable
                -> folly::SemiFuture<std::shared_ptr<CellAccessor<CellT>>> {
                    return folly::collect(futures).deferValue(
                        [this](std::vector<internal::ListNode::NodePin>&&
                                   pins) mutable
                        -> std::shared_ptr<CellAccessor<CellT>> {
                            return std::make_shared<CellAccessor<CellT>>(
                                this->shared_from_this(), std::move(pins));
                        });
                });
    }

    cid_t
    cell_id_of(uid_t uid) const {
        return translator_->cell_id_of(uid);
    }

    folly::SemiFuture<folly::Unit>
    RunLoad(std::unordered_set<cid_t>&& cids) {
        return folly::makeSemiFuture().deferValue(
            [this,
             cids = std::move(cids)](auto&&) -> folly::SemiFuture<folly::Unit> {
                try {
                    auto start = std::chrono::high_resolution_clock::now();
                    std::vector<cid_t> cids_vec(cids.begin(), cids.end());
                    auto results = translator_->get_cells(cids_vec);
                    auto latency =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::high_resolution_clock::now() - start);
                    for (auto& result : results) {
                        cells_[result.first].set_cell(
                            std::move(result.second),
                            cids.count(result.first) > 0);
                        internal::cache_load_latency(
                            translator_->meta()->storage_type)
                            .Observe(latency.count());
                    }
                    internal::cache_cell_loaded_count(
                        translator_->meta()->storage_type)
                        .Increment(results.size());
                    internal::cache_load_count_success(
                        translator_->meta()->storage_type)
                        .Increment(results.size());
                } catch (...) {
                    auto exception = std::current_exception();
                    auto ew = folly::exception_wrapper(exception);
                    internal::cache_load_count_fail(
                        translator_->meta()->storage_type)
                        .Increment(cids.size());
                    for (auto cid : cids) {
                        cells_[cid].set_error(ew);
                    }
                    return folly::makeSemiFuture<folly::Unit>(ew);
                }
                return folly::Unit();
            });
    }

    struct CacheCell : internal::ListNode {
     public:
        CacheCell() = default;
        CacheCell(CacheSlot<CellT>* slot, cid_t cid, ResourceUsage size)
            : internal::ListNode(slot->dlist_, size), slot_(slot), cid_(cid) {
        }
        ~CacheCell() {
            if (state_ == State::LOADING) {
                LOG_ERROR("CacheSlot Cell {} destroyed while loading", key());
            }
        }

        CellT*
        cell() {
            return cell_.get();
        }

        // Be careful that even though only a single thread can request loading a cell,
        // it is still possible that multiple threads call set_cell() concurrently.
        // For example, 2 RunLoad() calls tries to download cell 4 and 6, and both decided
        // to also download cell 5, if they finished at the same time, they will call set_cell()
        // of cell 5 concurrently.
        void
        set_cell(std::unique_ptr<CellT> cell, bool requesting_thread) {
            mark_loaded(
                [this, cell = std::move(cell)]() mutable {
                    cell_ = std::move(cell);
                    life_start_ = std::chrono::steady_clock::now();
                    milvus::monitor::internal_cache_used_bytes_memory.Increment(
                        size_.memory_bytes);
                    milvus::monitor::internal_cache_used_bytes_disk.Increment(
                        size_.file_bytes);
                },
                requesting_thread);
        }

        void
        set_error(folly::exception_wrapper error) {
            internal::ListNode::set_error(std::move(error));
        }

     protected:
        void
        unload() override {
            if (cell_) {
                internal::cache_cell_loaded_count(
                    slot_->translator_->meta()->storage_type)
                    .Decrement();
                auto life_time = std::chrono::steady_clock::now() - life_start_;
                auto seconds =
                    std::chrono::duration_cast<std::chrono::seconds>(life_time)
                        .count();
                internal::cache_item_lifetime_seconds(
                    slot_->translator_->meta()->storage_type)
                    .Observe(seconds);
                cell_ = nullptr;
                milvus::monitor::internal_cache_used_bytes_memory.Decrement(
                    size_.memory_bytes);
                milvus::monitor::internal_cache_used_bytes_disk.Decrement(
                    size_.file_bytes);
            }
        }
        std::string
        key() const override {
            return fmt::format("{}:{}", slot_->translator_->key(), cid_);
        }

     private:
        CacheSlot<CellT>* slot_{nullptr};
        cid_t cid_{0};
        std::unique_ptr<CellT> cell_{nullptr};
        std::chrono::steady_clock::time_point life_start_{};
    };

    size_t
    memory_overhead() const {
        return sizeof(*this) + cells_.size() * sizeof(CacheCell);
    }

    const std::unique_ptr<Translator<CellT>> translator_;
    // Each CacheCell's cid_t is its index in vector
    // Once initialized, cells_ should never be resized.
    std::vector<CacheCell> cells_;
    internal::DList* dlist_;
};

// - A thin wrapper for accessing cells in a CacheSlot.
// - When this class is created, the cells are loaded and pinned.
// - Accessing cells through this class does not incur any lock overhead.
// - Accessing cells that are not pinned by this CellAccessor is undefined behavior.
template <typename CellT>
class CellAccessor {
 public:
    CellAccessor(std::shared_ptr<CacheSlot<CellT>> slot,
                 std::vector<internal::ListNode::NodePin> pins)
        : slot_(std::move(slot)), pins_(std::move(pins)) {
    }

    CellT*
    get_cell_of(uid_t uid) {
        auto cid = slot_->cell_id_of(uid);
        return slot_->cells_[cid].cell();
    }

    CellT*
    get_ith_cell(cid_t cid) {
        return slot_->cells_[cid].cell();
    }

 private:
    // pins must be destroyed before slot_ is destroyed, thus
    // pins_ should be a member after slot_.
    std::shared_ptr<CacheSlot<CellT>> slot_;
    std::vector<internal::ListNode::NodePin> pins_;
};

// TODO(tiered storage 4): this class is a temp solution. Later we should modify all usage of this class
// to use folly::SemiFuture instead: all data access should happen within deferValue().
// Current impl requires the T type to be movable/copyable.
template <typename T>
class PinWrapper {
 public:
    PinWrapper() = default;
    PinWrapper(std::any raii, T&& content)
        : raii_(std::move(raii)), content_(std::move(content)) {
    }

    PinWrapper(std::any raii, const T& content)
        : raii_(std::move(raii)), content_(content) {
    }

    // For those that does not need a pin. eg: growing segment, views that actually copies the data, etc.
    PinWrapper(T&& content) : raii_(nullptr), content_(std::move(content)) {
    }
    PinWrapper(const T& content) : raii_(nullptr), content_(content) {
    }

    PinWrapper(PinWrapper&& other) noexcept
        : raii_(std::move(other.raii_)), content_(std::move(other.content_)) {
    }

    PinWrapper(const PinWrapper& other)
        : raii_(other.raii_), content_(other.content_) {
    }

    PinWrapper&
    operator=(PinWrapper&& other) noexcept {
        if (this != &other) {
            std::swap(raii_, other.raii_);
            std::swap(content_, other.content_);
        }
        return *this;
    }

    PinWrapper&
    operator=(const PinWrapper& other) {
        if (this != &other) {
            raii_ = other.raii_;
            content_ = other.content_;
        }
        return *this;
    }

    T&
    get() {
        return content_;
    }

    const T&
    get() const {
        return content_;
    }

    template <typename T2, typename Fn>
    PinWrapper<T2>
    transform(Fn&& transformer) && {
        T2 transformed = transformer(std::move(content_));
        return PinWrapper<T2>(std::move(raii_), std::move(transformed));
    }

 private:
    // CellAccessor is templated on CellT, we don't want to enforce that in this class.
    std::any raii_{nullptr};
    T content_;
};

}  // namespace milvus::cachinglayer
