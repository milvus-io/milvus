// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License
#include "cachinglayer/lrucache/ListNode.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>

#include <fmt/core.h>
#include <folly/ExceptionWrapper.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "cachinglayer/lrucache/DList.h"
#include "cachinglayer/Utils.h"
#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus::cachinglayer::internal {

ListNode::NodePin::NodePin(ListNode* node) : node_(node) {
    // The pin_count_ is incremented in ListNode::pin() before this constructor is called.
}

ListNode::NodePin::~NodePin() {
    if (node_) {
        node_->unpin();
    }
}

ListNode::NodePin::NodePin(NodePin&& other) : NodePin(nullptr) {
    std::swap(node_, other.node_);
}

ListNode::NodePin&
ListNode::NodePin::operator=(NodePin&& other) {
    std::swap(node_, other.node_);
    return *this;
}

ListNode::ListNode(DList* dlist, ResourceUsage size)
    : last_touch_(dlist ? (std::chrono::high_resolution_clock::now() -
                           2 * dlist->eviction_config().cache_touch_window)
                        : std::chrono::high_resolution_clock::now()),
      dlist_(dlist),
      size_(size),
      state_(State::NOT_LOADED) {
}

ListNode::~ListNode() {
    if (dlist_) {
        std::unique_lock<std::shared_mutex> lock(mtx_);
        dlist_->removeItem(this, size_);
    }
}

bool
ListNode::manual_evict() {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    if (state_ == State::LOADING) {
        LOG_ERROR("[MCL] manual_evict() called on a {} cell {}",
                  state_to_string(state_),
                  key());
        return false;
    }
    if (state_ == State::NOT_LOADED) {
        return false;
    }
    if (pin_count_.load() > 0) {
        LOG_ERROR(
            "[MCL] manual_evict() called on a LOADED and pinned cell {}, "
            "aborting eviction.",
            key());
        return false;
    }
    // cell is LOADED
    clear_data();
    if (dlist_) {
        dlist_->removeItem(this, size_);
    }
    return true;
}

const ResourceUsage&
ListNode::size() const {
    return size_;
}

std::pair<bool, folly::SemiFuture<ListNode::NodePin>>
ListNode::pin() {
    // must be called with lock acquired, and state must not be NOT_LOADED.
    auto read_op = [this]() -> std::pair<bool, folly::SemiFuture<NodePin>> {
        AssertInfo(state_ != State::NOT_LOADED,
                   "Programming error: read_op called on a {} cell",
                   state_to_string(state_));
        // pin the cell now so that we can avoid taking the lock again in deferValue.
        if (pin_count_.fetch_add(1) == 0 && state_ == State::LOADED && dlist_) {
            // node became inevictable, decrease evictable size
            dlist_->decreaseEvictableSize(size_);
        }
        auto p = NodePin(this);
        if (state_ == State::LOADED) {
            internal::cache_op_result_count_hit(size_.storage_type())
                .Increment();
            return std::make_pair(false, std::move(p));
        }
        internal::cache_op_result_count_miss(size_.storage_type()).Increment();
        return std::make_pair(false,
                              load_promise_->getSemiFuture().deferValue(
                                  [this, p = std::move(p)](auto&&) mutable {
                                      return std::move(p);
                                  }));
    };
    {
        std::shared_lock<std::shared_mutex> lock(mtx_);
        if (state_ != State::NOT_LOADED) {
            return read_op();
        }
    }
    std::unique_lock<std::shared_mutex> lock(mtx_);
    if (state_ != State::NOT_LOADED) {
        return read_op();
    }
    // need to load.
    internal::cache_op_result_count_miss(size_.storage_type()).Increment();
    load_promise_ = std::make_unique<folly::SharedPromise<folly::Unit>>();
    state_ = State::LOADING;

    // pin the cell now so that we can avoid taking the lock again in deferValue.
    pin_count_.fetch_add(1);
    auto p = NodePin(this);
    return std::make_pair(
        true,
        load_promise_->getSemiFuture().deferValue(
            [this, p = std::move(p)](auto&&) mutable { return std::move(p); }));
}

void
ListNode::set_error(folly::exception_wrapper error) {
    std::unique_ptr<folly::SharedPromise<folly::Unit>> promise = nullptr;
    {
        std::unique_lock<std::shared_mutex> lock(mtx_);
        AssertInfo(state_ != State::NOT_LOADED,
                   "Programming error: set_error() called on a {} cell",
                   state_to_string(state_));
        // may be successfully loaded by another thread as a bonus, they will update used memory.
        if (state_ == State::LOADED) {
            return;
        }
        // else: state_ is LOADING, reset to NOT_LOADED
        state_ = State::NOT_LOADED;
        if (load_promise_) {
            promise = std::move(load_promise_);
        }
    }
    // Notify waiting threads about the error
    // setException may call continuation of bound futures inline, and those continuation may also need to acquire the
    // lock, which may cause deadlock. So we release the lock before calling setException.
    if (promise) {
        promise->setException(error);
    }
}

std::string
ListNode::state_to_string(State state) {
    switch (state) {
        case State::NOT_LOADED:
            return "NOT_LOADED";
        case State::LOADING:
            return "LOADING";
        case State::LOADED:
            return "LOADED";
    }
    throw std::invalid_argument("Invalid state");
}

void
ListNode::unpin() {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    if (pin_count_.fetch_sub(1) == 1) {
        touch(false);
        // Notify DList that this node became evictable
        if (dlist_ && state_ == State::LOADED) {
            dlist_->increaseEvictableSize(size_);
        }
    }
}

void
ListNode::touch(bool update_used_memory) {
    auto now = std::chrono::high_resolution_clock::now();
    if (dlist_ &&
        now - last_touch_ > dlist_->eviction_config().cache_touch_window) {
        std::optional<ResourceUsage> size = std::nullopt;
        if (update_used_memory) {
            size = size_;
        }
        last_touch_ = dlist_->touchItem(this, size);
    }
}

void
ListNode::clear_data() {
    // if the cell is evicted, loaded, pinned and unpinned within a single refresh window,
    // the cell should be inserted into the cache again.
    if (dlist_) {
        last_touch_ = std::chrono::high_resolution_clock::now() -
                      2 * dlist_->eviction_config().cache_touch_window;
    }
    unload();
    LOG_TRACE(
        "[MCL] ListNode evicted: key={}, size={}", key(), size_.ToString());
    state_ = State::NOT_LOADED;
}

void
ListNode::unload() {
    // Default implementation does nothing
}

void
ListNode::remove_self_from_loading_resource() {
    if (dlist_) {
        dlist_->removeLoadingResource(size_);
    }
}

}  // namespace milvus::cachinglayer::internal
