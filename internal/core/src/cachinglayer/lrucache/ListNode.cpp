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
    if (node_) {
        node_->pin_count_++;
    }
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
    if (state_ == State::ERROR || state_ == State::LOADING) {
        LOG_ERROR("manual_evict() called on a {} cell",
                  state_to_string(state_));
        return true;
    }
    if (state_ == State::NOT_LOADED) {
        return true;
    }
    if (pin_count_.load() > 0) {
        LOG_ERROR(
            "manual_evict() called on a LOADED and pinned cell, aborting "
            "eviction.");
        return false;
    }
    // cell is LOADED
    clear_data();
    if (dlist_) {
        dlist_->removeItem(this, size_);
    }
    return true;
}

ResourceUsage&
ListNode::size() {
    return size_;
}

std::pair<bool, folly::SemiFuture<ListNode::NodePin>>
ListNode::pin() {
    // must be called with lock acquired, and state must not be NOT_LOADED.
    auto read_op = [this]() -> std::pair<bool, folly::SemiFuture<NodePin>> {
        AssertInfo(state_ != State::NOT_LOADED,
                   "Programming error: read_op called on a {} cell",
                   state_to_string(state_));
        if (state_ == State::ERROR) {
            return std::make_pair(false,
                                  folly::makeSemiFuture<NodePin>(error_));
        }
        // pin the cell now so that we can avoid taking the lock again in deferValue.
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
    if (dlist_ && !dlist_->reserveMemory(size())) {
        // if another thread sees LOADING status, the memory reservation has succeeded.
        state_ = State::ERROR;
        error_ = folly::make_exception_wrapper<std::runtime_error>(fmt::format(
            "Failed to load {} due to insufficient resource", key()));
        load_promise_->setException(error_);
        load_promise_ = nullptr;
        return std::make_pair(false, folly::makeSemiFuture<NodePin>(error_));
    }

    // pin the cell now so that we can avoid taking the lock again in deferValue.
    auto p = NodePin(this);
    return std::make_pair(
        true,
        load_promise_->getSemiFuture().deferValue(
            [this, p = std::move(p)](auto&&) mutable { return std::move(p); }));
}

void
ListNode::set_error(folly::exception_wrapper error) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    AssertInfo(state_ != State::NOT_LOADED && state_ != State::ERROR,
               "Programming error: set_error() called on a {} cell",
               state_to_string(state_));
    // load failed, release the memory reservation.
    if (dlist_) {
        dlist_->releaseMemory(size());
    }
    // may be successfully loaded by another thread as a bonus, they will update used memory.
    if (state_ == State::LOADED) {
        return;
    }
    // else: state_ is LOADING
    state_ = State::ERROR;
    load_promise_->setException(error);
    load_promise_ = nullptr;
    error_ = std::move(error);
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
        case State::ERROR:
            return "ERROR";
    }
    throw std::invalid_argument("Invalid state");
}

void
ListNode::unpin() {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    AssertInfo(
        state_ == State::LOADED || state_ == State::ERROR,
        "Programming error: unpin() called on a {} cell, current pin_count {}",
        state_to_string(state_),
        pin_count_.load());
    if (state_ == State::ERROR) {
        return;
    }
    if (pin_count_.fetch_sub(1) == 1) {
        touch(false);
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
        dlist_->touchItem(this, size);
        last_touch_ = now;
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
    state_ = State::NOT_LOADED;
}

void
ListNode::unload() {
    // Default implementation does nothing
}

}  // namespace milvus::cachinglayer::internal
