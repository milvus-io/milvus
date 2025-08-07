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
#include <utility>

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

ListNode::ListNode(DList* dlist,
                   bool evictable)
    : last_touch_(dlist ? (std::chrono::high_resolution_clock::now() -
                           2 * dlist->eviction_config().cache_touch_window)
                        : std::chrono::high_resolution_clock::now()),
      dlist_(dlist),
      evictable_(evictable) {
}

ListNode::~ListNode() {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    switch (state_) {
        case State::LOADED: {
            if (evictable_) {
                dlist_->removeItem(this, loaded_size_);
            }
            dlist_->decreaseLoadedResource(loaded_size_);
            break;
        }
        case State::LOADING: {
            // NOTE:
            // The LOADING state occurs during pin() and set_cell(), while LoadingResource +/- is handled in RunLoad().
            // We believe RunLoad() will handle all situations, including exceptions, so there's no need to handle
            // LoadingResource here.
            // If that edge case actually occurs, it shouldn't be the fault of ~ListNode() - the system must have bugs.
            break;
        }
        default:;  // do nothing
    }
}

bool
ListNode::manual_evict() {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    switch (state_) {
        case State::LOADED: {
            if (pin_count_.load() > 0) {
                LOG_ERROR(
                    "[MCL] manual_evict() called on a LOADED and pinned cell "
                    "{}, "
                    "aborting eviction.",
                    key());
                return false;
            }
            clear_data();
            if (evictable_) {
                dlist_->removeItem(this, loaded_size_);
            }
            dlist_->decreaseLoadedResource(loaded_size_);
            return true;
        }
        case State::LOADING: {
            LOG_ERROR("[MCL] manual_evict() called on a {} cell {}",
                      state_to_string(state_),
                      key());
            return false;
        }
        case State::NOT_LOADED: {
            return false;
        }
    }
}

const ResourceUsage&
ListNode::loaded_size() const {
    return loaded_size_;
}

std::pair<bool, folly::SemiFuture<ListNode::NodePin>>
ListNode::pin() {
    // must be called with lock acquired, and state must not be NOT_LOADED.
    auto read_op = [this]() -> std::pair<bool, folly::SemiFuture<NodePin>> {
        // pin the cell now so that we can avoid taking the lock again in deferValue.
        auto old_pin_count = pin_count_.fetch_add(1);
        switch (state_) {
            case State::LOADED: {
                if (old_pin_count == 0 && evictable_) {
                    // node became inevictable, freeze it if it is in dlist
                    dlist_->freezeItem(this, loaded_size_);
                }
                internal::cache_op_result_count_hit(loaded_size_.storage_type())
                    .Increment();
                auto p = NodePin(this);
                return std::make_pair(false, std::move(p));
            }
            case State::LOADING: {
                internal::cache_op_result_count_miss(
                    loaded_size_.storage_type())
                    .Increment();
                auto p = NodePin(this);
                return std::make_pair(
                    false,
                    load_promise_->getSemiFuture().deferValue(
                        [this, p = std::move(p)](auto&&) mutable {
                            return std::move(p);
                        }));
                break;
            }
            default:
                ThrowInfo(ErrorCode::UnexpectedError,
                          "Programming error: read_op called on a {} cell",
                          state_to_string(state_));
        }
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

    // need to load. state_ == State::NOT_LOADED
    internal::cache_op_result_count_miss(loaded_size_.storage_type())
        .Increment();
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
        switch (state_) {
            case State::LOADING: {
                state_ = State::NOT_LOADED;
                if (load_promise_) {
                    promise = std::move(load_promise_);
                }
                break;
            }
            case State::LOADED: {
                // may be successfully loaded by another thread as a bonus, they will update used memory.
                return;
            }
            default:
                ThrowInfo(ErrorCode::UnexpectedError,
                          "Programming error: set_error() called on a {} cell",
                          state_to_string(state_));
        }
    }
    // Notify waiting threads about the error
    // setException may call continuation of bound futures inline, and those continuation may also need to acquire the
    // lock, which may cause deadlock. So we release the lock before calling setException.
    if (promise) {
        promise->setException(std::move(error));
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
        if (evictable_) {
            touch(state_ == State::LOADED);
        }
    }
}

// ListNode::touch() should only be called when evictable_ is true
void
ListNode::touch(bool update_evictable_memory) {
    auto now = std::chrono::high_resolution_clock::now();
    if (now - last_touch_ > dlist_->eviction_config().cache_touch_window) {
        std::optional<ResourceUsage> size = std::nullopt;
        if (update_evictable_memory) {
            size = loaded_size_;
        }
        last_touch_ = dlist_->touchItem(this, size);
    }
}

void
ListNode::clear_data() {
    // if the cell is evicted, loaded, pinned and unpinned within a single refresh window,
    // the cell should be inserted into the cache again.
    if (evictable_) {
        last_touch_ = std::chrono::high_resolution_clock::now() -
                      2 * dlist_->eviction_config().cache_touch_window;
    }
    unload();
    LOG_TRACE("[MCL] ListNode evicted: key={}, size={}",
              key(),
              loaded_size_.ToString());
    state_ = State::NOT_LOADED;
}

void
ListNode::unload() {
    // Default implementation does nothing
}

}  // namespace milvus::cachinglayer::internal
