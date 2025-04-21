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
#include <chrono>
#include <memory>

#include <folly/ExceptionWrapper.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "cachinglayer/Utils.h"
#include "common/EasyAssert.h"

namespace milvus::cachinglayer::internal {

class DList;

// ListNode is not movable/copyable.
class ListNode {
 public:
    // RAII class to unpin the node.
    class NodePin {
     public:
        // NodePin is movable but not copyable.
        NodePin(NodePin&&);
        NodePin&
        operator=(NodePin&&);
        NodePin(const NodePin&) = delete;
        NodePin&
        operator=(const NodePin&) = delete;
        ~NodePin();

     private:
        NodePin(ListNode* node);
        friend class ListNode;
        ListNode* node_;
    };
    ListNode() = default;
    ListNode(DList* dlist, ResourceUsage size);
    virtual ~ListNode();

    // ListNode is not movable/copyable because it contains a shared_mutex.
    // ListNode also should not be movable/copyable because that would make
    // all NodePin::node_ dangling pointers.
    ListNode(const ListNode&) = delete;
    ListNode&
    operator=(const ListNode&) = delete;
    ListNode(ListNode&&) = delete;
    ListNode&
    operator=(ListNode&&) = delete;

    // bool in return value: whether the caller needs to load this cell.
    // - If the cell is already loaded, return false and an immediately ready future with a NodePin, the node is pinned
    //   upon return.
    // - If the cell is in error state, return false and an immediately ready future with an exception.
    // - If the cell is already being loaded by another thread, return false and a future that will be ready when the
    //   cell is loaded. The node will not be pinned until the future is ready.
    // - Otherwise, the cell is not loaded and not being loaded, return true and a future that will be ready when the
    //   cell is loaded. The caller needs to load this cell and call mark_loaded() to set the cell as loaded.
    //   The node will not be pinned until the future is ready.
    std::pair<bool, folly::SemiFuture<NodePin>>
    pin();

    ResourceUsage&
    size();

    // TODO(tiered storage 1): pin on ERROR should re-trigger loading.
    // NOT_LOADED ---> LOADING ---> ERROR
    //      ^            |
    //      |            v
    //      |------- LOADED
    enum class State { NOT_LOADED, LOADING, LOADED, ERROR };

 protected:
    // will be called during eviction, implementation should release all resources.
    virtual void
    unload();

    virtual std::string
    key() const = 0;

    template <typename Fn>
    void
    mark_loaded(Fn&& cb, bool requesting_thread) {
        std::unique_lock<std::shared_mutex> lock(mtx_);
        if (requesting_thread) {
            // requesting thread will promote NOT_LOADED to LOADING and only requesting
            // thread will set state to ERROR, thus it is not possible for the requesting
            // thread to see NOT_LOADED or ERROR.
            AssertInfo(state_ != State::NOT_LOADED && state_ != State::ERROR,
                       "Programming error: mark_loaded(requesting_thread=true) "
                       "called on a {} cell",
                       state_to_string(state_));
            // no need to touch() here: node is pinned thus not eligible for eviction.
            // we can delay touch() to when unpin() is called.
            if (state_ == State::LOADING) {
                cb();
                state_ = State::LOADED;
                load_promise_->setValue(folly::Unit());
                load_promise_ = nullptr;
            } else {
                // LOADED: cell has been loaded by another thread, do nothing.
                return;
            }
        } else {
            // Even though this thread did not request loading this cell, translator still
            // decided to download it because the adjacent cells are requested.
            if (state_ == State::NOT_LOADED || state_ == State::ERROR) {
                state_ = State::LOADED;
                cb();
                // memory of this cell is not reserved, touch() to track it.
                touch(true);
            } else if (state_ == State::LOADING) {
                // another thread has explicitly requested loading this cell, we did it first
                // thus we set up the state first.
                state_ = State::LOADED;
                load_promise_->setValue(folly::Unit());
                load_promise_ = nullptr;
                cb();
                // the node that marked LOADING has already reserved memory, do not double count.
                touch(false);
            } else {
                // LOADED: cell has been loaded by another thread, do nothing.
                return;
            }
        }
    }

    void
    set_error(folly::exception_wrapper error);

    State state_{State::NOT_LOADED};

    static std::string
    state_to_string(State state);

    ResourceUsage size_{};

 private:
    friend class DList;
    friend class NodePin;

    friend class MockListNode;
    friend class DListTest;
    friend class DListTestFriend;
    friend class ListNodeTestFriend;
    friend class ListNodeTest;

    // called by DList during eviction. must be called under the lock of mtx_.
    // Made virtual for mock testing.
    virtual void
    clear_data();

    void
    unpin();

    // must be called under the lock of mtx_.
    void
    touch(bool update_used_memory = true);

    mutable std::shared_mutex mtx_;
    std::chrono::high_resolution_clock::time_point last_touch_;
    // a nullptr dlist_ means this node is not in any DList, and is not prone to cache management.
    DList* dlist_;
    ListNode* prev_ = nullptr;
    ListNode* next_ = nullptr;
    std::atomic<int> pin_count_{0};

    std::unique_ptr<folly::SharedPromise<folly::Unit>> load_promise_{nullptr};
    folly::exception_wrapper error_;
};

}  // namespace milvus::cachinglayer::internal
