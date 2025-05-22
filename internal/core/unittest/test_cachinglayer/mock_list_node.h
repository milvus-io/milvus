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

#pragma once

#include <gmock/gmock.h>
#include <fmt/format.h>

#include "cachinglayer/lrucache/ListNode.h"
#include "cachinglayer/Utils.h"

namespace milvus::cachinglayer::internal {

class MockListNode : public ListNode {
 public:
    MockListNode(DList* dlist,
                 ResourceUsage size,
                 const std::string& key = "mock_key",
                 cid_t cid = 0)
        : ListNode(dlist, size), mock_key_(fmt::format("{}:{}", key, cid)) {
        ON_CALL(*this, clear_data).WillByDefault([this]() {
            unload();
            state_ = State::NOT_LOADED;
        });
    }

    MOCK_METHOD(void, clear_data, (), (override));

    std::string
    key() const override {
        return mock_key_;
    }

    // Directly manipulate state for test setup (Use carefully!)
    void
    test_set_state(State new_state) {
        std::unique_lock lock(mtx_);
        state_ = new_state;
    }
    State
    test_get_state() {
        std::shared_lock lock(mtx_);
        return state_;
    }

    void
    test_set_pin_count(int count) {
        pin_count_.store(count);
    }
    int
    test_get_pin_count() const {
        return pin_count_.load();
    }

    // Expose mutex for lock testing
    std::shared_mutex&
    test_get_mutex() {
        return mtx_;
    }

    ListNode*
    test_get_prev() const {
        return prev_;
    }
    ListNode*
    test_get_next() const {
        return next_;
    }

 private:
    friend class DListTest;
    friend class DListTestFriend;
    std::string mock_key_;
};

}  // namespace milvus::cachinglayer::internal