// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <assert.h>
#include <condition_variable>
#include <iostream>
#include <queue>
#include <vector>

namespace milvus {
namespace server {

template <typename T>
class BlockingQueue {
 public:
    BlockingQueue() : mtx(), full_(), empty_() {
    }

    virtual ~BlockingQueue() {
    }

    BlockingQueue(const BlockingQueue& rhs) = delete;

    BlockingQueue&
    operator=(const BlockingQueue& rhs) = delete;

    void
    Put(const T& task);

    T
    Take();

    T
    Front();

    T
    Back();

    size_t
    Size();

    bool
    Empty();

    void
    SetCapacity(const size_t capacity);

 protected:
    mutable std::mutex mtx;
    std::condition_variable full_;
    std::condition_variable empty_;
    std::queue<T> queue_;
    size_t capacity_ = 32;
};

}  // namespace server
}  // namespace milvus

#include "./BlockingQueue.inl"
