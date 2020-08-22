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

#include <atomic>
#include <condition_variable>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus {
namespace scheduler {

template <typename T>
class CircleQueue {
    using value_type = T;
    using atomic_size_type = std::atomic_ullong;
    using size_type = uint64_t;
    using const_reference = const value_type&;
#define MEMORY_ORDER (std::memory_order::memory_order_seq_cst)

 public:
    explicit CircleQueue(size_type cap) : data_(cap, nullptr), capacity_(cap), front_() {
        front_.store(cap - 1, MEMORY_ORDER);
    }

    CircleQueue() = delete;
    CircleQueue(const CircleQueue& q) = delete;
    CircleQueue(CircleQueue&& q) = delete;

 public:
    const_reference operator[](size_type n) const {
        return data_[n % capacity_];
    }

    size_type
    front() const {
        return front_.load(MEMORY_ORDER);
    }

    size_type
    rear() const {
        return rear_;
    }

    size_type
    size() const {
        return size_;
    }

    size_type
    capacity() const {
        return capacity_;
    }

    void
    set_front(uint64_t last_finish) {
        if (last_finish == rear_) {
            throw;
        }
        front_.store(last_finish % capacity_, MEMORY_ORDER);
    }

    void
    put(const value_type& x) {
        if ((rear_) % capacity_ == front_.load(MEMORY_ORDER)) {
            throw;
        }
        data_[rear_] = x;
        rear_ = ++rear_ % capacity_;
        if (size_ < capacity_) {
            ++size_;
        }
    }

    void
    put(value_type&& x) {
        if ((rear_) % capacity_ == front_.load(MEMORY_ORDER)) {
            throw;
        }
        data_[rear_] = std::move(x);
        rear_ = ++rear_ % capacity_;
        if (size_ < capacity_) {
            ++size_;
        }
    }

 private:
    std::vector<value_type> data_;
    size_type capacity_;
    atomic_size_type front_;
    size_type rear_ = 0;
    size_type size_ = 0;
#undef MEMORY_ORDER
};

}  // namespace scheduler
}  // namespace milvus
