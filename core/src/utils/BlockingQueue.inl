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

namespace milvus {
namespace server {

template <typename T>
void
BlockingQueue<T>::Put(const T& task) {
    std::unique_lock<std::mutex> lock(mtx);
    full_.wait(lock, [this] { return (queue_.size() < capacity_); });

    queue_.push(task);
    empty_.notify_all();
}

template <typename T>
T
BlockingQueue<T>::Take() {
    std::unique_lock<std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    T front(queue_.front());
    queue_.pop();
    full_.notify_all();
    return front;
}

template <typename T>
size_t
BlockingQueue<T>::Size() {
    std::lock_guard<std::mutex> lock(mtx);
    return queue_.size();
}

template <typename T>
T
BlockingQueue<T>::Front() {
    std::unique_lock<std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    T front(queue_.front());
    return front;
}

template <typename T>
T
BlockingQueue<T>::Back() {
    std::unique_lock<std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    T back(queue_.back());
    return back;
}

template <typename T>
bool
BlockingQueue<T>::Empty() {
    std::unique_lock<std::mutex> lock(mtx);
    return queue_.empty();
}

template <typename T>
void
BlockingQueue<T>::SetCapacity(const size_t capacity) {
    capacity_ = (capacity > 0 ? capacity : capacity_);
}

}  // namespace server
}  // namespace milvus
