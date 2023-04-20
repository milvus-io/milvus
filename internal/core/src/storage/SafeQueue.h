// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma

#include <queue>
#include <shared_mutex>
#include <utility>

namespace milvus {

template <typename T>
class SafeQueue {
 public:
    SafeQueue(void) {
    }
    ~SafeQueue() {
    }

    bool
    empty() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return queue_.empty();
    }

    size_t
    size() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return queue_.size();
    }

    // Add a element to queue
    void
    enqueue(T t) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        queue_.push(t);
    }

    // Get a available element from queue
    bool
    dequeue(T& t) {
        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (queue_.empty()) {
            return false;
        }

        t = std::move(queue_.front());
        queue_.pop();

        return true;
    }

 private:
    std::queue<T> queue_;
    mutable std::shared_mutex mutex_;
};
}  // namespace milvus
