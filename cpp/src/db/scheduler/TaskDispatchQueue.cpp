// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include "TaskDispatchQueue.h"
#include "TaskDispatchStrategy.h"
#include "utils/Error.h"
#include "utils/Log.h"

namespace zilliz {
namespace milvus {
namespace engine {

void
TaskDispatchQueue::Put(const ScheduleContextPtr &context) {
    std::unique_lock <std::mutex> lock(mtx);
    full_.wait(lock, [this] { return (queue_.size() < capacity_); });

    if(context == nullptr) {
        queue_.push_front(nullptr);
        empty_.notify_all();
        return;
    }

    TaskDispatchStrategy::Schedule(context, queue_);

    empty_.notify_all();
}

ScheduleTaskPtr
TaskDispatchQueue::Take() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    ScheduleTaskPtr front(queue_.front());
    queue_.pop_front();
    full_.notify_all();
    return front;
}

size_t
TaskDispatchQueue::Size() {
    std::lock_guard <std::mutex> lock(mtx);
    return queue_.size();
}

ScheduleTaskPtr
TaskDispatchQueue::Front() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });
    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw server::ServerException(SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }
    ScheduleTaskPtr front(queue_.front());
    return front;
}

ScheduleTaskPtr
TaskDispatchQueue::Back() {
    std::unique_lock <std::mutex> lock(mtx);
    empty_.wait(lock, [this] { return !queue_.empty(); });

    if (queue_.empty()) {
        std::string error_msg = "blocking queue empty";
        SERVER_LOG_ERROR << error_msg;
        throw server::ServerException(SERVER_BLOCKING_QUEUE_EMPTY, error_msg);
    }

    ScheduleTaskPtr back(queue_.back());
    return back;
}

bool
TaskDispatchQueue::Empty() {
    std::unique_lock <std::mutex> lock(mtx);
    return queue_.empty();
}

void
TaskDispatchQueue::SetCapacity(const size_t capacity) {
    capacity_ = (capacity > 0 ? capacity : capacity_);
}

}
}
}