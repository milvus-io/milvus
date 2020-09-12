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

#include "scheduler/CPUBuilder.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

void
CPUBuilder::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (not running_) {
        running_ = true;
        thread_ = std::thread(&CPUBuilder::worker_function, this);
    }
}

void
CPUBuilder::Stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) {
        this->Put(nullptr);
        thread_.join();
        running_ = false;
    }
}

void
CPUBuilder::Put(const TaskPtr& task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.push(task);
    }
    queue_cv_.notify_one();
}

void
CPUBuilder::worker_function() {
    SetThreadName("cpubuilder_thread");
    while (running_) {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [&] { return not queue_.empty(); });
        auto task = queue_.front();
        queue_.pop();
        lock.unlock();

        if (task == nullptr) {
            // thread exit
            break;
        }
        task->Load(LoadType::DISK2CPU, 0);
        task->Execute();
    }
}

}  // namespace scheduler
}  // namespace milvus
