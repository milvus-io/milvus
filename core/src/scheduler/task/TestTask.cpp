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

#include "scheduler/task/TestTask.h"

#include <utility>

#include "cache/GpuCacheMgr.h"

namespace milvus {
namespace scheduler {

TestTask::TestTask() : Task(TaskType::SearchTask) {
}

Status
TestTask::OnLoad(LoadType type, uint8_t device_id) {
    load_count_++;
    return Status::OK();
}

Status
TestTask::OnExecute() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        exec_count_++;
        done_ = true;
    }
    cv_.notify_one();

    return Status::OK();
}

void
TestTask::Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return done_; });
}

}  // namespace scheduler
}  // namespace milvus
