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

#include "scheduler/task/TestTask.h"

#include <utility>

#include "cache/GpuCacheMgr.h"

namespace milvus {
namespace scheduler {

TestTask::TestTask(const std::shared_ptr<server::Context>& context, TableFileSchemaPtr& file, TaskLabelPtr label)
    : XSearchTask(context, file, std::move(label)) {
}

void
TestTask::Load(LoadType type, uint8_t device_id) {
    load_count_++;
}

void
TestTask::Execute() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        exec_count_++;
        done_ = true;
    }
    cv_.notify_one();
}

void
TestTask::Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] { return done_; });
}

}  // namespace scheduler
}  // namespace milvus
