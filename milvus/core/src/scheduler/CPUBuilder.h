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

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include "task/Task.h"

namespace milvus {
namespace scheduler {

class CPUBuilder {
 public:
    CPUBuilder() = default;

    void
    Start();

    void
    Stop();

    void
    Put(const TaskPtr& task);

 private:
    void
    worker_function();

 private:
    bool running_ = false;
    std::mutex mutex_;
    std::thread thread_;

    std::queue<TaskPtr> queue_;
    std::condition_variable queue_cv_;
    std::mutex queue_mutex_;
};

using CPUBuilderPtr = std::shared_ptr<CPUBuilder>;

}  // namespace scheduler
}  // namespace milvus
