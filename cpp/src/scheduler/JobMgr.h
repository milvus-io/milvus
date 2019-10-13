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
#pragma once

#include <condition_variable>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ResourceMgr.h"
#include "job/Job.h"
#include "task/Task.h"

namespace milvus {
namespace scheduler {

class JobMgr {
 public:
    explicit JobMgr(ResourceMgrPtr res_mgr);

    void
    Start();

    void
    Stop();

 public:
    void
    Put(const JobPtr& job);

 private:
    void
    worker_function();

    static std::vector<TaskPtr>
    build_task(const JobPtr& job);

    void
    calculate_path(const TaskPtr& task);

 private:
    bool running_ = false;
    std::queue<JobPtr> queue_;

    std::thread worker_thread_;

    std::mutex mutex_;
    std::condition_variable cv_;

    ResourceMgrPtr res_mgr_ = nullptr;
};

using JobMgrPtr = std::shared_ptr<JobMgr>;

}  // namespace scheduler
}  // namespace milvus
