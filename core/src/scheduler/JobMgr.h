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

#include <memory>
#include <thread>
#include <vector>

#include "scheduler/ResourceMgr.h"
#include "scheduler/interface/interfaces.h"
#include "scheduler/job/Job.h"
#include "scheduler/task/Task.h"
#include "utils/BlockingQueue.h"

namespace milvus {
namespace scheduler {

class JobMgr : public interface::dumpable {
 public:
    explicit JobMgr(ResourceMgrPtr res_mgr);

    void
    Start();

    void
    Stop();

    json
    Dump() const override;

 public:
    void
    Put(const JobPtr& job);

 private:
    void
    worker_function();

    static std::vector<TaskPtr>
    build_task(const JobPtr& job);

 public:
    static void
    calculate_path(const ResourceMgrPtr& res_mgr, const TaskPtr& task);

 private:
    BlockingQueue<JobPtr> queue_;
    std::shared_ptr<std::thread> worker_thread_ = nullptr;
    ResourceMgrPtr res_mgr_ = nullptr;
};

using JobMgrPtr = std::shared_ptr<JobMgr>;

}  // namespace scheduler
}  // namespace milvus
