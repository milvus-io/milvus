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

#include <limits>
#include <string>
#include <utility>

#include "db/Utils.h"
#include "scheduler/Algorithm.h"
#include "scheduler/CPUBuilder.h"
#include "scheduler/JobMgr.h"
#include "scheduler/SchedInst.h"
#include "scheduler/TaskCreator.h"
#include "scheduler/selector/Optimizer.h"
#include "scheduler/task/Task.h"
#include "scheduler/tasklabel/SpecResLabel.h"

namespace milvus {
namespace scheduler {

JobMgr::JobMgr(ResourceMgrPtr res_mgr) : res_mgr_(std::move(res_mgr)) {
}

void
JobMgr::Start() {
    if (worker_thread_ == nullptr) {
        worker_thread_ = std::make_shared<std::thread>(&JobMgr::worker_function, this);
    }
}

void
JobMgr::Stop() {
    if (worker_thread_ != nullptr) {
        this->Put(nullptr);
        worker_thread_->join();
        worker_thread_ = nullptr;
    }
}

json
JobMgr::Dump() const {
    json ret{
        {"running", (worker_thread_ != nullptr ? true : false)},
        {"event_queue_length", queue_.Size()},
    };
    return ret;
}

void
JobMgr::Put(const JobPtr& job) {
    queue_.Put(job);
}

void
JobMgr::worker_function() {
    SetThreadName("jobmgr_thread");
    while (true) {
        auto job = queue_.Take();
        if (job == nullptr) {
            break;
        }

        //        auto search_job = std::dynamic_pointer_cast<SearchJob>(job);
        //        if (search_job != nullptr) {
        //            search_job->GetResultIds().resize(search_job->nq(), -1);
        //            search_job->GetResultDistances().resize(search_job->nq(), std::numeric_limits<float>::max());
        //        }

        auto tasks = build_task(job);
        for (auto& task : tasks) {
            OptimizerInst::GetInstance()->Run(task);
        }

        for (auto& task : tasks) {
            calculate_path(res_mgr_, task);
        }

        // disk resources NEVER be empty.
        if (auto disk = res_mgr_->GetDiskResources()[0].lock()) {
            // if (auto disk = res_mgr_->GetCpuResources()[0].lock()) {
            for (auto& task : tasks) {
                if (task->Type() == TaskType::BuildIndexTask && task->path().Last() == "cpu") {
                    CPUBuilderInst::GetInstance()->Put(task);
                } else {
                    disk->task_table().Put(task, nullptr);
                }
            }
        }
    }
}

std::vector<TaskPtr>
JobMgr::build_task(const JobPtr& job) {
    return TaskCreator::Create(job);
}

void
JobMgr::calculate_path(const ResourceMgrPtr& res_mgr, const TaskPtr& task) {
    if (task->type_ != TaskType::SearchTask && task->type_ != TaskType::BuildIndexTask) {
        return;
    }

    if (task->label()->Type() != TaskLabelType::SPECIFIED_RESOURCE) {
        return;
    }

    std::vector<std::string> path;
    auto spec_label = std::static_pointer_cast<SpecResLabel>(task->label());
    auto src = res_mgr->GetDiskResources()[0];
    auto dest = spec_label->resource();
    ShortestPath(src.lock(), dest.lock(), res_mgr, path);
    task->path() = Path(path, path.size() - 1);
}

}  // namespace scheduler
}  // namespace milvus
