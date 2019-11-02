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

#include "scheduler/TaskCreator.h"
#include "SchedInst.h"
#include "tasklabel/BroadcastLabel.h"
#include "tasklabel/DefaultLabel.h"
#include "tasklabel/SpecResLabel.h"

namespace milvus {
namespace scheduler {

std::vector<TaskPtr>
TaskCreator::Create(const JobPtr& job) {
    switch (job->type()) {
        case JobType::SEARCH: {
            return Create(std::static_pointer_cast<SearchJob>(job));
        }
        case JobType::DELETE: {
            return Create(std::static_pointer_cast<DeleteJob>(job));
        }
        case JobType::BUILD: {
            return Create(std::static_pointer_cast<BuildIndexJob>(job));
        }
        default: {
            // TODO(wxyu): error
            return std::vector<TaskPtr>();
        }
    }
}

std::vector<TaskPtr>
TaskCreator::Create(const SearchJobPtr& job) {
    std::vector<TaskPtr> tasks;
    for (auto& index_file : job->index_files()) {
        auto label = std::make_shared<DefaultLabel>();
        auto task = std::make_shared<XSearchTask>(index_file.second, label);
        task->job_ = job;
        tasks.emplace_back(task);
    }

    return tasks;
}

std::vector<TaskPtr>
TaskCreator::Create(const DeleteJobPtr& job) {
    std::vector<TaskPtr> tasks;
    auto label = std::make_shared<BroadcastLabel>();
    auto task = std::make_shared<XDeleteTask>(job, label);
    task->job_ = job;
    tasks.emplace_back(task);

    return tasks;
}

std::vector<TaskPtr>
TaskCreator::Create(const BuildIndexJobPtr& job) {
    std::vector<TaskPtr> tasks;
    server::Config& config = server::Config::GetInstance();
    int32_t build_index_id;
    Status stat = config.GetResourceConfigIndexBuildDevice(build_index_id);
    ResourcePtr res_ptr;
    if (build_index_id == server::CPU_DEVICE_ID) {
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else {
        res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, build_index_id);
    }

    for (auto& to_index_file : job->to_index_files()) {
        auto label = std::make_shared<SpecResLabel>(std::weak_ptr<Resource>(res_ptr));
        auto task = std::make_shared<XBuildIndexTask>(to_index_file.second, label);
        task->job_ = job;
        tasks.emplace_back(task);
    }
    return tasks;
}

}  // namespace scheduler
}  // namespace milvus
