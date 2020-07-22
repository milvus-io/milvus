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

#include "scheduler/TaskCreator.h"
#include "scheduler/SchedInst.h"
#include "scheduler/task/BuildIndexTask.h"
#include "scheduler/task/DeleteTask.h"
#include "scheduler/task/SSBuildIndexTask.h"
#include "scheduler/task/SSSearchTask.h"
#include "scheduler/task/SearchTask.h"
#include "scheduler/tasklabel/BroadcastLabel.h"
#include "scheduler/tasklabel/SpecResLabel.h"

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
        case JobType::SS_SEARCH: {
            return Create(std::static_pointer_cast<SSSearchJob>(job));
        }
        case JobType::SS_BUILD: {
            return Create(std::static_pointer_cast<SSBuildIndexJob>(job));
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
        auto task = std::make_shared<XSearchTask>(job->GetContext(), index_file.second, nullptr);
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
    for (auto& to_index_file : job->to_index_files()) {
        auto task = std::make_shared<XBuildIndexTask>(to_index_file.second, nullptr);
        task->job_ = job;
        tasks.emplace_back(task);
    }
    return tasks;
}

std::vector<TaskPtr>
TaskCreator::Create(const SSSearchJobPtr& job) {
    std::vector<TaskPtr> tasks;
    for (auto& id : job->segment_ids()) {
        auto task = std::make_shared<SSSearchTask>(job->GetContext(), job->options(), job->query_ptr(), id, nullptr);
        task->job_ = job;
        tasks.emplace_back(task);
    }
    return tasks;
}

std::vector<TaskPtr>
TaskCreator::Create(const SSBuildIndexJobPtr& job) {
    std::vector<TaskPtr> tasks;
    const std::string& collection_name = job->collection_name();
    for (auto& id : job->segment_ids()) {
        auto task = std::make_shared<SSBuildIndexTask>(job->options(), collection_name, id, nullptr);
        task->job_ = job;
        tasks.emplace_back(task);
    }
    return tasks;
}

}  // namespace scheduler
}  // namespace milvus
