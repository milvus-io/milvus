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
#include "scheduler/tasklabel/BroadcastLabel.h"
#include "tasklabel/DefaultLabel.h"

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
        auto task = std::make_shared<XSearchTask>(index_file.second);
        task->label() = std::make_shared<DefaultLabel>();
        task->job_ = job;
        tasks.emplace_back(task);
    }

    return tasks;
}

std::vector<TaskPtr>
TaskCreator::Create(const DeleteJobPtr& job) {
    std::vector<TaskPtr> tasks;
    auto task = std::make_shared<XDeleteTask>(job);
    task->label() = std::make_shared<BroadcastLabel>();
    task->job_ = job;
    tasks.emplace_back(task);

    return tasks;
}

}  // namespace scheduler
}  // namespace milvus
