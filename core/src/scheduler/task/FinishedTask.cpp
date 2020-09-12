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

#include "scheduler/task/FinishedTask.h"

namespace milvus::scheduler {

std::shared_ptr<FinishedTask>
FinishedTask::Create(const TaskPtr& task) {
    return std::make_shared<FinishedTask>(task);
}

FinishedTask::FinishedTask(const TaskPtr& task) : Task(TaskType::SearchTask, nullptr) {
    Task::task_path_ = task->task_path_;
    Task::type_ = task->type_;
    Task::label_ = task->label_;
}

Status
FinishedTask::OnLoad(LoadType type, uint8_t device_id) {
    return Status::OK();
}

Status
FinishedTask::OnExecute() {
    return Status::OK();
}

}  // namespace milvus::scheduler
