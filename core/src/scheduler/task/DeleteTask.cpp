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

#include "scheduler/task/DeleteTask.h"

#include <utility>

namespace milvus {
namespace scheduler {

XDeleteTask::XDeleteTask(const scheduler::DeleteJobPtr& delete_job, TaskLabelPtr label)
    : Task(TaskType::DeleteTask, std::move(label)), delete_job_(delete_job) {
}

void
XDeleteTask::Load(LoadType type, uint8_t device_id) {
}

void
XDeleteTask::Execute() {
    delete_job_->ResourceDone();
}

}  // namespace scheduler
}  // namespace milvus
