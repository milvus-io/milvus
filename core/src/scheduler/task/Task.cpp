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

#include "scheduler/task/Task.h"
#include "scheduler/job/Job.h"

#include "utils/Log.h"

namespace milvus {
namespace scheduler {

void
Task::Load(LoadType type, uint8_t device_id) {
    auto status = OnLoad(type, device_id);

    if (job_) {
        if (!status.ok()) {
            job_->status() = status;
            //            job_->TaskDone(this);
        }
    } else {
        LOG_ENGINE_ERROR_ << "Scheduler task's parent job not specified!";
    }
}

void
Task::Execute() {
    auto status = OnExecute();

    if (job_) {
        if (!status.ok()) {
            job_->status() = status;
        }
        job_->TaskDone(this);
    } else {
        LOG_ENGINE_ERROR_ << "Scheduler task's parent job not specified!";
    }
}

}  // namespace scheduler
}  // namespace milvus
