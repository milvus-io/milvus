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


#include "TaskConvert.h"
#include "scheduler/tasklabel/DefaultLabel.h"
#include "scheduler/tasklabel/BroadcastLabel.h"


namespace zilliz {
namespace milvus {
namespace engine {

TaskPtr
TaskConvert(const ScheduleTaskPtr &schedule_task) {
    switch (schedule_task->type()) {
        case ScheduleTaskType::kIndexLoad: {
            auto load_task = std::static_pointer_cast<IndexLoadTask>(schedule_task);
            auto task = std::make_shared<XSearchTask>(load_task->file_);
            task->label() = std::make_shared<DefaultLabel>();
            task->search_contexts_ = load_task->search_contexts_;
            return task;
        }
        case ScheduleTaskType::kDelete: {
            auto delete_task = std::static_pointer_cast<DeleteTask>(schedule_task);
            auto task = std::make_shared<XDeleteTask>(delete_task->context_);
            task->label() = std::make_shared<BroadcastLabel>();
            return task;
        }
        default: {
            // TODO: unexpected !!!
            return nullptr;
        }
    }
}

}
}
}
