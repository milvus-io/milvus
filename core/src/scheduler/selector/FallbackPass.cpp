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

#include "scheduler/selector/FallbackPass.h"
#include "scheduler/SchedInst.h"
#include "scheduler/tasklabel/SpecResLabel.h"

namespace milvus {
namespace scheduler {

void
FallbackPass::Init() {
}

bool
FallbackPass::Run(const TaskPtr& task) {
    auto task_type = task->Type();
    if (task_type != TaskType::SearchTask && task_type != TaskType::BuildIndexTask) {
        return false;
    }
    // NEVER be empty
    LOG_SERVER_DEBUG_ << "FallbackPass!";
    auto cpu = ResMgrInst::GetInstance()->GetCpuResources()[0];
    auto label = std::make_shared<SpecResLabel>(cpu);
    task->label() = label;
    return true;
}

}  // namespace scheduler
}  // namespace milvus
