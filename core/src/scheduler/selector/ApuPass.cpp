// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#ifdef MILVUS_APU_VERSION

#include "scheduler/selector/ApuPass.h"
#include "scheduler/SchedInst.h"
#include "scheduler/tasklabel/SpecResLabel.h"

namespace milvus {
namespace scheduler {

void
ApuPass::Init() {
}

bool
ApuPass::Run(const TaskPtr& task) {
    auto task_type = task->Type();
    auto search_task = std::static_pointer_cast<XSearchTask>(task);

    if (task_type != TaskType::SearchTask ||
        search_task->file_->engine_type_ != (int)engine::EngineType::FAISS_BIN_IDMAP) {
        return false;
    }

    auto apu = ResMgrInst::GetInstance()->GetResource(ResourceType::FPGA, 0);
    auto lable = std::make_shared<SpecResLabel>(apu);
    task->label() = lable;

    return true;
}

}  // namespace scheduler
}  // namespace milvus
#endif
