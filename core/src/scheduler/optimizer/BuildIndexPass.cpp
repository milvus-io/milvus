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
#include <fiu-local.h>

#include "scheduler/SchedInst.h"
#include "scheduler/Utils.h"
#include "scheduler/optimizer/BuildIndexPass.h"
#include "scheduler/tasklabel/SpecResLabel.h"
#ifdef MILVUS_GPU_VERSION
namespace milvus {
namespace scheduler {

void
BuildIndexPass::Init() {
    server::Config& config = server::Config::GetInstance();
    Status s = config.GetGpuResourceConfigBuildIndexResources(build_gpus_);
    fiu_do_on("BuildIndexPass.Init.get_config_fail", s = Status(milvus::SERVER_UNEXPECTED_ERROR, ""));
    if (!s.ok()) {
        throw std::exception();
    }

    SetIdentity("BuildIndexPass");
    AddGpuEnableListener();
    AddGpuBuildResourcesListener();
}

bool
BuildIndexPass::Run(const TaskPtr& task) {
    if (task->Type() != TaskType::BuildIndexTask)
        return false;

    ResourcePtr res_ptr;
    if (!gpu_enable_) {
        LOG_SERVER_DEBUG_ << "Gpu disabled, specify cpu to build index!";
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else {
        fiu_do_on("BuildIndexPass.Run.empty_gpu_ids", build_gpus_.clear());
        if (build_gpus_.empty()) {
            LOG_SERVER_WARNING_ << "BuildIndexPass cannot get build index gpu!";
            return false;
        }

        if (specified_gpu_id_ >= build_gpus_.size()) {
            specified_gpu_id_ = specified_gpu_id_ % build_gpus_.size();
        }
        LOG_SERVER_DEBUG_ << "Specify gpu" << specified_gpu_id_ << " to build index!";
        res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, build_gpus_[specified_gpu_id_]);
        specified_gpu_id_ = (specified_gpu_id_ + 1) % build_gpus_.size();
    }

    auto label = std::make_shared<SpecResLabel>(std::weak_ptr<Resource>(res_ptr));
    task->label() = label;

    return true;
}

}  // namespace scheduler
}  // namespace milvus
#endif
