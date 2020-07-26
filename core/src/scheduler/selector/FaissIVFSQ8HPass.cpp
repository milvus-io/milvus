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

#ifdef MILVUS_GPU_VERSION
#include "scheduler/selector/FaissIVFSQ8HPass.h"
#include "cache/GpuCacheMgr.h"
#include "config/ServerConfig.h"
#include "scheduler/SchedInst.h"
#include "scheduler/Utils.h"
#include "scheduler/task/SearchTask.h"
#include "scheduler/tasklabel/SpecResLabel.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

FaissIVFSQ8HPass::FaissIVFSQ8HPass() {
    ConfigMgr::GetInstance().Attach("gpu.gpu_search_threshold", this);
}

FaissIVFSQ8HPass::~FaissIVFSQ8HPass() {
    ConfigMgr::GetInstance().Detach("gpu.gpu_search_threshold", this);
}

void
FaissIVFSQ8HPass::Init() {
    gpu_enable_ = config.gpu.enable();
    threshold_ = config.gpu.gpu_search_threshold();
    search_gpus_ = ParseGPUDevices(config.gpu.search_devices());
}

bool
FaissIVFSQ8HPass::Run(const TaskPtr& task) {
    if (task->Type() != TaskType::SearchTask) {
        return false;
    }

    auto search_task = std::static_pointer_cast<SearchTask>(task);
    //    if (search_task->file_->engine_type_ != (int)engine::EngineType::FAISS_IVFSQ8H) {
    //        return false;
    //    }

    auto search_job = std::static_pointer_cast<SearchJob>(search_task->job_.lock());
    ResourcePtr res_ptr;
    if (!gpu_enable_) {
        LOG_SERVER_DEBUG_ << LogOut("[%s][%d] FaissIVFSQ8HPass: gpu disable, specify cpu to search!", "search", 0);
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    }
    if (search_job->nq() < (uint64_t)threshold_) {
        LOG_SERVER_DEBUG_ << LogOut("[%s][%d] FaissIVFSQ8HPass: nq < gpu_search_threshold, specify cpu to search!",
                                    "search", 0);
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else {
        LOG_SERVER_DEBUG_ << LogOut("[%s][%d] FaissIVFSQ8HPass: nq >= gpu_search_threshold, specify gpu %d to search!",
                                    "search", 0, search_gpus_[idx_]);
        res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, search_gpus_[idx_]);
        idx_ = (idx_ + 1) % search_gpus_.size();
    }
    auto label = std::make_shared<SpecResLabel>(res_ptr);
    task->label() = label;
    return true;
}

void
FaissIVFSQ8HPass::ConfigUpdate(const std::string& name) {
    threshold_ = config.gpu.gpu_search_threshold();
}

}  // namespace scheduler
}  // namespace milvus
#endif
