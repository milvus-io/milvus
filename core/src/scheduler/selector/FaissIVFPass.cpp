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
#include "scheduler/selector/FaissIVFPass.h"
#include "cache/GpuCacheMgr.h"
#include "config/ServerConfig.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "scheduler/SchedInst.h"
#include "scheduler/Utils.h"
#include "scheduler/task/SearchTask.h"
#include "server/ValidationUtil.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

FaissIVFPass::FaissIVFPass() {
    ConfigMgr::GetInstance().Attach("gpu.gpu_search_threshold", this);
}

FaissIVFPass::~FaissIVFPass() {
    ConfigMgr::GetInstance().Detach("gpu.gpu_search_threshold", this);
}

void
FaissIVFPass::Init() {
#ifdef MILVUS_GPU_VERSION
    gpu_enable_ = config.gpu.enable();
    threshold_ = config.gpu.gpu_search_threshold();
    search_gpus_ = ParseGPUDevices(config.gpu.search_devices());
#endif
}

bool
FaissIVFPass::Run(const TaskPtr& task) {
    if (task->Type() != TaskType::SearchTask) {
        return false;
    }

    auto search_task = std::static_pointer_cast<SearchTask>(task);
    auto index_type = search_task->IndexType();
    if (index_type != knowhere::IndexEnum::INDEX_FAISS_IVFFLAT &&
        index_type != knowhere::IndexEnum::INDEX_FAISS_IVFPQ && index_type != knowhere::IndexEnum::INDEX_FAISS_IVFSQ8) {
        return false;
    }

    ResourcePtr res_ptr;
    if (!gpu_enable_) {
        LOG_SERVER_DEBUG_ << LogOut("FaissIVFPass: gpu disable, specify cpu to search!");
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else if (search_task->nq() < threshold_) {
        LOG_SERVER_DEBUG_ << LogOut("FaissIVFPass: nq < gpu_search_threshold, specify cpu to search!");
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else if (search_task->topk() > server::GPU_QUERY_MAX_TOPK) {
        LOG_SERVER_DEBUG_ << LogOut("FaissIVFPass: topk > gpu_max_topk_threshold, specify cpu to search!");
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else if (search_task->ExtraParam()[knowhere::IndexParams::nprobe].get<int64_t>() > server::GPU_QUERY_MAX_NPROBE) {
        LOG_SERVER_DEBUG_ << LogOut("FaissIVFPass: nprobe > gpu_max_nprobe_threshold, specify cpu to search!");
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else {
        LOG_SERVER_DEBUG_ << LogOut("FaissIVFPass: nq >= gpu_search_threshold, specify gpu %d to search!",
                                    search_gpus_[idx_]);
        res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, search_gpus_[idx_]);
        idx_ = (idx_ + 1) % search_gpus_.size();
    }
    task->resource() = res_ptr;
    return true;
}

void
FaissIVFPass::ValueUpdate(const std::string& name) {
    threshold_ = config.gpu.gpu_search_threshold();
}

}  // namespace scheduler
}  // namespace milvus
#endif
