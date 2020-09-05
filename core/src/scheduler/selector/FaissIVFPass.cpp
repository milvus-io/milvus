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
#include "config/Config.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "scheduler/SchedInst.h"
#include "scheduler/Utils.h"
#include "scheduler/task/SearchTask.h"
#include "scheduler/tasklabel/SpecResLabel.h"
#include "utils/Log.h"
#include "utils/ValidationUtil.h"

namespace milvus {
namespace scheduler {
bool
FaissIVFPass::Run(const TaskPtr& task) {
    if (task->Type() != TaskType::SearchTask) {
        return false;
    }

    auto search_task = std::static_pointer_cast<XSearchTask>(task);
    switch (static_cast<engine::EngineType>(search_task->file_->engine_type_)) {
        case engine::EngineType::FAISS_IVFFLAT:
        case engine::EngineType::FAISS_PQ:
        case engine::EngineType::FAISS_IVFSQ8:
        case engine::EngineType::FAISS_IVFSQ8H:
            break;
        default:
            return false;
    }

    auto search_job = std::static_pointer_cast<SearchJob>(search_task->job_.lock());
    ResourcePtr res_ptr;
    if (!gpu_enable_) {
        LOG_SERVER_DEBUG_ << LogOut("[%s][%d] FaissIVFPass: gpu disable, specify cpu to search!", "search", 0);
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else if (search_job->nq() < (uint64_t)threshold_) {
        LOG_SERVER_DEBUG_ << LogOut("[%s][%d] FaissIVFPass: nq < gpu_search_threshold, specify cpu to search!",
                                    "search", 0);
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else if (search_job->extra_params()[knowhere::IndexParams::nprobe].get<int64_t>() >
               milvus::server::GPU_QUERY_MAX_NPROBE) {
        LOG_SERVER_DEBUG_ << LogOut("[%s][%d] FaissIVFPass: nprobe > gpu_nprobe_threshold, specify cpu to search!",
                                    "search", 0);
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else {
        LOG_SERVER_DEBUG_ << LogOut("[%s][%d] FaissIVFPass: nq >= gpu_search_threshold, specify gpu %d to search!",
                                    "search", 0, search_gpus_[idx_]);
        res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, search_gpus_[idx_]);
        idx_ = (idx_ + 1) % search_gpus_.size();
    }
    auto label = std::make_shared<SpecResLabel>(res_ptr);
    task->label() = label;
    return true;
}

}  // namespace scheduler
}  // namespace milvus
#endif
