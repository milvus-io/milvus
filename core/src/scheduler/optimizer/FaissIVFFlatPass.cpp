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
#include "scheduler/optimizer/FaissIVFFlatPass.h"
#include "cache/GpuCacheMgr.h"
#include "scheduler/SchedInst.h"
#include "scheduler/Utils.h"
#include "scheduler/task/SearchTask.h"
#include "scheduler/tasklabel/SpecResLabel.h"
#include "server/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace scheduler {

FaissIVFFlatPass::~FaissIVFFlatPass() {
    server::Config& config = server::Config::GetInstance();

    config.CancelCallBack(server::CONFIG_GPU_RESOURCE, server::CONFIG_GPU_RESOURCE_ENABLE, identity_);
    config.CancelCallBack(server::CONFIG_ENGINE, server::CONFIG_ENGINE_GPU_SEARCH_THRESHOLD, identity_);
    config.CancelCallBack(server::CONFIG_GPU_RESOURCE, server::CONFIG_GPU_RESOURCE_SEARCH_RESOURCES, identity_);
}

void
FaissIVFFlatPass::Init() {
#ifdef MILVUS_GPU_VERSION
    server::Config& config = server::Config::GetInstance();

    config.GenUniqueIdentityID("FaissIVFFlatPass", identity_);

    config.GetGpuResourceConfigEnable(gpu_enable_);
    server::ConfigCallBackF lambda_gpu_enable = [this](const std::string& value) -> Status {
        server::Config& config = server::Config::GetInstance();
        return config.GetGpuResourceConfigEnable(this->gpu_enable_);
    };
    config.RegisterCallBack(server::CONFIG_GPU_RESOURCE, server::CONFIG_GPU_RESOURCE_ENABLE, identity_,
                            lambda_gpu_enable);

    Status s = config.GetEngineConfigGpuSearchThreshold(threshold_);
    if (!s.ok()) {
        threshold_ = std::numeric_limits<int32_t>::max();
    }
    server::ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        server::Config& config = server::Config::GetInstance();
        int64_t threshold;
        auto status = config.GetEngineConfigGpuSearchThreshold(threshold);
        if (status.ok()) {
            this->threshold_ = threshold;
        }

        return status;
    };
    config.RegisterCallBack(server::CONFIG_ENGINE, server::CONFIG_ENGINE_GPU_SEARCH_THRESHOLD, identity_, lambda);

    s = config.GetGpuResourceConfigSearchResources(gpus);
    if (!s.ok()) {
        throw std::exception();
    }
    server::ConfigCallBackF lambda_gpu_search_res = [this](const std::string& value) -> Status {
        server::Config& config = server::Config::GetInstance();
        std::vector<int64_t> gpu_ids;
        auto status = config.GetGpuResourceConfigSearchResources(gpu_ids);
        if (status.ok()) {
            this->gpus = gpu_ids;
        }

        return status;
    };
    config.RegisterCallBack(server::CONFIG_GPU_RESOURCE, server::CONFIG_GPU_RESOURCE_SEARCH_RESOURCES, identity_,
                            lambda_gpu_search_res);
#endif
}

bool
FaissIVFFlatPass::Run(const TaskPtr& task) {
    if (task->Type() != TaskType::SearchTask) {
        return false;
    }

    auto search_task = std::static_pointer_cast<XSearchTask>(task);
    if (search_task->file_->engine_type_ != (int)engine::EngineType::FAISS_IVFFLAT) {
        return false;
    }

    auto search_job = std::static_pointer_cast<SearchJob>(search_task->job_.lock());
    ResourcePtr res_ptr;
    if (!gpu_enable_) {
        SERVER_LOG_DEBUG << "FaissIVFFlatPass: gpu disable, specify cpu to search!";
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else if (search_job->nq() < threshold_) {
        SERVER_LOG_DEBUG << "FaissIVFFlatPass: nq < gpu_search_threshold, specify cpu to search!";
        res_ptr = ResMgrInst::GetInstance()->GetResource("cpu");
    } else {
        auto best_device_id = count_ % gpus.size();
        SERVER_LOG_DEBUG << "FaissIVFFlatPass: nq > gpu_search_threshold, specify gpu" << best_device_id
                         << " to search!";
        count_++;
        res_ptr = ResMgrInst::GetInstance()->GetResource(ResourceType::GPU, gpus[best_device_id]);
    }
    auto label = std::make_shared<SpecResLabel>(res_ptr);
    task->label() = label;
    return true;
}

}  // namespace scheduler
}  // namespace milvus
#endif
