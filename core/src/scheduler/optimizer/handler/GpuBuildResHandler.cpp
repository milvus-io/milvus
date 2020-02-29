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
#include "scheduler/optimizer/handler/GpuBuildResHandler.h"

#include <string>
#include <vector>

namespace milvus {
namespace scheduler {

GpuBuildResHandler::GpuBuildResHandler() {
    server::Config& config = server::Config::GetInstance();
    config.GetGpuResourceConfigBuildIndexResources(build_gpus_);
}

GpuBuildResHandler::~GpuBuildResHandler() {
    server::Config& config = server::Config::GetInstance();
    config.CancelCallBack(server::CONFIG_GPU_RESOURCE, server::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, identity_);
}

////////////////////////////////////////////////////////////////
void
GpuBuildResHandler::OnGpuBuildResChanged(const std::vector<int64_t>& gpus) {
    build_gpus_ = gpus;
}

void
GpuBuildResHandler::AddGpuBuildResListener() {
    server::Config& config = server::Config::GetInstance();
    server::ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        server::Config& config = server::Config::GetInstance();
        std::vector<int64_t> gpu_ids;
        auto status = config.GetGpuResourceConfigSearchResources(gpu_ids);
        if (status.ok()) {
            OnGpuBuildResChanged(gpu_ids);
        }

        return status;
    };
    config.RegisterCallBack(server::CONFIG_GPU_RESOURCE, server::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, identity_,
                            lambda);
}

}  // namespace scheduler
}  // namespace milvus
#endif
