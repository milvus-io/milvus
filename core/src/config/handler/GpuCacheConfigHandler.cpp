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

#include "config/handler/GpuCacheConfigHandler.h"

#include <string>

#include "config/Config.h"

namespace milvus {
namespace server {

GpuCacheConfigHandler::GpuCacheConfigHandler() {
    server::Config& config = server::Config::GetInstance();
    config.GetGpuResourceConfigCacheCapacity(gpu_cache_capacity_);
}

GpuCacheConfigHandler::~GpuCacheConfigHandler() {
    RemoveGpuCacheCapacityListener();
}

void
GpuCacheConfigHandler::OnGpuCacheCapacityChanged(int64_t capacity) {
    gpu_cache_capacity_ = capacity;
}

void
GpuCacheConfigHandler::AddGpuCacheCapacityListener() {
    server::ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = server::Config::GetInstance();
        int64_t capacity = 1;
        auto status = config.GetGpuResourceConfigCacheCapacity(capacity);
        if (status.ok()) {
            OnGpuCacheCapacityChanged(capacity);
        }

        return status;
    };

    auto& config = server::Config::GetInstance();
    config.RegisterCallBack(server::CONFIG_GPU_RESOURCE, server::CONFIG_GPU_RESOURCE_CACHE_CAPACITY, identity_, lambda);
}

void
GpuCacheConfigHandler::RemoveGpuCacheCapacityListener() {
    auto& config = server::Config::GetInstance();
    config.CancelCallBack(server::CONFIG_GPU_RESOURCE, server::CONFIG_GPU_RESOURCE_CACHE_CAPACITY, identity_);
}

}  // namespace server
}  // namespace milvus

#endif
