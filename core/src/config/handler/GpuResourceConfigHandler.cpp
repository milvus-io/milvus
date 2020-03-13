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
#include "config/handler/GpuResourceConfigHandler.h"

namespace milvus {
namespace server {

GpuResourceConfigHandler::GpuResourceConfigHandler() {
    auto& config = Config::GetInstance();
    config.GetGpuResourceConfigEnable(gpu_enable_);
}

GpuResourceConfigHandler::~GpuResourceConfigHandler() {
    RemoveGpuEnableListener();
    RemoveGpuCacheCapacityListener();
    RemoveGpuBuildResourcesListener();
    RemoveGpuSearchThresholdListener();
    RemoveGpuSearchResourcesListener();
}

//////////////////////////// Hook methods //////////////////////////////////
void
GpuResourceConfigHandler::OnGpuEnableChanged(bool enable) {
    gpu_enable_ = enable;
}

void
GpuResourceConfigHandler::OnGpuCacheCapacityChanged(int64_t capacity) {
    gpu_cache_capacity_ = capacity;
}

void
GpuResourceConfigHandler::OnGpuBuildResChanged(const std::vector<int64_t>& gpus) {
    build_gpus_ = gpus;
}

void
GpuResourceConfigHandler::OnGpuSearchThresholdChanged(int64_t threshold) {
    threshold_ = threshold;
}

void
GpuResourceConfigHandler::OnGpuSearchResChanged(const std::vector<int64_t>& gpus) {
    search_gpus_ = gpus;
}

//////////////////////////// Listener methods //////////////////////////////////
void
GpuResourceConfigHandler::AddGpuEnableListener() {
    auto& config = Config::GetInstance();

    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        bool enable;
        auto status = config.GetGpuResourceConfigEnable(enable);
        if (status.ok()) {
            OnGpuEnableChanged(enable);
        }

        return status;
    };
    config.RegisterCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_ENABLE, identity_, lambda);
}

void
GpuResourceConfigHandler::AddGpuCacheCapacityListener() {
    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        int64_t capacity = 1;
        auto status = config.GetGpuResourceConfigCacheCapacity(capacity);
        if (status.ok()) {
            OnGpuCacheCapacityChanged(capacity);
        }

        return status;
    };

    auto& config = Config::GetInstance();
    config.RegisterCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_CACHE_CAPACITY, identity_, lambda);
}

void
GpuResourceConfigHandler::AddGpuBuildResourcesListener() {
    auto& config = Config::GetInstance();
    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        std::vector<int64_t> gpu_ids;
        auto status = config.GetGpuResourceConfigSearchResources(gpu_ids);
        if (status.ok()) {
            OnGpuBuildResChanged(gpu_ids);
        }

        return status;
    };
    config.RegisterCallBack(CONFIG_GPU_RESOURCE,CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, identity_,lambda);
}

void
GpuResourceConfigHandler::AddGpuSearchThresholdListener() {
    auto& config = Config::GetInstance();

    ConfigCallBackF lambda_gpu_threshold = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        int64_t threshold;
        auto status = config.GetEngineConfigGpuSearchThreshold(threshold);
        if (status.ok()) {
            OnGpuSearchThresholdChanged(threshold);
        }

        return status;
    };
    config.RegisterCallBack(CONFIG_ENGINE, CONFIG_ENGINE_GPU_SEARCH_THRESHOLD, identity_,
                            lambda_gpu_threshold);
}

void
GpuResourceConfigHandler::AddGpuSearchResourcesListener() {
    auto& config = Config::GetInstance();

    ConfigCallBackF lambda_gpu_search_res = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        std::vector<int64_t> gpu_ids;
        auto status = config.GetGpuResourceConfigSearchResources(gpu_ids);
        if (status.ok()) {
            OnGpuSearchResChanged(gpu_ids);
        }

        return status;
    };
    config.RegisterCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_SEARCH_RESOURCES, identity_,
                            lambda_gpu_search_res);
}

void
GpuResourceConfigHandler::RemoveGpuEnableListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_ENABLE, identity_);
}

void
GpuResourceConfigHandler::RemoveGpuCacheCapacityListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_CACHE_CAPACITY, identity_);
}

void
GpuResourceConfigHandler::RemoveGpuBuildResourcesListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, identity_);
}

void
GpuResourceConfigHandler::RemoveGpuSearchThresholdListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_ENGINE, CONFIG_ENGINE_GPU_SEARCH_THRESHOLD, identity_);
}

void
GpuResourceConfigHandler::RemoveGpuSearchResourcesListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_SEARCH_RESOURCES, identity_);
}

}  // namespace server
}  // namespace milvus
#endif
