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

//////////////////////////// Listener methods //////////////////////////////////
void
GpuResourceConfigHandler::AddGpuEnableListener() {
    auto& config = Config::GetInstance();

    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        auto status = config.GetGpuResourceConfigEnable(gpu_enable_);
        if (status.ok()) {
            OnGpuEnableChanged(gpu_enable_);
        }

        return status;
    };
    config.RegisterCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_ENABLE, identity_, lambda);
}

void
GpuResourceConfigHandler::AddGpuCacheCapacityListener() {
    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        if (!gpu_enable_) {
            std::string msg =
                std::string("GPU resources is disable. Cannot set config ") + CONFIG_GPU_RESOURCE_CACHE_CAPACITY;
            return Status(SERVER_UNEXPECTED_ERROR, msg);
        }

        auto& config = Config::GetInstance();
        auto status = config.GetGpuResourceConfigCacheCapacity(gpu_cache_capacity_);
        if (status.ok()) {
            OnGpuCacheCapacityChanged(gpu_cache_capacity_);
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
        if (!gpu_enable_) {
            std::string msg =
                std::string("GPU resources is disable. Cannot set config ") + CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES;
            return Status(SERVER_UNEXPECTED_ERROR, msg);
        }

        auto& config = Config::GetInstance();
        auto status = config.GetGpuResourceConfigSearchResources(build_gpus_);
        if (status.ok()) {
            OnGpuBuildResChanged(build_gpus_);
        }

        return status;
    };
    config.RegisterCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, identity_, lambda);
}

void
GpuResourceConfigHandler::AddGpuSearchThresholdListener() {
    auto& config = Config::GetInstance();

    ConfigCallBackF lambda_gpu_threshold = [this](const std::string& value) -> Status {
        if (!gpu_enable_) {
            std::string msg =
                std::string("GPU resources is disabled. Cannot set config ") + CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD;
            return Status(SERVER_UNEXPECTED_ERROR, msg);
        }

        auto& config = Config::GetInstance();
        auto status = config.GetGpuResourceConfigGpuSearchThreshold(threshold_);
        if (status.ok()) {
            OnGpuSearchThresholdChanged(threshold_);
        }

        return status;
    };
    config.RegisterCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD, identity_,
                            lambda_gpu_threshold);
}

void
GpuResourceConfigHandler::AddGpuSearchResourcesListener() {
    auto& config = Config::GetInstance();

    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        if (!gpu_enable_) {
            std::string msg =
                std::string("GPU resources is disable. Cannot set config ") + CONFIG_GPU_RESOURCE_SEARCH_RESOURCES;
            return Status(SERVER_UNEXPECTED_ERROR, msg);
        }

        auto& config = Config::GetInstance();
        auto status = config.GetGpuResourceConfigSearchResources(search_gpus_);
        if (status.ok()) {
            OnGpuSearchResChanged(search_gpus_);
        }

        return status;
    };
    config.RegisterCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_SEARCH_RESOURCES, identity_, lambda);
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
    config.CancelCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD, identity_);
}

void
GpuResourceConfigHandler::RemoveGpuSearchResourcesListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_GPU_RESOURCE, CONFIG_GPU_RESOURCE_SEARCH_RESOURCES, identity_);
}

}  // namespace server
}  // namespace milvus
#endif
