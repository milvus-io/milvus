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
#pragma once

#include <exception>
#include <limits>
#include <string>
#include <vector>

#include "config/Config.h"
#include "config/handler/ConfigHandler.h"

namespace milvus {
namespace server {

class GpuResourceConfigHandler : virtual public ConfigHandler {
 public:
    GpuResourceConfigHandler();

    virtual ~GpuResourceConfigHandler();

 protected:
    virtual void
    OnGpuEnableChanged(bool enable) {
    }

    virtual void
    OnGpuCacheCapacityChanged(int64_t capacity) {
    }

    virtual void
    OnGpuBuildResChanged(const std::vector<int64_t>& gpus) {
    }

    virtual void
    OnGpuSearchThresholdChanged(int64_t threshold) {
    }

    virtual void
    OnGpuSearchResChanged(const std::vector<int64_t>& gpus) {
    }

 protected:
    void
    AddGpuEnableListener();

    void
    AddGpuCacheCapacityListener();

    void
    AddGpuBuildResourcesListener();

    void
    AddGpuSearchThresholdListener();

    void
    AddGpuSearchResourcesListener();

 protected:
    void
    RemoveGpuEnableListener();

    void
    RemoveGpuCacheCapacityListener();

    void
    RemoveGpuBuildResourcesListener();

    void
    RemoveGpuSearchThresholdListener();

    void
    RemoveGpuSearchResourcesListener();

 protected:
    bool gpu_enable_ = true;
    int64_t gpu_cache_capacity_ = std::stoll(CONFIG_GPU_RESOURCE_CACHE_CAPACITY_DEFAULT) /* GiB */;
    int64_t threshold_ = std::stoll(CONFIG_ENGINE_USE_BLAS_THRESHOLD_DEFAULT);
    std::vector<int64_t> build_gpus_;
    std::vector<int64_t> search_gpus_;
};

}  // namespace server
}  // namespace milvus
#endif
