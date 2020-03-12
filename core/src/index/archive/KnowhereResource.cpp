// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "knowhere/KnowhereResource.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

#include "scheduler/Utils.h"
#include "server/Config.h"

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

// namespace milvus {
namespace knowhere {

constexpr int64_t M_BYTE = 1024 * 1024;

void
KnowhereResource::Initialize() {
#ifdef MILVUS_GPU_VERSION
    bool enable_gpu = false;
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    config.GetGpuResourceConfigEnable(enable_gpu);
    if (not enable_gpu) {
        return;
    }

    struct GpuResourceSetting {
        int64_t pinned_memory = 300 * M_BYTE;
        int64_t temp_memory = 300 * M_BYTE;
        int64_t resource_num = 2;
    };
    using GpuResourcesArray = std::map<int64_t, GpuResourceSetting>;
    GpuResourcesArray gpu_resources;

    // get build index gpu resource
    std::vector<int64_t> build_index_gpus;
    config.GetGpuResourceConfigBuildIndexResources(build_index_gpus);
    for (auto gpu_id : build_index_gpus) {
        gpu_resources.insert(std::make_pair(gpu_id, GpuResourceSetting()));
    }

    // get search gpu resource
    std::vector<int64_t> search_gpus;
    config.GetGpuResourceConfigSearchResources(search_gpus);
    for (auto& gpu_id : search_gpus) {
        gpu_resources.insert(std::make_pair(gpu_id, GpuResourceSetting()));
    }

    // init gpu resources
    for (auto iter = gpu_resources.begin(); iter != gpu_resources.end(); ++iter) {
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(iter->first, iter->second.pinned_memory,
                                                                iter->second.temp_memory, iter->second.resource_num);
    }
#endif
}

void
KnowhereResource::Finalize() {
#ifdef MILVUS_GPU_VERSION
    knowhere::FaissGpuResourceMgr::GetInstance().Free();  // free gpu resource.
#endif
}

}  // namespace knowhere
//}  // namespace milvus
