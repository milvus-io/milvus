// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "wrapper/KnowhereResource.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "scheduler/Utils.h"
#include "server/Config.h"

namespace milvus {
namespace engine {

constexpr int64_t M_BYTE = 1024 * 1024;

Status
KnowhereResource::Initialize() {
#ifdef MILVUS_GPU_VERSION
    Status s;
    bool enable_gpu = false;
    server::Config& config = server::Config::GetInstance();
    s = config.GetGpuResourceConfigEnable(enable_gpu);
    if (!s.ok())
        return s;

    if (not enable_gpu)
        return Status::OK();

    struct GpuResourceSetting {
        int64_t pinned_memory = 300 * M_BYTE;
        int64_t temp_memory = 300 * M_BYTE;
        int64_t resource_num = 2;
    };
    using GpuResourcesArray = std::map<int64_t, GpuResourceSetting>;
    GpuResourcesArray gpu_resources;

    // get build index gpu resource
    std::vector<int64_t> build_index_gpus;
    s = config.GetGpuResourceConfigBuildIndexResources(build_index_gpus);
    if (!s.ok())
        return s;

    for (auto gpu_id : build_index_gpus) {
        gpu_resources.insert(std::make_pair(gpu_id, GpuResourceSetting()));
    }

    // get search gpu resource
    std::vector<int64_t> search_gpus;
    s = config.GetGpuResourceConfigSearchResources(search_gpus);
    if (!s.ok())
        return s;

    for (auto& gpu_id : search_gpus) {
        gpu_resources.insert(std::make_pair(gpu_id, GpuResourceSetting()));
    }

    // init gpu resources
    for (auto iter = gpu_resources.begin(); iter != gpu_resources.end(); ++iter) {
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(iter->first, iter->second.pinned_memory,
                                                                iter->second.temp_memory, iter->second.resource_num);
    }

#endif

    return Status::OK();
}

Status
KnowhereResource::Finalize() {
#ifdef MILVUS_GPU_VERSION
    knowhere::FaissGpuResourceMgr::GetInstance().Free();  // free gpu resource.
#endif
    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
