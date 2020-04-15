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

#include "index/archive/KnowhereResource.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

#include "config/Config.h"
#include "faiss/FaissHook.h"
#include "scheduler/Utils.h"
#include "utils/Error.h"
#include "utils/Log.h"

#include <fiu-local.h>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {

constexpr int64_t M_BYTE = 1024 * 1024;

Status
KnowhereResource::Initialize() {
    server::Config& config = server::Config::GetInstance();
    bool use_avx512 = true;
    CONFIG_CHECK(config.GetEngineConfigUseAVX512(use_avx512));
    faiss::faiss_use_avx512 = use_avx512;
    std::string cpu_flag;
    if (faiss::hook_init(cpu_flag)) {
        LOG_ENGINE_DEBUG_ << "FAISS hook " << cpu_flag;
    } else {
        return Status(KNOWHERE_UNEXPECTED_ERROR, "FAISS hook fail, CPU not supported!");
    }

#ifdef MILVUS_GPU_VERSION
    bool enable_gpu = false;
    CONFIG_CHECK(config.GetGpuResourceConfigEnable(enable_gpu));
    fiu_do_on("KnowhereResource.Initialize.disable_gpu", enable_gpu = false);
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
    CONFIG_CHECK(config.GetGpuResourceConfigBuildIndexResources(build_index_gpus));

    for (auto gpu_id : build_index_gpus) {
        gpu_resources.insert(std::make_pair(gpu_id, GpuResourceSetting()));
    }

    // get search gpu resource
    std::vector<int64_t> search_gpus;
    CONFIG_CHECK(config.GetGpuResourceConfigSearchResources(search_gpus));

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
