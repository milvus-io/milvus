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
#include <faiss/Clustering.h>
#include <faiss/utils/distances.h>

#include "NGT/lib/NGT/defines.h"
#include "faiss/FaissHook.h"
#include "faiss/common.h"
#include "faiss/utils/utils.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/IndexHNSW.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"
#include "utils/ConfigUtils.h"
#include "utils/Error.h"
#include "utils/Log.h"

#include <fiu/fiu-local.h>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace milvus {
namespace engine {

constexpr int64_t M_BYTE = 1024 * 1024;

Status
KnowhereResource::Finalize() {
#ifdef MILVUS_GPU_VERSION
    knowhere::FaissGpuResourceMgr::GetInstance().Free();  // free gpu resource.
#endif
    return Status::OK();
}

void
KnowhereResource::SetSimdType(const int64_t simd_type) {
    if (simd_type == SimdType::AVX512) {
        faiss::faiss_use_avx512 = true;
        faiss::faiss_use_avx2 = false;
        faiss::faiss_use_sse = false;
    } else if (simd_type == SimdType::AVX2) {
        faiss::faiss_use_avx512 = false;
        faiss::faiss_use_avx2 = true;
        faiss::faiss_use_sse = false;
    } else if (simd_type == SimdType::SSE) {
        faiss::faiss_use_avx512 = false;
        faiss::faiss_use_avx2 = false;
        faiss::faiss_use_sse = true;
    } else {
        faiss::faiss_use_avx512 = true;
        faiss::faiss_use_avx2 = true;
        faiss::faiss_use_sse = true;
    }
}

Status
KnowhereResource::FaissHook() {
    std::string cpu_flag;
    if (faiss::hook_init(cpu_flag)) {
        std::cout << "FAISS hook " << cpu_flag << std::endl;
        LOG_ENGINE_DEBUG_ << "FAISS hook " << cpu_flag;
        return Status::OK();
    } else {
        return Status(KNOWHERE_UNEXPECTED_ERROR, "FAISS hook fail, CPU not supported!");
    }
}

void
KnowhereResource::SetBlasThreshold(const int64_t use_blas_threshold) {
    faiss::distance_compute_blas_threshold = (int)use_blas_threshold;
}

void
KnowhereResource::SetEarlyStopThreshold(const double early_stop_threshold) {
    faiss::early_stop_threshold = early_stop_threshold;
}

void
KnowhereResource::SetClusteringType(const int64_t clustering_type) {
    switch (clustering_type) {
        case ClusteringType::K_MEANS:
        default:
            faiss::clustering_type = faiss::ClusteringType::K_MEANS;
            break;
        case ClusteringType::K_MEANS_PLUS_PLUS:
            faiss::clustering_type = faiss::ClusteringType::K_MEANS_PLUS_PLUS;
            break;
    }
}

void
KnowhereResource::SetStatisticsLevel(const int64_t stat_level) {
    milvus::knowhere::STATISTICS_LEVEL = stat_level;
    faiss::STATISTICS_LEVEL = stat_level;
}

void
KnowhereResource::SetLogHandler() {
    faiss::LOG_ERROR_ = &knowhere::log_error_;
    faiss::LOG_WARNING_ = &knowhere::log_warning_;
    // faiss::LOG_DEBUG_ = &knowhere::log_debug_;
    NGT_LOG_ERROR_ = &knowhere::log_error_;
    NGT_LOG_WARNING_ = &knowhere::log_warning_;
    // NGT_LOG_DEBUG_ = &knowhere::log_debug_;
}

void
KnowhereResource::SetGPUEnable(bool enable_gpu) {
    fiu_do_on("KnowhereResource.Initialize.disable_gpu", enable_gpu = false);
    if (!enable_gpu) {
        return;
    }
#ifdef MILVUS_GPU_VERSION
    struct GpuResourceSetting {
        int64_t pinned_memory = 256 * M_BYTE;
        int64_t temp_memory = 256 * M_BYTE;
        int64_t resource_num = 2;
    };
    using GpuResourcesArray = std::map<int64_t, GpuResourceSetting>;
    GpuResourcesArray gpu_resources;

    // get build index gpu resource
    std::vector<int64_t> build_index_gpus = ParseGPUDevices(config.gpu.build_index_devices());

    for (auto gpu_id : build_index_gpus) {
        gpu_resources.insert(std::make_pair(gpu_id, GpuResourceSetting()));
    }

    // get search gpu resource
    std::vector<int64_t> search_gpus = ParseGPUDevices(config.gpu.search_devices());

    for (auto& gpu_id : search_gpus) {
        gpu_resources.insert(std::make_pair(gpu_id, GpuResourceSetting()));
    }

    // init gpu resources
    for (auto& gpu_resource : gpu_resources) {
        knowhere::FaissGpuResourceMgr::GetInstance().InitDevice(gpu_resource.first, gpu_resource.second.pinned_memory,
                                                                gpu_resource.second.temp_memory,
                                                                gpu_resource.second.resource_num);
    }
#endif
}

}  // namespace engine
}  // namespace milvus
