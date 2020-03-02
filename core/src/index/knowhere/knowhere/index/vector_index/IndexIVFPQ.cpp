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

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>
#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuCloner.h>
#endif

#include <memory>
#include <string>
#include <utility>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexGPUIVFPQ.h"
#endif
#include "knowhere/index/vector_index/IndexIVFPQ.h"

namespace knowhere {

IndexModelPtr
IVFPQ::Train(const DatasetPtr& dataset, const Config& config) {
    GETTENSOR(dataset)

    faiss::Index* coarse_quantizer = new faiss::IndexFlat(dim, GetMetricType(config[Metric::TYPE].get<std::string>()));
    auto index = std::make_shared<faiss::IndexIVFPQ>(coarse_quantizer, dim, config[IndexParams::nlist].get<int64_t>(),
                                                     config[IndexParams::m].get<int64_t>(),
                                                     config[IndexParams::nbits].get<int64_t>());
    index->train(rows, (float*)p_data);

    return std::make_shared<IVFIndexModel>(index);
}

std::shared_ptr<faiss::IVFSearchParameters>
IVFPQ::GenParams(const Config& config) {
    auto params = std::make_shared<faiss::IVFPQSearchParameters>();
    params->nprobe = config[IndexParams::nprobe];
    // params->scan_table_threshold = config["scan_table_threhold"]
    // params->polysemous_ht = config["polysemous_ht"]
    // params->max_codes = config["max_codes"]

    return params;
}

// VectorIndexPtr
// IVFPQ::Clone_impl(const std::shared_ptr<faiss::Index>& index) {
//    return std::make_shared<IVFPQ>(index);
//}

VectorIndexPtr
IVFPQ::CopyCpuToGpu(const int64_t& device_id, const Config& config) {
#ifdef MILVUS_GPU_VERSION
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);
        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), device_id, index_.get());

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        return std::make_shared<GPUIVFPQ>(device_index, device_id, res);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
#else
    KNOWHERE_THROW_MSG("Calling IVFPQ::CopyCpuToGpu when we are using CPU version");
#endif
}

}  // namespace knowhere
