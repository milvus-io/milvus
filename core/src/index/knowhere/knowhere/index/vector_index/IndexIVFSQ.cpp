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
#include <faiss/gpu/GpuAutoTune.h>
#include <faiss/gpu/GpuCloner.h>
#endif
#include <faiss/index_factory.h>

#include <memory>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"
#endif

namespace knowhere {

IndexModelPtr
IVFSQ::Train(const DatasetPtr& dataset, const Config& config) {
    auto build_cfg = std::dynamic_pointer_cast<IVFSQCfg>(config);
    if (build_cfg != nullptr) {
        build_cfg->CheckValid();  // throw exception
    }

    GETTENSOR(dataset)

    std::stringstream index_type;
    index_type << "IVF" << build_cfg->nlist << ","
               << "SQ" << build_cfg->nbits;
    auto build_index = faiss::index_factory(dim, index_type.str().c_str(), GetMetricType(build_cfg->metric_type));
    build_index->train(rows, (float*)p_data);

    std::shared_ptr<faiss::Index> ret_index;
    ret_index.reset(build_index);
    return std::make_shared<IVFIndexModel>(ret_index);
}

// VectorIndexPtr
// IVFSQ::Clone_impl(const std::shared_ptr<faiss::Index>& index) {
//    return std::make_shared<IVFSQ>(index);
//}

VectorIndexPtr
IVFSQ::CopyCpuToGpu(const int64_t& device_id, const Config& config) {
#ifdef MILVUS_GPU_VERSION

    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), device_id, index_.get());

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        return std::make_shared<GPUIVFSQ>(device_index, device_id, res);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }

#else
    KNOWHERE_THROW_MSG("Calling IVFSQ::CopyCpuToGpu when we are using CPU version");
#endif
}

}  // namespace knowhere
