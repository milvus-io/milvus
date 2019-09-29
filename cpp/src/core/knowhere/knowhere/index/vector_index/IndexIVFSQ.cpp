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

#include <faiss/gpu/GpuAutoTune.h>
#include <memory>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"

namespace zilliz {
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

VectorIndexPtr
IVFSQ::Clone_impl(const std::shared_ptr<faiss::Index>& index) {
    return std::make_shared<IVFSQ>(index);
}

VectorIndexPtr
IVFSQ::CopyCpuToGpu(const int64_t& device_id, const Config& config) {
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), device_id, index_.get(), &option);

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        return std::make_shared<GPUIVFSQ>(device_index, device_id, res);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
}

}  // namespace knowhere
}  // namespace zilliz
