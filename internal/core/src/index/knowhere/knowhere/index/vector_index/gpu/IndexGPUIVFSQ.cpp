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

#include <faiss/IndexFlat.h>
#include <faiss/IndexScalarQuantizer.h>
#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/GpuIndexIVFScalarQuantizer.h>

#include <memory>
#include <string>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

void
GPUIVFSQ::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)
    gpu_id_ = config[knowhere::meta::DEVICEID];
    auto gpu_res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_);
    if (gpu_res != nullptr) {
        ResScope rs(gpu_res, gpu_id_, true);
        faiss::gpu::GpuIndexIVFScalarQuantizerConfig idx_config;
        idx_config.device = static_cast<int32_t>(gpu_id_);
        int32_t nlist = config[IndexParams::nlist];
        faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
        index_ = std::make_shared<faiss::gpu::GpuIndexIVFScalarQuantizer>(
            gpu_res->faiss_res.get(), dim, nlist, faiss::QuantizerType::QT_8bit, metric_type, true, idx_config);
        index_->train(rows, (float*)p_data);
        res_ = gpu_res;
    } else {
        KNOWHERE_THROW_MSG("Build IVFSQ can't get gpu resource");
    }
}

VecIndexPtr
GPUIVFSQ::CopyGpuToCpu(const Config& config) {
    faiss::Index* device_index = index_.get();
    faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IVFSQ>(new_index);
}

}  // namespace knowhere
}  // namespace milvus
