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

#include <faiss/IndexIVFPQ.h>
#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/index_factory.h>

#include <memory>
#include <string>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexGPUIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"

namespace knowhere {

IndexModelPtr
GPUIVFPQ::Train(const DatasetPtr& dataset, const Config& config) {
    GETTENSOR(dataset)
    gpu_id_ = config[knowhere::meta::DEVICEID];

    auto temp_resource = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_);
    if (temp_resource != nullptr) {
        ResScope rs(temp_resource, gpu_id_, true);
        auto device_index = new faiss::gpu::GpuIndexIVFPQ(
            temp_resource->faiss_res.get(), dim, config[IndexParams::nlist].get<int64_t>(), config[IndexParams::m],
            config[IndexParams::nbits],
            GetMetricType(config[Metric::TYPE].get<std::string>()));  // IP not support
        device_index->train(rows, (float*)p_data);
        std::shared_ptr<faiss::Index> host_index = nullptr;
        host_index.reset(faiss::gpu::index_gpu_to_cpu(device_index));
        return std::make_shared<IVFIndexModel>(host_index);
    } else {
        KNOWHERE_THROW_MSG("Build IVFPQ can't get gpu resource");
    }
}

std::shared_ptr<faiss::IVFSearchParameters>
GPUIVFPQ::GenParams(const Config& config) {
    auto params = std::make_shared<faiss::IVFPQSearchParameters>();
    params->nprobe = config[IndexParams::nprobe];
    // params->scan_table_threshold = config["scan_table_threhold"]
    // params->polysemous_ht = config["polysemous_ht"]
    // params->max_codes = config["max_codes"]

    return params;
}

VectorIndexPtr
GPUIVFPQ::CopyGpuToCpu(const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);

    faiss::Index* device_index = index_.get();
    faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IVFPQ>(new_index);
}

}  // namespace knowhere
