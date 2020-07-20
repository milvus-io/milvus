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
#include <faiss/index_factory.h>

#include <memory>
#include <string>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQNR.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "knowhere/index/vector_offset_index/IndexIVFSQNR_NM.h"

namespace milvus {
namespace knowhere {

void
GPUIVFSQNR::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)
    gpu_id_ = config[knowhere::meta::DEVICEID];

    // std::stringstream index_type;
    // index_type << "IVF" << config[IndexParams::nlist] << ","
    //            << "SQ" << config[IndexParams::nbits];
    // faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    // auto build_index = faiss::index_factory(dim, index_type.str().c_str(), metric_type);

    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    faiss::Index* coarse_quantizer = new faiss::IndexFlat(dim, metric_type);
    auto build_index =
        new faiss::IndexIVFScalarQuantizer(coarse_quantizer, dim, config[IndexParams::nlist].get<int64_t>(),
                                           faiss::QuantizerType::QT_8bit, metric_type, false);

    auto gpu_res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_);
    if (gpu_res != nullptr) {
        ResScope rs(gpu_res, gpu_id_, true);
        auto device_index = faiss::gpu::index_cpu_to_gpu(gpu_res->faiss_res.get(), gpu_id_, build_index);
        device_index->train(rows, (float*)p_data);

        index_.reset(device_index);
        res_ = gpu_res;
    } else {
        KNOWHERE_THROW_MSG("Build IVFSQ can't get gpu resource");
    }
}

VecIndexPtr
GPUIVFSQNR::CopyGpuToCpu(const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);

    faiss::Index* device_index = index_.get();
    faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IVFSQNR_NM>(new_index);
}

}  // namespace knowhere
}  // namespace milvus
