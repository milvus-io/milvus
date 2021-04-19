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

#include <string>

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/clone_index.h>
#ifdef MILVUS_GPU_VERSION
#include <faiss/gpu/GpuCloner.h>
#endif

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/ConfAdapter.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVFPQ.h"
#endif

namespace milvus {
namespace knowhere {

void
IVFPQ::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)

    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    faiss::Index* coarse_quantizer = new faiss::IndexFlat(dim, metric_type);
    auto index = std::make_shared<faiss::IndexIVFPQ>(coarse_quantizer, dim, config[IndexParams::nlist].get<int64_t>(),
                                                     config[IndexParams::m].get<int64_t>(),
                                                     config[IndexParams::nbits].get<int64_t>(), metric_type);
    index->own_fields = true;
    index->train(rows, reinterpret_cast<const float*>(p_data));
    index_ = index;
}

VecIndexPtr
IVFPQ::CopyCpuToGpu(const int64_t device_id, const Config& config) {
#ifdef MILVUS_GPU_VERSION
    auto ivfpq_index = dynamic_cast<faiss::IndexIVFPQ*>(index_.get());
    int64_t dim = ivfpq_index->d;
    int64_t m = ivfpq_index->pq.M;
    int64_t nbits = ivfpq_index->pq.nbits;
    if (!IVFPQConfAdapter::CheckGPUPQParams(dim, m, nbits)) {
        return nullptr;
    }
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

std::shared_ptr<faiss::IVFSearchParameters>
IVFPQ::GenParams(const Config& config) {
    auto params = std::make_shared<faiss::IVFPQSearchParameters>();
    params->nprobe = config[IndexParams::nprobe];
    // params->scan_table_threshold = config["scan_table_threhold"]
    // params->polysemous_ht = config["polysemous_ht"]
    // params->max_codes = config["max_codes"]

    return params;
}

void
IVFPQ::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    auto ivfpq_index = dynamic_cast<faiss::IndexIVFPQ*>(index_.get());
    auto nb = ivfpq_index->invlists->compute_ntotal();
    auto code_size = ivfpq_index->code_size;
    auto pq = ivfpq_index->pq;
    auto nlist = ivfpq_index->nlist;
    auto d = ivfpq_index->d;

    // ivf codes, ivf ids and quantizer
    auto capacity = nb * code_size + nb * sizeof(int64_t) + nlist * d * sizeof(float);
    auto centroid_table = pq.M * pq.ksub * pq.dsub * sizeof(float);
    auto precomputed_table = nlist * pq.M * pq.ksub * sizeof(float);
    if (precomputed_table > ivfpq_index->precomputed_table_max_bytes) {
        // will not precompute table
        precomputed_table = 0;
    }
    index_size_ = capacity + centroid_table + precomputed_table;
}

}  // namespace knowhere
}  // namespace milvus
