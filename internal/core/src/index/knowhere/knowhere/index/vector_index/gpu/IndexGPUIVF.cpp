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

#include <algorithm>
#include <memory>

#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/GpuIndexIVF.h>
#include <faiss/gpu/GpuIndexIVFFlat.h>
#include <faiss/index_io.h>
#include <fiu/fiu-local.h>
#include <string>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

void
GPUIVF::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)
    gpu_id_ = config[knowhere::meta::DEVICEID];

    auto gpu_res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_);
    if (gpu_res != nullptr) {
        ResScope rs(gpu_res, gpu_id_, true);
        faiss::gpu::GpuIndexIVFFlatConfig idx_config;
        idx_config.device = static_cast<int32_t>(gpu_id_);
        int32_t nlist = config[IndexParams::nlist];
        faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
        index_ = std::make_shared<faiss::gpu::GpuIndexIVFFlat>(gpu_res->faiss_res.get(), dim, nlist, metric_type,
                                                               idx_config);
        index_->train(rows, reinterpret_cast<const float*>(p_data));
        res_ = gpu_res;
    } else {
        KNOWHERE_THROW_MSG("Build IVF can't get gpu resource");
    }
}

void
GPUIVF::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (auto spt = res_.lock()) {
        ResScope rs(res_, gpu_id_);
        IVF::AddWithoutIds(dataset_ptr, config);
    } else {
        KNOWHERE_THROW_MSG("Add IVF can't get gpu resource");
    }
}

VecIndexPtr
GPUIVF::CopyGpuToCpu(const Config& config) {
    auto device_idx = std::dynamic_pointer_cast<faiss::gpu::GpuIndexIVF>(index_);
    if (device_idx != nullptr) {
        faiss::Index* device_index = index_.get();
        faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

        std::shared_ptr<faiss::Index> new_index;
        new_index.reset(host_index);
        return std::make_shared<IVF>(new_index);
    } else {
        return std::make_shared<IVF>(index_);
    }
}

VecIndexPtr
GPUIVF::CopyGpuToGpu(const int64_t device_id, const Config& config) {
    auto host_index = CopyGpuToCpu(config);
    return std::static_pointer_cast<IVF>(host_index)->CopyCpuToGpu(device_id, config);
}

BinarySet
GPUIVF::SerializeImpl(const IndexType& type) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        fiu_do_on("GPUIVF.SerializeImpl.throw_exception", throw std::exception());
        MemoryIOWriter writer;
        {
            faiss::Index* index = index_.get();
            faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(index);

            faiss::write_index(host_index, &writer);
            delete host_index;
        }
        std::shared_ptr<uint8_t[]> data(writer.data_);

        BinarySet res_set;
        res_set.Append("IVF", data, writer.rp);

        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
GPUIVF::LoadImpl(const BinarySet& binary_set, const IndexType& type) {
    auto binary = binary_set.GetByName("IVF");
    MemoryIOReader reader;
    {
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        faiss::Index* index = faiss::read_index(&reader);

        if (auto temp_res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_)) {
            ResScope rs(temp_res, gpu_id_, false);
            auto device_index = faiss::gpu::index_cpu_to_gpu(temp_res->faiss_res.get(), gpu_id_, index);
            index_.reset(device_index);
            res_ = temp_res;
        } else {
            KNOWHERE_THROW_MSG("Load error, can't get gpu resource");
        }

        delete index;
    }
}

void
GPUIVF::QueryImpl(int64_t n,
                  const float* data,
                  int64_t k,
                  float* distances,
                  int64_t* labels,
                  const Config& config,
                  const faiss::BitsetView bitset) {
    auto device_index = std::dynamic_pointer_cast<faiss::gpu::GpuIndexIVF>(index_);
    fiu_do_on("GPUIVF.search_impl.invald_index", device_index = nullptr);
    if (device_index) {
        device_index->nprobe = std::min(static_cast<int>(config[IndexParams::nprobe]), device_index->nlist);
        ResScope rs(res_, gpu_id_);

        // if query size > 2048 we search by blocks to avoid malloc issue
        const int64_t block_size = 2048;
        int64_t dim = device_index->d;
        for (int64_t i = 0; i < n; i += block_size) {
            int64_t search_size = (n - i > block_size) ? block_size : (n - i);
            device_index->search(search_size, reinterpret_cast<const float*>(data) + i * dim, k, distances + i * k,
                                 labels + i * k, bitset);
        }
    } else {
        KNOWHERE_THROW_MSG("Not a GpuIndexIVF type.");
    }
}

}  // namespace knowhere
}  // namespace milvus
