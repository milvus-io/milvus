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

#include <faiss/IndexSQHybrid.h>
#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/GpuIndexIVF.h>
#include <faiss/index_factory.h>
#include <fiu/fiu-local.h>
#include <string>
#include <utility>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"

namespace milvus {
namespace knowhere {

#ifdef MILVUS_GPU_VERSION

void
IVFSQHybrid::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GET_TENSOR_DATA_DIM(dataset_ptr)
    gpu_id_ = config[knowhere::meta::DEVICEID];

    auto gpu_res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_);
    if (gpu_res != nullptr) {
        ResScope rs(gpu_res, gpu_id_, true);
        faiss::gpu::GpuIndexIVFSQHybridConfig idx_config;
        idx_config.device = static_cast<int32_t>(gpu_id_);
        int32_t nlist = config[IndexParams::nlist];
        faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
        index_ = std::make_shared<faiss::gpu::GpuIndexIVFSQHybrid>(
            gpu_res->faiss_res.get(), dim, nlist, faiss::QuantizerType::QT_8bit, metric_type, true, idx_config);
        index_->train(rows, reinterpret_cast<const float*>(p_data));
        res_ = gpu_res;
        gpu_mode_ = 2;
        index_mode_ = IndexMode::MODE_GPU;
    } else {
        KNOWHERE_THROW_MSG("Build IVFSQHybrid can't get gpu resource");
    }
}

VecIndexPtr
IVFSQHybrid::CopyGpuToCpu(const Config& config) {
    if (gpu_mode_ == 0) {
        return std::make_shared<IVFSQHybrid>(index_);
    }

    faiss::Index* device_index = index_.get();
    faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    if (auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(host_index)) {
        ivf_index->to_readonly();
        ivf_index->backup_quantizer();
    }

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IVFSQHybrid>(new_index);
}

VecIndexPtr
IVFSQHybrid::CopyCpuToGpu(const int64_t device_id, const Config& config) {
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        auto idx = dynamic_cast<faiss::IndexIVF*>(index_.get());
        idx->restore_quantizer();
        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), device_id, index_.get(), &option);
        std::shared_ptr<faiss::Index> device_index = std::shared_ptr<faiss::Index>(gpu_index);
        auto new_idx = std::make_shared<IVFSQHybrid>(device_index, device_id, res);
        return new_idx;
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu: " + std::to_string(gpu_id_) + "resource");
    }
}

std::pair<VecIndexPtr, FaissIVFQuantizerPtr>
IVFSQHybrid::CopyCpuToGpuWithQuantizer(const int64_t device_id, const Config& config) {
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        ResScope rs(res, device_id, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        faiss::IndexComposition index_composition;
        index_composition.index = index_.get();
        index_composition.quantizer = nullptr;
        index_composition.mode = 0;  // copy all

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), device_id, &index_composition, &option);

        std::shared_ptr<faiss::Index> device_index;
        device_index.reset(gpu_index);
        auto new_idx = std::make_shared<IVFSQHybrid>(device_index, device_id, res);

        auto q = std::make_shared<FaissIVFQuantizer>();
        q->quantizer = index_composition.quantizer;
        q->size = index_composition.quantizer->d * index_composition.quantizer->getNumVecs() * sizeof(float);
        q->gpu_id = device_id;
        return std::make_pair(new_idx, q);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu: " + std::to_string(gpu_id_) + "resource");
    }
}

VecIndexPtr
IVFSQHybrid::LoadData(const FaissIVFQuantizerPtr& quantizer_ptr, const Config& config) {
    int64_t gpu_id = config[knowhere::meta::DEVICEID];

    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id)) {
        ResScope rs(res, gpu_id, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        auto ivf_quantizer = std::dynamic_pointer_cast<FaissIVFQuantizer>(quantizer_ptr);
        if (ivf_quantizer == nullptr)
            KNOWHERE_THROW_MSG("quantizer type not faissivfquantizer");

        faiss::IndexComposition index_composition;
        index_composition.index = index_.get();
        index_composition.quantizer = ivf_quantizer->quantizer;
        index_composition.mode = 2;  // only copy data

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), gpu_id, &index_composition, &option);
        std::shared_ptr<faiss::Index> new_idx;
        new_idx.reset(gpu_index);
        auto sq_idx = std::make_shared<IVFSQHybrid>(new_idx, gpu_id, res);
        return sq_idx;
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu: " + std::to_string(gpu_id) + "resource");
    }
}

FaissIVFQuantizerPtr
IVFSQHybrid::LoadQuantizer(const Config& config) {
    auto gpu_id = config[knowhere::meta::DEVICEID].get<int64_t>();

    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id)) {
        ResScope rs(res, gpu_id, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        faiss::IndexComposition index_composition;
        index_composition.index = index_.get();
        index_composition.quantizer = nullptr;
        index_composition.mode = 1;  // only copy quantizer

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), gpu_id, &index_composition, &option);
        delete gpu_index;

        auto q = std::make_shared<FaissIVFQuantizer>();

        auto q_ptr = index_composition.quantizer;
        q->size = q_ptr->d * q_ptr->getNumVecs() * sizeof(float);
        q->quantizer = q_ptr;
        q->gpu_id = gpu_id;
        return q;
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu: " + std::to_string(gpu_id) + "resource");
    }
}

void
IVFSQHybrid::SetQuantizer(const FaissIVFQuantizerPtr& quantizer_ptr) {
    faiss::IndexIVF* ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    if (ivf_index == nullptr) {
        KNOWHERE_THROW_MSG("Index type error");
    }

    // Once SetQuantizer() is called, make sure UnsetQuantizer() is also called before destructuring.
    // Otherwise, ivf_index->quantizer will be double free.

    quantizer_ = quantizer_ptr;
    ivf_index->quantizer = quantizer_->quantizer;
    gpu_mode_ = 1;
}

void
IVFSQHybrid::UnsetQuantizer() {
    auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    if (ivf_index == nullptr) {
        KNOWHERE_THROW_MSG("Index type error");
    }

    // set back to cpu mode
    ivf_index->restore_quantizer();
    quantizer_ = nullptr;
    gpu_mode_ = 0;
}

BinarySet
IVFSQHybrid::SerializeImpl(const IndexType& type) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    fiu_do_on("IVFSQHybrid.SerializeImpl.zero_gpu_mode", gpu_mode_ = 0);
    if (gpu_mode_ == 0) {
        MemoryIOWriter writer;
        faiss::write_index(index_.get(), &writer);

        std::shared_ptr<uint8_t[]> data(writer.data_);

        BinarySet res_set;
        res_set.Append("IVF", data, writer.rp);

        return res_set;
    } else if (gpu_mode_ == 2) {
        return GPUIVF::SerializeImpl(type);
    } else {
        KNOWHERE_THROW_MSG("Can't serialize IVFSQ8Hybrid");
    }
}

void
IVFSQHybrid::LoadImpl(const BinarySet& binary_set, const IndexType& type) {
    FaissBaseIndex::LoadImpl(binary_set, index_type_);  // load on cpu
    auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    ivf_index->backup_quantizer();
    gpu_mode_ = 0;
}

void
IVFSQHybrid::QueryImpl(int64_t n,
                       const float* data,
                       int64_t k,
                       float* distances,
                       int64_t* labels,
                       const Config& config,
                       const faiss::BitsetView bitset) {
    if (gpu_mode_ == 2) {
        GPUIVF::QueryImpl(n, data, k, distances, labels, config, bitset);
        //        index_->search(n, (float*)data, k, distances, labels);
    } else if (gpu_mode_ == 1) {  // hybrid
        auto gpu_id = quantizer_->gpu_id;
        if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id)) {
            ResScope rs(res, gpu_id, true);
            IVF::QueryImpl(n, data, k, distances, labels, config, bitset);
        } else {
            KNOWHERE_THROW_MSG("Hybrid Search Error, can't get gpu: " + std::to_string(gpu_id) + "resource");
        }
    } else if (gpu_mode_ == 0) {
        IVF::QueryImpl(n, data, k, distances, labels, config, bitset);
    }
}

void
IVFSQHybrid::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    auto ivfsqh_index = dynamic_cast<faiss::IndexIVFSQHybrid*>(index_.get());
    auto nb = ivfsqh_index->invlists->compute_ntotal();
    auto code_size = ivfsqh_index->code_size;
    auto nlist = ivfsqh_index->nlist;
    auto d = ivfsqh_index->d;
    // ivf codes, ivf ids, sq trained vectors and quantizer
    index_size_ = nb * code_size + nb * sizeof(int64_t) + 2 * d * sizeof(float) + nlist * d * sizeof(float);
}

FaissIVFQuantizer::~FaissIVFQuantizer() {
    if (quantizer != nullptr) {
        delete quantizer;
        quantizer = nullptr;
    }
}

#endif

}  // namespace knowhere
}  // namespace milvus
