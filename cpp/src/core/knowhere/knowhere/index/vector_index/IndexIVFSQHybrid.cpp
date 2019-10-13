//
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

#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"
#include "faiss/AutoTune.h"
#include "faiss/gpu/GpuAutoTune.h"
#include "faiss/gpu/GpuIndexIVF.h"
#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"

namespace knowhere {

#ifdef CUSTOMIZATION
IndexModelPtr
IVFSQHybrid::Train(const DatasetPtr& dataset, const Config& config) {
    auto build_cfg = std::dynamic_pointer_cast<IVFSQCfg>(config);
    if (build_cfg != nullptr) {
        build_cfg->CheckValid();  // throw exception
    }
    gpu_id_ = build_cfg->gpu_id;

    GETTENSOR(dataset)

    std::stringstream index_type;
    index_type << "IVF" << build_cfg->nlist << ","
               << "SQ8Hybrid";
    auto build_index = faiss::index_factory(dim, index_type.str().c_str(), GetMetricType(build_cfg->metric_type));

    auto temp_resource = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_);
    if (temp_resource != nullptr) {
        ResScope rs(temp_resource, gpu_id_, true);
        auto device_index = faiss::gpu::index_cpu_to_gpu(temp_resource->faiss_res.get(), gpu_id_, build_index);
        device_index->train(rows, (float*)p_data);

        std::shared_ptr<faiss::Index> host_index = nullptr;
        host_index.reset(faiss::gpu::index_gpu_to_cpu(device_index));

        delete device_index;
        delete build_index;

        return std::make_shared<IVFIndexModel>(host_index);
    } else {
        KNOWHERE_THROW_MSG("Build IVFSQHybrid can't get gpu resource");
    }
}

VectorIndexPtr
IVFSQHybrid::CopyGpuToCpu(const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);

    if (auto device_idx = std::dynamic_pointer_cast<faiss::IndexIVF>(index_)) {
        faiss::Index* device_index = index_.get();
        faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

        std::shared_ptr<faiss::Index> new_index;
        new_index.reset(host_index);
        return std::make_shared<IVFSQHybrid>(new_index);
    } else {
        // TODO(linxj): why? jinhai
        return std::make_shared<IVFSQHybrid>(index_);
    }
}

VectorIndexPtr
IVFSQHybrid::CopyCpuToGpu(const int64_t& device_id, const Config& config) {
    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(device_id)) {
        auto p = CopyCpuToGpuWithQuantizer(device_id, config);
        return p.first;
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
}

void
IVFSQHybrid::LoadImpl(const BinarySet& index_binary) {
    FaissBaseIndex::LoadImpl(index_binary);  // load on cpu
    auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    ivf_index->backup_quantizer();
}

void
IVFSQHybrid::search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels,
                         const Config& cfg) {
    if (gpu_mode == 2) {
        GPUIVF::search_impl(n, data, k, distances, labels, cfg);
    } else if (gpu_mode == 1) {
        ResScope rs(res_, gpu_id_);
        IVF::search_impl(n, data, k, distances, labels, cfg);
    } else if (gpu_mode == 0) {
        IVF::search_impl(n, data, k, distances, labels, cfg);
    }
}

QuantizerPtr
IVFSQHybrid::LoadQuantizer(const Config& conf) {
    auto quantizer_conf = std::dynamic_pointer_cast<QuantizerCfg>(conf);
    if (quantizer_conf != nullptr) {
        if (quantizer_conf->mode != 1) {
            KNOWHERE_THROW_MSG("mode only support 1 in this func");
        }
    }
    gpu_id_ = quantizer_conf->gpu_id;

    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_)) {
        ResScope rs(res, gpu_id_, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        auto index_composition = new faiss::IndexComposition;
        index_composition->index = index_.get();
        index_composition->quantizer = nullptr;
        index_composition->mode = quantizer_conf->mode;  // only 1

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), gpu_id_, index_composition, &option);
        delete gpu_index;

        auto q = std::make_shared<FaissIVFQuantizer>();

        auto& q_ptr = index_composition->quantizer;
        q->size = q_ptr->d * q_ptr->getNumVecs() * sizeof(float);
        q->quantizer = q_ptr;
        res_ = res;
        gpu_mode = 1;
        return q;
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
}

void
IVFSQHybrid::SetQuantizer(const QuantizerPtr& q) {
    auto ivf_quantizer = std::dynamic_pointer_cast<FaissIVFQuantizer>(q);
    if (ivf_quantizer == nullptr) {
        KNOWHERE_THROW_MSG("Quantizer type error");
    }

    faiss::IndexIVF* ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());

    faiss::gpu::GpuIndexFlat* is_gpu_flat_index = dynamic_cast<faiss::gpu::GpuIndexFlat*>(ivf_index->quantizer);
    if (is_gpu_flat_index == nullptr) {
        //        delete ivf_index->quantizer;
        ivf_index->quantizer = ivf_quantizer->quantizer;
    }
}

void
IVFSQHybrid::UnsetQuantizer() {
    auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    if (ivf_index == nullptr) {
        KNOWHERE_THROW_MSG("Index type error");
    }

    ivf_index->quantizer = nullptr;
}

VectorIndexPtr
IVFSQHybrid::LoadData(const knowhere::QuantizerPtr& q, const Config& conf) {
    auto quantizer_conf = std::dynamic_pointer_cast<QuantizerCfg>(conf);
    if (quantizer_conf != nullptr) {
        if (quantizer_conf->mode != 2) {
            KNOWHERE_THROW_MSG("mode only support 2 in this func");
        }
    }
//    if (quantizer_conf->gpu_id != gpu_id_) {
//        KNOWHERE_THROW_MSG("quantizer and data must on the same gpu card");
//    }
    gpu_id_ = quantizer_conf->gpu_id;

    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_)) {
        ResScope rs(res, gpu_id_, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        auto ivf_quantizer = std::dynamic_pointer_cast<FaissIVFQuantizer>(q);
        if (ivf_quantizer == nullptr)
            KNOWHERE_THROW_MSG("quantizer type not faissivfquantizer");

        auto index_composition = new faiss::IndexComposition;
        index_composition->index = index_.get();
        index_composition->quantizer = ivf_quantizer->quantizer;
        index_composition->mode = quantizer_conf->mode;  // only 2

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), gpu_id_, index_composition, &option);
        std::shared_ptr<faiss::Index> new_idx;
        new_idx.reset(gpu_index);
        auto sq_idx = std::make_shared<IVFSQHybrid>(new_idx, gpu_id_, res);
        return sq_idx;
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
}

std::pair<VectorIndexPtr, QuantizerPtr>
IVFSQHybrid::CopyCpuToGpuWithQuantizer(const int64_t& device_id, const Config& config) {
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
        return std::make_pair(new_idx, q);
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu_resource");
    }
}

FaissIVFQuantizer::~FaissIVFQuantizer() {
    if (quantizer != nullptr) {
        delete quantizer;
        quantizer = nullptr;
    }
    // else do nothing
}

#else

QuantizerPtr
IVFSQHybrid::LoadQuantizer(const Config& conf) {
    return knowhere::QuantizerPtr();
}

void
IVFSQHybrid::SetQuantizer(const QuantizerPtr& q) {
}

void
IVFSQHybrid::UnsetQuantizer() {
}

void
IVFSQHybrid::LoadData(const knowhere::QuantizerPtr& q, const Config& conf) {
}

IndexModelPtr
IVFSQHybrid::Train(const DatasetPtr& dataset, const Config& config) {
    return GPUIVFSQ::Train(dataset, config);
}

VectorIndexPtr
IVFSQHybrid::CopyGpuToCpu(const Config& config) {
    return GPUIVFSQ::CopyGpuToCpu(config);
}

VectorIndexPtr
IVFSQHybrid::CopyCpuToGpu(const int64_t& device_id, const Config& config) {
    return IVF::CopyCpuToGpu(device_id, config);
}

void
IVFSQHybrid::search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels,
                         const Config& cfg) {
    GPUIVF::search_impl(n, data, k, distances, labels, cfg);
}

void
IVFSQHybrid::LoadImpl(const BinarySet& index_binary) {
    GPUIVF::LoadImpl(index_binary);
}

#endif
}  // namespace knowhere
