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
#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

#include <utility>

#include <faiss/gpu/GpuCloner.h>
#include <faiss/gpu/GpuIndexIVF.h>
#include <faiss/index_factory.h>

namespace knowhere {

#ifdef CUSTOMIZATION

// std::mutex g_mutex;

IndexModelPtr
IVFSQHybrid::Train(const DatasetPtr& dataset, const Config& config) {
    //    std::lock_guard<std::mutex> lk(g_mutex);

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
    if (gpu_mode == 0) {
        return std::make_shared<IVFSQHybrid>(index_);
    }
    std::lock_guard<std::mutex> lk(mutex_);

    faiss::Index* device_index = index_.get();
    faiss::Index* host_index = faiss::gpu::index_gpu_to_cpu(device_index);

    if (auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(host_index)) {
        if (ivf_index != nullptr) {
            ivf_index->to_readonly();
        }
        ivf_index->backup_quantizer();
    }

    std::shared_ptr<faiss::Index> new_index;
    new_index.reset(host_index);
    return std::make_shared<IVFSQHybrid>(new_index);
}

VectorIndexPtr
IVFSQHybrid::CopyCpuToGpu(const int64_t& device_id, const Config& config) {
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

void
IVFSQHybrid::LoadImpl(const BinarySet& index_binary) {
    FaissBaseIndex::LoadImpl(index_binary);  // load on cpu
    auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    ivf_index->backup_quantizer();
    gpu_mode = 0;
}

void
IVFSQHybrid::search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels,
                         const Config& cfg) {
    //        std::lock_guard<std::mutex> lk(g_mutex);
    //        static int64_t search_count;
    //        ++search_count;

    if (gpu_mode == 2) {
        GPUIVF::search_impl(n, data, k, distances, labels, cfg);
        //        index_->search(n, (float*)data, k, distances, labels);
    } else if (gpu_mode == 1) {  // hybrid
        if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(quantizer_gpu_id_)) {
            ResScope rs(res, quantizer_gpu_id_, true);
            IVF::search_impl(n, data, k, distances, labels, cfg);
        } else {
            KNOWHERE_THROW_MSG("Hybrid Search Error, can't get gpu: " + std::to_string(quantizer_gpu_id_) + "resource");
        }
    } else if (gpu_mode == 0) {
        IVF::search_impl(n, data, k, distances, labels, cfg);
    }
}

QuantizerPtr
IVFSQHybrid::LoadQuantizer(const Config& conf) {
    //    std::lock_guard<std::mutex> lk(g_mutex);

    auto quantizer_conf = std::dynamic_pointer_cast<QuantizerCfg>(conf);
    if (quantizer_conf != nullptr) {
        if (quantizer_conf->mode != 1) {
            KNOWHERE_THROW_MSG("mode only support 1 in this func");
        }
    }
    auto gpu_id = quantizer_conf->gpu_id;

    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id)) {
        ResScope rs(res, gpu_id, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        auto index_composition = new faiss::IndexComposition;
        index_composition->index = index_.get();
        index_composition->quantizer = nullptr;
        index_composition->mode = quantizer_conf->mode;  // only 1

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), gpu_id, index_composition, &option);
        delete gpu_index;

        auto q = std::make_shared<FaissIVFQuantizer>();

        auto& q_ptr = index_composition->quantizer;
        q->size = q_ptr->d * q_ptr->getNumVecs() * sizeof(float);
        q->quantizer = q_ptr;
        q->gpu_id = gpu_id;
        res_ = res;
        gpu_mode = 1;
        return q;
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu: " + std::to_string(gpu_id) + "resource");
    }
}

void
IVFSQHybrid::SetQuantizer(const QuantizerPtr& q) {
    //    std::lock_guard<std::mutex> lk(g_mutex);

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
    quantizer_gpu_id_ = ivf_quantizer->gpu_id;
    gpu_mode = 1;
}

void
IVFSQHybrid::UnsetQuantizer() {
    //    std::lock_guard<std::mutex> lk(g_mutex);

    auto* ivf_index = dynamic_cast<faiss::IndexIVF*>(index_.get());
    if (ivf_index == nullptr) {
        KNOWHERE_THROW_MSG("Index type error");
    }

    ivf_index->quantizer = nullptr;
    quantizer_gpu_id_ = -1;
}

VectorIndexPtr
IVFSQHybrid::LoadData(const knowhere::QuantizerPtr& q, const Config& conf) {
    //    std::lock_guard<std::mutex> lk(g_mutex);

    auto quantizer_conf = std::dynamic_pointer_cast<QuantizerCfg>(conf);
    if (quantizer_conf != nullptr) {
        if (quantizer_conf->mode != 2) {
            KNOWHERE_THROW_MSG("mode only support 2 in this func");
        }
    } else {
        KNOWHERE_THROW_MSG("conf error");
    }

    auto gpu_id = quantizer_conf->gpu_id;

    if (auto res = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id)) {
        ResScope rs(res, gpu_id, false);
        faiss::gpu::GpuClonerOptions option;
        option.allInGpu = true;

        auto ivf_quantizer = std::dynamic_pointer_cast<FaissIVFQuantizer>(q);
        if (ivf_quantizer == nullptr)
            KNOWHERE_THROW_MSG("quantizer type not faissivfquantizer");

        auto index_composition = new faiss::IndexComposition;
        index_composition->index = index_.get();
        index_composition->quantizer = ivf_quantizer->quantizer;
        index_composition->mode = quantizer_conf->mode;  // only 2

        auto gpu_index = faiss::gpu::index_cpu_to_gpu(res->faiss_res.get(), gpu_id, index_composition, &option);
        std::shared_ptr<faiss::Index> new_idx;
        new_idx.reset(gpu_index);
        auto sq_idx = std::make_shared<IVFSQHybrid>(new_idx, gpu_id, res);
        return sq_idx;
    } else {
        KNOWHERE_THROW_MSG("CopyCpuToGpu Error, can't get gpu: " + std::to_string(gpu_id) + "resource");
    }
}

std::pair<VectorIndexPtr, QuantizerPtr>
IVFSQHybrid::CopyCpuToGpuWithQuantizer(const int64_t& device_id, const Config& config) {
    //    std::lock_guard<std::mutex> lk(g_mutex);

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

void
IVFSQHybrid::set_index_model(IndexModelPtr model) {
    std::lock_guard<std::mutex> lk(mutex_);

    auto host_index = std::static_pointer_cast<IVFIndexModel>(model);
    if (auto gpures = FaissGpuResourceMgr::GetInstance().GetRes(gpu_id_)) {
        ResScope rs(gpures, gpu_id_, false);
        auto device_index = faiss::gpu::index_cpu_to_gpu(gpures->faiss_res.get(), gpu_id_, host_index->index_.get());
        index_.reset(device_index);
        res_ = gpures;
        gpu_mode = 2;
    } else {
        KNOWHERE_THROW_MSG("load index model error, can't get gpu_resource");
    }
}

BinarySet
IVFSQHybrid::SerializeImpl() {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    if (gpu_mode == 0) {
        MemoryIOWriter writer;
        faiss::write_index(index_.get(), &writer);

        auto data = std::make_shared<uint8_t>();
        data.reset(writer.data_);

        BinarySet res_set;
        res_set.Append("IVF", data, writer.rp);

        return res_set;
    } else if (gpu_mode == 2) {
        return GPUIVF::SerializeImpl();
    } else {
        KNOWHERE_THROW_MSG("Can't serialize IVFSQ8Hybrid");
    }
}

FaissIVFQuantizer::~FaissIVFQuantizer() {
    if (quantizer != nullptr) {
        delete quantizer;
        quantizer = nullptr;
    }
    // else do nothing
}

#endif
}  // namespace knowhere
