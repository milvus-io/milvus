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

#include <fiu-local.h>
#include <string>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"
#include "knowhere/index/IndexType.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/impl/nsg/NSGIO.h"
#include "knowhere/index/vector_offset_index/IndexNSG_NM.h"

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/gpu/IndexGPUIDMAP.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/helpers/Cloner.h"
#endif

namespace milvus {
namespace knowhere {

BinarySet
NSG_NM::Serialize(const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        fiu_do_on("NSG_NM.Serialize.throw_exception", throw std::exception());
        std::lock_guard<std::mutex> lk(mutex_);
        impl::NsgIndex* index = index_.get();

        MemoryIOWriter writer;
        impl::write_index(index, writer);
        std::shared_ptr<uint8_t[]> data(writer.data_);

        BinarySet res_set;
        res_set.Append("NSG_NM", data, writer.rp);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
NSG_NM::Load(const BinarySet& index_binary) {
    try {
        fiu_do_on("NSG_NM.Load.throw_exception", throw std::exception());
        std::lock_guard<std::mutex> lk(mutex_);
        auto binary = index_binary.GetByName("NSG_NM");

        MemoryIOReader reader;
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        auto index = impl::read_index(reader);
        index_.reset(index);

        data_ = index_binary.GetByName(RAW_DATA)->data;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

DatasetPtr
NSG_NM::Query(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    GET_TENSOR_DATA_DIM(dataset_ptr)

    try {
        auto topK = config[meta::TOPK].get<int64_t>();
        auto elems = rows * topK;
        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        auto p_id = (int64_t*)malloc(p_id_size);
        auto p_dist = (float*)malloc(p_dist_size);

        faiss::ConcurrentBitsetPtr blacklist = GetBlacklist();

        impl::SearchParams s_params;
        s_params.search_length = config[IndexParams::search_length];
        s_params.k = config[meta::TOPK];
        {
            std::lock_guard<std::mutex> lk(mutex_);
            // index_->ori_data_ = (float*) data_.get();
            index_->Search((float*)p_data, (float*)data_.get(), rows, dim, topK, p_dist, p_id, s_params, blacklist);
        }

        auto ret_ds = std::make_shared<Dataset>();
        ret_ds->Set(meta::IDS, p_id);
        ret_ds->Set(meta::DISTANCE, p_dist);
        return ret_ds;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
NSG_NM::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    auto idmap = std::make_shared<IDMAP>();
    idmap->Train(dataset_ptr, config);
    idmap->AddWithoutIds(dataset_ptr, config);
    impl::Graph knng;
    const float* raw_data = idmap->GetRawVectors();
    const int64_t k = config[IndexParams::knng].get<int64_t>();
#ifdef MILVUS_GPU_VERSION
    const int64_t device_id = config[knowhere::meta::DEVICEID].get<int64_t>();
    if (device_id == -1) {
        auto preprocess_index = std::make_shared<IVF>();
        preprocess_index->Train(dataset_ptr, config);
        preprocess_index->AddWithoutIds(dataset_ptr, config);
        preprocess_index->GenGraph(raw_data, k, knng, config);
    } else {
        auto gpu_idx = cloner::CopyCpuToGpu(idmap, device_id, config);
        auto gpu_idmap = std::dynamic_pointer_cast<GPUIDMAP>(gpu_idx);
        gpu_idmap->GenGraph(raw_data, k, knng, config);
    }
#else
    auto preprocess_index = std::make_shared<IVF>();
    preprocess_index->Train(dataset_ptr, config);
    preprocess_index->AddWithoutIds(dataset_ptr, config);
    preprocess_index->GenGraph(raw_data, k, knng, config);
#endif

    impl::BuildParams b_params;
    b_params.candidate_pool_size = config[IndexParams::candidate];
    b_params.out_degree = config[IndexParams::out_degree];
    b_params.search_length = config[IndexParams::search_length];

    auto p_ids = dataset_ptr->Get<const int64_t*>(meta::IDS);

    GET_TENSOR_DATA_DIM(dataset_ptr)
    impl::NsgIndex::Metric_Type metric_type_nsg;
    if (config[Metric::TYPE].get<std::string>() == "IP") {
        metric_type_nsg = impl::NsgIndex::Metric_Type::Metric_Type_IP;
    } else if (config[Metric::TYPE].get<std::string>() == "L2") {
        metric_type_nsg = impl::NsgIndex::Metric_Type::Metric_Type_L2;
    } else {
        KNOWHERE_THROW_MSG("either IP or L2");
    }
    index_ = std::make_shared<impl::NsgIndex>(dim, rows, metric_type_nsg);
    index_->SetKnnGraph(knng);
    index_->Build_with_ids(rows, (float*)p_data, (int64_t*)p_ids, b_params);
}

int64_t
NSG_NM::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->ntotal;
}

int64_t
NSG_NM::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->dimension;
}

void
NSG_NM::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_->GetSize() + Dim() * Count() * sizeof(float);
}

}  // namespace knowhere
}  // namespace milvus
