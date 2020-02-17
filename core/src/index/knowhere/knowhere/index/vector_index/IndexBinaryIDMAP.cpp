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

#include <faiss/IndexBinaryFlat.h>
#include <faiss/MetaIndexes.h>

#include <faiss/index_factory.h>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexBinaryIDMAP.h"

namespace knowhere {

BinarySet
BinaryIDMAP::Serialize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    return SerializeImpl();
}

void
BinaryIDMAP::Load(const BinarySet& index_binary) {
    std::lock_guard<std::mutex> lk(mutex_);
    LoadImpl(index_binary);
}

DatasetPtr
BinaryIDMAP::Search(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GETBINARYTENSOR(dataset)

    auto elems = rows * config->k;
    size_t p_id_size = sizeof(int64_t) * elems;
    size_t p_dist_size = sizeof(float) * elems;
    auto p_id = (int64_t*)malloc(p_id_size);
    auto p_dist = (float*)malloc(p_dist_size);

    search_impl(rows, (uint8_t*)p_data, config->k, p_dist, p_id, Config());

    auto ret_ds = std::make_shared<Dataset>();
    if (index_->metric_type == faiss::METRIC_Hamming) {
        auto pf_dist = (float*)malloc(p_dist_size);
        int32_t* pi_dist = (int32_t*)p_dist;
        for (int i = 0; i < elems; i++) {
            *(pf_dist + i) = (float)(*(pi_dist + i));
        }
        ret_ds->Set(meta::IDS, p_id);
        ret_ds->Set(meta::DISTANCE, pf_dist);
        free(p_dist);
    } else {
        ret_ds->Set(meta::IDS, p_id);
        ret_ds->Set(meta::DISTANCE, p_dist);
    }
    return ret_ds;
}

void
BinaryIDMAP::search_impl(int64_t n, const uint8_t* data, int64_t k, float* distances, int64_t* labels,
                         const Config& cfg) {
    int32_t* pdistances = (int32_t*)distances;
    index_->search(n, (uint8_t*)data, k, pdistances, labels);
}

void
BinaryIDMAP::Add(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETBINARYTENSOR(dataset)

    auto p_ids = dataset->Get<const int64_t*>(meta::IDS);
    index_->add_with_ids(rows, (uint8_t*)p_data, p_ids);
}

void
BinaryIDMAP::Train(const Config& config) {
    auto build_cfg = std::dynamic_pointer_cast<BinIDMAPCfg>(config);
    if (build_cfg == nullptr) {
        KNOWHERE_THROW_MSG("not support this kind of config");
    }
    config->CheckValid();

    const char* type = "BFlat";
    auto index = faiss::index_binary_factory(config->d, type, GetMetricType(config->metric_type));
    index_.reset(index);
}

int64_t
BinaryIDMAP::Count() {
    return index_->ntotal;
}

int64_t
BinaryIDMAP::Dimension() {
    return index_->d;
}

const uint8_t*
BinaryIDMAP::GetRawVectors() {
    try {
        auto file_index = dynamic_cast<faiss::IndexBinaryIDMap*>(index_.get());
        auto flat_index = dynamic_cast<faiss::IndexBinaryFlat*>(file_index->index);
        return flat_index->xb.data();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

const int64_t*
BinaryIDMAP::GetRawIds() {
    try {
        auto file_index = dynamic_cast<faiss::IndexBinaryIDMap*>(index_.get());
        return file_index->id_map.data();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
BinaryIDMAP::Seal() {
    // do nothing
}

}  // namespace knowhere
