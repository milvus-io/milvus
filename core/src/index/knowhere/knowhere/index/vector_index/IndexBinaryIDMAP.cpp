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

#include "knowhere/index/vector_index/IndexBinaryIDMAP.h"

#include <faiss/IndexBinaryFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/index_factory.h>

#include <string>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

namespace milvus {
namespace knowhere {

BinarySet
BinaryIDMAP::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    return SerializeImpl(index_type_);
}

void
BinaryIDMAP::Load(const BinarySet& index_binary) {
    std::lock_guard<std::mutex> lk(mutex_);
    LoadImpl(index_binary, index_type_);
}

DatasetPtr
BinaryIDMAP::Query(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GETTENSOR(dataset_ptr)

    int64_t k = config[meta::TOPK].get<int64_t>();
    auto elems = rows * k;
    size_t p_id_size = sizeof(int64_t) * elems;
    size_t p_dist_size = sizeof(float) * elems;
    auto p_id = (int64_t*)malloc(p_id_size);
    auto p_dist = (float*)malloc(p_dist_size);

    QueryImpl(rows, (uint8_t*)p_data, k, p_dist, p_id, Config());

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

#if 0
DatasetPtr
BinaryIDMAP::QueryById(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    auto dim = dataset_ptr->Get<int64_t>(meta::DIM);
    auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);
    auto p_data = dataset_ptr->Get<const int64_t*>(meta::IDS);

    int64_t k = config[meta::TOPK].get<int64_t>();
    auto elems = rows * k;
    size_t p_id_size = sizeof(int64_t) * elems;
    size_t p_dist_size = sizeof(float) * elems;
    auto p_id = (int64_t*)malloc(p_id_size);
    auto p_dist = (float*)malloc(p_dist_size);

    auto* pdistances = (int32_t*)p_dist;
    index_->search_by_id(rows, p_data, k, pdistances, p_id, bitset_);

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
#endif

void
BinaryIDMAP::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETTENSORWITHIDS(dataset_ptr)

    index_->add_with_ids(rows, (uint8_t*)p_data, p_ids);
}

void
BinaryIDMAP::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    const char* desc = "BFlat";
    int64_t dim = config[meta::DIM].get<int64_t>();
    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    auto index = faiss::index_binary_factory(dim, desc, metric_type);
    index_.reset(index);
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
BinaryIDMAP::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    GETTENSOR(dataset_ptr)

    std::vector<int64_t> new_ids(rows);
    for (int i = 0; i < rows; ++i) {
        new_ids[i] = i;
    }

    index_->add_with_ids(rows, (uint8_t*)p_data, new_ids.data());
}

#if 0
DatasetPtr
BinaryIDMAP::GetVectorById(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    //    GETBINARYTENSOR(dataset_ptr)
    // auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);
    auto p_data = dataset_ptr->Get<const int64_t*>(meta::IDS);
    auto elems = dataset_ptr->Get<int64_t>(meta::DIM);

    size_t p_x_size = sizeof(uint8_t) * elems;
    auto p_x = (uint8_t*)malloc(p_x_size);

    index_->get_vector_by_id(1, p_data, p_x, bitset_);

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::TENSOR, p_x);
    return ret_ds;
}
#endif

void
BinaryIDMAP::QueryImpl(int64_t n, const uint8_t* data, int64_t k, float* distances, int64_t* labels,
                       const Config& config) {
    int32_t* pdistances = (int32_t*)distances;
    index_->search(n, (uint8_t*)data, k, pdistances, labels, bitset_);
}

}  // namespace knowhere
}  // namespace milvus
