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

#include "knowhere/index/vector_index/IndexBinaryIVF.h"

#include <faiss/IndexBinaryFlat.h>
#include <faiss/IndexBinaryIVF.h>

#include <chrono>
#include <string>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

namespace milvus {
namespace knowhere {

using stdclock = std::chrono::high_resolution_clock;

BinarySet
BinaryIVF::Serialize(const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    return SerializeImpl(index_type_);
}

void
BinaryIVF::Load(const BinarySet& index_binary) {
    std::lock_guard<std::mutex> lk(mutex_);
    LoadImpl(index_binary, index_type_);
}

DatasetPtr
BinaryIVF::Query(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    GETTENSOR(dataset_ptr)

    try {
        int64_t k = config[meta::TOPK].get<int64_t>();
        auto elems = rows * k;

        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        auto p_id = (int64_t*)malloc(p_id_size);
        auto p_dist = (float*)malloc(p_dist_size);

        QueryImpl(rows, (uint8_t*)p_data, k, p_dist, p_id, config);

        auto ret_ds = std::make_shared<Dataset>();
        ret_ds->Set(meta::IDS, p_id);
        ret_ds->Set(meta::DISTANCE, p_dist);

        return ret_ds;
    } catch (faiss::FaissException& e) {
        KNOWHERE_THROW_MSG(e.what());
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

#if 0
DatasetPtr
BinaryIVF::QueryById(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);
    auto p_data = dataset_ptr->Get<const int64_t*>(meta::IDS);

    try {
        int64_t k = config[meta::TOPK].get<int64_t>();
        auto elems = rows * k;

        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        auto p_id = (int64_t*)malloc(p_id_size);
        auto p_dist = (float*)malloc(p_dist_size);

        int32_t* pdistances = (int32_t*)p_dist;
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
    } catch (faiss::FaissException& e) {
        KNOWHERE_THROW_MSG(e.what());
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}
#endif

int64_t
BinaryIVF::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->ntotal;
}

int64_t
BinaryIVF::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->d;
}

void
BinaryIVF::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    auto bin_ivf_index = dynamic_cast<faiss::IndexBinaryIVF*>(index_.get());
    auto nb = bin_ivf_index->invlists->compute_ntotal();
    auto nlist = bin_ivf_index->nlist;
    auto code_size = bin_ivf_index->code_size;

    // binary ivf codes, ids and quantizer
    index_size_ = nb * code_size + nb * sizeof(int64_t) + nlist * code_size;
}

void
BinaryIVF::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    GETTENSORWITHIDS(dataset_ptr)

    int64_t nlist = config[IndexParams::nlist];
    faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
    faiss::IndexBinary* coarse_quantizer = new faiss::IndexBinaryFlat(dim, metric_type);
    auto index = std::make_shared<faiss::IndexBinaryIVF>(coarse_quantizer, dim, nlist, metric_type);
    index->train(rows, (uint8_t*)p_data);
    index->add_with_ids(rows, (uint8_t*)p_data, p_ids);
    index_ = index;
}

#if 0
DatasetPtr
BinaryIVF::GetVectorById(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    // GETBINARYTENSOR(dataset_ptr)
    // auto rows = dataset_ptr->Get<int64_t>(meta::ROWS);
    auto p_data = dataset_ptr->Get<const int64_t*>(meta::IDS);
    auto elems = dataset_ptr->Get<int64_t>(meta::DIM);

    try {
        size_t p_x_size = sizeof(uint8_t) * elems;
        auto p_x = (uint8_t*)malloc(p_x_size);

        index_->get_vector_by_id(1, p_data, p_x, bitset_);

        auto ret_ds = std::make_shared<Dataset>();
        ret_ds->Set(meta::TENSOR, p_x);
        return ret_ds;
    } catch (faiss::FaissException& e) {
        KNOWHERE_THROW_MSG(e.what());
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}
#endif

std::shared_ptr<faiss::IVFSearchParameters>
BinaryIVF::GenParams(const Config& config) {
    auto params = std::make_shared<faiss::IVFSearchParameters>();
    params->nprobe = config[IndexParams::nprobe];
    // params->max_codes = config["max_code"];
    return params;
}

void
BinaryIVF::QueryImpl(int64_t n, const uint8_t* data, int64_t k, float* distances, int64_t* labels,
                     const Config& config) {
    auto params = GenParams(config);
    auto ivf_index = dynamic_cast<faiss::IndexBinaryIVF*>(index_.get());
    ivf_index->nprobe = params->nprobe;

    stdclock::time_point before = stdclock::now();
    int32_t* i_distances = reinterpret_cast<int32_t*>(distances);
    index_->search(n, (uint8_t*)data, k, i_distances, labels, GetBlacklist());

    stdclock::time_point after = stdclock::now();
    double search_cost = (std::chrono::duration<double, std::micro>(after - before)).count();
    LOG_KNOWHERE_DEBUG_ << "IVF search cost: " << search_cost
                        << ", quantization cost: " << faiss::indexIVF_stats.quantization_time
                        << ", data search cost: " << faiss::indexIVF_stats.search_time;
    faiss::indexIVF_stats.quantization_time = 0;
    faiss::indexIVF_stats.search_time = 0;

    // if hamming, it need transform int32 to float
    if (ivf_index->metric_type == faiss::METRIC_Hamming) {
        int64_t num = n * k;
        for (int64_t i = 0; i < num; i++) {
            distances[i] = static_cast<float>(i_distances[i]);
        }
    }
}

}  // namespace knowhere
}  // namespace milvus
