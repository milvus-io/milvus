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

#include <faiss/IndexBinaryFlat.h>
#include <faiss/IndexBinaryIVF.h>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexBinaryIVF.h"

#include <chrono>

namespace knowhere {

using stdclock = std::chrono::high_resolution_clock;

BinarySet
BinaryIVF::Serialize() {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    std::lock_guard<std::mutex> lk(mutex_);
    return SerializeImpl();
}

void
BinaryIVF::Load(const BinarySet& index_binary) {
    std::lock_guard<std::mutex> lk(mutex_);
    LoadImpl(index_binary);
}

DatasetPtr
BinaryIVF::Search(const DatasetPtr& dataset, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    //    auto search_cfg = std::dynamic_pointer_cast<IVFBinCfg>(config);
    //    if (search_cfg == nullptr) {
    //        KNOWHERE_THROW_MSG("not support this kind of config");
    //    }

    GETBINARYTENSOR(dataset)

    try {
        auto elems = rows * config->k;

        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        auto p_id = (int64_t*)malloc(p_id_size);
        auto p_dist = (float*)malloc(p_dist_size);

        search_impl(rows, (uint8_t*)p_data, config->k, p_dist, p_id, config);

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

void
BinaryIVF::search_impl(int64_t n, const uint8_t* data, int64_t k, float* distances, int64_t* labels,
                       const Config& cfg) {
    auto params = GenParams(cfg);
    auto ivf_index = dynamic_cast<faiss::IndexBinaryIVF*>(index_.get());
    ivf_index->nprobe = params->nprobe;
    int32_t* pdistances = (int32_t*)distances;
    stdclock::time_point before = stdclock::now();
    // todo: remove static cast (zhiru)
    static_cast<faiss::IndexBinary*>(index_.get())->search(n, (uint8_t*)data, k, pdistances, labels, bitset_);
    stdclock::time_point after = stdclock::now();
    double search_cost = (std::chrono::duration<double, std::micro>(after - before)).count();
    KNOWHERE_LOG_DEBUG << "IVF search cost: " << search_cost
                       << ", quantization cost: " << faiss::indexIVF_stats.quantization_time
                       << ", data search cost: " << faiss::indexIVF_stats.search_time;
    faiss::indexIVF_stats.quantization_time = 0;
    faiss::indexIVF_stats.search_time = 0;
}

std::shared_ptr<faiss::IVFSearchParameters>
BinaryIVF::GenParams(const Config& config) {
    auto params = std::make_shared<faiss::IVFSearchParameters>();

    auto search_cfg = std::dynamic_pointer_cast<IVFCfg>(config);
    params->nprobe = search_cfg->nprobe;
    // params->max_codes = config.get_with_default("max_codes", size_t(0));

    return params;
}

IndexModelPtr
BinaryIVF::Train(const DatasetPtr& dataset, const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);

    auto build_cfg = std::dynamic_pointer_cast<IVFBinCfg>(config);
    if (build_cfg != nullptr) {
        build_cfg->CheckValid();  // throw exception
    }

    GETBINARYTENSOR(dataset)
    auto p_ids = dataset->Get<const int64_t*>(meta::IDS);

    faiss::IndexBinary* coarse_quantizer = new faiss::IndexBinaryFlat(dim, GetMetricType(build_cfg->metric_type));
    auto index = std::make_shared<faiss::IndexBinaryIVF>(coarse_quantizer, dim, build_cfg->nlist,
                                                         GetMetricType(build_cfg->metric_type));
    index->train(rows, (uint8_t*)p_data);
    index->add_with_ids(rows, (uint8_t*)p_data, p_ids);
    index_ = index;
    return nullptr;
}

int64_t
BinaryIVF::Count() {
    return index_->ntotal;
}

int64_t
BinaryIVF::Dimension() {
    return index_->d;
}

void
BinaryIVF::Add(const DatasetPtr& dataset, const Config& config) {
    KNOWHERE_THROW_MSG("not support yet");
}

void
BinaryIVF::Seal() {
    // do nothing
}

DatasetPtr
BinaryIVF::SearchById(const DatasetPtr& dataset, const Config& config) {
    if (!index_ || !index_->is_trained) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    //    auto search_cfg = std::dynamic_pointer_cast<IVFBinCfg>(config);
    //    if (search_cfg == nullptr) {
    //        KNOWHERE_THROW_MSG("not support this kind of config");
    //    }

    GETBINARYTENSOR(dataset)

    try {
        auto elems = rows * config->k;

        size_t p_id_size = sizeof(int64_t) * elems;
        size_t p_dist_size = sizeof(float) * elems;
        auto p_id = (int64_t*)malloc(p_id_size);
        auto p_dist = (float*)malloc(p_dist_size);

        int32_t* pdistances = (int32_t*)p_dist;
        auto whitelist = dataset->Get<faiss::ConcurrentBitsetPtr>("bitset");
        index_->searchById(rows, (uint8_t *)p_data, config->k, pdistances, p_id, whitelist);

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

void
BinaryIVF::SetBlacklist(faiss::ConcurrentBitsetPtr list) {
    bitset_ = std::move(list);
}

}  // namespace knowhere
