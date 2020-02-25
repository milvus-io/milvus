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

#include "knowhere/index/vector_index/IndexHNSW.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <utility>
#include <vector>

#include "hnswlib/hnswalg.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace knowhere {

BinarySet
IndexHNSW::Serialize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        MemoryIOWriter writer;
        index_->saveIndex(writer);
        auto data = std::make_shared<uint8_t>();
        data.reset(writer.data_);

        BinarySet res_set;
        res_set.Append("HNSW", data, writer.total);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW::Load(const BinarySet& index_binary) {
    try {
        auto binary = index_binary.GetByName("HNSW");

        MemoryIOReader reader;
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        hnswlib::SpaceInterface<float>* space;
        index_ = std::make_shared<hnswlib::HierarchicalNSW<float>>(space);
        index_->loadIndex(reader);
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

DatasetPtr
IndexHNSW::Search(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    GETTENSOR(dataset)

    size_t id_size = sizeof(int64_t) * config->k;
    size_t dist_size = sizeof(float) * config->k;
    auto p_id = (int64_t*)malloc(id_size * rows);
    auto p_dist = (float*)malloc(dist_size * rows);

    using P = std::pair<float, int64_t>;
    auto compare = [](P& v1, P& v2) { return v1.first < v2.first; };
#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        const float* single_query = p_data + i * dim;
        std::vector<std::pair<float, int64_t>> ret = index_->searchKnn(single_query, config->k, compare);
        while (ret.size() < config->k) {
            ret.push_back(std::make_pair(-1, -1));
        }
        std::vector<float> dist;
        std::vector<int64_t> ids;
        std::transform(ret.begin(), ret.end(), std::back_inserter(dist),
                       [](const std::pair<float, int64_t>& e) { return e.first; });
        std::transform(ret.begin(), ret.end(), std::back_inserter(ids),
                       [](const std::pair<float, int64_t>& e) { return e.second; });

        memcpy(p_dist + i * config->k, dist.data(), dist_size);
        memcpy(p_id + i * config->k, ids.data(), id_size);
    }

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

IndexModelPtr
IndexHNSW::Train(const DatasetPtr& dataset, const Config& config) {
    auto build_cfg = std::dynamic_pointer_cast<HNSWCfg>(config);
    if (build_cfg != nullptr) {
        build_cfg->CheckValid();  // throw exception
    }

    GETTENSOR(dataset)

    hnswlib::SpaceInterface<float>* space;
    if (config->metric_type == METRICTYPE::L2) {
        space = new hnswlib::L2Space(dim);
    } else if (config->metric_type == METRICTYPE::IP) {
        space = new hnswlib::InnerProductSpace(dim);
    }
    index_ = std::make_shared<hnswlib::HierarchicalNSW<float>>(space, rows, build_cfg->M, build_cfg->ef);

    return nullptr;
}

void
IndexHNSW::Add(const DatasetPtr& dataset, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);

    GETTENSOR(dataset)
    auto p_ids = dataset->Get<const int64_t*>(meta::IDS);

    for (int i = 0; i < 1; i++) {
        index_->addPoint((void*)(p_data + dim * i), p_ids[i]);
    }
#pragma omp parallel for
    for (int i = 1; i < rows; i++) {
        index_->addPoint((void*)(p_data + dim * i), p_ids[i]);
    }
}

void
IndexHNSW::Seal() {
    // do nothing
}

int64_t
IndexHNSW::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->cur_element_count;
}

int64_t
IndexHNSW::Dimension() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return (*(size_t*)index_->dist_func_param_);
}

}  // namespace knowhere
