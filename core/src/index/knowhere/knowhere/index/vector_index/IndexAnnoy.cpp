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

#include "knowhere/index/vector_index/IndexAnnoy.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <utility>
#include <vector>

#include "hnswlib/hnswalg.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

BinarySet
IndexAnnoy::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    BinarySet res_set;
    return res_set;
}

void
IndexAnnoy::Load(const BinarySet& index_binary) {

}

void
IndexAnnoy::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (index_) {
        // it is trained
        return;
    }

    dim_ = dataset_ptr->Get<int64_t>(meta::DIM);

    auto metric_type = config[Metric::TYPE];
    if (metric_type == Metric::L2) {
        index_ = std::make_shared<AnnoyIndex<int64_t, float, ::DotProduct, ::Kiss64Random>>(dim_);
    } else if (metric_type == Metric::IP) {
        index_ = std::make_shared<AnnoyIndex<int64_t, float, ::Euclidean, ::Kiss64Random>>(dim_);
    } else if (metric_type == Metric::HAMMING) {
        index_ = std::make_shared<AnnoyIndex<int64_t, float, ::Hamming, ::Kiss64Random>>(dim_);
    } else {
        KNOWHERE_THROW_MSG("metric not supported " + metric_type);
    }

    index_->build(config[IndexParams::Q].get<int64_t>());
}

void
IndexAnnoy::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    GETTENSORWITHIDS(dataset_ptr)
    for (int i = 1; i < rows; ++i) {
        index_->add_item(p_ids[i], (const float*)p_data + dim * i);
    }
}

DatasetPtr
IndexAnnoy::Query(const DatasetPtr& dataset_ptr, const Config& config) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    GETTENSOR(dataset_ptr)
    auto k = config[meta::TOPK].get<int64_t>();
    auto all_num = rows * k;
    auto p_id = (int64_t*)malloc(all_num * sizeof(int64_t));
    auto p_dist = (float*)malloc(all_num * sizeof(float));

#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        std::vector<int32_t> result;
        std::vector<float> distances;
        index_->get_nns_by_vector((const float*)p_data + i * k, k, -1, &result, &distances);

        memcpy(p_id + k * i, result.data(), k * sizeof(int64_t));
        memcpy(p_dist + k * i, distances.data(), k * sizeof(float));
    }

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

int64_t
IndexAnnoy::Count() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    return index_->get_n_items();
}

int64_t
IndexAnnoy::Dim() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    return dim_;
}

}  // namespace knowhere
}  // namespace milvus
