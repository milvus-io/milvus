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
#include <string>
#include <utility>
#include <vector>

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

    auto metric_type_length = metric_type_.length();
    std::shared_ptr<uint8_t[]> metric_type(new uint8_t[metric_type_length]);
    memcpy(metric_type.get(), metric_type_.data(), metric_type_.length());

    auto dim = Dim();
    std::shared_ptr<uint8_t[]> dim_data(new uint8_t[sizeof(uint64_t)]);
    memcpy(dim_data.get(), &dim, sizeof(uint64_t));

    size_t index_length = index_->get_index_length();
    std::shared_ptr<uint8_t[]> index_data(new uint8_t[index_length]);
    memcpy(index_data.get(), index_->get_index(), index_length);

    BinarySet res_set;
    res_set.Append("annoy_metric_type", metric_type, metric_type_length);
    res_set.Append("annoy_dim", dim_data, sizeof(uint64_t));
    res_set.Append("annoy_index_data", index_data, index_length);
    if (config.contains(INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
        Disassemble(config[INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>() * 1024 * 1024, res_set);
    }
    return res_set;
}

void
IndexAnnoy::Load(const BinarySet& index_binary) {
    Assemble(const_cast<BinarySet&>(index_binary));
    auto metric_type = index_binary.GetByName("annoy_metric_type");
    metric_type_.resize(static_cast<size_t>(metric_type->size));
    memcpy(metric_type_.data(), metric_type->data.get(), static_cast<size_t>(metric_type->size));

    auto dim_data = index_binary.GetByName("annoy_dim");
    uint64_t dim;
    memcpy(&dim, dim_data->data.get(), static_cast<size_t>(dim_data->size));

    if (metric_type_ == Metric::L2) {
        index_ = std::make_shared<AnnoyIndex<int64_t, float, ::Euclidean, ::Kiss64Random>>(dim);
    } else if (metric_type_ == Metric::IP) {
        index_ = std::make_shared<AnnoyIndex<int64_t, float, ::DotProduct, ::Kiss64Random>>(dim);
    } else {
        KNOWHERE_THROW_MSG("metric not supported " + metric_type_);
    }

    auto index_data = index_binary.GetByName("annoy_index_data");
    char* p = nullptr;
    if (!index_->load_index(reinterpret_cast<void*>(index_data->data.get()), index_data->size, &p)) {
        std::string error_msg(p);
        free(p);
        KNOWHERE_THROW_MSG(error_msg);
    }
}

void
IndexAnnoy::BuildAll(const DatasetPtr& dataset_ptr, const Config& config) {
    if (index_) {
        // it is builded all
        LOG_KNOWHERE_DEBUG_ << "IndexAnnoy::BuildAll: index_ has been built!";
        return;
    }

    GET_TENSOR_DATA_DIM(dataset_ptr)

    metric_type_ = config[Metric::TYPE];
    if (metric_type_ == Metric::L2) {
        index_ = std::make_shared<AnnoyIndex<int64_t, float, ::Euclidean, ::Kiss64Random>>(dim);
    } else if (metric_type_ == Metric::IP) {
        index_ = std::make_shared<AnnoyIndex<int64_t, float, ::DotProduct, ::Kiss64Random>>(dim);
    } else {
        KNOWHERE_THROW_MSG("metric not supported " + metric_type_);
    }

    for (int i = 0; i < rows; ++i) {
        index_->add_item(i, static_cast<const float*>(p_data) + dim * i);
    }

    index_->build(config[IndexParams::n_trees].get<int64_t>());
}

DatasetPtr
IndexAnnoy::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::BitsetView bitset) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    GET_TENSOR_DATA_DIM(dataset_ptr)
    auto k = config[meta::TOPK].get<int64_t>();
    auto search_k = config[IndexParams::search_k].get<int64_t>();
    auto all_num = rows * k;
    auto p_id = static_cast<int64_t*>(malloc(all_num * sizeof(int64_t)));
    auto p_dist = static_cast<float*>(malloc(all_num * sizeof(float)));

#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        std::vector<int64_t> result;
        result.reserve(k);
        std::vector<float> distances;
        distances.reserve(k);
        index_->get_nns_by_vector(static_cast<const float*>(p_data) + i * dim, k, search_k, &result, &distances,
                                  bitset);

        size_t result_num = result.size();
        auto local_p_id = p_id + k * i;
        auto local_p_dist = p_dist + k * i;
        memcpy(local_p_id, result.data(), result_num * sizeof(int64_t));
        memcpy(local_p_dist, distances.data(), result_num * sizeof(float));

        MapOffsetToUid(local_p_id, result_num);

        for (; result_num < k; result_num++) {
            local_p_id[result_num] = -1;
            local_p_dist[result_num] = 1.0 / 0.0;
        }
    }

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

int64_t
IndexAnnoy::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->get_n_items();
}

int64_t
IndexAnnoy::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->get_dim();
}

void
IndexAnnoy::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_->cal_size();
}

}  // namespace knowhere
}  // namespace milvus
