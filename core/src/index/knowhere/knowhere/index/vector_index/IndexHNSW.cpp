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

#include "faiss/BuilderSuspend.h"
#include "hnswlib/hnswalg.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

// void
// normalize_vector(float* data, float* norm_array, size_t dim) {
//     float norm = 0.0f;
//     for (int i = 0; i < dim; i++) norm += data[i] * data[i];
//     norm = 1.0f / (sqrtf(norm) + 1e-30f);
//     for (int i = 0; i < dim; i++) norm_array[i] = data[i] * norm;
// }

BinarySet
IndexHNSW::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        MemoryIOWriter writer;
        index_->saveIndex(writer);
        std::shared_ptr<uint8_t[]> data(writer.data_);

        BinarySet res_set;
        res_set.Append("HNSW", data, writer.rp);
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

void
IndexHNSW::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    try {
        GETTENSOR_DIM_ROWS(dataset_ptr)

        hnswlib::SpaceInterface<float>* space;
        if (config[Metric::TYPE] == Metric::L2) {
            space = new hnswlib::L2Space(dim);
        } else if (config[Metric::TYPE] == Metric::IP) {
            space = new hnswlib::InnerProductSpace(dim);
        }
        index_ = std::make_shared<hnswlib::HierarchicalNSW<float>>(space, rows, config[IndexParams::M].get<int64_t>(),
                                                                   config[IndexParams::efConstruction].get<int64_t>());
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW::AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    GETTENSOR(dataset_ptr)

    if (rows > 0) {
        index_->addPoint(p_data, 0);
#pragma omp parallel for
        for (int i = 1; i < rows; ++i) {
            faiss::BuilderSuspend::check_wait();
            index_->addPoint(((float*)p_data + dim * i), i);
        }
    }
}

DatasetPtr
IndexHNSW::Query(const DatasetPtr& dataset_ptr, const Config& config, faiss::ConcurrentBitsetPtr blacklist) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    GETTENSOR(dataset_ptr)

    size_t k = config[meta::TOPK].get<int64_t>();
    size_t id_size = sizeof(int64_t) * k;
    size_t dist_size = sizeof(float) * k;
    auto p_id = (int64_t*)malloc(id_size * rows);
    auto p_dist = (float*)malloc(dist_size * rows);

    index_->setEf(config[IndexParams::ef]);

    bool transform = (index_->metric_type_ == 1);  // InnerProduct: 1

#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        auto single_query = (float*)p_data + i * dim;
        auto rst = index_->searchKnn(single_query, k, blacklist);
        size_t rst_size = rst.size();

        auto p_single_dis = p_dist + i * k;
        auto p_single_id = p_id + i * k;
        size_t idx = rst_size - 1;
        while (!rst.empty()) {
            auto& it = rst.top();
            p_single_dis[idx] = transform ? (1 - it.first) : it.first;
            p_single_id[idx] = it.second;
            rst.pop();
            idx--;
        }
        MapOffsetToUid(p_single_id, rst_size);

        for (idx = rst_size; idx < k; idx++) {
            p_single_dis[idx] = float(1.0 / 0.0);
            p_single_id[idx] = -1;
        }
    }

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

int64_t
IndexHNSW::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->cur_element_count;
}

int64_t
IndexHNSW::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return (*(size_t*)index_->dist_func_param_);
}

void
IndexHNSW::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_->cal_size();
}

}  // namespace knowhere
}  // namespace milvus
