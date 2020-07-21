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

#include "knowhere/index/vector_offset_index/IndexHNSW_NM.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <utility>
#include <vector>

#include "faiss/BuilderSuspend.h"
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
IndexHNSW_NM::Serialize(const Config& config) {
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
IndexHNSW_NM::Load(const BinarySet& index_binary) {
    try {
        auto binary = index_binary.GetByName("HNSW");

        MemoryIOReader reader;
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        hnswlib_nm::SpaceInterface<float>* space;
        index_ = std::make_shared<hnswlib_nm::HierarchicalNSW_NM<float>>(space);
        index_->loadIndex(reader);

        normalize = (index_->metric_type_ == 1);  // 1 == InnerProduct

        data_ = index_binary.GetByName(RAW_DATA)->data;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW_NM::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    try {
        int64_t dim = dataset_ptr->Get<int64_t>(meta::DIM);
        int64_t rows = dataset_ptr->Get<int64_t>(meta::ROWS);

        hnswlib_nm::SpaceInterface<float>* space;
        if (config[Metric::TYPE] == Metric::L2) {
            space = new hnswlib_nm::L2Space(dim);
        } else if (config[Metric::TYPE] == Metric::IP) {
            space = new hnswlib_nm::InnerProductSpace(dim);
            normalize = true;
        }
        index_ = std::make_shared<hnswlib_nm::HierarchicalNSW_NM<float>>(
            space, rows, config[IndexParams::M].get<int64_t>(), config[IndexParams::efConstruction].get<int64_t>());
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexHNSW_NM::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    // It will not call Query() just after Add()
    // So, not to set 'data_' is allowed.

    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }

    std::lock_guard<std::mutex> lk(mutex_);

    GET_TENSOR_DATA_ID(dataset_ptr)

    auto base = index_->getCurrentElementCount();
    auto pp_data = const_cast<void*>(p_data);
    index_->addPoint(pp_data, p_ids[0], base, 0);
#pragma omp parallel for
    for (int i = 1; i < rows; ++i) {
        faiss::BuilderSuspend::check_wait();
        index_->addPoint(pp_data, p_ids[i], base, i);
    }
}

DatasetPtr
IndexHNSW_NM::Query(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    GET_TENSOR_DATA_DIM(dataset_ptr)

    size_t k = config[meta::TOPK].get<int64_t>();
    size_t id_size = sizeof(int64_t) * k;
    size_t dist_size = sizeof(float) * k;
    auto p_id = (int64_t*)malloc(id_size * rows);
    auto p_dist = (float*)malloc(dist_size * rows);

    index_->setEf(config[IndexParams::ef]);

    using P = std::pair<float, int64_t>;
    auto compare = [](const P& v1, const P& v2) { return v1.first < v2.first; };

    faiss::ConcurrentBitsetPtr blacklist = GetBlacklist();
#pragma omp parallel for
    for (unsigned int i = 0; i < rows; ++i) {
        std::vector<P> ret;
        const float* single_query = (float*)p_data + i * dim;

        ret = index_->searchKnn_NM((void*)single_query, k, compare, blacklist, (float*)(data_.get()));

        while (ret.size() < k) {
            ret.emplace_back(std::make_pair(-1, -1));
        }
        std::vector<float> dist;
        std::vector<int64_t> ids;

        if (normalize) {
            std::transform(ret.begin(), ret.end(), std::back_inserter(dist),
                           [](const std::pair<float, int64_t>& e) { return float(1 - e.first); });
        } else {
            std::transform(ret.begin(), ret.end(), std::back_inserter(dist),
                           [](const std::pair<float, int64_t>& e) { return e.first; });
        }
        std::transform(ret.begin(), ret.end(), std::back_inserter(ids),
                       [](const std::pair<float, int64_t>& e) { return e.second; });

        memcpy(p_dist + i * k, dist.data(), dist_size);
        memcpy(p_id + i * k, ids.data(), id_size);
    }

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

int64_t
IndexHNSW_NM::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->cur_element_count;
}

int64_t
IndexHNSW_NM::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return (*(size_t*)index_->dist_func_param_);
}

void
IndexHNSW_NM::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_->cal_size();
}

}  // namespace knowhere
}  // namespace milvus
