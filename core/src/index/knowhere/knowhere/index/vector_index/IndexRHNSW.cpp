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

#include "knowhere/index/vector_index/IndexRHNSW.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <utility>
#include <vector>

#include "faiss/BuilderSuspend.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

BinarySet
IndexRHNSW::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        MemoryIOWriter writer;
        writer.name = this->index_type() + "_Index";
        faiss::write_index(index_.get(), &writer);
        std::shared_ptr<uint8_t[]> data(writer.data_);

        BinarySet res_set;
        res_set.Append(writer.name, data, writer.rp);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSW::Load(const BinarySet& index_binary) {
    try {
        MemoryIOReader reader;
        reader.name = this->index_type() + "_Index";
        auto binary = index_binary.GetByName(reader.name);

        reader.total = static_cast<size_t>(binary->size);
        reader.data_ = binary->data.get();

        auto idx = faiss::read_index(&reader);
        index_.reset(idx);
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSW::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    KNOWHERE_THROW_MSG("IndexRHNSW has no implementation of Train, please use IndexRHNSW(Flat/SQ/PQ) instead!");
}

void
IndexRHNSW::Add(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    GET_TENSOR_DATA(dataset_ptr)

    index_->add(rows, reinterpret_cast<const float*>(p_data));
}

DatasetPtr
IndexRHNSW::Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::ConcurrentBitsetPtr& bitset) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    GET_TENSOR_DATA(dataset_ptr)

    auto k = config[meta::TOPK].get<int64_t>();
    int64_t id_size = sizeof(int64_t) * k;
    int64_t dist_size = sizeof(float) * k;
    auto p_id = static_cast<int64_t*>(malloc(id_size * rows));
    auto p_dist = static_cast<float*>(malloc(dist_size * rows));
    auto hnsw_stats = std::dynamic_pointer_cast<RHNSWStatistics>(stats);
    hnsw_stats->bitset_percentage1_sum += (double)bitset->count_1() / bitset->count();
    hnsw_stats->nq_cnt += rows;
    for (auto i = 0; i < k * rows; ++i) {
        p_id[i] = -1;
        p_dist[i] = -1;
    }

    auto real_index = dynamic_cast<faiss::IndexRHNSW*>(index_.get());

    real_index->hnsw.efSearch = (config[IndexParams::ef]);
    real_index->search(rows, reinterpret_cast<const float*>(p_data), k, p_dist, p_id, bitset);

    auto ret_ds = std::make_shared<Dataset>();
    ret_ds->Set(meta::IDS, p_id);
    ret_ds->Set(meta::DISTANCE, p_dist);
    return ret_ds;
}

int64_t
IndexRHNSW::Count() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->ntotal;
}

int64_t
IndexRHNSW::Dim() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    return index_->d;
}

StatisticsPtr
IndexRHNSW::GetStatistics() {
    auto hnsw_stats = std::dynamic_pointer_cast<RHNSWStatistics>(stats);
    auto real_index = dynamic_cast<faiss::IndexRHNSW*>(index_.get());
    real_index->calculate_stats(hnsw_stats->access_gini_coefficient);
    return hnsw_stats;
}

void
IndexRHNSW::UpdateIndexSize() {
    KNOWHERE_THROW_MSG(
        "IndexRHNSW has no implementation of UpdateIndexSize, please use IndexRHNSW(Flat/SQ/PQ) instead!");
}

/*
BinarySet
IndexRHNSW::SerializeImpl(const milvus::knowhere::IndexType &type) { return BinarySet(); }

void
IndexRHNSW::SealImpl() {}

void
IndexRHNSW::LoadImpl(const milvus::knowhere::BinarySet &, const milvus::knowhere::IndexType &type) {}
*/

void
IndexRHNSW::AddWithoutIds(const milvus::knowhere::DatasetPtr& dataset, const milvus::knowhere::Config& config) {
    KNOWHERE_THROW_MSG("IndexRHNSW has no implementation of AddWithoutIds, please use IndexRHNSW(Flat/SQ/PQ) instead!");
}
}  // namespace knowhere
}  // namespace milvus
