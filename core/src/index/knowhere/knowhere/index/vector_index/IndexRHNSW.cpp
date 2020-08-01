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
        MemoryIOWriter writer1, writer2;
        writer1.name = "IndexData";
        writer2.name = "RawData";
        faiss::write_index(index_.get(), &writer1);
        faiss::write_index(index_.get(), &writer2);
        std::shared_ptr<uint8_t[]> data1(writer1.data_);
        std::shared_ptr<uint8_t[]> data2(writer2.data_);

        BinarySet res_set;
        res_set.Append(writer1.name, data1, writer1.rp);
        res_set.Append(writer2.name, data2, writer2.rp);
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSW::Load(const BinarySet& index_binary) {
    try {
        MemoryIOReader reader1, reader2;
        reader1.name = "IndexData";
        reader2.name = "RawData";
        auto binary1 = index_binary.GetByName(reader1.name);
        auto binary2 = index_binary.GetByName(reader2.name);

        reader1.total = (size_t)binary1->size;
        reader1.data_ = binary1->data.get();
        reader2.total = (size_t)binary2->size;
        reader2.data_ = binary2->data.get();

        // todo: add interface in faiss: read_index(faiss::Index, &reader)
        faiss::Index* index1 = faiss::read_index(&reader1);
        faiss::Index* index2 = faiss::read_index(&reader2);
        auto check_index = dynamic_cast<faiss::IndexRHNSW*>(index_.get());

        index_.reset(index1);
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

    index_->add(rows, (float*)p_data);
}

DatasetPtr
IndexRHNSW::Query(const DatasetPtr& dataset_ptr, const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }
    GET_TENSOR_DATA(dataset_ptr)

    size_t k = config[meta::TOPK].get<int64_t>();
    size_t id_size = sizeof(int64_t) * k;
    size_t dist_size = sizeof(float) * k;
    auto p_id = (int64_t*)malloc(id_size * rows);
    auto p_dist = (float*)malloc(dist_size * rows);
    for (auto i = 0; i < k * rows; ++ i) {
        p_id[i] = -1;
        p_dist[i] = -1;
    }

    auto real_index = dynamic_cast<faiss::IndexRHNSW*>(index_.get());
    faiss::ConcurrentBitsetPtr blacklist = GetBlacklist();

    real_index->hnsw.efSearch = (config[IndexParams::ef]);
    real_index->search(rows, (float*)p_data, k, p_dist, p_id, blacklist);

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

void
IndexRHNSW::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_->cal_size();
}

}  // namespace knowhere
}  // namespace milvus
