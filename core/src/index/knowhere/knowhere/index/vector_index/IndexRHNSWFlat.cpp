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

#include "knowhere/index/vector_index/IndexRHNSWFlat.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "faiss/BuilderSuspend.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

IndexRHNSWFlat::IndexRHNSWFlat(int d, int M, milvus::knowhere::MetricType metric) {
    faiss::MetricType mt =
        metric == Metric::L2 ? faiss::MetricType::METRIC_L2 : faiss::MetricType::METRIC_INNER_PRODUCT;
    index_ = std::shared_ptr<faiss::Index>(new faiss::IndexRHNSWFlat(d, M, mt));
}

BinarySet
IndexRHNSWFlat::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        auto res_set = IndexRHNSW::Serialize(config);
        auto real_idx = dynamic_cast<faiss::IndexRHNSWFlat*>(index_.get());
        if (real_idx == nullptr) {
            KNOWHERE_THROW_MSG("index is not a faiss::IndexRHNSWFlat");
        }

        auto write_meta = [&](const std::string& key, const int64_t& value) {
            auto space = reinterpret_cast<uint8_t*>(malloc(sizeof(int64_t)));
            memcpy(space, &value, sizeof(int64_t));
            std::shared_ptr<uint8_t[]> space_sp(space, std::default_delete<uint8_t[]>());
            res_set.Append(key, space_sp, sizeof(int64_t));
        };

        write_meta("metric_type", (int64_t)(real_idx->storage->metric_type));
        write_meta("dimension", (int64_t)(real_idx->storage->d));
        write_meta("ntotal", (int64_t)(real_idx->storage->ntotal));
        if (config.contains(INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
            Disassemble(config[INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>() * 1024 * 1024, res_set);
        }
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSWFlat::Load(const BinarySet& index_binary) {
    try {
        Assemble(const_cast<BinarySet&>(index_binary));
        IndexRHNSW::Load(index_binary);
        auto real_idx = dynamic_cast<faiss::IndexRHNSWFlat*>(index_.get());
        auto read_meta = [&](const std::string& key, int64_t& value) {
            auto meta_data = index_binary.GetByName(key);
            memcpy(&value, meta_data->data.get(), meta_data->size);
        };
        int64_t metric_type, dim, ntotal;
        read_meta("metric_type", metric_type);
        read_meta("dimension", dim);
        read_meta("ntotal", ntotal);
        switch ((faiss::MetricType)metric_type) {
            case faiss::MetricType::METRIC_L2:
                real_idx->storage = new faiss::IndexFlatL2();
                break;
            case faiss::MetricType::METRIC_INNER_PRODUCT:
                real_idx->storage = new faiss::IndexFlatIP();
                break;
            default:
                real_idx->storage = new faiss::IndexFlat();
                break;
        }
        real_idx->storage->ntotal = ntotal;
        real_idx->storage->d = (int)dim;
        real_idx->storage->metric_type = (faiss::MetricType)metric_type;
        auto binary_data = index_binary.GetByName(RAW_DATA);
        real_idx->storage->add(ntotal, reinterpret_cast<const float*>(binary_data->data.get()));
        real_idx->init_hnsw();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSWFlat::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    try {
        GET_TENSOR_DATA_DIM(dataset_ptr)
        faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());

        auto idx = new faiss::IndexRHNSWFlat(int(dim), config[IndexParams::M], metric_type);
        idx->hnsw.efConstruction = config[IndexParams::efConstruction];
        index_ = std::shared_ptr<faiss::Index>(idx);
        index_->train(rows, reinterpret_cast<const float*>(p_data));
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSWFlat::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = dynamic_cast<faiss::IndexRHNSWFlat*>(index_.get())->cal_size();
}

}  // namespace knowhere
}  // namespace milvus
