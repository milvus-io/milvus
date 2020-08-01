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

#include "knowhere/index/vector_index/IndexRHNSWSQ.h"

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
IndexRHNSWSQ::Serialize(const Config& config) {
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
IndexRHNSWSQ::Load(const BinarySet& index_binary) {
    try {
        auto binary = index_binary.GetByName("HNSW");

        MemoryIOReader reader;
        reader.total = binary->size;
        reader.data_ = binary->data.get();

        hnswlib::SpaceInterface<float>* space;
        index_ = std::make_shared<hnswlib::HierarchicalNSW<float>>(space);
        index_->loadIndex(reader);

        normalize = index_->metric_type_ == 1;  // 1 == InnerProduct
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSWSQ::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    try {
        GET_TENSOR_DATA_DIM(dataset_ptr)
        faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());

        index_ = std::shared_ptr<faiss::Index>(new faiss::IndexRHNSWSQ(int(dim), faiss::QuantizerType::QT_8bit, config[IndexParams::M], metric_type));
        index_->train(rows, (float*)p_data);
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSWSQ::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = index_->cal_size();
}

}  // namespace knowhere
}  // namespace milvus
