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
#include <string>
#include <utility>
#include <vector>

#include "knowhere/common/Exception.h"
#include "knowhere/common/Log.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "knowhere/index/vector_index/helpers/FaissIO.h"

namespace milvus {
namespace knowhere {

IndexRHNSWSQ::IndexRHNSWSQ(int d, faiss::QuantizerType qtype, int M, milvus::knowhere::MetricType metric) {
    faiss::MetricType mt =
        metric == Metric::L2 ? faiss::MetricType::METRIC_L2 : faiss::MetricType::METRIC_INNER_PRODUCT;
    index_ = std::shared_ptr<faiss::Index>(new faiss::IndexRHNSWSQ(d, qtype, M, mt));
}

BinarySet
IndexRHNSWSQ::Serialize(const Config& config) {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize or trained");
    }

    try {
        auto res_set = IndexRHNSW::Serialize(config);
        MemoryIOWriter writer;
        writer.name = QUANTIZATION_DATA;
        auto real_idx = dynamic_cast<faiss::IndexRHNSWSQ*>(index_.get());
        if (real_idx == nullptr) {
            KNOWHERE_THROW_MSG("dynamic_cast<faiss::IndexRHNSWSQ*>(index_) failed during Serialize!");
        }
        faiss::write_index(real_idx->storage, &writer);
        std::shared_ptr<uint8_t[]> data(writer.data_);

        res_set.Append(writer.name, data, writer.rp);
        if (config.contains(INDEX_FILE_SLICE_SIZE_IN_MEGABYTE)) {
            Disassemble(config[INDEX_FILE_SLICE_SIZE_IN_MEGABYTE].get<int64_t>() * 1024 * 1024, res_set);
        }
        return res_set;
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSWSQ::Load(const BinarySet& index_binary) {
    try {
        Assemble(const_cast<BinarySet&>(index_binary));
        IndexRHNSW::Load(index_binary);
        MemoryIOReader reader;
        reader.name = QUANTIZATION_DATA;
        auto binary = index_binary.GetByName(reader.name);

        reader.total = static_cast<size_t>(binary->size);
        reader.data_ = binary->data.get();

        auto real_idx = dynamic_cast<faiss::IndexRHNSWSQ*>(index_.get());
        if (real_idx == nullptr) {
            KNOWHERE_THROW_MSG("dynamic_cast<faiss::IndexRHNSWSQ*>(index_) failed during Load!");
        }
        real_idx->storage = faiss::read_index(&reader);
        real_idx->init_hnsw();
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSWSQ::Train(const DatasetPtr& dataset_ptr, const Config& config) {
    try {
        GET_TENSOR_DATA_DIM(dataset_ptr)
        faiss::MetricType metric_type = GetMetricType(config[Metric::TYPE].get<std::string>());
        int32_t efConstruction = config[IndexParams::efConstruction];

        auto idx =
            new faiss::IndexRHNSWSQ(int(dim), faiss::QuantizerType::QT_8bit, config[IndexParams::M], metric_type);
        idx->hnsw.efConstruction = efConstruction;
        index_ = std::shared_ptr<faiss::Index>(idx);
        index_->train(rows, static_cast<const float*>(p_data));
    } catch (std::exception& e) {
        KNOWHERE_THROW_MSG(e.what());
    }
}

void
IndexRHNSWSQ::UpdateIndexSize() {
    if (!index_) {
        KNOWHERE_THROW_MSG("index not initialize");
    }
    index_size_ = dynamic_cast<faiss::IndexRHNSWSQ*>(index_.get())->cal_size();
}

}  // namespace knowhere
}  // namespace milvus
