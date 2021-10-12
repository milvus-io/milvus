// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/FieldIndexing.h"
#include <thread>
#include <knowhere/index/vector_index/IndexIVF.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <string>
#include "common/SystemProperty.h"

namespace milvus::segcore {
void
VectorFieldIndexing::BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) {
    AssertInfo(field_meta_.get_data_type() == DataType::VECTOR_FLOAT, "Data type of vector field is not VECTOR_FLOAT");
    auto dim = field_meta_.get_dim();

    auto source = dynamic_cast<const ConcurrentVector<FloatVector>*>(vec_base);
    AssertInfo(source, "vec_base can't cast to ConcurrentVector type");
    auto num_chunk = source->num_chunk();
    AssertInfo(ack_end <= num_chunk, "ack_end is bigger than num_chunk");
    auto conf = get_build_params();
    data_.grow_to_at_least(ack_end);
    for (int chunk_id = ack_beg; chunk_id < ack_end; chunk_id++) {
        const auto& chunk = source->get_chunk(chunk_id);
        // build index for chunk
        auto indexing = std::make_unique<knowhere::IVF>();
        auto dataset = knowhere::GenDataset(source->get_size_per_chunk(), dim, chunk.data());
        indexing->Train(dataset, conf);
        indexing->AddWithoutIds(dataset, conf);
        data_[chunk_id] = std::move(indexing);
    }
}

knowhere::Config
VectorFieldIndexing::get_build_params() const {
    // TODO
    auto type_opt = field_meta_.get_metric_type();
    AssertInfo(type_opt.has_value(), "Metric type of field meta doesn't have value");
    auto metric_type = type_opt.value();
    auto type_name = MetricTypeToName(metric_type);
    auto& config = segcore_config_.at(metric_type);
    auto base_params = config.build_params;

    AssertInfo(base_params.count("nlist"), "Can't get nlist from index params");
    base_params[knowhere::meta::DIM] = field_meta_.get_dim();
    base_params[knowhere::Metric::TYPE] = type_name;

    return base_params;
}

knowhere::Config
VectorFieldIndexing::get_search_params(int top_K) const {
    // TODO
    auto type_opt = field_meta_.get_metric_type();
    AssertInfo(type_opt.has_value(), "Metric type of field meta doesn't have value");
    auto metric_type = type_opt.value();
    auto type_name = MetricTypeToName(metric_type);
    auto& config = segcore_config_.at(metric_type);

    auto base_params = config.search_params;
    AssertInfo(base_params.count("nprobe"), "Can't get nprobe from base params");
    base_params[knowhere::meta::TOPK] = top_K;
    base_params[knowhere::Metric::TYPE] = type_name;

    return base_params;
}

void
IndexingRecord::UpdateResourceAck(int64_t chunk_ack, const InsertRecord& record) {
    if (resource_ack_ >= chunk_ack) {
        return;
    }

    std::unique_lock lck(mutex_);
    int64_t old_ack = resource_ack_;
    if (old_ack >= chunk_ack) {
        return;
    }
    resource_ack_ = chunk_ack;
    lck.unlock();

    //    std::thread([this, old_ack, chunk_ack, &record] {
    for (auto& [field_offset, entry] : field_indexings_) {
        auto vec_base = record.get_field_data_base(field_offset);
        entry->BuildIndexRange(old_ack, chunk_ack, vec_base);
    }
    finished_ack_.AddSegment(old_ack, chunk_ack);
    //    }).detach();
}

template <typename T>
void
ScalarFieldIndexing<T>::BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) {
    auto source = dynamic_cast<const ConcurrentVector<T>*>(vec_base);
    AssertInfo(source, "vec_base can't cast to ConcurrentVector type");
    auto num_chunk = source->num_chunk();
    AssertInfo(ack_end <= num_chunk, "Ack_end is bigger than num_chunk");
    data_.grow_to_at_least(ack_end);
    for (int chunk_id = ack_beg; chunk_id < ack_end; chunk_id++) {
        const auto& chunk = source->get_chunk(chunk_id);
        // build index for chunk
        // TODO
        auto indexing = std::make_unique<knowhere::scalar::StructuredIndexSort<T>>();
        indexing->Build(vec_base->get_size_per_chunk(), chunk.data());
        data_[chunk_id] = std::move(indexing);
    }
}

std::unique_ptr<FieldIndexing>
CreateIndex(const FieldMeta& field_meta, const SegcoreConfig& segcore_config) {
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return std::make_unique<VectorFieldIndexing>(field_meta, segcore_config);
        } else {
            // TODO
            PanicInfo("unsupported");
        }
    }
    switch (field_meta.get_data_type()) {
        case DataType::BOOL:
            return std::make_unique<ScalarFieldIndexing<bool>>(field_meta, segcore_config);
        case DataType::INT8:
            return std::make_unique<ScalarFieldIndexing<int8_t>>(field_meta, segcore_config);
        case DataType::INT16:
            return std::make_unique<ScalarFieldIndexing<int16_t>>(field_meta, segcore_config);
        case DataType::INT32:
            return std::make_unique<ScalarFieldIndexing<int32_t>>(field_meta, segcore_config);
        case DataType::INT64:
            return std::make_unique<ScalarFieldIndexing<int64_t>>(field_meta, segcore_config);
        case DataType::FLOAT:
            return std::make_unique<ScalarFieldIndexing<float>>(field_meta, segcore_config);
        case DataType::DOUBLE:
            return std::make_unique<ScalarFieldIndexing<double>>(field_meta, segcore_config);
        default:
            PanicInfo("unsupported");
    }
}

}  // namespace milvus::segcore
