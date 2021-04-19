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

#include "segcore/IndexingEntry.h"
#include <thread>
#include <knowhere/index/vector_index/IndexIVF.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>

namespace milvus::segcore {
void
VecIndexingEntry::BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) {
    assert(field_meta_.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field_meta_.get_dim();

    auto source = dynamic_cast<const ConcurrentVector<FloatVector>*>(vec_base);
    Assert(source);
    auto num_chunk = source->num_chunk();
    assert(ack_end <= num_chunk);
    auto conf = get_build_conf();
    data_.grow_to_at_least(ack_end);
    for (int chunk_id = ack_beg; chunk_id < ack_end; chunk_id++) {
        const auto& chunk = source->get_chunk(chunk_id);
        // build index for chunk
        auto indexing = std::make_unique<knowhere::IVF>();
        auto dataset = knowhere::GenDataset(source->get_chunk_size(), dim, chunk.data());
        indexing->Train(dataset, conf);
        indexing->AddWithoutIds(dataset, conf);
        data_[chunk_id] = std::move(indexing);
    }
}

knowhere::Config
VecIndexingEntry::get_build_conf() const {
    return knowhere::Config{{knowhere::meta::DIM, field_meta_.get_dim()},
                            {knowhere::IndexParams::nlist, 100},
                            {knowhere::IndexParams::nprobe, 4},
                            {knowhere::Metric::TYPE, MetricTypeToName(field_meta_.get_metric_type())},
                            {knowhere::meta::DEVICEID, 0}};
}

knowhere::Config
VecIndexingEntry::get_search_conf(int top_K) const {
    return knowhere::Config{{knowhere::meta::DIM, field_meta_.get_dim()},
                            {knowhere::meta::TOPK, top_K},
                            {knowhere::IndexParams::nlist, 100},
                            {knowhere::IndexParams::nprobe, 4},
                            {knowhere::Metric::TYPE, MetricTypeToName(field_meta_.get_metric_type())},
                            {knowhere::meta::DEVICEID, 0}};
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
    for (auto& [field_offset, entry] : entries_) {
        auto vec_base = record.get_base_entity(field_offset);
        entry->BuildIndexRange(old_ack, chunk_ack, vec_base);
    }
    finished_ack_.AddSegment(old_ack, chunk_ack);
    //    }).detach();
}

template <typename T>
void
ScalarIndexingEntry<T>::BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) {
    auto source = dynamic_cast<const ConcurrentVector<T>*>(vec_base);
    Assert(source);
    auto num_chunk = source->num_chunk();
    assert(ack_end <= num_chunk);
    data_.grow_to_at_least(ack_end);
    for (int chunk_id = ack_beg; chunk_id < ack_end; chunk_id++) {
        const auto& chunk = source->get_chunk(chunk_id);
        // build index for chunk
        // TODO
        auto indexing = std::make_unique<knowhere::scalar::StructuredIndexSort<T>>();
        indexing->Build(vec_base->get_chunk_size(), chunk.data());
        data_[chunk_id] = std::move(indexing);
    }
}

std::unique_ptr<IndexingEntry>
CreateIndex(const FieldMeta& field_meta, int64_t chunk_size) {
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return std::make_unique<VecIndexingEntry>(field_meta, chunk_size);
        } else {
            // TODO
            PanicInfo("unsupported");
        }
    }
    switch (field_meta.get_data_type()) {
        case DataType::BOOL:
            return std::make_unique<ScalarIndexingEntry<bool>>(field_meta, chunk_size);
        case DataType::INT8:
            return std::make_unique<ScalarIndexingEntry<int8_t>>(field_meta, chunk_size);
        case DataType::INT16:
            return std::make_unique<ScalarIndexingEntry<int16_t>>(field_meta, chunk_size);
        case DataType::INT32:
            return std::make_unique<ScalarIndexingEntry<int32_t>>(field_meta, chunk_size);
        case DataType::INT64:
            return std::make_unique<ScalarIndexingEntry<int64_t>>(field_meta, chunk_size);
        case DataType::FLOAT:
            return std::make_unique<ScalarIndexingEntry<float>>(field_meta, chunk_size);
        case DataType::DOUBLE:
            return std::make_unique<ScalarIndexingEntry<double>>(field_meta, chunk_size);
        default:
            PanicInfo("unsupported");
    }
}

}  // namespace milvus::segcore
