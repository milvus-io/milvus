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

#include "segcore/SegmentSealedImpl.h"
namespace milvus::segcore {
void
SegmentSealedImpl::LoadIndex(const LoadIndexInfo& info) {
    auto field_id = FieldId(info.field_id);
    auto field_offset = schema_->get_offset(field_id);

    Assert(info.index_params.count("metric_type"));
    auto metric_type_str = info.index_params.at("metric_type");
    auto row_count = info.index->Count();
    Assert(row_count > 0);

    std::unique_lock lck(mutex_);
    if (row_count_opt_.has_value()) {
        AssertInfo(row_count_opt_.value() == row_count, "load data has different row count from other columns");
    } else {
        row_count_opt_ = row_count;
    }
    Assert(!vec_indexings_.is_ready(field_offset));
    vec_indexings_.add_entry(field_offset, GetMetricType(metric_type_str), info.index);
    ++ready_count_;
}

void
SegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& info) {
    // TODO
    Assert(info.row_count > 0);
    auto field_id = FieldId(info.field_id);
    auto field_offset = schema_->get_offset(field_id);
    auto& field_meta = schema_->operator[](field_offset);
    Assert(!field_meta.is_vector());
    auto element_sizeof = field_meta.get_sizeof();
    auto length_in_bytes = element_sizeof * info.row_count;
    aligned_vector<char> vecdata(length_in_bytes);
    memcpy(vecdata.data(), info.blob, length_in_bytes);
    std::unique_lock lck(mutex_);
    if (row_count_opt_.has_value()) {
        AssertInfo(row_count_opt_.value() == info.row_count, "load data has different row count from other columns");
    } else {
        row_count_opt_ = info.row_count;
    }
    AssertInfo(columns_data_[field_offset.get()].empty(), "already exists");
    columns_data_[field_offset.get()] = std::move(vecdata);
    ++ready_count_;
}

int64_t
SegmentSealedImpl::num_chunk_index_safe(FieldOffset field_offset) const {
    // TODO: support scalar index
    return 0;
}

int64_t
SegmentSealedImpl::num_chunk_data() const {
    PanicInfo("unimplemented");
}

int64_t
SegmentSealedImpl::size_per_chunk() const {
    PanicInfo("unimplemented");
}

SpanBase
SegmentSealedImpl::chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const {
    PanicInfo("unimplemented");
}

const knowhere::Index*
SegmentSealedImpl::chunk_index_impl(FieldOffset field_offset, int64_t chunk_id) const {
    PanicInfo("unimplemented");
}

void
SegmentSealedImpl::FillTargetEntry(const query::Plan* Plan, QueryResult& results) const {
    PanicInfo("unimplemented");
}

QueryResult
SegmentSealedImpl::Search(const query::Plan* Plan,
                          const query::PlaceholderGroup** placeholder_groups,
                          const Timestamp* timestamps,
                          int64_t num_groups) const {
    PanicInfo("unimplemented");
}

int64_t
SegmentSealedImpl::GetMemoryUsageInBytes() const {
    PanicInfo("unimplemented");
}

int64_t
SegmentSealedImpl::get_row_count() const {
    std::shared_lock lck(mutex_);
    AssertInfo(row_count_opt_.has_value(), "Data not loaded");
    return row_count_opt_.value();
}

const Schema&
SegmentSealedImpl::get_schema() const {
    return *schema_;
}

SegmentSealedPtr
CreateSealedSegment(SchemaPtr schema, int64_t chunk_size) {
    return std::make_unique<SegmentSealedImpl>(schema);
}

}  // namespace milvus::segcore
