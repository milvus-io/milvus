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
#include "query/SearchOnSealed.h"
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
    Assert(info.blob);
    Assert(info.row_count > 0);
    if (SystemProperty::Instance().IsSystem(field_id)) {
        auto system_field_type = SystemProperty::Instance().GetSystemFieldType(field_id);
        Assert(system_field_type == SystemFieldType::RowId);
        auto src_ptr = reinterpret_cast<const idx_t*>(info.blob);

        // prepare data
        aligned_vector<idx_t> vec_data(info.row_count);
        std::copy_n(src_ptr, info.row_count, vec_data.data());

        // write data under lock
        std::unique_lock lck(mutex_);
        update_row_count(info.row_count);
        AssertInfo(row_ids_.empty(), "already exists");
        row_ids_ = std::move(vec_data);

        ++ready_count_;
    } else {
        // prepare data
        auto field_offset = schema_->get_offset(field_id);
        auto& field_meta = schema_->operator[](field_offset);
        Assert(!field_meta.is_vector());
        auto element_sizeof = field_meta.get_sizeof();
        auto length_in_bytes = element_sizeof * info.row_count;
        aligned_vector<char> vec_data(length_in_bytes);
        memcpy(vec_data.data(), info.blob, length_in_bytes);

        // write data under lock
        std::unique_lock lck(mutex_);
        update_row_count(info.row_count);
        AssertInfo(columns_data_[field_offset.get()].empty(), "already exists");
        columns_data_[field_offset.get()] = std::move(vec_data);

        ++ready_count_;
    }
}

int64_t
SegmentSealedImpl::num_chunk_index_safe(FieldOffset field_offset) const {
    // TODO: support scalar index
    return 0;
}

int64_t
SegmentSealedImpl::num_chunk_data() const {
    return 1;
}

int64_t
SegmentSealedImpl::size_per_chunk() const {
    return get_row_count();
}

SpanBase
SegmentSealedImpl::chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const {
    std::shared_lock lck(mutex_);
    auto& field_meta = schema_->operator[](field_offset);
    auto element_sizeof = field_meta.get_sizeof();
    Assert(is_all_ready());
    SpanBase base(columns_data_[field_offset.get()].data(), row_count_opt_.value(), element_sizeof);
    return base;
}

const knowhere::Index*
SegmentSealedImpl::chunk_index_impl(FieldOffset field_offset, int64_t chunk_id) const {
    // TODO: support scalar index
    return nullptr;
}

int64_t
SegmentSealedImpl::GetMemoryUsageInBytes() const {
    // TODO: add estimate for index
    auto row_count = row_count_opt_.value_or(0);
    return schema_->get_total_sizeof() * row_count;
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

void
SegmentSealedImpl::vector_search(int64_t vec_count,
                                 query::QueryInfo query_info,
                                 const void* query_data,
                                 int64_t query_count,
                                 const BitsetView& bitset,
                                 QueryResult& output) const {
    auto field_offset = query_info.field_offset_;
    auto& field_meta = schema_->operator[](field_offset);
    Assert(field_meta.is_vector());
    Assert(vec_indexings_.is_ready(field_offset));
    query::SearchOnSealed(*schema_, vec_indexings_, query_info, query_data, query_count, bitset, output);
}

SegmentSealedPtr
CreateSealedSegment(SchemaPtr schema, int64_t chunk_size) {
    return std::make_unique<SegmentSealedImpl>(schema);
}

}  // namespace milvus::segcore
