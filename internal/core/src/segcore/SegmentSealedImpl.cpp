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
#include "query/ScalarIndex.h"
#include "query/SearchBruteForce.h"

namespace milvus::segcore {

static inline void
set_bit(boost::dynamic_bitset<>& bitset, FieldOffset field_offset, bool flag = true) {
    bitset[field_offset.get()] = flag;
}

static inline bool
get_bit(const boost::dynamic_bitset<>& bitset, FieldOffset field_offset) {
    return bitset[field_offset.get()];
}

void
SegmentSealedImpl::LoadIndex(const LoadIndexInfo& info) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto field_offset = schema_->get_offset(field_id);

    AssertInfo(info.index_params.count("metric_type"), "Can't get metric_type in index_params");
    auto metric_type_str = info.index_params.at("metric_type");
    auto row_count = info.index->Count();
    AssertInfo(row_count > 0, "Index count is 0");

    std::unique_lock lck(mutex_);
    AssertInfo(!get_bit(vecindex_ready_bitset_, field_offset),
               "Can't get bitset element at " + std::to_string(field_offset.get()));
    if (row_count_opt_.has_value()) {
        AssertInfo(row_count_opt_.value() == row_count, "load data has different row count from other columns");
    } else {
        row_count_opt_ = row_count;
    }
    AssertInfo(!vecindexs_.is_ready(field_offset), "vec index is not ready");
    vecindexs_.append_field_indexing(field_offset, GetMetricType(metric_type_str), info.index);

    set_bit(vecindex_ready_bitset_, field_offset, true);
    lck.unlock();
}

void
SegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& info) {
    // NOTE: lock only when data is ready to avoid starvation
    AssertInfo(info.row_count > 0, "The row count of field data is 0");
    auto field_id = FieldId(info.field_id);
    AssertInfo(info.blob, "Field info blob is null");
    auto create_index = [](const int64_t* data, int64_t size) {
        AssertInfo(size, "Vector data size is 0 when create index");
        auto pk_index = std::make_unique<ScalarIndexVector>();
        pk_index->append_data(data, size, SegOffset(0));
        pk_index->build();
        return pk_index;
    };

    if (SystemProperty::Instance().IsSystem(field_id)) {
        auto system_field_type = SystemProperty::Instance().GetSystemFieldType(field_id);
        if (system_field_type == SystemFieldType::Timestamp) {
            auto src_ptr = reinterpret_cast<const Timestamp*>(info.blob);
            aligned_vector<Timestamp> vec_data(info.row_count);
            std::copy_n(src_ptr, info.row_count, vec_data.data());

            auto size = info.row_count;

            // TODO: load from outside
            TimestampIndex index;
            auto min_slice_length = size < 4096 ? 1 : 4096;
            auto meta = GenerateFakeSlices(src_ptr, size, min_slice_length);
            index.set_length_meta(std::move(meta));
            index.build_with(src_ptr, size);

            // use special index
            std::unique_lock lck(mutex_);
            update_row_count(info.row_count);
            AssertInfo(timestamps_.empty(), "already exists");
            timestamps_ = std::move(vec_data);
            timestamp_index_ = std::move(index);

        } else {
            AssertInfo(system_field_type == SystemFieldType::RowId, "System field type of id column is not RowId");
            auto src_ptr = reinterpret_cast<const idx_t*>(info.blob);

            // prepare data
            aligned_vector<idx_t> vec_data(info.row_count);
            std::copy_n(src_ptr, info.row_count, vec_data.data());

            std::unique_ptr<ScalarIndexBase> pk_index_;
            // fix unintentional index update
            if (schema_->get_is_auto_id()) {
                pk_index_ = create_index(vec_data.data(), vec_data.size());
            }

            // write data under lock
            std::unique_lock lck(mutex_);
            update_row_count(info.row_count);
            AssertInfo(row_ids_.empty(), "already exists");
            row_ids_ = std::move(vec_data);

            if (schema_->get_is_auto_id()) {
                primary_key_index_ = std::move(pk_index_);
            }
        }
        ++system_ready_count_;
    } else {
        // prepare data
        auto field_offset = schema_->get_offset(field_id);
        auto& field_meta = schema_->operator[](field_offset);
        // Assert(!field_meta.is_vector());
        auto element_sizeof = field_meta.get_sizeof();
        auto span = SpanBase(info.blob, info.row_count, element_sizeof);
        auto length_in_bytes = element_sizeof * info.row_count;
        aligned_vector<char> vec_data(length_in_bytes);
        memcpy(vec_data.data(), info.blob, length_in_bytes);

        // generate scalar index
        std::unique_ptr<knowhere::Index> index;
        if (!field_meta.is_vector()) {
            index = query::generate_scalar_index(span, field_meta.get_data_type());
        }

        std::unique_ptr<ScalarIndexBase> pk_index_;
        if (schema_->get_primary_key_offset() == field_offset) {
            pk_index_ = create_index((const int64_t*)vec_data.data(), info.row_count);
        }

        // write data under lock
        std::unique_lock lck(mutex_);
        update_row_count(info.row_count);
        AssertInfo(fields_data_[field_offset.get()].empty(), "field data already exists");

        if (field_meta.is_vector()) {
            AssertInfo(!vecindexs_.is_ready(field_offset), "field data can't be loaded when indexing exists");
            fields_data_[field_offset.get()] = std::move(vec_data);
        } else {
            AssertInfo(!scalar_indexings_[field_offset.get()], "scalar indexing not cleared");
            fields_data_[field_offset.get()] = std::move(vec_data);
            scalar_indexings_[field_offset.get()] = std::move(index);
        }

        if (schema_->get_primary_key_offset() == field_offset) {
            primary_key_index_ = std::move(pk_index_);
        }

        set_bit(field_data_ready_bitset_, field_offset, true);
    }
}

int64_t
SegmentSealedImpl::num_chunk_index(FieldOffset field_offset) const {
    return 1;
}

int64_t
SegmentSealedImpl::num_chunk() const {
    return 1;
}

int64_t
SegmentSealedImpl::size_per_chunk() const {
    return get_row_count();
}

SpanBase
SegmentSealedImpl::chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_offset),
               "Can't get bitset element at " + std::to_string(field_offset.get()));
    auto& field_meta = schema_->operator[](field_offset);
    auto element_sizeof = field_meta.get_sizeof();
    SpanBase base(fields_data_[field_offset.get()].data(), row_count_opt_.value(), element_sizeof);
    return base;
}

const knowhere::Index*
SegmentSealedImpl::chunk_index_impl(FieldOffset field_offset, int64_t chunk_id) const {
    AssertInfo(chunk_id == 0, "Chunk_id is not equal to 0");
    // TODO: support scalar index
    auto ptr = scalar_indexings_[field_offset.get()].get();
    AssertInfo(ptr, "Scalar index of " + std::to_string(field_offset.get()) + " is null");
    return ptr;
}

int64_t
SegmentSealedImpl::GetMemoryUsageInBytes() const {
    // TODO: add estimate for index
    std::shared_lock lck(mutex_);
    auto row_count = row_count_opt_.value_or(0);
    return schema_->get_total_sizeof() * row_count;
}

int64_t
SegmentSealedImpl::get_row_count() const {
    std::shared_lock lck(mutex_);
    return row_count_opt_.value_or(0);
}

const Schema&
SegmentSealedImpl::get_schema() const {
    return *schema_;
}

void
SegmentSealedImpl::vector_search(int64_t vec_count,
                                 query::SearchInfo search_info,
                                 const void* query_data,
                                 int64_t query_count,
                                 Timestamp timestamp,
                                 const BitsetView& bitset,
                                 SearchResult& output) const {
    AssertInfo(is_system_field_ready(), "System field is not ready");
    auto field_offset = search_info.field_offset_;
    auto& field_meta = schema_->operator[](field_offset);

    AssertInfo(field_meta.is_vector(), "The meta type of vector field is not vector type");
    if (get_bit(vecindex_ready_bitset_, field_offset)) {
        AssertInfo(vecindexs_.is_ready(field_offset),
                   "vector indexes isn't ready for field " + std::to_string(field_offset.get()));
        query::SearchOnSealed(*schema_, vecindexs_, search_info, query_data, query_count, bitset, output);
        return;
    } else if (!get_bit(field_data_ready_bitset_, field_offset)) {
        PanicInfo("Field Data is not loaded");
    }

    query::dataset::SearchDataset dataset;
    dataset.query_data = query_data;
    dataset.num_queries = query_count;
    // if(field_meta.is)
    dataset.metric_type = search_info.metric_type_;
    dataset.topk = search_info.topk_;
    dataset.dim = field_meta.get_dim();
    dataset.round_decimal = search_info.round_decimal_;

    AssertInfo(get_bit(field_data_ready_bitset_, field_offset),
               "Can't get bitset element at " + std::to_string(field_offset.get()));
    AssertInfo(row_count_opt_.has_value(), "Can't get row count value");
    auto row_count = row_count_opt_.value();
    auto chunk_data = fields_data_[field_offset.get()].data();

    auto sub_qr = [&] {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return query::FloatSearchBruteForce(dataset, chunk_data, row_count, bitset);
        } else {
            return query::BinarySearchBruteForce(dataset, chunk_data, row_count, bitset);
        }
    }();

    SearchResult results;
    results.result_distances_ = std::move(sub_qr.mutable_values());
    results.internal_seg_offsets_ = std::move(sub_qr.mutable_labels());
    results.topk_ = dataset.topk;
    results.num_queries_ = dataset.num_queries;

    output = std::move(results);
}

void
SegmentSealedImpl::DropFieldData(const FieldId field_id) {
    if (SystemProperty::Instance().IsSystem(field_id)) {
        auto system_field_type = SystemProperty::Instance().GetSystemFieldType(field_id);

        std::unique_lock lck(mutex_);
        --system_ready_count_;
        if (system_field_type == SystemFieldType::RowId) {
            auto row_ids = std::move(row_ids_);
        } else if (system_field_type == SystemFieldType::Timestamp) {
            auto ts = std::move(timestamps_);
        }
        lck.unlock();
    } else {
        auto field_offset = schema_->get_offset(field_id);
        auto& field_meta = schema_->operator[](field_offset);

        std::unique_lock lck(mutex_);
        set_bit(field_data_ready_bitset_, field_offset, false);
        auto vec = std::move(fields_data_[field_offset.get()]);
        lck.unlock();

        vec.clear();
    }
}

void
SegmentSealedImpl::DropIndex(const FieldId field_id) {
    AssertInfo(!SystemProperty::Instance().IsSystem(field_id),
               "Field id:" + std::to_string(field_id.get()) + " isn't one of system type when drop index");
    auto field_offset = schema_->get_offset(field_id);
    auto& field_meta = schema_->operator[](field_offset);
    AssertInfo(field_meta.is_vector(),
               "Field meta of offset:" + std::to_string(field_offset.get()) + " is not vector type");

    std::unique_lock lck(mutex_);
    vecindexs_.drop_field_indexing(field_offset);
    set_bit(vecindex_ready_bitset_, field_offset, false);
}

void
SegmentSealedImpl::check_search(const query::Plan* plan) const {
    AssertInfo(plan, "Search plan is null");
    AssertInfo(plan->extra_info_opt_.has_value(), "Extra info of search plan doesn't have value");

    if (!is_system_field_ready()) {
        PanicInfo("System Field RowID or Timestamp is not loaded");
    }

    auto& request_fields = plan->extra_info_opt_.value().involved_fields_;
    auto field_ready_bitset = field_data_ready_bitset_ | vecindex_ready_bitset_;
    AssertInfo(request_fields.size() == field_ready_bitset.size(),
               "Request fields size not equal to field ready bitset size when check search");
    auto absent_fields = request_fields - field_ready_bitset;

    if (absent_fields.any()) {
        auto field_offset = FieldOffset(absent_fields.find_first());
        auto& field_meta = schema_->operator[](field_offset);
        PanicInfo("User Field(" + field_meta.get_name().get() + ") is not loaded");
    }
}

SegmentSealedImpl::SegmentSealedImpl(SchemaPtr schema)
    : schema_(schema),
      fields_data_(schema->size()),
      field_data_ready_bitset_(schema->size()),
      vecindex_ready_bitset_(schema->size()),
      scalar_indexings_(schema->size()) {
}
void
SegmentSealedImpl::bulk_subscript(SystemFieldType system_type,
                                  const int64_t* seg_offsets,
                                  int64_t count,
                                  void* output) const {
    AssertInfo(is_system_field_ready(), "System field isn't ready when do bulk_insert");
    AssertInfo(system_type == SystemFieldType::RowId, "System field type of id column is not RowId");
    bulk_subscript_impl<int64_t>(row_ids_.data(), seg_offsets, count, output);
}
template <typename T>
void
SegmentSealedImpl::bulk_subscript_impl(const void* src_raw, const int64_t* seg_offsets, int64_t count, void* dst_raw) {
    static_assert(IsScalar<T>);
    auto src = reinterpret_cast<const T*>(src_raw);
    auto dst = reinterpret_cast<T*>(dst_raw);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst[i] = offset == -1 ? -1 : src[offset];
    }
}

// for vector
void
SegmentSealedImpl::bulk_subscript_impl(
    int64_t element_sizeof, const void* src_raw, const int64_t* seg_offsets, int64_t count, void* dst_raw) {
    auto src_vec = reinterpret_cast<const char*>(src_raw);
    auto dst_vec = reinterpret_cast<char*>(dst_raw);
    std::vector<char> none(element_sizeof, 0);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        auto dst = dst_vec + i * element_sizeof;
        const char* src;
        if (offset != -1) {
            src = src_vec + element_sizeof * offset;
        } else {
            src = none.data();
        }
        memcpy(dst, src, element_sizeof);
    }
}

void
SegmentSealedImpl::bulk_subscript(FieldOffset field_offset,
                                  const int64_t* seg_offsets,
                                  int64_t count,
                                  void* output) const {
    // Assert(get_bit(field_data_ready_bitset_, field_offset));
    if (!get_bit(field_data_ready_bitset_, field_offset)) {
        return;
    }
    auto& field_meta = schema_->operator[](field_offset);
    auto src_vec = fields_data_[field_offset.get()].data();
    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            bulk_subscript_impl<bool>(src_vec, seg_offsets, count, output);
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t>(src_vec, seg_offsets, count, output);
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t>(src_vec, seg_offsets, count, output);
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(src_vec, seg_offsets, count, output);
            break;
        }
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(src_vec, seg_offsets, count, output);
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(src_vec, seg_offsets, count, output);
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(src_vec, seg_offsets, count, output);
            break;
        }

        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY: {
            bulk_subscript_impl(field_meta.get_sizeof(), src_vec, seg_offsets, count, output);
            break;
        }

        default: {
            PanicInfo("unsupported");
        }
    }
}

bool
SegmentSealedImpl::HasIndex(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    AssertInfo(!SystemProperty::Instance().IsSystem(field_id),
               "Field id:" + std::to_string(field_id.get()) + " isn't one of system type when drop index");
    auto field_offset = schema_->get_offset(field_id);
    return get_bit(vecindex_ready_bitset_, field_offset);
}

bool
SegmentSealedImpl::HasFieldData(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    if (SystemProperty::Instance().IsSystem(field_id)) {
        return is_system_field_ready();
    } else {
        auto field_offset = schema_->get_offset(field_id);
        return get_bit(field_data_ready_bitset_, field_offset);
    }
}

std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
SegmentSealedImpl::search_ids(const IdArray& id_array, Timestamp timestamp) const {
    AssertInfo(id_array.has_int_id(), "string ids are not implemented");
    auto arr = id_array.int_id();
    AssertInfo(primary_key_index_, "Primary key index is null");
    return primary_key_index_->do_search_ids(id_array);
}

std::vector<SegOffset>
SegmentSealedImpl::search_ids(const boost::dynamic_bitset<>& bitset, Timestamp timestamp) const {
    std::vector<SegOffset> dst_offset;
    for (int i = 0; i < bitset.size(); i++) {
        if (bitset[i]) {
            dst_offset.emplace_back(SegOffset(i));
        }
    }
    return std::move(dst_offset);
}

std::string
SegmentSealedImpl::debug() const {
    std::string log_str;
    log_str += "Sealed\n";
    log_str += "Index:" + primary_key_index_->debug();
    log_str += "\n";
    return log_str;
}

void
SegmentSealedImpl::LoadSegmentMeta(const proto::segcore::LoadSegmentMeta& segment_meta) {
    std::unique_lock lck(mutex_);
    std::vector<int64_t> slice_lengths;
    for (auto& info : segment_meta.metas()) {
        slice_lengths.push_back(info.row_count());
    }
    timestamp_index_.set_length_meta(std::move(slice_lengths));
    PanicInfo("unimplemented");
}
int64_t
SegmentSealedImpl::get_active_count(Timestamp ts) const {
    // TODO optimize here to reduce expr search range
    return this->get_row_count();
}
void
SegmentSealedImpl::mask_with_timestamps(boost::dynamic_bitset<>& bitset_chunk, Timestamp timestamp) const {
    // TODO change the
    AssertInfo(this->timestamps_.size() == get_row_count(), "Timestamp size not equal to row count");
    auto range = timestamp_index_.get_active_range(timestamp);

    // range == (size_, size_) and size_ is this->timestamps_.size().
    // it means these data are all useful, we don't need to update bitset_chunk.
    // It can be thought of as an AND operation with another bitmask that is all 1s, but it is not necessary to do so.
    if (range.first == range.second && range.first == this->timestamps_.size()) {
        // just skip
        return;
    }
    // range == (0, 0). it means these data can not be used, directly set bitset_chunk to all 0s.
    // It can be thought of as an AND operation with another bitmask that is all 0s.
    if (range.first == range.second && range.first == 0) {
        bitset_chunk.reset();
        return;
    }
    auto mask = TimestampIndex::GenerateBitset(timestamp, range, this->timestamps_.data(), this->timestamps_.size());
    bitset_chunk &= mask;
}

SegmentSealedPtr
CreateSealedSegment(SchemaPtr schema) {
    return std::make_unique<SegmentSealedImpl>(schema);
}

}  // namespace milvus::segcore
