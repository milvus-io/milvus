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

#include <algorithm>
#include <numeric>
#include <queue>
#include <thread>
#include <boost/iterator/counting_iterator.hpp>

#include "common/Consts.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "query/PlanNode.h"
#include "query/SearchOnSealed.h"
#include "query/generated/ExecPlanNodeVisitor.h"
#include "segcore/Reduce.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"

namespace milvus::segcore {

int64_t
SegmentGrowingImpl::PreInsert(int64_t size) {
    auto reserved_begin = insert_record_.reserved.fetch_add(size);
    return reserved_begin;
}

int64_t
SegmentGrowingImpl::PreDelete(int64_t size) {
    auto reserved_begin = deleted_record_.reserved.fetch_add(size);
    return reserved_begin;
}

void
SegmentGrowingImpl::mask_with_delete(BitsetType& bitset, int64_t ins_barrier, Timestamp timestamp) const {
    auto del_barrier = get_barrier(get_deleted_record(), timestamp);
    if (del_barrier == 0) {
        return;
    }
    auto bitmap_holder =
        get_deleted_bitmap(del_barrier, ins_barrier, deleted_record_, insert_record_, pk2offset_, timestamp);
    if (!bitmap_holder || !bitmap_holder->bitmap_ptr) {
        return;
    }
    auto& delete_bitset = *bitmap_holder->bitmap_ptr;
    AssertInfo(delete_bitset.size() == bitset.size(), "Deleted bitmap size not equal to filtered bitmap size");
    bitset |= delete_bitset;
}

void
SegmentGrowingImpl::Insert(int64_t reserved_offset,
                           int64_t size,
                           const int64_t* row_ids,
                           const Timestamp* timestamps_raw,
                           const InsertData* insert_data) {
    AssertInfo(insert_data->num_rows() == size, "Entities_raw count not equal to insert size");
    //    AssertInfo(insert_data->fields_data_size() == schema_->size(),
    //               "num fields of insert data not equal to num of schema fields");
    // step 1: check insert data if valid
    std::unordered_map<FieldId, int64_t> field_id_to_offset;
    int64_t field_offset = 0;
    for (auto field : insert_data->fields_data()) {
        auto field_id = FieldId(field.field_id());
        AssertInfo(!field_id_to_offset.count(field_id), "duplicate field data");
        field_id_to_offset.emplace(field_id, field_offset++);
    }

    // step 2: sort timestamp
    // query node already guarantees that the timestamp is ordered, avoid field data copy in c++

    // step 3: fill into Segment.ConcurrentVector
    insert_record_.timestamps_.set_data_raw(reserved_offset, timestamps_raw, size);
    insert_record_.row_ids_.set_data_raw(reserved_offset, row_ids, size);
    for (auto [field_id, field_meta] : schema_->get_fields()) {
        AssertInfo(field_id_to_offset.count(field_id), "Cannot find field_id");
        auto data_offset = field_id_to_offset[field_id];
        insert_record_.get_field_data_base(field_id)->set_data_raw(reserved_offset, size,
                                                                   &insert_data->fields_data(data_offset), field_meta);
    }

    // step 4: set pks to offset
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    std::vector<PkType> pks(size);
    ParsePksFromFieldData(pks, insert_data->fields_data(field_id_to_offset[field_id]));
    for (int i = 0; i < size; ++i) {
        pk2offset_.insert(std::make_pair(pks[i], reserved_offset + i));
    }

    // step 5: update small indexes
    insert_record_.ack_responder_.AddSegment(reserved_offset, reserved_offset + size);
    if (enable_small_index_) {
        int64_t chunk_rows = segcore_config_.get_chunk_rows();
        indexing_record_.UpdateResourceAck(insert_record_.ack_responder_.GetAck() / chunk_rows, insert_record_);
    }
}

Status
SegmentGrowingImpl::Delete(int64_t reserved_begin, int64_t size, const IdArray* ids, const Timestamp* timestamps_raw) {
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_->operator[](field_id);
    std::vector<PkType> pks(size);
    ParsePksFromIDs(pks, field_meta.get_data_type(), *ids);

    // step 1: sort timestamp
    std::vector<std::tuple<Timestamp, PkType>> ordering(size);
    for (int i = 0; i < size; i++) {
        ordering[i] = std::make_tuple(timestamps_raw[i], pks[i]);
    }
    std::sort(ordering.begin(), ordering.end());
    std::vector<PkType> sort_pks(size);
    std::vector<Timestamp> sort_timestamps(size);

    for (int i = 0; i < size; i++) {
        auto [t, pk] = ordering[i];
        sort_timestamps[i] = t;
        sort_pks[i] = pk;
    }

    // step 2: fill delete record
    deleted_record_.timestamps_.set_data_raw(reserved_begin, sort_timestamps.data(), size);
    deleted_record_.pks_.set_data_raw(reserved_begin, sort_pks.data(), size);
    deleted_record_.ack_responder_.AddSegment(reserved_begin, reserved_begin + size);
    return Status::OK();
}

int64_t
SegmentGrowingImpl::GetMemoryUsageInBytes() const {
    int64_t total_bytes = 0;
    auto chunk_rows = segcore_config_.get_chunk_rows();
    int64_t ins_n = upper_align(insert_record_.reserved, chunk_rows);
    total_bytes += ins_n * (schema_->get_total_sizeof() + 16 + 1);
    int64_t del_n = upper_align(deleted_record_.reserved, chunk_rows);
    total_bytes += del_n * (16 * 2);
    return total_bytes;
}

SpanBase
SegmentGrowingImpl::chunk_data_impl(FieldId field_id, int64_t chunk_id) const {
    auto vec = get_insert_record().get_field_data_base(field_id);
    return vec->get_span_base(chunk_id);
}

int64_t
SegmentGrowingImpl::num_chunk() const {
    auto size = get_insert_record().ack_responder_.GetAck();
    return upper_div(size, segcore_config_.get_chunk_rows());
}

void
SegmentGrowingImpl::vector_search(int64_t vec_count,
                                  query::SearchInfo search_info,
                                  const void* query_data,
                                  int64_t query_count,
                                  Timestamp timestamp,
                                  const BitsetView& bitset,
                                  SearchResult& output) const {
    auto& sealed_indexing = this->get_sealed_indexing_record();
    if (sealed_indexing.is_ready(search_info.field_id_)) {
        query::SearchOnSealed(this->get_schema(), sealed_indexing, search_info, query_data, query_count, bitset, output,
                              id_);
    } else {
        SearchOnGrowing(*this, vec_count, search_info, query_data, query_count, bitset, output);
    }
}

std::unique_ptr<DataArray>
SegmentGrowingImpl::bulk_subscript(FieldId field_id, const int64_t* seg_offsets, int64_t count) const {
    // TODO: support more types
    auto vec_ptr = insert_record_.get_field_data_base(field_id);
    auto& field_meta = schema_->operator[](field_id);
    if (field_meta.is_vector()) {
        aligned_vector<char> output(field_meta.get_sizeof() * count);
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            bulk_subscript_impl<FloatVector>(field_meta.get_sizeof(), *vec_ptr, seg_offsets, count, output.data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            bulk_subscript_impl<BinaryVector>(field_meta.get_sizeof(), *vec_ptr, seg_offsets, count, output.data());
        } else {
            PanicInfo("logical error");
        }
        return CreateVectorDataArrayFrom(output.data(), count, field_meta);
    }

    AssertInfo(!field_meta.is_vector(), "Scalar field meta type is vector type");
    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            FixedVector<bool> output(count);
            bulk_subscript_impl<bool>(*vec_ptr, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT8: {
            FixedVector<bool> output(count);
            bulk_subscript_impl<int8_t>(*vec_ptr, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT16: {
            FixedVector<int16_t> output(count);
            bulk_subscript_impl<int16_t>(*vec_ptr, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT32: {
            FixedVector<int32_t> output(count);
            bulk_subscript_impl<int32_t>(*vec_ptr, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT64: {
            FixedVector<int64_t> output(count);
            bulk_subscript_impl<int64_t>(*vec_ptr, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::FLOAT: {
            FixedVector<float> output(count);
            bulk_subscript_impl<float>(*vec_ptr, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::DOUBLE: {
            FixedVector<double> output(count);
            bulk_subscript_impl<double>(*vec_ptr, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::VARCHAR: {
            FixedVector<std::string> output(count);
            bulk_subscript_impl<std::string>(*vec_ptr, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        default: {
            PanicInfo("unsupported type");
        }
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_impl(int64_t element_sizeof,
                                        const VectorBase& vec_raw,
                                        const int64_t* seg_offsets,
                                        int64_t count,
                                        void* output_raw) const {
    static_assert(IsVector<T>);
    auto vec_ptr = dynamic_cast<const ConcurrentVector<T>*>(&vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    std::vector<uint8_t> empty(element_sizeof, 0);
    auto output_base = reinterpret_cast<char*>(output_raw);
    for (int i = 0; i < count; ++i) {
        auto dst = output_base + i * element_sizeof;
        auto offset = seg_offsets[i];
        const uint8_t* src = (offset == INVALID_SEG_OFFSET ? empty.data() : (const uint8_t*)vec.get_element(offset));
        memcpy(dst, src, element_sizeof);
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_impl(const VectorBase& vec_raw,
                                        const int64_t* seg_offsets,
                                        int64_t count,
                                        void* output_raw) const {
    static_assert(IsScalar<T>);
    auto vec_ptr = dynamic_cast<const ConcurrentVector<T>*>(&vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    auto output = reinterpret_cast<T*>(output_raw);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset != INVALID_SEG_OFFSET) {
            output[i] = vec[offset];
        }
    }
}

void
SegmentGrowingImpl::bulk_subscript(SystemFieldType system_type,
                                   const int64_t* seg_offsets,
                                   int64_t count,
                                   void* output) const {
    switch (system_type) {
        case SystemFieldType::Timestamp:
            PanicInfo("timestamp unsupported");
        case SystemFieldType::RowId:
            bulk_subscript_impl<int64_t>(this->insert_record_.row_ids_, seg_offsets, count, output);
            break;
        default:
            PanicInfo("unknown subscript fields");
    }
}

std::vector<SegOffset>
SegmentGrowingImpl::search_ids(const BitsetType& bitset, Timestamp timestamp) const {
    std::vector<SegOffset> res_offsets;

    for (int i = 0; i < bitset.size(); i++) {
        if (bitset[i]) {
            auto offset = SegOffset(i);
            if (insert_record_.timestamps_[offset.get()] <= timestamp) {
                res_offsets.push_back(offset);
            }
        }
    }
    return res_offsets;
}

std::vector<SegOffset>
SegmentGrowingImpl::search_ids(const BitsetView& bitset, Timestamp timestamp) const {
    std::vector<SegOffset> res_offsets;

    for (int i = 0; i < bitset.size(); ++i) {
        if (!bitset.test(i)) {
            auto offset = SegOffset(i);
            if (insert_record_.timestamps_[offset.get()] <= timestamp) {
                res_offsets.push_back(offset);
            }
        }
    }
    return res_offsets;
}

std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
SegmentGrowingImpl::search_ids(const IdArray& id_array, Timestamp timestamp) const {
    AssertInfo(id_array.has_int_id(), "Id array doesn't have int_id element");
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_->operator[](field_id);
    auto data_type = field_meta.get_data_type();
    auto ids_size = GetSizeOfIdArray(id_array);
    std::vector<PkType> pks(ids_size);
    ParsePksFromIDs(pks, data_type, id_array);

    auto res_id_arr = std::make_unique<IdArray>();
    std::vector<SegOffset> res_offsets;
    for (auto pk : pks) {
        auto [iter_b, iter_e] = pk2offset_.equal_range(pk);
        for (auto iter = iter_b; iter != iter_e; ++iter) {
            auto offset = SegOffset(iter->second);
            if (insert_record_.timestamps_[offset.get()] <= timestamp) {
                switch (data_type) {
                    case DataType::INT64: {
                        res_id_arr->mutable_int_id()->add_data(std::get<int64_t>(pk));
                        break;
                    }
                    case DataType::VARCHAR: {
                        res_id_arr->mutable_str_id()->add_data(std::get<std::string>(pk));
                        break;
                    }
                    default: {
                        PanicInfo("unsupported type");
                    }
                }
                res_offsets.push_back(offset);
            }
        }
    }
    return {std::move(res_id_arr), std::move(res_offsets)};
}

std::string
SegmentGrowingImpl::debug() const {
    return "Growing\n";
}

int64_t
SegmentGrowingImpl::get_active_count(Timestamp ts) const {
    auto row_count = this->get_row_count();
    auto& ts_vec = this->get_insert_record().timestamps_;
    auto iter = std::upper_bound(boost::make_counting_iterator((int64_t)0), boost::make_counting_iterator(row_count),
                                 ts, [&](Timestamp ts, int64_t index) { return ts < ts_vec[index]; });
    return *iter;
}

void
SegmentGrowingImpl::mask_with_timestamps(BitsetType& bitset_chunk, Timestamp timestamp) const {
    // DO NOTHING
}

}  // namespace milvus::segcore
