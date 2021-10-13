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

#include <random>

#include <algorithm>
#include <numeric>
#include <thread>
#include <queue>

#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <faiss/utils/distances.h>
#include <query/SearchOnSealed.h>
#include <iostream>
#include "query/generated/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "query/PlanNode.h"
#include "query/PlanImpl.h"
#include "segcore/Reduce.h"
#include "utils/tools.h"
#include <boost/iterator/counting_iterator.hpp>

namespace milvus::segcore {

int64_t
SegmentGrowingImpl::PreInsert(int64_t size) {
    auto reserved_begin = record_.reserved.fetch_add(size);
    return reserved_begin;
}

int64_t
SegmentGrowingImpl::PreDelete(int64_t size) {
    auto reserved_begin = deleted_record_.reserved.fetch_add(size);
    return reserved_begin;
}

auto
SegmentGrowingImpl::get_deleted_bitmap(int64_t del_barrier,
                                       Timestamp query_timestamp,
                                       int64_t insert_barrier,
                                       bool force) -> std::shared_ptr<DeletedRecord::TmpBitmap> {
    auto old = deleted_record_.get_lru_entry();

    if (!force || old->bitmap_ptr->count() == insert_barrier) {
        if (old->del_barrier == del_barrier) {
            return old;
        }
    }

    auto current = old->clone(insert_barrier);
    current->del_barrier = del_barrier;

    auto bitmap = current->bitmap_ptr;
    if (del_barrier < old->del_barrier) {
        for (auto del_index = del_barrier; del_index < old->del_barrier; ++del_index) {
            // get uid in delete logs
            auto uid = deleted_record_.uids_[del_index];
            // map uid to corresponding offsets, select the max one, which should be the target
            // the max one should be closest to query_timestamp, so the delete log should refer to it
            int64_t the_offset = -1;
            auto [iter_b, iter_e] = uid2offset_.equal_range(uid);
            for (auto iter = iter_b; iter != iter_e; ++iter) {
                auto offset = iter->second;
                if (record_.timestamps_[offset] < query_timestamp) {
                    AssertInfo(offset < insert_barrier, "Timestamp offset is larger than insert barrier");
                    the_offset = std::max(the_offset, offset);
                }
            }
            // if not found, skip
            if (the_offset == -1) {
                continue;
            }
            // otherwise, clear the flag
            bitmap->clear(the_offset);
        }
        return current;
    } else {
        for (auto del_index = old->del_barrier; del_index < del_barrier; ++del_index) {
            // get uid in delete logs
            auto uid = deleted_record_.uids_[del_index];
            // map uid to corresponding offsets, select the max one, which should be the target
            // the max one should be closest to query_timestamp, so the delete log should refer to it
            int64_t the_offset = -1;
            auto [iter_b, iter_e] = uid2offset_.equal_range(uid);
            for (auto iter = iter_b; iter != iter_e; ++iter) {
                auto offset = iter->second;
                if (offset >= insert_barrier) {
                    continue;
                }
                if (record_.timestamps_[offset] < query_timestamp) {
                    AssertInfo(offset < insert_barrier, "Timestamp offset is larger than insert barrier");
                    the_offset = std::max(the_offset, offset);
                }
            }

            // if not found, skip
            if (the_offset == -1) {
                continue;
            }

            // otherwise, set the flag
            bitmap->set(the_offset);
        }
        this->deleted_record_.insert_lru_entry(current);
    }
    return current;
}

const BitsetView
SegmentGrowingImpl::get_filtered_bitmap(BitsetView& bitset, int64_t ins_barrier, Timestamp timestamp) {
    auto del_barrier = get_barrier(get_deleted_record(), timestamp);
    auto bitmap_holder = get_deleted_bitmap(del_barrier, timestamp, ins_barrier);
    AssertInfo(bitmap_holder, "bitmap_holder is null");
    auto deleted_bitmap = bitmap_holder->bitmap_ptr;
    AssertInfo(deleted_bitmap->count() == bitset.u8size(), "Deleted bitmap count not equal to filtered bitmap count");

    auto filtered_bitmap =
        std::make_shared<faiss::ConcurrentBitset>(faiss::ConcurrentBitset(bitset.u8size(), bitset.data()));

    auto final_bitmap = (*deleted_bitmap.get()) | (*filtered_bitmap.get());

    return BitsetView(final_bitmap);
}

Status
SegmentGrowingImpl::Insert(int64_t reserved_begin,
                           int64_t size,
                           const int64_t* uids_raw,
                           const Timestamp* timestamps_raw,
                           const RowBasedRawData& entities_raw) {
    AssertInfo(entities_raw.count == size, "Entities_raw count not equal to insert size");
    // step 1: check schema if valid
    if (entities_raw.sizeof_per_row != schema_->get_total_sizeof()) {
        std::string msg = "entity length = " + std::to_string(entities_raw.sizeof_per_row) +
                          ", schema length = " + std::to_string(schema_->get_total_sizeof());
        throw std::runtime_error(msg);
    }

    // step 2: sort timestamp
    auto raw_data = reinterpret_cast<const char*>(entities_raw.raw_data);
    auto len_per_row = entities_raw.sizeof_per_row;
    std::vector<std::tuple<Timestamp, idx_t, int64_t>> ordering;
    ordering.resize(size);
    // #pragma omp parallel for
    for (int i = 0; i < size; ++i) {
        ordering[i] = std::make_tuple(timestamps_raw[i], uids_raw[i], i);
    }
    std::sort(ordering.begin(), ordering.end());

    // step 3: and convert row-based data to column-based data accordingly
    auto sizeof_infos = schema_->get_sizeof_infos();
    std::vector<int> offset_infos(schema_->size() + 1, 0);
    std::partial_sum(sizeof_infos.begin(), sizeof_infos.end(), offset_infos.begin() + 1);
    std::vector<aligned_vector<uint8_t>> entities(schema_->size());

    for (int fid = 0; fid < schema_->size(); ++fid) {
        auto len = sizeof_infos[fid];
        entities[fid].resize(len * size);
    }

    std::vector<idx_t> uids(size);
    std::vector<Timestamp> timestamps(size);
    // #pragma omp parallel for
    for (int index = 0; index < size; ++index) {
        auto [t, uid, order_index] = ordering[index];
        timestamps[index] = t;
        uids[index] = uid;
        for (int fid = 0; fid < schema_->size(); ++fid) {
            auto len = sizeof_infos[fid];
            auto offset = offset_infos[fid];
            auto src = raw_data + order_index * len_per_row + offset;
            auto dst = entities[fid].data() + index * len;
            memcpy(dst, src, len);
        }
    }

    do_insert(reserved_begin, size, uids.data(), timestamps.data(), entities);
    return Status::OK();
}

void
SegmentGrowingImpl::do_insert(int64_t reserved_begin,
                              int64_t size,
                              const idx_t* row_ids,
                              const Timestamp* timestamps,
                              const std::vector<aligned_vector<uint8_t>>& columns_data) {
    // step 4: fill into Segment.ConcurrentVector
    record_.timestamps_.set_data(reserved_begin, timestamps, size);
    record_.uids_.set_data(reserved_begin, row_ids, size);
    for (int fid = 0; fid < schema_->size(); ++fid) {
        auto field_offset = FieldOffset(fid);
        record_.get_field_data_base(field_offset)->set_data_raw(reserved_begin, columns_data[fid].data(), size);
    }

    if (schema_->get_is_auto_id()) {
        for (int i = 0; i < size; ++i) {
            auto row_id = row_ids[i];
            // NOTE: this must be the last step, cannot be put above
            uid2offset_.insert(std::make_pair(row_id, reserved_begin + i));
        }
    } else {
        auto offset = schema_->get_primary_key_offset().value_or(FieldOffset(-1));
        AssertInfo(offset.get() != -1, "Primary key offset is -1");
        auto& row = columns_data[offset.get()];
        auto row_ptr = reinterpret_cast<const int64_t*>(row.data());
        for (int i = 0; i < size; ++i) {
            uid2offset_.insert(std::make_pair(row_ptr[i], reserved_begin + i));
        }
    }

    record_.ack_responder_.AddSegment(reserved_begin, reserved_begin + size);
    if (enable_small_index_) {
        int64_t chunk_rows = segcore_config_.get_chunk_rows();
        indexing_record_.UpdateResourceAck(record_.ack_responder_.GetAck() / chunk_rows, record_);
    }
}

Status
SegmentGrowingImpl::Delete(int64_t reserved_begin,
                           int64_t size,
                           const int64_t* uids_raw,
                           const Timestamp* timestamps_raw) {
    std::vector<std::tuple<Timestamp, idx_t>> ordering;
    ordering.resize(size);
    // #pragma omp parallel for
    for (int i = 0; i < size; ++i) {
        ordering[i] = std::make_tuple(timestamps_raw[i], uids_raw[i]);
    }
    std::sort(ordering.begin(), ordering.end());
    std::vector<idx_t> uids(size);
    std::vector<Timestamp> timestamps(size);
    // #pragma omp parallel for
    for (int index = 0; index < size; ++index) {
        auto [t, uid] = ordering[index];
        timestamps[index] = t;
        uids[index] = uid;
    }
    deleted_record_.timestamps_.set_data(reserved_begin, timestamps.data(), size);
    deleted_record_.uids_.set_data(reserved_begin, uids.data(), size);
    deleted_record_.ack_responder_.AddSegment(reserved_begin, reserved_begin + size);
    return Status::OK();
    //    for (int i = 0; i < size; ++i) {
    //        auto key = row_ids[i];
    //        auto time = timestamps[i];
    //        delete_logs_.insert(std::make_pair(key, time));
    //    }
    //    return Status::OK();
}

int64_t
SegmentGrowingImpl::GetMemoryUsageInBytes() const {
    int64_t total_bytes = 0;
    auto chunk_rows = segcore_config_.get_chunk_rows();
    int64_t ins_n = upper_align(record_.reserved, chunk_rows);
    total_bytes += ins_n * (schema_->get_total_sizeof() + 16 + 1);
    int64_t del_n = upper_align(deleted_record_.reserved, chunk_rows);
    total_bytes += del_n * (16 * 2);
    return total_bytes;
}

SpanBase
SegmentGrowingImpl::chunk_data_impl(FieldOffset field_offset, int64_t chunk_id) const {
    auto vec = get_insert_record().get_field_data_base(field_offset);
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
    // TODO(yukun): get final filtered bitmap
    auto& sealed_indexing = this->get_sealed_indexing_record();
    if (sealed_indexing.is_ready(search_info.field_offset_)) {
        query::SearchOnSealed(this->get_schema(), sealed_indexing, search_info, query_data, query_count, bitset,
                              output);
    } else {
        SearchOnGrowing(*this, vec_count, search_info, query_data, query_count, bitset, output);
    }
}

void
SegmentGrowingImpl::bulk_subscript(FieldOffset field_offset,
                                   const int64_t* seg_offsets,
                                   int64_t count,
                                   void* output) const {
    // TODO: support more types
    auto vec_ptr = record_.get_field_data_base(field_offset);
    auto& field_meta = schema_->operator[](field_offset);
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            bulk_subscript_impl<FloatVector>(field_meta.get_sizeof(), *vec_ptr, seg_offsets, count, output);
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            bulk_subscript_impl<BinaryVector>(field_meta.get_sizeof(), *vec_ptr, seg_offsets, count, output);
        } else {
            PanicInfo("logical error");
        }
        return;
    }

    AssertInfo(!field_meta.is_vector(), "Scalar field meta type is vector type");
    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            bulk_subscript_impl<bool>(*vec_ptr, seg_offsets, count, false, output);
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t>(*vec_ptr, seg_offsets, count, -1, output);
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t>(*vec_ptr, seg_offsets, count, -1, output);
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(*vec_ptr, seg_offsets, count, -1, output);
            break;
        }
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(*vec_ptr, seg_offsets, count, -1, output);
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(*vec_ptr, seg_offsets, count, -1.0, output);
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(*vec_ptr, seg_offsets, count, -1.0, output);
            break;
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
        const uint8_t* src = offset == -1 ? empty.data() : (const uint8_t*)vec.get_element(offset);
        memcpy(dst, src, element_sizeof);
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_impl(
    const VectorBase& vec_raw, const int64_t* seg_offsets, int64_t count, T default_value, void* output_raw) const {
    static_assert(IsScalar<T>);
    auto vec_ptr = dynamic_cast<const ConcurrentVector<T>*>(&vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    auto output = reinterpret_cast<T*>(output_raw);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        output[i] = offset == -1 ? default_value : vec[offset];
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
            bulk_subscript_impl<int64_t>(this->record_.uids_, seg_offsets, count, -1, output);
            break;
        default:
            PanicInfo("unknown subscript fields");
    }
}

// copied from stack overflow
template <typename T>
std::vector<size_t>
sort_indexes(const T* src, int64_t size) {
    // initialize original index locations
    std::vector<size_t> idx(size);
    iota(idx.begin(), idx.end(), 0);

    // sort indexes based on comparing values in v
    // using std::stable_sort instead of std::sort
    // to avoid unnecessary index re-orderings
    // when v contains elements of equal values
    std::stable_sort(idx.begin(), idx.end(), [src](size_t i1, size_t i2) { return src[i1] < src[i2]; });

    return idx;
}

void
SegmentGrowingImpl::Insert(int64_t reserved_offset,
                           int64_t size,
                           const int64_t* row_ids_raw,
                           const Timestamp* timestamps_raw,
                           const ColumnBasedRawData& values) {
    auto indexes = sort_indexes(timestamps_raw, size);
    std::vector<Timestamp> timestamps(size);
    std::vector<idx_t> row_ids(size);
    AssertInfo(values.count == size, "Insert values count not equal to insert size");
    for (int64_t i = 0; i < size; ++i) {
        auto offset = indexes[i];
        timestamps[i] = timestamps_raw[offset];
        row_ids[i] = row_ids_raw[i];
    }
    std::vector<aligned_vector<uint8_t>> columns_data;

    for (int field_offset = 0; field_offset < schema_->size(); ++field_offset) {
        auto& field_meta = schema_->operator[](FieldOffset(field_offset));
        aligned_vector<uint8_t> column;
        auto element_sizeof = field_meta.get_sizeof();
        auto& src_vec = values.columns_[field_offset];
        AssertInfo(src_vec.size() == element_sizeof * size, "Vector size is not aligned");
        for (int64_t i = 0; i < size; ++i) {
            auto offset = indexes[i];
            auto beg = src_vec.data() + offset * element_sizeof;
            column.insert(column.end(), beg, beg + element_sizeof);
        }
        columns_data.emplace_back(std::move(column));
    }
    do_insert(reserved_offset, size, row_ids.data(), timestamps.data(), columns_data);
}

std::vector<SegOffset>
SegmentGrowingImpl::search_ids(const boost::dynamic_bitset<>& bitset, Timestamp timestamp) const {
    std::vector<SegOffset> res_offsets;

    for (int i = 0; i < bitset.size(); i++) {
        if (bitset[i]) {
            SegOffset the_offset(-1);
            auto offset = SegOffset(i);
            if (record_.timestamps_[offset.get()] < timestamp) {
                the_offset = std::max(the_offset, offset);
            }

            if (the_offset == SegOffset(-1)) {
                continue;
            }
            res_offsets.push_back(the_offset);
        }
    }
    return res_offsets;
}

std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
SegmentGrowingImpl::search_ids(const IdArray& id_array, Timestamp timestamp) const {
    AssertInfo(id_array.has_int_id(), "Id array doesn't have int_id element");
    auto& src_int_arr = id_array.int_id();
    auto res_id_arr = std::make_unique<IdArray>();
    auto res_int_id_arr = res_id_arr->mutable_int_id();
    std::vector<SegOffset> res_offsets;
    for (auto uid : src_int_arr.data()) {
        auto [iter_b, iter_e] = uid2offset_.equal_range(uid);
        SegOffset the_offset(-1);
        for (auto iter = iter_b; iter != iter_e; ++iter) {
            auto offset = SegOffset(iter->second);
            if (record_.timestamps_[offset.get()] < timestamp) {
                the_offset = std::max(the_offset, offset);
            }
        }
        // if not found, skip
        if (the_offset == SegOffset(-1)) {
            continue;
        }
        res_int_id_arr->add_data(uid);
        res_offsets.push_back(the_offset);
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
SegmentGrowingImpl::mask_with_timestamps(boost::dynamic_bitset<>& bitset_chunk, Timestamp timestamp) const {
    // DO NOTHING
}

}  // namespace milvus::segcore
