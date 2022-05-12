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

#include "SegmentSealedImpl.h"
#include "common/Consts.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "query/ScalarIndex.h"
#include "Utils.h"

namespace milvus::segcore {

static inline void
set_bit(BitsetType& bitset, FieldId field_id, bool flag = true) {
    auto pos = field_id.get() - START_USER_FIELDID;
    AssertInfo(pos >= 0, "invalid field id");
    bitset[pos] = flag;
}

static inline bool
get_bit(const BitsetType& bitset, FieldId field_id) {
    auto pos = field_id.get() - START_USER_FIELDID;
    AssertInfo(pos >= 0, "invalid field id");
    return bitset[pos];
}

int64_t
SegmentSealedImpl::PreDelete(int64_t size) {
    auto reserved_begin = deleted_record_.reserved.fetch_add(size);
    return reserved_begin;
}

void
print(const std::map<std::string, std::string>& m) {
    for (const auto& [k, v] : m) {
        std::cout << k << ": " << v << std::endl;
    }
}

void
print(const LoadIndexInfo& info) {
    std::cout << "------------------LoadIndexInfo----------------------" << std::endl;
    std::cout << "field_id: " << info.field_id << std::endl;
    std::cout << "field_type: " << info.field_type << std::endl;
    std::cout << "index_params:" << std::endl;
    print(info.index_params);
    std::cout << "------------------LoadIndexInfo----------------------" << std::endl;
}

void
print(const LoadFieldDataInfo& info) {
    std::cout << "------------------LoadFieldDataInfo----------------------" << std::endl;
    std::cout << "field_id: " << info.field_id << std::endl;
    std::cout << "------------------LoadFieldDataInfo----------------------" << std::endl;
}

void
SegmentSealedImpl::LoadIndex(const LoadIndexInfo& info) {
    // print(info);
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto& field_meta = schema_->operator[](field_id);

    if (field_meta.is_vector()) {
        LoadVecIndex(info);
    } else {
        LoadScalarIndex(info);
    }
}

void
SegmentSealedImpl::LoadVecIndex(const LoadIndexInfo& info) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);

    auto index = std::dynamic_pointer_cast<knowhere::VecIndex>(info.index);
    AssertInfo(info.index_params.count("metric_type"), "Can't get metric_type in index_params");
    auto metric_type_str = info.index_params.at("metric_type");
    auto row_count = index->Count();
    AssertInfo(row_count > 0, "Index count is 0");

    std::unique_lock lck(mutex_);
    AssertInfo(!get_bit(vecindex_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (row_count_opt_.has_value()) {
        AssertInfo(row_count_opt_.value() == row_count, "load data has different row count from other columns");
    } else {
        row_count_opt_ = row_count;
    }
    AssertInfo(!vector_indexings_.is_ready(field_id), "vec index is not ready");
    vector_indexings_.append_field_indexing(field_id, GetMetricType(metric_type_str), index);

    set_bit(vecindex_ready_bitset_, field_id, true);
    lck.unlock();
}

void
SegmentSealedImpl::LoadScalarIndex(const LoadIndexInfo& info) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);

    auto index = std::dynamic_pointer_cast<scalar::IndexBase>(info.index);
    auto row_count = index->Count();
    AssertInfo(row_count > 0, "Index count is 0");

    std::unique_lock lck(mutex_);

    if (row_count_opt_.has_value()) {
        AssertInfo(row_count_opt_.value() == row_count, "load data has different row count from other columns");
    } else {
        row_count_opt_ = row_count;
    }

    scalar_indexings_[field_id] = std::move(index);

    set_bit(field_data_ready_bitset_, field_id, true);
    lck.unlock();
}

void
SegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& info) {
    // print(info);
    // NOTE: lock only when data is ready to avoid starvation
    AssertInfo(info.row_count > 0, "The row count of field data is 0");
    auto field_id = FieldId(info.field_id);
    AssertInfo(info.field_data != nullptr, "Field info blob is null");
    auto size = info.row_count;

    if (SystemProperty::Instance().IsSystem(field_id)) {
        auto system_field_type = SystemProperty::Instance().GetSystemFieldType(field_id);
        if (system_field_type == SystemFieldType::Timestamp) {
            auto timestamps = reinterpret_cast<const Timestamp*>(info.field_data->scalars().long_data().data().data());

            TimestampIndex index;
            auto min_slice_length = size < 4096 ? 1 : 4096;
            auto meta = GenerateFakeSlices(timestamps, size, min_slice_length);
            index.set_length_meta(std::move(meta));
            index.build_with(timestamps, size);

            // use special index
            std::unique_lock lck(mutex_);
            AssertInfo(insert_record_.timestamps_.empty(), "already exists");
            insert_record_.timestamps_.fill_chunk_data(timestamps, size);
            insert_record_.timestamp_index_ = std::move(index);
            AssertInfo(insert_record_.timestamps_.num_chunk() == 1, "num chunk not equal to 1 for sealed segment");
        } else {
            AssertInfo(system_field_type == SystemFieldType::RowId, "System field type of id column is not RowId");
            auto row_ids = reinterpret_cast<const idx_t*>(info.field_data->scalars().long_data().data().data());
            // write data under lock
            std::unique_lock lck(mutex_);
            AssertInfo(insert_record_.row_ids_.empty(), "already exists");
            insert_record_.row_ids_.fill_chunk_data(row_ids, size);
            AssertInfo(insert_record_.row_ids_.num_chunk() == 1, "num chunk not equal to 1 for sealed segment");
        }
        ++system_ready_count_;
    } else {
        // prepare data
        auto& field_meta = schema_->operator[](field_id);
        auto data_type = field_meta.get_data_type();
        AssertInfo(data_type == DataType(info.field_data->type()),
                   "field type of load data is inconsistent with the schema");
        auto field_data = insert_record_.get_field_data_base(field_id);
        AssertInfo(field_data->empty(), "already exists");

        // write data under lock
        std::unique_lock lck(mutex_);

        // insert data to insertRecord
        field_data->fill_chunk_data(size, info.field_data, field_meta);
        AssertInfo(field_data->num_chunk() == 1, "num chunk not equal to 1 for sealed segment");

        // set pks to offset
        if (schema_->get_primary_field_id() == field_id) {
            AssertInfo(field_id.get() != -1, "Primary key is -1");
            AssertInfo(pk2offset_.empty(), "already exists");
            std::vector<PkType> pks(size);
            ParsePksFromFieldData(pks, *info.field_data);
            for (int i = 0; i < size; ++i) {
                pk2offset_.insert(std::make_pair(pks[i], i));
            }
        }

        if (field_meta.is_vector()) {
            AssertInfo(!vector_indexings_.is_ready(field_id), "field data can't be loaded when indexing exists");
        } else if (!scalar_indexings_.count(field_id)) {
            // generate scalar index
            std::unique_ptr<knowhere::Index> index;
            index = query::generate_scalar_index(field_data->get_span_base(0), data_type);
            scalar_indexings_[field_id] = std::move(index);
        }

        set_bit(field_data_ready_bitset_, field_id, true);
    }
    update_row_count(info.row_count);
}

void
SegmentSealedImpl::LoadDeletedRecord(const LoadDeletedRecordInfo& info) {
    AssertInfo(info.row_count > 0, "The row count of deleted record is 0");
    AssertInfo(info.primary_keys, "Deleted primary keys is null");
    AssertInfo(info.timestamps, "Deleted timestamps is null");
    // step 1: get pks and timestamps
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_->operator[](field_id);
    int64_t size = info.row_count;
    std::vector<PkType> pks(size);
    ParsePksFromIDs(pks, field_meta.get_data_type(), *info.primary_keys);
    auto timestamps = reinterpret_cast<const Timestamp*>(info.timestamps);

    // step 2: fill pks and timestamps
    deleted_record_.pks_.set_data_raw(0, pks.data(), size);
    deleted_record_.timestamps_.set_data_raw(0, timestamps, size);
    deleted_record_.ack_responder_.AddSegment(0, size);
    deleted_record_.reserved.fetch_add(size);
    deleted_record_.record_size_ = size;
}

int64_t
SegmentSealedImpl::num_chunk_index(FieldId field_id) const {
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
SegmentSealedImpl::chunk_data_impl(FieldId field_id, int64_t chunk_id) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    auto& field_meta = schema_->operator[](field_id);
    auto element_sizeof = field_meta.get_sizeof();
    auto field_data = insert_record_.get_field_data_base(field_id);
    AssertInfo(field_data->num_chunk() == 1, "num chunk not equal to 1 for sealed segment");
    return field_data->get_span_base(0);
}

const knowhere::Index*
SegmentSealedImpl::chunk_index_impl(FieldId field_id, int64_t chunk_id) const {
    AssertInfo(chunk_id == 0, "Chunk_id is not equal to 0");
    // TODO: support scalar index
    auto ptr = scalar_indexings_.at(field_id).get();
    AssertInfo(ptr, "Scalar index of " + std::to_string(field_id.get()) + " is null");
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
SegmentSealedImpl::mask_with_delete(BitsetType& bitset, int64_t ins_barrier, Timestamp timestamp) const {
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
SegmentSealedImpl::vector_search(int64_t vec_count,
                                 query::SearchInfo search_info,
                                 const void* query_data,
                                 int64_t query_count,
                                 Timestamp timestamp,
                                 const BitsetView& bitset,
                                 SearchResult& output) const {
    AssertInfo(is_system_field_ready(), "System field is not ready");
    auto field_id = search_info.field_id_;
    auto& field_meta = schema_->operator[](field_id);

    AssertInfo(field_meta.is_vector(), "The meta type of vector field is not vector type");
    if (get_bit(vecindex_ready_bitset_, field_id)) {
        AssertInfo(vector_indexings_.is_ready(field_id),
                   "vector indexes isn't ready for field " + std::to_string(field_id.get()));
        query::SearchOnSealed(*schema_, vector_indexings_, search_info, query_data, query_count, bitset, output, id_);
        return;
    } else if (!get_bit(field_data_ready_bitset_, field_id)) {
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

    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    AssertInfo(row_count_opt_.has_value(), "Can't get row count value");
    auto row_count = row_count_opt_.value();
    auto vec_data = insert_record_.get_field_data_base(field_id);
    AssertInfo(vec_data->num_chunk() == 1, "num chunk not equal to 1 for sealed segment");
    auto chunk_data = vec_data->get_chunk_data(0);

    auto sub_qr = [&] {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            return query::FloatSearchBruteForce(dataset, chunk_data, row_count, bitset);
        } else {
            return query::BinarySearchBruteForce(dataset, chunk_data, row_count, bitset);
        }
    }();

    SearchResult results;
    results.distances_ = std::move(sub_qr.mutable_distances());
    results.seg_offsets_ = std::move(sub_qr.mutable_seg_offsets());
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
            insert_record_.row_ids_.clear();
        } else if (system_field_type == SystemFieldType::Timestamp) {
            insert_record_.timestamps_.clear();
        }
        lck.unlock();
    } else {
        auto& field_meta = schema_->operator[](field_id);
        std::unique_lock lck(mutex_);
        set_bit(field_data_ready_bitset_, field_id, false);
        insert_record_.drop_field_data(field_id);
        lck.unlock();
    }
}

void
SegmentSealedImpl::DropIndex(const FieldId field_id) {
    AssertInfo(!SystemProperty::Instance().IsSystem(field_id),
               "Field id:" + std::to_string(field_id.get()) + " isn't one of system type when drop index");
    auto& field_meta = schema_->operator[](field_id);
    AssertInfo(field_meta.is_vector(),
               "Field meta of offset:" + std::to_string(field_id.get()) + " is not vector type");

    std::unique_lock lck(mutex_);
    vector_indexings_.drop_field_indexing(field_id);
    set_bit(vecindex_ready_bitset_, field_id, false);
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
        auto field_id = FieldId(absent_fields.find_first() + START_USER_FIELDID);
        auto& field_meta = schema_->operator[](field_id);
        PanicInfo("User Field(" + field_meta.get_name().get() + ") is not loaded");
    }
}

SegmentSealedImpl::SegmentSealedImpl(SchemaPtr schema, int64_t segment_id)
    : schema_(schema),
      insert_record_(*schema, MAX_ROW_COUNT),
      field_data_ready_bitset_(schema->size()),
      vecindex_ready_bitset_(schema->size()),
      scalar_indexings_(schema->size()),
      id_(segment_id) {
}

void
SegmentSealedImpl::bulk_subscript(SystemFieldType system_type,
                                  const int64_t* seg_offsets,
                                  int64_t count,
                                  void* output) const {
    AssertInfo(is_system_field_ready(), "System field isn't ready when do bulk_insert");
    AssertInfo(system_type == SystemFieldType::RowId, "System field type of id column is not RowId");
    AssertInfo(insert_record_.row_ids_.num_chunk() == 1, "num chunk not equal to 1 for sealed segment");
    auto field_data = insert_record_.row_ids_.get_chunk_data(0);
    bulk_subscript_impl<int64_t>(field_data, seg_offsets, count, output);
}

template <typename T>
void
SegmentSealedImpl::bulk_subscript_impl(const void* src_raw, const int64_t* seg_offsets, int64_t count, void* dst_raw) {
    static_assert(IsScalar<T>);
    auto src = reinterpret_cast<const T*>(src_raw);
    auto dst = reinterpret_cast<T*>(dst_raw);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset != INVALID_SEG_OFFSET) {
            dst[i] = src[offset];
        }
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
        const char* src = (offset == INVALID_SEG_OFFSET ? none.data() : (src_vec + element_sizeof * offset));
        memcpy(dst, src, element_sizeof);
    }
}

std::unique_ptr<DataArray>
SegmentSealedImpl::fill_with_empty(FieldId field_id, int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            FixedVector<bool> output(count);
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT8: {
            FixedVector<int8_t> output(count);
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT16: {
            FixedVector<int16_t> output(count);
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT32: {
            FixedVector<int32_t> output(count);
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT64: {
            FixedVector<int64_t> output(count);
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::FLOAT: {
            FixedVector<float> output(count);
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::DOUBLE: {
            FixedVector<double> output(count);
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::VARCHAR: {
            FixedVector<std::string> output(count);
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }

        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY: {
            aligned_vector<char> output(field_meta.get_sizeof() * count);
            return CreateVectorDataArrayFrom(output.data(), count, field_meta);
        }

        default: {
            PanicInfo("unsupported");
        }
    }
}

std::unique_ptr<DataArray>
SegmentSealedImpl::bulk_subscript(FieldId field_id, const int64_t* seg_offsets, int64_t count) const {
    if (!HasFieldData(field_id)) {
        return fill_with_empty(field_id, count);
    }

    Assert(get_bit(field_data_ready_bitset_, field_id));

    auto& field_meta = schema_->operator[](field_id);
    auto field_data = insert_record_.get_field_data_base(field_id);
    AssertInfo(field_data->num_chunk() == 1, std::string("num chunk not equal to 1 for sealed segment, num_chunk: ") +
                                                 std::to_string(field_data->num_chunk()));
    auto src_vec = field_data->get_chunk_data(0);
    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            FixedVector<bool> output(count);
            bulk_subscript_impl<bool>(src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT8: {
            FixedVector<int8_t> output(count);
            bulk_subscript_impl<int8_t>(src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT16: {
            FixedVector<int16_t> output(count);
            bulk_subscript_impl<int16_t>(src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT32: {
            FixedVector<int32_t> output(count);
            bulk_subscript_impl<int32_t>(src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT64: {
            FixedVector<int64_t> output(count);
            bulk_subscript_impl<int64_t>(src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::FLOAT: {
            FixedVector<float> output(count);
            bulk_subscript_impl<float>(src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::DOUBLE: {
            FixedVector<double> output(count);
            bulk_subscript_impl<double>(src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::VARCHAR: {
            FixedVector<std::string> output(count);
            bulk_subscript_impl<std::string>(src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }

        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY: {
            aligned_vector<char> output(field_meta.get_sizeof() * count);
            bulk_subscript_impl(field_meta.get_sizeof(), src_vec, seg_offsets, count, output.data());
            return CreateVectorDataArrayFrom(output.data(), count, field_meta);
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
    return get_bit(vecindex_ready_bitset_, field_id);
}

bool
SegmentSealedImpl::HasFieldData(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    if (SystemProperty::Instance().IsSystem(field_id)) {
        return is_system_field_ready();
    } else {
        return get_bit(field_data_ready_bitset_, field_id);
    }
}

std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
SegmentSealedImpl::search_ids(const IdArray& id_array, Timestamp timestamp) const {
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

Status
SegmentSealedImpl::Delete(int64_t reserved_offset, int64_t size, const IdArray* ids, const Timestamp* timestamps_raw) {
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
    deleted_record_.timestamps_.set_data_raw(reserved_offset, sort_timestamps.data(), size);
    deleted_record_.pks_.set_data_raw(reserved_offset, sort_pks.data(), size);
    deleted_record_.ack_responder_.AddSegment(reserved_offset, size);
    return Status::OK();
}

std::vector<SegOffset>
SegmentSealedImpl::search_ids(const BitsetType& bitset, Timestamp timestamp) const {
    std::vector<SegOffset> dst_offset;
    for (int i = 0; i < bitset.size(); i++) {
        if (bitset[i]) {
            auto offset = SegOffset(i);
            if (insert_record_.timestamps_[offset.get()] <= timestamp) {
                dst_offset.push_back(offset);
            }
        }
    }
    return dst_offset;
}

std::vector<SegOffset>
SegmentSealedImpl::search_ids(const BitsetView& bitset, Timestamp timestamp) const {
    std::vector<SegOffset> dst_offset;
    for (int i = 0; i < bitset.size(); i++) {
        if (!bitset.test(i)) {
            auto offset = SegOffset(i);
            if (insert_record_.timestamps_[offset.get()] <= timestamp) {
                dst_offset.push_back(offset);
            }
        }
    }
    return dst_offset;
}

std::string
SegmentSealedImpl::debug() const {
    std::string log_str;
    log_str += "Sealed\n";
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
    insert_record_.timestamp_index_.set_length_meta(std::move(slice_lengths));
    PanicInfo("unimplemented");
}

int64_t
SegmentSealedImpl::get_active_count(Timestamp ts) const {
    // TODO optimize here to reduce expr search range
    return this->get_row_count();
}

void
SegmentSealedImpl::mask_with_timestamps(BitsetType& bitset_chunk, Timestamp timestamp) const {
    // TODO change the
    AssertInfo(insert_record_.timestamps_.num_chunk() == 1, "num chunk not equal to 1 for sealed segment");
    auto timestamps_data = insert_record_.timestamps_.get_chunk(0);
    AssertInfo(timestamps_data.size() == get_row_count(), "Timestamp size not equal to row count");
    auto range = insert_record_.timestamp_index_.get_active_range(timestamp);

    // range == (size_, size_) and size_ is this->timestamps_.size().
    // it means these data are all useful, we don't need to update bitset_chunk.
    // It can be thought of as an AND operation with another bitmask that is all 1s, but it is not necessary to do so.
    if (range.first == range.second && range.first == timestamps_data.size()) {
        // just skip
        return;
    }
    // range == (0, 0). it means these data can not be used, directly set bitset_chunk to all 0s.
    // It can be thought of as an AND operation with another bitmask that is all 0s.
    if (range.first == range.second && range.first == 0) {
        bitset_chunk.reset();
        return;
    }
    auto mask = TimestampIndex::GenerateBitset(timestamp, range, timestamps_data.data(), timestamps_data.size());
    bitset_chunk &= mask;
}

}  // namespace milvus::segcore
