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

#include <fcntl.h>
#include <fmt/core.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "Utils.h"
#include "Types.h"
#include "common/Json.h"
#include "mmap/Column.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "log/Log.h"
#include "query/ScalarIndex.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "storage/FieldData.h"
#include "storage/Util.h"
#include "storage/ThreadPool.h"

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
    auto& field_meta = schema_->operator[](field_id);

    AssertInfo(info.index_params.count("metric_type"),
               "Can't get metric_type in index_params");
    auto metric_type = info.index_params.at("metric_type");
    auto row_count = info.index->Count();
    AssertInfo(row_count > 0, "Index count is 0");

    std::unique_lock lck(mutex_);
    // Don't allow vector raw data and index exist at the same time
    AssertInfo(!get_bit(field_data_ready_bitset_, field_id),
               "vector index can't be loaded when raw data exists at field " +
                   std::to_string(field_id.get()));
    AssertInfo(
        !get_bit(index_ready_bitset_, field_id),
        "vector index has been exist at " + std::to_string(field_id.get()));
    if (row_count_opt_.has_value()) {
        AssertInfo(row_count_opt_.value() == row_count,
                   "field (" + std::to_string(field_id.get()) +
                       ") data has different row count (" +
                       std::to_string(row_count) +
                       ") than other column's row count (" +
                       std::to_string(row_count_opt_.value()) + ")");
    }
    AssertInfo(!vector_indexings_.is_ready(field_id), "vec index is not ready");
    vector_indexings_.append_field_indexing(
        field_id,
        metric_type,
        std::move(const_cast<LoadIndexInfo&>(info).index));

    set_bit(index_ready_bitset_, field_id, true);
    update_row_count(row_count);
}

void
SegmentSealedImpl::LoadScalarIndex(const LoadIndexInfo& info) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto& field_meta = schema_->operator[](field_id);

    auto row_count = info.index->Count();
    AssertInfo(row_count > 0, "Index count is 0");

    std::unique_lock lck(mutex_);
    // Don't allow scalar raw data and index exist at the same time
    AssertInfo(!get_bit(field_data_ready_bitset_, field_id),
               "scalar index can't be loaded when raw data exists at field " +
                   std::to_string(field_id.get()));
    AssertInfo(
        !get_bit(index_ready_bitset_, field_id),
        "scalar index has been exist at " + std::to_string(field_id.get()));
    if (row_count_opt_.has_value()) {
        AssertInfo(row_count_opt_.value() == row_count,
                   "field (" + std::to_string(field_id.get()) +
                       ") data has different row count (" +
                       std::to_string(row_count) +
                       ") than other column's row count (" +
                       std::to_string(row_count_opt_.value()) + ")");
    }

    scalar_indexings_[field_id] =
        std::move(const_cast<LoadIndexInfo&>(info).index);
    // reverse pk from scalar index and set pks to offset
    if (schema_->get_primary_field_id() == field_id) {
        AssertInfo(field_id.get() != -1, "Primary key is -1");
        AssertInfo(insert_record_.empty_pks(), "already exists");
        switch (field_meta.get_data_type()) {
            case DataType::INT64: {
                auto int64_index = dynamic_cast<index::ScalarIndex<int64_t>*>(
                    scalar_indexings_[field_id].get());
                for (int i = 0; i < row_count; ++i) {
                    insert_record_.insert_pk(int64_index->Reverse_Lookup(i), i);
                }
                insert_record_.seal_pks();
                break;
            }
            case DataType::VARCHAR: {
                auto string_index =
                    dynamic_cast<index::ScalarIndex<std::string>*>(
                        scalar_indexings_[field_id].get());
                for (int i = 0; i < row_count; ++i) {
                    insert_record_.insert_pk(string_index->Reverse_Lookup(i),
                                             i);
                }
                insert_record_.seal_pks();
                break;
            }
            default: {
                PanicInfo("unsupported primary key type");
            }
        }
    }

    set_bit(index_ready_bitset_, field_id, true);
    update_row_count(row_count);
    lck.unlock();
}

void
SegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& load_info) {
    // NOTE: lock only when data is ready to avoid starvation
    // only one field for now, parallel load field data in golang
    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);

    for (auto& [id, info] : load_info.field_infos) {
        AssertInfo(info.row_count > 0, "The row count of field data is 0");

        auto field_id = FieldId(id);
        auto insert_files = info.insert_files;
        auto field_data_info =
            FieldDataInfo(field_id.get(), num_rows, load_info.mmap_dir_path);

        auto& pool = ThreadPool::GetInstance();
        auto load_future = pool.Submit(
            LoadFieldDatasFromRemote, insert_files, field_data_info.channel);

        if (load_info.mmap_dir_path.empty() ||
            SystemProperty::Instance().IsSystem(field_id)) {
            LoadFieldData(field_id, field_data_info);
        } else {
            MapFieldData(field_id, field_data_info);
        }
    }
}

void
SegmentSealedImpl::LoadFieldData(FieldId field_id, FieldDataInfo& data) {
    auto num_rows = data.row_count;
    if (SystemProperty::Instance().IsSystem(field_id)) {
        auto system_field_type =
            SystemProperty::Instance().GetSystemFieldType(field_id);
        if (system_field_type == SystemFieldType::Timestamp) {
            std::vector<Timestamp> timestamps(num_rows);
            int64_t offset = 0;
            auto field_data = CollectFieldDataChannel(data.channel);
            for (auto& data : field_data) {
                int64_t row_count = data->get_num_rows();
                std::copy_n(static_cast<const Timestamp*>(data->Data()),
                            row_count,
                            timestamps.data() + offset);
                offset += row_count;
            }

            TimestampIndex index;
            auto min_slice_length = num_rows < 4096 ? 1 : 4096;
            auto meta = GenerateFakeSlices(
                timestamps.data(), num_rows, min_slice_length);
            index.set_length_meta(std::move(meta));
            // todo ::opt to avoid copy timestamps from field data
            index.build_with(timestamps.data(), num_rows);

            // use special index
            std::unique_lock lck(mutex_);
            AssertInfo(insert_record_.timestamps_.empty(), "already exists");
            insert_record_.timestamps_.fill_chunk_data(field_data);
            insert_record_.timestamp_index_ = std::move(index);
            AssertInfo(insert_record_.timestamps_.num_chunk() == 1,
                       "num chunk not equal to 1 for sealed segment");
        } else {
            AssertInfo(system_field_type == SystemFieldType::RowId,
                       "System field type of id column is not RowId");

            auto field_data = CollectFieldDataChannel(data.channel);

            // write data under lock
            std::unique_lock lck(mutex_);
            AssertInfo(insert_record_.row_ids_.empty(), "already exists");
            insert_record_.row_ids_.fill_chunk_data(field_data);
            AssertInfo(insert_record_.row_ids_.num_chunk() == 1,
                       "num chunk not equal to 1 for sealed segment");
        }
        ++system_ready_count_;
    } else {
        // prepare data
        auto& field_meta = (*schema_)[field_id];
        auto data_type = field_meta.get_data_type();

        // Don't allow raw data and index exist at the same time
        AssertInfo(!get_bit(index_ready_bitset_, field_id),
                   "field data can't be loaded when indexing exists");

        std::shared_ptr<ColumnBase> column{};
        if (datatype_is_variable(data_type)) {
            switch (data_type) {
                case milvus::DataType::STRING:
                case milvus::DataType::VARCHAR: {
                    auto var_column =
                        std::make_shared<VariableColumn<std::string>>(
                            num_rows, field_meta);
                    storage::FieldDataPtr field_data;
                    while (data.channel->pop(field_data)) {
                        for (auto i = 0; i < field_data->get_num_rows(); i++) {
                            auto str = static_cast<const std::string*>(
                                field_data->RawValue(i));
                            var_column->Append(str->data(), str->size());
                        }
                    }
                    var_column->Seal();
                    column = std::move(var_column);
                    break;
                }
                case milvus::DataType::JSON: {
                    auto var_column =
                        std::make_shared<VariableColumn<milvus::Json>>(
                            num_rows, field_meta);
                    storage::FieldDataPtr field_data;
                    while (data.channel->pop(field_data)) {
                        for (auto i = 0; i < field_data->get_num_rows(); i++) {
                            auto padded_string =
                                static_cast<const milvus::Json*>(
                                    field_data->RawValue(i))
                                    ->data();
                            var_column->Append(padded_string.data(),
                                               padded_string.size());
                        }
                    }
                    var_column->Seal();
                    column = std::move(var_column);
                    break;
                }
                default: {
                }
            }
        } else {
            column = std::make_shared<Column>(num_rows, field_meta);
            storage::FieldDataPtr field_data;
            while (data.channel->pop(field_data)) {
                column->Append(static_cast<const char*>(field_data->Data()),
                               field_data->Size());
            }
        }

        {
            std::unique_lock lck(mutex_);
            fields_.emplace(field_id, column);
        }

        // set pks to offset
        if (schema_->get_primary_field_id() == field_id) {
            AssertInfo(field_id.get() != -1, "Primary key is -1");
            AssertInfo(insert_record_.empty_pks(), "already exists");
            insert_record_.insert_pks(data_type, column);
            insert_record_.seal_pks();
        }

        std::unique_lock lck(mutex_);
        set_bit(field_data_ready_bitset_, field_id, true);
    }
    std::unique_lock lck(mutex_);
    update_row_count(num_rows);
}

void
SegmentSealedImpl::MapFieldData(const FieldId field_id, FieldDataInfo& data) {
    auto filepath = std::filesystem::path(data.mmap_dir_path) /
                    std::to_string(get_segment_id()) /
                    std::to_string(field_id.get());
    auto dir = filepath.parent_path();
    std::filesystem::create_directories(dir);

    int fd =
        open(filepath.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    AssertInfo(fd != -1,
               fmt::format("failed to create mmap file {}", filepath.c_str()));

    auto& field_meta = (*schema_)[field_id];
    auto data_type = field_meta.get_data_type();

    // write the field data to disk
    size_t total_written{0};
    auto data_size = 0;
    std::vector<uint64_t> indices{};
    storage::FieldDataPtr field_data;
    while (data.channel->pop(field_data)) {
        data_size += field_data->Size();
        auto written = WriteFieldData(fd, data_type, field_data);
        if (written != field_data->Size()) {
            break;
        }

        for (auto i = 0; i < field_data->get_num_rows(); i++) {
            auto size = field_data->Size(i);
            indices.emplace_back(total_written);
            total_written += size;
        }
    }
    AssertInfo(
        total_written == data_size,
        fmt::format(
            "failed to write data file {}, written {} but total {}, err: {}",
            filepath.c_str(),
            total_written,
            data_size,
            strerror(errno)));
    int ok = fsync(fd);
    AssertInfo(ok == 0,
               fmt::format("failed to fsync mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

    auto num_rows = data.row_count;
    std::shared_ptr<ColumnBase> column{};
    if (datatype_is_variable(data_type)) {
        switch (data_type) {
            case milvus::DataType::STRING:
            case milvus::DataType::VARCHAR: {
                auto var_column = std::make_shared<VariableColumn<std::string>>(
                    fd, total_written, field_meta);
                var_column->Seal(std::move(indices));
                column = std::move(var_column);
                break;
            }
            case milvus::DataType::JSON: {
                auto var_column =
                    std::make_shared<VariableColumn<milvus::Json>>(
                        fd, total_written, field_meta);
                var_column->Seal(std::move(indices));
                column = std::move(var_column);
                break;
            }
            default: {
            }
        }
    } else {
        column = std::make_shared<Column>(fd, total_written, field_meta);
    }

    {
        std::unique_lock lck(mutex_);
        fields_.emplace(field_id, column);
    }

    ok = unlink(filepath.c_str());
    AssertInfo(ok == 0,
               fmt::format("failed to unlink mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));
    ok = close(fd);
    AssertInfo(ok == 0,
               fmt::format("failed to close data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

    // set pks to offset
    if (schema_->get_primary_field_id() == field_id) {
        AssertInfo(field_id.get() != -1, "Primary key is -1");
        AssertInfo(insert_record_.empty_pks(), "already exists");
        insert_record_.insert_pks(data_type, column);
        insert_record_.seal_pks();
    }

    std::unique_lock lck(mutex_);
    set_bit(field_data_ready_bitset_, field_id, true);
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
    deleted_record_.push(pks, timestamps);
}

// internal API: support scalar index only
int64_t
SegmentSealedImpl::num_chunk_index(FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    if (field_meta.is_vector()) {
        return int64_t(vector_indexings_.is_ready(field_id));
    }

    return scalar_indexings_.count(field_id);
}

int64_t
SegmentSealedImpl::num_chunk_data(FieldId field_id) const {
    return get_bit(field_data_ready_bitset_, field_id) ? 1 : 0;
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
    if (auto it = fields_.find(field_id); it != fields_.end()) {
        auto& field_data = it->second;
        return field_data->Span();
    }
    auto field_data = insert_record_.get_field_data_base(field_id);
    AssertInfo(field_data->num_chunk() == 1,
               "num chunk not equal to 1 for sealed segment");
    return field_data->get_span_base(0);
}

const index::IndexBase*
SegmentSealedImpl::chunk_index_impl(FieldId field_id, int64_t chunk_id) const {
    AssertInfo(scalar_indexings_.find(field_id) != scalar_indexings_.end(),
               "Cannot find scalar_indexing with field_id: " +
                   std::to_string(field_id.get()));
    auto ptr = scalar_indexings_.at(field_id).get();
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

int64_t
SegmentSealedImpl::get_deleted_count() const {
    std::shared_lock lck(mutex_);
    return deleted_record_.size();
}

const Schema&
SegmentSealedImpl::get_schema() const {
    return *schema_;
}

void
SegmentSealedImpl::mask_with_delete(BitsetType& bitset,
                                    int64_t ins_barrier,
                                    Timestamp timestamp) const {
    auto del_barrier = get_barrier(get_deleted_record(), timestamp);
    if (del_barrier == 0) {
        return;
    }

    auto bitmap_holder = get_deleted_bitmap(
        del_barrier, ins_barrier, deleted_record_, insert_record_, timestamp);
    if (!bitmap_holder || !bitmap_holder->bitmap_ptr) {
        return;
    }
    auto& delete_bitset = *bitmap_holder->bitmap_ptr;
    AssertInfo(delete_bitset.size() == bitset.size(),
               "Deleted bitmap size not equal to filtered bitmap size");
    bitset |= delete_bitset;
}

void
SegmentSealedImpl::vector_search(SearchInfo& search_info,
                                 const void* query_data,
                                 int64_t query_count,
                                 Timestamp timestamp,
                                 const BitsetView& bitset,
                                 SearchResult& output) const {
    AssertInfo(is_system_field_ready(), "System field is not ready");
    auto field_id = search_info.field_id_;
    auto& field_meta = schema_->operator[](field_id);

    AssertInfo(field_meta.is_vector(),
               "The meta type of vector field is not vector type");
    if (get_bit(index_ready_bitset_, field_id)) {
        AssertInfo(vector_indexings_.is_ready(field_id),
                   "vector indexes isn't ready for field " +
                       std::to_string(field_id.get()));
        query::SearchOnSealedIndex(*schema_,
                                   vector_indexings_,
                                   search_info,
                                   query_data,
                                   query_count,
                                   bitset,
                                   output);
    } else {
        AssertInfo(
            get_bit(field_data_ready_bitset_, field_id),
            "Field Data is not loaded: " + std::to_string(field_id.get()));
        AssertInfo(row_count_opt_.has_value(), "Can't get row count value");
        auto row_count = row_count_opt_.value();
        auto& vec_data = fields_.at(field_id);
        query::SearchOnSealed(*schema_,
                              vec_data->Data(),
                              search_info,
                              query_data,
                              query_count,
                              row_count,
                              bitset,
                              output);
    }
}

std::unique_ptr<DataArray>
SegmentSealedImpl::get_vector(FieldId field_id,
                              const int64_t* ids,
                              int64_t count) const {
    auto& filed_meta = schema_->operator[](field_id);
    AssertInfo(filed_meta.is_vector(), "vector field is not vector type");

    if (get_bit(index_ready_bitset_, field_id)) {
        AssertInfo(vector_indexings_.is_ready(field_id),
                   "vector index is not ready");
        auto field_indexing = vector_indexings_.get_field_indexing(field_id);
        auto vec_index =
            dynamic_cast<index::VectorIndex*>(field_indexing->indexing_.get());

        auto index_type = vec_index->GetIndexType();
        auto metric_type = vec_index->GetMetricType();
        auto has_raw_data = vec_index->HasRawData();

        if (has_raw_data) {
            auto ids_ds = GenIdsDataset(count, ids);
            auto vector = vec_index->GetVector(ids_ds);
            return segcore::CreateVectorDataArrayFrom(
                vector.data(), count, filed_meta);
        }
    }

    return fill_with_empty(field_id, count);
}

void
SegmentSealedImpl::DropFieldData(const FieldId field_id) {
    if (SystemProperty::Instance().IsSystem(field_id)) {
        auto system_field_type =
            SystemProperty::Instance().GetSystemFieldType(field_id);

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
               "Field id:" + std::to_string(field_id.get()) +
                   " isn't one of system type when drop index");
    auto& field_meta = schema_->operator[](field_id);
    AssertInfo(field_meta.is_vector(),
               "Field meta of offset:" + std::to_string(field_id.get()) +
                   " is not vector type");

    std::unique_lock lck(mutex_);
    vector_indexings_.drop_field_indexing(field_id);
    set_bit(index_ready_bitset_, field_id, false);
}

void
SegmentSealedImpl::check_search(const query::Plan* plan) const {
    AssertInfo(plan, "Search plan is null");
    AssertInfo(plan->extra_info_opt_.has_value(),
               "Extra info of search plan doesn't have value");

    if (!is_system_field_ready()) {
        PanicInfo(
            "failed to load row ID or timestamp, potential missing bin logs or "
            "empty segments. Segment ID = " +
            std::to_string(this->id_));
    }

    auto& request_fields = plan->extra_info_opt_.value().involved_fields_;
    auto field_ready_bitset = field_data_ready_bitset_ | index_ready_bitset_;
    AssertInfo(request_fields.size() == field_ready_bitset.size(),
               "Request fields size not equal to field ready bitset size when "
               "check search");
    auto absent_fields = request_fields - field_ready_bitset;

    if (absent_fields.any()) {
        auto field_id =
            FieldId(absent_fields.find_first() + START_USER_FIELDID);
        auto& field_meta = schema_->operator[](field_id);
        PanicInfo("User Field(" + field_meta.get_name().get() +
                  ") is not loaded");
    }
}

SegmentSealedImpl::SegmentSealedImpl(SchemaPtr schema, int64_t segment_id)
    : schema_(schema),
      insert_record_(*schema, MAX_ROW_COUNT),
      field_data_ready_bitset_(schema->size()),
      index_ready_bitset_(schema->size()),
      scalar_indexings_(schema->size()),
      id_(segment_id) {
}

void
SegmentSealedImpl::bulk_subscript(SystemFieldType system_type,
                                  const int64_t* seg_offsets,
                                  int64_t count,
                                  void* output) const {
    AssertInfo(is_system_field_ready(),
               "System field isn't ready when do bulk_insert");
    switch (system_type) {
        case SystemFieldType::Timestamp:
            AssertInfo(
                insert_record_.timestamps_.num_chunk() == 1,
                "num chunk of timestamp not equal to 1 for sealed segment");
            bulk_subscript_impl<Timestamp>(
                this->insert_record_.timestamps_.get_chunk_data(0),
                seg_offsets,
                count,
                output);
            break;
        case SystemFieldType::RowId:
            AssertInfo(insert_record_.row_ids_.num_chunk() == 1,
                       "num chunk of rowID not equal to 1 for sealed segment");
            bulk_subscript_impl<int64_t>(
                this->insert_record_.row_ids_.get_chunk_data(0),
                seg_offsets,
                count,
                output);
            break;
        default:
            PanicInfo("unknown subscript fields");
    }
}

template <typename T>
void
SegmentSealedImpl::bulk_subscript_impl(const void* src_raw,
                                       const int64_t* seg_offsets,
                                       int64_t count,
                                       void* dst_raw) {
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

template <typename S, typename T>
void
SegmentSealedImpl::bulk_subscript_impl(const ColumnBase* column,
                                       const int64_t* seg_offsets,
                                       int64_t count,
                                       void* dst_raw) {
    auto field = reinterpret_cast<const VariableColumn<S>*>(column);
    auto dst = reinterpret_cast<T*>(dst_raw);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset != INVALID_SEG_OFFSET) {
            dst[i] = std::move(T(field->RawAt(offset)));
        }
    }
}

// for vector
void
SegmentSealedImpl::bulk_subscript_impl(int64_t element_sizeof,
                                       const void* src_raw,
                                       const int64_t* seg_offsets,
                                       int64_t count,
                                       void* dst_raw) {
    auto src_vec = reinterpret_cast<const char*>(src_raw);
    auto dst_vec = reinterpret_cast<char*>(dst_raw);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        auto dst = dst_vec + i * element_sizeof;
        const char* src = (offset == INVALID_SEG_OFFSET
                               ? nullptr
                               : (src_vec + element_sizeof * offset));
        if (!src) {
            continue;
        }
        memcpy(dst, src, element_sizeof);
    }
}

std::unique_ptr<DataArray>
SegmentSealedImpl::fill_with_empty(FieldId field_id, int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    if (datatype_is_vector(field_meta.get_data_type())) {
        return CreateVectorDataArray(count, field_meta);
    }
    return CreateScalarDataArray(count, field_meta);
}

std::unique_ptr<DataArray>
SegmentSealedImpl::bulk_subscript(FieldId field_id,
                                  const int64_t* seg_offsets,
                                  int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    // if count == 0, return empty data array
    if (count == 0) {
        return fill_with_empty(field_id, count);
    }

    if (HasIndex(field_id)) {
        // if field has load scalar index, reverse raw data from index
        if (!datatype_is_vector(field_meta.get_data_type())) {
            AssertInfo(num_chunk() == 1,
                       "num chunk not equal to 1 for sealed segment");
            auto index = chunk_index_impl(field_id, 0);
            return ReverseDataFromIndex(index, seg_offsets, count, field_meta);
        }

        return get_vector(field_id, seg_offsets, count);
    }

    Assert(get_bit(field_data_ready_bitset_, field_id));

    if (datatype_is_variable(field_meta.get_data_type())) {
        switch (field_meta.get_data_type()) {
            case DataType::VARCHAR:
            case DataType::STRING: {
                FixedVector<std::string> output(count);
                bulk_subscript_impl<std::string>(fields_.at(field_id).get(),
                                                 seg_offsets,
                                                 count,
                                                 output.data());
                return CreateScalarDataArrayFrom(
                    output.data(), count, field_meta);
            }

            case DataType::JSON: {
                FixedVector<std::string> output(count);
                bulk_subscript_impl<Json, std::string>(
                    fields_.at(field_id).get(),
                    seg_offsets,
                    count,
                    output.data());
                return CreateScalarDataArrayFrom(
                    output.data(), count, field_meta);
            }

            default:
                PanicInfo(
                    fmt::format("718 unsupported data type: {}",
                                datatype_name(field_meta.get_data_type())));
        }
    }

    auto src_vec = fields_.at(field_id)->Data();
    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            FixedVector<bool> output(count);
            bulk_subscript_impl<bool>(
                src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT8: {
            FixedVector<int8_t> output(count);
            bulk_subscript_impl<int8_t>(
                src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT16: {
            FixedVector<int16_t> output(count);
            bulk_subscript_impl<int16_t>(
                src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT32: {
            FixedVector<int32_t> output(count);
            bulk_subscript_impl<int32_t>(
                src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::INT64: {
            FixedVector<int64_t> output(count);
            bulk_subscript_impl<int64_t>(
                src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::FLOAT: {
            FixedVector<float> output(count);
            bulk_subscript_impl<float>(
                src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }
        case DataType::DOUBLE: {
            FixedVector<double> output(count);
            bulk_subscript_impl<double>(
                src_vec, seg_offsets, count, output.data());
            return CreateScalarDataArrayFrom(output.data(), count, field_meta);
        }

        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY: {
            aligned_vector<char> output(field_meta.get_sizeof() * count);
            bulk_subscript_impl(field_meta.get_sizeof(),
                                src_vec,
                                seg_offsets,
                                count,
                                output.data());
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
    return get_bit(index_ready_bitset_, field_id);
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

bool
SegmentSealedImpl::HasRawData(int64_t field_id) const {
    std::shared_lock lck(mutex_);
    auto fieldID = FieldId(field_id);
    const auto& field_meta = schema_->operator[](fieldID);
    if (datatype_is_vector(field_meta.get_data_type())) {
        if (get_bit(index_ready_bitset_, fieldID)) {
            AssertInfo(vector_indexings_.is_ready(fieldID),
                       "vector index is not ready");
            auto field_indexing = vector_indexings_.get_field_indexing(fieldID);
            auto vec_index = dynamic_cast<index::VectorIndex*>(
                field_indexing->indexing_.get());
            return vec_index->HasRawData();
        }
    }
    return true;
}

std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
SegmentSealedImpl::search_ids(const IdArray& id_array,
                              Timestamp timestamp) const {
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
        auto segOffsets = insert_record_.search_pk(pk, timestamp);
        for (auto offset : segOffsets) {
            switch (data_type) {
                case DataType::INT64: {
                    res_id_arr->mutable_int_id()->add_data(
                        std::get<int64_t>(pk));
                    break;
                }
                case DataType::VARCHAR: {
                    res_id_arr->mutable_str_id()->add_data(
                        std::get<std::string>(pk));
                    break;
                }
                default: {
                    PanicInfo("unsupported type");
                }
            }
            res_offsets.push_back(offset);
        }
    }
    return {std::move(res_id_arr), std::move(res_offsets)};
}

Status
SegmentSealedImpl::Delete(int64_t reserved_offset,  // deprecated
                          int64_t size,
                          const IdArray* ids,
                          const Timestamp* timestamps_raw) {
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

    deleted_record_.push(sort_pks, sort_timestamps.data());
    return Status::OK();
}

std::vector<SegOffset>
SegmentSealedImpl::search_ids(const BitsetType& bitset,
                              Timestamp timestamp) const {
    std::vector<SegOffset> dst_offset;
    for (int offset = bitset.find_first(); offset < bitset.size();
         offset = bitset.find_next(offset)) {
        if (offset == BitsetType::npos) {
            return dst_offset;
        }
        if (insert_record_.timestamps_[offset] <= timestamp) {
            dst_offset.emplace_back(offset);
        }
    }
    return dst_offset;
}

std::vector<SegOffset>
SegmentSealedImpl::search_ids(const BitsetView& bitset,
                              Timestamp timestamp) const {
    std::vector<SegOffset> dst_offset;
    for (int offset = 0; offset < bitset.size(); offset++) {
        if (!bitset.test(offset)) {
            if (insert_record_.timestamps_[offset] <= timestamp) {
                dst_offset.emplace_back(offset);
            }
        }
    }
    return dst_offset;
}

std::vector<SegOffset>
SegmentSealedImpl::search_ids(const BitsetView& bitset,
                              const std::vector<int64_t>& offsets,
                              Timestamp timestamp) const {
    std::vector<SegOffset> dst_offset;
    for (auto& offset : offsets) {
        if (!bitset.test(offset)) {
            if (insert_record_.timestamps_[offset] <= timestamp) {
                dst_offset.emplace_back(offset);
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
SegmentSealedImpl::LoadSegmentMeta(
    const proto::segcore::LoadSegmentMeta& segment_meta) {
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
SegmentSealedImpl::mask_with_timestamps(BitsetType& bitset_chunk,
                                        Timestamp timestamp) const {
    // TODO change the
    AssertInfo(insert_record_.timestamps_.num_chunk() == 1,
               "num chunk not equal to 1 for sealed segment");
    const auto& timestamps_data = insert_record_.timestamps_.get_chunk(0);
    AssertInfo(timestamps_data.size() == get_row_count(),
               "Timestamp size not equal to row count");
    auto range = insert_record_.timestamp_index_.get_active_range(timestamp);

    // range == (size_, size_) and size_ is this->timestamps_.size().
    // it means these data are all useful, we don't need to update bitset_chunk.
    // It can be thought of as an OR operation with another bitmask that is all 0s, but it is not necessary to do so.
    if (range.first == range.second && range.first == timestamps_data.size()) {
        // just skip
        return;
    }
    // range == (0, 0). it means these data can not be used, directly set bitset_chunk to all 1s.
    // It can be thought of as an OR operation with another bitmask that is all 1s.
    if (range.first == range.second && range.first == 0) {
        bitset_chunk.set();
        return;
    }
    auto mask = TimestampIndex::GenerateBitset(
        timestamp, range, timestamps_data.data(), timestamps_data.size());
    bitset_chunk |= mask;
}

}  // namespace milvus::segcore
