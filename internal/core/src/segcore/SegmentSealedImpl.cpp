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
#include <sys/stat.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "Utils.h"
#include "Types.h"
#include "common/Array.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldMeta.h"
#include "common/File.h"
#include "common/Json.h"
#include "common/LoadInfo.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "google/protobuf/message_lite.h"
#include "index/VectorMemIndex.h"
#include "mmap/Column.h"
#include "mmap/Utils.h"
#include "mmap/Types.h"
#include "log/Log.h"
#include "pb/schema.pb.h"
#include "query/ScalarIndex.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "storage/Util.h"
#include "storage/ThreadPools.h"
#include "storage/MmapManager.h"

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
    AssertInfo(
        !get_bit(index_ready_bitset_, field_id),
        "vector index has been exist at " + std::to_string(field_id.get()));
    if (num_rows_.has_value()) {
        AssertInfo(num_rows_.value() == row_count,
                   "field (" + std::to_string(field_id.get()) +
                       ") data has different row count (" +
                       std::to_string(row_count) +
                       ") than other column's row count (" +
                       std::to_string(num_rows_.value()) + ")");
    }
    LOG_INFO(
        "Before setting field_bit for field index, fieldID:{}. segmentID:{}, ",
        info.field_id,
        id_);
    if (get_bit(field_data_ready_bitset_, field_id)) {
        fields_.erase(field_id);
        set_bit(field_data_ready_bitset_, field_id, false);
    } else if (get_bit(binlog_index_bitset_, field_id)) {
        set_bit(binlog_index_bitset_, field_id, false);
        vector_indexings_.drop_field_indexing(field_id);
    }
    update_row_count(row_count);
    vector_indexings_.append_field_indexing(
        field_id,
        metric_type,
        std::move(const_cast<LoadIndexInfo&>(info).index));
    set_bit(index_ready_bitset_, field_id, true);
    LOG_INFO("Has load vec index done, fieldID:{}. segmentID:{}, ",
             info.field_id,
             id_);
}

void
SegmentSealedImpl::WarmupChunkCache(const FieldId field_id, bool mmap_enabled) {
    auto& field_meta = schema_->operator[](field_id);
    AssertInfo(field_meta.is_vector(), "vector field is not vector type");

    if (!get_bit(index_ready_bitset_, field_id) &&
        !get_bit(binlog_index_bitset_, field_id)) {
        return;
    }

    AssertInfo(vector_indexings_.is_ready(field_id),
               "vector index is not ready");
    auto field_indexing = vector_indexings_.get_field_indexing(field_id);
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(field_indexing->indexing_.get());
    AssertInfo(vec_index, "invalid vector indexing");

    auto it = field_data_info_.field_infos.find(field_id.get());
    AssertInfo(it != field_data_info_.field_infos.end(),
               "cannot find binlog file for field: {}, seg: {}",
               field_id.get(),
               id_);
    auto field_info = it->second;

    auto cc = storage::MmapManager::GetInstance().GetChunkCache();
    for (const auto& data_path : field_info.insert_files) {
        auto column =
            cc->Read(data_path, mmap_descriptor_, field_meta, mmap_enabled);
    }
}

void
SegmentSealedImpl::LoadScalarIndex(const LoadIndexInfo& info) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto& field_meta = schema_->operator[](field_id);

    auto row_count = info.index->Count();
    AssertInfo(row_count > 0, "Index count is 0");

    std::unique_lock lck(mutex_);
    AssertInfo(
        !get_bit(index_ready_bitset_, field_id),
        "scalar index has been exist at " + std::to_string(field_id.get()));
    if (num_rows_.has_value()) {
        AssertInfo(num_rows_.value() == row_count,
                   "field (" + std::to_string(field_id.get()) +
                       ") data has different row count (" +
                       std::to_string(row_count) +
                       ") than other column's row count (" +
                       std::to_string(num_rows_.value()) + ")");
    }

    scalar_indexings_[field_id] =
        std::move(const_cast<LoadIndexInfo&>(info).index);
    // reverse pk from scalar index and set pks to offset
    if (schema_->get_primary_field_id() == field_id) {
        AssertInfo(field_id.get() != -1, "Primary key is -1");
        switch (field_meta.get_data_type()) {
            case DataType::INT64: {
                auto int64_index = dynamic_cast<index::ScalarIndex<int64_t>*>(
                    scalar_indexings_[field_id].get());
                if (!is_sorted_by_pk_ && insert_record_.empty_pks() &&
                    int64_index->HasRawData()) {
                    for (int i = 0; i < row_count; ++i) {
                        insert_record_.insert_pk(int64_index->Reverse_Lookup(i),
                                                 i);
                    }
                    insert_record_.seal_pks();
                }
                break;
            }
            case DataType::VARCHAR: {
                auto string_index =
                    dynamic_cast<index::ScalarIndex<std::string>*>(
                        scalar_indexings_[field_id].get());
                if (!is_sorted_by_pk_ && insert_record_.empty_pks() &&
                    string_index->HasRawData()) {
                    for (int i = 0; i < row_count; ++i) {
                        insert_record_.insert_pk(
                            string_index->Reverse_Lookup(i), i);
                    }
                    insert_record_.seal_pks();
                }
                break;
            }
            default: {
                PanicInfo(DataTypeInvalid,
                          fmt::format("unsupported primary key type {}",
                                      field_meta.get_data_type()));
            }
        }
    }

    set_bit(index_ready_bitset_, field_id, true);
    update_row_count(row_count);
    // release field column if the index contains raw data
    if (scalar_indexings_[field_id]->HasRawData() &&
        get_bit(field_data_ready_bitset_, field_id)) {
        fields_.erase(field_id);
        set_bit(field_data_ready_bitset_, field_id, false);
    }

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
        std::sort(insert_files.begin(),
                  insert_files.end(),
                  [](const std::string& a, const std::string& b) {
                      return std::stol(a.substr(a.find_last_of('/') + 1)) <
                             std::stol(b.substr(b.find_last_of('/') + 1));
                  });

        auto field_data_info =
            FieldDataInfo(field_id.get(), num_rows, load_info.mmap_dir_path);
        LOG_INFO("segment {} loads field {} with num_rows {}",
                 this->get_segment_id(),
                 field_id.get(),
                 num_rows);

        auto parallel_degree = static_cast<uint64_t>(
            DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
        field_data_info.channel->set_capacity(parallel_degree * 2);
        auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
        pool.Submit(
            LoadFieldDatasFromRemote, insert_files, field_data_info.channel);

        LOG_INFO("segment {} submits load field {} task to thread pool",
                 this->get_segment_id(),
                 field_id.get());
        bool use_mmap = false;
        if (!info.enable_mmap ||
            SystemProperty::Instance().IsSystem(field_id)) {
            LoadFieldData(field_id, field_data_info);
        } else {
            MapFieldData(field_id, field_data_info);
            use_mmap = true;
        }
        LOG_INFO("segment {} loads field {} mmap {} done",
                 this->get_segment_id(),
                 field_id.get(),
                 use_mmap);
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
            auto field_data = storage::CollectFieldDataChannel(data.channel);
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
            stats_.mem_size += sizeof(Timestamp) * data.row_count;
        } else {
            AssertInfo(system_field_type == SystemFieldType::RowId,
                       "System field type of id column is not RowId");
            // Consume rowid field data but not really load it
            storage::CollectFieldDataChannel(data.channel);
        }
        ++system_ready_count_;
    } else {
        // prepare data
        auto& field_meta = (*schema_)[field_id];
        auto data_type = field_meta.get_data_type();

        // Don't allow raw data and index exist at the same time
        //        AssertInfo(!get_bit(index_ready_bitset_, field_id),
        //                   "field data can't be loaded when indexing exists");
        auto get_block_size = [&]() -> size_t {
            return schema_->get_primary_field_id() == field_id
                       ? DEFAULT_PK_VRCOL_BLOCK_SIZE
                       : DEFAULT_MEM_VRCOL_BLOCK_SIZE;
        };

        std::shared_ptr<ColumnBase> column{};
        if (IsVariableDataType(data_type)) {
            int64_t field_data_size = 0;
            switch (data_type) {
                case milvus::DataType::STRING:
                case milvus::DataType::VARCHAR: {
                    auto var_column =
                        std::make_shared<VariableColumn<std::string>>(
                            num_rows, field_meta, get_block_size());
                    FieldDataPtr field_data;
                    while (data.channel->pop(field_data)) {
                        var_column->Append(std::move(field_data));
                    }
                    var_column->Seal();
                    field_data_size = var_column->ByteSize();
                    stats_.mem_size += var_column->ByteSize();
                    LoadStringSkipIndex(field_id, 0, *var_column);
                    column = std::move(var_column);
                    break;
                }
                case milvus::DataType::JSON: {
                    auto var_column =
                        std::make_shared<VariableColumn<milvus::Json>>(
                            num_rows, field_meta, get_block_size());
                    FieldDataPtr field_data;
                    while (data.channel->pop(field_data)) {
                        var_column->Append(std::move(field_data));
                    }
                    var_column->Seal();
                    stats_.mem_size += var_column->ByteSize();
                    field_data_size = var_column->ByteSize();
                    column = std::move(var_column);
                    break;
                }
                case milvus::DataType::ARRAY: {
                    auto var_column =
                        std::make_shared<ArrayColumn>(num_rows, field_meta);
                    FieldDataPtr field_data;
                    while (data.channel->pop(field_data)) {
                        for (auto i = 0; i < field_data->get_num_rows(); i++) {
                            auto rawValue = field_data->RawValue(i);
                            auto array =
                                static_cast<const milvus::Array*>(rawValue);
                            if (field_data->IsNullable()) {
                                var_column->Append(*array,
                                                   field_data->is_valid(i));
                            } else {
                                var_column->Append(*array);
                            }

                            // we stores the offset for each array element, so there is a additional uint64_t for each array element
                            field_data_size =
                                array->byte_size() + sizeof(uint64_t);
                            stats_.mem_size +=
                                array->byte_size() + sizeof(uint64_t);
                        }
                    }
                    var_column->Seal();
                    column = std::move(var_column);
                    break;
                }
                case milvus::DataType::VECTOR_SPARSE_FLOAT: {
                    auto col = std::make_shared<SparseFloatColumn>(field_meta);
                    FieldDataPtr field_data;
                    while (data.channel->pop(field_data)) {
                        stats_.mem_size += field_data->Size();
                        col->AppendBatch(field_data);
                    }
                    column = std::move(col);
                    break;
                }
                default: {
                    PanicInfo(DataTypeInvalid,
                              fmt::format("unsupported data type", data_type));
                }
            }

            // update average row data size
            SegmentInternalInterface::set_field_avg_size(
                field_id, num_rows, field_data_size);
        } else {
            column = std::make_shared<Column>(num_rows, field_meta);
            FieldDataPtr field_data;
            while (data.channel->pop(field_data)) {
                column->AppendBatch(field_data);
                stats_.mem_size += field_data->Size();
            }
            LoadPrimitiveSkipIndex(field_id,
                                   0,
                                   data_type,
                                   column->Span().data(),
                                   column->Span().valid_data(),
                                   num_rows);
        }

        AssertInfo(column->NumRows() == num_rows,
                   fmt::format("data lost while loading column {}: loaded "
                               "num rows {} but expected {}",
                               data.field_id,
                               column->NumRows(),
                               num_rows));

        {
            std::unique_lock lck(mutex_);
            fields_.emplace(field_id, column);
        }

        // set pks to offset
        // if the segments are already sorted by pk, there is no need to build a pk offset index.
        // it can directly perform a binary search on the pk column.
        if (schema_->get_primary_field_id() == field_id && !is_sorted_by_pk_) {
            AssertInfo(field_id.get() != -1, "Primary key is -1");
            AssertInfo(insert_record_.empty_pks(), "already exists");
            insert_record_.insert_pks(data_type, column);
            insert_record_.seal_pks();
        }

        bool use_temp_index = false;
        {
            // update num_rows to build temperate binlog index
            std::unique_lock lck(mutex_);
            update_row_count(num_rows);
        }

        if (generate_interim_index(field_id)) {
            std::unique_lock lck(mutex_);
            fields_.erase(field_id);
            set_bit(field_data_ready_bitset_, field_id, false);
            use_temp_index = true;
        }

        if (!use_temp_index) {
            std::unique_lock lck(mutex_);
            set_bit(field_data_ready_bitset_, field_id, true);
        }
    }
    {
        std::unique_lock lck(mutex_);
        update_row_count(num_rows);
    }
}

void
SegmentSealedImpl::MapFieldData(const FieldId field_id, FieldDataInfo& data) {
    auto filepath = std::filesystem::path(data.mmap_dir_path) /
                    std::to_string(get_segment_id()) /
                    std::to_string(field_id.get());
    auto dir = filepath.parent_path();
    std::filesystem::create_directories(dir);

    auto file = File::Open(filepath.string(), O_CREAT | O_TRUNC | O_RDWR);

    auto& field_meta = (*schema_)[field_id];
    auto data_type = field_meta.get_data_type();

    // write the field data to disk
    FieldDataPtr field_data;
    uint64_t total_written = 0;
    std::vector<uint64_t> indices{};
    std::vector<std::vector<uint64_t>> element_indices{};
    FixedVector<bool> valid_data{};
    while (data.channel->pop(field_data)) {
        WriteFieldData(file,
                       data_type,
                       field_data,
                       total_written,
                       indices,
                       element_indices,
                       valid_data);
    }
    WriteFieldPadding(file, data_type, total_written);
    std::shared_ptr<ColumnBase> column{};
    auto num_rows = data.row_count;
    if (IsVariableDataType(data_type)) {
        switch (data_type) {
            case milvus::DataType::STRING:
            case milvus::DataType::VARCHAR: {
                auto var_column = std::make_shared<VariableColumn<std::string>>(
                    file,
                    total_written,
                    field_meta,
                    DEFAULT_MMAP_VRCOL_BLOCK_SIZE);
                var_column->Seal(std::move(indices));
                column = std::move(var_column);
                break;
            }
            case milvus::DataType::JSON: {
                auto var_column =
                    std::make_shared<VariableColumn<milvus::Json>>(
                        file,
                        total_written,
                        field_meta,
                        DEFAULT_MMAP_VRCOL_BLOCK_SIZE);
                var_column->Seal(std::move(indices));
                column = std::move(var_column);
                break;
            }
            case milvus::DataType::ARRAY: {
                auto arr_column = std::make_shared<ArrayColumn>(
                    file, total_written, field_meta);
                arr_column->Seal(std::move(indices),
                                 std::move(element_indices));
                column = std::move(arr_column);
                break;
            }
            case milvus::DataType::VECTOR_SPARSE_FLOAT: {
                auto sparse_column = std::make_shared<SparseFloatColumn>(
                    file, total_written, field_meta);
                sparse_column->Seal(std::move(indices));
                column = std::move(sparse_column);
                break;
            }
            default: {
                PanicInfo(DataTypeInvalid,
                          fmt::format("unsupported data type {}", data_type));
            }
        }
    } else {
        column = std::make_shared<Column>(file, total_written, field_meta);
    }

    column->SetValidData(std::move(valid_data));

    {
        std::unique_lock lck(mutex_);
        fields_.emplace(field_id, column);
        mmap_fields_.insert(field_id);
    }

    auto ok = unlink(filepath.c_str());
    AssertInfo(ok == 0,
               fmt::format("failed to unlink mmap data file {}, err: {}",
                           filepath.c_str(),
                           strerror(errno)));

    // set pks to offset
    // no need pk
    if (schema_->get_primary_field_id() == field_id && !is_sorted_by_pk_) {
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

void
SegmentSealedImpl::AddFieldDataInfoForSealed(
    const LoadFieldDataInfo& field_data_info) {
    // copy assignment
    field_data_info_ = field_data_info;
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

std::pair<BufferView, FixedVector<bool>>
SegmentSealedImpl::get_chunk_buffer(FieldId field_id,
                                    int64_t chunk_id,
                                    int64_t start_offset,
                                    int64_t length) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    auto& field_meta = schema_->operator[](field_id);
    if (auto it = fields_.find(field_id); it != fields_.end()) {
        auto& field_data = it->second;
        FixedVector<bool> valid_data;
        if (field_data->IsNullable()) {
            valid_data.reserve(length);
            for (int i = 0; i < length; i++) {
                valid_data.push_back(field_data->IsValid(start_offset + i));
            }
        }
        return std::make_pair(field_data->GetBatchBuffer(start_offset, length),
                              valid_data);
    }
    PanicInfo(ErrorCode::UnexpectedError,
              "get_chunk_buffer only used for  variable column field");
}

bool
SegmentSealedImpl::is_mmap_field(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    return mmap_fields_.find(field_id) != mmap_fields_.end();
}

SpanBase
SegmentSealedImpl::chunk_data_impl(FieldId field_id, int64_t chunk_id) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    auto& field_meta = schema_->operator[](field_id);
    if (auto it = fields_.find(field_id); it != fields_.end()) {
        auto& field_data = it->second;
        return field_data->Span();
    }
    auto field_data = insert_record_.get_data_base(field_id);
    AssertInfo(field_data->num_chunk() == 1,
               "num chunk not equal to 1 for sealed segment");
    // system field
    return field_data->get_span_base(0);
}

std::pair<std::vector<std::string_view>, FixedVector<bool>>
SegmentSealedImpl::chunk_view_impl(FieldId field_id, int64_t chunk_id) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    auto& field_meta = schema_->operator[](field_id);
    if (auto it = fields_.find(field_id); it != fields_.end()) {
        auto& field_data = it->second;
        return field_data->StringViews();
    }
    PanicInfo(ErrorCode::UnexpectedError,
              "chunk_view_impl only used for variable column field ");
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
SegmentSealedImpl::get_row_count() const {
    std::shared_lock lck(mutex_);
    return num_rows_.value_or(0);
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

std::vector<SegOffset>
SegmentSealedImpl::search_pk(const PkType& pk, Timestamp timestamp) const {
    auto pk_field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = fields_.at(pk_field_id);
    std::vector<SegOffset> pk_offsets;
    switch (schema_->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64: {
            auto target = std::get<int64_t>(pk);
            // get int64 pks
            auto src = reinterpret_cast<const int64_t*>(pk_column->Data());
            auto it =
                std::lower_bound(src,
                                 src + pk_column->NumRows(),
                                 target,
                                 [](const int64_t& elem, const int64_t& value) {
                                     return elem < value;
                                 });
            for (; it != src + pk_column->NumRows() && *it == target; it++) {
                auto offset = it - src;
                if (insert_record_.timestamps_[offset] <= timestamp) {
                    pk_offsets.emplace_back(it - src);
                }
            }
            break;
        }
        case DataType::VARCHAR: {
            auto target = std::get<std::string>(pk);
            // get varchar pks
            auto var_column =
                std::dynamic_pointer_cast<VariableColumn<std::string>>(
                    pk_column);
            auto views = var_column->Views();
            auto it = std::lower_bound(views.begin(), views.end(), target);
            for (; it != views.end() && *it == target; it++) {
                auto offset = std::distance(views.begin(), it);
                if (insert_record_.timestamps_[offset] <= timestamp) {
                    pk_offsets.emplace_back(offset);
                }
            }
            break;
        }
        default: {
            PanicInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported type {}",
                    schema_->get_fields().at(pk_field_id).get_data_type()));
        }
    }

    return pk_offsets;
}

std::vector<SegOffset>
SegmentSealedImpl::search_pk(const PkType& pk, int64_t insert_barrier) const {
    auto pk_field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = fields_.at(pk_field_id);
    std::vector<SegOffset> pk_offsets;
    switch (schema_->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64: {
            auto target = std::get<int64_t>(pk);
            // get int64 pks
            auto src = reinterpret_cast<const int64_t*>(pk_column->Data());
            auto it =
                std::lower_bound(src,
                                 src + pk_column->NumRows(),
                                 target,
                                 [](const int64_t& elem, const int64_t& value) {
                                     return elem < value;
                                 });
            for (; it != src + pk_column->NumRows() && *it == target; it++) {
                if (it - src < insert_barrier) {
                    pk_offsets.emplace_back(it - src);
                }
            }
            break;
        }
        case DataType::VARCHAR: {
            auto target = std::get<std::string>(pk);
            // get varchar pks
            auto var_column =
                std::dynamic_pointer_cast<VariableColumn<std::string>>(
                    pk_column);
            auto views = var_column->Views();
            auto it = std::lower_bound(views.begin(), views.end(), target);
            while (it != views.end() && *it == target) {
                auto offset = std::distance(views.begin(), it);
                if (offset < insert_barrier) {
                    pk_offsets.emplace_back(offset);
                }
                ++it;
            }
            break;
        }
        default: {
            PanicInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported type {}",
                    schema_->get_fields().at(pk_field_id).get_data_type()));
        }
    }

    return pk_offsets;
}

std::shared_ptr<DeletedRecord::TmpBitmap>
SegmentSealedImpl::get_deleted_bitmap_s(int64_t del_barrier,
                                        int64_t insert_barrier,
                                        DeletedRecord& delete_record,
                                        Timestamp query_timestamp) const {
    // if insert_barrier and del_barrier have not changed, use cache data directly
    bool hit_cache = false;
    int64_t old_del_barrier = 0;
    auto current = delete_record.clone_lru_entry(
        insert_barrier, del_barrier, old_del_barrier, hit_cache);
    if (hit_cache) {
        return current;
    }

    auto bitmap = current->bitmap_ptr;

    int64_t start, end;
    if (del_barrier < old_del_barrier) {
        // in this case, ts of delete record[current_del_barrier : old_del_barrier] > query_timestamp
        // so these deletion records do not take effect in query/search
        // so bitmap corresponding to those pks in delete record[current_del_barrier:old_del_barrier] will be reset to 0
        // for example, current_del_barrier = 2, query_time = 120, the bitmap will be reset to [0, 1, 1, 0, 0, 0, 0, 0]
        start = del_barrier;
        end = old_del_barrier;
    } else {
        // the cache is not enough, so update bitmap using new pks in delete record[old_del_barrier:current_del_barrier]
        // for example, current_del_barrier = 4, query_time = 300, bitmap will be updated to [0, 1, 1, 0, 1, 1, 0, 0]
        start = old_del_barrier;
        end = del_barrier;
    }

    // Avoid invalid calculations when there are a lot of repeated delete pks
    std::unordered_map<PkType, Timestamp> delete_timestamps;
    for (auto del_index = start; del_index < end; ++del_index) {
        auto pk = delete_record.pks()[del_index];
        auto timestamp = delete_record.timestamps()[del_index];

        delete_timestamps[pk] = timestamp > delete_timestamps[pk]
                                    ? timestamp
                                    : delete_timestamps[pk];
    }

    for (auto& [pk, timestamp] : delete_timestamps) {
        auto segOffsets = search_pk(pk, insert_barrier);
        for (auto offset : segOffsets) {
            int64_t insert_row_offset = offset.get();

            // The deletion record do not take effect in search/query,
            // and reset bitmap to 0
            if (timestamp > query_timestamp) {
                bitmap->reset(insert_row_offset);
                continue;
            }
            // Insert after delete with same pk, delete will not task effect on this insert record,
            // and reset bitmap to 0
            if (insert_record_.timestamps_[offset.get()] >= timestamp) {
                bitmap->reset(insert_row_offset);
                continue;
            }
            // insert data corresponding to the insert_row_offset will be ignored in search/query
            bitmap->set(insert_row_offset);
        }
    }

    delete_record.insert_lru_entry(current);
    return current;
}

void
SegmentSealedImpl::mask_with_delete(BitsetType& bitset,
                                    int64_t ins_barrier,
                                    Timestamp timestamp) const {
    auto del_barrier = get_barrier(get_deleted_record(), timestamp);
    if (del_barrier == 0) {
        return;
    }

    auto bitmap_holder = std::shared_ptr<DeletedRecord::TmpBitmap>();

    if (!is_sorted_by_pk_) {
        bitmap_holder = get_deleted_bitmap(del_barrier,
                                           ins_barrier,
                                           deleted_record_,
                                           insert_record_,
                                           timestamp);
    } else {
        bitmap_holder = get_deleted_bitmap_s(
            del_barrier, ins_barrier, deleted_record_, timestamp);
    }

    if (!bitmap_holder || !bitmap_holder->bitmap_ptr) {
        return;
    }
    auto& delete_bitset = *bitmap_holder->bitmap_ptr;
    AssertInfo(
        delete_bitset.size() == bitset.size(),
        fmt::format(
            "Deleted bitmap size:{} not equal to filtered bitmap size:{}",
            delete_bitset.size(),
            bitset.size()));
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
    if (get_bit(binlog_index_bitset_, field_id)) {
        AssertInfo(
            vec_binlog_config_.find(field_id) != vec_binlog_config_.end(),
            "The binlog params is not generate.");
        auto binlog_search_info =
            vec_binlog_config_.at(field_id)->GetSearchConf(search_info);

        AssertInfo(vector_indexings_.is_ready(field_id),
                   "vector indexes isn't ready for field " +
                       std::to_string(field_id.get()));
        query::SearchOnSealedIndex(*schema_,
                                   vector_indexings_,
                                   binlog_search_info,
                                   query_data,
                                   query_count,
                                   bitset,
                                   output);
        milvus::tracer::AddEvent(
            "finish_searching_vector_temperate_binlog_index");
    } else if (get_bit(index_ready_bitset_, field_id)) {
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
        milvus::tracer::AddEvent("finish_searching_vector_index");
    } else {
        AssertInfo(
            get_bit(field_data_ready_bitset_, field_id),
            "Field Data is not loaded: " + std::to_string(field_id.get()));
        AssertInfo(num_rows_.has_value(), "Can't get row count value");
        auto row_count = num_rows_.value();
        auto vec_data = fields_.at(field_id);
        query::SearchOnSealed(*schema_,
                              vec_data->Data(),
                              search_info,
                              query_data,
                              query_count,
                              row_count,
                              bitset,
                              output);
        milvus::tracer::AddEvent("finish_searching_vector_data");
    }
}

std::tuple<std::string, int64_t>
SegmentSealedImpl::GetFieldDataPath(FieldId field_id, int64_t offset) const {
    auto offset_in_binlog = offset;
    auto data_path = std::string();
    auto it = field_data_info_.field_infos.find(field_id.get());
    AssertInfo(it != field_data_info_.field_infos.end(),
               fmt::format("cannot find binlog file for field: {}, seg: {}",
                           field_id.get(),
                           id_));
    auto field_info = it->second;

    for (auto i = 0; i < field_info.insert_files.size(); i++) {
        if (offset_in_binlog < field_info.entries_nums[i]) {
            data_path = field_info.insert_files[i];
            break;
        } else {
            offset_in_binlog -= field_info.entries_nums[i];
        }
    }
    return {data_path, offset_in_binlog};
}

std::tuple<std::string, std::shared_ptr<ColumnBase>> static ReadFromChunkCache(
    const storage::ChunkCachePtr& cc,
    const std::string& data_path,
    const storage::MmapChunkDescriptorPtr& descriptor) {
    // For mmap mode, field_meta is unused, so just construct a fake field meta.
    auto fm =
        FieldMeta(FieldName(""), FieldId(0), milvus::DataType::NONE, false);
    // TODO: add Load() interface for chunk cache when support retrieve_enable, make Read() raise error if cache miss
    auto column = cc->Read(data_path, descriptor, fm, true);
    cc->Prefetch(data_path);
    return {data_path, column};
}

std::unique_ptr<DataArray>
SegmentSealedImpl::get_vector(FieldId field_id,
                              const int64_t* ids,
                              int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    AssertInfo(field_meta.is_vector(), "vector field is not vector type");

    if (!get_bit(index_ready_bitset_, field_id) &&
        !get_bit(binlog_index_bitset_, field_id)) {
        return fill_with_empty(field_id, count);
    }

    AssertInfo(vector_indexings_.is_ready(field_id),
               "vector index is not ready");
    auto field_indexing = vector_indexings_.get_field_indexing(field_id);
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(field_indexing->indexing_.get());
    AssertInfo(vec_index, "invalid vector indexing");

    auto index_type = vec_index->GetIndexType();
    auto metric_type = vec_index->GetMetricType();
    auto has_raw_data = vec_index->HasRawData();

    if (has_raw_data && !TEST_skip_index_for_retrieve_) {
        // If index has raw data, get vector from memory.
        auto ids_ds = GenIdsDataset(count, ids);
        if (field_meta.get_data_type() == DataType::VECTOR_SPARSE_FLOAT) {
            auto res = vec_index->GetSparseVector(ids_ds);
            return segcore::CreateVectorDataArrayFrom(
                res.get(), count, field_meta);
        } else {
            // dense vector:
            auto vector = vec_index->GetVector(ids_ds);
            return segcore::CreateVectorDataArrayFrom(
                vector.data(), count, field_meta);
        }
    }

    // If index doesn't have raw data, get vector from chunk cache.
    auto cc = storage::MmapManager::GetInstance().GetChunkCache();

    // group by data_path
    auto id_to_data_path =
        std::unordered_map<std::int64_t, std::tuple<std::string, int64_t>>{};
    auto path_to_column =
        std::unordered_map<std::string, std::shared_ptr<ColumnBase>>{};
    for (auto i = 0; i < count; i++) {
        const auto& tuple = GetFieldDataPath(field_id, ids[i]);
        id_to_data_path.emplace(ids[i], tuple);
        path_to_column.emplace(std::get<0>(tuple), nullptr);
    }

    // read and prefetch
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    std::vector<
        std::future<std::tuple<std::string, std::shared_ptr<ColumnBase>>>>
        futures;
    futures.reserve(path_to_column.size());
    for (const auto& iter : path_to_column) {
        const auto& data_path = iter.first;
        futures.emplace_back(
            pool.Submit(ReadFromChunkCache, cc, data_path, mmap_descriptor_));
    }

    for (int i = 0; i < futures.size(); ++i) {
        const auto& [data_path, column] = futures[i].get();
        path_to_column[data_path] = column;
    }

    if (field_meta.get_data_type() == DataType::VECTOR_SPARSE_FLOAT) {
        auto buf = std::vector<knowhere::sparse::SparseRow<float>>(count);
        for (auto i = 0; i < count; ++i) {
            const auto& [data_path, offset_in_binlog] =
                id_to_data_path.at(ids[i]);
            const auto& column = path_to_column.at(data_path);
            AssertInfo(
                offset_in_binlog < column->NumRows(),
                "column idx out of range, idx: {}, size: {}, data_path: {}",
                offset_in_binlog,
                column->NumRows(),
                data_path);
            auto sparse_column =
                std::dynamic_pointer_cast<SparseFloatColumn>(column);
            AssertInfo(sparse_column, "incorrect column created");
            buf[i] = static_cast<const knowhere::sparse::SparseRow<float>*>(
                static_cast<const void*>(
                    sparse_column->Data()))[offset_in_binlog];
        }
        return segcore::CreateVectorDataArrayFrom(
            buf.data(), count, field_meta);
    } else {
        // assign to data array
        auto row_bytes = field_meta.get_sizeof();
        auto buf = std::vector<char>(count * row_bytes);
        for (auto i = 0; i < count; ++i) {
            AssertInfo(id_to_data_path.count(ids[i]) != 0, "id not found");
            const auto& [data_path, offset_in_binlog] =
                id_to_data_path.at(ids[i]);
            AssertInfo(path_to_column.count(data_path) != 0,
                       "column not found");
            const auto& column = path_to_column.at(data_path);
            AssertInfo(
                offset_in_binlog * row_bytes < column->ByteSize(),
                "column idx out of range, idx: {}, size: {}, data_path: {}",
                offset_in_binlog * row_bytes,
                column->ByteSize(),
                data_path);
            auto vector = &column->Data()[offset_in_binlog * row_bytes];
            std::memcpy(buf.data() + i * row_bytes, vector, row_bytes);
        }
        return segcore::CreateVectorDataArrayFrom(
            buf.data(), count, field_meta);
    }
}

void
SegmentSealedImpl::DropFieldData(const FieldId field_id) {
    if (SystemProperty::Instance().IsSystem(field_id)) {
        auto system_field_type =
            SystemProperty::Instance().GetSystemFieldType(field_id);

        std::unique_lock lck(mutex_);
        --system_ready_count_;
        if (system_field_type == SystemFieldType::Timestamp) {
            insert_record_.timestamps_.clear();
        }
        lck.unlock();
    } else {
        auto& field_meta = schema_->operator[](field_id);
        std::unique_lock lck(mutex_);
        if (get_bit(field_data_ready_bitset_, field_id)) {
            fields_.erase(field_id);
            set_bit(field_data_ready_bitset_, field_id, false);
        }
        if (get_bit(binlog_index_bitset_, field_id)) {
            set_bit(binlog_index_bitset_, field_id, false);
            vector_indexings_.drop_field_indexing(field_id);
        }
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
            FieldNotLoaded,
            "failed to load row ID or timestamp, potential missing bin logs or "
            "empty segments. Segment ID = " +
                std::to_string(this->id_));
    }

    auto& request_fields = plan->extra_info_opt_.value().involved_fields_;
    auto field_ready_bitset =
        field_data_ready_bitset_ | index_ready_bitset_ | binlog_index_bitset_;
    AssertInfo(request_fields.size() == field_ready_bitset.size(),
               "Request fields size not equal to field ready bitset size when "
               "check search");
    auto absent_fields = request_fields - field_ready_bitset;

    if (absent_fields.any()) {
        // absent_fields.find_first() returns std::optional<>
        auto field_id =
            FieldId(absent_fields.find_first().value() + START_USER_FIELDID);
        auto& field_meta = schema_->operator[](field_id);
        PanicInfo(
            FieldNotLoaded,
            "User Field(" + field_meta.get_name().get() + ") is not loaded");
    }
}

SegmentSealedImpl::SegmentSealedImpl(SchemaPtr schema,
                                     IndexMetaPtr index_meta,
                                     const SegcoreConfig& segcore_config,
                                     int64_t segment_id,
                                     bool TEST_skip_index_for_retrieve,
                                     bool is_sorted_by_pk)
    : segcore_config_(segcore_config),
      field_data_ready_bitset_(schema->size()),
      index_ready_bitset_(schema->size()),
      binlog_index_bitset_(schema->size()),
      scalar_indexings_(schema->size()),
      insert_record_(*schema, MAX_ROW_COUNT),
      schema_(schema),
      id_(segment_id),
      col_index_meta_(index_meta),
      TEST_skip_index_for_retrieve_(TEST_skip_index_for_retrieve),
      is_sorted_by_pk_(is_sorted_by_pk) {
    mmap_descriptor_ = std::shared_ptr<storage::MmapChunkDescriptor>(
        new storage::MmapChunkDescriptor({segment_id, SegmentType::Sealed}));
    auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
    mcm->Register(mmap_descriptor_);
}

SegmentSealedImpl::~SegmentSealedImpl() {
    auto cc = storage::MmapManager::GetInstance().GetChunkCache();
    if (cc == nullptr) {
        return;
    }
    // munmap and remove binlog from chunk cache
    for (const auto& iter : field_data_info_.field_infos) {
        for (const auto& binlog : iter.second.insert_files) {
            cc->Remove(binlog);
        }
    }
    if (mmap_descriptor_ != nullptr) {
        auto mm = storage::MmapManager::GetInstance().GetMmapChunkManager();
        mm->UnRegister(mmap_descriptor_);
    }
}

void
SegmentSealedImpl::bulk_subscript(SystemFieldType system_type,
                                  const int64_t* seg_offsets,
                                  int64_t count,
                                  void* output) const {
    AssertInfo(is_system_field_ready(),
               "System field isn't ready when do bulk_insert, segID:{}",
               id_);
    switch (system_type) {
        case SystemFieldType::Timestamp:
            AssertInfo(
                insert_record_.timestamps_.num_chunk() == 1,
                "num chunk of timestamp not equal to 1 for sealed segment");
            bulk_subscript_impl<Timestamp>(
                this->insert_record_.timestamps_.get_chunk_data(0),
                seg_offsets,
                count,
                static_cast<Timestamp*>(output));
            break;
        case SystemFieldType::RowId:
            PanicInfo(ErrorCode::Unsupported, "RowId retrieve not supported");
            break;
        default:
            PanicInfo(DataTypeInvalid,
                      fmt::format("unknown subscript fields", system_type));
    }
}

template <typename S, typename T>
void
SegmentSealedImpl::bulk_subscript_impl(const void* src_raw,
                                       const int64_t* seg_offsets,
                                       int64_t count,
                                       T* dst) {
    static_assert(IsScalar<T>);
    auto src = static_cast<const S*>(src_raw);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst[i] = src[offset];
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
        dst[i] = std::move(T(field->RawAt(offset)));
    }
}

template <typename S, typename T>
void
SegmentSealedImpl::bulk_subscript_ptr_impl(
    const ColumnBase* column,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) {
    auto field = reinterpret_cast<const VariableColumn<S>*>(column);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst->at(i) = std::move(T(field->RawAt(offset)));
    }
}

template <typename T>
void
SegmentSealedImpl::bulk_subscript_array_impl(
    const ColumnBase* column,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) {
    auto field = reinterpret_cast<const ArrayColumn*>(column);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst->at(i) = std::move(field->RawAt(offset));
    }
}

// for dense vector
void
SegmentSealedImpl::bulk_subscript_impl(int64_t element_sizeof,
                                       const void* src_raw,
                                       const int64_t* seg_offsets,
                                       int64_t count,
                                       void* dst_raw) {
    auto column = reinterpret_cast<const char*>(src_raw);
    auto dst_vec = reinterpret_cast<char*>(dst_raw);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        auto src = column + element_sizeof * offset;
        auto dst = dst_vec + i * element_sizeof;
        memcpy(dst, src, element_sizeof);
    }
}

void
SegmentSealedImpl::ClearData() {
    {
        std::unique_lock lck(mutex_);
        field_data_ready_bitset_.reset();
        index_ready_bitset_.reset();
        binlog_index_bitset_.reset();
        system_ready_count_ = 0;
        num_rows_ = std::nullopt;
        scalar_indexings_.clear();
        vector_indexings_.clear();
        insert_record_.clear();
        fields_.clear();
        variable_fields_avg_size_.clear();
        stats_.mem_size = 0;
    }
    auto cc = storage::MmapManager::GetInstance().GetChunkCache();
    if (cc == nullptr) {
        return;
    }
    // munmap and remove binlog from chunk cache
    for (const auto& iter : field_data_info_.field_infos) {
        for (const auto& binlog : iter.second.insert_files) {
            cc->Remove(binlog);
        }
    }
}

std::unique_ptr<DataArray>
SegmentSealedImpl::fill_with_empty(FieldId field_id, int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    if (IsVectorDataType(field_meta.get_data_type())) {
        return CreateVectorDataArray(count, field_meta);
    }
    return CreateScalarDataArray(count, field_meta);
}

std::unique_ptr<DataArray>
SegmentSealedImpl::get_raw_data(FieldId field_id,
                                const FieldMeta& field_meta,
                                const int64_t* seg_offsets,
                                int64_t count) const {
    // DO NOT directly access the column by map like: `fields_.at(field_id)->Data()`,
    // we have to clone the shared pointer,
    // to make sure it won't get released if segment released
    auto column = fields_.at(field_id);
    auto ret = fill_with_empty(field_id, count);
    if (column->IsNullable()) {
        auto dst = ret->mutable_valid_data()->mutable_data();
        for (int64_t i = 0; i < count; ++i) {
            auto offset = seg_offsets[i];
            dst[i] = column->IsValid(offset);
        }
    }
    switch (field_meta.get_data_type()) {
        case DataType::VARCHAR:
        case DataType::STRING: {
            bulk_subscript_ptr_impl<std::string>(
                column.get(),
                seg_offsets,
                count,
                ret->mutable_scalars()->mutable_string_data()->mutable_data());
            break;
        }

        case DataType::JSON: {
            bulk_subscript_ptr_impl<Json, std::string>(
                column.get(),
                seg_offsets,
                count,
                ret->mutable_scalars()->mutable_json_data()->mutable_data());
            break;
        }

        case DataType::ARRAY: {
            bulk_subscript_array_impl(
                column.get(),
                seg_offsets,
                count,
                ret->mutable_scalars()->mutable_array_data()->mutable_data());
            break;
        }

        case DataType::BOOL: {
            bulk_subscript_impl<bool>(column->Data(),
                                      seg_offsets,
                                      count,
                                      ret->mutable_scalars()
                                          ->mutable_bool_data()
                                          ->mutable_data()
                                          ->mutable_data());
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t>(column->Data(),
                                        seg_offsets,
                                        count,
                                        ret->mutable_scalars()
                                            ->mutable_int_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t>(column->Data(),
                                         seg_offsets,
                                         count,
                                         ret->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(column->Data(),
                                         seg_offsets,
                                         count,
                                         ret->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(column->Data(),
                                         seg_offsets,
                                         count,
                                         ret->mutable_scalars()
                                             ->mutable_long_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(column->Data(),
                                       seg_offsets,
                                       count,
                                       ret->mutable_scalars()
                                           ->mutable_float_data()
                                           ->mutable_data()
                                           ->mutable_data());
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(column->Data(),
                                        seg_offsets,
                                        count,
                                        ret->mutable_scalars()
                                            ->mutable_double_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::VECTOR_FLOAT: {
            bulk_subscript_impl(field_meta.get_sizeof(),
                                column->Data(),
                                seg_offsets,
                                count,
                                ret->mutable_vectors()
                                    ->mutable_float_vector()
                                    ->mutable_data()
                                    ->mutable_data());
            break;
        }
        case DataType::VECTOR_FLOAT16: {
            bulk_subscript_impl(
                field_meta.get_sizeof(),
                column->Data(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_float16_vector()->data());
            break;
        }
        case DataType::VECTOR_BFLOAT16: {
            bulk_subscript_impl(
                field_meta.get_sizeof(),
                column->Data(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_bfloat16_vector()->data());
            break;
        }
        case DataType::VECTOR_BINARY: {
            bulk_subscript_impl(
                field_meta.get_sizeof(),
                column->Data(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_binary_vector()->data());
            break;
        }
        case DataType::VECTOR_SPARSE_FLOAT: {
            auto rows = static_cast<const knowhere::sparse::SparseRow<float>*>(
                static_cast<const void*>(column->Data()));
            auto dst = ret->mutable_vectors()->mutable_sparse_float_vector();
            SparseRowsToProto(
                [&](size_t i) {
                    auto offset = seg_offsets[i];
                    return offset != INVALID_SEG_OFFSET ? (rows + offset)
                                                        : nullptr;
                },
                count,
                dst);
            ret->mutable_vectors()->set_dim(dst->dim());
            break;
        }

        default: {
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported data type {}",
                                  field_meta.get_data_type()));
        }
    }
    return ret;
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
        if (!IsVectorDataType(field_meta.get_data_type())) {
            AssertInfo(num_chunk() == 1,
                       "num chunk not equal to 1 for sealed segment");
            auto index = chunk_index_impl(field_id, 0);
            if (index->HasRawData()) {
                return ReverseDataFromIndex(
                    index, seg_offsets, count, field_meta);
            }
            return get_raw_data(field_id, field_meta, seg_offsets, count);
        }
        return get_vector(field_id, seg_offsets, count);
    }

    Assert(get_bit(field_data_ready_bitset_, field_id));

    return get_raw_data(field_id, field_meta, seg_offsets, count);
}

std::unique_ptr<DataArray>
SegmentSealedImpl::bulk_subscript(
    FieldId field_id,
    const int64_t* seg_offsets,
    int64_t count,
    const std::vector<std::string>& dynamic_field_names) const {
    Assert(!dynamic_field_names.empty());
    auto& field_meta = schema_->operator[](field_id);
    if (count == 0) {
        return fill_with_empty(field_id, 0);
    }

    auto column = fields_.at(field_id);
    auto ret = fill_with_empty(field_id, count);
    if (column->IsNullable()) {
        auto dst = ret->mutable_valid_data()->mutable_data();
        for (int64_t i = 0; i < count; ++i) {
            auto offset = seg_offsets[i];
            dst[i] = column->IsValid(offset);
        }
    }
    auto dst = ret->mutable_scalars()->mutable_json_data()->mutable_data();
    auto field = reinterpret_cast<const VariableColumn<Json>*>(column.get());
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst->at(i) = ExtractSubJson(std::string(field->RawAt(offset)),
                                    dynamic_field_names);
    }
    return ret;
}

bool
SegmentSealedImpl::HasIndex(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    return get_bit(index_ready_bitset_, field_id) |
           get_bit(binlog_index_bitset_, field_id);
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
    if (IsVectorDataType(field_meta.get_data_type())) {
        if (get_bit(index_ready_bitset_, fieldID) |
            get_bit(binlog_index_bitset_, fieldID)) {
            AssertInfo(vector_indexings_.is_ready(fieldID),
                       "vector index is not ready");
            auto field_indexing = vector_indexings_.get_field_indexing(fieldID);
            auto vec_index = dynamic_cast<index::VectorIndex*>(
                field_indexing->indexing_.get());
            return vec_index->HasRawData();
        }
    } else {
        auto scalar_index = scalar_indexings_.find(fieldID);
        if (scalar_index != scalar_indexings_.end()) {
            return scalar_index->second->HasRawData();
        }
    }
    return true;
}

DataType
SegmentSealedImpl::GetFieldDataType(milvus::FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    return field_meta.get_data_type();
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
    res_offsets.reserve(pks.size());

    for (auto& pk : pks) {
        std::vector<SegOffset> pk_offsets;
        if (!is_sorted_by_pk_) {
            pk_offsets = insert_record_.search_pk(pk, timestamp);
        } else {
            pk_offsets = search_pk(pk, timestamp);
        }
        for (auto offset : pk_offsets) {
            switch (data_type) {
                case DataType::INT64: {
                    res_id_arr->mutable_int_id()->add_data(
                        std::get<int64_t>(pk));
                    break;
                }
                case DataType::VARCHAR: {
                    res_id_arr->mutable_str_id()->add_data(
                        std::get<std::string>(std::move(pk)));
                    break;
                }
                default: {
                    PanicInfo(DataTypeInvalid,
                              fmt::format("unsupported type {}", data_type));
                }
            }
            res_offsets.push_back(offset);
        }
    }
    return {std::move(res_id_arr), std::move(res_offsets)};
}

std::pair<std::vector<OffsetMap::OffsetType>, bool>
SegmentSealedImpl::find_first(int64_t limit, const BitsetType& bitset) const {
    if (!is_sorted_by_pk_) {
        return insert_record_.pk2offset_->find_first(limit, bitset);
    }
    if (limit == Unlimited || limit == NoLimit) {
        limit = num_rows_.value();
    }

    int64_t hit_num = 0;  // avoid counting the number everytime.
    auto size = bitset.size();
    int64_t cnt = size - bitset.count();
    auto more_hit_than_limit = cnt > limit;
    limit = std::min(limit, cnt);
    std::vector<int64_t> seg_offsets;
    seg_offsets.reserve(limit);

    int64_t offset = 0;
    for (; hit_num < limit && offset < num_rows_.value(); offset++) {
        if (offset >= size) {
            // In fact, this case won't happen on sealed segments.
            continue;
        }

        if (!bitset[offset]) {
            seg_offsets.push_back(offset);
            hit_num++;
        }
    }

    return {seg_offsets, more_hit_than_limit && offset != num_rows_.value()};
}

SegcoreError
SegmentSealedImpl::Delete(int64_t reserved_offset,  // deprecated
                          int64_t size,
                          const IdArray* ids,
                          const Timestamp* timestamps_raw) {
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_->operator[](field_id);
    std::vector<PkType> pks(size);
    ParsePksFromIDs(pks, field_meta.get_data_type(), *ids);

    // filter out the deletions that the primary key not exists
    std::vector<std::tuple<Timestamp, PkType>> ordering(size);
    for (int i = 0; i < size; i++) {
        ordering[i] = std::make_tuple(timestamps_raw[i], pks[i]);
    }
    // if insert_record_ is empty (may be only-load meta but not data for lru-cache at go side),
    // filtering may cause the deletion lost, skip the filtering to avoid it.
    if (!insert_record_.empty_pks()) {
        auto end = std::remove_if(
            ordering.begin(),
            ordering.end(),
            [&](const std::tuple<Timestamp, PkType>& record) {
                return !insert_record_.contain(std::get<1>(record));
            });
        size = end - ordering.begin();
        ordering.resize(size);
    }
    if (size == 0) {
        return SegcoreError::success();
    }

    // step 1: sort timestamp
    std::sort(ordering.begin(), ordering.end());
    std::vector<PkType> sort_pks(size);
    std::vector<Timestamp> sort_timestamps(size);

    for (int i = 0; i < size; i++) {
        auto [t, pk] = ordering[i];
        sort_timestamps[i] = t;
        sort_pks[i] = pk;
    }

    deleted_record_.push(sort_pks, sort_timestamps.data());
    return SegcoreError::success();
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
    PanicInfo(NotImplemented, "unimplemented");
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
    auto timestamps_data =
        (const milvus::Timestamp*)insert_record_.timestamps_.get_chunk_data(0);
    auto timestamps_data_size = insert_record_.timestamps_.get_chunk_size(0);

    AssertInfo(timestamps_data_size == get_row_count(),
               fmt::format("Timestamp size not equal to row count: {}, {}",
                           timestamps_data_size,
                           get_row_count()));
    auto range = insert_record_.timestamp_index_.get_active_range(timestamp);

    // range == (size_, size_) and size_ is this->timestamps_.size().
    // it means these data are all useful, we don't need to update bitset_chunk.
    // It can be thought of as an OR operation with another bitmask that is all 0s, but it is not necessary to do so.
    if (range.first == range.second && range.first == timestamps_data_size) {
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
        timestamp, range, timestamps_data, timestamps_data_size);
    bitset_chunk |= mask;
}

bool
SegmentSealedImpl::generate_interim_index(const FieldId field_id) {
    if (col_index_meta_ == nullptr || !col_index_meta_->HasFiled(field_id)) {
        return false;
    }
    auto& field_meta = schema_->operator[](field_id);
    auto& field_index_meta = col_index_meta_->GetFieldIndexMeta(field_id);
    auto& index_params = field_index_meta.GetIndexParams();

    bool is_sparse =
        field_meta.get_data_type() == DataType::VECTOR_SPARSE_FLOAT;

    auto enable_binlog_index = [&]() {
        // checkout config
        if (!segcore_config_.get_enable_interim_segment_index()) {
            return false;
        }
        // check data type
        if (field_meta.get_data_type() != DataType::VECTOR_FLOAT &&
            !is_sparse) {
            return false;
        }
        // check index type
        if (index_params.find(knowhere::meta::INDEX_TYPE) ==
                index_params.end() ||
            field_index_meta.IsFlatIndex()) {
            return false;
        }
        // check index exist
        if (vector_indexings_.is_ready(field_id)) {
            return false;
        }
        return true;
    };
    if (!enable_binlog_index()) {
        return false;
    }
    try {
        // get binlog data and meta
        int64_t row_count;
        {
            std::shared_lock lck(mutex_);
            row_count = num_rows_.value();
        }

        // generate index params
        auto field_binlog_config = std::unique_ptr<VecIndexConfig>(
            new VecIndexConfig(row_count,
                               field_index_meta,
                               segcore_config_,
                               SegmentType::Sealed,
                               is_sparse));
        if (row_count < field_binlog_config->GetBuildThreshold()) {
            return false;
        }
        std::shared_ptr<ColumnBase> vec_data{};
        {
            std::shared_lock lck(mutex_);
            vec_data = fields_.at(field_id);
        }
        auto dim = is_sparse
                       ? dynamic_cast<SparseFloatColumn*>(vec_data.get())->Dim()
                       : field_meta.get_dim();

        auto build_config = field_binlog_config->GetBuildBaseParams();
        build_config[knowhere::meta::DIM] = std::to_string(dim);
        build_config[knowhere::meta::NUM_BUILD_THREAD] = std::to_string(1);
        auto index_metric = field_binlog_config->GetMetricType();

        auto dataset =
            knowhere::GenDataSet(row_count, dim, (void*)vec_data->Data());
        dataset->SetIsOwner(false);
        dataset->SetIsSparse(is_sparse);

        index::IndexBasePtr vec_index =
            std::make_unique<index::VectorMemIndex<float>>(
                field_binlog_config->GetIndexType(),
                index_metric,
                knowhere::Version::GetCurrentVersion().VersionNumber());
        vec_index->BuildWithDataset(dataset, build_config);
        if (enable_binlog_index()) {
            std::unique_lock lck(mutex_);
            vector_indexings_.append_field_indexing(
                field_id, index_metric, std::move(vec_index));

            vec_binlog_config_[field_id] = std::move(field_binlog_config);
            set_bit(binlog_index_bitset_, field_id, true);
            LOG_INFO(
                "replace binlog with binlog index in segment {}, field {}.",
                this->get_segment_id(),
                field_id.get());
        }
        return true;
    } catch (std::exception& e) {
        LOG_WARN("fail to generate binlog index, because {}", e.what());
        return false;
    }
}
void
SegmentSealedImpl::RemoveFieldFile(const FieldId field_id) {
    auto cc = storage::MmapManager::GetInstance().GetChunkCache();
    if (cc == nullptr) {
        return;
    }
    for (const auto& iter : field_data_info_.field_infos) {
        if (iter.second.field_id == field_id.get()) {
            for (const auto& binlog : iter.second.insert_files) {
                cc->Remove(binlog);
            }
            return;
        }
    }
}

}  // namespace milvus::segcore
