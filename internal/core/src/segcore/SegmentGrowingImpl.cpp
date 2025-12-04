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
#include <cstring>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <boost/iterator/counting_iterator.hpp>
#include <type_traits>
#include <unordered_map>
#include <variant>

#include "cachinglayer/CacheSlot.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/Schema.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/Common.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "query/PlanNode.h"
#include "query/SearchOnSealed.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"
#include "storage/Util.h"
#include "storage/ThreadPools.h"
#include "storage/KeyRetriever.h"
#include "common/TypeTraits.h"

#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/common/constants.h"

namespace milvus::segcore {

using namespace milvus::cachinglayer;

int64_t
SegmentGrowingImpl::PreInsert(int64_t size) {
    auto reserved_begin = insert_record_.reserved.fetch_add(size);
    return reserved_begin;
}

void
SegmentGrowingImpl::mask_with_delete(BitsetTypeView& bitset,
                                     int64_t ins_barrier,
                                     Timestamp timestamp) const {
    deleted_record_.Query(bitset, ins_barrier, timestamp);
}

void
SegmentGrowingImpl::try_remove_chunks(FieldId fieldId) {
    //remove the chunk data to reduce memory consumption
    auto& field_meta = schema_->operator[](fieldId);
    auto data_type = field_meta.get_data_type();
    if (IsVectorDataType(data_type)) {
        if (indexing_record_.HasRawData(fieldId)) {
            auto vec_data_base = insert_record_.get_data_base(fieldId);
            if (vec_data_base && vec_data_base->num_chunk() > 0 &&
                chunk_mutex_.try_lock()) {
                vec_data_base->clear();
                chunk_mutex_.unlock();
            }
        }
    }
}

void
SegmentGrowingImpl::Insert(int64_t reserved_offset,
                           int64_t num_rows,
                           const int64_t* row_ids,
                           const Timestamp* timestamps_raw,
                           InsertRecordProto* insert_record_proto) {
    AssertInfo(insert_record_proto->num_rows() == num_rows,
               "Entities_raw count not equal to insert size");
    // protect schema being changed during insert
    // schema change cannot happends during insertion,
    // otherwise, there might be some data not following new schema
    std::shared_lock lck(sch_mutex_);

    // step 1: check insert data if valid
    std::unordered_map<FieldId, int64_t> field_id_to_offset;
    int64_t field_offset = 0;
    int64_t exist_rows = stats_.mem_size / (sizeof(Timestamp) + sizeof(idx_t));

    for (const auto& field : insert_record_proto->fields_data()) {
        auto field_id = FieldId(field.field_id());
        AssertInfo(!field_id_to_offset.count(field_id), "duplicate field data");
        field_id_to_offset.emplace(field_id, field_offset++);
        // may be added field, add the null if has existed data
        if (exist_rows > 0 && !insert_record_.is_data_exist(field_id)) {
            LOG_WARN(
                "heterogeneous insert data found for segment {}, field id {}, "
                "data type {}",
                id_,
                field_id.get(),
                field.type());
            schema_->AddField(FieldName(field.field_name()),
                              field_id,
                              DataType(field.type()),
                              true,
                              std::nullopt);
            auto field_meta = schema_->get_fields().at(field_id);
            insert_record_.append_field_meta(
                field_id, field_meta, size_per_chunk(), mmap_descriptor_);
            auto data = bulk_subscript_not_exist_field(field_meta, exist_rows);
            insert_record_.get_data_base(field_id)->set_data_raw(
                0, exist_rows, data.get(), field_meta);
        }
    }

    // segment have latest schema while insert used old one
    // need to fill insert data with field_meta
    for (auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        if (field_id_to_offset.count(field_id) > 0) {
            continue;
        }
        LOG_INFO(
            "schema newer than insert data found for segment {}, attach empty "
            "field data"
            "not exist field {}, data type {}",
            id_,
            field_id.get(),
            field_meta.get_data_type());
        auto data = bulk_subscript_not_exist_field(field_meta, num_rows);
        insert_record_proto->add_fields_data()->CopyFrom(*data);
        field_id_to_offset.emplace(field_id, field_offset++);
    }

    // step 2: sort timestamp
    // query node already guarantees that the timestamp is ordered, avoid field data copy in c++

    // step 3: fill into Segment.ConcurrentVector
    insert_record_.timestamps_.set_data_raw(
        reserved_offset, timestamps_raw, num_rows);

    // update the mem size of timestamps and row IDs
    stats_.mem_size += num_rows * (sizeof(Timestamp) + sizeof(idx_t));
    for (auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        AssertInfo(field_id_to_offset.count(field_id),
                   fmt::format("can't find field {}", field_id.get()));
        auto data_offset = field_id_to_offset[field_id];
        if (!indexing_record_.HasRawData(field_id)) {
            if (field_meta.is_nullable()) {
                insert_record_.get_valid_data(field_id)->set_data_raw(
                    num_rows,
                    &insert_record_proto->fields_data(data_offset),
                    field_meta);
            }
            insert_record_.get_data_base(field_id)->set_data_raw(
                reserved_offset,
                num_rows,
                &insert_record_proto->fields_data(data_offset),
                field_meta);
        }
        //insert vector data into index
        if (segcore_config_.get_enable_interim_segment_index()) {
            indexing_record_.AppendingIndex(
                reserved_offset,
                num_rows,
                field_id,
                &insert_record_proto->fields_data(data_offset),
                insert_record_);
        }

        // index text.
        if (field_meta.enable_match()) {
            // TODO: iterate texts and call `AddText` instead of `AddTexts`. This may cost much more memory.
            std::vector<std::string> texts(
                insert_record_proto->fields_data(data_offset)
                    .scalars()
                    .string_data()
                    .data()
                    .begin(),
                insert_record_proto->fields_data(data_offset)
                    .scalars()
                    .string_data()
                    .data()
                    .end());
            FixedVector<bool> texts_valid_data(
                insert_record_proto->fields_data(data_offset)
                    .valid_data()
                    .begin(),
                insert_record_proto->fields_data(data_offset)
                    .valid_data()
                    .end());
            AddTexts(field_id,
                     texts.data(),
                     texts_valid_data.data(),
                     num_rows,
                     reserved_offset);
        }

        // update average row data size
        auto field_data_size = GetRawDataSizeOfDataArray(
            &insert_record_proto->fields_data(data_offset),
            field_meta,
            num_rows);
        if (IsVariableDataType(field_meta.get_data_type())) {
            SegmentInternalInterface::set_field_avg_size(
                field_id, num_rows, field_data_size);
        }

        // Build geometry cache for GEOMETRY fields
        if (field_meta.get_data_type() == DataType::GEOMETRY &&
            segcore_config_.get_enable_geometry_cache()) {
            BuildGeometryCacheForInsert(
                field_id,
                &insert_record_proto->fields_data(data_offset),
                num_rows);
        }

        stats_.mem_size += field_data_size;

        try_remove_chunks(field_id);
    }

    // step 4: set pks to offset
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    std::vector<PkType> pks(num_rows);
    ParsePksFromFieldData(
        pks, insert_record_proto->fields_data(field_id_to_offset[field_id]));
    for (int i = 0; i < num_rows; ++i) {
        insert_record_.insert_pk(pks[i], reserved_offset + i);
    }

    // step 5: update small indexes
    insert_record_.ack_responder_.AddSegment(reserved_offset,
                                             reserved_offset + num_rows);
}

void
SegmentGrowingImpl::LoadFieldData(const LoadFieldDataInfo& infos) {
    switch (infos.storage_version) {
        case 2:
            load_column_group_data_internal(infos);
            break;
        default:
            load_field_data_internal(infos);
            break;
    }
}

void
SegmentGrowingImpl::load_field_data_internal(const LoadFieldDataInfo& infos) {
    AssertInfo(infos.field_infos.find(TimestampFieldID.get()) !=
                   infos.field_infos.end(),
               "timestamps field data should be included");
    AssertInfo(
        infos.field_infos.find(RowFieldID.get()) != infos.field_infos.end(),
        "rowID field data should be included");
    auto primary_field_id =
        schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(primary_field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    AssertInfo(infos.field_infos.find(primary_field_id.get()) !=
                   infos.field_infos.end(),
               "primary field data should be included");

    size_t num_rows = storage::GetNumRowsForLoadInfo(infos);
    auto reserved_offset = PreInsert(num_rows);
    for (auto& [id, info] : infos.field_infos) {
        auto field_id = FieldId(id);
        auto insert_files = info.insert_files;
        storage::SortByPath(insert_files);

        auto channel = std::make_shared<FieldDataChannel>();
        auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);

        int total = 0;
        for (int num : info.entries_nums) {
            total += num;
        }
        if (total != info.row_count) {
            AssertInfo(total <= info.row_count,
                       "binlog number should less than or equal row_count");
            auto field_meta = get_schema()[field_id];
            AssertInfo(field_meta.is_nullable(),
                       "nullable must be true when lack rows");
            auto lack_num = info.row_count - total;
            auto field_data =
                storage::CreateFieldData(field_meta.get_data_type(),
                                         field_meta.get_element_type(),
                                         true,
                                         1,
                                         lack_num);
            field_data->FillFieldData(field_meta.default_value(), lack_num);
            channel->push(field_data);
        }

        LOG_INFO("segment {} loads field {} with num_rows {}",
                 this->get_segment_id(),
                 field_id.get(),
                 num_rows);
        auto load_future = pool.Submit(LoadFieldDatasFromRemote,
                                       insert_files,
                                       channel,
                                       infos.load_priority);

        LOG_INFO("segment {} submits load field {} task to thread pool",
                 this->get_segment_id(),
                 field_id.get());
        auto field_data = storage::CollectFieldDataChannel(channel);
        load_field_data_common(
            field_id, reserved_offset, field_data, primary_field_id, num_rows);
    }

    // step 5: update small indexes
    insert_record_.ack_responder_.AddSegment(reserved_offset,
                                             reserved_offset + num_rows);
}

void
SegmentGrowingImpl::load_field_data_common(
    FieldId field_id,
    size_t reserved_offset,
    const std::vector<FieldDataPtr>& field_data,
    FieldId primary_field_id,
    size_t num_rows) {
    if (field_id == TimestampFieldID) {
        // step 2: sort timestamp
        // query node already guarantees that the timestamp is ordered, avoid field data copy in c++

        // step 3: fill into Segment.ConcurrentVector
        insert_record_.timestamps_.set_data_raw(reserved_offset, field_data);
        return;
    }

    if (field_id == RowFieldID) {
        return;
    }

    if (!indexing_record_.HasRawData(field_id)) {
        if (insert_record_.is_valid_data_exist(field_id)) {
            insert_record_.get_valid_data(field_id)->set_data_raw(field_data);
        }
        insert_record_.get_data_base(field_id)->set_data_raw(reserved_offset,
                                                             field_data);
    }
    if (segcore_config_.get_enable_interim_segment_index()) {
        auto offset = reserved_offset;
        for (auto& data : field_data) {
            auto row_count = data->get_num_rows();
            indexing_record_.AppendingIndex(
                offset, row_count, field_id, data, insert_record_);
            offset += row_count;
        }
    }
    try_remove_chunks(field_id);

    if (field_id == primary_field_id) {
        insert_record_.insert_pks(field_data);
    }

    // update average row data size
    auto field_meta = (*schema_)[field_id];
    if (IsVariableDataType(field_meta.get_data_type())) {
        SegmentInternalInterface::set_field_avg_size(
            field_id, num_rows, storage::GetByteSizeOfFieldDatas(field_data));
    }

    // build text match index
    if (field_meta.enable_match()) {
        auto pinned = GetTextIndex(nullptr, field_id);
        auto index = pinned.get();
        index->BuildIndexFromFieldData(field_data, field_meta.is_nullable());
        index->Commit();
        // Reload reader so that the index can be read immediately
        index->Reload();
    }

    // update the mem size
    stats_.mem_size += storage::GetByteSizeOfFieldDatas(field_data);

    LOG_INFO("segment {} loads field {} done",
             this->get_segment_id(),
             field_id.get());
}

void
SegmentGrowingImpl::load_column_group_data_internal(
    const LoadFieldDataInfo& infos) {
    auto primary_field_id =
        schema_->get_primary_field_id().value_or(FieldId(-1));

    size_t num_rows = storage::GetNumRowsForLoadInfo(infos);
    auto reserved_offset = PreInsert(num_rows);
    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    ArrowSchemaPtr arrow_schema = schema_->ConvertToArrowSchema();

    for (auto& [id, info] : infos.field_infos) {
        auto column_group_id = FieldId(id);
        auto insert_files = info.insert_files;
        storage::SortByPath(insert_files);
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();
        auto column_group_info =
            FieldDataInfo(column_group_id.get(), num_rows, "");
        column_group_info.arrow_reader_channel->set_capacity(parallel_degree);

        LOG_INFO(
            "[StorageV2] segment {} loads column group {} with num_rows {}",
            this->get_segment_id(),
            column_group_id.get(),
            num_rows);

        auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);

        // Get all row groups for each file
        std::vector<std::vector<int64_t>> row_group_lists;
        row_group_lists.reserve(insert_files.size());
        for (const auto& file : insert_files) {
            auto result = milvus_storage::FileRowGroupReader::Make(
                fs,
                file,
                milvus_storage::DEFAULT_READ_BUFFER_SIZE,
                storage::GetReaderProperties());
            AssertInfo(result.ok(),
                       "[StorageV2] Failed to create file row group reader: " +
                           result.status().ToString());
            auto reader = result.ValueOrDie();
            auto row_group_num =
                reader->file_metadata()->GetRowGroupMetadataVector().size();
            std::vector<int64_t> all_row_groups(row_group_num);
            std::iota(all_row_groups.begin(), all_row_groups.end(), 0);
            row_group_lists.push_back(all_row_groups);
            auto status = reader->Close();
            AssertInfo(
                status.ok(),
                "[StorageV2] failed to close file reader when get row group "
                "metadata from file {} with error {}",
                file,
                status.ToString());
        }

        // create parallel degree split strategy
        auto strategy =
            std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);

        auto load_future = pool.Submit([&]() {
            return LoadWithStrategy(insert_files,
                                    column_group_info.arrow_reader_channel,
                                    DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                    std::move(strategy),
                                    row_group_lists,
                                    fs,
                                    nullptr,
                                    infos.load_priority);
        });

        LOG_INFO(
            "[StorageV2] segment {} submits load column group {} task to "
            "thread pool",
            this->get_segment_id(),
            column_group_id.get());

        std::shared_ptr<milvus::ArrowDataWrapper> r;

        std::unordered_map<FieldId, std::vector<FieldDataPtr>> field_data_map;
        while (column_group_info.arrow_reader_channel->pop(r)) {
            for (const auto& table_info : r->arrow_tables) {
                size_t batch_num_rows = table_info.table->num_rows();
                for (int i = 0; i < table_info.table->schema()->num_fields();
                     ++i) {
                    AssertInfo(
                        table_info.table->schema()
                            ->field(i)
                            ->metadata()
                            ->Contains(milvus_storage::ARROW_FIELD_ID_KEY),
                        "[StorageV2] field id not found in metadata for field "
                        "{}",
                        table_info.table->schema()->field(i)->name());
                    auto field_id =
                        std::stoll(table_info.table->schema()
                                       ->field(i)
                                       ->metadata()
                                       ->Get(milvus_storage::ARROW_FIELD_ID_KEY)
                                       ->data());
                    for (auto& field : schema_->get_fields()) {
                        if (field.second.get_id().get() != field_id) {
                            continue;
                        }
                        auto data_type = field.second.get_data_type();
                        auto field_data = storage::CreateFieldData(
                            data_type,
                            field.second.get_element_type(),
                            field.second.is_nullable(),
                            IsVectorDataType(data_type) &&
                                    !IsSparseFloatVectorDataType(data_type)
                                ? field.second.get_dim()
                                : 1,
                            batch_num_rows);
                        field_data->FillFieldData(table_info.table->column(i));
                        field_data_map[FieldId(field_id)].push_back(field_data);
                    }
                }
            }
        }

        for (auto& [field_id, field_data] : field_data_map) {
            load_field_data_common(field_id,
                                   reserved_offset,
                                   field_data,
                                   primary_field_id,
                                   num_rows);
            // Build geometry cache for GEOMETRY fields
            if (schema_->operator[](field_id).get_data_type() ==
                    DataType::GEOMETRY &&
                segcore_config_.get_enable_geometry_cache()) {
                BuildGeometryCacheForLoad(field_id, field_data);
            }
        }
    }

    // step 5: update small indexes
    insert_record_.ack_responder_.AddSegment(reserved_offset,
                                             reserved_offset + num_rows);
}

SegcoreError
SegmentGrowingImpl::Delete(int64_t size,
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
    auto end =
        std::remove_if(ordering.begin(),
                       ordering.end(),
                       [&](const std::tuple<Timestamp, PkType>& record) {
                           return !insert_record_.contain(std::get<1>(record));
                       });
    size = end - ordering.begin();
    ordering.resize(size);
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

    // step 2: fill delete record
    deleted_record_.StreamPush(sort_pks, sort_timestamps.data());
    return SegcoreError::success();
}

void
SegmentGrowingImpl::LoadDeletedRecord(const LoadDeletedRecordInfo& info) {
    AssertInfo(info.row_count > 0, "The row count of deleted record is 0");
    AssertInfo(info.primary_keys, "Deleted primary keys is null");
    AssertInfo(info.timestamps, "Deleted timestamps is null");
    // step 1: get pks and timestamps
    auto field_id =
        schema_->get_primary_field_id().value_or(FieldId(INVALID_FIELD_ID));
    AssertInfo(field_id.get() != INVALID_FIELD_ID,
               "Primary key has invalid field id");
    auto& field_meta = schema_->operator[](field_id);
    int64_t size = info.row_count;
    std::vector<PkType> pks(size);
    ParsePksFromIDs(pks, field_meta.get_data_type(), *info.primary_keys);
    auto timestamps = reinterpret_cast<const Timestamp*>(info.timestamps);

    // step 2: push delete info to delete_record
    deleted_record_.LoadPush(pks, timestamps);
}

PinWrapper<SpanBase>
SegmentGrowingImpl::chunk_data_impl(milvus::OpContext* op_ctx,
                                    FieldId field_id,
                                    int64_t chunk_id) const {
    return PinWrapper<SpanBase>(
        get_insert_record().get_span_base(field_id, chunk_id));
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_string_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "chunk string view impl not implement for growing segment");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_array_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "chunk array view impl not implement for growing segment");
}

PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_vector_array_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "chunk vector array view impl not implement for growing segment");
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_string_views_by_offsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "chunk view by offsets not implemented for growing segment");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_array_views_by_offsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    ThrowInfo(
        ErrorCode::NotImplemented,
        "chunk array views by offsets not implemented for growing segment");
}

int64_t
SegmentGrowingImpl::num_chunk(FieldId field_id) const {
    auto size = get_insert_record().ack_responder_.GetAck();
    return upper_div(size, segcore_config_.get_chunk_rows());
}

DataType
SegmentGrowingImpl::GetFieldDataType(milvus::FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    return field_meta.get_data_type();
}

void
SegmentGrowingImpl::search_batch_pks(
    const std::vector<PkType>& pks,
    const Timestamp* timestamps,
    bool include_same_ts,
    const std::function<void(const SegOffset offset, const Timestamp ts)>&
        callback) const {
    for (size_t i = 0; i < pks.size(); ++i) {
        auto timestamp = timestamps[i];
        auto offsets =
            insert_record_.search_pk(pks[i], timestamp, include_same_ts);
        for (auto offset : offsets) {
            callback(offset, timestamp);
        }
    }
}

void
SegmentGrowingImpl::vector_search(SearchInfo& search_info,
                                  const void* query_data,
                                  const size_t* query_offsets,
                                  int64_t query_count,
                                  Timestamp timestamp,
                                  const BitsetView& bitset,
                                  milvus::OpContext* op_context,
                                  SearchResult& output) const {
    query::SearchOnGrowing(*this,
                           search_info,
                           query_data,
                           query_offsets,
                           query_count,
                           timestamp,
                           bitset,
                           op_context,
                           output);
}

std::unique_ptr<DataArray>
SegmentGrowingImpl::bulk_subscript(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const int64_t* seg_offsets,
    int64_t count,
    const std::vector<std::string>& dynamic_field_names) const {
    Assert(!dynamic_field_names.empty());
    auto& field_meta = schema_->operator[](field_id);
    auto vec_ptr = insert_record_.get_data_base(field_id);
    auto result = CreateEmptyScalarDataArray(count, field_meta);
    if (field_meta.is_nullable()) {
        auto valid_data_ptr = insert_record_.get_valid_data(field_id);
        auto res = result->mutable_valid_data()->mutable_data();
        for (int64_t i = 0; i < count; ++i) {
            auto offset = seg_offsets[i];
            res[i] = valid_data_ptr->is_valid(offset);
        }
    }
    auto vec = dynamic_cast<const ConcurrentVector<Json>*>(vec_ptr);
    auto dst = result->mutable_scalars()->mutable_json_data()->mutable_data();
    auto& src = *vec;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst->at(i) =
            ExtractSubJson(std::string(src[offset]), dynamic_field_names);
    }
    return result;
}

std::unique_ptr<DataArray>
SegmentGrowingImpl::bulk_subscript(milvus::OpContext* op_ctx,
                                   FieldId field_id,
                                   const int64_t* seg_offsets,
                                   int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    auto vec_ptr = insert_record_.get_data_base(field_id);
    if (field_meta.is_vector()) {
        auto result = CreateEmptyVectorDataArray(count, field_meta);
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            bulk_subscript_impl<FloatVector>(op_ctx,
                                             field_id,
                                             field_meta.get_sizeof(),
                                             vec_ptr,
                                             seg_offsets,
                                             count,
                                             result->mutable_vectors()
                                                 ->mutable_float_vector()
                                                 ->mutable_data()
                                                 ->mutable_data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
            bulk_subscript_impl<BinaryVector>(
                op_ctx,
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_binary_vector()->data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            bulk_subscript_impl<Float16Vector>(
                op_ctx,
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_float16_vector()->data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_BFLOAT16) {
            bulk_subscript_impl<BFloat16Vector>(
                op_ctx,
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_bfloat16_vector()->data());
        } else if (field_meta.get_data_type() ==
                   DataType::VECTOR_SPARSE_U32_F32) {
            bulk_subscript_sparse_float_vector_impl(
                op_ctx,
                field_id,
                (const ConcurrentVector<SparseFloatVector>*)vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_sparse_float_vector());
            result->mutable_vectors()->set_dim(
                result->vectors().sparse_float_vector().dim());
        } else if (field_meta.get_data_type() == DataType::VECTOR_INT8) {
            bulk_subscript_impl<Int8Vector>(
                op_ctx,
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_int8_vector()->data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_ARRAY) {
            bulk_subscript_vector_array_impl(op_ctx,
                                             *vec_ptr,
                                             seg_offsets,
                                             count,
                                             result->mutable_vectors()
                                                 ->mutable_vector_array()
                                                 ->mutable_data());
        } else {
            ThrowInfo(DataTypeInvalid, "logical error");
        }
        return result;
    }

    AssertInfo(!field_meta.is_vector(),
               "Scalar field meta type is vector type");

    auto result = CreateEmptyScalarDataArray(count, field_meta);
    if (field_meta.is_nullable()) {
        auto valid_data_ptr = insert_record_.get_valid_data(field_id);
        auto res = result->mutable_valid_data()->mutable_data();
        for (int64_t i = 0; i < count; ++i) {
            auto offset = seg_offsets[i];
            res[i] = valid_data_ptr->is_valid(offset);
        }
    }
    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            bulk_subscript_impl<bool>(op_ctx,
                                      vec_ptr,
                                      seg_offsets,
                                      count,
                                      result->mutable_scalars()
                                          ->mutable_bool_data()
                                          ->mutable_data()
                                          ->mutable_data());
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t>(op_ctx,
                                        vec_ptr,
                                        seg_offsets,
                                        count,
                                        result->mutable_scalars()
                                            ->mutable_int_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_long_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(op_ctx,
                                       vec_ptr,
                                       seg_offsets,
                                       count,
                                       result->mutable_scalars()
                                           ->mutable_float_data()
                                           ->mutable_data()
                                           ->mutable_data());
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(op_ctx,
                                        vec_ptr,
                                        seg_offsets,
                                        count,
                                        result->mutable_scalars()
                                            ->mutable_double_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::TIMESTAMPTZ: {
            bulk_subscript_impl<int64_t>(op_ctx,
                                         vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_timestamptz_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::VARCHAR:
        case DataType::TEXT: {
            bulk_subscript_ptr_impl<std::string>(op_ctx,
                                                 vec_ptr,
                                                 seg_offsets,
                                                 count,
                                                 result->mutable_scalars()
                                                     ->mutable_string_data()
                                                     ->mutable_data());
            break;
        }
        case DataType::JSON: {
            bulk_subscript_ptr_impl<Json>(
                op_ctx,
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_scalars()->mutable_json_data()->mutable_data());
            break;
        }
        case DataType::GEOMETRY: {
            bulk_subscript_ptr_impl<std::string>(op_ctx,
                                                 vec_ptr,
                                                 seg_offsets,
                                                 count,
                                                 result->mutable_scalars()
                                                     ->mutable_geometry_data()
                                                     ->mutable_data());
            break;
        }
        case DataType::ARRAY: {
            // element
            bulk_subscript_array_impl(op_ctx,
                                      *vec_ptr,
                                      seg_offsets,
                                      count,
                                      result->mutable_scalars()
                                          ->mutable_array_data()
                                          ->mutable_data());
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("unsupported type {}", field_meta.get_data_type()));
        }
    }
    return result;
}

void
SegmentGrowingImpl::bulk_subscript_sparse_float_vector_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const ConcurrentVector<SparseFloatVector>* vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    milvus::proto::schema::SparseFloatArray* output) const {
    AssertInfo(HasRawData(field_id.get()), "Growing segment loss raw data");

    // if index has finished building, grab from index without any
    // synchronization operations.
    if (indexing_record_.SyncDataWithIndex(field_id)) {
        indexing_record_.GetDataFromIndex(
            field_id, seg_offsets, count, 0, output);
        return;
    }
    {
        std::lock_guard<std::shared_mutex> guard(chunk_mutex_);
        // check again after lock to make sure: if index has finished building
        // after the above check but before we grabbed the lock, we should grab
        // from index as the data in chunk may have been removed in
        // try_remove_chunks.
        if (!indexing_record_.SyncDataWithIndex(field_id)) {
            // copy from raw data
            SparseRowsToProto(
                [&](size_t i) {
                    auto offset = seg_offsets[i];
                    return offset != INVALID_SEG_OFFSET
                               ? vec_raw->get_element(offset)
                               : nullptr;
                },
                count,
                output);
            return;
        }
        // else: release lock and copy from index
    }
    indexing_record_.GetDataFromIndex(field_id, seg_offsets, count, 0, output);
}

template <typename S>
void
SegmentGrowingImpl::bulk_subscript_ptr_impl(
    milvus::OpContext* op_ctx,
    const VectorBase* vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<std::string>* dst) const {
    auto vec = dynamic_cast<const ConcurrentVector<S>*>(vec_raw);
    auto& src = *vec;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (IsVariableTypeSupportInChunk<S> && src.is_mmap()) {
            dst->at(i) = std::move(std::string(src.view_element(offset)));
        } else {
            dst->at(i) = std::move(std::string(src[offset]));
        }
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_impl(milvus::OpContext* op_ctx,
                                        FieldId field_id,
                                        int64_t element_sizeof,
                                        const VectorBase* vec_raw,
                                        const int64_t* seg_offsets,
                                        int64_t count,
                                        void* output_raw) const {
    static_assert(IsVector<T>);
    auto vec_ptr = dynamic_cast<const ConcurrentVector<T>*>(vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;

    // HasRawData interface guarantees that data can be fetched from growing segment
    AssertInfo(HasRawData(field_id.get()), "Growing segment loss raw data");

    // if index has finished building, grab from index without any
    // synchronization operations.
    if (indexing_record_.HasRawData(field_id)) {
        indexing_record_.GetDataFromIndex(
            field_id, seg_offsets, count, element_sizeof, output_raw);
        return;
    }
    {
        std::lock_guard<std::shared_mutex> guard(chunk_mutex_);
        // check again after lock to make sure: if index has finished building
        // after the above check but before we grabbed the lock, we should grab
        // from index as the data in chunk may have been removed in
        // try_remove_chunks.
        if (!indexing_record_.HasRawData(field_id)) {
            auto output_base = reinterpret_cast<char*>(output_raw);
            for (int i = 0; i < count; ++i) {
                auto dst = output_base + i * element_sizeof;
                auto offset = seg_offsets[i];
                if (offset == INVALID_SEG_OFFSET) {
                    memset(dst, 0, element_sizeof);
                } else {
                    auto src = (const uint8_t*)vec.get_element(offset);
                    memcpy(dst, src, element_sizeof);
                }
            }
            return;
        }
        // else: release lock and copy from index
    }
    indexing_record_.GetDataFromIndex(
        field_id, seg_offsets, count, element_sizeof, output_raw);
}

template <typename S, typename T>
void
SegmentGrowingImpl::bulk_subscript_impl(milvus::OpContext* op_ctx,
                                        const VectorBase* vec_raw,
                                        const int64_t* seg_offsets,
                                        int64_t count,
                                        T* output) const {
    static_assert(IsScalar<S>);
    auto vec_ptr = dynamic_cast<const ConcurrentVector<S>*>(vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        output[i] = vec[offset];
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_array_impl(
    milvus::OpContext* op_ctx,
    const VectorBase& vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) const {
    auto vec_ptr = dynamic_cast<const ConcurrentVector<Array>*>(&vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset != INVALID_SEG_OFFSET) {
            dst->at(i) = vec[offset].output_data();
        }
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_vector_array_impl(
    milvus::OpContext* op_ctx,
    const VectorBase& vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) const {
    auto vec_ptr = dynamic_cast<const ConcurrentVector<VectorArray>*>(&vec_raw);
    AssertInfo(vec_ptr, "Pointer of vec_raw is nullptr");
    auto& vec = *vec_ptr;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (offset != INVALID_SEG_OFFSET) {
            dst->at(i) = vec[offset].output_data();
        }
    }
}

void
SegmentGrowingImpl::bulk_subscript(milvus::OpContext* op_ctx,
                                   SystemFieldType system_type,
                                   const int64_t* seg_offsets,
                                   int64_t count,
                                   void* output) const {
    switch (system_type) {
        case SystemFieldType::Timestamp:
            bulk_subscript_impl<Timestamp>(op_ctx,
                                           &this->insert_record_.timestamps_,
                                           seg_offsets,
                                           count,
                                           static_cast<Timestamp*>(output));
            break;
        case SystemFieldType::RowId:
            ThrowInfo(ErrorCode::Unsupported,
                      "RowId retrieve is not supported");
            break;
        default:
            ThrowInfo(DataTypeInvalid, "unknown subscript fields");
    }
}

void
SegmentGrowingImpl::search_ids(BitsetType& bitset,
                               const IdArray& id_array) const {
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_->operator[](field_id);
    auto data_type = field_meta.get_data_type();
    auto ids_size = GetSizeOfIdArray(id_array);
    std::vector<PkType> pks(ids_size);
    ParsePksFromIDs(pks, data_type, id_array);

    BitsetTypeView bitset_view(bitset);
    for (auto& pk : pks) {
        insert_record_.search_pk_range(
            pk, proto::plan::OpType::Equal, bitset_view);
    }
}

int64_t
SegmentGrowingImpl::get_active_count(Timestamp ts) const {
    auto row_count = this->get_row_count();
    auto& ts_vec = this->get_insert_record().timestamps_;
    auto iter = std::upper_bound(
        boost::make_counting_iterator(static_cast<int64_t>(0)),
        boost::make_counting_iterator(row_count),
        ts,
        [&](Timestamp ts, int64_t index) { return ts < ts_vec[index]; });
    return *iter;
}

void
SegmentGrowingImpl::mask_with_timestamps(BitsetTypeView& bitset_chunk,
                                         Timestamp timestamp,
                                         Timestamp collection_ttl) const {
    if (collection_ttl > 0) {
        auto& timestamps = get_timestamps();
        auto size = bitset_chunk.size();
        if (timestamps[size - 1] <= collection_ttl) {
            bitset_chunk.set();
            return;
        }
        auto pilot = upper_bound(timestamps, 0, size, collection_ttl);
        BitsetType bitset;
        bitset.reserve(size);
        bitset.resize(pilot, true);
        bitset.resize(size, false);
        bitset_chunk |= bitset;
    }
}

void
SegmentGrowingImpl::CreateTextIndex(FieldId field_id) {
    std::unique_lock lock(mutex_);
    const auto& field_meta = schema_->operator[](field_id);
    AssertInfo(IsStringDataType(field_meta.get_data_type()),
               "cannot create text index on non-string type");
    std::string unique_id = GetUniqueFieldId(field_meta.get_id().get());
    // todo: make this(200) configurable.
    auto index = std::make_unique<index::TextMatchIndex>(
        200,
        unique_id.c_str(),
        "milvus_tokenizer",
        field_meta.get_analyzer_params().c_str());
    index->Commit();
    index->CreateReader(milvus::index::SetBitsetGrowing);
    index->RegisterTokenizer("milvus_tokenizer",
                             field_meta.get_analyzer_params().c_str());
    text_indexes_[field_id] = std::move(index);
}

void
SegmentGrowingImpl::CreateTextIndexes() {
    for (auto [field_id, field_meta] : schema_->get_fields()) {
        if (IsStringDataType(field_meta.get_data_type()) &&
            field_meta.enable_match()) {
            CreateTextIndex(FieldId(field_id));
        }
    }
}

void
SegmentGrowingImpl::AddTexts(milvus::FieldId field_id,
                             const std::string* texts,
                             const bool* texts_valid_data,
                             size_t n,
                             int64_t offset_begin) {
    std::unique_lock lock(mutex_);
    auto iter = text_indexes_.find(field_id);
    if (iter == text_indexes_.end()) {
        throw SegcoreError(
            ErrorCode::TextIndexNotFound,
            fmt::format("text index not found for field {}", field_id.get()));
    }
    // only unique_ptr is supported for growing segment
    if (auto p = std::get_if<std::unique_ptr<milvus::index::TextMatchIndex>>(
            &iter->second)) {
        (*p)->AddTextsGrowing(n, texts, texts_valid_data, offset_begin);
    } else {
        throw SegcoreError(ErrorCode::UnexpectedError,
                           fmt::format("text index of growing segment is not a "
                                       "unique_ptr for field {}",
                                       field_id.get()));
    }
}

void
SegmentGrowingImpl::BulkGetJsonData(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    std::function<void(milvus::Json, size_t, bool)> fn,
    const int64_t* offsets,
    int64_t count) const {
    auto vec_ptr = dynamic_cast<const ConcurrentVector<Json>*>(
        insert_record_.get_data_base(field_id));
    auto& src = *vec_ptr;
    auto& field_meta = schema_->operator[](field_id);
    if (field_meta.is_nullable()) {
        auto valid_data_ptr = insert_record_.get_valid_data(field_id);
        for (int64_t i = 0; i < count; ++i) {
            auto offset = offsets[i];
            fn(src[offset], i, valid_data_ptr->is_valid(offset));
        }
    } else {
        for (int64_t i = 0; i < count; ++i) {
            auto offset = offsets[i];
            fn(src[offset], i, true);
        }
    }
}

void
SegmentGrowingImpl::LazyCheckSchema(SchemaPtr sch) {
    if (sch->get_schema_version() > schema_->get_schema_version()) {
        LOG_INFO(
            "lazy check schema segment {} found newer schema version, "
            "current "
            "schema version {}, new schema version {}",
            id_,
            schema_->get_schema_version(),
            sch->get_schema_version());
        Reopen(sch);
    }
}

void
SegmentGrowingImpl::Reopen(SchemaPtr sch) {
    std::unique_lock lck(sch_mutex_);

    // double check condition, avoid multiple assignment
    if (sch->get_schema_version() > schema_->get_schema_version()) {
        auto absent_fields = sch->AbsentFields(*schema_);

        for (const auto& field_meta : *absent_fields) {
            fill_empty_field(field_meta);
        }

        schema_ = sch;
    }
}

void
SegmentGrowingImpl::Load(milvus::tracer::TraceContext& trace_ctx) {
    // Convert load_info_ (SegmentLoadInfo) to LoadFieldDataInfo
    LoadFieldDataInfo field_data_info;

    // Set storage version
    field_data_info.storage_version = load_info_.storageversion();

    // Set load priority
    field_data_info.load_priority = load_info_.priority();

    auto manifest_path = load_info_.manifest_path();
    if (manifest_path != "") {
        LoadColumnsGroups(manifest_path);
        return;
    }

    // Convert binlog_paths to field_infos
    for (const auto& field_binlog : load_info_.binlog_paths()) {
        FieldBinlogInfo binlog_info;
        binlog_info.field_id = field_binlog.fieldid();

        // Process each binlog
        int64_t total_row_count = 0;
        auto binlog_count = field_binlog.binlogs().size();
        binlog_info.entries_nums.reserve(binlog_count);
        binlog_info.insert_files.reserve(binlog_count);
        binlog_info.memory_sizes.reserve(binlog_count);
        for (const auto& binlog : field_binlog.binlogs()) {
            binlog_info.entries_nums.push_back(binlog.entries_num());
            binlog_info.insert_files.push_back(binlog.log_path());
            binlog_info.memory_sizes.push_back(binlog.memory_size());
            total_row_count += binlog.entries_num();
        }
        binlog_info.row_count = total_row_count;

        // Set child field ids
        binlog_info.child_field_ids.reserve(field_binlog.child_fields().size());
        for (const auto& child_field : field_binlog.child_fields()) {
            binlog_info.child_field_ids.push_back(child_field);
        }

        // Add to field_infos map
        field_data_info.field_infos[binlog_info.field_id] =
            std::move(binlog_info);
    }

    // Call LoadFieldData with the converted info
    if (!field_data_info.field_infos.empty()) {
        LoadFieldData(field_data_info);
    }
}

void
SegmentGrowingImpl::FinishLoad() {
    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        // append_data is called according to schema before
        // so we must check data empty here
        if (!IsVectorDataType(field_meta.get_data_type()) &&
            insert_record_.get_data_base(field_id)->empty()) {
            fill_empty_field(field_meta);
        }
    }
}

void
SegmentGrowingImpl::LoadColumnsGroups(std::string manifest_path) {
    LOG_INFO(
        "Loading segment {} field data with manifest {}", id_, manifest_path);
    // size_t num_rows = storage::GetNumRowsForLoadInfo(infos);
    auto num_rows = load_info_.num_of_rows();
    auto primary_field_id =
        schema_->get_primary_field_id().value_or(FieldId(-1));
    auto properties = milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                          .GetProperties();
    auto column_groups = GetColumnGroups(manifest_path, properties);

    auto arrow_schema = schema_->ConvertToArrowSchema();
    reader_ = milvus_storage::api::Reader::create(
        column_groups, arrow_schema, nullptr, *properties);

    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    std::vector<
        std::future<std::unordered_map<FieldId, std::vector<FieldDataPtr>>>>
        load_group_futures;
    for (int64_t i = 0; i < column_groups->size(); ++i) {
        auto future = pool.Submit([this, column_groups, properties, i] {
            return LoadColumnGroup(column_groups, properties, i);
        });
        load_group_futures.emplace_back(std::move(future));
    }

    std::vector<std::unordered_map<FieldId, std::vector<FieldDataPtr>>>
        column_group_results;
    std::vector<std::exception_ptr> load_exceptions;
    for (auto& future : load_group_futures) {
        try {
            column_group_results.emplace_back(future.get());
        } catch (...) {
            load_exceptions.push_back(std::current_exception());
        }
    }

    // If any exceptions occurred during index loading, handle them
    if (!load_exceptions.empty()) {
        LOG_ERROR("Failed to load {} out of {} indexes for segment {}",
                  load_exceptions.size(),
                  load_group_futures.size(),
                  id_);

        // Rethrow the first exception
        std::rethrow_exception(load_exceptions[0]);
    }

    auto reserved_offset = PreInsert(num_rows);

    for (auto& column_group_result : column_group_results) {
        for (auto& [field_id, field_data] : column_group_result) {
            load_field_data_common(field_id,
                                   reserved_offset,
                                   field_data,
                                   primary_field_id,
                                   num_rows);
            // Build geometry cache for GEOMETRY fields
            if (schema_->operator[](field_id).get_data_type() ==
                    DataType::GEOMETRY &&
                segcore_config_.get_enable_geometry_cache()) {
                BuildGeometryCacheForLoad(field_id, field_data);
            }
        }
    }

    insert_record_.ack_responder_.AddSegment(reserved_offset,
                                             reserved_offset + num_rows);
}

std::unordered_map<FieldId, std::vector<FieldDataPtr>>
SegmentGrowingImpl::LoadColumnGroup(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    int64_t index) {
    AssertInfo(index < column_groups->size(),
               "load column group index out of range");
    auto column_group = column_groups->get_column_group(index);
    LOG_INFO("Loading segment {} column group {}", id_, index);

    auto chunk_reader_result = reader_->get_chunk_reader(index);
    AssertInfo(chunk_reader_result.ok(),
               "get chunk reader failed, segment {}, column group index {}",
               get_segment_id(),
               index);

    auto chunk_reader = std::move(chunk_reader_result.ValueOrDie());

    auto parallel_degree =
        static_cast<uint64_t>(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);

    std::vector<int64_t> all_row_groups(chunk_reader->total_number_of_chunks());

    std::iota(all_row_groups.begin(), all_row_groups.end(), 0);

    // create parallel degree split strategy
    auto strategy =
        std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);
    auto split_result = strategy->split(all_row_groups);

    auto& thread_pool =
        ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);

    auto part_futures = std::vector<
        std::future<std::vector<std::shared_ptr<arrow::RecordBatch>>>>();
    for (const auto& part : split_result) {
        part_futures.emplace_back(
            thread_pool.Submit([chunk_reader = chunk_reader.get(), part]() {
                std::vector<int64_t> chunk_ids(part.count);
                std::iota(chunk_ids.begin(), chunk_ids.end(), part.offset);

                auto result = chunk_reader->get_chunks(chunk_ids, 1);
                AssertInfo(result.ok(), "get chunks failed");
                return result.ValueOrDie();
            }));
    }

    std::unordered_map<FieldId, std::vector<FieldDataPtr>> field_data_map;
    for (auto& future : part_futures) {
        auto part_result = future.get();
        for (auto& record_batch : part_result) {
            // result->emplace_back(std::move(record_batch));
            auto batch_num_rows = record_batch->num_rows();
            for (auto i = 0; i < column_group->columns.size(); ++i) {
                auto column = column_group->columns[i];
                auto field_id = FieldId(std::stoll(column));

                auto field = schema_->operator[](field_id);
                auto data_type = field.get_data_type();

                auto field_data = storage::CreateFieldData(
                    data_type,
                    field.get_element_type(),
                    field.is_nullable(),
                    IsVectorDataType(data_type) &&
                            !IsSparseFloatVectorDataType(data_type)
                        ? field.get_dim()
                        : 1,
                    batch_num_rows);
                auto array = record_batch->column(i);
                field_data->FillFieldData(array);
                field_data_map[FieldId(field_id)].push_back(field_data);
            }
        }
    }

    LOG_INFO("Finished loading segment {} column group {}", id_, index);
    return field_data_map;
}

void
SegmentGrowingImpl::fill_empty_field(const FieldMeta& field_meta) {
    auto field_id = field_meta.get_id();
    LOG_INFO("start fill empty field {} (data type {}) for growing segment {}",
             field_meta.get_data_type(),
             field_id.get(),
             id_);
    // append meta only needed when schema is old
    // loading old segment with new schema will have meta appended
    if (!insert_record_.is_data_exist(field_id)) {
        insert_record_.append_field_meta(
            field_id, field_meta, size_per_chunk(), mmap_descriptor_);
    }

    auto total_row_num = insert_record_.size();

    auto data = bulk_subscript_not_exist_field(field_meta, total_row_num);
    insert_record_.get_valid_data(field_id)->set_data_raw(
        total_row_num, data.get(), field_meta);
    insert_record_.get_data_base(field_id)->set_data_raw(
        0, total_row_num, data.get(), field_meta);

    LOG_INFO("fill empty field {} (data type {}) for growing segment {} done",
             field_meta.get_data_type(),
             field_id.get(),
             id_);
}

void
SegmentGrowingImpl::BuildGeometryCacheForInsert(FieldId field_id,
                                                const DataArray* data_array,
                                                int64_t num_rows) {
    try {
        // Get geometry cache for this segment+field
        auto& geometry_cache =
            milvus::exec::SimpleGeometryCacheManager::Instance()
                .GetOrCreateCache(get_segment_id(), field_id);

        // Process geometry data from DataArray
        const auto& geometry_data = data_array->scalars().geometry_data();
        const auto& valid_data = data_array->valid_data();

        for (int64_t i = 0; i < num_rows; ++i) {
            if (valid_data.empty() ||
                (i < valid_data.size() && valid_data[i])) {
                // Valid geometry data
                const auto& wkb_data = geometry_data.data(i);
                geometry_cache.AppendData(
                    ctx_, wkb_data.data(), wkb_data.size());
            } else {
                // Null/invalid geometry
                geometry_cache.AppendData(ctx_, nullptr, 0);
            }
        }

        LOG_INFO(
            "Successfully appended {} geometries to cache for growing "
            "segment "
            "{} field {}",
            num_rows,
            get_segment_id(),
            field_id.get());

    } catch (const std::exception& e) {
        ThrowInfo(UnexpectedError,
                  "Failed to build geometry cache for growing segment {} field "
                  "{} insert: {}",
                  get_segment_id(),
                  field_id.get(),
                  e.what());
    }
}

void
SegmentGrowingImpl::BuildGeometryCacheForLoad(
    FieldId field_id, const std::vector<FieldDataPtr>& field_data) {
    try {
        // Get geometry cache for this segment+field
        auto& geometry_cache =
            milvus::exec::SimpleGeometryCacheManager::Instance()
                .GetOrCreateCache(get_segment_id(), field_id);

        // Process each field data chunk
        for (const auto& data : field_data) {
            auto num_rows = data->get_num_rows();

            for (int64_t i = 0; i < num_rows; ++i) {
                if (data->is_valid(i)) {
                    // Valid geometry data
                    auto wkb_data =
                        static_cast<const std::string*>(data->RawValue(i));
                    geometry_cache.AppendData(
                        ctx_, wkb_data->data(), wkb_data->size());
                } else {
                    // Null/invalid geometry
                    geometry_cache.AppendData(ctx_, nullptr, 0);
                }
            }
        }

        size_t total_rows = 0;
        for (const auto& data : field_data) {
            total_rows += data->get_num_rows();
        }

        LOG_INFO(
            "Successfully loaded {} geometries to cache for growing "
            "segment {} "
            "field {}",
            total_rows,
            get_segment_id(),
            field_id.get());

    } catch (const std::exception& e) {
        ThrowInfo(UnexpectedError,
                  "Failed to build geometry cache for growing segment {} field "
                  "{} load: {}",
                  get_segment_id(),
                  field_id.get(),
                  e.what());
    }
}

}  // namespace milvus::segcore
