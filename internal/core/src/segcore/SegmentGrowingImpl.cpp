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
#include "storage/Util.h"
#include "storage/ThreadPools.h"

#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/filesystem/fs.h"

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
    if (indexing_record_.SyncDataWithIndex(fieldId)) {
        VectorBase* vec_data_base =
            dynamic_cast<segcore::ConcurrentVector<FloatVector>*>(
                insert_record_.get_data_base(fieldId));
        if (!vec_data_base) {
            vec_data_base =
                dynamic_cast<segcore::ConcurrentVector<SparseFloatVector>*>(
                    insert_record_.get_data_base(fieldId));
        }
        if (vec_data_base && vec_data_base->num_chunk() > 0 &&
            chunk_mutex_.try_lock()) {
            vec_data_base->clear();
            chunk_mutex_.unlock();
        }
    }
}

void
SegmentGrowingImpl::Insert(int64_t reserved_offset,
                           int64_t num_rows,
                           const int64_t* row_ids,
                           const Timestamp* timestamps_raw,
                           const InsertRecordProto* insert_record_proto) {
    AssertInfo(insert_record_proto->num_rows() == num_rows,
               "Entities_raw count not equal to insert size");
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
            schema_->AddField(FieldName(field.field_name()),
                              field_id,
                              DataType(field.type()),
                              true,
                              std::nullopt);
            auto field_meta = schema_->get_fields().at(field_id);
            insert_record_.append_field_meta(
                field_id, field_meta, size_per_chunk());
            auto data = bulk_subscript_not_exist_field(field_meta, exist_rows);
            insert_record_.get_data_base(field_id)->set_data_raw(
                0, exist_rows, data.get(), field_meta);
        }
    }

    // step 2: sort timestamp
    // query node already guarantees that the timestamp is ordered, avoid field data copy in c++

    // step 3: fill into Segment.ConcurrentVector
    insert_record_.timestamps_.set_data_raw(
        reserved_offset, timestamps_raw, num_rows);

    // update the mem size of timestamps and row IDs
    stats_.mem_size += num_rows * (sizeof(Timestamp) + sizeof(idx_t));
    for (auto [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        AssertInfo(field_id_to_offset.count(field_id),
                   fmt::format("can't find field {}", field_id.get()));
        auto data_offset = field_id_to_offset[field_id];
        if (!indexing_record_.SyncDataWithIndex(field_id)) {
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

        // index json.
        if (field_meta.enable_growing_jsonStats()) {
            std::vector<std::string> jsonDatas(
                insert_record_proto->fields_data(data_offset)
                    .scalars()
                    .json_data()
                    .data()
                    .begin(),
                insert_record_proto->fields_data(data_offset)
                    .scalars()
                    .json_data()
                    .data()
                    .end());
            FixedVector<bool> jsonDatas_valid_data(
                insert_record_proto->fields_data(data_offset)
                    .valid_data()
                    .begin(),
                insert_record_proto->fields_data(data_offset)
                    .valid_data()
                    .end());
            AddJSONDatas(field_id,
                         jsonDatas.data(),
                         jsonDatas_valid_data.data(),
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
            auto field_data = storage::CreateFieldData(
                static_cast<DataType>(field_meta.get_data_type()),
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
        auto load_future =
            pool.Submit(LoadFieldDatasFromRemote, insert_files, channel);

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

    if (!indexing_record_.SyncDataWithIndex(field_id)) {
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
        auto index = GetTextIndex(field_id);
        index->BuildIndexFromFieldData(field_data, field_meta.is_nullable());
        index->Commit();
        // Reload reader so that the index can be read immediately
        index->Reload();
    }

    // build json match index
    if (field_meta.enable_growing_jsonStats()) {
        auto index = GetJsonKeyIndex(field_id);
        index->BuildWithFieldData(field_data, field_meta.is_nullable());
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
        auto file_reader = std::make_shared<milvus_storage::FileRowGroupReader>(
            fs, insert_files[0], arrow_schema);
        std::shared_ptr<milvus_storage::PackedFileMetadata> metadata =
            file_reader->file_metadata();

        auto field_id_mapping = metadata->GetFieldIDMapping();

        milvus_storage::FieldIDList field_ids =
            metadata->GetGroupFieldIDList().GetFieldIDList(
                column_group_id.get());

        auto column_group_info =
            FieldDataInfo(column_group_id.get(), num_rows, infos.mmap_dir_path);
        column_group_info.arrow_reader_channel->set_capacity(parallel_degree);

        LOG_INFO("segment {} loads column group {} with num_rows {}",
                 this->get_segment_id(),
                 column_group_id.get(),
                 num_rows);

        auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);

        // Get all row groups for each file
        std::vector<std::vector<int64_t>> row_group_lists;
        row_group_lists.reserve(insert_files.size());
        for (const auto& file : insert_files) {
            auto reader =
                std::make_shared<milvus_storage::FileRowGroupReader>(fs, file);
            auto row_group_num =
                reader->file_metadata()->GetRowGroupMetadataVector().size();
            std::vector<int64_t> all_row_groups(row_group_num);
            std::iota(all_row_groups.begin(), all_row_groups.end(), 0);
            row_group_lists.push_back(all_row_groups);
        }

        // create parallel degree split strategy
        auto strategy =
            std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);

        auto load_future = pool.Submit([&]() {
            return LoadWithStrategy(insert_files,
                                    column_group_info.arrow_reader_channel,
                                    DEFAULT_FIELD_MAX_MEMORY_LIMIT,
                                    std::move(strategy),
                                    row_group_lists);
        });

        LOG_INFO("segment {} submits load fields {} task to thread pool",
                 this->get_segment_id(),
                 field_ids.ToString());

        std::shared_ptr<milvus::ArrowDataWrapper> r;

        std::unordered_map<FieldId, std::vector<FieldDataPtr>> field_data_map;
        while (column_group_info.arrow_reader_channel->pop(r)) {
            for (const auto& table : r->arrow_tables) {
                size_t batch_num_rows = table->num_rows();
                for (int i = 0; i < field_ids.size(); ++i) {
                    auto field_id = FieldId(field_ids.Get(i));
                    for (auto& field : schema_->get_fields()) {
                        if (field.second.get_id().get() != field_id.get()) {
                            continue;
                        }
                        auto field_data = storage::CreateFieldData(
                            field.second.get_data_type(),
                            field.second.is_nullable(),
                            field.second.is_vector() ? field.second.get_dim()
                                                     : 0,
                            batch_num_rows);
                        field_data->FillFieldData(table->column(i));
                        field_data_map[field_id].push_back(field_data);
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
SegmentGrowingImpl::chunk_data_impl(FieldId field_id, int64_t chunk_id) const {
    return PinWrapper<SpanBase>(
        get_insert_record().get_span_base(field_id, chunk_id));
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_string_view_impl(
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len =
        std::nullopt) const {
    PanicInfo(ErrorCode::NotImplemented,
              "chunk string view impl not implement for growing segment");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_array_view_impl(
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len =
        std::nullopt) const {
    PanicInfo(ErrorCode::NotImplemented,
              "chunk array view impl not implement for growing segment");
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
SegmentGrowingImpl::chunk_view_by_offsets(
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    PanicInfo(ErrorCode::NotImplemented,
              "chunk view by offsets not implemented for growing segment");
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
SegmentGrowingImpl::vector_search(SearchInfo& search_info,
                                  const void* query_data,
                                  int64_t query_count,
                                  Timestamp timestamp,
                                  const BitsetView& bitset,
                                  SearchResult& output) const {
    query::SearchOnGrowing(
        *this, search_info, query_data, query_count, timestamp, bitset, output);
}

std::unique_ptr<DataArray>
SegmentGrowingImpl::bulk_subscript(
    FieldId field_id,
    const int64_t* seg_offsets,
    int64_t count,
    const std::vector<std::string>& dynamic_field_names) const {
    Assert(!dynamic_field_names.empty());
    auto& field_meta = schema_->operator[](field_id);
    auto vec_ptr = insert_record_.get_data_base(field_id);
    auto result = CreateScalarDataArray(count, field_meta);
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
SegmentGrowingImpl::bulk_subscript(FieldId field_id,
                                   const int64_t* seg_offsets,
                                   int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    auto vec_ptr = insert_record_.get_data_base(field_id);
    if (field_meta.is_vector()) {
        auto result = CreateVectorDataArray(count, field_meta);
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            bulk_subscript_impl<FloatVector>(field_id,
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
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_binary_vector()->data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            bulk_subscript_impl<Float16Vector>(
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_float16_vector()->data());
        } else if (field_meta.get_data_type() == DataType::VECTOR_BFLOAT16) {
            bulk_subscript_impl<BFloat16Vector>(
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_bfloat16_vector()->data());
        } else if (field_meta.get_data_type() ==
                   DataType::VECTOR_SPARSE_FLOAT) {
            bulk_subscript_sparse_float_vector_impl(
                field_id,
                (const ConcurrentVector<SparseFloatVector>*)vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_sparse_float_vector());
            result->mutable_vectors()->set_dim(
                result->vectors().sparse_float_vector().dim());
        } else if (field_meta.get_data_type() == DataType::VECTOR_INT8) {
            bulk_subscript_impl<Int8Vector>(
                field_id,
                field_meta.get_sizeof(),
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_vectors()->mutable_int8_vector()->data());
        } else {
            PanicInfo(DataTypeInvalid, "logical error");
        }
        return result;
    }

    AssertInfo(!field_meta.is_vector(),
               "Scalar field meta type is vector type");

    auto result = CreateScalarDataArray(count, field_meta);
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
            bulk_subscript_impl<bool>(vec_ptr,
                                      seg_offsets,
                                      count,
                                      result->mutable_scalars()
                                          ->mutable_bool_data()
                                          ->mutable_data()
                                          ->mutable_data());
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t>(vec_ptr,
                                        seg_offsets,
                                        count,
                                        result->mutable_scalars()
                                            ->mutable_int_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t>(vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(vec_ptr,
                                         seg_offsets,
                                         count,
                                         result->mutable_scalars()
                                             ->mutable_long_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(vec_ptr,
                                       seg_offsets,
                                       count,
                                       result->mutable_scalars()
                                           ->mutable_float_data()
                                           ->mutable_data()
                                           ->mutable_data());
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(vec_ptr,
                                        seg_offsets,
                                        count,
                                        result->mutable_scalars()
                                            ->mutable_double_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::VARCHAR:
        case DataType::TEXT: {
            bulk_subscript_ptr_impl<std::string>(vec_ptr,
                                                 seg_offsets,
                                                 count,
                                                 result->mutable_scalars()
                                                     ->mutable_string_data()
                                                     ->mutable_data());
            break;
        }
        case DataType::JSON: {
            bulk_subscript_ptr_impl<Json>(
                vec_ptr,
                seg_offsets,
                count,
                result->mutable_scalars()->mutable_json_data()->mutable_data());
            break;
        }
        case DataType::ARRAY: {
            // element
            bulk_subscript_array_impl(*vec_ptr,
                                      seg_offsets,
                                      count,
                                      result->mutable_scalars()
                                          ->mutable_array_data()
                                          ->mutable_data());
            break;
        }
        default: {
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported type {}", field_meta.get_data_type()));
        }
    }
    return result;
}

void
SegmentGrowingImpl::bulk_subscript_sparse_float_vector_impl(
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
    const VectorBase* vec_raw,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<std::string>* dst) const {
    auto vec = dynamic_cast<const ConcurrentVector<S>*>(vec_raw);
    auto& src = *vec;
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        if (IsVariableTypeSupportInChunk<S> && mmap_descriptor_ != nullptr) {
            dst->at(i) = std::move(std::string(src.view_element(offset)));
        } else {
            dst->at(i) = std::move(std::string(src[offset]));
        }
    }
}

template <typename T>
void
SegmentGrowingImpl::bulk_subscript_impl(FieldId field_id,
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
    if (indexing_record_.SyncDataWithIndex(field_id)) {
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
        if (!indexing_record_.SyncDataWithIndex(field_id)) {
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
SegmentGrowingImpl::bulk_subscript_impl(const VectorBase* vec_raw,
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

void
SegmentGrowingImpl::bulk_subscript(SystemFieldType system_type,
                                   const int64_t* seg_offsets,
                                   int64_t count,
                                   void* output) const {
    switch (system_type) {
        case SystemFieldType::Timestamp:
            bulk_subscript_impl<Timestamp>(&this->insert_record_.timestamps_,
                                           seg_offsets,
                                           count,
                                           static_cast<Timestamp*>(output));
            break;
        case SystemFieldType::RowId:
            PanicInfo(ErrorCode::Unsupported,
                      "RowId retrieve is not supported");
            break;
        default:
            PanicInfo(DataTypeInvalid, "unknown subscript fields");
    }
}

std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
SegmentGrowingImpl::search_ids(const IdArray& id_array,
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

std::string
SegmentGrowingImpl::debug() const {
    return "Growing\n";
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
                                         Timestamp timestamp) const {
    // DO NOTHING
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
    index->CreateReader();
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
    iter->second->AddTexts(n, texts, texts_valid_data, offset_begin);
}

void
SegmentGrowingImpl::AddJSONDatas(FieldId field_id,
                                 const std::string* jsondatas,
                                 const bool* jsondatas_valid_data,
                                 size_t n,
                                 int64_t offset_begin) {
    std::unique_lock lock(mutex_);
    auto iter = json_indexes_.find(field_id);
    AssertInfo(iter != json_indexes_.end(), "json index not found");
    iter->second->AddJSONDatas(
        n, jsondatas, jsondatas_valid_data, offset_begin);
}

void
SegmentGrowingImpl::CreateJSONIndexes() {
    for (auto [field_id, field_meta] : schema_->get_fields()) {
        if (field_meta.enable_growing_jsonStats()) {
            CreateJSONIndex(FieldId(field_id));
        }
    }
}

void
SegmentGrowingImpl::CreateJSONIndex(FieldId field_id) {
    std::unique_lock lock(mutex_);
    const auto& field_meta = schema_->operator[](field_id);
    AssertInfo(IsJsonDataType(field_meta.get_data_type()),
               "cannot create json index on non-json type");
    std::string unique_id = GetUniqueFieldId(field_meta.get_id().get());
    auto index = std::make_unique<index::JsonKeyStatsInvertedIndex>(
        JSON_KEY_STATS_COMMIT_INTERVAL, unique_id.c_str());

    index->Commit();
    index->CreateReader();

    json_indexes_[field_id] = std::move(index);
}

std::pair<milvus::Json, bool>
SegmentGrowingImpl::GetJsonData(FieldId field_id, size_t offset) const {
    auto vec_ptr = dynamic_cast<const ConcurrentVector<Json>*>(
        insert_record_.get_data_base(field_id));
    auto& src = *vec_ptr;
    auto& field_meta = schema_->operator[](field_id);
    if (field_meta.is_nullable()) {
        auto valid_data_ptr = insert_record_.get_valid_data(field_id);
        return std::make_pair(src[offset], valid_data_ptr->is_valid(offset));
    }
    return std::make_pair(src[offset], true);
}

void
SegmentGrowingImpl::LazyCheckSchema(const Schema& sch) {
    if (sch.get_schema_version() > schema_->get_schema_version()) {
        Reopen(std::make_shared<Schema>(sch));
    }
}

void
SegmentGrowingImpl::Reopen(SchemaPtr sch) {
    std::unique_lock lck(mutex_);

    auto absent_fields = sch->absent_fields(*schema_);

    for (const auto& field_meta : *absent_fields) {
        fill_empty_field(field_meta);
    }

    schema_ = sch;
}

void
SegmentGrowingImpl::FinishLoad() {
    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        if (!insert_record_.is_data_exist(field_id)) {
            fill_empty_field(field_meta);
        }
    }
}

void
SegmentGrowingImpl::fill_empty_field(const FieldMeta& field_meta) {
    auto field_id = field_meta.get_id();
    insert_record_.append_field_meta(field_id, field_meta, size_per_chunk());

    auto total_row_num = insert_record_.size();

    auto data = bulk_subscript_not_exist_field(field_meta, total_row_num);
    insert_record_.get_data_base(field_id)->set_data_raw(
        0, total_row_num, data.get(), field_meta);
    insert_record_.get_valid_data(field_id)->set_data_raw(
        total_row_num, data.get(), field_meta);
}

}  // namespace milvus::segcore
