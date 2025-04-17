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

#include "ChunkedSegmentSealedImpl.h"

#include <arrow/record_batch.h>
#include <fcntl.h>
#include <fmt/core.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "Utils.h"
#include "Types.h"
#include "cachinglayer/Manager.h"
#include "common/Array.h"
#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/ChunkWriter.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/SystemProperty.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "google/protobuf/message_lite.h"
#include "index/VectorMemIndex.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/Types.h"
#include "monitor/prometheus_client.h"
#include "log/Log.h"
#include "pb/schema.pb.h"
#include "query/SearchOnSealed.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "segcore/storagev1translator/DefaultValueChunkTranslator.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"
#include "mmap/ChunkedColumnInterface.h"
#include "mmap/ChunkedColumnGroup.h"
#include "storage/Util.h"
#include "storage/ThreadPools.h"
#include "storage/MmapManager.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/filesystem/fs.h"

namespace milvus::segcore {

using namespace milvus::cachinglayer;

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
ChunkedSegmentSealedImpl::LoadIndex(const LoadIndexInfo& info) {
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
ChunkedSegmentSealedImpl::LoadVecIndex(const LoadIndexInfo& info) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);

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
ChunkedSegmentSealedImpl::LoadScalarIndex(const LoadIndexInfo& info) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto& field_meta = schema_->operator[](field_id);

    auto is_pk = field_id == schema_->get_primary_field_id();

    // if segment is pk sorted, user created indexes bring no performance gain but extra memory usage
    if (is_pk && is_sorted_by_pk_) {
        LOG_INFO(
            "segment pk sorted, skip user index loading for primary key field");
        return;
    }

    std::unique_lock lck(mutex_);
    AssertInfo(
        !get_bit(index_ready_bitset_, field_id),
        "scalar index has been exist at " + std::to_string(field_id.get()));

    if (field_meta.get_data_type() == DataType::JSON) {
        auto path = info.index_params.at(JSON_PATH);
        JSONIndexKey key;
        key.nested_path = path;
        key.field_id = field_id;
        json_indexings_[key] =
            std::move(const_cast<LoadIndexInfo&>(info).index);
        return;
    }
    auto row_count = info.index->Count();
    AssertInfo(row_count > 0, "Index count is 0");
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

    set_bit(index_ready_bitset_, field_id, true);
    // release field column if the index contains raw data
    // only release non-primary field when in pk sorted mode
    if (scalar_indexings_[field_id]->HasRawData() &&
        get_bit(field_data_ready_bitset_, field_id) && !is_pk) {
        // We do not erase the primary key field: if insert record is evicted from memory, when reloading it'll
        // need the pk field again.
        fields_.erase(field_id);
        set_bit(field_data_ready_bitset_, field_id, false);
    }
}

void
ChunkedSegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& load_info) {
    switch (load_info.storage_version) {
        case 2:
            load_column_group_data_internal(load_info);
            if (fields_.find(TimestampFieldID) != fields_.end()) {
                auto timestamp_proxy_column = fields_.at(TimestampFieldID);
                auto num_rows =
                    load_info.field_infos.at(TimestampFieldID.get()).row_count;
                std::vector<Timestamp> timestamps(num_rows);
                int64_t offset = 0;
                for (int i = 0; i < timestamp_proxy_column->num_chunks(); i++) {
                    auto chunk = timestamp_proxy_column->GetChunk(i);
                    auto fixed_chunk =
                        static_cast<FixedWidthChunk*>(chunk.get());
                    auto span = fixed_chunk->Span();
                    for (size_t j = 0; j < span.row_count(); j++) {
                        auto ts = *(int64_t*)((char*)span.data() +
                                              j * span.element_sizeof());
                        timestamps[offset++] = ts;
                    }
                }
                init_timestamp_index(timestamps, num_rows);
                system_ready_count_++;
                AssertInfo(
                    offset == num_rows,
                    "timestamp total row count {} not equal to expected {}",
                    offset,
                    num_rows);
            }
            break;
        default:
            load_field_data_internal(load_info);
            break;
    }
}

void
ChunkedSegmentSealedImpl::load_column_group_data_internal(
    const LoadFieldDataInfo& load_info) {
    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    ArrowSchemaPtr arrow_schema = schema_->ConvertToArrowSchema();

    for (auto& [id, info] : load_info.field_infos) {
        AssertInfo(info.row_count > 0, "The row count of field data is 0");

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

        std::vector<milvus_storage::RowGroupMetadataVector> row_group_meta_list;
        for (const auto& file : insert_files) {
            auto reader =
                std::make_shared<milvus_storage::FileRowGroupReader>(fs, file);
            row_group_meta_list.push_back(
                reader->file_metadata()->GetRowGroupMetadataVector());
        }

        milvus_storage::FieldIDList field_id_list =
            metadata->GetGroupFieldIDList().GetFieldIDList(
                column_group_id.get());
        std::vector<FieldId> milvus_field_ids;
        for (int i = 0; i < field_id_list.size(); ++i) {
            milvus_field_ids.push_back(FieldId(field_id_list.Get(i)));
        }

        auto column_group_info = FieldDataInfo(
            column_group_id.get(), num_rows, load_info.mmap_dir_path);
        LOG_INFO("segment {} loads column group {} with num_rows {}",
                 this->get_segment_id(),
                 column_group_id.get(),
                 num_rows);

        auto field_metas = schema_->get_field_metas(milvus_field_ids);

        auto translator =
            std::make_unique<storagev2translator::GroupChunkTranslator>(
                get_segment_id(),
                field_metas,
                column_group_info,
                insert_files,
                info.enable_mmap,
                row_group_meta_list,
                field_id_list);

        auto chunked_column_group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));

        // Create ProxyChunkColumn for each field in this column group
        for (const auto& field_id : milvus_field_ids) {
            auto field_meta = field_metas.at(field_id);
            auto column = std::make_shared<ProxyChunkColumn>(
                chunked_column_group, field_id, field_meta);
            auto data_type = field_meta.get_data_type();

            load_field_data_common(
                field_id, column, num_rows, data_type, info.enable_mmap, true);
        }
    }
}

void
ChunkedSegmentSealedImpl::load_field_data_internal(
    const LoadFieldDataInfo& load_info) {
    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    AssertInfo(
        !num_rows_.has_value() || num_rows_ == num_rows,
        "num_rows_ is set but not equal to num_rows of LoadFieldDataInfo");

    for (auto& [id, info] : load_info.field_infos) {
        AssertInfo(info.row_count > 0, "The row count of field data is 0");

        auto field_id = FieldId(id);
        auto insert_files = info.insert_files;
        storage::SortByPath(insert_files);

        auto field_data_info =
            FieldDataInfo(field_id.get(), num_rows, load_info.mmap_dir_path);
        LOG_INFO("segment {} loads field {} with num_rows {}",
                 this->get_segment_id(),
                 field_id.get(),
                 num_rows);

        if (SystemProperty::Instance().IsSystem(field_id)) {
            auto parallel_degree = static_cast<uint64_t>(
                DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
            field_data_info.arrow_reader_channel->set_capacity(parallel_degree *
                                                               2);
            auto& pool =
                ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
            pool.Submit(LoadArrowReaderFromRemote,
                        insert_files,
                        field_data_info.arrow_reader_channel);

            LOG_INFO("segment {} submits load field {} task to thread pool",
                     this->get_segment_id(),
                     field_id.get());
            load_system_field_internal(field_id, field_data_info);
            LOG_INFO("segment {} loads system field {} mmap false done",
                     this->get_segment_id(),
                     field_id.get());
        } else {
            auto field_meta = schema_->operator[](field_id);
            std::unique_ptr<Translator<milvus::Chunk>> translator =
                std::make_unique<storagev1translator::ChunkTranslator>(
                    this->get_segment_id(),
                    field_meta,
                    field_data_info,
                    insert_files,
                    info.enable_mmap);

            auto data_type = field_meta.get_data_type();
            auto column = MakeChunkedColumnBase(
                data_type, std::move(translator), field_meta);

            load_field_data_common(
                field_id, column, num_rows, data_type, info.enable_mmap, false);
        }
    }
}

void
ChunkedSegmentSealedImpl::load_system_field_internal(FieldId field_id,
                                                     FieldDataInfo& data) {
    auto num_rows = data.row_count;
    AssertInfo(SystemProperty::Instance().IsSystem(field_id),
               "system field is not system field");
    auto system_field_type =
        SystemProperty::Instance().GetSystemFieldType(field_id);
    if (system_field_type == SystemFieldType::Timestamp) {
        std::vector<Timestamp> timestamps(num_rows);
        int64_t offset = 0;
        FieldMeta field_meta(
            FieldName(""), FieldId(0), DataType::INT64, false, std::nullopt);
        std::shared_ptr<milvus::ArrowDataWrapper> r;
        while (data.arrow_reader_channel->pop(r)) {
            auto array_vec = read_single_column_batches(r->reader);
            auto chunk = create_chunk(field_meta, 1, array_vec);
            auto chunk_ptr = static_cast<FixedWidthChunk*>(chunk.get());
            std::copy_n(static_cast<const Timestamp*>(chunk_ptr->Span().data()),
                        chunk_ptr->Span().row_count(),
                        timestamps.data() + offset);
            offset += chunk_ptr->Span().row_count();
        }

        init_timestamp_index(timestamps, num_rows);
        ++system_ready_count_;
    } else {
        AssertInfo(system_field_type == SystemFieldType::RowId,
                   "System field type of id column is not RowId");
        // Consume rowid field data but not really load it
        // storage::CollectFieldDataChannel(data.arrow_reader_channel);
        std::shared_ptr<milvus::ArrowDataWrapper> r;
        while (data.arrow_reader_channel->pop(r)) {
        }
    }
    {
        std::unique_lock lck(mutex_);
        update_row_count(num_rows);
    }
}

void
ChunkedSegmentSealedImpl::LoadDeletedRecord(const LoadDeletedRecordInfo& info) {
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

    // step 2: push delete info to delete_record
    deleted_record_.LoadPush(pks, timestamps);
}

void
ChunkedSegmentSealedImpl::AddFieldDataInfoForSealed(
    const LoadFieldDataInfo& field_data_info) {
    // copy assignment
    field_data_info_ = field_data_info;
}

// internal API: support scalar index only
int64_t
ChunkedSegmentSealedImpl::num_chunk_index(FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    if (field_meta.is_vector()) {
        return int64_t(vector_indexings_.is_ready(field_id));
    }

    return scalar_indexings_.count(field_id);
}

int64_t
ChunkedSegmentSealedImpl::num_chunk_data(FieldId field_id) const {
    return get_bit(field_data_ready_bitset_, field_id)
               ? fields_.find(field_id) != fields_.end()
                     ? fields_.at(field_id)->num_chunks()
                     : 1
               : 0;
}

int64_t
ChunkedSegmentSealedImpl::num_chunk(FieldId field_id) const {
    return get_bit(field_data_ready_bitset_, field_id)
               ? fields_.find(field_id) != fields_.end()
                     ? fields_.at(field_id)->num_chunks()
                     : 1
               : 1;
}

int64_t
ChunkedSegmentSealedImpl::size_per_chunk() const {
    return get_row_count();
}

int64_t
ChunkedSegmentSealedImpl::chunk_size(FieldId field_id, int64_t chunk_id) const {
    return get_bit(field_data_ready_bitset_, field_id)
               ? fields_.find(field_id) != fields_.end()
                     ? fields_.at(field_id)->chunk_row_nums(chunk_id)
                     : num_rows_.value()
               : 0;
}

std::pair<int64_t, int64_t>
ChunkedSegmentSealedImpl::get_chunk_by_offset(FieldId field_id,
                                              int64_t offset) const {
    return fields_.at(field_id)->GetChunkIDByOffset(offset);
}

int64_t
ChunkedSegmentSealedImpl::num_rows_until_chunk(FieldId field_id,
                                               int64_t chunk_id) const {
    return fields_.at(field_id)->GetNumRowsUntilChunk(chunk_id);
}

bool
ChunkedSegmentSealedImpl::is_mmap_field(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    return mmap_fields_.find(field_id) != mmap_fields_.end();
}

PinWrapper<SpanBase>
ChunkedSegmentSealedImpl::chunk_data_impl(FieldId field_id,
                                          int64_t chunk_id) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto it = fields_.find(field_id); it != fields_.end()) {
        return it->second->Span(chunk_id);
    }
    PanicInfo(ErrorCode::UnexpectedError,
              "chunk_data_impl only used for chunk column field ");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
ChunkedSegmentSealedImpl::chunk_array_view_impl(
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len =
        std::nullopt) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto it = fields_.find(field_id); it != fields_.end()) {
        return it->second->ArrayViews(chunk_id, offset_len);
    }
    PanicInfo(ErrorCode::UnexpectedError,
              "chunk_array_view_impl only used for chunk column field ");
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
ChunkedSegmentSealedImpl::chunk_string_view_impl(
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len =
        std::nullopt) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto it = fields_.find(field_id); it != fields_.end()) {
        auto column = it->second;
        return column->StringViews(chunk_id, offset_len);
    }
    PanicInfo(ErrorCode::UnexpectedError,
              "chunk_string_view_impl only used for variable column field ");
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
ChunkedSegmentSealedImpl::chunk_view_by_offsets(
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto it = fields_.find(field_id); it != fields_.end()) {
        return it->second->ViewsByOffsets(chunk_id, offsets);
    }
    PanicInfo(ErrorCode::UnexpectedError,
              "chunk_view_by_offsets only used for variable column field ");
}

const index::IndexBase*
ChunkedSegmentSealedImpl::chunk_index_impl(FieldId field_id,
                                           int64_t chunk_id) const {
    AssertInfo(scalar_indexings_.find(field_id) != scalar_indexings_.end(),
               "Cannot find scalar_indexing with field_id: " +
                   std::to_string(field_id.get()));
    return scalar_indexings_.at(field_id).get();
}

int64_t
ChunkedSegmentSealedImpl::get_row_count() const {
    std::shared_lock lck(mutex_);
    return num_rows_.value_or(0);
}

int64_t
ChunkedSegmentSealedImpl::get_deleted_count() const {
    std::shared_lock lck(mutex_);
    return deleted_record_.size();
}

const Schema&
ChunkedSegmentSealedImpl::get_schema() const {
    return *schema_;
}

void
ChunkedSegmentSealedImpl::mask_with_delete(BitsetTypeView& bitset,
                                           int64_t ins_barrier,
                                           Timestamp timestamp) const {
    deleted_record_.Query(bitset, ins_barrier, timestamp);
}

void
ChunkedSegmentSealedImpl::vector_search(SearchInfo& search_info,
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

        // get index params for bm25 brute force
        std::map<std::string, std::string> index_info;
        if (search_info.metric_type_ == knowhere::metric::BM25) {
            index_info =
                col_index_meta_->GetFieldIndexMeta(field_id).GetIndexParams();
        }

        query::SearchOnSealedColumn(*schema_,
                                    vec_data.get(),
                                    search_info,
                                    index_info,
                                    query_data,
                                    query_count,
                                    row_count,
                                    bitset,
                                    output);
        milvus::tracer::AddEvent("finish_searching_vector_data");
    }
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::get_vector(FieldId field_id,
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

    AssertInfo(false, "get_vector called on vector index without raw data");
    return nullptr;
}

void
ChunkedSegmentSealedImpl::DropFieldData(const FieldId field_id) {
    AssertInfo(!SystemProperty::Instance().IsSystem(field_id),
               "Dropping system field is not supported, field id: {}",
               field_id.get());
    std::unique_lock<std::shared_mutex> lck(mutex_);
    if (get_bit(field_data_ready_bitset_, field_id)) {
        fields_.erase(field_id);
        set_bit(field_data_ready_bitset_, field_id, false);
    }
    if (get_bit(binlog_index_bitset_, field_id)) {
        set_bit(binlog_index_bitset_, field_id, false);
        vector_indexings_.drop_field_indexing(field_id);
    }
}

void
ChunkedSegmentSealedImpl::DropIndex(const FieldId field_id) {
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
ChunkedSegmentSealedImpl::check_search(const query::Plan* plan) const {
    AssertInfo(plan, "Search plan is null");
    AssertInfo(plan->extra_info_opt_.has_value(),
               "Extra info of search plan doesn't have value");

    if (!is_system_field_ready()) {
        PanicInfo(FieldNotLoaded,
                  "failed to load row ID or timestamp, potential missing "
                  "bin logs or "
                  "empty segments. Segment ID = " +
                      std::to_string(this->id_));
    }

    auto& request_fields = plan->extra_info_opt_.value().involved_fields_;
    auto field_ready_bitset =
        field_data_ready_bitset_ | index_ready_bitset_ | binlog_index_bitset_;

    // allow absent fields after supporting add fields
    AssertInfo(request_fields.size() >= field_ready_bitset.size(),
               "Request fields size less than field ready bitset size when "
               "check search");

    auto absent_fields = request_fields - field_ready_bitset;

    if (absent_fields.any()) {
        // absent_fields.find_first() returns std::optional<>
        auto field_id =
            FieldId(absent_fields.find_first().value() + START_USER_FIELDID);
        auto& field_meta = plan->schema_.operator[](field_id);
        // request field may has added field
        if (!field_meta.is_nullable()) {
            PanicInfo(FieldNotLoaded,
                      "User Field(" + field_meta.get_name().get() +
                          ") is not loaded");
        }
    }
}

std::vector<SegOffset>
ChunkedSegmentSealedImpl::search_pk(const PkType& pk,
                                    Timestamp timestamp) const {
    if (!is_sorted_by_pk_) {
        return insert_record_.search_pk(pk, timestamp);
    }
    return search_sorted_pk(pk, [this, timestamp](int64_t offset) {
        return insert_record_.timestamps_[offset] <= timestamp;
    });
}

template <typename Condition>
std::vector<SegOffset>
ChunkedSegmentSealedImpl::search_sorted_pk(const PkType& pk,
                                           Condition condition) const {
    auto pk_field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = fields_.at(pk_field_id);
    std::vector<SegOffset> pk_offsets;

    switch (schema_->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64: {
            auto target = std::get<int64_t>(pk);
            // get int64 pks

            auto num_chunk = pk_column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                auto pw = pk_column->DataOfChunk(i);
                auto src = reinterpret_cast<const int64_t*>(pw.get());
                auto chunk_row_num = pk_column->chunk_row_nums(i);
                auto it = std::lower_bound(
                    src,
                    src + chunk_row_num,
                    target,
                    [](const int64_t& elem, const int64_t& value) {
                        return elem < value;
                    });
                auto num_rows_until_chunk = pk_column->GetNumRowsUntilChunk(i);
                for (; it != src + chunk_row_num && *it == target; ++it) {
                    auto offset = it - src + num_rows_until_chunk;
                    if (condition(offset)) {
                        pk_offsets.emplace_back(offset);
                    }
                }
            }

            break;
        }
        case DataType::VARCHAR: {
            auto target = std::get<std::string>(pk);
            // get varchar pks

            auto num_chunk = pk_column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                // TODO @xiaocai2333, @sunby: chunk need to record the min/max.
                auto num_rows_until_chunk = pk_column->GetNumRowsUntilChunk(i);
                auto pw = pk_column->GetChunk(i);
                auto string_chunk = static_cast<StringChunk*>(pw.get());
                auto offset = string_chunk->binary_search_string(target);
                for (; offset != -1 && offset < string_chunk->RowNums() &&
                       string_chunk->operator[](offset) == target;
                     ++offset) {
                    auto segment_offset = offset + num_rows_until_chunk;
                    if (condition(segment_offset)) {
                        pk_offsets.emplace_back(segment_offset);
                    }
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

std::pair<std::vector<OffsetMap::OffsetType>, bool>
ChunkedSegmentSealedImpl::find_first(int64_t limit,
                                     const BitsetType& bitset) const {
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
    std::optional<size_t> result = bitset.find_first(false);
    while (result.has_value() && hit_num < limit) {
        hit_num++;
        seg_offsets.push_back(result.value());
        offset = result.value();
        if (offset >= size) {
            // In fact, this case won't happen on sealed segments.
            continue;
        }
        result = bitset.find_next(offset, false);
    }

    return {seg_offsets, more_hit_than_limit && result.has_value()};
}

ChunkedSegmentSealedImpl::ChunkedSegmentSealedImpl(
    SchemaPtr schema,
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
      is_sorted_by_pk_(is_sorted_by_pk),
      deleted_record_(
          &insert_record_,
          [this](const PkType& pk, Timestamp timestamp) {
              return this->search_pk(pk, timestamp);
          },
          segment_id) {
    mmap_descriptor_ = std::shared_ptr<storage::MmapChunkDescriptor>(
        new storage::MmapChunkDescriptor({segment_id, SegmentType::Sealed}));
    auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
    mcm->Register(mmap_descriptor_);
}

ChunkedSegmentSealedImpl::~ChunkedSegmentSealedImpl() {
    if (mmap_descriptor_ != nullptr) {
        auto mm = storage::MmapManager::GetInstance().GetMmapChunkManager();
        mm->UnRegister(mmap_descriptor_);
    }
}

void
ChunkedSegmentSealedImpl::bulk_subscript(SystemFieldType system_type,
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
ChunkedSegmentSealedImpl::bulk_subscript_impl(const void* src_raw,
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
ChunkedSegmentSealedImpl::bulk_subscript_impl(ChunkedColumnInterface* field,
                                              const int64_t* seg_offsets,
                                              int64_t count,
                                              T* dst) {
    static_assert(IsScalar<T>);
    auto column = reinterpret_cast<ChunkedColumn*>(field);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst[i] = *static_cast<const S*>(
            static_cast<const void*>(column->ValueAt(offset)));
    }
}

template <typename S, typename T>
void
ChunkedSegmentSealedImpl::bulk_subscript_ptr_impl(
    ChunkedColumnInterface* column,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) {
    auto field = reinterpret_cast<ChunkedVariableColumn<S>*>(column);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst->at(i) = std::move(T(field->RawAt(offset)));
    }
}

template <typename T>
void
ChunkedSegmentSealedImpl::bulk_subscript_array_impl(
    ChunkedColumnInterface* column,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) {
    auto field = reinterpret_cast<ChunkedArrayColumn*>(column);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst->at(i) = std::move(field->PrimitivieRawAt(offset));
    }
}

// for dense vector
void
ChunkedSegmentSealedImpl::bulk_subscript_impl(int64_t element_sizeof,
                                              ChunkedColumnInterface* field,
                                              const int64_t* seg_offsets,
                                              int64_t count,
                                              void* dst_raw) {
    auto dst_vec = reinterpret_cast<char*>(dst_raw);
    auto column = reinterpret_cast<ChunkedColumn*>(field);
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        auto src = column->ValueAt(offset);
        auto dst = dst_vec + i * element_sizeof;
        memcpy(dst, src, element_sizeof);
    }
}

void
ChunkedSegmentSealedImpl::ClearData() {
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
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::fill_with_empty(FieldId field_id,
                                          int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    if (IsVectorDataType(field_meta.get_data_type())) {
        return CreateVectorDataArray(count, field_meta);
    }
    return CreateScalarDataArray(count, field_meta);
}

void
ChunkedSegmentSealedImpl::CreateTextIndex(FieldId field_id) {
    std::unique_lock lck(mutex_);

    const auto& field_meta = schema_->operator[](field_id);
    auto& cfg = storage::MmapManager::GetInstance().GetMmapConfig();
    std::unique_ptr<index::TextMatchIndex> index;
    std::string unique_id = GetUniqueFieldId(field_meta.get_id().get());
    if (!cfg.GetScalarIndexEnableMmap()) {
        // build text index in ram.
        index = std::make_unique<index::TextMatchIndex>(
            std::numeric_limits<int64_t>::max(),
            unique_id.c_str(),
            "milvus_tokenizer",
            field_meta.get_analyzer_params().c_str());
    } else {
        // build text index using mmap.
        index = std::make_unique<index::TextMatchIndex>(
            cfg.GetMmapPath(),
            unique_id.c_str(),
            // todo: make it configurable
            index::TANTIVY_INDEX_LATEST_VERSION,
            "milvus_tokenizer",
            field_meta.get_analyzer_params().c_str());
    }

    {
        // build
        auto iter = fields_.find(field_id);
        if (iter != fields_.end()) {
            auto column =
                std::dynamic_pointer_cast<ChunkedVariableColumn<std::string>>(
                    iter->second);
            AssertInfo(
                column != nullptr,
                "failed to create text index, field is not of text type: {}",
                field_id.get());
            auto n = column->NumRows();
            for (size_t i = 0; i < n; i++) {
                index->AddText(
                    std::string(column->RawAt(i)), column->IsValid(i), i);
            }
        } else {  // fetch raw data from index.
            auto field_index_iter = scalar_indexings_.find(field_id);
            AssertInfo(field_index_iter != scalar_indexings_.end(),
                       "failed to create text index, neither raw data nor "
                       "index are found");
            auto ptr = field_index_iter->second.get();
            AssertInfo(ptr->HasRawData(),
                       "text raw data not found, trying to create text index "
                       "from index, but this index don't contain raw data");
            auto impl = dynamic_cast<index::ScalarIndex<std::string>*>(ptr);
            AssertInfo(impl != nullptr,
                       "failed to create text index, field index cannot be "
                       "converted to string index");
            auto n = impl->Size();
            for (size_t i = 0; i < n; i++) {
                auto raw = impl->Reverse_Lookup(i);
                if (!raw.has_value()) {
                    index->AddNull(i);
                }
                index->AddText(raw.value(), true, i);
            }
        }
    }

    // create index reader.
    index->CreateReader();
    // release index writer.
    index->Finish();

    index->Reload();

    index->RegisterTokenizer("milvus_tokenizer",
                             field_meta.get_analyzer_params().c_str());

    text_indexes_[field_id] = std::move(index);
}

void
ChunkedSegmentSealedImpl::LoadTextIndex(
    FieldId field_id, std::unique_ptr<index::TextMatchIndex> index) {
    std::unique_lock lck(mutex_);
    const auto& field_meta = schema_->operator[](field_id);
    index->RegisterTokenizer("milvus_tokenizer",
                             field_meta.get_analyzer_params().c_str());
    text_indexes_[field_id] = std::move(index);
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::get_raw_data(FieldId field_id,
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
        case DataType::STRING:
        case DataType::TEXT: {
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
            bulk_subscript_impl<bool>(column.get(),
                                      seg_offsets,
                                      count,
                                      ret->mutable_scalars()
                                          ->mutable_bool_data()
                                          ->mutable_data()
                                          ->mutable_data());
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t>(column.get(),
                                        seg_offsets,
                                        count,
                                        ret->mutable_scalars()
                                            ->mutable_int_data()
                                            ->mutable_data()
                                            ->mutable_data());
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t>(column.get(),
                                         seg_offsets,
                                         count,
                                         ret->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(column.get(),
                                         seg_offsets,
                                         count,
                                         ret->mutable_scalars()
                                             ->mutable_int_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(column.get(),
                                         seg_offsets,
                                         count,
                                         ret->mutable_scalars()
                                             ->mutable_long_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(column.get(),
                                       seg_offsets,
                                       count,
                                       ret->mutable_scalars()
                                           ->mutable_float_data()
                                           ->mutable_data()
                                           ->mutable_data());
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(column.get(),
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
                                column.get(),
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
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_float16_vector()->data());
            break;
        }
        case DataType::VECTOR_BFLOAT16: {
            bulk_subscript_impl(
                field_meta.get_sizeof(),
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_bfloat16_vector()->data());
            break;
        }
        case DataType::VECTOR_BINARY: {
            bulk_subscript_impl(
                field_meta.get_sizeof(),
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_binary_vector()->data());
            break;
        }
        case DataType::VECTOR_SPARSE_FLOAT: {
            auto dst = ret->mutable_vectors()->mutable_sparse_float_vector();
            auto col = reinterpret_cast<ChunkedColumn*>(column.get());
            SparseRowsToProto(
                [&](size_t i) {
                    auto offset = seg_offsets[i];
                    auto row =
                        static_cast<const knowhere::sparse::SparseRow<float>*>(
                            static_cast<const void*>(col->ValueAt(offset)));
                    return offset != INVALID_SEG_OFFSET ? row : nullptr;
                },
                count,
                dst);
            ret->mutable_vectors()->set_dim(dst->dim());
            break;
        }
        case DataType::VECTOR_INT8: {
            bulk_subscript_impl(
                field_meta.get_sizeof(),
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_int8_vector()->data());
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
ChunkedSegmentSealedImpl::bulk_subscript(FieldId field_id,
                                         const int64_t* seg_offsets,
                                         int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    // if count == 0, return empty data array
    if (count == 0) {
        return fill_with_empty(field_id, count);
    }

    if (!HasIndex(field_id)) {
        Assert(get_bit(field_data_ready_bitset_, field_id));
        return get_raw_data(field_id, field_meta, seg_offsets, count);
    }

    auto index_has_raw = HasRawData(field_id.get());

    if (!IsVectorDataType(field_meta.get_data_type())) {
        // if field has load scalar index, reverse raw data from index
        if (index_has_raw) {
            auto index = chunk_index_impl(field_id, 0);
            return ReverseDataFromIndex(index, seg_offsets, count, field_meta);
        }
        return get_raw_data(field_id, field_meta, seg_offsets, count);
    }

    std::chrono::high_resolution_clock::time_point get_vector_start =
        std::chrono::high_resolution_clock::now();

    std::unique_ptr<DataArray> vector{nullptr};
    if (index_has_raw) {
        vector = get_vector(field_id, seg_offsets, count);
    } else {
        vector = get_raw_data(field_id, field_meta, seg_offsets, count);
    }

    std::chrono::high_resolution_clock::time_point get_vector_end =
        std::chrono::high_resolution_clock::now();
    double get_vector_cost = std::chrono::duration<double, std::micro>(
                                 get_vector_end - get_vector_start)
                                 .count();
    monitor::internal_core_get_vector_latency.Observe(get_vector_cost / 1000);

    return vector;
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::bulk_subscript(
    FieldId field_id,
    const int64_t* seg_offsets,
    int64_t count,
    const std::vector<std::string>& dynamic_field_names) const {
    Assert(!dynamic_field_names.empty());
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
    auto field = reinterpret_cast<ChunkedVariableColumn<Json>*>(column.get());
    for (int64_t i = 0; i < count; ++i) {
        auto offset = seg_offsets[i];
        dst->at(i) = ExtractSubJson(std::string(field->RawAt(offset)),
                                    dynamic_field_names);
    }
    return ret;
}

bool
ChunkedSegmentSealedImpl::HasIndex(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    return get_bit(index_ready_bitset_, field_id) |
           get_bit(binlog_index_bitset_, field_id);
}

bool
ChunkedSegmentSealedImpl::HasFieldData(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    if (SystemProperty::Instance().IsSystem(field_id)) {
        return is_system_field_ready();
    } else {
        return get_bit(field_data_ready_bitset_, field_id);
    }
}

bool
ChunkedSegmentSealedImpl::HasRawData(int64_t field_id) const {
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
    } else if (IsJsonDataType(field_meta.get_data_type())) {
        return get_bit(field_data_ready_bitset_, fieldID);
    } else {
        auto scalar_index = scalar_indexings_.find(fieldID);
        if (scalar_index != scalar_indexings_.end()) {
            return scalar_index->second->HasRawData();
        }
    }
    return true;
}

DataType
ChunkedSegmentSealedImpl::GetFieldDataType(milvus::FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    return field_meta.get_data_type();
}

std::pair<std::unique_ptr<IdArray>, std::vector<SegOffset>>
ChunkedSegmentSealedImpl::search_ids(const IdArray& id_array,
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

SegcoreError
ChunkedSegmentSealedImpl::Delete(int64_t size,
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
    // if insert record is empty (may be only-load meta but not data for lru-cache at go side),
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

    deleted_record_.StreamPush(sort_pks, sort_timestamps.data());
    return SegcoreError::success();
}

std::string
ChunkedSegmentSealedImpl::debug() const {
    std::string log_str;
    log_str += "Sealed\n";
    log_str += "\n";
    return log_str;
}

void
ChunkedSegmentSealedImpl::LoadSegmentMeta(
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
ChunkedSegmentSealedImpl::get_active_count(Timestamp ts) const {
    // TODO optimize here to reduce expr search range
    return this->get_row_count();
}

void
ChunkedSegmentSealedImpl::mask_with_timestamps(BitsetTypeView& bitset_chunk,
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
ChunkedSegmentSealedImpl::generate_interim_index(const FieldId field_id) {
    if (col_index_meta_ == nullptr || !col_index_meta_->HasFiled(field_id)) {
        return false;
    }
    auto& field_meta = schema_->operator[](field_id);
    auto& field_index_meta = col_index_meta_->GetFieldIndexMeta(field_id);
    auto& index_params = field_index_meta.GetIndexParams();

    bool is_sparse =
        field_meta.get_data_type() == DataType::VECTOR_SPARSE_FLOAT;

    bool enable_growing_mmap = storage::MmapManager::GetInstance()
                                   .GetMmapConfig()
                                   .GetEnableGrowingMmap();

    auto enable_binlog_index = [&]() {
        // check milvus config
        if (!segcore_config_.get_enable_interim_segment_index() ||
            enable_growing_mmap) {
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
        // if (row_count < field_binlog_config->GetBuildThreshold()) {
        //     return false;
        // }
        std::shared_ptr<ChunkedColumnInterface> vec_data{};
        {
            std::shared_lock lck(mutex_);
            vec_data = fields_.at(field_id);
        }
        auto dim = is_sparse ? std::numeric_limits<uint32_t>::max()
                             : field_meta.get_dim();

        auto build_config = field_binlog_config->GetBuildBaseParams();
        build_config[knowhere::meta::DIM] = std::to_string(dim);
        build_config[knowhere::meta::NUM_BUILD_THREAD] = std::to_string(1);
        auto index_metric = field_binlog_config->GetMetricType();

        auto vec_index = std::make_unique<index::VectorMemIndex<float>>(
            field_binlog_config->GetIndexType(),
            index_metric,
            knowhere::Version::GetCurrentVersion().VersionNumber());
        auto num_chunk = vec_data->num_chunks();
        for (int i = 0; i < num_chunk; ++i) {
            auto pw = vec_data->GetChunk(i);
            auto chunk = pw.get();
            auto dataset = knowhere::GenDataSet(
                vec_data->chunk_row_nums(i), dim, chunk->Data());
            dataset->SetIsOwner(false);
            dataset->SetIsSparse(is_sparse);

            if (i == 0) {
                vec_index->BuildWithDataset(dataset, build_config);
            } else {
                vec_index->AddWithDataset(dataset, build_config);
            }
        }

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
ChunkedSegmentSealedImpl::RemoveFieldFile(const FieldId field_id) {
}

void
ChunkedSegmentSealedImpl::LazyCheckSchema(const Schema& sch) {
    if (sch.get_schema_version() > schema_->get_schema_version()) {
        Reopen(std::make_shared<Schema>(sch));
    }
}

void
ChunkedSegmentSealedImpl::load_field_data_common(
    FieldId field_id,
    const std::shared_ptr<ChunkedColumnInterface>& column,
    size_t num_rows,
    DataType data_type,
    bool enable_mmap,
    bool is_proxy_column) {
    {
        std::unique_lock lck(mutex_);
        fields_.emplace(field_id, column);
        if (enable_mmap) {
            mmap_fields_.insert(field_id);
        }
    }
    // system field only needs to emplace column to fields_ map
    if (SystemProperty::Instance().IsSystem(field_id)) {
        return;
    }

    if (!enable_mmap) {
        stats_.mem_size += column->DataByteSize();
        if (IsVariableDataType(data_type)) {
            if (IsStringDataType(data_type)) {
                if (!is_proxy_column) {
                    auto var_column = std::dynamic_pointer_cast<
                        ChunkedVariableColumn<std::string>>(column);
                    AssertInfo(var_column != nullptr,
                               "column is not of variable type");
                    LoadStringSkipIndex(field_id, 0, *var_column);
                } else {
                    auto var_column =
                        std::dynamic_pointer_cast<ProxyChunkColumn>(column);
                    AssertInfo(var_column != nullptr,
                               "column is not of variable type");
                    LoadStringSkipIndex(field_id, 0, *var_column);
                }
            }
            // update average row data size
            SegmentInternalInterface::set_field_avg_size(
                field_id, num_rows, column->DataByteSize());
        } else {
            auto num_chunk = column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                if (!is_proxy_column) {
                    auto primitive_column =
                        std::dynamic_pointer_cast<ChunkedColumn>(column);
                    AssertInfo(primitive_column != nullptr,
                               "column is not of primitive type");
                    auto pw = primitive_column->Span(i);
                    LoadPrimitiveSkipIndex(field_id,
                                           i,
                                           data_type,
                                           pw.get().data(),
                                           pw.get().valid_data(),
                                           pw.get().row_count());
                } else {
                    auto proxy_column =
                        std::dynamic_pointer_cast<ProxyChunkColumn>(column);
                    AssertInfo(proxy_column != nullptr,
                               "column is not of proxy type");
                    auto pw = proxy_column->Span(i);
                    LoadPrimitiveSkipIndex(field_id,
                                           i,
                                           data_type,
                                           pw.get().data(),
                                           pw.get().valid_data(),
                                           pw.get().row_count());
                }
            }
        }
    }

    // set pks to offset
    if (schema_->get_primary_field_id() == field_id && !is_sorted_by_pk_) {
        AssertInfo(field_id.get() != -1, "Primary key is -1");
        AssertInfo(insert_record_.empty_pks(), "already exists");
        insert_record_.insert_pks(data_type, column.get());
        insert_record_.seal_pks();
    }

    if (generate_interim_index(field_id)) {
        std::unique_lock lck(mutex_);
        // mmap_fields is useless, no change
        fields_.erase(field_id);
        set_bit(field_data_ready_bitset_, field_id, false);
    } else {
        std::unique_lock lck(mutex_);
        set_bit(field_data_ready_bitset_, field_id, true);
    }

    {
        std::unique_lock lck(mutex_);
        update_row_count(num_rows);
    }
}

void
ChunkedSegmentSealedImpl::init_timestamp_index(
    const std::vector<Timestamp>& timestamps, size_t num_rows) {
    TimestampIndex index;
    auto min_slice_length = num_rows < 4096 ? 1 : 4096;
    auto meta =
        GenerateFakeSlices(timestamps.data(), num_rows, min_slice_length);
    index.set_length_meta(std::move(meta));
    // todo ::opt to avoid copy timestamps from field data
    index.build_with(timestamps.data(), num_rows);

    // use special index
    std::unique_lock lck(mutex_);
    AssertInfo(insert_record_.timestamps_.empty(), "already exists");
    insert_record_.timestamps_.set_data_raw(
        0, timestamps.data(), timestamps.size());
    insert_record_.timestamp_index_ = std::move(index);
    AssertInfo(insert_record_.timestamps_.num_chunk() == 1,
               "num chunk not equal to 1 for sealed segment");
    stats_.mem_size += sizeof(Timestamp) * num_rows;
}

void
ChunkedSegmentSealedImpl::Reopen(SchemaPtr sch) {
    std::unique_lock lck(mutex_);

    field_data_ready_bitset_.resize(sch->size());
    index_ready_bitset_.resize(sch->size());
    binlog_index_bitset_.resize(sch->size());

    auto absent_fields = sch->absent_fields(*schema_);
    for (const auto& field_meta : *absent_fields) {
        // vector field is not supported to be "added field", thus if a vector
        // field is absent, it means for some reason we want to skip loading this
        // field.
        if (!IsVectorDataType(field_meta.get_data_type())) {
            fill_empty_field(field_meta);
        }
    }

    schema_ = sch;
}

void
ChunkedSegmentSealedImpl::FinishLoad() {
    std::unique_lock lck(mutex_);
    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        if (field_id.get() < START_USER_FIELDID) {
            continue;
        }
        // cannot use is_field_exist, since it check schema only
        // this shall check the ready bitset here
        if (!get_bit(field_data_ready_bitset_, field_id) &&
            !get_bit(index_ready_bitset_, field_id)) {
            // vector field is not supported to be "added field", thus if a vector
            // field is absent, it means for some reason we want to skip loading this
            // field.
            if (!IsVectorDataType(field_meta.get_data_type())) {
                fill_empty_field(field_meta);
            }
        }
    }
}

void
ChunkedSegmentSealedImpl::fill_empty_field(const FieldMeta& field_meta) {
    int64_t size = num_rows_.value();
    AssertInfo(size > 0, "Chunked Sealed segment must have more than 0 row");
    auto field_data_info = FieldDataInfo(field_meta.get_id().get(), size, "");
    std::unique_ptr<Translator<milvus::Chunk>> translator =
        std::make_unique<storagev1translator::DefaultValueChunkTranslator>(
            get_segment_id(), field_meta, field_data_info, false);
    std::shared_ptr<milvus::ChunkedColumnBase> column{};
    switch (field_meta.get_data_type()) {
        case milvus::DataType::STRING:
        case milvus::DataType::VARCHAR:
        case milvus::DataType::TEXT: {
            column = std::make_shared<ChunkedVariableColumn<std::string>>(
                std::move(translator), field_meta);
            break;
        }
        case milvus::DataType::JSON: {
            column = std::make_shared<ChunkedVariableColumn<milvus::Json>>(
                std::move(translator), field_meta);
            break;
        }
        case milvus::DataType::ARRAY: {
            column = std::make_shared<ChunkedArrayColumn>(std::move(translator),
                                                          field_meta);
            break;
        }
        default: {
            column = std::make_shared<ChunkedColumn>(std::move(translator),
                                                     field_meta);
            break;
        }
    }
    auto field_id = field_meta.get_id();
    fields_.emplace(field_id, column);
    set_bit(field_data_ready_bitset_, field_id, true);
}

}  // namespace milvus::segcore
