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
#include <ctime>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "Utils.h"
#include "Types.h"
#include "cachinglayer/Manager.h"
#include "common/Array.h"
#include "common/Chunk.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/ChunkWriter.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/SystemProperty.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/resource_c.h"
#include "folly/Synchronized.h"
#include "monitor/scope_metric.h"
#include "google/protobuf/message_lite.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "milvus-storage/common/metadata.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/Types.h"
#include "monitor/Monitor.h"
#include "log/Log.h"
#include "pb/schema.pb.h"
#include "query/SearchOnSealed.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "segcore/storagev1translator/DefaultValueChunkTranslator.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"
#include "mmap/ChunkedColumnInterface.h"
#include "mmap/ChunkedColumnGroup.h"
#include "segcore/storagev1translator/InterimSealedIndexTranslator.h"
#include "segcore/storagev1translator/TextMatchIndexTranslator.h"
#include "storage/Util.h"
#include "storage/ThreadPools.h"
#include "storage/MmapManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "milvus-storage/filesystem/fs.h"
#include "cachinglayer/CacheSlot.h"
#include "storage/LocalChunkManagerSingleton.h"

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

    std::unique_lock lck(mutex_);
    AssertInfo(
        !get_bit(index_ready_bitset_, field_id),
        "vector index has been exist at " + std::to_string(field_id.get()));
    LOG_INFO(
        "Before setting field_bit for field index, fieldID:{}. "
        "segmentID:{}, ",
        info.field_id,
        id_);
    auto& field_meta = schema_->operator[](field_id);
    LoadResourceRequest request =
        milvus::index::IndexFactory::GetInstance().VecIndexLoadResource(
            field_meta.get_data_type(),
            info.element_type,
            info.index_engine_version,
            info.index_size,
            info.index_params,
            info.enable_mmap,
            info.num_rows,
            info.dim);

    if (request.has_raw_data && get_bit(field_data_ready_bitset_, field_id)) {
        fields_.rlock()->at(field_id)->ManualEvictCache();
    }
    if (get_bit(binlog_index_bitset_, field_id)) {
        set_bit(binlog_index_bitset_, field_id, false);
        vector_indexings_.drop_field_indexing(field_id);
    }
    vector_indexings_.append_field_indexing(
        field_id,
        metric_type,
        std::move(const_cast<LoadIndexInfo&>(info).cache_index));
    set_bit(index_ready_bitset_, field_id, true);
    index_has_raw_data_[field_id] = request.has_raw_data;
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

    LOG_INFO("LoadScalarIndex, fieldID:{}. segmentID:{}, is_pk:{}",
             info.field_id,
             id_,
             is_pk);
    // if segment is pk sorted, user created indexes bring no performance gain but extra memory usage
    if (is_pk && is_sorted_by_pk_) {
        LOG_INFO(
            "segment pk sorted, skip user index loading for primary key "
            "field");
        return;
    }

    std::unique_lock lck(mutex_);
    AssertInfo(
        !get_bit(index_ready_bitset_, field_id),
        "scalar index has been exist at " + std::to_string(field_id.get()));

    if (field_meta.get_data_type() == DataType::JSON) {
        auto path = info.index_params.at(JSON_PATH);
        if (auto it = info.index_params.find(index::INDEX_TYPE);
            it != info.index_params.end() &&
            it->second == index::NGRAM_INDEX_TYPE) {
            auto ngram_indexings = ngram_indexings_.wlock();
            if (ngram_indexings->find(field_id) == ngram_indexings->end()) {
                (*ngram_indexings)[field_id] =
                    std::unordered_map<std::string, index::CacheIndexBasePtr>();
            }
            (*ngram_indexings)[field_id][path] =
                std::move(const_cast<LoadIndexInfo&>(info).cache_index);
            return;
        } else {
            JsonIndex index;
            index.nested_path = path;
            index.field_id = field_id;
            index.index =
                std::move(const_cast<LoadIndexInfo&>(info).cache_index);
            index.cast_type =
                JsonCastType::FromString(info.index_params.at(JSON_CAST_TYPE));
            json_indices.wlock()->push_back(std::move(index));
            return;
        }
    }

    if (auto it = info.index_params.find(index::INDEX_TYPE);
        it != info.index_params.end() &&
        it->second == index::NGRAM_INDEX_TYPE) {
        auto [scalar_indexings, ngram_fields] =
            lock(folly::wlock(scalar_indexings_), folly::wlock(ngram_fields_));
        ngram_fields->insert(field_id);
        scalar_indexings->insert(
            {field_id,
             std::move(const_cast<LoadIndexInfo&>(info).cache_index)});
    } else {
        scalar_indexings_.wlock()->insert(
            {field_id,
             std::move(const_cast<LoadIndexInfo&>(info).cache_index)});
    }

    LoadResourceRequest request =
        milvus::index::IndexFactory::GetInstance().ScalarIndexLoadResource(
            field_meta.get_data_type(),
            info.index_engine_version,
            info.index_size,
            info.index_params,
            info.enable_mmap);

    set_bit(index_ready_bitset_, field_id, true);
    index_has_raw_data_[field_id] = request.has_raw_data;
    // release field column if the index contains raw data
    // only release non-primary field when in pk sorted mode
    if (request.has_raw_data && get_bit(field_data_ready_bitset_, field_id) &&
        !is_pk) {
        // We do not erase the primary key field: if insert record is evicted from memory, when reloading it'll
        // need the pk field again.
        fields_.rlock()->at(field_id)->ManualEvictCache();
    }
    LOG_INFO(
        "Has load scalar index done, fieldID:{}. segmentID:{}, has_raw_data:{}",
        info.field_id,
        id_,
        request.has_raw_data);
}

LoadIndexInfo
ChunkedSegmentSealedImpl::ConvertFieldIndexInfoToLoadIndexInfo(
    const milvus::proto::segcore::FieldIndexInfo* field_index_info) const {
    LoadIndexInfo load_index_info;

    load_index_info.segment_id = id_;
    // Extract field ID
    auto field_id = FieldId(field_index_info->fieldid());
    load_index_info.field_id = field_id.get();

    // Get field type from schema
    const auto& field_meta = get_schema()[field_id];
    load_index_info.field_type = field_meta.get_data_type();
    load_index_info.element_type = field_meta.get_element_type();

    // Set index metadata
    load_index_info.index_id = field_index_info->indexid();
    load_index_info.index_build_id = field_index_info->buildid();
    load_index_info.index_version = field_index_info->index_version();
    load_index_info.index_store_version =
        field_index_info->index_store_version();
    load_index_info.index_engine_version =
        static_cast<IndexVersion>(field_index_info->current_index_version());
    load_index_info.index_size = field_index_info->index_size();
    load_index_info.num_rows = field_index_info->num_rows();
    load_index_info.schema = field_meta.ToProto();

    // Copy index file paths, excluding indexParams file
    for (const auto& file_path : field_index_info->index_file_paths()) {
        size_t last_slash = file_path.find_last_of('/');
        std::string filename = (last_slash != std::string::npos)
                                   ? file_path.substr(last_slash + 1)
                                   : file_path;

        if (filename != "indexParams") {
            load_index_info.index_files.push_back(file_path);
        }
    }

    bool mmap_enabled = false;
    // Set index params
    for (const auto& kv_pair : field_index_info->index_params()) {
        if (kv_pair.key() == "mmap.enable") {
            std::string lower;
            std::transform(kv_pair.value().begin(),
                           kv_pair.value().end(),
                           std::back_inserter(lower),
                           ::tolower);
            mmap_enabled = lower == "true";
        }
        load_index_info.index_params[kv_pair.key()] = kv_pair.value();
    }

    size_t dim =
        IsVectorDataType(field_meta.get_data_type()) &&
                !IsSparseFloatVectorDataType(field_meta.get_data_type())
            ? field_meta.get_dim()
            : 1;
    load_index_info.dim = dim;
    auto remote_chunk_manager =
        milvus::storage::RemoteChunkManagerSingleton::GetInstance()
            .GetRemoteChunkManager();
    load_index_info.mmap_dir_path =
        milvus::storage::LocalChunkManagerSingleton::GetInstance()
            .GetChunkManager()
            ->GetRootPath();
    load_index_info.enable_mmap = mmap_enabled;

    return load_index_info;
}

void
ChunkedSegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& load_info) {
    switch (load_info.storage_version) {
        case 2: {
            load_column_group_data_internal(load_info);
            break;
        }
        default:
            load_field_data_internal(load_info);
            break;
    }
}

std::optional<ChunkedSegmentSealedImpl::ParquetStatistics>
parse_parquet_statistics(
    const std::vector<std::shared_ptr<parquet::FileMetaData>>& file_metas,
    const std::map<int64_t, milvus_storage::ColumnOffset>& field_id_mapping,
    int64_t field_id) {
    ChunkedSegmentSealedImpl::ParquetStatistics statistics;
    if (file_metas.size() == 0) {
        return std::nullopt;
    }
    auto it = field_id_mapping.find(field_id);
    AssertInfo(it != field_id_mapping.end(),
               "field id {} not found in field id mapping",
               field_id);
    auto offset = it->second;

    for (auto& file_meta : file_metas) {
        auto num_row_groups = file_meta->num_row_groups();
        for (auto i = 0; i < num_row_groups; i++) {
            auto row_group = file_meta->RowGroup(i);
            auto column_chunk = row_group->ColumnChunk(offset.col_index);
            if (!column_chunk->is_stats_set()) {
                AssertInfo(statistics.size() == 0,
                           "Statistics is not set for some column chunks "
                           "for field {}",
                           field_id);
                continue;
            }
            auto stats = column_chunk->statistics();
            statistics.push_back(stats);
        }
    }
    return statistics;
}

void
ChunkedSegmentSealedImpl::load_column_group_data_internal(
    const LoadFieldDataInfo& load_info) {
    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    ArrowSchemaPtr arrow_schema = schema_->ConvertToArrowSchema();

    for (auto& [id, info] : load_info.field_infos) {
        AssertInfo(info.row_count > 0,
                   "[StorageV2] The row count of field data is 0");

        auto column_group_id = FieldId(id);
        auto insert_files = info.insert_files;
        storage::SortByPath(insert_files);
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();

        milvus_storage::FieldIDList field_id_list;
        if (info.child_field_ids.size() == 0) {
            // legacy binlog meta, parse from reader
            field_id_list = storage::GetFieldIDList(
                column_group_id, insert_files[0], arrow_schema, fs);
        } else {
            field_id_list = milvus_storage::FieldIDList(info.child_field_ids);
        }

        // if multiple fields share same column group
        // hint for not loading certain field shall not be working for now
        // warmup will be disabled only when all columns are not in load list
        bool merged_in_load_list = false;
        std::vector<FieldId> milvus_field_ids;
        for (int i = 0; i < field_id_list.size(); ++i) {
            milvus_field_ids.push_back(FieldId(field_id_list.Get(i)));
            merged_in_load_list = merged_in_load_list ||
                                  schema_->ShouldLoadField(milvus_field_ids[i]);
        }

        auto mmap_dir_path =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager()
                ->GetRootPath();
        auto column_group_info = FieldDataInfo(column_group_id.get(),
                                               num_rows,
                                               mmap_dir_path,
                                               merged_in_load_list);
        LOG_INFO(
            "[StorageV2] segment {} loads column group {} with field ids "
            "{} "
            "with "
            "num_rows "
            "{} mmap_dir_path={}",
            this->get_segment_id(),
            column_group_id.get(),
            field_id_list.ToString(),
            num_rows,
            mmap_dir_path);

        auto field_metas = schema_->get_field_metas(milvus_field_ids);

        auto translator =
            std::make_unique<storagev2translator::GroupChunkTranslator>(
                get_segment_id(),
                field_metas,
                column_group_info,
                insert_files,
                info.enable_mmap,
                milvus_field_ids.size(),
                load_info.load_priority);

        auto file_metas = translator->parquet_file_metas();
        auto field_id_mapping = translator->field_id_mapping();
        auto chunked_column_group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));

        // Create ProxyChunkColumn for each field in this column group
        for (const auto& field_id : milvus_field_ids) {
            auto field_meta = field_metas.at(field_id);
            auto column = std::make_shared<ProxyChunkColumn>(
                chunked_column_group, field_id, field_meta);
            auto data_type = field_meta.get_data_type();
            std::optional<ParquetStatistics> statistics_opt;
            if (ENABLE_PARQUET_STATS_SKIP_INDEX) {
                statistics_opt = parse_parquet_statistics(
                    file_metas, field_id_mapping, field_id.get());
            }

            load_field_data_common(field_id,
                                   column,
                                   num_rows,
                                   data_type,
                                   info.enable_mmap,
                                   true,
                                   ENABLE_PARQUET_STATS_SKIP_INDEX
                                       ? statistics_opt
                                       : std::nullopt);
            if (field_id == TimestampFieldID) {
                auto timestamp_proxy_column = get_column(TimestampFieldID);
                AssertInfo(timestamp_proxy_column != nullptr,
                           "timestamp proxy column is nullptr");
                // TODO check timestamp_index ready instead of check system_ready_count_
                int64_t num_rows;
                for (auto& [_, info] : load_info.field_infos) {
                    num_rows = info.row_count;
                }
                auto all_ts_chunks =
                    timestamp_proxy_column->GetAllChunks(nullptr);
                std::vector<Timestamp> timestamps(num_rows);
                int64_t offset = 0;
                for (int i = 0; i < all_ts_chunks.size(); i++) {
                    auto chunk_data = all_ts_chunks[i].get();
                    auto fixed_chunk =
                        static_cast<FixedWidthChunk*>(chunk_data);
                    auto span = fixed_chunk->Span();
                    for (size_t j = 0; j < span.row_count(); j++) {
                        auto ts = *(int64_t*)((char*)span.data() +
                                              j * span.element_sizeof());
                        timestamps[offset++] = ts;
                    }
                }
                init_timestamp_index(timestamps, num_rows);
                system_ready_count_++;
                AssertInfo(offset == num_rows,
                           "[StorageV2] timestamp total row count {} not equal "
                           "to expected {}",
                           offset,
                           num_rows);
            }
        }

        if (column_group_id.get() == DEFAULT_SHORT_COLUMN_GROUP_ID) {
            stats_.mem_size += chunked_column_group->memory_size();
        }
    }
}

void
ChunkedSegmentSealedImpl::load_field_data_internal(
    const LoadFieldDataInfo& load_info) {
    SCOPE_CGO_CALL_METRIC();

    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    AssertInfo(
        !num_rows_.has_value() || num_rows_ == num_rows,
        "num_rows_ is set but not equal to num_rows of LoadFieldDataInfo");

    for (auto& [id, info] : load_info.field_infos) {
        AssertInfo(info.row_count > 0, "The row count of field data is 0");

        auto field_id = FieldId(id);

        auto mmap_dir_path =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager()
                ->GetRootPath();
        auto field_data_info =
            FieldDataInfo(field_id.get(),
                          num_rows,
                          mmap_dir_path,
                          schema_->ShouldLoadField(field_id));
        LOG_INFO("segment {} loads field {} with num_rows {}, sorted by pk {}",
                 this->get_segment_id(),
                 field_id.get(),
                 num_rows,
                 is_sorted_by_pk_);

        if (SystemProperty::Instance().IsSystem(field_id)) {
            auto insert_files = info.insert_files;
            storage::SortByPath(insert_files);
            auto parallel_degree = static_cast<uint64_t>(
                DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
            field_data_info.arrow_reader_channel->set_capacity(parallel_degree *
                                                               2);
            auto& pool =
                ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
            pool.Submit(LoadArrowReaderFromRemote,
                        insert_files,
                        field_data_info.arrow_reader_channel,
                        load_info.load_priority);

            LOG_INFO("segment {} submits load field {} task to thread pool",
                     this->get_segment_id(),
                     field_id.get());
            load_system_field_internal(field_id, field_data_info);
            LOG_INFO("segment {} loads system field {} mmap false done",
                     this->get_segment_id(),
                     field_id.get());
        } else {
            std::vector<storagev1translator::ChunkTranslator::FileInfo>
                file_infos;
            file_infos.reserve(info.insert_files.size());
            for (int i = 0; i < info.insert_files.size(); i++) {
                file_infos.emplace_back(
                    storagev1translator::ChunkTranslator::FileInfo{
                        info.insert_files[i],
                        info.entries_nums[i],
                        info.memory_sizes[i]});
            }

            storage::SortByPath(file_infos);

            auto field_meta = schema_->operator[](field_id);
            std::unique_ptr<Translator<milvus::Chunk>> translator =
                std::make_unique<storagev1translator::ChunkTranslator>(
                    this->get_segment_id(),
                    field_meta,
                    field_data_info,
                    std::move(file_infos),
                    info.enable_mmap,
                    load_info.load_priority);

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
    SCOPE_CGO_CALL_METRIC();

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
            auto chunk = create_chunk(field_meta, array_vec);
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
    SCOPE_CGO_CALL_METRIC();

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

int64_t
ChunkedSegmentSealedImpl::num_chunk_data(FieldId field_id) const {
    if (!get_bit(field_data_ready_bitset_, field_id)) {
        return 0;
    }
    auto column = get_column(field_id);
    return column ? column->num_chunks() : 1;
}

int64_t
ChunkedSegmentSealedImpl::num_chunk(FieldId field_id) const {
    if (!get_bit(field_data_ready_bitset_, field_id)) {
        return 1;
    }
    auto column = get_column(field_id);
    return column ? column->num_chunks() : 1;
}

int64_t
ChunkedSegmentSealedImpl::size_per_chunk() const {
    return get_row_count();
}

int64_t
ChunkedSegmentSealedImpl::chunk_size(FieldId field_id, int64_t chunk_id) const {
    if (!get_bit(field_data_ready_bitset_, field_id)) {
        return 0;
    }
    auto column = get_column(field_id);
    return column ? column->chunk_row_nums(chunk_id) : num_rows_.value();
}

std::pair<int64_t, int64_t>
ChunkedSegmentSealedImpl::get_chunk_by_offset(FieldId field_id,
                                              int64_t offset) const {
    auto column = get_column(field_id);
    AssertInfo(column != nullptr,
               "field {} must exist when getting chunk by offset",
               field_id.get());
    return column->GetChunkIDByOffset(offset);
}

int64_t
ChunkedSegmentSealedImpl::num_rows_until_chunk(FieldId field_id,
                                               int64_t chunk_id) const {
    auto column = get_column(field_id);
    AssertInfo(column != nullptr,
               "field {} must exist when getting rows until chunk",
               field_id.get());
    return column->GetNumRowsUntilChunk(chunk_id);
}

bool
ChunkedSegmentSealedImpl::is_mmap_field(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    return mmap_field_ids_.find(field_id) != mmap_field_ids_.end();
}

void
ChunkedSegmentSealedImpl::prefetch_chunks(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const std::vector<int64_t>& chunk_ids) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(field_id)) {
        column->PrefetchChunks(op_ctx, chunk_ids);
    }
}

PinWrapper<SpanBase>
ChunkedSegmentSealedImpl::chunk_data_impl(milvus::OpContext* op_ctx,
                                          FieldId field_id,
                                          int64_t chunk_id) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(field_id)) {
        return column->Span(op_ctx, chunk_id);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "chunk_data_impl only used for chunk column field ");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
ChunkedSegmentSealedImpl::chunk_array_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len =
        std::nullopt) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(field_id)) {
        return column->ArrayViews(op_ctx, chunk_id, offset_len);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "chunk_array_view_impl only used for chunk column field ");
}

PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
ChunkedSegmentSealedImpl::chunk_vector_array_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len =
        std::nullopt) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(field_id)) {
        return column->VectorArrayViews(op_ctx, chunk_id, offset_len);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "chunk_vector_array_view_impl only used for chunk column field ");
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
ChunkedSegmentSealedImpl::chunk_string_view_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len =
        std::nullopt) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(field_id)) {
        return column->StringViews(op_ctx, chunk_id, offset_len);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "chunk_string_view_impl only used for variable column field ");
}

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
ChunkedSegmentSealedImpl::chunk_string_views_by_offsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(field_id)) {
        return column->StringViewsByOffsets(op_ctx, chunk_id, offsets);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "chunk_view_by_offsets only used for variable column field ");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
ChunkedSegmentSealedImpl::chunk_array_views_by_offsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    std::shared_lock lck(mutex_);
    AssertInfo(get_bit(field_data_ready_bitset_, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(field_id)) {
        return column->ArrayViewsByOffsets(op_ctx, chunk_id, offsets);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "chunk_array_views_by_offsets only used for variable column "
              "field ");
}

PinWrapper<index::NgramInvertedIndex*>
ChunkedSegmentSealedImpl::GetNgramIndex(milvus::OpContext* op_ctx,
                                        FieldId field_id) const {
    std::shared_lock lck(mutex_);
    auto [scalar_indexings, ngram_fields] =
        lock(folly::rlock(scalar_indexings_), folly::rlock(ngram_fields_));

    auto has = ngram_fields->find(field_id);
    if (has == ngram_fields->end()) {
        return PinWrapper<index::NgramInvertedIndex*>(nullptr);
    }

    auto iter = scalar_indexings->find(field_id);
    if (iter == scalar_indexings->end()) {
        return PinWrapper<index::NgramInvertedIndex*>(nullptr);
    }
    auto slot = iter->second.get();
    lck.unlock();

    auto ca = SemiInlineGet(slot->PinCells(op_ctx, {0}));
    auto index = dynamic_cast<index::NgramInvertedIndex*>(ca->get_cell_of(0));
    AssertInfo(index != nullptr,
               "ngram index cache is corrupted, field_id: {}",
               field_id.get());
    return PinWrapper<index::NgramInvertedIndex*>(ca, index);
}

PinWrapper<index::NgramInvertedIndex*>
ChunkedSegmentSealedImpl::GetNgramIndexForJson(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const std::string& nested_path) const {
    std::shared_lock lck(mutex_);
    return ngram_indexings_.withRLock([&](auto& ngram_indexings) {
        auto iter = ngram_indexings.find(field_id);
        if (iter == ngram_indexings.end() ||
            iter->second.find(nested_path) == iter->second.end()) {
            return PinWrapper<index::NgramInvertedIndex*>(nullptr);
        }

        auto slot = iter->second.at(nested_path).get();

        auto ca = SemiInlineGet(slot->PinCells(op_ctx, {0}));
        auto index =
            dynamic_cast<index::NgramInvertedIndex*>(ca->get_cell_of(0));
        AssertInfo(index != nullptr,
                   "ngram index cache for json is corrupted, field_id: {}, "
                   "nested_path: {}",
                   field_id.get(),
                   nested_path);
        return PinWrapper<index::NgramInvertedIndex*>(ca, index);
    });
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
                                        const size_t* query_offsets,
                                        int64_t query_count,
                                        Timestamp timestamp,
                                        const BitsetView& bitset,
                                        milvus::OpContext* op_context,
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
                                   query_offsets,
                                   query_count,
                                   bitset,
                                   op_context,
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
                                   query_offsets,
                                   query_count,
                                   bitset,
                                   op_context,
                                   output);
        milvus::tracer::AddEvent("finish_searching_vector_index");
    } else {
        AssertInfo(
            get_bit(field_data_ready_bitset_, field_id),
            "Field Data is not loaded: " + std::to_string(field_id.get()));
        AssertInfo(num_rows_.has_value(), "Can't get row count value");
        auto row_count = num_rows_.value();
        auto vec_data = get_column(field_id);
        AssertInfo(
            vec_data != nullptr, "vector field {} not loaded", field_id.get());

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
                                    query_offsets,
                                    query_count,
                                    row_count,
                                    bitset,
                                    op_context,
                                    output);
        milvus::tracer::AddEvent("finish_searching_vector_data");
    }
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::get_vector(milvus::OpContext* op_ctx,
                                     FieldId field_id,
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
    auto cache_index = field_indexing->indexing_;
    auto ca = SemiInlineGet(cache_index->PinCells(op_ctx, {0}));
    auto vec_index = dynamic_cast<index::VectorIndex*>(ca->get_cell_of(0));
    AssertInfo(vec_index, "invalid vector indexing");

    auto index_type = vec_index->GetIndexType();
    auto metric_type = vec_index->GetMetricType();
    auto has_raw_data = vec_index->HasRawData();

    if (has_raw_data) {
        // If index has raw data, get vector from memory.
        auto ids_ds = GenIdsDataset(count, ids);
        if (field_meta.get_data_type() == DataType::VECTOR_SPARSE_U32_F32) {
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
        fields_.wlock()->erase(field_id);
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
    AssertInfo(!field_meta.is_vector(), "vector field cannot drop index");

    std::unique_lock lck(mutex_);
    auto [scalar_indexings, ngram_fields] =
        lock(folly::wlock(scalar_indexings_), folly::wlock(ngram_fields_));
    scalar_indexings->erase(field_id);
    ngram_fields->erase(field_id);

    set_bit(index_ready_bitset_, field_id, false);
}

void
ChunkedSegmentSealedImpl::DropJSONIndex(const FieldId field_id,
                                        const std::string& nested_path) {
    std::unique_lock lck(mutex_);
    json_indices.withWLock([&](auto& vec) {
        vec.erase(std::remove_if(vec.begin(),
                                 vec.end(),
                                 [field_id, nested_path](const auto& index) {
                                     return index.field_id == field_id &&
                                            index.nested_path == nested_path;
                                 }),
                  vec.end());
    });

    ngram_indexings_.withWLock([&](auto& ngram_indexings) {
        auto iter = ngram_indexings.find(field_id);
        if (iter != ngram_indexings.end()) {
            iter->second.erase(nested_path);
            if (iter->second.empty()) {
                ngram_indexings.erase(iter);
            }
        }
    });
}

void
ChunkedSegmentSealedImpl::check_search(const query::Plan* plan) const {
    AssertInfo(plan, "Search plan is null");
    AssertInfo(plan->extra_info_opt_.has_value(),
               "Extra info of search plan doesn't have value");

    if (!is_system_field_ready()) {
        ThrowInfo(FieldNotLoaded,
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
        auto& field_meta = plan->schema_->operator[](field_id);
        // request field may has added field
        if (!field_meta.is_nullable()) {
            ThrowInfo(FieldNotLoaded,
                      "User Field(" + field_meta.get_name().get() +
                          ") is not loaded");
        }
    }
}

void
ChunkedSegmentSealedImpl::search_pks(BitsetType& bitset,
                                     const std::vector<PkType>& pks) const {
    BitsetTypeView bitset_view(bitset);
    if (!is_sorted_by_pk_) {
        for (auto& pk : pks) {
            insert_record_.search_pk_range(
                pk, proto::plan::OpType::Equal, bitset_view);
        }
        return;
    }

    auto pk_field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = get_column(pk_field_id);
    AssertInfo(pk_column != nullptr, "primary key column not loaded");

    auto all_chunk_pins = pk_column->GetAllChunks(nullptr);
    switch (schema_->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64: {
            auto num_chunk = pk_column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                auto pw = all_chunk_pins[i];
                auto src =
                    reinterpret_cast<const int64_t*>(pw.get()->RawData());
                auto chunk_row_num = pk_column->chunk_row_nums(i);
                for (size_t j = 0; j < pks.size(); j++) {
                    // get int64 pks
                    auto target = std::get<int64_t>(pks[j]);
                    auto it = std::lower_bound(
                        src,
                        src + chunk_row_num,
                        target,
                        [](const int64_t& elem, const int64_t& value) {
                            return elem < value;
                        });
                    auto num_rows_until_chunk =
                        pk_column->GetNumRowsUntilChunk(i);
                    for (; it != src + chunk_row_num && *it == target; ++it) {
                        auto offset = it - src + num_rows_until_chunk;
                        bitset[offset] = true;
                    }
                }
            }

            break;
        }
        case DataType::VARCHAR: {
            auto num_chunk = pk_column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                // TODO @xiaocai2333, @sunby: chunk need to record the min/max.
                auto num_rows_until_chunk = pk_column->GetNumRowsUntilChunk(i);
                auto pw = all_chunk_pins[i];
                auto string_chunk = static_cast<StringChunk*>(pw.get());
                for (size_t j = 0; j < pks.size(); ++j) {
                    // get varchar pks
                    auto& target = std::get<std::string>(pks[j]);
                    auto offset = string_chunk->binary_search_string(target);
                    for (; offset != -1 && offset < string_chunk->RowNums() &&
                           string_chunk->operator[](offset) == target;
                         ++offset) {
                        auto segment_offset = offset + num_rows_until_chunk;
                        bitset[segment_offset] = true;
                    }
                }
            }
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported type {}",
                    schema_->get_fields().at(pk_field_id).get_data_type()));
        }
    }
}

void
ChunkedSegmentSealedImpl::search_batch_pks(
    const std::vector<PkType>& pks,
    const std::function<Timestamp(const size_t idx)>& get_timestamp,
    bool include_same_ts,
    const std::function<void(const SegOffset offset, const Timestamp ts)>&
        callback) const {
    // handle unsorted case
    if (!is_sorted_by_pk_) {
        for (size_t i = 0; i < pks.size(); i++) {
            auto timestamp = get_timestamp(i);
            auto offsets =
                insert_record_.search_pk(pks[i], timestamp, include_same_ts);
            for (auto offset : offsets) {
                callback(offset, timestamp);
            }
        }
        return;
    }

    auto pk_field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = get_column(pk_field_id);
    AssertInfo(pk_column != nullptr, "primary key column not loaded");

    auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

    auto timestamp_hit = include_same_ts
                             ? [](const Timestamp& ts1,
                                  const Timestamp& ts2) { return ts1 <= ts2; }
                             : [](const Timestamp& ts1, const Timestamp& ts2) {
                                   return ts1 < ts2;
                               };

    switch (schema_->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64: {
            auto num_chunk = pk_column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                auto pw = all_chunk_pins[i];
                auto src =
                    reinterpret_cast<const int64_t*>(pw.get()->RawData());
                auto chunk_row_num = pk_column->chunk_row_nums(i);
                for (size_t j = 0; j < pks.size(); j++) {
                    // get int64 pks
                    auto target = std::get<int64_t>(pks[j]);
                    auto timestamp = get_timestamp(j);
                    auto it = std::lower_bound(
                        src,
                        src + chunk_row_num,
                        target,
                        [](const int64_t& elem, const int64_t& value) {
                            return elem < value;
                        });
                    auto num_rows_until_chunk =
                        pk_column->GetNumRowsUntilChunk(i);
                    for (; it != src + chunk_row_num && *it == target; ++it) {
                        auto offset = it - src + num_rows_until_chunk;
                        if (timestamp_hit(insert_record_.timestamps_[offset],
                                          timestamp)) {
                            callback(SegOffset(offset), timestamp);
                        }
                    }
                }
            }

            break;
        }
        case DataType::VARCHAR: {
            auto num_chunk = pk_column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                // TODO @xiaocai2333, @sunby: chunk need to record the min/max.
                auto num_rows_until_chunk = pk_column->GetNumRowsUntilChunk(i);
                auto pw = all_chunk_pins[i];
                auto string_chunk = static_cast<StringChunk*>(pw.get());
                for (size_t j = 0; j < pks.size(); ++j) {
                    // get varchar pks
                    auto& target = std::get<std::string>(pks[j]);
                    auto timestamp = get_timestamp(j);
                    auto offset = string_chunk->binary_search_string(target);
                    for (; offset != -1 && offset < string_chunk->RowNums() &&
                           string_chunk->operator[](offset) == target;
                         ++offset) {
                        auto segment_offset = offset + num_rows_until_chunk;
                        if (timestamp_hit(
                                insert_record_.timestamps_[segment_offset],
                                timestamp)) {
                            callback(SegOffset(segment_offset), timestamp);
                        }
                    }
                }
            }
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported type {}",
                    schema_->get_fields().at(pk_field_id).get_data_type()));
        }
    }
}

void
ChunkedSegmentSealedImpl::pk_range(milvus::OpContext* op_ctx,
                                   proto::plan::OpType op,
                                   const PkType& pk,
                                   BitsetTypeView& bitset) const {
    if (!is_sorted_by_pk_) {
        insert_record_.search_pk_range(pk, op, bitset);
        return;
    }

    search_sorted_pk_range(op_ctx, op, pk, bitset);
}

void
ChunkedSegmentSealedImpl::search_sorted_pk_range(milvus::OpContext* op_ctx,
                                                 proto::plan::OpType op,
                                                 const PkType& pk,
                                                 BitsetTypeView& bitset) const {
    auto pk_field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = get_column(pk_field_id);
    AssertInfo(pk_column != nullptr, "primary key column not loaded");

    switch (schema_->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64: {
            // get int64 pks
            auto target = std::get<int64_t>(pk);

            auto num_chunk = pk_column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                auto pw = pk_column->DataOfChunk(op_ctx, i);
                auto src = reinterpret_cast<const int64_t*>(pw.get());
                auto chunk_row_num = pk_column->chunk_row_nums(i);
                if (op == proto::plan::OpType::GreaterEqual) {
                    auto it = std::lower_bound(
                        src,
                        src + chunk_row_num,
                        target,
                        [](const int64_t& elem, const int64_t& value) {
                            return elem < value;
                        });
                    auto num_rows_until_chunk =
                        pk_column->GetNumRowsUntilChunk(i);
                    for (; it != src + chunk_row_num; ++it) {
                        auto offset = it - src + num_rows_until_chunk;
                        bitset[offset] = true;
                    }
                } else if (op == proto::plan::OpType::GreaterThan) {
                    auto it = std::upper_bound(
                        src,
                        src + chunk_row_num,
                        target,
                        [](const int64_t& elem, const int64_t& value) {
                            return elem < value;
                        });
                    auto num_rows_until_chunk =
                        pk_column->GetNumRowsUntilChunk(i);
                    for (; it != src + chunk_row_num; ++it) {
                        auto offset = it - src + num_rows_until_chunk;
                        bitset[offset] = true;
                    }
                } else if (op == proto::plan::OpType::LessEqual) {
                    auto it = std::upper_bound(
                        src,
                        src + chunk_row_num,
                        target,
                        [](const int64_t& elem, const int64_t& value) {
                            return elem < value;
                        });
                    if (it == src) {
                        break;
                    }
                    auto num_rows_until_chunk =
                        pk_column->GetNumRowsUntilChunk(i);
                    for (auto ptr = src; ptr < it; ++ptr) {
                        auto offset = ptr - src + num_rows_until_chunk;
                        bitset[offset] = true;
                    }
                } else if (op == proto::plan::OpType::LessThan) {
                    auto it =
                        std::lower_bound(src, src + chunk_row_num, target);
                    if (it == src) {
                        break;
                    }
                    auto num_rows_until_chunk =
                        pk_column->GetNumRowsUntilChunk(i);
                    for (auto ptr = src; ptr < it; ++ptr) {
                        auto offset = ptr - src + num_rows_until_chunk;
                        bitset[offset] = true;
                    }
                } else if (op == proto::plan::OpType::Equal) {
                    auto it = std::lower_bound(
                        src,
                        src + chunk_row_num,
                        target,
                        [](const int64_t& elem, const int64_t& value) {
                            return elem < value;
                        });
                    auto num_rows_until_chunk =
                        pk_column->GetNumRowsUntilChunk(i);
                    for (; it != src + chunk_row_num && *it == target; ++it) {
                        auto offset = it - src + num_rows_until_chunk;
                        bitset[offset] = true;
                    }
                    if (it != src + chunk_row_num && *it > target) {
                        break;
                    }
                } else {
                    ThrowInfo(ErrorCode::Unsupported,
                              fmt::format("unsupported op type {}", op));
                }
            }
            break;
        }
        case DataType::VARCHAR: {
            // get varchar pks
            auto target = std::get<std::string>(pk);

            auto num_chunk = pk_column->num_chunks();
            for (int i = 0; i < num_chunk; ++i) {
                auto num_rows_until_chunk = pk_column->GetNumRowsUntilChunk(i);
                auto pw = pk_column->GetChunk(op_ctx, i);
                auto string_chunk = static_cast<StringChunk*>(pw.get());

                if (op == proto::plan::OpType::Equal) {
                    auto offset = string_chunk->lower_bound_string(target);
                    for (; offset < string_chunk->RowNums() &&
                           string_chunk->operator[](offset) == target;
                         ++offset) {
                        auto segment_offset = offset + num_rows_until_chunk;
                        bitset[segment_offset] = true;
                    }
                    if (offset < string_chunk->RowNums() &&
                        string_chunk->operator[](offset) > target) {
                        break;
                    }
                } else if (op == proto::plan::OpType::GreaterEqual) {
                    auto offset = string_chunk->lower_bound_string(target);
                    for (; offset < string_chunk->RowNums(); ++offset) {
                        auto segment_offset = offset + num_rows_until_chunk;
                        bitset[segment_offset] = true;
                    }
                } else if (op == proto::plan::OpType::GreaterThan) {
                    auto offset = string_chunk->upper_bound_string(target);
                    for (; offset < string_chunk->RowNums(); ++offset) {
                        auto segment_offset = offset + num_rows_until_chunk;
                        bitset[segment_offset] = true;
                    }
                } else if (op == proto::plan::OpType::LessEqual) {
                    auto pos = string_chunk->upper_bound_string(target);
                    if (pos == 0) {
                        break;
                    }
                    for (auto offset = 0; offset < pos; ++offset) {
                        auto segment_offset = offset + num_rows_until_chunk;
                        bitset[segment_offset] = true;
                    }
                } else if (op == proto::plan::OpType::LessThan) {
                    auto pos = string_chunk->lower_bound_string(target);
                    if (pos == 0) {
                        break;
                    }
                    for (auto offset = 0; offset < pos; ++offset) {
                        auto segment_offset = offset + num_rows_until_chunk;
                        bitset[segment_offset] = true;
                    }
                } else {
                    ThrowInfo(ErrorCode::Unsupported,
                              fmt::format("unsupported op type {}", op));
                }
            }
            break;
        }
        default: {
            ThrowInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported type {}",
                    schema_->get_fields().at(pk_field_id).get_data_type()));
        }
    }
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
    bool is_sorted_by_pk)
    : segcore_config_(segcore_config),
      field_data_ready_bitset_(schema->size()),
      index_ready_bitset_(schema->size()),
      binlog_index_bitset_(schema->size()),
      ngram_fields_(std::unordered_set<FieldId>(schema->size())),
      scalar_indexings_(std::unordered_map<FieldId, index::CacheIndexBasePtr>(
          schema->size())),
      insert_record_(*schema, MAX_ROW_COUNT),
      schema_(schema),
      id_(segment_id),
      col_index_meta_(index_meta),
      is_sorted_by_pk_(is_sorted_by_pk),
      deleted_record_(
          &insert_record_,
          [this](const std::vector<PkType>& pks,
                 const Timestamp* timestamps,
                 std::function<void(const SegOffset offset, const Timestamp ts)>
                     callback) {
              this->search_batch_pks(
                  pks,
                  [&](const size_t idx) { return timestamps[idx]; },
                  false,
                  callback);
          },
          segment_id) {
    auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
    mmap_descriptor_ = mcm->Register();
}

ChunkedSegmentSealedImpl::~ChunkedSegmentSealedImpl() {
    // Clean up geometry cache for all fields in this segment
    auto& cache_manager = milvus::exec::SimpleGeometryCacheManager::Instance();
    cache_manager.RemoveSegmentCaches(ctx_, get_segment_id());

    if (ctx_) {
        GEOS_finish_r(ctx_);
        ctx_ = nullptr;
    }

    if (mmap_descriptor_ != nullptr) {
        auto mm = storage::MmapManager::GetInstance().GetMmapChunkManager();
        mm->UnRegister(mmap_descriptor_);
    }
}

void
ChunkedSegmentSealedImpl::bulk_subscript(milvus::OpContext* op_ctx,
                                         SystemFieldType system_type,
                                         const int64_t* seg_offsets,
                                         int64_t count,
                                         void* output) const {
    AssertInfo(is_system_field_ready(),
               "System field isn't ready when do bulk_insert, segID:{}",
               id_);
    switch (system_type) {
        case SystemFieldType::Timestamp:
            AssertInfo(insert_record_.timestamps_.num_chunk() == 1,
                       "num chunk of timestamp not equal to 1 for "
                       "sealed segment");
            bulk_subscript_impl<Timestamp>(
                op_ctx,
                this->insert_record_.timestamps_.get_chunk_data(0),
                seg_offsets,
                count,
                static_cast<Timestamp*>(output));
            break;
        case SystemFieldType::RowId:
            ThrowInfo(ErrorCode::Unsupported, "RowId retrieve not supported");
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unknown subscript fields", system_type));
    }
}

template <typename S, typename T>
void
ChunkedSegmentSealedImpl::bulk_subscript_impl(milvus::OpContext* op_ctx,
                                              const void* src_raw,
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
ChunkedSegmentSealedImpl::bulk_subscript_impl(milvus::OpContext* op_ctx,
                                              ChunkedColumnInterface* field,
                                              const int64_t* seg_offsets,
                                              int64_t count,
                                              T* dst) {
    static_assert(std::is_fundamental_v<S> && std::is_fundamental_v<T>);
    // use field->data_type_ to determine the type of dst
    field->BulkPrimitiveValueAt(
        op_ctx, static_cast<void*>(dst), seg_offsets, count);
}

// for dense vector
void
ChunkedSegmentSealedImpl::bulk_subscript_impl(milvus::OpContext* op_ctx,
                                              int64_t element_sizeof,
                                              ChunkedColumnInterface* field,
                                              const int64_t* seg_offsets,
                                              int64_t count,
                                              void* dst_raw) {
    auto dst_vec = reinterpret_cast<char*>(dst_raw);
    field->BulkVectorValueAt(
        op_ctx, dst_vec, seg_offsets, element_sizeof, count);
}

template <typename S>
void
ChunkedSegmentSealedImpl::bulk_subscript_ptr_impl(
    milvus::OpContext* op_ctx,
    ChunkedColumnInterface* column,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<std::string>* dst) {
    if constexpr (std::is_same_v<S, Json>) {
        column->BulkRawJsonAt(
            op_ctx,
            [&](Json json, size_t offset, bool is_valid) {
                dst->at(offset) = std::move(std::string(json.data()));
            },
            seg_offsets,
            count);
    } else {
        static_assert(std::is_same_v<S, std::string>);
        column->BulkRawStringAt(
            op_ctx,
            [dst](std::string_view value, size_t offset, bool is_valid) {
                dst->at(offset) = std::move(std::string(value));
            },
            seg_offsets,
            count);
    }
}

template <typename T>
void
ChunkedSegmentSealedImpl::bulk_subscript_array_impl(
    milvus::OpContext* op_ctx,
    ChunkedColumnInterface* column,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) {
    column->BulkArrayAt(
        op_ctx,
        [dst](ScalarFieldProto&& array, size_t i) {
            dst->at(i) = std::move(array);
        },
        seg_offsets,
        count);
}

template <typename T>
void
ChunkedSegmentSealedImpl::bulk_subscript_vector_array_impl(
    milvus::OpContext* op_ctx,
    const ChunkedColumnInterface* column,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<T>* dst) {
    column->BulkVectorArrayAt(
        op_ctx,
        [dst](VectorFieldProto&& array, size_t i) {
            dst->at(i) = std::move(array);
        },
        seg_offsets,
        count);
}

void
ChunkedSegmentSealedImpl::ClearData() {
    {
        std::unique_lock lck(mutex_);
        field_data_ready_bitset_.reset();
        index_ready_bitset_.reset();
        binlog_index_bitset_.reset();
        index_has_raw_data_.clear();
        system_ready_count_ = 0;
        num_rows_ = std::nullopt;
        ngram_fields_.wlock()->clear();
        scalar_indexings_.wlock()->clear();
        vector_indexings_.clear();
        ngram_indexings_.wlock()->clear();
        insert_record_.clear();
        fields_.wlock()->clear();
        variable_fields_avg_size_.clear();
        stats_.mem_size = 0;
    }
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::fill_with_empty(FieldId field_id,
                                          int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    if (IsVectorDataType(field_meta.get_data_type())) {
        return CreateEmptyVectorDataArray(count, field_meta);
    }
    return CreateEmptyScalarDataArray(count, field_meta);
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
        auto column = get_column(field_id);
        if (column) {
            column->BulkRawStringAt(
                nullptr,
                [&](std::string_view value, size_t offset, bool is_valid) {
                    index->AddTextSealed(std::string(value), is_valid, offset);
                });
        } else {  // fetch raw data from index.
            auto field_index_iter =
                scalar_indexings_.withRLock([&](auto& mapping) {
                    auto iter = mapping.find(field_id);
                    AssertInfo(iter != mapping.end(),
                               "failed to create text index, neither "
                               "raw data nor "
                               "index are found");
                    return iter;
                });
            auto accessor =
                SemiInlineGet(field_index_iter->second->PinCells(nullptr, {0}));
            auto ptr = accessor->get_cell_of(0);
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
                    index->AddNullSealed(i);
                }
                index->AddTextSealed(raw.value(), true, i);
            }
        }
    }

    // create index reader.
    index->CreateReader(milvus::index::SetBitsetSealed);
    // release index writer.
    index->Finish();

    index->Reload();

    index->RegisterTokenizer("milvus_tokenizer",
                             field_meta.get_analyzer_params().c_str());

    text_indexes_[field_id] = std::make_shared<index::TextMatchIndexHolder>(
        std::move(index), cfg.GetScalarIndexEnableMmap());
}

void
ChunkedSegmentSealedImpl::LoadTextIndex(
    std::unique_ptr<milvus::proto::indexcgo::LoadTextIndexInfo> info_proto) {
    std::unique_lock lck(mutex_);

    milvus::storage::FieldDataMeta field_data_meta{info_proto->collectionid(),
                                                   info_proto->partitionid(),
                                                   this->get_segment_id(),
                                                   info_proto->fieldid(),
                                                   info_proto->schema()};
    milvus::storage::IndexMeta index_meta{this->get_segment_id(),
                                          info_proto->fieldid(),
                                          info_proto->buildid(),
                                          info_proto->version()};
    auto remote_chunk_manager =
        milvus::storage::RemoteChunkManagerSingleton::GetInstance()
            .GetRemoteChunkManager();
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();
    AssertInfo(fs != nullptr, "arrow file system is null");

    milvus::Config config;
    std::vector<std::string> files;
    for (const auto& f : info_proto->files()) {
        files.push_back(f);
    }
    config[milvus::index::INDEX_FILES] = files;
    config[milvus::LOAD_PRIORITY] = info_proto->load_priority();
    config[milvus::index::ENABLE_MMAP] = info_proto->enable_mmap();
    milvus::storage::FileManagerContext file_ctx(
        field_data_meta, index_meta, remote_chunk_manager, fs);

    auto field_id = milvus::FieldId(info_proto->fieldid());
    const auto& field_meta = schema_->operator[](field_id);
    milvus::segcore::storagev1translator::TextMatchIndexLoadInfo load_info{
        info_proto->enable_mmap(),
        this->get_segment_id(),
        info_proto->fieldid(),
        field_meta.get_analyzer_params(),
        info_proto->index_size()};

    std::unique_ptr<
        milvus::cachinglayer::Translator<milvus::index::TextMatchIndex>>
        translator = std::make_unique<
            milvus::segcore::storagev1translator::TextMatchIndexTranslator>(
            load_info, file_ctx, config);
    auto cache_slot =
        milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
            std::move(translator));
    text_indexes_[field_id] = std::move(cache_slot);
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::get_raw_data(milvus::OpContext* op_ctx,
                                       FieldId field_id,
                                       const FieldMeta& field_meta,
                                       const int64_t* seg_offsets,
                                       int64_t count) const {
    // DO NOT directly access the column by map like: `fields_.at(field_id)->Data()`,
    // we have to clone the shared pointer,
    // to make sure it won't get released if segment released
    auto column = get_column(field_id);
    AssertInfo(column != nullptr,
               "field {} must exist when getting raw data",
               field_id.get());
    auto ret = fill_with_empty(field_id, count);
    if (column->IsNullable()) {
        auto dst = ret->mutable_valid_data()->mutable_data();
        column->BulkIsValid(
            op_ctx,
            [&](bool is_valid, size_t offset) { dst[offset] = is_valid; },
            seg_offsets,
            count);
    }
    switch (field_meta.get_data_type()) {
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TEXT: {
            bulk_subscript_ptr_impl<std::string>(
                op_ctx,
                column.get(),
                seg_offsets,
                count,
                ret->mutable_scalars()->mutable_string_data()->mutable_data());
            break;
        }

        case DataType::JSON: {
            bulk_subscript_ptr_impl<Json>(
                op_ctx,
                column.get(),
                seg_offsets,
                count,
                ret->mutable_scalars()->mutable_json_data()->mutable_data());
            break;
        }

        case DataType::GEOMETRY: {
            bulk_subscript_ptr_impl<std::string>(op_ctx,
                                                 column.get(),
                                                 seg_offsets,
                                                 count,
                                                 ret->mutable_scalars()
                                                     ->mutable_geometry_data()
                                                     ->mutable_data());
            break;
        }

        case DataType::ARRAY: {
            bulk_subscript_array_impl(
                op_ctx,
                column.get(),
                seg_offsets,
                count,
                ret->mutable_scalars()->mutable_array_data()->mutable_data());
            break;
        }

        case DataType::BOOL: {
            bulk_subscript_impl<bool, bool>(op_ctx,
                                            column.get(),
                                            seg_offsets,
                                            count,
                                            ret->mutable_scalars()
                                                ->mutable_bool_data()
                                                ->mutable_data()
                                                ->mutable_data());
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t, int32_t>(op_ctx,
                                                 column.get(),
                                                 seg_offsets,
                                                 count,
                                                 ret->mutable_scalars()
                                                     ->mutable_int_data()
                                                     ->mutable_data()
                                                     ->mutable_data());
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t, int32_t>(op_ctx,
                                                  column.get(),
                                                  seg_offsets,
                                                  count,
                                                  ret->mutable_scalars()
                                                      ->mutable_int_data()
                                                      ->mutable_data()
                                                      ->mutable_data());
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t, int32_t>(op_ctx,
                                                  column.get(),
                                                  seg_offsets,
                                                  count,
                                                  ret->mutable_scalars()
                                                      ->mutable_int_data()
                                                      ->mutable_data()
                                                      ->mutable_data());
            break;
        }
        case DataType::INT64: {
            bulk_subscript_impl<int64_t, int64_t>(op_ctx,
                                                  column.get(),
                                                  seg_offsets,
                                                  count,
                                                  ret->mutable_scalars()
                                                      ->mutable_long_data()
                                                      ->mutable_data()
                                                      ->mutable_data());
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float, float>(op_ctx,
                                              column.get(),
                                              seg_offsets,
                                              count,
                                              ret->mutable_scalars()
                                                  ->mutable_float_data()
                                                  ->mutable_data()
                                                  ->mutable_data());
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double, double>(op_ctx,
                                                column.get(),
                                                seg_offsets,
                                                count,
                                                ret->mutable_scalars()
                                                    ->mutable_double_data()
                                                    ->mutable_data()
                                                    ->mutable_data());
            break;
        }
        case DataType::TIMESTAMPTZ: {
            bulk_subscript_impl<int64_t>(op_ctx,
                                         column.get(),
                                         seg_offsets,
                                         count,
                                         ret->mutable_scalars()
                                             ->mutable_timestamptz_data()
                                             ->mutable_data()
                                             ->mutable_data());
            break;
        }
        case DataType::VECTOR_FLOAT: {
            bulk_subscript_impl(op_ctx,
                                field_meta.get_sizeof(),
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
                op_ctx,
                field_meta.get_sizeof(),
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_float16_vector()->data());
            break;
        }
        case DataType::VECTOR_BFLOAT16: {
            bulk_subscript_impl(
                op_ctx,
                field_meta.get_sizeof(),
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_bfloat16_vector()->data());
            break;
        }
        case DataType::VECTOR_BINARY: {
            bulk_subscript_impl(
                op_ctx,
                field_meta.get_sizeof(),
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_binary_vector()->data());
            break;
        }
        case DataType::VECTOR_INT8: {
            bulk_subscript_impl(
                op_ctx,
                field_meta.get_sizeof(),
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_int8_vector()->data());
            break;
        }
        case DataType::VECTOR_SPARSE_U32_F32: {
            auto dst = ret->mutable_vectors()->mutable_sparse_float_vector();
            int64_t max_dim = 0;
            column->BulkValueAt(
                op_ctx,
                [&](const char* value, size_t i) mutable {
                    auto offset = seg_offsets[i];
                    auto row =
                        offset != INVALID_SEG_OFFSET
                            ? static_cast<const knowhere::sparse::SparseRow<
                                  SparseValueType>*>(
                                  static_cast<const void*>(value))
                            : nullptr;
                    if (row == nullptr) {
                        dst->add_contents();
                        return;
                    }
                    max_dim = std::max(max_dim, row->dim());
                    dst->add_contents(row->data(), row->data_byte_size());
                },
                seg_offsets,
                count);
            dst->set_dim(max_dim);
            ret->mutable_vectors()->set_dim(dst->dim());
            break;
        }
        case DataType::VECTOR_ARRAY: {
            bulk_subscript_vector_array_impl(
                op_ctx,
                column.get(),
                seg_offsets,
                count,
                ret->mutable_vectors()->mutable_vector_array()->mutable_data());
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported data type {}",
                                  field_meta.get_data_type()));
        }
    }
    return ret;
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::bulk_subscript(milvus::OpContext* op_ctx,
                                         FieldId field_id,
                                         const int64_t* seg_offsets,
                                         int64_t count) const {
    auto& field_meta = schema_->operator[](field_id);
    // if count == 0, return empty data array
    if (count == 0) {
        return fill_with_empty(field_id, count);
    }

    // hold field shared_ptr here, preventing field got destroyed
    auto [field, exist] = GetFieldDataIfExist(field_id);
    if (exist) {
        Assert(get_bit(field_data_ready_bitset_, field_id));
        return get_raw_data(op_ctx, field_id, field_meta, seg_offsets, count);
    }

    PinWrapper<const index::IndexBase*> pin_scalar_index_ptr;
    auto scalar_indexes = PinIndex(op_ctx, field_id);
    if (!scalar_indexes.empty()) {
        pin_scalar_index_ptr = std::move(scalar_indexes[0]);
    }

    auto index_has_raw = HasRawData(field_id.get());

    if (!IsVectorDataType(field_meta.get_data_type())) {
        // if field has load scalar index, reverse raw data from index
        if (index_has_raw) {
            return ReverseDataFromIndex(
                pin_scalar_index_ptr.get(), seg_offsets, count, field_meta);
        }
        return get_raw_data(op_ctx, field_id, field_meta, seg_offsets, count);
    }

    std::chrono::high_resolution_clock::time_point get_vector_start =
        std::chrono::high_resolution_clock::now();

    std::unique_ptr<DataArray> vector{nullptr};
    if (index_has_raw) {
        vector = get_vector(op_ctx, field_id, seg_offsets, count);
    } else {
        vector = get_raw_data(op_ctx, field_id, field_meta, seg_offsets, count);
    }

    std::chrono::high_resolution_clock::time_point get_vector_end =
        std::chrono::high_resolution_clock::now();
    double get_vector_cost = std::chrono::duration<double, std::micro>(
                                 get_vector_end - get_vector_start)
                                 .count();
    milvus::monitor::internal_core_get_vector_latency.Observe(get_vector_cost /
                                                              1000);

    return vector;
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::bulk_subscript(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const int64_t* seg_offsets,
    int64_t count,
    const std::vector<std::string>& dynamic_field_names) const {
    Assert(!dynamic_field_names.empty());
    if (count == 0) {
        return fill_with_empty(field_id, 0);
    }

    auto column = get_column(field_id);
    AssertInfo(column != nullptr,
               "json field {} must exist when bulk_subscript",
               field_id.get());
    auto ret = fill_with_empty(field_id, count);
    if (column->IsNullable()) {
        auto dst = ret->mutable_valid_data()->mutable_data();
        column->BulkIsValid(
            op_ctx,
            [&](bool is_valid, size_t offset) { dst[offset] = is_valid; },
            seg_offsets,
            count);
    }
    auto dst = ret->mutable_scalars()->mutable_json_data()->mutable_data();
    column->BulkRawJsonAt(
        op_ctx,
        [&](Json json, size_t offset, bool is_valid) {
            dst->at(offset) =
                ExtractSubJson(std::string(json.data()), dynamic_field_names);
        },
        seg_offsets,
        count);
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

std::pair<std::shared_ptr<ChunkedColumnInterface>, bool>
ChunkedSegmentSealedImpl::GetFieldDataIfExist(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    bool exists;
    if (SystemProperty::Instance().IsSystem(field_id)) {
        exists = is_system_field_ready();
    } else {
        exists = get_bit(field_data_ready_bitset_, field_id);
    }
    if (!exists) {
        return {nullptr, false};
    }
    auto column = get_column(field_id);
    AssertInfo(column != nullptr,
               "field {} must exist if bitset is set",
               field_id.get());
    return {column, exists};
}

bool
ChunkedSegmentSealedImpl::HasRawData(int64_t field_id) const {
    std::shared_lock lck(mutex_);
    auto fieldID = FieldId(field_id);
    const auto& field_meta = schema_->operator[](fieldID);
    if (IsVectorDataType(field_meta.get_data_type())) {
        if (get_bit(index_ready_bitset_, fieldID)) {
            AssertInfo(vector_indexings_.is_ready(fieldID),
                       "vector index is not ready");
            AssertInfo(
                index_has_raw_data_.find(fieldID) != index_has_raw_data_.end(),
                "index_has_raw_data_ is not set for fieldID: " +
                    std::to_string(fieldID.get()));
            return index_has_raw_data_.at(fieldID);
        } else if (get_bit(binlog_index_bitset_, fieldID)) {
            AssertInfo(vector_indexings_.is_ready(fieldID),
                       "interim index is not ready");
            AssertInfo(
                index_has_raw_data_.find(fieldID) != index_has_raw_data_.end(),
                "index_has_raw_data_ is not set for fieldID: " +
                    std::to_string(fieldID.get()));
            return index_has_raw_data_.at(fieldID) ||
                   get_bit(field_data_ready_bitset_, fieldID);
        }
    } else if (IsJsonDataType(field_meta.get_data_type())) {
        return get_bit(field_data_ready_bitset_, fieldID);
    } else {
        auto has_scalar_index = scalar_indexings_.withRLock([&](auto& mapping) {
            return mapping.find(fieldID) != mapping.end();
        });
        if (has_scalar_index) {
            AssertInfo(
                index_has_raw_data_.find(fieldID) != index_has_raw_data_.end(),
                "index_has_raw_data_ is not set for fieldID: " +
                    std::to_string(fieldID.get()));
            return index_has_raw_data_.at(fieldID);
        }
    }
    return true;
}

DataType
ChunkedSegmentSealedImpl::GetFieldDataType(milvus::FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    return field_meta.get_data_type();
}

void
ChunkedSegmentSealedImpl::search_ids(BitsetType& bitset,
                                     const IdArray& id_array) const {
    auto field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_->operator[](field_id);
    auto data_type = field_meta.get_data_type();
    auto ids_size = GetSizeOfIdArray(id_array);
    std::vector<PkType> pks(ids_size);
    ParsePksFromIDs(pks, data_type, id_array);

    this->search_pks(bitset, pks);
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

void
ChunkedSegmentSealedImpl::LoadSegmentMeta(
    const proto::segcore::LoadSegmentMeta& segment_meta) {
    std::unique_lock lck(mutex_);
    std::vector<int64_t> slice_lengths;
    for (auto& info : segment_meta.metas()) {
        slice_lengths.push_back(info.row_count());
    }
    insert_record_.timestamp_index_.set_length_meta(std::move(slice_lengths));
    ThrowInfo(NotImplemented, "unimplemented");
}

int64_t
ChunkedSegmentSealedImpl::get_active_count(Timestamp ts) const {
    // TODO optimize here to reduce expr search range
    return this->get_row_count();
}

void
ChunkedSegmentSealedImpl::mask_with_timestamps(BitsetTypeView& bitset_chunk,
                                               Timestamp timestamp,
                                               Timestamp collection_ttl) const {
    // TODO change the
    AssertInfo(insert_record_.timestamps_.num_chunk() == 1,
               "num chunk not equal to 1 for sealed segment");
    auto timestamps_data =
        (const milvus::Timestamp*)insert_record_.timestamps_.get_chunk_data(0);
    auto timestamps_data_size = insert_record_.timestamps_.get_chunk_size(0);
    if (collection_ttl > 0) {
        auto range =
            insert_record_.timestamp_index_.get_active_range(collection_ttl);
        if (range.first == range.second &&
            range.first == timestamps_data_size) {
            bitset_chunk.set();
            return;
        } else {
            auto ttl_mask = TimestampIndex::GenerateTTLBitset(
                timestamps_data, timestamps_data_size, collection_ttl, range);
            bitset_chunk |= ttl_mask;
        }
    }

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
ChunkedSegmentSealedImpl::generate_interim_index(const FieldId field_id,
                                                 int64_t num_rows) {
    if (col_index_meta_ == nullptr || !col_index_meta_->HasField(field_id)) {
        return false;
    }
    auto& field_meta = schema_->operator[](field_id);
    auto& field_index_meta = col_index_meta_->GetFieldIndexMeta(field_id);
    auto& index_params = field_index_meta.GetIndexParams();

    bool is_sparse =
        field_meta.get_data_type() == DataType::VECTOR_SPARSE_U32_F32;

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
            field_meta.get_data_type() != DataType::VECTOR_FLOAT16 &&
            field_meta.get_data_type() != DataType::VECTOR_BFLOAT16 &&
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
        int64_t row_count = num_rows;

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
        std::shared_ptr<ChunkedColumnInterface> vec_data = get_column(field_id);
        AssertInfo(
            vec_data != nullptr, "vector field {} not loaded", field_id.get());
        auto dim = is_sparse ? std::numeric_limits<uint32_t>::max()
                             : field_meta.get_dim();
        auto interim_index_type = field_binlog_config->GetIndexType();
        auto build_config =
            field_binlog_config->GetBuildBaseParams(field_meta.get_data_type());
        build_config[knowhere::meta::DIM] = std::to_string(dim);
        build_config[knowhere::meta::NUM_BUILD_THREAD] = std::to_string(1);
        auto index_metric = field_binlog_config->GetMetricType();

        if (enable_binlog_index()) {
            std::unique_lock lck(mutex_);

            std::unique_ptr<
                milvus::cachinglayer::Translator<milvus::index::IndexBase>>
                translator =
                    std::make_unique<milvus::segcore::storagev1translator::
                                         InterimSealedIndexTranslator>(
                        vec_data,
                        std::to_string(id_),
                        std::to_string(field_id.get()),
                        interim_index_type,
                        index_metric,
                        build_config,
                        dim,
                        is_sparse,
                        field_meta.get_data_type());

            auto interim_index_cache_slot =
                milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
                    std::move(translator));
            // TODO: how to handle the binlog index?
            vector_indexings_.append_field_indexing(
                field_id, index_metric, std::move(interim_index_cache_slot));

            vec_binlog_config_[field_id] = std::move(field_binlog_config);
            set_bit(binlog_index_bitset_, field_id, true);
            auto index_version =
                knowhere::Version::GetCurrentVersion().VersionNumber();
            if (is_sparse ||
                field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                index_has_raw_data_[field_id] =
                    knowhere::IndexStaticFaced<float>::HasRawData(
                        interim_index_type, index_version, build_config);
            } else if (field_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
                index_has_raw_data_[field_id] =
                    knowhere::IndexStaticFaced<float16>::HasRawData(
                        interim_index_type, index_version, build_config);
            } else if (field_meta.get_data_type() ==
                       DataType::VECTOR_BFLOAT16) {
                index_has_raw_data_[field_id] =
                    knowhere::IndexStaticFaced<bfloat16>::HasRawData(
                        interim_index_type, index_version, build_config);
            }

            LOG_INFO(
                "replace binlog with intermin index in segment {}, "
                "field {}.",
                this->get_segment_id(),
                field_id.get());
        }
        return true;
    } catch (std::exception& e) {
        LOG_WARN("fail to generate intermin index, because {}", e.what());
        return false;
    }
}
void
ChunkedSegmentSealedImpl::RemoveFieldFile(const FieldId field_id) {
}

void
ChunkedSegmentSealedImpl::LazyCheckSchema(SchemaPtr sch) {
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
ChunkedSegmentSealedImpl::load_field_data_common(
    FieldId field_id,
    const std::shared_ptr<ChunkedColumnInterface>& column,
    size_t num_rows,
    DataType data_type,
    bool enable_mmap,
    bool is_proxy_column,
    std::optional<ParquetStatistics> statistics) {
    {
        std::unique_lock lck(mutex_);
        AssertInfo(SystemProperty::Instance().IsSystem(field_id) ||
                       !get_bit(field_data_ready_bitset_, field_id),
                   "non system field {} data already loaded",
                   field_id.get());
        bool already_exists = false;
        fields_.withRLock([&](auto& fields) {
            already_exists = fields.find(field_id) != fields.end();
        });
        AssertInfo(
            !already_exists, "field {} column already exists", field_id.get());
        fields_.wlock()->emplace(field_id, column);
        if (enable_mmap) {
            mmap_field_ids_.insert(field_id);
        }
    }
    // system field only needs to emplace column to fields_ map
    if (SystemProperty::Instance().IsSystem(field_id)) {
        return;
    }

    if (!enable_mmap) {
        if (!is_proxy_column ||
            is_proxy_column &&
                field_id.get() != DEFAULT_SHORT_COLUMN_GROUP_ID) {
            stats_.mem_size += column->DataByteSize();
        }
        if (IsVariableDataType(data_type)) {
            // update average row data size
            SegmentInternalInterface::set_field_avg_size(
                field_id, num_rows, column->DataByteSize());
        }
    }
    if (!IsVariableDataType(data_type) || IsStringDataType(data_type)) {
        if (statistics) {
            LoadSkipIndexFromStatistics(
                field_id, data_type, statistics.value());
        } else {
            LoadSkipIndex(field_id, data_type, column);
        }
    }

    // set pks to offset
    if (schema_->get_primary_field_id() == field_id && !is_sorted_by_pk_) {
        AssertInfo(field_id.get() != -1, "Primary key is -1");
        AssertInfo(insert_record_.empty_pks(),
                   "primary key records already exists, current field id {}",
                   field_id.get());
        insert_record_.insert_pks(data_type, column.get());
        insert_record_.seal_pks();
    }

    bool generated_interim_index = generate_interim_index(field_id, num_rows);

    std::unique_lock lck(mutex_);
    AssertInfo(!get_bit(field_data_ready_bitset_, field_id),
               "field {} data already loaded",
               field_id.get());
    set_bit(field_data_ready_bitset_, field_id, true);
    update_row_count(num_rows);
    if (generated_interim_index) {
        auto column = get_column(field_id);
        if (column) {
            column->ManualEvictCache();
        }
    }
    if (data_type == DataType::GEOMETRY &&
        segcore_config_.get_enable_geometry_cache()) {
        // Construct GeometryCache for the entire field
        LoadGeometryCache(field_id, column);
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
    insert_record_.init_timestamps(timestamps, index);
    stats_.mem_size += sizeof(Timestamp) * num_rows;
}

void
ChunkedSegmentSealedImpl::Reopen(SchemaPtr sch) {
    std::unique_lock lck(mutex_);

    field_data_ready_bitset_.resize(sch->size());
    index_ready_bitset_.resize(sch->size());
    binlog_index_bitset_.resize(sch->size());

    auto absent_fields = sch->AbsentFields(*schema_);
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
            // no filling system fields
            continue;
        }
        if (get_bit(field_data_ready_bitset_, field_id)) {
            // no filling fields that data already loaded
            continue;
        }
        if (get_bit(index_ready_bitset_, field_id) &&
            (index_has_raw_data_[field_id])) {
            // no filling fields that index already loaded and has raw data
            continue;
        }
        if (IsVectorDataType(field_meta.get_data_type())) {
            // no filling vector fields
            continue;
        }
        fill_empty_field(field_meta);
    }
}

void
ChunkedSegmentSealedImpl::fill_empty_field(const FieldMeta& field_meta) {
    auto field_id = field_meta.get_id();
    LOG_INFO(
        "start fill empty field {} (data type {}) for sealed segment "
        "{}",
        field_meta.get_data_type(),
        field_id.get(),
        id_);
    int64_t size = num_rows_.value();
    AssertInfo(size > 0, "Chunked Sealed segment must have more than 0 row");
    auto field_data_info = FieldDataInfo(field_id.get(), size, "");
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
        case milvus::DataType::GEOMETRY: {
            column = std::make_shared<ChunkedVariableColumn<std::string>>(
                std::move(translator), field_meta);
            break;
        }
        case milvus::DataType::ARRAY: {
            column = std::make_shared<ChunkedArrayColumn>(std::move(translator),
                                                          field_meta);
            break;
        }
        case milvus::DataType::VECTOR_ARRAY: {
            column = std::make_shared<ChunkedVectorArrayColumn>(
                std::move(translator), field_meta);
            break;
        }
        default: {
            column = std::make_shared<ChunkedColumn>(std::move(translator),
                                                     field_meta);
            break;
        }
    }

    fields_.wlock()->emplace(field_id, column);
    set_bit(field_data_ready_bitset_, field_id, true);
    LOG_INFO(
        "fill empty field {} (data type {}) for growing segment {} "
        "done",
        field_meta.get_data_type(),
        field_id.get(),
        id_);
}

void
ChunkedSegmentSealedImpl::LoadGeometryCache(
    FieldId field_id, const std::shared_ptr<ChunkedColumnInterface>& column) {
    try {
        // Get geometry cache for this segment+field
        auto& geometry_cache =
            milvus::exec::SimpleGeometryCacheManager::Instance()
                .GetOrCreateCache(get_segment_id(), field_id);

        // Iterate through all chunks and collect WKB data
        auto num_chunks = column->num_chunks();
        for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
            // Get all string views from this chunk
            auto pw = column->StringViews(nullptr, chunk_id);
            auto [string_views, valid_data] = pw.get();

            // Add each string view to the geometry cache
            for (size_t i = 0; i < string_views.size(); ++i) {
                if (valid_data.empty() || valid_data[i]) {
                    // Valid geometry data
                    const auto& wkb_data = string_views[i];
                    geometry_cache.AppendData(
                        ctx_, wkb_data.data(), wkb_data.size());
                } else {
                    // Null/invalid geometry
                    geometry_cache.AppendData(ctx_, nullptr, 0);
                }
            }
        }

        LOG_INFO(
            "Successfully loaded geometry cache for segment {} field {} with "
            "{} geometries",
            get_segment_id(),
            field_id.get(),
            geometry_cache.Size());

    } catch (const std::exception& e) {
        ThrowInfo(UnexpectedError,
                  "Failed to load geometry cache for segment {} field {}: {}",
                  get_segment_id(),
                  field_id.get(),
                  e.what());
    }
}

void
ChunkedSegmentSealedImpl::SetLoadInfo(
    const proto::segcore::SegmentLoadInfo& load_info) {
    std::unique_lock lck(mutex_);
    segment_load_info_ = load_info;
    LOG_INFO(
        "SetLoadInfo for segment {}, num_rows: {}, index count: {}, "
        "storage_version: {}",
        id_,
        load_info.num_of_rows(),
        load_info.index_infos_size(),
        load_info.storageversion());
}

void
ChunkedSegmentSealedImpl::Load(milvus::tracer::TraceContext& trace_ctx) {
    // Get load info from segment_load_info_
    auto num_rows = segment_load_info_.num_of_rows();
    LOG_INFO("Loading segment {} with {} rows", id_, num_rows);

    // Step 1: Separate indexed and non-indexed fields
    std::map<FieldId, const proto::segcore::FieldIndexInfo*>
        field_id_to_index_info;
    std::set<FieldId> indexed_fields;

    for (int i = 0; i < segment_load_info_.index_infos_size(); i++) {
        const auto& index_info = segment_load_info_.index_infos(i);
        if (index_info.index_file_paths_size() == 0) {
            continue;
        }
        auto field_id = FieldId(index_info.fieldid());
        field_id_to_index_info[field_id] = &index_info;
        indexed_fields.insert(field_id);
    }

    // Step 2: Load indexes in parallel using thread pool
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    std::vector<std::future<void>> load_index_futures;

    for (const auto& pair : field_id_to_index_info) {
        auto field_id = pair.first;
        auto index_info_ptr = pair.second;
        auto future = pool.Submit(
            [this, trace_ctx, field_id, index_info_ptr, num_rows]() mutable
            -> void {
                // Convert proto FieldIndexInfo to LoadIndexInfo
                auto load_index_info =
                    ConvertFieldIndexInfoToLoadIndexInfo(index_info_ptr);

                LOG_INFO("Loading index for segment {} field {} with {} files",
                         id_,
                         field_id.get(),
                         load_index_info.index_files.size());

                // Download & compose index
                LoadIndexData(trace_ctx, &load_index_info);

                // Load index into segment
                LoadIndex(load_index_info);
            });

        load_index_futures.push_back(std::move(future));
    }

    // Wait for all index loading to complete and collect exceptions
    std::vector<std::exception_ptr> index_exceptions;
    for (auto& future : load_index_futures) {
        try {
            future.get();
        } catch (...) {
            index_exceptions.push_back(std::current_exception());
        }
    }

    // If any exceptions occurred during index loading, handle them
    if (!index_exceptions.empty()) {
        LOG_ERROR("Failed to load {} out of {} indexes for segment {}",
                  index_exceptions.size(),
                  load_index_futures.size(),
                  id_);

        // Rethrow the first exception
        std::rethrow_exception(index_exceptions[0]);
    }

    LOG_INFO("Finished loading {} indexes for segment {}",
             field_id_to_index_info.size(),
             id_);

    // Step 3: Prepare field data info for non-indexed fields
    std::map<FieldId, LoadFieldDataInfo> field_data_to_load;
    for (int i = 0; i < segment_load_info_.binlog_paths_size(); i++) {
        LoadFieldDataInfo load_field_data_info;
        load_field_data_info.storage_version =
            segment_load_info_.storageversion();

        const auto& field_binlog = segment_load_info_.binlog_paths(i);
        auto field_id = FieldId(field_binlog.fieldid());

        // Skip if this field has an index with raw data
        auto iter = index_has_raw_data_.find(field_id);
        if (iter != index_has_raw_data_.end() && iter->second) {
            LOG_INFO(
                "Skip loading binlog for segment {} field {} because index "
                "has raw data",
                id_,
                field_id.get());
            continue;
        }

        // Build FieldBinlogInfo
        FieldBinlogInfo field_binlog_info;
        field_binlog_info.field_id = field_id.get();

        // Calculate total row count and collect binlog paths
        int64_t total_entries = 0;
        for (const auto& binlog : field_binlog.binlogs()) {
            field_binlog_info.insert_files.push_back(binlog.log_path());
            field_binlog_info.entries_nums.push_back(binlog.entries_num());
            field_binlog_info.memory_sizes.push_back(binlog.memory_size());
            total_entries += binlog.entries_num();
        }
        field_binlog_info.row_count = total_entries;

        // Store in map
        load_field_data_info.field_infos[field_id.get()] = field_binlog_info;

        field_data_to_load[field_id] = load_field_data_info;
    }

    // Step 4: Load field data for non-indexed fields
    if (!field_data_to_load.empty()) {
        LOG_INFO("Loading field data for {} fields in segment {}",
                 field_data_to_load.size(),
                 id_);
        std::vector<std::future<void>> load_field_futures;

        for (const auto& [field_id, load_field_data_info] :
             field_data_to_load) {
            // Create a local copy to capture in lambda (C++17 compatible)
            const auto field_data = load_field_data_info;
            auto future = pool.Submit(
                [this, field_data]() -> void { LoadFieldData(field_data); });

            load_field_futures.push_back(std::move(future));
        }

        // Wait for all field data loading to complete and collect exceptions
        std::vector<std::exception_ptr> field_exceptions;
        for (auto& future : load_field_futures) {
            try {
                future.get();
            } catch (...) {
                field_exceptions.push_back(std::current_exception());
            }
        }

        // If any exceptions occurred during field data loading, handle them
        if (!field_exceptions.empty()) {
            LOG_ERROR("Failed to load {} out of {} field data for segment {}",
                      field_exceptions.size(),
                      load_field_futures.size(),
                      id_);

            // Rethrow the first exception
            std::rethrow_exception(field_exceptions[0]);
        }
    }

    LOG_INFO("Successfully loaded segment {} with {} rows", id_, num_rows);
}

}  // namespace milvus::segcore
