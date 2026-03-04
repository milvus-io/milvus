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

#include <cxxabi.h>
#include <fmt/core.h>
#include <folly/Try.h>
#include <simdjson.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <exception>
#include <future>
#include <iosfwd>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ratio>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "Types.h"
#include "Utils.h"
#include "arrow/result.h"
#include "bitset/bitset.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/Array.h"
#include "common/ArrayOffsets.h"
#include "common/ArrowDataWrapper.h"
#include "common/Channel.h"
#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/GeometryCache.h"
#include "common/GroupChunk.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/LoadInfo.h"
#include "common/OffsetMapping.h"
#include "common/QueryInfo.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/SystemProperty.h"
#include "common/Tracer.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorArray.h"
#include "common/resource_c.h"
#include "common/type_c.h"
#include "folly/Synchronized.h"
#include "geos_c.h"
#include "glog/logging.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/Meta.h"
#include "index/NgramInvertedIndex.h"
#include "index/ScalarIndex.h"
#include "index/TextMatchIndex.h"
#include "index/Utils.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_static.h"
#include "knowhere/sparse_utils.h"
#include "knowhere/version.h"
#include "log/Log.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/chunk_manager.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnGroup.h"
#include "mmap/ChunkedColumnInterface.h"
#include "mmap/Types.h"
#include "monitor/Monitor.h"
#include "monitor/scope_metric.h"
#include "nlohmann/json.hpp"
#include "parquet/metadata.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "prometheus/histogram.h"
#include "query/PlanImpl.h"
#include "query/SearchOnSealed.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/DeletedRecord.h"
#include "segcore/SealedIndexingRecord.h"
#include "segcore/SegmentSealed.h"
#include "segcore/TimestampIndex.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "segcore/storagev1translator/DefaultValueChunkTranslator.h"
#include "segcore/storagev1translator/InterimSealedIndexTranslator.h"
#include "segcore/storagev1translator/TextMatchIndexTranslator.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"
#include "segcore/storagev2translator/ManifestGroupTranslator.h"
#include "storage/FileManager.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"

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
ChunkedSegmentSealedImpl::LoadIndex(LoadIndexInfo& info) {
    LoadIndex(info, false);
}

void
ChunkedSegmentSealedImpl::LoadIndex(LoadIndexInfo& info, bool is_replace) {
    // print(info);
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto& field_meta = schema_->operator[](field_id);

    if (field_meta.is_vector()) {
        LoadVecIndex(info, is_replace);
    } else {
        LoadScalarIndex(info, is_replace);
    }
}

void
ChunkedSegmentSealedImpl::LoadVecIndex(LoadIndexInfo& info, bool is_replace) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);

    AssertInfo(info.index_params.count("metric_type"),
               "Can't get metric_type in index_params");
    auto metric_type = info.index_params.at("metric_type");

    std::unique_lock lck(mutex_);
    if (is_replace) {
        // Drop existing vector indexing for this field before replacing
        if (get_bit(index_ready_bitset_, field_id)) {
            vector_indexings_.drop_field_indexing(field_id);
        }
        LOG_INFO("Replacing vector index for field {} in segment {}",
                 field_id.get(),
                 id_);
    } else {
        AssertInfo(
            !get_bit(index_ready_bitset_, field_id),
            "vector index has been exist at " + std::to_string(field_id.get()));
    }
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
        field_id, metric_type, std::move(info.cache_index));
    set_bit(index_ready_bitset_, field_id, true);
    index_has_raw_data_[field_id] = request.has_raw_data;
    LOG_INFO("Has load vec index done, fieldID:{}. segmentID:{}, ",
             info.field_id,
             id_);
}

void
ChunkedSegmentSealedImpl::LoadScalarIndex(LoadIndexInfo& info,
                                          bool is_replace) {
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
    if (is_replace) {
        // Drop existing scalar indexing before replacing
        if (get_bit(index_ready_bitset_, field_id)) {
            auto [scalar_indexings, ngram_fields] = lock(
                folly::wlock(scalar_indexings_), folly::wlock(ngram_fields_));
            scalar_indexings->erase(field_id);
            ngram_fields->erase(field_id);
        }
        LOG_INFO("Replacing scalar index for field {} in segment {}",
                 field_id.get(),
                 id_);
    } else {
        AssertInfo(
            !get_bit(index_ready_bitset_, field_id),
            "scalar index has been exist at " + std::to_string(field_id.get()));
    }

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
            (*ngram_indexings)[field_id][path] = std::move(info.cache_index);
            return;
        } else {
            JsonIndex index;
            index.nested_path = path;
            index.field_id = field_id;
            index.index = std::move(info.cache_index);
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
        scalar_indexings->insert({field_id, std::move(info.cache_index)});
    } else {
        scalar_indexings_.wlock()->insert(
            {field_id, std::move(info.cache_index)});
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

void
ChunkedSegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& load_info,
                                        milvus::OpContext* op_ctx) {
    LoadFieldData(load_info, op_ctx, false);
}

void
ChunkedSegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& load_info,
                                        milvus::OpContext* op_ctx,
                                        bool is_replace) {
    switch (load_info.storage_version) {
        case 2: {
            load_column_group_data_internal(load_info, op_ctx, is_replace);
            break;
        }
        default:
            load_field_data_internal(load_info, op_ctx, is_replace);
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
    const LoadFieldDataInfo& load_info,
    milvus::OpContext* op_ctx,
    bool is_replace) {
    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    ArrowSchemaPtr arrow_schema = schema_->ConvertToArrowSchema();
    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();

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
        milvus_field_ids.reserve(field_id_list.size());
        for (int i = 0; i < field_id_list.size(); ++i) {
            milvus_field_ids.emplace_back(field_id_list.Get(i));
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
                GroupChunkType::DEFAULT,
                field_metas,
                column_group_info,
                insert_files,
                info.enable_mmap,
                mmap_config.GetMmapPopulate(),
                milvus_field_ids.size(),
                load_info.load_priority,
                info.warmup_policy);

        auto file_metas = translator->parquet_file_metas();
        auto field_id_mapping = translator->field_id_mapping();
        auto chunked_column_group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));

        // Create ProxyChunkColumn for each field in this column group
        for (const auto& field_id : milvus_field_ids) {
            const auto& field_meta = field_metas.at(field_id);
            auto column = std::make_shared<ProxyChunkColumn>(
                chunked_column_group, field_id, field_meta);
            auto data_type = field_meta.get_data_type();
            std::optional<ParquetStatistics> statistics_opt;
            if (ENABLE_PARQUET_STATS_SKIP_INDEX) {
                statistics_opt = parse_parquet_statistics(
                    file_metas, field_id_mapping, field_id.get());
            }

            load_field_data_common(
                field_id,
                column,
                num_rows,
                data_type,
                info.enable_mmap,
                true,
                ENABLE_PARQUET_STATS_SKIP_INDEX ? statistics_opt : std::nullopt,
                op_ctx,
                is_replace);
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
    const LoadFieldDataInfo& load_info,
    milvus::OpContext* op_ctx,
    bool is_replace) {
    SCOPE_CGO_CALL_METRIC();

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();

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
            LoadArrowReaderFromRemote(insert_files,
                                      field_data_info.arrow_reader_channel,
                                      load_info.load_priority);

            LOG_INFO("segment {} submits load field {} task to thread pool",
                     this->get_segment_id(),
                     field_id.get());
            load_system_field_internal(
                field_id, field_data_info, load_info.load_priority);
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
                    mmap_config.GetMmapPopulate(),
                    load_info.load_priority,
                    info.warmup_policy);

            auto data_type = field_meta.get_data_type();
            auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot(
                std::move(translator), op_ctx);
            auto column =
                MakeChunkedColumnBase(data_type, std::move(slot), field_meta);

            load_field_data_common(field_id,
                                   column,
                                   num_rows,
                                   data_type,
                                   info.enable_mmap,
                                   false,
                                   std::nullopt,
                                   op_ctx,
                                   is_replace);
        }
    }
}

void
ChunkedSegmentSealedImpl::load_system_field_internal(
    FieldId field_id,
    FieldDataInfo& data,
    proto::common::LoadPriority load_priority) {
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
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
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
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
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
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
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

        // get index params for bm25 and minhash brute force
        std::map<std::string, std::string> index_info;
        if (search_info.metric_type_ == knowhere::metric::BM25 ||
            search_info.metric_type_ == knowhere::metric::MHJACCARD) {
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

ChunkedSegmentSealedImpl::ValidResult
ChunkedSegmentSealedImpl::FilterVectorValidOffsets(milvus::OpContext* op_ctx,
                                                   FieldId field_id,
                                                   const int64_t* seg_offsets,
                                                   int64_t count) const {
    ValidResult result;
    result.valid_count = count;

    if (vector_indexings_.is_ready(field_id)) {
        auto field_indexing = vector_indexings_.get_field_indexing(field_id);
        auto cache_index = field_indexing->indexing_;
        auto ca = SemiInlineGet(cache_index->PinCells(op_ctx, {0}));
        auto vec_index = dynamic_cast<index::VectorIndex*>(ca->get_cell_of(0));

        if (vec_index != nullptr && vec_index->HasValidData()) {
            result.valid_data = std::make_unique<bool[]>(count);
            result.valid_offsets.reserve(count);

            for (int64_t i = 0; i < count; ++i) {
                bool is_valid = vec_index->IsRowValid(seg_offsets[i]);
                result.valid_data[i] = is_valid;
                if (is_valid) {
                    int64_t physical_offset =
                        vec_index->GetPhysicalOffset(seg_offsets[i]);
                    if (physical_offset >= 0) {
                        result.valid_offsets.push_back(physical_offset);
                    }
                }
            }
            result.valid_count = result.valid_offsets.size();
        }
    } else {
        auto column = get_column(field_id);
        if (column != nullptr && column->IsNullable()) {
            result.valid_data = std::make_unique<bool[]>(count);
            result.valid_offsets.reserve(count);

            const auto& offset_mapping = column->GetOffsetMapping();
            for (int64_t i = 0; i < count; ++i) {
                bool is_valid = offset_mapping.IsValid(seg_offsets[i]);
                result.valid_data[i] = is_valid;
                if (is_valid) {
                    int64_t physical_offset =
                        offset_mapping.GetPhysicalOffset(seg_offsets[i]);
                    if (physical_offset >= 0) {
                        result.valid_offsets.push_back(physical_offset);
                    }
                }
            }
            result.valid_count = result.valid_offsets.size();
        }
    }
    return result;
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
        ValidResult filter_result;
        knowhere::DataSetPtr ids_ds;
        int64_t valid_count = count;
        const bool* valid_data = nullptr;
        if (field_meta.is_nullable()) {
            filter_result =
                FilterVectorValidOffsets(op_ctx, field_id, ids, count);
            ids_ds = GenIdsDataset(filter_result.valid_count,
                                   filter_result.valid_offsets.data());
            valid_count = filter_result.valid_count;
            valid_data = filter_result.valid_data.get();
        } else {
            ids_ds = GenIdsDataset(count, ids);
        }
        if (field_meta.get_data_type() == DataType::VECTOR_SPARSE_U32_F32) {
            auto res = vec_index->GetSparseVector(ids_ds);
            return segcore::CreateVectorDataArrayFrom(
                res.get(), valid_data, count, valid_count, field_meta);
        } else {
            // dense vector:
            auto vector = vec_index->GetVector(ids_ds);
            return segcore::CreateVectorDataArrayFrom(
                vector.data(), valid_data, count, valid_count, field_meta);
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
    if (pks.empty()) {
        return;
    }
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

    switch (schema_->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64:
            search_pks_with_two_pointers_impl<int64_t>(
                bitset_view, pks, pk_column);
            break;
        case DataType::VARCHAR:
            search_pks_with_two_pointers_impl<std::string>(
                bitset_view, pks, pk_column);
            break;
        default:
            ThrowInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported type {}",
                    schema_->get_fields().at(pk_field_id).get_data_type()));
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
                const auto& pw = all_chunk_pins[i];
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
                const auto& pw = all_chunk_pins[i];
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
        case DataType::INT64:
            search_sorted_pk_range_impl<int64_t>(
                op, std::get<int64_t>(pk), pk_column, bitset);
            break;
        case DataType::VARCHAR:
            search_sorted_pk_range_impl<std::string>(
                op, std::get<std::string>(pk), pk_column, bitset);
            break;
        default:
            ThrowInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported type {}",
                    schema_->get_fields().at(pk_field_id).get_data_type()));
    }
}

void
ChunkedSegmentSealedImpl::pk_binary_range(milvus::OpContext* op_ctx,
                                          const PkType& lower_pk,
                                          bool lower_inclusive,
                                          const PkType& upper_pk,
                                          bool upper_inclusive,
                                          BitsetTypeView& bitset) const {
    if (!is_sorted_by_pk_) {
        // For unsorted segments, use the InsertRecord's binary range search
        insert_record_.search_pk_binary_range(
            lower_pk, lower_inclusive, upper_pk, upper_inclusive, bitset);
        return;
    }

    // For sorted segments, use binary search
    auto pk_field_id = schema_->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = get_column(pk_field_id);
    AssertInfo(pk_column != nullptr, "primary key column not loaded");

    switch (schema_->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64:
            search_sorted_pk_binary_range_impl<int64_t>(
                std::get<int64_t>(lower_pk),
                lower_inclusive,
                std::get<int64_t>(upper_pk),
                upper_inclusive,
                pk_column,
                bitset);
            break;
        case DataType::VARCHAR:
            search_sorted_pk_binary_range_impl<std::string>(
                std::get<std::string>(lower_pk),
                lower_inclusive,
                std::get<std::string>(upper_pk),
                upper_inclusive,
                pk_column,
                bitset);
            break;
        default:
            ThrowInfo(
                DataTypeInvalid,
                fmt::format(
                    "unsupported type {}",
                    schema_->get_fields().at(pk_field_id).get_data_type()));
    }
}

std::pair<std::vector<OffsetMap::OffsetType>, bool>
ChunkedSegmentSealedImpl::find_first(int64_t limit,
                                     const BitsetTypeView& bitset) const {
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
      segment_load_info_(milvus::proto::segcore::SegmentLoadInfo(), schema),
      schema_(schema),
      id_(segment_id),
      col_index_meta_(index_meta),
      is_sorted_by_pk_(is_sorted_by_pk),
      deleted_record_(
          &insert_record_,
          [this](const std::vector<PkType>& pks,
                 const Timestamp* timestamps,
                 const std::function<void(const SegOffset offset,
                                          const Timestamp ts)>& callback) {
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

void
ChunkedSegmentSealedImpl::bulk_subscript(milvus::OpContext* op_ctx,
                                         FieldId field_id,
                                         DataType data_type,
                                         const int64_t* seg_offsets,
                                         int64_t count,
                                         void* data,
                                         TargetBitmap& valid_map,
                                         bool small_int_raw_type) const {
    auto& field_meta = schema_->operator[](field_id);
    // DO NOT directly access the column by map like: `fields_.at(field_id)->Data()`,
    // we have to clone the shared pointer, to make sure it won't get released
    // if segment released
    auto column = get_column(field_id);
    AssertInfo(column != nullptr,
               "field {} must exist when doing bulk_subscript",
               field_id.get());
    if (column->IsNullable()) {
        for (auto i = 0; i < count; i++) {
            valid_map.set(i, column->IsValid(op_ctx, seg_offsets[i]));
        }
    } else {
        valid_map.set();
    }
    switch (data_type) {
        case DataType::BOOL: {
            bulk_subscript_impl<bool>(op_ctx,
                                      column.get(),
                                      seg_offsets,
                                      count,
                                      static_cast<bool*>(data));
            break;
        }
        case DataType::INT8: {
            bulk_subscript_impl<int8_t>(op_ctx,
                                        column.get(),
                                        seg_offsets,
                                        count,
                                        static_cast<int8_t*>(data),
                                        small_int_raw_type);
            break;
        }
        case DataType::INT16: {
            bulk_subscript_impl<int16_t>(op_ctx,
                                         column.get(),
                                         seg_offsets,
                                         count,
                                         static_cast<int16_t*>(data),
                                         small_int_raw_type);
            break;
        }
        case DataType::INT32: {
            bulk_subscript_impl<int32_t>(op_ctx,
                                         column.get(),
                                         seg_offsets,
                                         count,
                                         static_cast<int32_t*>(data));
            break;
        }
        case DataType::TIMESTAMPTZ:
        case DataType::INT64: {
            bulk_subscript_impl<int64_t>(op_ctx,
                                         column.get(),
                                         seg_offsets,
                                         count,
                                         static_cast<int64_t*>(data));
            break;
        }
        case DataType::FLOAT: {
            bulk_subscript_impl<float>(op_ctx,
                                       column.get(),
                                       seg_offsets,
                                       count,
                                       static_cast<float*>(data));
            break;
        }
        case DataType::DOUBLE: {
            bulk_subscript_impl<double>(op_ctx,
                                        column.get(),
                                        seg_offsets,
                                        count,
                                        static_cast<double*>(data));
            break;
        }
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TEXT: {
            // dst must have at least count elements; the callback's offset
            // parameter is guaranteed to be in [0, count)
            bulk_subscript_ptr_impl<std::string>(
                op_ctx,
                column.get(),
                seg_offsets,
                count,
                static_cast<std::string*>(data));
            break;
        }
        case DataType::JSON: {
            // dst must have at least count elements; the callback's offset
            // parameter is guaranteed to be in [0, count)
            bulk_subscript_ptr_impl<Json>(op_ctx,
                                          column.get(),
                                          seg_offsets,
                                          count,
                                          static_cast<Json*>(data));
            break;
        }
        case DataType::GEOMETRY: {
            // dst must have at least count elements; the callback's offset
            // parameter is guaranteed to be in [0, count)
            bulk_subscript_ptr_impl<std::string>(
                op_ctx,
                column.get(),
                seg_offsets,
                count,
                static_cast<std::string*>(data));
            break;
        }
        case DataType::ARRAY: {
            // dst must have at least count elements; the callback's index
            // parameter is guaranteed to be in [0, count)
            auto dst = static_cast<Array*>(data);
            column->BulkArrayAt(
                op_ctx,
                [dst](const ArrayView& view, size_t i) {
                    view.output_data(dst[i]);
                },
                seg_offsets,
                count);
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported data type {}",
                                  field_meta.get_data_type()));
        }
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
                                              T* dst,
                                              bool small_int_raw_type) {
    static_assert(std::is_fundamental_v<S> && std::is_fundamental_v<T>);
    // use field->data_type_ to determine the type of dst
    field->BulkPrimitiveValueAt(op_ctx,
                                static_cast<void*>(dst),
                                seg_offsets,
                                count,
                                small_int_raw_type);
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

template <typename S, typename T>
void
ChunkedSegmentSealedImpl::bulk_subscript_ptr_impl(
    milvus::OpContext* op_ctx,
    const ChunkedColumnInterface* column,
    const int64_t* seg_offsets,
    int64_t count,
    T* dst) {
    if constexpr (std::is_same_v<S, Json>) {
        column->BulkRawJsonAt(
            op_ctx,
            [&](Json json, size_t offset, bool is_valid) {
                dst[offset] = std::move(T(json));
            },
            seg_offsets,
            count);
    } else {
        static_assert(std::is_same_v<S, std::string>);
        column->BulkRawStringAt(
            op_ctx,
            [&](std::string_view value, size_t offset, bool is_valid) {
                dst[offset] = std::move(T(value));
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
        [dst](const ArrayView& view, size_t i) {
            view.output_data(dst->at(i));
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
                                          int64_t count,
                                          int64_t valid_count,
                                          const void* valid_data) const {
    auto& field_meta = schema_->operator[](field_id);
    if (IsVectorDataType(field_meta.get_data_type())) {
        return CreateEmptyVectorDataArray(
            count, valid_count, valid_data, field_meta);
    }
    return CreateEmptyScalarDataArray(count, field_meta);
}

void
ChunkedSegmentSealedImpl::CreateTextIndex(FieldId field_id,
                                          milvus::OpContext* op_ctx) {
    // Check for cancellation before starting
    CheckCancellation(op_ctx,
                      id_,
                      field_id.get(),
                      "ChunkedSegmentSealedImpl::CreateTextIndex()");

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
            // Check for cancellation before bulk operation
            CheckCancellation(op_ctx,
                              id_,
                              field_id.get(),
                              "ChunkedSegmentSealedImpl::CreateTextIndex()");
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

    // Check for cancellation before finalizing
    CheckCancellation(op_ctx,
                      id_,
                      field_id.get(),
                      "ChunkedSegmentSealedImpl::CreateTextIndex()");

    // create index reader.
    index->CreateReader(milvus::index::SetBitsetSealed);
    // release index writer.
    index->Finish();

    index->Reload();

    index->RegisterAnalyzer("milvus_tokenizer",
                            field_meta.get_analyzer_params().c_str());

    text_indexes_[field_id] = std::make_shared<index::TextMatchIndexHolder>(
        std::move(index), cfg.GetScalarIndexEnableMmap());
}

void
ChunkedSegmentSealedImpl::LoadTextIndex(
    milvus::OpContext* op_ctx,
    std::shared_ptr<milvus::proto::indexcgo::LoadTextIndexInfo> info_proto) {
    // Check for cancellation before starting
    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::LoadTextIndex()");

    milvus::storage::FieldDataMeta field_data_meta{info_proto->collectionid(),
                                                   info_proto->partitionid(),
                                                   this->get_segment_id(),
                                                   info_proto->fieldid(),
                                                   info_proto->schema()};
    milvus::storage::IndexMeta index_meta{this->get_segment_id(),
                                          info_proto->fieldid(),
                                          info_proto->buildid(),
                                          info_proto->version()};
    auto field_meta = milvus::FieldMeta::ParseFrom(info_proto->schema());
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
    if (info_proto->warmup_policy() != "") {
        config[milvus::index::WARMUP] = info_proto->warmup_policy();
    }
    milvus::storage::FileManagerContext file_ctx(
        field_data_meta, index_meta, remote_chunk_manager, fs);

    auto field_id = milvus::FieldId(info_proto->fieldid());
    // const auto& field_meta = schema_->operator[](field_id);
    milvus::segcore::storagev1translator::TextMatchIndexLoadInfo load_info{
        info_proto->enable_mmap(),
        this->get_segment_id(),
        info_proto->fieldid(),
        field_meta.get_analyzer_params(),
        info_proto->index_size(),
        info_proto->warmup_policy()};

    std::unique_ptr<
        milvus::cachinglayer::Translator<milvus::index::TextMatchIndex>>
        translator = std::make_unique<
            milvus::segcore::storagev1translator::TextMatchIndexTranslator>(
            load_info, file_ctx, config);
    auto cache_slot =
        milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
            std::move(translator), op_ctx);

    std::unique_lock lck(mutex_);
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

    int64_t valid_count = count;
    const bool* valid_data = nullptr;
    const int64_t* valid_offsets = seg_offsets;
    ValidResult filter_result;

    if (field_meta.is_vector() && field_meta.is_nullable()) {
        filter_result =
            FilterVectorValidOffsets(op_ctx, field_id, seg_offsets, count);
        valid_count = filter_result.valid_count;
        valid_data = filter_result.valid_data.get();
        valid_offsets = filter_result.valid_offsets.data();
    }
    auto ret = fill_with_empty(field_id, count, valid_count, valid_data);

    if (!field_meta.is_vector() && column->IsNullable()) {
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
            bulk_subscript_impl<int64_t, int64_t>(
                op_ctx,
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
                                valid_offsets,
                                valid_count,
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
                valid_offsets,
                valid_count,
                ret->mutable_vectors()->mutable_float16_vector()->data());
            break;
        }
        case DataType::VECTOR_BFLOAT16: {
            bulk_subscript_impl(
                op_ctx,
                field_meta.get_sizeof(),
                column.get(),
                valid_offsets,
                valid_count,
                ret->mutable_vectors()->mutable_bfloat16_vector()->data());
            break;
        }
        case DataType::VECTOR_BINARY: {
            bulk_subscript_impl(
                op_ctx,
                field_meta.get_sizeof(),
                column.get(),
                valid_offsets,
                valid_count,
                ret->mutable_vectors()->mutable_binary_vector()->data());
            break;
        }
        case DataType::VECTOR_INT8: {
            bulk_subscript_impl(
                op_ctx,
                field_meta.get_sizeof(),
                column.get(),
                valid_offsets,
                valid_count,
                ret->mutable_vectors()->mutable_int8_vector()->data());
            break;
        }
        case DataType::VECTOR_SPARSE_U32_F32: {
            auto dst = ret->mutable_vectors()->mutable_sparse_float_vector();
            int64_t max_dim = 0;
            column->BulkValueAt(
                op_ctx,
                [&](const char* value, size_t i) mutable {
                    auto offset = valid_offsets[i];
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
                valid_offsets,
                valid_count);
            dst->set_dim(max_dim);
            ret->mutable_vectors()->set_dim(dst->dim());
            break;
        }
        case DataType::VECTOR_ARRAY: {
            bulk_subscript_vector_array_impl(
                op_ctx,
                column.get(),
                valid_offsets,
                valid_count,
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
        std::shared_ptr<ChunkedColumnInterface> vec_data = get_column(field_id);
        AssertInfo(
            vec_data != nullptr, "vector field {} not loaded", field_id.get());
        int64_t row_count = field_meta.is_nullable()
                                ? vec_data->GetOffsetMapping().GetValidCount()
                                : num_rows;

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
                        id_,
                        field_id.get(),
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
    std::optional<ParquetStatistics> statistics,
    milvus::OpContext* op_ctx,
    bool is_replace) {
    {
        std::unique_lock lck(mutex_);
        if (is_replace) {
            // Subtract old column memory before replacing
            auto old_column = get_column(field_id);
            if (old_column && !enable_mmap) {
                if (!is_proxy_column ||
                    is_proxy_column &&
                        field_id.get() != DEFAULT_SHORT_COLUMN_GROUP_ID) {
                    stats_.mem_size -= old_column->DataByteSize();
                }
            }
            fields_.wlock()->insert_or_assign(field_id, column);
            LOG_INFO(
                "Replacing field {} data in segment {}", field_id.get(), id_);
        } else {
            AssertInfo(SystemProperty::Instance().IsSystem(field_id) ||
                           !get_bit(field_data_ready_bitset_, field_id),
                       "non system field {} data already loaded",
                       field_id.get());
            bool already_exists = false;
            fields_.withRLock([&](auto& fields) {
                already_exists = fields.find(field_id) != fields.end();
            });
            AssertInfo(!already_exists,
                       "field {} column already exists",
                       field_id.get());
            fields_.wlock()->emplace(field_id, column);
        }
        if (enable_mmap) {
            mmap_field_ids_.insert(field_id);
        }
    }
    // system field only needs to emplace column to fields_ map
    if (SystemProperty::Instance().IsSystem(field_id)) {
        return;
    }

    if (column->IsNullable() && IsVectorDataType(data_type)) {
        column->BuildValidRowIds(op_ctx);
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
        if (!is_replace) {
            AssertInfo(
                insert_record_.empty_pks(),
                "primary key records already exists, current field id {}",
                field_id.get());
            insert_record_.insert_pks(data_type, column.get());
            insert_record_.seal_pks();
        }
        // TODO: handle PK replacement for insert_record_ in future
    }

    bool generated_interim_index = generate_interim_index(field_id, num_rows);

    std::string struct_name;
    const FieldMeta* field_meta_ptr = nullptr;

    {
        std::unique_lock lck(mutex_);
        if (!is_replace) {
            AssertInfo(!get_bit(field_data_ready_bitset_, field_id),
                       "field {} data already loaded",
                       field_id.get());
        }
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

        // Check if need to build ArrayOffsetsSealed for struct array fields
        if (data_type == DataType::ARRAY ||
            data_type == DataType::VECTOR_ARRAY) {
            auto& field_meta = schema_->operator[](field_id);
            const std::string& field_name = field_meta.get_name().get();

            if (field_name.find('[') != std::string::npos &&
                field_name.find(']') != std::string::npos) {
                struct_name = field_name.substr(0, field_name.find('['));

                auto it = struct_to_array_offsets_.find(struct_name);
                if (it != struct_to_array_offsets_.end()) {
                    array_offsets_map_[field_id] = it->second;
                } else {
                    field_meta_ptr = &field_meta;  // need to build
                }
            }
        }
    }

    // Build ArrayOffsetsSealed outside lock (expensive operation)
    if (field_meta_ptr) {
        auto new_offsets =
            ArrayOffsetsSealed::BuildFromSegment(this, *field_meta_ptr);

        std::unique_lock lck(mutex_);
        // Double-check after re-acquiring lock
        auto it = struct_to_array_offsets_.find(struct_name);
        if (it == struct_to_array_offsets_.end()) {
            struct_to_array_offsets_[struct_name] = new_offsets;
            array_offsets_map_[field_id] = new_offsets;
        } else {
            array_offsets_map_[field_id] = it->second;
        }
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
        fill_empty_field(field_meta);
    }

    schema_ = sch;
}

void
ChunkedSegmentSealedImpl::Reopen(
    const milvus::proto::segcore::SegmentLoadInfo& new_load_info) {
    SegmentLoadInfo new_seg_load_info(new_load_info, schema_);

    std::unique_lock lck(mutex_);
    SegmentLoadInfo current(segment_load_info_);
    segment_load_info_ = new_seg_load_info;
    lck.unlock();

    // compute load diff
    auto diff = current.ComputeDiff(new_seg_load_info);
    LOG_INFO("Reopen segment {} with diff {}", id_, diff.ToString());
    ApplyLoadDiff(new_seg_load_info, diff);

    LOG_INFO("Reopen segment {} done", id_);
}

void
ChunkedSegmentSealedImpl::ApplyLoadDiff(SegmentLoadInfo& segment_load_info,
                                        LoadDiff& diff,
                                        milvus::OpContext* op_ctx) {
    // TODO: pass trace_ctx separately when needed
    milvus::tracer::TraceContext trace_ctx;

    // Load new indexes (fields without existing index)
    if (!diff.indexes_to_load.empty()) {
        LoadBatchIndexes(trace_ctx, diff.indexes_to_load, op_ctx);
    }
    // Replace indexes (fields that already have an index loaded)
    if (!diff.indexes_to_replace.empty()) {
        LoadBatchIndexes(trace_ctx, diff.indexes_to_replace, op_ctx, true);
    }

    // reload fields
    if (!diff.fields_to_reload.empty()) {
        ReloadColumns(diff.fields_to_reload, op_ctx);
    }

    // drop index, must after reload binlog
    if (!diff.indexes_to_drop.empty()) {
        for (auto field_id : diff.indexes_to_drop) {
            DropIndex(field_id);
        }
    }

    // load column groups
    bool has_cg_changes = !diff.column_groups_to_load.empty() ||
                          !diff.column_groups_to_replace.empty() ||
                          !diff.column_groups_to_lazyload.empty() ||
                          !diff.column_groups_to_lazyreplace.empty();
    if (has_cg_changes) {
        auto properties =
            milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                .GetProperties();
        auto column_groups = segment_load_info.GetColumnGroups();
        auto arrow_schema = schema_->ConvertToLoonArrowSchema();
        auto needed_columns = std::make_shared<std::vector<std::string>>();
        for (const auto& field_id : schema_->get_field_ids()) {
            needed_columns->push_back(std::to_string(field_id.get()));
        }
        reader_ = milvus_storage::api::Reader::create(
            column_groups, arrow_schema, needed_columns, *properties);
        // New column group fields
        if (!diff.column_groups_to_load.empty()) {
            LoadColumnGroups(column_groups,
                             properties,
                             diff.column_groups_to_load,
                             true,
                             op_ctx);
        }
        if (!diff.column_groups_to_lazyload.empty()) {
            LoadColumnGroups(column_groups,
                             properties,
                             diff.column_groups_to_lazyload,
                             false,
                             op_ctx);
        }
        // Replace column group fields
        if (!diff.column_groups_to_replace.empty()) {
            LoadColumnGroups(column_groups,
                             properties,
                             diff.column_groups_to_replace,
                             true,
                             op_ctx,
                             true);
        }
        if (!diff.column_groups_to_lazyreplace.empty()) {
            LoadColumnGroups(column_groups,
                             properties,
                             diff.column_groups_to_lazyreplace,
                             false,
                             op_ctx,
                             true);
        }
    }

    // load pre-built text indexes
    if (!diff.text_indexes_to_load.empty()) {
        LoadBatchTextIndexes(op_ctx, diff.text_indexes_to_load);
    }

    // Load new field binlogs
    if (!diff.binlogs_to_load.empty()) {
        LoadBatchFieldData(trace_ctx, diff.binlogs_to_load, op_ctx);
    }
    // Replace field binlogs
    if (!diff.binlogs_to_replace.empty()) {
        LoadBatchFieldData(trace_ctx, diff.binlogs_to_replace, op_ctx, true);
    }

    // fill default values for fields without data sources (schema evolution)
    if (!diff.fields_to_fill_default.empty()) {
        FillDefaultValueFields(diff.fields_to_fill_default);
    }

    // create text indexes from raw data
    if (!diff.text_indexes_to_create.empty()) {
        for (const auto& field_id : diff.text_indexes_to_create) {
            CreateTextIndex(field_id, op_ctx);
            segment_load_info.SetTextIndexCreated(field_id);
        }
    }

    // drop field
    if (!diff.field_data_to_drop.empty()) {
        for (auto field_id : diff.field_data_to_drop) {
            DropFieldData(field_id);
        }
    }
}

void
ChunkedSegmentSealedImpl::fill_empty_field(const FieldMeta& field_meta) {
    auto field_id = field_meta.get_id();
    auto data_type = field_meta.get_data_type();
    LOG_INFO(
        "start fill empty field {} (data type {}) for sealed segment "
        "{}",
        data_type,
        field_id.get(),
        id_);
    auto [field_has_setting, field_mmap_enabled] =
        schema_->MmapEnabled(field_id);
    auto is_vector = IsVectorDataType(field_meta.get_data_type());
    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    bool global_use_mmap = is_vector ? mmap_config.GetVectorFieldEnableMmap()
                                     : mmap_config.GetScalarFieldEnableMmap();
    bool use_mmap = field_has_setting ? field_mmap_enabled : global_use_mmap;
    auto mmap_dir_path =
        milvus::storage::LocalChunkManagerSingleton::GetInstance()
            .GetChunkManager()
            ->GetRootPath();
    int64_t size = num_rows_.value();
    AssertInfo(size > 0, "Chunked Sealed segment must have more than 0 row");
    auto field_data_info = FieldDataInfo(field_id.get(), size, mmap_dir_path);

    auto [field_has_warmup, field_warmup_policy] = schema_->WarmupPolicy(
        field_id, IsVectorDataType(data_type), /*is_index=*/false);
    std::string warmup_policy = field_has_warmup ? field_warmup_policy : "";
    std::unique_ptr<Translator<milvus::Chunk>> translator =
        std::make_unique<storagev1translator::DefaultValueChunkTranslator>(
            get_segment_id(),
            field_meta,
            field_data_info,
            use_mmap,
            mmap_config.GetMmapPopulate(),
            warmup_policy);
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot(
        std::move(translator), nullptr);
    auto column = MakeChunkedColumnBase(data_type, std::move(slot), field_meta);

    if (column->IsNullable() && IsVectorDataType(data_type)) {
        column->BuildValidRowIds(nullptr);
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
ChunkedSegmentSealedImpl::FillDefaultValueFields(
    const std::vector<FieldId>& field_ids) {
    std::unique_lock lck(mutex_);
    for (const auto& field_id : field_ids) {
        // Skip if field data already loaded
        if (get_bit(field_data_ready_bitset_, field_id)) {
            continue;
        }
        // Skip if index has raw data
        if (get_bit(index_ready_bitset_, field_id) &&
            index_has_raw_data_[field_id]) {
            continue;
        }
        const auto& field_meta = schema_->operator[](field_id);
        fill_empty_field(field_meta);
    }
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
    segment_load_info_ = SegmentLoadInfo(load_info, schema_);
    LOG_INFO(
        "SetLoadInfo for segment {}, num_rows: {}, index count: {}, "
        "storage_version: {}",
        id_,
        segment_load_info_.GetNumOfRows(),
        segment_load_info_.GetIndexInfoCount(),
        segment_load_info_.GetStorageVersion());
}

void
ChunkedSegmentSealedImpl::LoadColumnGroups(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    std::vector<std::pair<int, std::vector<FieldId>>>& cg_field_ids,
    bool eager_load,
    milvus::OpContext* op_ctx,
    bool is_replace) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> load_group_futures;
    for (const auto& pair : cg_field_ids) {
        auto cg_index = pair.first;
        const auto& field_ids = pair.second;
        auto future = pool.Submit([this,
                                   column_groups,
                                   properties,
                                   cg_index,
                                   field_ids,
                                   eager_load,
                                   op_ctx,
                                   is_replace]() {
            // Early exit if cancelled while queued
            CheckCancellation(op_ctx,
                              id_,
                              cg_index,
                              "ChunkedSegmentSealedImpl::LoadColumnGroup()");
            LoadColumnGroup(column_groups,
                            properties,
                            cg_index,
                            field_ids,
                            eager_load,
                            op_ctx,
                            is_replace);
        });
        load_group_futures.emplace_back(std::move(future));
    }

    storage::WaitAllFutures(load_group_futures);
}

void
ChunkedSegmentSealedImpl::LoadColumnGroup(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    int64_t index,
    const std::vector<FieldId>& milvus_field_ids,
    bool eager_load,
    milvus::OpContext* op_ctx,
    bool is_replace) {
    AssertInfo(index < column_groups->size(),
               "load column group index out of range");
    auto column_group = column_groups->at(index);

    auto field_metas = schema_->get_field_metas(milvus_field_ids);

    // assumption: vector field occupies whole column group
    bool is_vector = false;
    bool has_mmap_setting = false;
    bool mmap_enabled = false;
    bool has_warmup_setting = false;
    bool warmup_sync = false;
    for (auto& [field_id, field_meta] : field_metas) {
        if (IsVectorDataType(field_meta.get_data_type())) {
            is_vector = true;
        }
        std::shared_lock lck(mutex_);
        auto iter = index_has_raw_data_.find(field_id);

        // if field has mmap setting, use it
        // - mmap setting at collection level, then all field are the same
        // - mmap setting at field level, we define that as long as one field shall be mmap, then whole group shall be mmaped
        auto [field_has_setting, field_mmap_enabled] =
            schema_->MmapEnabled(field_id);
        has_mmap_setting = has_mmap_setting || field_has_setting;
        mmap_enabled = mmap_enabled || field_mmap_enabled;

        // if field has warmup setting, use it
        // - warmup setting at collection level, uses appropriate key based on field type
        // - warmup setting at field level, use the most aggressive policy (sync > disable)
        // Note: this is for field data loading, not index (is_index = false)
        bool field_is_vector = IsVectorDataType(field_meta.get_data_type());
        auto [field_has_warmup, field_warmup_policy] = schema_->WarmupPolicy(
            field_id, field_is_vector, /*is_index=*/false);
        if (field_has_warmup) {
            has_warmup_setting = true;
            warmup_sync = warmup_sync || (field_warmup_policy == "sync");
        }
    }

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    bool global_use_mmap = is_vector ? mmap_config.GetVectorFieldEnableMmap()
                                     : mmap_config.GetScalarFieldEnableMmap();
    auto use_mmap = has_mmap_setting ? mmap_enabled : global_use_mmap;

    auto chunk_reader_result = reader_->get_chunk_reader(index);
    AssertInfo(chunk_reader_result.ok(),
               "get chunk reader failed, segment {}, column group index {}, "
               "status msg: {}",
               get_segment_id(),
               index,
               chunk_reader_result.status().ToString());

    auto chunk_reader = std::move(chunk_reader_result).ValueOrDie();

    LOG_INFO("[StorageV2] segment {} loads manifest cg index {}",
             this->get_segment_id(),
             index);
    auto mmap_dir_path =
        milvus::storage::LocalChunkManagerSingleton::GetInstance()
            .GetChunkManager()
            ->GetRootPath();

    // Determine warmup policy: use per-field settings if any,
    // otherwise pass empty string to fall back to global config
    std::string warmup_policy =
        has_warmup_setting ? (warmup_sync ? "sync" : "disable") : "";

    auto translator =
        std::make_unique<storagev2translator::ManifestGroupTranslator>(
            get_segment_id(),
            GroupChunkType::DEFAULT,
            index,
            std::move(chunk_reader),
            field_metas,
            use_mmap,
            mmap_config.GetMmapPopulate(),
            mmap_dir_path,
            column_group->columns.size(),
            segment_load_info_.GetPriority(),
            eager_load,
            warmup_policy);
    auto chunked_column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    // Create ProxyChunkColumn for each field
    for (const auto& field_id : milvus_field_ids) {
        const auto& field_meta = field_metas.at(field_id);
        auto column = std::make_shared<ProxyChunkColumn>(
            chunked_column_group, field_id, field_meta);
        auto data_type = field_meta.get_data_type();
        std::optional<ParquetStatistics> statistics_opt;
        load_field_data_common(
            field_id,
            column,
            segment_load_info_.GetNumOfRows(),
            data_type,
            use_mmap,
            true,
            std::
                nullopt,  // manifest cannot provide parquet skip index directly
            op_ctx,
            is_replace);
        if (field_id == TimestampFieldID) {
            auto timestamp_proxy_column = get_column(TimestampFieldID);
            AssertInfo(timestamp_proxy_column != nullptr,
                       "timestamp proxy column is nullptr");
            // TODO check timestamp_index ready instead of check system_ready_count_
            int64_t num_rows = segment_load_info_.GetNumOfRows();
            auto all_ts_chunks = timestamp_proxy_column->GetAllChunks(nullptr);
            std::vector<Timestamp> timestamps(num_rows);
            int64_t offset = 0;
            for (auto& all_ts_chunk : all_ts_chunks) {
                auto chunk_data = all_ts_chunk.get();
                auto fixed_chunk = dynamic_cast<FixedWidthChunk*>(chunk_data);
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
}

void
ChunkedSegmentSealedImpl::ReloadColumns(const std::vector<FieldId>& field_ids,
                                        milvus::OpContext* op_ctx) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> reload_futures;
    for (auto& field_id : field_ids) {
        auto future = pool.Submit([this, field_id, op_ctx]() {
            auto column = get_column(field_id);
            AssertInfo(column != nullptr,
                       "cannot reload non-existing field column {}",
                       field_id.get());
            auto num_chunks = column->num_chunks();
            std::vector<int64_t> chunk_ids(num_chunks);
            for (int64_t chunk_id = 0; chunk_id < num_chunks; chunk_id++) {
                chunk_ids[chunk_id] = chunk_id;
            }
            column->PrefetchChunks(op_ctx, chunk_ids);
        });
        reload_futures.push_back(std::move(future));
    }

    storage::WaitAllFutures(reload_futures);
}

void
ChunkedSegmentSealedImpl::LoadBatchTextIndexes(
    milvus::OpContext* op_ctx,
    std::unordered_map<FieldId,
                       std::shared_ptr<proto::indexcgo::LoadTextIndexInfo>>&
        text_indexes_to_load) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> load_index_futures;

    load_index_futures.reserve(text_indexes_to_load.size());
    for (auto& [field_id, load_text_index_info] : text_indexes_to_load) {
        auto future = pool.Submit(
            [this, op_ctx, info = std::move(load_text_index_info)]() mutable
            -> void { LoadTextIndex(op_ctx, std::move(info)); });
        load_index_futures.emplace_back(std::move(future));
    }

    storage::WaitAllFutures(load_index_futures);
}

void
ChunkedSegmentSealedImpl::LoadBatchIndexes(
    milvus::tracer::TraceContext& trace_ctx,
    std::unordered_map<FieldId, std::vector<LoadIndexInfo>>&
        field_id_to_index_info,
    milvus::OpContext* op_ctx,
    bool is_replace) {
    auto num_rows = segment_load_info_.GetNumOfRows();
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> load_index_futures;
    load_index_futures.reserve(field_id_to_index_info.size());

    for (auto& pair : field_id_to_index_info) {
        auto field_id = pair.first;
        auto& index_infos = pair.second;
        for (auto& load_index_info : index_infos) {
            auto* load_index_info_ptr = &load_index_info;
            auto future = pool.Submit([this,
                                       trace_ctx,
                                       field_id,
                                       load_index_info_ptr,
                                       num_rows,
                                       op_ctx,
                                       is_replace]() mutable -> void {
                // Early exit if cancelled while queued
                CheckCancellation(op_ctx, id_, field_id.get(), "LoadIndex");

                LOG_INFO("Loading index for segment {} field {} with {} files",
                         id_,
                         field_id.get(),
                         load_index_info_ptr->index_files.size());

                // Download & compose index
                LoadIndexData(trace_ctx, load_index_info_ptr, op_ctx);

                // Load index into segment
                LoadIndex(*load_index_info_ptr, is_replace);
            });

            load_index_futures.push_back(std::move(future));
        }
    }

    storage::WaitAllFutures(load_index_futures);
}

void
ChunkedSegmentSealedImpl::LoadBatchFieldData(
    milvus::tracer::TraceContext& trace_ctx,
    std::vector<std::pair<std::vector<FieldId>, proto::segcore::FieldBinlog>>&
        field_binlog_to_load,
    milvus::OpContext* op_ctx,
    bool is_replace) {
    LOG_INFO("Loading field binlog for {} fields in segment {}",
             field_binlog_to_load.size(),
             id_);

    std::map<FieldId, LoadFieldDataInfo> field_data_to_load;
    for (auto& [field_ids, field_binlog] : field_binlog_to_load) {
        LoadFieldDataInfo load_field_data_info;
        load_field_data_info.storage_version =
            segment_load_info_.GetStorageVersion();
        // when child fields specified, field id is group id, child field ids are actual id values here
        if (field_binlog.child_fields_size() > 0) {
            field_ids.reserve(field_binlog.child_fields_size());
            for (auto field_id : field_binlog.child_fields()) {
                field_ids.emplace_back(field_id);
            }
        } else {
            field_ids.emplace_back(field_binlog.fieldid());
        }

        bool index_has_raw_data = true;
        bool has_mmap_setting = false;
        bool mmap_enabled = false;
        bool is_vector = false;

        bool has_warmup_setting = false;
        bool warmup_sync = false;
        for (const auto& child_field_id : field_ids) {
            auto& field_meta = schema_->operator[](child_field_id);
            if (IsVectorDataType(field_meta.get_data_type())) {
                is_vector = true;
            }

            // if field has mmap setting, use it
            // - mmap setting at collection level, then all field are the same
            // - mmap setting at field level, we define that as long as one field shall be mmap, then whole group shall be mmaped
            auto [field_has_setting, field_mmap_enabled] =
                schema_->MmapEnabled(child_field_id);
            has_mmap_setting = has_mmap_setting || field_has_setting;
            mmap_enabled = mmap_enabled || field_mmap_enabled;

            auto iter = index_has_raw_data_.find(child_field_id);
            if (iter != index_has_raw_data_.end()) {
                index_has_raw_data = index_has_raw_data && iter->second;
            } else {
                index_has_raw_data = false;
            }

            auto [field_has_warmup, field_warmup_policy] =
                schema_->WarmupPolicy(
                    child_field_id,
                    IsVectorDataType(field_meta.get_data_type()),
                    /*is_index=*/false);
            if (field_has_warmup) {
                has_warmup_setting = true;
                warmup_sync = warmup_sync || (field_warmup_policy == "sync");
            }
        }

        auto group_id = field_binlog.fieldid();
        // Skip if this field has an index with raw data
        if (index_has_raw_data) {
            LOG_INFO(
                "Skip loading fielddata for segment {} group {} because "
                "index "
                "has raw data",
                id_,
                group_id);
            continue;
        }

        // Build FieldBinlogInfo
        FieldBinlogInfo field_binlog_info;
        field_binlog_info.field_id = group_id;

        // Calculate total row count and collect binlog paths
        int64_t total_entries = 0;
        auto binlog_count = field_binlog.binlogs().size();
        field_binlog_info.insert_files.reserve(binlog_count);
        field_binlog_info.entries_nums.reserve(binlog_count);
        field_binlog_info.memory_sizes.reserve(binlog_count);
        for (const auto& binlog : field_binlog.binlogs()) {
            field_binlog_info.insert_files.push_back(binlog.log_path());
            field_binlog_info.entries_nums.push_back(binlog.entries_num());
            field_binlog_info.memory_sizes.push_back(binlog.memory_size());
            total_entries += binlog.entries_num();
        }
        field_binlog_info.row_count = total_entries;

        auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
        auto global_use_mmap = is_vector
                                   ? mmap_config.GetVectorFieldEnableMmap()
                                   : mmap_config.GetScalarFieldEnableMmap();
        field_binlog_info.enable_mmap =
            has_mmap_setting ? mmap_enabled : global_use_mmap;

        // Determine group warmup policy: use per-field settings if any,
        // otherwise fall back to global warmup policy
        field_binlog_info.warmup_policy =
            has_warmup_setting ? (warmup_sync ? "sync" : "disable") : "";

        // Store in map
        load_field_data_info.field_infos[group_id] = field_binlog_info;

        field_data_to_load[FieldId(group_id)] = load_field_data_info;
    }

    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> load_field_futures;
    load_field_futures.reserve(field_data_to_load.size());

    for (const auto& [field_id, load_field_data_info] : field_data_to_load) {
        // Create local copies to capture in lambda (C++17 compatible)
        const auto field_data = load_field_data_info;
        const auto captured_field_id = field_id;
        auto future = pool.Submit(
            [this, field_data, captured_field_id, op_ctx, is_replace]()
                -> void {
                // Early exit if cancelled while queued
                CheckCancellation(op_ctx,
                                  id_,
                                  captured_field_id.get(),
                                  "ChunkedSegmentSealedImpl::LoadFieldData()");
                LoadFieldData(field_data, op_ctx, is_replace);
            });

        load_field_futures.push_back(std::move(future));
    }

    storage::WaitAllFutures(load_field_futures);
}

void
ChunkedSegmentSealedImpl::Load(milvus::tracer::TraceContext& trace_ctx,
                               milvus::OpContext* op_ctx) {
    // Get load info from segment_load_info_
    auto num_rows = segment_load_info_.GetNumOfRows();
    LOG_INFO("Loading segment {} with {} rows", id_, num_rows);

    auto diff = segment_load_info_.GetLoadDiff();
    LOG_WARN("Load segment {} with diff {}", id_, diff.ToString());
    ApplyLoadDiff(segment_load_info_, diff, op_ctx);

    LOG_INFO("Successfully loaded segment {} with {} rows", id_, num_rows);
}

}  // namespace milvus::segcore
