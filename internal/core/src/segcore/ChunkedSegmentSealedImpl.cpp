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
#include "segcore/default_fs.h"

#include <cxxabi.h>
#include <fmt/core.h>
#include <folly/ScopeGuard.h>
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
#include <filesystem>
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
#include <nlohmann/json.hpp>

#include "NamedType/named_type_impl.hpp"
#include "Types.h"
#include "Utils.h"
#include "arrow/array.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "bitset/bitset.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "common/Array.h"
#include "common/ArrayOffsets.h"
#include "common/ArrowDataWrapper.h"
#include "common/Channel.h"
#include "common/FastMem.h"
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
#include "common/ScopedTimer.h"
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
#include "index/json_stats/JsonKeyStats.h"
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
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "milvus-storage/packed/chunk_manager.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnGroup.h"
#include "mmap/ChunkedColumnInterface.h"
#include "mmap/VirtualPKChunkedColumn.h"
#include "mmap/Types.h"
#include "common/VirtualPK.h"
#include "monitor/Monitor.h"
#include "monitor/scope_metric.h"
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
#include "segcore/storagev2translator/SystemIndexTranslator.h"
#include "segcore/storagev1translator/InterimSealedIndexTranslator.h"
#include "segcore/storagev1translator/TextMatchIndexTranslator.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"
#include "segcore/storagev2translator/ManifestGroupTranslator.h"
#include "segcore/TextColumnCache.h"
#include "storage/FileManager.h"
#include "storage/KeyRetriever.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/loon_ffi/util.h"

namespace milvus::segcore {
using namespace milvus::cachinglayer;

constexpr auto kCollectionSchemaVersionNotReady = static_cast<ErrorCode>(2046);

static std::string
FormatFieldIds(const std::vector<FieldId>& field_ids) {
    std::vector<int64_t> ids;
    ids.reserve(field_ids.size());
    for (const auto& field_id : field_ids) {
        ids.push_back(field_id.get());
    }
    return fmt::format("{}", ids);
}

static std::string
FormatRuntimeFieldIds(
    const std::shared_ptr<const ChunkedSegmentSealedImpl::RuntimeResourceState>&
        runtime) {
    if (runtime == nullptr) {
        return "[]";
    }
    std::vector<int64_t> ids;
    ids.reserve(runtime->fields.size());
    for (const auto& [field_id, _] : runtime->fields) {
        ids.push_back(field_id.get());
    }
    std::sort(ids.begin(), ids.end());
    return fmt::format("{}", ids);
}

static void
CheckVectorOutputCellsLoaded(int64_t segment_id,
                             FieldId field_id,
                             const FieldMeta& field_meta,
                             const ChunkedColumnInterface* column,
                             const int64_t* offsets,
                             int64_t count);

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

static inline bool
get_bit_if_present(const BitsetType& bitset, FieldId field_id) {
    auto pos = field_id.get() - START_USER_FIELDID;
    return pos >= 0 && static_cast<size_t>(pos) < bitset.size() && bitset[pos];
}

static inline bool
field_exists_in_schema(const SchemaPtr& schema, FieldId field_id) {
    return field_id.get() < START_USER_FIELDID ||
           schema->get_fields().find(field_id) != schema->get_fields().end();
}

static inline bool
has_bit_position(const BitsetType& bitset, FieldId field_id) {
    auto pos = field_id.get() - START_USER_FIELDID;
    return pos >= 0 && static_cast<size_t>(pos) < bitset.size();
}

static inline void
clear_bit_if_present(BitsetType& bitset, FieldId field_id) {
    if (has_bit_position(bitset, field_id)) {
        set_bit(bitset, field_id, false);
    }
}

static inline void
cancel_warmup(const index::CacheIndexBasePtr& index) {
    if (index) {
        index->CancelWarmup();
    }
}

static inline void
cancel_and_erase_scalar_index(
    std::unordered_map<FieldId, index::CacheIndexBasePtr>& scalar_indexings,
    FieldId field_id) {
    if (auto it = scalar_indexings.find(field_id);
        it != scalar_indexings.end()) {
        cancel_warmup(it->second);
        scalar_indexings.erase(it);
    }
}

PinWrapper<const storagev2translator::TimestampIndexCell*>
ChunkedSegmentSealedImpl::PinTimestampIndex(
    const std::shared_ptr<const RuntimeResourceState>& runtime,
    milvus::OpContext* op_ctx) const {
    auto slot = runtime != nullptr ? runtime->timestamp_index_slot : nullptr;
    if (!slot) {
        return PinWrapper<const storagev2translator::TimestampIndexCell*>(
            nullptr);
    }
    auto ca = SemiInlineGet(slot->PinCells(op_ctx, {0}));
    auto* cell = ca->get_cell_of(0);
    AssertInfo(
        cell != nullptr, "timestamp index cache is corrupted, segment {}", id_);
    return PinWrapper<const storagev2translator::TimestampIndexCell*>(ca, cell);
}

PinWrapper<const storagev2translator::TimestampIndexCell*>
ChunkedSegmentSealedImpl::PinTimestampIndex(milvus::OpContext* op_ctx) const {
    return PinTimestampIndex(CaptureRuntimeResourceState(), op_ctx);
}

Timestamp
ChunkedSegmentSealedImpl::ReadTimestamp(
    int64_t offset,
    const std::shared_ptr<const RuntimeResourceState>& runtime,
    std::optional<Timestamp> effective_commit_ts) const {
    if (effective_commit_ts) {
        return *effective_commit_ts;
    }
    auto timestamps = runtime != nullptr ? runtime->timestamps : nullptr;
    if (timestamps != nullptr && !timestamps->empty()) {
        return (*timestamps)[offset];
    }
    auto column = get_column(runtime, TimestampFieldID);
    AssertInfo(column != nullptr, "timestamp data is not ready");
    const auto chunk_pos = column->GetChunkIDByOffset(offset);
    auto pin = column->Span(nullptr, chunk_pos.first);
    auto span = milvus::Span<Timestamp>(pin.get());
    return span[chunk_pos.second];
}

PinWrapper<const storagev2translator::PkIndexCell*>
ChunkedSegmentSealedImpl::PinPkIndex(
    const std::shared_ptr<const RuntimeResourceState>& runtime,
    milvus::OpContext* op_ctx) const {
    auto slot = runtime != nullptr ? runtime->pk_index_slot : nullptr;
    if (!slot) {
        return PinWrapper<const storagev2translator::PkIndexCell*>(nullptr);
    }
    auto ca = SemiInlineGet(slot->PinCells(op_ctx, {0}));
    auto* cell = ca->get_cell_of(0);
    AssertInfo(cell != nullptr, "pk index cache is corrupted, segment {}", id_);
    return PinWrapper<const storagev2translator::PkIndexCell*>(ca, cell);
}

std::vector<PinWrapper<const index::IndexBase*>>
ChunkedSegmentSealedImpl::PinJsonIndex(milvus::OpContext* op_ctx,
                                       FieldId field_id,
                                       const std::string& path,
                                       DataType data_type,
                                       bool any_type,
                                       bool is_array) const {
    auto runtime = CaptureRuntimeResourceState();
    int path_len_diff = std::numeric_limits<int>::max();
    index::CacheIndexBasePtr best_match = nullptr;
    std::string_view path_view = path;
    for (const auto& index : runtime->json_indices) {
        if (index.field_id != field_id) {
            continue;
        }
        switch (index.cast_type.data_type()) {
            case JsonCastType::DataType::JSON:
                if (path_view.length() < index.nested_path.length()) {
                    continue;
                }
                if (path_view.substr(0, index.nested_path.length()) ==
                    index.nested_path) {
                    int current_len_diff =
                        path_view.length() - index.nested_path.length();
                    if (current_len_diff < path_len_diff) {
                        path_len_diff = current_len_diff;
                        best_match = index.index;
                    }
                    if (path_len_diff == 0) {
                        break;
                    }
                }
                break;
            default:
                if (index.nested_path != path) {
                    continue;
                }
                if (any_type || milvus::index::json::IsDataTypeSupported(
                                    index.cast_type, data_type, is_array)) {
                    best_match = index.index;
                }
                break;
        }
    }
    if (best_match == nullptr) {
        return {};
    }
    auto ca = SemiInlineGet(best_match->PinCells(op_ctx, {0}));
    auto pinned_index = ca->get_cell_of(0);
    return {PinWrapper<const index::IndexBase*>(std::move(ca), pinned_index)};
}

std::string
ChunkedSegmentSealedImpl::GetJsonFlatIndexNestedPath(
    FieldId field_id, std::string_view query_path) const {
    auto runtime = CaptureRuntimeResourceState();
    std::string best_path;
    int path_len_diff = std::numeric_limits<int>::max();
    for (const auto& index : runtime->json_indices) {
        if (index.field_id != field_id ||
            index.cast_type.data_type() != JsonCastType::DataType::JSON ||
            query_path.length() < index.nested_path.length() ||
            query_path.substr(0, index.nested_path.length()) !=
                index.nested_path) {
            continue;
        }
        int current_len_diff = query_path.length() - index.nested_path.length();
        if (current_len_diff < path_len_diff) {
            path_len_diff = current_len_diff;
            best_path = index.nested_path;
        }
        if (path_len_diff == 0) {
            break;
        }
    }
    return best_path;
}

bool
ChunkedSegmentSealedImpl::Contain(const PkType& pk) const {
    auto snapshot = CapturePublishedState();
    auto schema_snapshot = snapshot->schema;
    auto runtime = snapshot->runtime;
    // Zero-storage pk2offset (VirtualPKOffsetMap) resolves PKs by bit-extract.
    // Skips PinPkIndex + sorted-pk binary search on the virtual PK column.
    if (runtime != nullptr && runtime->virtual_pk2offset != nullptr) {
        return runtime->virtual_pk2offset->contain(pk);
    }
    auto pk_index = PinPkIndex(runtime, nullptr);
    if (pk_index.get() != nullptr && pk_index.get()->has_pk2offset()) {
        return pk_index.get()->contain(pk);
    }
    // Sorted-by-pk segment: binary search on pk column directly.
    if (is_sorted_by_pk_) {
        auto pk_field_id =
            schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
        AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
        auto pk_column = get_column(runtime, pk_field_id);
        if (pk_column != nullptr) {
            auto num_chunks = pk_column->num_chunks();
            auto all_chunks = pk_column->GetAllChunks(nullptr);
            switch (
                schema_snapshot->get_fields().at(pk_field_id).get_data_type()) {
                case DataType::INT64: {
                    auto target = std::get<int64_t>(pk);
                    for (int64_t i = 0; i < num_chunks; ++i) {
                        auto* src = reinterpret_cast<const int64_t*>(
                            all_chunks[i].get()->RawData());
                        auto rows = pk_column->chunk_row_nums(i);
                        auto it = std::lower_bound(src, src + rows, target);
                        if (it != src + rows && *it == target) {
                            return true;
                        }
                    }
                    return false;
                }
                case DataType::VARCHAR: {
                    auto& target = std::get<std::string>(pk);
                    for (int64_t i = 0; i < num_chunks; ++i) {
                        auto* chunk =
                            static_cast<StringChunk*>(all_chunks[i].get());
                        auto offset = chunk->binary_search_string(target);
                        if (offset != -1 && offset < chunk->RowNums() &&
                            chunk->operator[](offset) == target) {
                            return true;
                        }
                    }
                    return false;
                }
                default:
                    break;
            }
        }
    }
    return false;
}

bool
ChunkedSegmentSealedImpl::is_system_field_ready() const {
    return CapturePublishedState()->system_field_ready;
}

void
ChunkedSegmentSealedImpl::init_storage_v2_timestamp_index(
    const std::shared_ptr<ChunkedColumnInterface>& column,
    size_t num_rows,
    const std::string& warmup_policy) {
    auto runtime = CloneMutableRuntimeResourceState();
    init_storage_v2_timestamp_index(
        column, num_rows, warmup_policy, runtime.get());
    PublishRuntimeStateLocked(ToConstRuntimeState(std::move(runtime)));
}

void
ChunkedSegmentSealedImpl::init_storage_v2_timestamp_index(
    const std::shared_ptr<ChunkedColumnInterface>& column,
    size_t num_rows,
    const std::string& warmup_policy,
    RuntimeResourceState* runtime) {
    std::unique_ptr<Translator<storagev2translator::TimestampIndexCell>>
        translator =
            std::make_unique<storagev2translator::TimestampIndexTranslator>(
                id_, column, num_rows, warmup_policy);
    auto slot = Manager::GetInstance().CreateCacheSlot(std::move(translator));
    auto cell_holder = SemiInlineGet(slot->PinCells(nullptr, {0}));
    auto* cell = cell_holder->get_cell_of(0);
    AssertInfo(
        cell != nullptr, "timestamp index cache is corrupted, segment {}", id_);

    auto timestamps = std::make_shared<TimestampData>();
    auto pins = column->GetAllChunks(nullptr);
    timestamps->InitFromPinnedChunks(column, std::move(pins));

    auto target_runtime = runtime;
    std::shared_ptr<RuntimeResourceState> owned_runtime;
    if (target_runtime == nullptr) {
        owned_runtime = CloneMutableRuntimeResourceState();
        target_runtime = owned_runtime.get();
    }
    target_runtime->timestamps = std::move(timestamps);
    target_runtime->timestamp_index =
        std::make_shared<const TimestampIndex>(cell->timestamp_index());
    target_runtime->timestamp_index_slot = std::move(slot);

    if (owned_runtime != nullptr) {
        PublishRuntimeStateLocked(
            ToConstRuntimeState(std::move(owned_runtime)));
    }
}

void
ChunkedSegmentSealedImpl::init_storage_v1_timestamp_index(
    std::vector<Timestamp> timestamps,
    size_t num_rows,
    RuntimeResourceState* runtime) {
    auto index = std::make_shared<const TimestampIndex>(
        build_timestamp_index(timestamps.data(), num_rows));
    auto timestamp_data = std::make_shared<TimestampData>();
    timestamp_data->InitFromOwnedData(std::move(timestamps));

    auto target_runtime = runtime;
    std::shared_ptr<RuntimeResourceState> owned_runtime;
    if (target_runtime == nullptr) {
        owned_runtime = CloneMutableRuntimeResourceState();
        target_runtime = owned_runtime.get();
    }
    target_runtime->timestamps = std::move(timestamp_data);
    target_runtime->timestamp_index = std::move(index);
    target_runtime->timestamp_index_slot.reset();

    stats_.mem_size += sizeof(Timestamp) * num_rows;

    if (owned_runtime != nullptr) {
        PublishRuntimeStateLocked(
            ToConstRuntimeState(std::move(owned_runtime)));
    }
}

void
ChunkedSegmentSealedImpl::init_storage_v1_timestamp_index(
    std::vector<Timestamp> timestamps, size_t num_rows) {
    init_storage_v1_timestamp_index(std::move(timestamps), num_rows, nullptr);
}

std::shared_ptr<CacheSlot<storagev2translator::PkIndexCell>>
ChunkedSegmentSealedImpl::BuildPkIndexSlot(
    const std::shared_ptr<ChunkedColumnInterface>& column,
    DataType data_type,
    bool eager,
    milvus::OpContext* op_ctx) const {
    std::unique_ptr<Translator<storagev2translator::PkIndexCell>> translator =
        std::make_unique<storagev2translator::PkIndexTranslator>(
            id_, column, data_type, is_sorted_by_pk_);
    auto slot = Manager::GetInstance().CreateCacheSlot(std::move(translator));
    if (eager) {
        auto cell_holder = SemiInlineGet(slot->PinCells(op_ctx, {0}));
        AssertInfo(cell_holder->get_cell_of(0) != nullptr,
                   "primary key index cache is corrupted, segment {}",
                   id_);
    }
    return slot;
}

bool
ChunkedSegmentSealedImpl::StagedStateCommitter::IsVectorIndexReady(
    FieldId field_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return get_bit_if_present(staged_state_->index_ready_bitset, field_id) ||
           get_bit_if_present(staged_state_->binlog_index_bitset, field_id);
}

void
ChunkedSegmentSealedImpl::LoadIndex(LoadIndexInfo& info) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    LoadIndex(info, false);
}

void
ChunkedSegmentSealedImpl::LoadIndex(LoadIndexInfo& info, bool is_replace) {
    LoadIndex(info, CaptureSchemaSnapshot(), is_replace, nullptr);
}

void
ChunkedSegmentSealedImpl::LoadIndex(LoadIndexInfo& info,
                                    const SchemaPtr& schema_snapshot,
                                    bool is_replace,
                                    RuntimeResourceState* runtime,
                                    PublishedSegmentState* staged_state,
                                    StagedStateCommitter* committer) {
    // print(info);
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto& field_meta = schema_snapshot->operator[](field_id);

    if (field_meta.is_vector()) {
        LoadVecIndex(
            info, schema_snapshot, is_replace, staged_state, committer);
    } else {
        LoadScalarIndex(info,
                        schema_snapshot,
                        is_replace,
                        runtime,
                        staged_state,
                        committer);
    }
}

void
ChunkedSegmentSealedImpl::LoadIndex(LoadIndexInfo& info,
                                    bool is_replace,
                                    RuntimeResourceState* runtime,
                                    PublishedSegmentState* staged_state,
                                    StagedStateCommitter* committer) {
    LoadIndex(info,
              CaptureSchemaSnapshot(),
              is_replace,
              runtime,
              staged_state,
              committer);
}

void
ChunkedSegmentSealedImpl::LoadVecIndex(LoadIndexInfo& info,
                                       const SchemaPtr& schema_snapshot,
                                       bool is_replace,
                                       PublishedSegmentState* staged_state,
                                       StagedStateCommitter* committer) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto snapshot = CapturePublishedState();

    AssertInfo(info.index_params.count("metric_type"),
               "Can't get metric_type in index_params");
    auto metric_type = info.index_params.at("metric_type");

    const auto& visible_state =
        staged_state != nullptr ? *staged_state : *snapshot;
    bool has_index =
        get_bit_if_present(visible_state.index_ready_bitset, field_id);
    bool has_binlog_index =
        get_bit_if_present(visible_state.binlog_index_bitset, field_id);

    if (is_replace) {
        LOG_INFO("Replacing vector index for field {} in segment {}",
                 field_id.get(),
                 id_);
    } else {
        AssertInfo(
            !has_index,
            "vector index has been exist at " + std::to_string(field_id.get()));
    }
    LOG_INFO(
        "Before setting field_bit for field index, fieldID:{}. "
        "segmentID:{}, ",
        info.field_id,
        id_);
    auto& field_meta = schema_snapshot->operator[](field_id);
    LoadResourceRequest request{};
    if (info.load_resource_request.has_value()) {
        request = *info.load_resource_request;
    } else {
        request =
            milvus::index::IndexFactory::GetInstance().VecIndexLoadResource(
                field_meta.get_data_type(),
                info.element_type,
                info.index_engine_version,
                info.index_size,
                info.index_params,
                info.enable_mmap,
                info.num_rows,
                info.dim);
    }
    request.has_raw_data =
        milvus::index::IndexFactory::CanUseIndexRawDataForField(
            field_meta.get_data_type(), request.has_raw_data);

    // Note: raw data lifecycle (eviction/drop) is handled by LoadDiff + ApplyLoadDiff,
    // not here. This avoids unsafe ManualEvictCache on column groups.

    bool drop_existing = (is_replace && has_index) || has_binlog_index;
    if (staged_state != nullptr) {
        AssertInfo(committer != nullptr,
                   "staged vector index load requires committer");
        committer->StageVectorIndexMutationLocked(
            field_id, metric_type, std::move(info.cache_index), drop_existing);
        LOG_INFO("Has staged vec index load, fieldID:{}. segmentID:{}, ",
                 info.field_id,
                 id_);

        clear_bit_if_present(staged_state->published_binlog_index_ready_bitset,
                             field_id);
        set_bit(staged_state->published_index_ready_bitset, field_id, true);
        SetPublishedIndexRawDataInState(
            *staged_state, field_id, request.has_raw_data);
        NormalizePublishedState(*staged_state);
    } else {
        auto next_runtime = CloneRuntimeResourceState(snapshot->runtime);
        if (drop_existing) {
            DropVectorIndexing(*next_runtime, field_id);
            next_runtime->vec_binlog_config.erase(field_id);
        }
        next_runtime->vector_indexings[field_id] =
            BuildVectorIndexEntry(metric_type, std::move(info.cache_index));
        LOG_INFO("Has load vec index done, fieldID:{}. segmentID:{}, ",
                 info.field_id,
                 id_);
        PublishIndexReadyLocked(field_id,
                                request.has_raw_data,
                                ToConstRuntimeState(std::move(next_runtime)));
    }
}

void
ChunkedSegmentSealedImpl::LoadScalarIndex(LoadIndexInfo& info,
                                          const SchemaPtr& schema_snapshot,
                                          bool is_replace,
                                          RuntimeResourceState* runtime,
                                          PublishedSegmentState* staged_state,
                                          StagedStateCommitter* committer) {
    // NOTE: lock only when data is ready to avoid starvation
    auto field_id = FieldId(info.field_id);
    auto snapshot = CapturePublishedState();
    auto& field_meta = schema_snapshot->operator[](field_id);

    auto is_pk = field_id ==
                 schema_snapshot->get_primary_field_id().value_or(FieldId(-1));

    RuntimeResourceState* target_runtime = runtime;
    std::shared_ptr<RuntimeResourceState> owned_runtime;
    std::vector<index::CacheIndexBasePtr> retired_indexings;

    auto retire_indexing = [&](index::CacheIndexBasePtr indexing) {
        if (indexing == nullptr) {
            return;
        }
        if (committer != nullptr) {
            committer->RetireCacheIndexingLocked(std::move(indexing));
        } else if (owned_runtime != nullptr) {
            retired_indexings.push_back(std::move(indexing));
        }
    };

    auto cancel_retired_indexings = [&] {
        for (auto& indexing : retired_indexings) {
            if (indexing != nullptr) {
                indexing->CancelWarmup();
            }
        }
    };

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

    const auto& visible_state =
        staged_state != nullptr ? *staged_state : *snapshot;
    bool has_index =
        get_bit_if_present(visible_state.index_ready_bitset, field_id);

    std::unique_lock lck(mutex_);
    if (is_replace) {
        if (target_runtime == nullptr) {
            owned_runtime = CloneRuntimeResourceState(snapshot->runtime);
            target_runtime = owned_runtime.get();
        }
        cancel_and_erase_scalar_index(target_runtime->scalar_indexings,
                                      field_id);
        target_runtime->ngram_fields.erase(field_id);
        LOG_INFO("Replacing scalar index for field {} in segment {}",
                 field_id.get(),
                 id_);
    } else {
        AssertInfo(
            !has_index,
            "scalar index has been exist at " + std::to_string(field_id.get()));
    }

    if (field_meta.get_data_type() == DataType::JSON) {
        auto path = info.index_params.at(JSON_PATH);
        if (target_runtime == nullptr) {
            owned_runtime = CloneRuntimeResourceState(snapshot->runtime);
            target_runtime = owned_runtime.get();
        }
        for (auto& retired :
             EraseJsonIndexesAtPath(*target_runtime, field_id, path)) {
            retire_indexing(std::move(retired));
        }
        if (auto it = info.index_params.find(index::INDEX_TYPE);
            it != info.index_params.end() &&
            it->second == index::NGRAM_INDEX_TYPE) {
            target_runtime->ngram_indexings[field_id][path] =
                std::move(info.cache_index);
            if (staged_state != nullptr) {
                clear_bit_if_present(
                    staged_state->published_binlog_index_ready_bitset,
                    field_id);
                set_bit(
                    staged_state->published_index_ready_bitset, field_id, true);
                SetPublishedIndexRawDataInState(*staged_state, field_id, false);
                NormalizePublishedState(*staged_state);
            } else {
                auto published_runtime =
                    owned_runtime != nullptr
                        ? ToConstRuntimeState(std::move(owned_runtime))
                        : FreezeRuntimeResourceState(*target_runtime);
                lck.unlock();
                PublishIndexReadyLocked(field_id, false, published_runtime);
                cancel_retired_indexings();
            }
            return;
        } else {
            JsonIndex index;
            index.nested_path = path;
            index.field_id = field_id;
            index.index = std::move(info.cache_index);
            index.cast_type =
                JsonCastType::FromString(info.index_params.at(JSON_CAST_TYPE));
            target_runtime->json_indices.push_back(std::move(index));
            if (staged_state != nullptr) {
                SyncJsonNgramIndexState(
                    *staged_state, *target_runtime, field_id);
                NormalizePublishedState(*staged_state);
            } else if (owned_runtime != nullptr) {
                auto published_runtime =
                    ToConstRuntimeState(std::move(owned_runtime));
                lck.unlock();
                MutatePublishedStateLocked([&](PublishedSegmentState& state) {
                    state.runtime = published_runtime;
                    SyncJsonNgramIndexState(
                        state, *published_runtime, field_id);
                });
                cancel_retired_indexings();
            }
            return;
        }
    }

    if (target_runtime == nullptr) {
        owned_runtime = CloneRuntimeResourceState(snapshot->runtime);
        target_runtime = owned_runtime.get();
    }
    auto cache_index = info.cache_index;
    if (auto it = info.index_params.find(index::INDEX_TYPE);
        it != info.index_params.end() &&
        it->second == index::NGRAM_INDEX_TYPE) {
        target_runtime->ngram_fields.insert(field_id);
        target_runtime->scalar_indexings[field_id] = cache_index;
    } else {
        target_runtime->scalar_indexings[field_id] = cache_index;
    }
    info.cache_index = std::move(cache_index);

    LoadResourceRequest request{};
    if (info.load_resource_request.has_value()) {
        request = *info.load_resource_request;
    } else {
        request =
            milvus::index::IndexFactory::GetInstance().ScalarIndexLoadResource(
                field_meta.get_data_type(),
                info.index_engine_version,
                info.index_size,
                info.index_params,
                info.enable_mmap,
                target_runtime->row_count);
    }

    request.has_raw_data =
        milvus::index::IndexFactory::CanUseIndexRawDataForField(
            field_meta.get_data_type(), request.has_raw_data);
    // Note: raw data lifecycle (eviction/drop) is handled by LoadDiff + ApplyLoadDiff,
    // not here. This avoids unsafe ManualEvictCache on column groups.
    LOG_INFO(
        "Has load scalar index done, fieldID:{}. segmentID:{}, has_raw_data:{}",
        info.field_id,
        id_,
        request.has_raw_data);
    lck.unlock();
    if (staged_state != nullptr) {
        clear_bit_if_present(staged_state->published_binlog_index_ready_bitset,
                             field_id);
        set_bit(staged_state->published_index_ready_bitset, field_id, true);
        SetPublishedIndexRawDataInState(
            *staged_state, field_id, request.has_raw_data);
        NormalizePublishedState(*staged_state);
    } else if (owned_runtime != nullptr) {
        auto published_runtime = ToConstRuntimeState(std::move(owned_runtime));
        PublishIndexReadyLocked(
            field_id, request.has_raw_data, published_runtime);
    }
}

void
ChunkedSegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& load_info,
                                        milvus::OpContext* op_ctx) {
    LoadFieldData(load_info, op_ctx, false);
}

SchemaPtr
ChunkedSegmentSealedImpl::CaptureSchemaSnapshot() const {
    return CapturePublishedState()->schema;
}

void
ChunkedSegmentSealedImpl::ValidateSchemaCompatibility(
    const SchemaPtr& plan_schema) const {
    if (plan_schema == nullptr) {
        return;
    }

    auto published_schema = CaptureSchemaSnapshot();
    AssertInfo(published_schema != nullptr,
               "published schema is null for segment {}",
               id_);
    if (published_schema->get_schema_version() <
        plan_schema->get_schema_version()) {
        ThrowInfo(UnexpectedError,
                  "published schema version {} is older than plan schema "
                  "version {} for segment {}",
                  published_schema->get_schema_version(),
                  plan_schema->get_schema_version(),
                  id_);
    }

    auto plan_primary = plan_schema->get_primary_field_id();
    auto published_primary = published_schema->get_primary_field_id();
    if (plan_primary.has_value() && published_primary.has_value() &&
        plan_primary.value() != published_primary.value()) {
        ThrowInfo(UnexpectedError,
                  "primary field changed from {} to {} across schema "
                  "versions for segment {}",
                  plan_primary.value().get(),
                  published_primary.value().get(),
                  id_);
    }

    for (const auto& [field_id, plan_field] : plan_schema->get_fields()) {
        if (!published_schema->has_field(field_id)) {
            continue;
        }
        const auto& published_field = published_schema->operator[](field_id);
        if (plan_field.get_data_type() != published_field.get_data_type() ||
            plan_field.get_element_type() !=
                published_field.get_element_type()) {
            ThrowInfo(UnexpectedError,
                      "field {} type changed across schema versions for "
                      "segment {}",
                      field_id.get(),
                      id_);
        }
        if (plan_field.is_vector() &&
            !IsSparseFloatVectorDataType(plan_field.get_data_type()) &&
            plan_field.get_dim() != published_field.get_dim()) {
            ThrowInfo(UnexpectedError,
                      "field {} dimension changed from {} to {} across "
                      "schema versions for segment {}",
                      field_id.get(),
                      plan_field.get_dim(),
                      published_field.get_dim(),
                      id_);
        }
    }
}

std::shared_ptr<const SegmentLoadInfo>
ChunkedSegmentSealedImpl::CaptureLoadInfoSnapshot() const {
    return CapturePublishedState()->load_info;
}

std::shared_ptr<const ChunkedSegmentSealedImpl::PublishedSegmentState>
ChunkedSegmentSealedImpl::CapturePublishedState() const {
    return std::atomic_load(&published_state_);
}

std::shared_ptr<const ChunkedSegmentSealedImpl::RuntimeResourceState>
ChunkedSegmentSealedImpl::CaptureRuntimeResourceState() const {
    auto state = CapturePublishedState();
    if (state == nullptr || state->runtime == nullptr) {
        return BuildRuntimeResourceState();
    }
    return state->runtime;
}

std::shared_ptr<const ChunkedSegmentSealedImpl::RuntimeResourceState>
ChunkedSegmentSealedImpl::BuildRuntimeResourceState() {
    auto runtime = std::make_shared<RuntimeResourceState>();
    runtime->skip_index = std::make_shared<SkipIndex>();
    return ToConstRuntimeState(std::move(runtime));
}

std::shared_ptr<ChunkedSegmentSealedImpl::RuntimeResourceState>
ChunkedSegmentSealedImpl::CloneRuntimeResourceState(
    const std::shared_ptr<const RuntimeResourceState>& current) {
    auto state = std::make_shared<RuntimeResourceState>();
    state->skip_index = std::make_shared<SkipIndex>();
    if (!current) {
        return state;
    }
    state->fields = current->fields;
    state->struct_to_array_offsets = current->struct_to_array_offsets;
    state->array_offsets_map = current->array_offsets_map;
    state->scalar_indexings = current->scalar_indexings;
    state->vector_indexings = current->vector_indexings;
    state->vec_binlog_config = current->vec_binlog_config;
    state->ngram_fields = current->ngram_fields;
    state->ngram_indexings = current->ngram_indexings;
    state->text_lob_paths = current->text_lob_paths;
    state->text_indexes = current->text_indexes;
    state->json_indices = current->json_indices;
    state->json_stats = current->json_stats;
    state->reader = current->reader;
    state->timestamps = current->timestamps;
    state->timestamp_index = current->timestamp_index;
    state->timestamp_index_slot = current->timestamp_index_slot;
    state->pk_index_slot = current->pk_index_slot;
    state->virtual_pk2offset = current->virtual_pk2offset;
    state->skip_index =
        current->skip_index ? current->skip_index->Clone() : state->skip_index;
    state->mmap_field_ids = current->mmap_field_ids;
    state->variable_fields_avg_size = current->variable_fields_avg_size;
    state->row_count = current->row_count;
    return state;
}

bool
ChunkedSegmentSealedImpl::IsSystemFieldReadyFromState(
    const PublishedSegmentState& state,
    const SegmentLoadInfo* load_info) const {
    if (state.commit_ts != 0) {
        return true;
    }

    if (state.runtime != nullptr) {
        if (state.runtime->timestamps != nullptr &&
            !state.runtime->timestamps->empty()) {
            return true;
        }
        auto it = state.runtime->fields.find(TimestampFieldID);
        if (it != state.runtime->fields.end() && it->second != nullptr) {
            return true;
        }
    }

    return false;
}

std::shared_ptr<const ChunkedSegmentSealedImpl::PublishedSegmentState>
ChunkedSegmentSealedImpl::BuildPublishedState(
    const SchemaPtr& schema,
    const std::shared_ptr<const SegmentLoadInfo>& load_info,
    Timestamp commit_ts) const {
    auto state = std::make_shared<PublishedSegmentState>();
    state->schema = schema;
    state->load_info = load_info;
    state->runtime = BuildRuntimeResourceState();
    state->commit_ts = commit_ts;
    NormalizePublishedState(*state);
    return state;
}

std::shared_ptr<ChunkedSegmentSealedImpl::RuntimeResourceState>
ChunkedSegmentSealedImpl::CloneMutableRuntimeResourceState() const {
    return CloneRuntimeResourceState(CaptureRuntimeResourceState());
}

std::shared_ptr<milvus_storage::api::Reader>
ChunkedSegmentSealedImpl::CaptureReaderSnapshot() const {
    auto runtime = CaptureRuntimeResourceState();
    return runtime != nullptr ? runtime->reader : nullptr;
}

std::shared_ptr<const TimestampData>
ChunkedSegmentSealedImpl::CaptureTimestampSnapshot() const {
    auto runtime = CaptureRuntimeResourceState();
    return runtime != nullptr ? runtime->timestamps : nullptr;
}

SealedIndexingEntryPtr
ChunkedSegmentSealedImpl::BuildVectorIndexEntry(
    const MetricType& metric_type, index::CacheIndexBasePtr indexing) {
    auto entry = std::make_shared<SealedIndexingEntry>();
    entry->metric_type_ = metric_type;
    entry->indexing_ = std::move(indexing);
    return entry;
}

bool
ChunkedSegmentSealedImpl::RuntimeVectorIndexReady(
    const RuntimeResourceState* runtime, FieldId field_id) {
    return runtime != nullptr && runtime->vector_indexings.find(field_id) !=
                                     runtime->vector_indexings.end();
}

SealedIndexingEntryPtr
ChunkedSegmentSealedImpl::GetVectorIndexing(
    const std::shared_ptr<const RuntimeResourceState>& runtime,
    FieldId field_id) {
    if (runtime == nullptr) {
        return nullptr;
    }
    auto it = runtime->vector_indexings.find(field_id);
    return it != runtime->vector_indexings.end() ? it->second : nullptr;
}

SealedIndexingEntryPtr
ChunkedSegmentSealedImpl::EraseVectorIndexing(RuntimeResourceState& runtime,
                                              FieldId field_id) {
    auto it = runtime.vector_indexings.find(field_id);
    if (it == runtime.vector_indexings.end()) {
        return nullptr;
    }
    auto entry = std::move(it->second);
    runtime.vector_indexings.erase(it);
    return entry;
}

void
ChunkedSegmentSealedImpl::DropVectorIndexing(RuntimeResourceState& runtime,
                                             FieldId field_id) {
    auto entry = EraseVectorIndexing(runtime, field_id);
    if (entry != nullptr && entry->indexing_ != nullptr) {
        entry->indexing_->CancelWarmup();
    }
}

std::vector<index::CacheIndexBasePtr>
ChunkedSegmentSealedImpl::EraseJsonIndexings(RuntimeResourceState& runtime,
                                             FieldId field_id,
                                             std::string_view nested_path) {
    std::vector<index::CacheIndexBasePtr> retired;
    auto new_end = std::remove_if(runtime.json_indices.begin(),
                                  runtime.json_indices.end(),
                                  [&](JsonIndex& index) {
                                      if (index.field_id != field_id ||
                                          index.nested_path != nested_path) {
                                          return false;
                                      }
                                      retired.push_back(std::move(index.index));
                                      return true;
                                  });
    runtime.json_indices.erase(new_end, runtime.json_indices.end());
    return retired;
}

index::CacheIndexBasePtr
ChunkedSegmentSealedImpl::EraseJsonNgramIndexing(RuntimeResourceState& runtime,
                                                 FieldId field_id,
                                                 std::string_view nested_path) {
    auto field_it = runtime.ngram_indexings.find(field_id);
    if (field_it == runtime.ngram_indexings.end()) {
        return nullptr;
    }

    auto& path_indexings = field_it->second;
    auto path_it = path_indexings.find(std::string(nested_path));
    if (path_it == path_indexings.end()) {
        return nullptr;
    }

    auto retired = std::move(path_it->second);
    path_indexings.erase(path_it);
    if (path_indexings.empty()) {
        runtime.ngram_indexings.erase(field_it);
    }
    return retired;
}

std::vector<index::CacheIndexBasePtr>
ChunkedSegmentSealedImpl::EraseJsonIndexesAtPath(RuntimeResourceState& runtime,
                                                 FieldId field_id,
                                                 std::string_view nested_path) {
    auto retired = EraseJsonIndexings(runtime, field_id, nested_path);
    if (auto ngram = EraseJsonNgramIndexing(runtime, field_id, nested_path);
        ngram != nullptr) {
        retired.push_back(std::move(ngram));
    }
    return retired;
}

bool
ChunkedSegmentSealedImpl::RuntimeJsonNgramIndexReady(
    const RuntimeResourceState& runtime, FieldId field_id) {
    auto it = runtime.ngram_indexings.find(field_id);
    return it != runtime.ngram_indexings.end() && !it->second.empty();
}

void
ChunkedSegmentSealedImpl::SyncJsonNgramIndexState(
    PublishedSegmentState& state,
    const RuntimeResourceState& runtime,
    FieldId field_id) {
    if (RuntimeJsonNgramIndexReady(runtime, field_id)) {
        set_bit(state.published_index_ready_bitset, field_id, true);
        SetPublishedIndexRawDataInState(state, field_id, false);
        return;
    }

    clear_bit_if_present(state.published_index_ready_bitset, field_id);
    clear_bit_if_present(state.index_ready_bitset, field_id);
    if (!get_bit_if_present(state.published_binlog_index_ready_bitset,
                            field_id)) {
        ClearPublishedIndexRawDataInState(state, field_id);
        ClearIndexRawDataInState(state, field_id);
    }
}

std::shared_ptr<ChunkedSegmentSealedImpl::PublishedSegmentState>
ChunkedSegmentSealedImpl::ClonePublishedState(
    const std::shared_ptr<const PublishedSegmentState>& current) const {
    auto state = std::make_shared<PublishedSegmentState>();
    if (!current) {
        state->runtime = BuildRuntimeResourceState();
        return state;
    }
    state->schema = current->schema;
    state->load_info = current->load_info;
    state->runtime =
        current->runtime ? current->runtime : BuildRuntimeResourceState();
    state->commit_ts = current->commit_ts;
    state->use_take_for_output = current->use_take_for_output;
    state->system_field_ready = current->system_field_ready;
    state->published_index_ready_bitset =
        current->published_index_ready_bitset.clone();
    state->published_binlog_index_ready_bitset =
        current->published_binlog_index_ready_bitset.clone();
    state->published_index_has_raw_data = current->published_index_has_raw_data;
    state->field_data_ready_bitset = current->field_data_ready_bitset.clone();
    state->index_ready_bitset = current->index_ready_bitset.clone();
    state->binlog_index_bitset = current->binlog_index_bitset.clone();
    state->index_has_raw_data = current->index_has_raw_data;
    return state;
}

void
ChunkedSegmentSealedImpl::ApplyDeltaToState(PublishedSegmentState& state,
                                            const StateDelta& delta) const {
    if (delta.schema.has_value()) {
        state.schema = *delta.schema;
    }
    if (delta.load_info.has_value()) {
        state.load_info = *delta.load_info;
    }
    if (delta.runtime.has_value()) {
        state.runtime = *delta.runtime;
    }
    if (delta.commit_ts.has_value()) {
        state.commit_ts = *delta.commit_ts;
    }
    if (delta.published_index_ready_bitset.has_value()) {
        state.published_index_ready_bitset =
            delta.published_index_ready_bitset->clone();
    }
    if (delta.published_binlog_index_ready_bitset.has_value()) {
        state.published_binlog_index_ready_bitset =
            delta.published_binlog_index_ready_bitset->clone();
    }
    if (delta.published_index_has_raw_data.has_value()) {
        state.published_index_has_raw_data =
            *delta.published_index_has_raw_data;
    }
}

void
ChunkedSegmentSealedImpl::NormalizePublishedState(
    PublishedSegmentState& state) const {
    state.use_take_for_output =
        state.load_info != nullptr && state.load_info->GetUseTakeForOutput();

    state.system_field_ready = false;
    state.field_data_ready_bitset.reset();
    state.index_ready_bitset.reset();
    state.binlog_index_bitset.reset();
    state.index_has_raw_data.clear();

    if (state.schema) {
        ResizeStateBitsets(state, *state.schema);
        if (state.runtime != nullptr) {
            for (const auto& [field_id, column] : state.runtime->fields) {
                if (column == nullptr ||
                    SystemProperty::Instance().IsSystem(field_id) ||
                    !field_exists_in_schema(state.schema, field_id)) {
                    continue;
                }
                set_bit(state.field_data_ready_bitset, field_id, true);
            }

            for (const auto& [field_id, _] : state.runtime->scalar_indexings) {
                if (!field_exists_in_schema(state.schema, field_id)) {
                    continue;
                }
                set_bit(state.index_ready_bitset, field_id, true);
                auto raw_it = state.published_index_has_raw_data.find(field_id);
                if (raw_it != state.published_index_has_raw_data.end()) {
                    state.index_has_raw_data[field_id] = raw_it->second;
                }
            }

            for (const auto& field_id : state.runtime->ngram_fields) {
                if (!field_exists_in_schema(state.schema, field_id)) {
                    continue;
                }
                set_bit(state.index_ready_bitset, field_id, true);
                auto raw_it = state.published_index_has_raw_data.find(field_id);
                if (raw_it != state.published_index_has_raw_data.end()) {
                    state.index_has_raw_data[field_id] = raw_it->second;
                }
            }

            for (const auto& [field_id, path_indexings] :
                 state.runtime->ngram_indexings) {
                if (path_indexings.empty() ||
                    !field_exists_in_schema(state.schema, field_id)) {
                    continue;
                }
                set_bit(state.index_ready_bitset, field_id, true);
                state.index_has_raw_data[field_id] = false;
            }
        }

        for (size_t i = 0; i < state.published_index_ready_bitset.size(); ++i) {
            auto field_id =
                FieldId(START_USER_FIELDID + static_cast<int64_t>(i));
            if (!field_exists_in_schema(state.schema, field_id) ||
                !state.published_index_ready_bitset[i]) {
                continue;
            }
            set_bit(state.index_ready_bitset, field_id, true);
            auto raw_it = state.published_index_has_raw_data.find(field_id);
            if (raw_it != state.published_index_has_raw_data.end()) {
                state.index_has_raw_data[field_id] = raw_it->second;
            }
        }

        for (size_t i = 0; i < state.published_binlog_index_ready_bitset.size();
             ++i) {
            auto field_id =
                FieldId(START_USER_FIELDID + static_cast<int64_t>(i));
            if (!field_exists_in_schema(state.schema, field_id) ||
                !state.published_binlog_index_ready_bitset[i]) {
                continue;
            }
            set_bit(state.binlog_index_bitset, field_id, true);
            auto raw_it = state.published_index_has_raw_data.find(field_id);
            if (raw_it != state.published_index_has_raw_data.end()) {
                state.index_has_raw_data[field_id] = raw_it->second;
            }
        }
    }
    ClearFieldBitsForAbsentLoadInfo(state);
    SetSystemFieldReadyInState(state, state.load_info.get());
}

std::shared_ptr<ChunkedSegmentSealedImpl::PublishedSegmentState>
ChunkedSegmentSealedImpl::BuildNextPublishedState(
    const std::shared_ptr<const PublishedSegmentState>& current,
    const StateDelta& delta) const {
    auto next = ClonePublishedState(current);
    ApplyDeltaToState(*next, delta);
    NormalizePublishedState(*next);
    return next;
}

ChunkedSegmentSealedImpl::StateDelta
ChunkedSegmentSealedImpl::MakeStateDelta(
    const SchemaPtr& schema_snapshot,
    const std::shared_ptr<const SegmentLoadInfo>& load_info,
    Timestamp commit_ts) {
    StateDelta delta;
    delta.schema = schema_snapshot;
    delta.load_info = load_info;
    delta.commit_ts = commit_ts;
    return delta;
}

ChunkedSegmentSealedImpl::StateDelta
ChunkedSegmentSealedImpl::MakeStateDelta(
    const SchemaPtr& schema_snapshot,
    const std::shared_ptr<const SegmentLoadInfo>& load_info,
    const std::shared_ptr<const RuntimeResourceState>& runtime,
    Timestamp commit_ts) {
    StateDelta delta;
    delta.schema = schema_snapshot;
    delta.load_info = load_info;
    delta.runtime = runtime;
    delta.commit_ts = commit_ts;
    return delta;
}

std::shared_ptr<const ChunkedSegmentSealedImpl::RuntimeResourceState>
ChunkedSegmentSealedImpl::ToConstRuntimeState(
    std::shared_ptr<RuntimeResourceState> runtime) {
    return std::const_pointer_cast<const RuntimeResourceState>(
        std::move(runtime));
}

std::shared_ptr<const ChunkedSegmentSealedImpl::RuntimeResourceState>
ChunkedSegmentSealedImpl::FreezeRuntimeResourceState(
    const RuntimeResourceState& current) {
    auto runtime = std::make_shared<RuntimeResourceState>();
    runtime->fields = current.fields;
    runtime->struct_to_array_offsets = current.struct_to_array_offsets;
    runtime->array_offsets_map = current.array_offsets_map;
    runtime->scalar_indexings = current.scalar_indexings;
    runtime->vector_indexings = current.vector_indexings;
    runtime->vec_binlog_config = current.vec_binlog_config;
    runtime->ngram_fields = current.ngram_fields;
    runtime->ngram_indexings = current.ngram_indexings;
    runtime->text_lob_paths = current.text_lob_paths;
    runtime->text_indexes = current.text_indexes;
    runtime->json_indices = current.json_indices;
    runtime->json_stats = current.json_stats;
    runtime->reader = current.reader;
    runtime->timestamps = current.timestamps;
    runtime->timestamp_index = current.timestamp_index;
    runtime->timestamp_index_slot = current.timestamp_index_slot;
    runtime->pk_index_slot = current.pk_index_slot;
    runtime->virtual_pk2offset = current.virtual_pk2offset;
    runtime->skip_index = current.skip_index ? current.skip_index->Clone()
                                             : std::make_shared<SkipIndex>();
    runtime->mmap_field_ids = current.mmap_field_ids;
    runtime->variable_fields_avg_size = current.variable_fields_avg_size;
    runtime->row_count = current.row_count;
    return ToConstRuntimeState(std::move(runtime));
}

std::shared_ptr<const SegmentLoadInfo>
ChunkedSegmentSealedImpl::CloneLoadInfoWithDefaultFilled(
    const std::shared_ptr<const SegmentLoadInfo>& current,
    const std::vector<FieldId>& field_ids) {
    auto load_info_copy = std::make_shared<SegmentLoadInfo>(*current);
    for (auto field_id : field_ids) {
        load_info_copy->SetFieldFilledWithDefault(field_id);
    }
    return std::const_pointer_cast<const SegmentLoadInfo>(load_info_copy);
}

std::shared_ptr<const SegmentLoadInfo>
ChunkedSegmentSealedImpl::CloneLoadInfoWithTextIndexCreated(
    const std::shared_ptr<const SegmentLoadInfo>& current, FieldId field_id) {
    auto load_info_copy = std::make_shared<SegmentLoadInfo>(*current);
    load_info_copy->SetTextIndexCreated(field_id);
    return std::const_pointer_cast<const SegmentLoadInfo>(load_info_copy);
}

std::shared_ptr<const SegmentLoadInfo>
ChunkedSegmentSealedImpl::CloneLoadInfoForReopen(
    const SegmentLoadInfo& load_info, const SchemaPtr& schema_snapshot) {
    return std::make_shared<const SegmentLoadInfo>(load_info.GetProto(),
                                                   schema_snapshot);
}

bool
ChunkedSegmentSealedImpl::HasIndexRawDataFromState(
    const PublishedSegmentState& state, FieldId field_id) {
    auto it = state.index_has_raw_data.find(field_id);
    return it != state.index_has_raw_data.end() && it->second;
}

void
ChunkedSegmentSealedImpl::SetIndexRawDataInState(PublishedSegmentState& state,
                                                 FieldId field_id,
                                                 bool has_raw_data) {
    state.index_has_raw_data[field_id] = has_raw_data;
}

bool
ChunkedSegmentSealedImpl::HasPublishedIndexRawDataFromState(
    const PublishedSegmentState& state, FieldId field_id) {
    auto it = state.published_index_has_raw_data.find(field_id);
    return it != state.published_index_has_raw_data.end() && it->second;
}

void
ChunkedSegmentSealedImpl::SetPublishedIndexRawDataInState(
    PublishedSegmentState& state, FieldId field_id, bool has_raw_data) {
    state.published_index_has_raw_data[field_id] = has_raw_data;
}

void
ChunkedSegmentSealedImpl::ClearIndexRawDataInState(PublishedSegmentState& state,
                                                   FieldId field_id) {
    state.index_has_raw_data.erase(field_id);
}

void
ChunkedSegmentSealedImpl::ClearPublishedIndexRawDataInState(
    PublishedSegmentState& state, FieldId field_id) {
    state.published_index_has_raw_data.erase(field_id);
}

void
ChunkedSegmentSealedImpl::ClearFieldBitsForAbsentLoadInfo(
    PublishedSegmentState& state) {
    if (!state.load_info) {
        state.published_index_ready_bitset.reset();
        state.published_binlog_index_ready_bitset.reset();
        state.published_index_has_raw_data.clear();
        state.field_data_ready_bitset.reset();
        state.index_ready_bitset.reset();
        state.binlog_index_bitset.reset();
        state.index_has_raw_data.clear();
        return;
    }

    for (size_t i = 0; i < state.field_data_ready_bitset.size(); ++i) {
        auto field_id = FieldId(START_USER_FIELDID + static_cast<int64_t>(i));
        if (state.load_info->HasFieldInSchema(field_id)) {
            continue;
        }
        state.published_index_ready_bitset[i] = false;
        state.published_binlog_index_ready_bitset[i] = false;
        state.field_data_ready_bitset[i] = false;
        state.index_ready_bitset[i] = false;
        state.binlog_index_bitset[i] = false;
        state.published_index_has_raw_data.erase(field_id);
        state.index_has_raw_data.erase(field_id);
    }
}

void
ChunkedSegmentSealedImpl::ResizeStateBitsets(PublishedSegmentState& state,
                                             const Schema& schema) {
    auto size = schema.get_field_id_bitset_size();
    state.published_index_ready_bitset.resize(size);
    state.published_binlog_index_ready_bitset.resize(size);
    state.field_data_ready_bitset.resize(size);
    state.index_ready_bitset.resize(size);
    state.binlog_index_bitset.resize(size);
}

void
ChunkedSegmentSealedImpl::SetSystemFieldReadyInState(
    PublishedSegmentState& state, const SegmentLoadInfo* load_info) const {
    state.system_field_ready = IsSystemFieldReadyFromState(state, load_info);
}

void
ChunkedSegmentSealedImpl::DropFieldFromState(PublishedSegmentState& state,
                                             FieldId field_id) {
    clear_bit_if_present(state.field_data_ready_bitset, field_id);
    clear_bit_if_present(state.published_binlog_index_ready_bitset, field_id);
    clear_bit_if_present(state.binlog_index_bitset, field_id);
    if (!get_bit_if_present(state.index_ready_bitset, field_id) &&
        !get_bit_if_present(state.binlog_index_bitset, field_id)) {
        ClearPublishedIndexRawDataInState(state, field_id);
        ClearIndexRawDataInState(state, field_id);
    }
}

void
ChunkedSegmentSealedImpl::DropIndexFromState(PublishedSegmentState& state,
                                             FieldId field_id) {
    clear_bit_if_present(state.published_index_ready_bitset, field_id);
    clear_bit_if_present(state.index_ready_bitset, field_id);
    if (!get_bit_if_present(state.index_ready_bitset, field_id) &&
        !get_bit_if_present(state.binlog_index_bitset, field_id)) {
        ClearPublishedIndexRawDataInState(state, field_id);
        ClearIndexRawDataInState(state, field_id);
    }
}

void
ChunkedSegmentSealedImpl::ClearState(PublishedSegmentState& state) {
    state.system_field_ready = false;
    state.runtime = nullptr;
    state.published_index_ready_bitset.reset();
    state.published_binlog_index_ready_bitset.reset();
    state.published_index_has_raw_data.clear();
    state.field_data_ready_bitset.reset();
    state.index_ready_bitset.reset();
    state.binlog_index_bitset.reset();
    state.index_has_raw_data.clear();
}

void
ChunkedSegmentSealedImpl::PublishReopenState(
    const std::shared_ptr<const PublishedSegmentState>& current,
    const StateDelta& delta) {
    PublishStateOnline(BuildNextPublishedState(current, delta));
}

void
ChunkedSegmentSealedImpl::PublishReopenState(
    const std::shared_ptr<const PublishedSegmentState>& current,
    const SchemaPtr& sch,
    const std::shared_ptr<const SegmentLoadInfo>& published) {
    auto schema_snapshot = sch ? sch : current->schema;
    Timestamp commit_ts = 0;
    {
        std::shared_lock lck(mutex_);
        commit_ts = commit_ts_;
    }
    auto load_info = published ? published : current->load_info;
    PublishReopenState(current,
                       MakeStateDelta(schema_snapshot, load_info, commit_ts));
}

void
ChunkedSegmentSealedImpl::PublishReopenState(const StateDelta& delta) {
    PublishReopenState(CapturePublishedState(), delta);
}

void
ChunkedSegmentSealedImpl::PublishReopenState(
    const SchemaPtr& sch,
    const std::shared_ptr<const SegmentLoadInfo>& published) {
    auto current = CapturePublishedState();
    auto current_schema = current->schema;
    SchemaPtr schema_snapshot = sch ? sch : current_schema;

    if (schema_snapshot && schema_snapshot->get_schema_version() <=
                               current_schema->get_schema_version()) {
        schema_snapshot = current_schema;
    }

    Timestamp commit_ts = 0;
    {
        std::shared_lock lck(mutex_);
        commit_ts = commit_ts_;
    }
    auto load_info = published ? published : current->load_info;
    PublishReopenState(current,
                       MakeStateDelta(schema_snapshot, load_info, commit_ts));
}

bool
ChunkedSegmentSealedImpl::HasRawDataFromState(
    const PublishedSegmentState& state, FieldId field_id) const {
    const auto& field_meta = state.schema->operator[](field_id);
    if (IsVectorDataType(field_meta.get_data_type())) {
        if (get_bit(state.index_ready_bitset, field_id)) {
            return HasIndexRawDataFromState(state, field_id);
        }
        if (get_bit(state.binlog_index_bitset, field_id)) {
            return HasIndexRawDataFromState(state, field_id) ||
                   get_bit(state.field_data_ready_bitset, field_id);
        }
        return true;
    }
    if (IsJsonDataType(field_meta.get_data_type())) {
        return get_bit(state.field_data_ready_bitset, field_id);
    }
    if (get_bit(state.index_ready_bitset, field_id) ||
        get_bit(state.binlog_index_bitset, field_id)) {
        return HasIndexRawDataFromState(state, field_id);
    }
    return true;
}

bool
ChunkedSegmentSealedImpl::IndexHasRawDataFromState(
    const PublishedSegmentState& state, FieldId field_id) const {
    if (!get_bit_if_present(state.index_ready_bitset, field_id) &&
        !get_bit_if_present(state.binlog_index_bitset, field_id)) {
        return false;
    }
    return HasIndexRawDataFromState(state, field_id);
}

void
ChunkedSegmentSealedImpl::SetUseTakeForOutputForTestingLocked(bool val) {
    auto current = CapturePublishedState();
    auto next = ClonePublishedState(current);
    next->use_take_for_output = val;
    PublishStateOnline(std::move(next));
}

void
ChunkedSegmentSealedImpl::SetUseTakeForOutputForTestingImpl(bool val) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    SetUseTakeForOutputForTestingLocked(val);
}

void
ChunkedSegmentSealedImpl::MarkSystemFieldReadyLocked(bool value) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        state.system_field_ready = value;
        SetSystemFieldReadyInState(state, state.load_info.get());
    });
}

void
ChunkedSegmentSealedImpl::MarkFieldDataReadyLocked(FieldId field_id,
                                                   bool value) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (value) {
            set_bit(state.field_data_ready_bitset, field_id, true);
        } else {
            clear_bit_if_present(state.field_data_ready_bitset, field_id);
        }
    });
}

void
ChunkedSegmentSealedImpl::MarkIndexReadyLocked(FieldId field_id, bool value) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (value) {
            set_bit(state.published_index_ready_bitset, field_id, true);
        } else {
            clear_bit_if_present(state.published_index_ready_bitset, field_id);
            if (!get_bit(state.published_binlog_index_ready_bitset, field_id)) {
                ClearPublishedIndexRawDataInState(state, field_id);
            }
        }
    });
}

void
ChunkedSegmentSealedImpl::MarkBinlogIndexReadyLocked(FieldId field_id,
                                                     bool value) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (value) {
            set_bit(state.published_binlog_index_ready_bitset, field_id, true);
        } else {
            clear_bit_if_present(state.published_binlog_index_ready_bitset,
                                 field_id);
            if (!get_bit(state.published_index_ready_bitset, field_id)) {
                ClearPublishedIndexRawDataInState(state, field_id);
            }
        }
    });
}

void
ChunkedSegmentSealedImpl::MarkIndexHasRawDataLocked(FieldId field_id,
                                                    bool has_raw_data) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        SetPublishedIndexRawDataInState(state, field_id, has_raw_data);
    });
}

void
ChunkedSegmentSealedImpl::ClearIndexHasRawDataLocked(FieldId field_id) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        ClearPublishedIndexRawDataInState(state, field_id);
        ClearIndexRawDataInState(state, field_id);
    });
}

void
ChunkedSegmentSealedImpl::ResizeStateBitsetsLocked(
    const SchemaPtr& schema_snapshot) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (schema_snapshot) {
            state.schema = schema_snapshot;
            ResizeStateBitsets(state, *schema_snapshot);
        }
    });
}

void
ChunkedSegmentSealedImpl::ClearPublishedStateLocked() {
    MutatePublishedStateLocked(
        [&](PublishedSegmentState& state) { ClearState(state); });
}

void
ChunkedSegmentSealedImpl::PublishSystemFieldStateLocked() {
    MarkSystemFieldReadyLocked(true);
}

void
ChunkedSegmentSealedImpl::PublishFieldDataReadyLocked(
    FieldId field_id,
    const std::shared_ptr<const RuntimeResourceState>& runtime) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (runtime != nullptr) {
            state.runtime = runtime;
        }
        set_bit(state.field_data_ready_bitset, field_id, true);
    });
}

void
ChunkedSegmentSealedImpl::PublishIndexReadyLocked(
    FieldId field_id,
    bool has_raw_data,
    const std::shared_ptr<const RuntimeResourceState>& runtime) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (runtime != nullptr) {
            state.runtime = runtime;
        }
        clear_bit_if_present(state.published_binlog_index_ready_bitset,
                             field_id);
        set_bit(state.published_index_ready_bitset, field_id, true);
        SetPublishedIndexRawDataInState(state, field_id, has_raw_data);
    });
}

void
ChunkedSegmentSealedImpl::PublishBinlogIndexReadyLocked(
    FieldId field_id,
    bool has_raw_data,
    const std::shared_ptr<const RuntimeResourceState>& runtime) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (runtime != nullptr) {
            state.runtime = runtime;
        }
        clear_bit_if_present(state.published_index_ready_bitset, field_id);
        set_bit(state.published_binlog_index_ready_bitset, field_id, true);
        SetPublishedIndexRawDataInState(state, field_id, has_raw_data);
    });
}

void
ChunkedSegmentSealedImpl::PublishVectorIndexFactsLocked(
    FieldId field_id,
    bool ready,
    bool binlog_ready,
    std::optional<bool> has_raw_data) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (ready) {
            set_bit(state.published_index_ready_bitset, field_id, true);
        } else {
            clear_bit_if_present(state.published_index_ready_bitset, field_id);
        }
        if (binlog_ready) {
            set_bit(state.published_binlog_index_ready_bitset, field_id, true);
        } else {
            clear_bit_if_present(state.published_binlog_index_ready_bitset,
                                 field_id);
        }
        if (has_raw_data.has_value()) {
            SetPublishedIndexRawDataInState(state, field_id, *has_raw_data);
        } else if (!ready && !binlog_ready) {
            ClearPublishedIndexRawDataInState(state, field_id);
        }
    });
}

void
ChunkedSegmentSealedImpl::PublishFieldDroppedLocked(
    FieldId field_id,
    const std::shared_ptr<const RuntimeResourceState>& runtime) {
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        if (runtime != nullptr) {
            state.runtime = runtime;
        }
        DropFieldFromState(state, field_id);
    });
}

void
ChunkedSegmentSealedImpl::PublishIndexDroppedLocked(
    FieldId field_id,
    const std::shared_ptr<const RuntimeResourceState>& runtime,
    milvus::OpContext* op_ctx) {
    MutatePublishedStateLocked(
        [&](PublishedSegmentState& state) {
            if (runtime != nullptr) {
                state.runtime = runtime;
            }
            DropIndexFromState(state, field_id);
        },
        op_ctx);
}

void
ChunkedSegmentSealedImpl::PublishRuntimeStateLocked(
    const std::shared_ptr<const RuntimeResourceState>& runtime) {
    if (runtime == nullptr) {
        return;
    }
    auto current = CapturePublishedState();
    PublishStateOnline(BuildNextPublishedState(
        current,
        MakeStateDelta(
            current->schema, current->load_info, runtime, current->commit_ts)));
}
PinWrapper<index::TextMatchIndex*>
ChunkedSegmentSealedImpl::GetTextIndex(milvus::OpContext* op_ctx,
                                       FieldId field_id) const {
    auto snapshot = CapturePublishedState();
    auto runtime = snapshot != nullptr ? snapshot->runtime : nullptr;
    if (runtime == nullptr) {
        ThrowInfo(milvus::ErrorCode::TextIndexNotFound,
                  "text index not found for field {}",
                  field_id.get());
    }

    auto iter = runtime->text_indexes.find(field_id);
    if (iter == runtime->text_indexes.end()) {
        ThrowInfo(milvus::ErrorCode::TextIndexNotFound,
                  "text index not found for field {}",
                  field_id.get());
    }

    auto make_pin = [&](auto&& alt) -> PinWrapper<index::TextMatchIndex*> {
        using Alt = std::decay_t<decltype(alt)>;
        if constexpr (std::is_same_v<
                          Alt,
                          std::shared_ptr<
                              milvus::index::TextMatchIndexHolder>>) {
            return PinWrapper<index::TextMatchIndex*>(alt, alt->get());
        } else if constexpr (std::is_same_v<
                                 Alt,
                                 std::shared_ptr<
                                     milvus::cachinglayer::CacheSlot<
                                         milvus::index::TextMatchIndex>>>) {
            auto ca = SemiInlineGet(alt->PinCells(op_ctx, {0}));
            auto index = ca->get_cell_of(0);
            return PinWrapper<index::TextMatchIndex*>(std::move(ca), index);
        } else {
            ThrowInfo(milvus::ErrorCode::UnexpectedError,
                      "text index of segment is not supported for field {}",
                      field_id.get());
        }
    };

    return std::visit(make_pin, iter->second);
}

std::shared_ptr<index::JsonKeyStats>
ChunkedSegmentSealedImpl::GetJsonStats(milvus::OpContext* op_ctx,
                                       FieldId field_id) const {
    auto runtime = CaptureRuntimeResourceState();
    if (runtime == nullptr) {
        return nullptr;
    }
    auto iter = runtime->json_stats.find(field_id);
    if (iter == runtime->json_stats.end()) {
        return nullptr;
    }
    return iter->second;
}

void
ChunkedSegmentSealedImpl::RefreshPublishedLoadInfoLocked(
    const std::shared_ptr<const SegmentLoadInfo>& load_info,
    Timestamp commit_ts) {
    auto current = CapturePublishedState();
    PublishStateOnline(BuildNextPublishedState(
        current, MakeStateDelta(current->schema, load_info, commit_ts)));
}

void
ChunkedSegmentSealedImpl::RefreshPublishedSchemaLocked(
    const SchemaPtr& schema_snapshot) {
    auto current = CapturePublishedState();
    PublishStateOnline(BuildNextPublishedState(
        current,
        MakeStateDelta(
            schema_snapshot, current->load_info, current->commit_ts)));
}

void
ChunkedSegmentSealedImpl::RefreshPublishedStateLocked(
    const SchemaPtr& schema_snapshot,
    const std::shared_ptr<const SegmentLoadInfo>& load_info,
    Timestamp commit_ts) {
    auto current = CapturePublishedState();
    PublishStateOnline(BuildNextPublishedState(
        current, MakeStateDelta(schema_snapshot, load_info, commit_ts)));
}

void
ChunkedSegmentSealedImpl::PrepareMutableStateForPublish(
    const SchemaPtr& schema_snapshot,
    Timestamp commit_ts,
    std::shared_ptr<PublishedSegmentState>& next) const {
    auto current = CapturePublishedState();
    next = BuildNextPublishedState(
        current,
        MakeStateDelta(schema_snapshot, current->load_info, commit_ts));
}

void
ChunkedSegmentSealedImpl::PublishState(
    PublishLease& publish_lease,
    const std::shared_ptr<const PublishedSegmentState>& state) {
    if (!state) {
        return;
    }
    AssertInfo(publish_lease.valid(), "online publication requires a lease");
    std::atomic_store(&published_state_, state);
    publish_lease.MarkPublished();
}

void
ChunkedSegmentSealedImpl::PublishState(
    PublishLease& publish_lease, std::shared_ptr<PublishedSegmentState> state) {
    PublishState(
        publish_lease,
        std::const_pointer_cast<const PublishedSegmentState>(std::move(state)));
}

void
ChunkedSegmentSealedImpl::PublishStateOnline(
    const std::shared_ptr<const PublishedSegmentState>& state,
    milvus::OpContext* op_ctx,
    PublishMode publish_mode) {
    if (!state) {
        return;
    }
    auto publish_lease =
        publish_mode == PublishMode::FailFast
            ? operation_gate_.AcquirePublishFailFast(op_ctx, id_)
            : operation_gate_.AcquirePublish(op_ctx, id_);
    PublishState(publish_lease, state);
}

void
ChunkedSegmentSealedImpl::PublishStateOnline(
    std::shared_ptr<PublishedSegmentState> state,
    milvus::OpContext* op_ctx,
    PublishMode publish_mode) {
    PublishStateOnline(
        std::const_pointer_cast<const PublishedSegmentState>(std::move(state)),
        op_ctx,
        publish_mode);
}

void
ChunkedSegmentSealedImpl::LoadFieldData(
    const LoadFieldDataInfo& load_info,
    const SegmentLoadInfo& segment_load_info,
    milvus::OpContext* op_ctx,
    bool is_replace,
    const SchemaPtr& schema_snapshot,
    RuntimeResourceState* runtime) {
    switch (load_info.storage_version) {
        case 2: {
            load_column_group_data_internal(load_info,
                                            segment_load_info,
                                            schema_snapshot,
                                            op_ctx,
                                            is_replace,
                                            runtime);
            break;
        }
        default:
            load_field_data_internal(load_info,
                                     segment_load_info,
                                     schema_snapshot,
                                     op_ctx,
                                     is_replace,
                                     runtime);
            break;
    }
}

void
ChunkedSegmentSealedImpl::LoadFieldData(
    const LoadFieldDataInfo& load_info,
    const SegmentLoadInfo& segment_load_info,
    milvus::OpContext* op_ctx,
    bool is_replace,
    const SchemaPtr& schema_snapshot,
    StagedStateCommitter& committer) {
    switch (load_info.storage_version) {
        case 2: {
            load_column_group_data_internal(load_info,
                                            segment_load_info,
                                            schema_snapshot,
                                            op_ctx,
                                            is_replace,
                                            committer);
            break;
        }
        default:
            load_field_data_internal(load_info,
                                     segment_load_info,
                                     schema_snapshot,
                                     op_ctx,
                                     is_replace,
                                     committer);
            break;
    }
}

void
ChunkedSegmentSealedImpl::LoadFieldData(const LoadFieldDataInfo& load_info,
                                        milvus::OpContext* op_ctx,
                                        bool is_replace) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    auto snapshot = CapturePublishedState();
    LoadFieldData(load_info,
                  *snapshot->load_info,
                  op_ctx,
                  is_replace,
                  snapshot->schema,
                  nullptr);
}

void
ChunkedSegmentSealedImpl::LoadColumnGroups(
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    StagedStateCommitter& committer) {
    auto load_cg_start = std::chrono::high_resolution_clock::now();
    CheckCancellation(
        op_ctx, id_, "ChunkedSegmentSealedImpl::LoadColumnGroups()");
    auto properties = std::make_shared<milvus_storage::api::Properties>(
        *milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
             .GetProperties());
    auto column_groups = segment_load_info.GetColumnGroups();

    if (schema_snapshot->is_external_collection()) {
        InjectExternalSpecProperties(*properties,
                                     segment_load_info.GetCollectionID(),
                                     schema_snapshot->get_external_source(),
                                     schema_snapshot->get_external_spec());
    }

    auto needed_columns = schema_snapshot->GetExternalColumnNames();
    auto reader = std::shared_ptr<milvus_storage::api::Reader>(
        milvus_storage::api::Reader::create(column_groups,
                                            /*arrow_schema=*/nullptr,
                                            needed_columns,
                                            *properties)
            .release());
    committer.Commit(
        [reader = std::move(reader)](RuntimeResourceState& runtime,
                                     PublishedSegmentState&) mutable {
            runtime.reader = std::move(reader);
        });

    auto reader_create_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - load_cg_start)
            .count();
    LOG_INFO(
        "[LoadColumnGroups] segment {} reader created in {}ms, {} column "
        "groups",
        id_,
        reader_create_ms,
        column_groups->size());

    // A manifest column whose field was dropped from the schema is a legal
    // leftover of drop semantics: filter it out (a group filtered to empty
    // produces no load task) instead of tripping the schema lookup on it.
    std::vector<std::pair<int, std::vector<FieldId>>> cg_field_ids;
    cg_field_ids.reserve(column_groups->size());
    for (size_t i = 0; i < column_groups->size(); ++i) {
        auto cg = column_groups->at(i);
        std::vector<FieldId> field_ids;
        std::vector<int64_t> dropped_fields;
        field_ids.reserve(cg->columns.size());
        for (auto& column : cg->columns) {
            auto field_id = schema_snapshot->ResolveColumnFieldId(column);
            if (!schema_snapshot->has_field(field_id)) {
                dropped_fields.push_back(field_id.get());
                continue;
            }
            field_ids.emplace_back(field_id);
        }
        if (!dropped_fields.empty()) {
            LOG_INFO(
                "segment {} skips dropped fields {} of column group {} on "
                "load",
                id_,
                fmt::format("{}", dropped_fields),
                i);
        }
        cg_field_ids.emplace_back(static_cast<int>(i), std::move(field_ids));
    }

    struct FieldGroupTask {
        int cg_index;
        std::vector<FieldId> field_ids;
        bool eager_load;
    };
    std::vector<FieldGroupTask> tasks;
    for (const auto& pair : cg_field_ids) {
        auto cg_index = pair.first;
        const auto& all_fields = pair.second;

        std::vector<FieldId> eager_fields;
        std::vector<FieldId> lazy_fields;

        for (const auto& field_id : all_fields) {
            const auto& field_meta = (*schema_snapshot)[field_id];
            bool field_is_vector = IsVectorDataType(field_meta.get_data_type());
            const auto pk_field_id = schema_snapshot->get_primary_field_id();
            if (pk_field_id.has_value() &&
                pk_field_id.value().get() == field_id.get() &&
                schema_snapshot->IsExternalDataField(field_id)) {
                eager_fields.push_back(field_id);
                continue;
            }
            auto warmup_str = resolve_field_data_warmup_policy(
                field_id, segment_load_info, schema_snapshot);
            auto resolved = getCacheWarmupPolicy(warmup_str,
                                                 field_is_vector,
                                                 /*is_index=*/false,
                                                 /*in_load_list=*/true);
            if (resolved != CacheWarmupPolicy::CacheWarmupPolicy_Disable) {
                eager_fields.push_back(field_id);
            } else {
                lazy_fields.push_back(field_id);
            }
        }

        if (!eager_fields.empty()) {
            tasks.push_back({cg_index, std::move(eager_fields), true});
        }
        for (const auto& fid : lazy_fields) {
            tasks.push_back({cg_index, {fid}, false});
        }
        LOG_INFO(
            "[LoadColumnGroups] segment {} cg {} fields={} eager_fields={} "
            "lazy_fields={}",
            get_segment_id(),
            cg_index,
            FormatFieldIds(all_fields),
            FormatFieldIds(eager_fields),
            FormatFieldIds(lazy_fields));
    }

    LOG_INFO(
        "[LoadColumnGroups] segment {} external table: {} tasks from {} column "
        "groups",
        get_segment_id(),
        tasks.size(),
        cg_field_ids.size());

    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> load_group_futures;
    for (auto& task : tasks) {
        auto future = pool.Submit([this,
                                   column_groups,
                                   properties,
                                   cg_index = task.cg_index,
                                   field_ids = std::move(task.field_ids),
                                   &segment_load_info,
                                   schema_snapshot,
                                   eager_load = task.eager_load,
                                   op_ctx,
                                   &committer] {
            CheckCancellation(op_ctx,
                              id_,
                              cg_index,
                              "ChunkedSegmentSealedImpl::LoadColumnGroup()");
            LoadColumnGroup(column_groups,
                            properties,
                            cg_index,
                            field_ids,
                            segment_load_info,
                            schema_snapshot,
                            eager_load,
                            op_ctx,
                            /*is_replace=*/false,
                            committer);
        });
        load_group_futures.emplace_back(std::move(future));
    }

    storage::WaitAllFutures(load_group_futures);
    if (schema_snapshot->is_external_collection()) {
        committer.Commit(
            [&](RuntimeResourceState& runtime, PublishedSegmentState&) {
                SynthesizeExternalSystemFields(
                    segment_load_info, schema_snapshot, &runtime);
            });
    }
}

void
ChunkedSegmentSealedImpl::SynthesizeExternalSystemFields(
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    RuntimeResourceState* runtime) {
    AssertInfo(runtime != nullptr,
               "runtime must not be null when synthesizing external system "
               "fields for segment {}",
               id_);

    auto get_runtime_column =
        [&](FieldId field_id) -> std::shared_ptr<ChunkedColumnInterface> {
        auto it = runtime->fields.find(field_id);
        if (it != runtime->fields.end()) {
            return it->second;
        }
        return nullptr;
    };
    auto pk_field_id = schema_snapshot->get_primary_field_id().value();
    int64_t num_rows = segment_load_info.GetNumOfRows();
    if (num_rows == 0) {
        runtime->fields.erase(pk_field_id);
        runtime->pk_index_slot.reset();
        runtime->virtual_pk2offset.reset();
        if (!schema_snapshot->IsExternalDataField(pk_field_id)) {
            runtime->virtual_pk2offset =
                std::make_shared<const VirtualPKOffsetMap>(id_, 0);
        }
        auto timestamps = std::make_shared<TimestampData>();
        timestamps->InitFromOwnedData({});
        runtime->timestamps = std::move(timestamps);
        runtime->timestamp_index = std::make_shared<const TimestampIndex>();
        runtime->timestamp_index_slot.reset();
        {
            std::unique_lock lck(mutex_);
            update_row_count(*runtime, 0);
        }
        return;
    }

    if (!schema_snapshot->IsExternalDataField(pk_field_id)) {
        // 1. VirtualPKChunkedColumn for the synthetic primary key.
        //    This is lazy; data is only materialized if DataOfChunk/Span is called.
        auto virtual_pk =
            std::make_shared<VirtualPKChunkedColumn>(id_, num_rows);
        runtime->fields.insert_or_assign(pk_field_id, virtual_pk);

        // 2. PK to offset index using VirtualPKOffsetMap (zero storage).
        //    Virtual PK = (seg_id << 32) | offset, so pk to offset is a simple
        //    bit-extract. This replaces the OffsetOrderedArray that would
        //    otherwise store num_rows (pk, offset) pairs (~17 GB for 1B rows).
        runtime->virtual_pk2offset =
            std::make_shared<const VirtualPKOffsetMap>(id_, num_rows);
        runtime->pk_index_slot.reset();
    } else {
        AssertInfo(
            get_runtime_column(pk_field_id) != nullptr,
            "external primary key column {} is not loaded for segment {}",
            pk_field_id.get(),
            id_);
    }

    if (schema_snapshot->RequiresSourceInsertTimestamps()) {
        // Real-PK milvus-table segments load source deltalogs. Those deltas use
        // source delete timestamps, so the insert side must keep source row
        // timestamps too; otherwise a delete-before-reinsert sequence would
        // incorrectly hide the reinserted row.
        AssertInfo(
            get_runtime_column(TimestampFieldID) != nullptr,
            "source timestamp column is not loaded for milvus-table segment {}",
            id_);
    } else {
        // Synthetic timestamps: constant mode (all 0; rows always visible).
        // No data is materialized, saving ~8 GB for 1B-row external tables.
        auto timestamps = std::make_shared<TimestampData>();
        timestamps->InitConstant(num_rows, 0);
        runtime->timestamps = timestamps;
        runtime->timestamp_index = std::make_shared<const TimestampIndex>();
        runtime->timestamp_index_slot.reset();
    }

    // Row count
    {
        std::unique_lock lck(mutex_);
        update_row_count(*runtime, num_rows);
    }
}

void
ChunkedSegmentSealedImpl::SynthesizeExternalSystemFields(
    RuntimeResourceState* runtime) {
    AssertInfo(runtime != nullptr,
               "runtime must not be null when synthesizing external system "
               "fields for segment {}",
               id_);
    auto snapshot = CapturePublishedState();
    SynthesizeExternalSystemFields(
        *snapshot->load_info, snapshot->schema, runtime);
}

namespace {

// A column group or field-binlog group may contain multiple fields but has only
// one translator warmup policy. Accumulate per-field policies into the most
// aggressive group policy: sync > async > disable. Empty means no field has
// provided an explicit warmup setting yet, so downstream logic may still fall
// back to global config.
void
AccumulateWarmupPolicyForGroup(const std::string& policy,
                               std::string& aggregated_warmup_policy) {
    if (policy.empty()) {
        return;
    }

    if (policy == "sync") {
        aggregated_warmup_policy = "sync";
    } else if (policy == "async" && aggregated_warmup_policy != "sync") {
        aggregated_warmup_policy = "async";
    } else if (policy == "disable" && aggregated_warmup_policy.empty()) {
        aggregated_warmup_policy = "disable";
    }
}

struct FileMetadataLoadResult {
    milvus_storage::RowGroupMetadataVector row_group_meta;
    // per field_id → per-row-group statistics; nullptr entry means the row
    // group had no statistics set for this field.
    std::map<int64_t, std::vector<std::shared_ptr<parquet::Statistics>>>
        per_field_row_group_stats;
};

}  // namespace

LoadedGroupChunkMetadata
LoadGroupChunkMetadata(const std::vector<std::string>& insert_files,
                       const std::vector<FieldId>& field_ids_for_stats,
                       const std::string& debug_key) {
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::HIGH);

    std::vector<std::future<FileMetadataLoadResult>> futures;
    futures.reserve(insert_files.size());
    for (const auto& file : insert_files) {
        // Futures are always joined below before this function returns, so
        // capturing loader inputs by reference is safe here.
        futures.push_back(pool.Submit([&fs,
                                       file,
                                       &field_ids_for_stats,
                                       &debug_key]() {
            auto result = milvus_storage::FileRowGroupReader::Make(
                fs,
                file,
                milvus_storage::DEFAULT_READ_BUFFER_SIZE,
                storage::GetReaderProperties(),
                storage::GetArrowReaderProperties());
            AssertInfo(result.ok(),
                       "[StorageV2] Failed to create file row group reader: " +
                           result.status().ToString());

            auto reader = result.ValueOrDie();
            FileMetadataLoadResult load_result;
            auto file_metadata = reader->file_metadata();
            load_result.row_group_meta =
                file_metadata->GetRowGroupMetadataVector();

            if (!field_ids_for_stats.empty()) {
                auto field_id_mapping = file_metadata->GetFieldIDMapping();
                auto parquet_metadata = file_metadata->GetParquetMetadata();
                auto num_row_groups = parquet_metadata->num_row_groups();
                for (const auto& field_id : field_ids_for_stats) {
                    auto it = field_id_mapping.find(field_id.get());
                    AssertInfo(it != field_id_mapping.end(),
                               "field id {} not found in field id mapping",
                               field_id.get());
                    auto& per_rg =
                        load_result.per_field_row_group_stats[field_id.get()];
                    per_rg.reserve(num_row_groups);
                    for (int i = 0; i < num_row_groups; ++i) {
                        auto column_chunk =
                            parquet_metadata->RowGroup(i)->ColumnChunk(
                                it->second.col_index);
                        per_rg.push_back(column_chunk->is_stats_set()
                                             ? column_chunk->statistics()
                                             : nullptr);
                    }
                }
            }

            auto status = reader->Close();
            AssertInfo(status.ok(),
                       "[StorageV2] metadata loader {} failed to close "
                       "file reader for {} with error {}",
                       debug_key,
                       file,
                       status.ToString());
            return load_result;
        }));
    }

    auto futures_guard = folly::makeGuard([&futures]() {
        for (auto& future : futures) {
            if (future.valid()) {
                try {
                    future.get();
                } catch (...) {
                }
            }
        }
    });

    LoadedGroupChunkMetadata metadata;
    metadata.row_group_meta_list.reserve(insert_files.size());

    for (auto& future : futures) {
        auto load_result = future.get();
        metadata.row_group_meta_list.push_back(
            std::move(load_result.row_group_meta));
        // Walk files in order and replicate the original single-threaded
        // semantics: once any row group has reported stats for a field, every
        // subsequent row group (in this file or any later file) must also
        // report stats; otherwise fail.
        for (const auto& field_id : field_ids_for_stats) {
            auto& stats_vec = metadata.parquet_stats_by_field[field_id.get()];
            auto it =
                load_result.per_field_row_group_stats.find(field_id.get());
            if (it == load_result.per_field_row_group_stats.end()) {
                continue;
            }
            for (auto& stat : it->second) {
                if (stat == nullptr) {
                    AssertInfo(stats_vec.empty(),
                               "Statistics is not set for some column chunks "
                               "for field {}",
                               field_id.get());
                    continue;
                }
                stats_vec.push_back(std::move(stat));
            }
        }
    }

    return metadata;
}

void
ChunkedSegmentSealedImpl::load_column_group_data_internal(
    const LoadFieldDataInfo& load_info,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    bool is_replace,
    RuntimeResourceState* runtime) {
    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    ArrowSchemaPtr arrow_schema = schema_snapshot->ConvertToArrowSchema();
    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();

    for (auto& [id, info] : load_info.field_infos) {
        AssertInfo(info.row_count > 0,
                   "[StorageV2] The row count of field data is 0");

        auto column_group_id = FieldId(id);
        auto insert_files = info.insert_files;
        storage::SortByPath(insert_files);
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();

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
            auto fid = FieldId(field_id_list.Get(i));
            // Defense-in-depth for legacy binlog meta (child_field_ids empty):
            // dropped fields are normally filtered at ComputeDiffBinlogs level,
            // but legacy data reads field IDs from parquet which may still
            // contain dropped fields.
            if (!schema_snapshot->has_field(fid)) {
                continue;
            }
            milvus_field_ids.emplace_back(fid);
            merged_in_load_list =
                merged_in_load_list || schema_snapshot->ShouldLoadField(fid);
        }
        if (milvus_field_ids.empty()) {
            continue;
        }

        auto mmap_dir_path =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager()
                ->GetRootPath();
        auto column_group_info = FieldDataInfo(column_group_id.get(),
                                               num_rows,
                                               mmap_dir_path,
                                               merged_in_load_list,
                                               load_info.shard);
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

        auto field_metas = schema_snapshot->get_field_metas(milvus_field_ids);
        auto warmup_policy =
            resolve_field_data_group_warmup_policy(field_metas,
                                                   segment_load_info,
                                                   schema_snapshot,
                                                   info.warmup_policy);

        std::vector<FieldId> fields_for_stats;
        if (ENABLE_PARQUET_STATS_SKIP_INDEX) {
            fields_for_stats = milvus_field_ids;
        } else {
            for (auto field_id : milvus_field_ids) {
                const auto& fm = field_metas.at(field_id);
                if (fm.is_nullable() && IsVectorDataType(fm.get_data_type())) {
                    fields_for_stats.push_back(field_id);
                }
            }
        }
        auto metadata = LoadGroupChunkMetadata(
            insert_files,
            fields_for_stats,
            fmt::format(
                "seg_{}_cg_{}", get_segment_id(), column_group_id.get()));
        auto parquet_stats_by_field =
            std::move(metadata.parquet_stats_by_field);

        auto translator =
            std::make_unique<storagev2translator::GroupChunkTranslator>(
                get_segment_id(),
                GroupChunkType::DEFAULT,
                field_metas,
                column_group_info,
                std::move(insert_files),
                std::move(metadata.row_group_meta_list),
                info.enable_mmap,
                mmap_config.GetMmapPopulate(),
                milvus_field_ids.size(),
                load_info.load_priority,
                warmup_policy);
        auto chunked_column_group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));

        // Create ProxyChunkColumn for each field in this column group
        for (const auto& field_id : milvus_field_ids) {
            const auto& field_meta = field_metas.at(field_id);
            auto column = std::make_shared<ProxyChunkColumn>(
                chunked_column_group, field_id, field_meta);
            auto data_type = field_meta.get_data_type();
            std::optional<ParquetStatistics> statistics_opt;
            auto it = parquet_stats_by_field.find(field_id.get());
            if (it != parquet_stats_by_field.end()) {
                statistics_opt = std::move(it->second);
            }

            load_field_data_common(field_id,
                                   column,
                                   num_rows,
                                   data_type,
                                   info.enable_mmap,
                                   true,
                                   segment_load_info,
                                   schema_snapshot,
                                   runtime,
                                   statistics_opt,
                                   op_ctx,
                                   is_replace);
            if (field_id == TimestampFieldID) {
                if (commit_ts_ != 0) {
                    std::vector<Timestamp> ts(num_rows, commit_ts_);
                    init_storage_v1_timestamp_index(
                        std::move(ts), num_rows, runtime);
                } else {
                    init_storage_v2_timestamp_index(
                        column, num_rows, warmup_policy, runtime);
                }
                if (runtime == nullptr) {
                    PublishSystemFieldStateLocked();
                }
            }
        }

        if (column_group_id.get() == DEFAULT_SHORT_COLUMN_GROUP_ID) {
            stats_.mem_size += chunked_column_group->memory_size();
        }
    }
}

void
ChunkedSegmentSealedImpl::load_column_group_data_internal(
    const LoadFieldDataInfo& load_info,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    bool is_replace,
    StagedStateCommitter& committer) {
    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    ArrowSchemaPtr arrow_schema = schema_snapshot->ConvertToArrowSchema();
    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();

    for (auto& [id, info] : load_info.field_infos) {
        AssertInfo(info.row_count > 0,
                   "[StorageV2] The row count of field data is 0");

        auto column_group_id = FieldId(id);
        auto insert_files = info.insert_files;
        storage::SortByPath(insert_files);
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();

        milvus_storage::FieldIDList field_id_list;
        if (info.child_field_ids.size() == 0) {
            field_id_list = storage::GetFieldIDList(
                column_group_id, insert_files[0], arrow_schema, fs);
        } else {
            field_id_list = milvus_storage::FieldIDList(info.child_field_ids);
        }

        bool merged_in_load_list = false;
        std::vector<FieldId> milvus_field_ids;
        milvus_field_ids.reserve(field_id_list.size());
        for (int i = 0; i < field_id_list.size(); ++i) {
            auto fid = FieldId(field_id_list.Get(i));
            if (!schema_snapshot->has_field(fid)) {
                continue;
            }
            milvus_field_ids.emplace_back(fid);
            merged_in_load_list =
                merged_in_load_list || schema_snapshot->ShouldLoadField(fid);
        }
        if (milvus_field_ids.empty()) {
            continue;
        }

        auto mmap_dir_path =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager()
                ->GetRootPath();
        auto column_group_info = FieldDataInfo(column_group_id.get(),
                                               num_rows,
                                               mmap_dir_path,
                                               merged_in_load_list,
                                               load_info.shard);
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

        auto field_metas = schema_snapshot->get_field_metas(milvus_field_ids);
        auto warmup_policy =
            resolve_field_data_group_warmup_policy(field_metas,
                                                   segment_load_info,
                                                   schema_snapshot,
                                                   info.warmup_policy);

        std::vector<FieldId> fields_for_stats;
        if (ENABLE_PARQUET_STATS_SKIP_INDEX) {
            fields_for_stats = milvus_field_ids;
        } else {
            for (auto field_id : milvus_field_ids) {
                const auto& fm = field_metas.at(field_id);
                if (fm.is_nullable() && IsVectorDataType(fm.get_data_type())) {
                    fields_for_stats.push_back(field_id);
                }
            }
        }
        auto metadata = LoadGroupChunkMetadata(
            insert_files,
            fields_for_stats,
            fmt::format(
                "seg_{}_cg_{}", get_segment_id(), column_group_id.get()));
        auto parquet_stats_by_field =
            std::move(metadata.parquet_stats_by_field);

        auto translator =
            std::make_unique<storagev2translator::GroupChunkTranslator>(
                get_segment_id(),
                GroupChunkType::DEFAULT,
                field_metas,
                column_group_info,
                std::move(insert_files),
                std::move(metadata.row_group_meta_list),
                info.enable_mmap,
                mmap_config.GetMmapPopulate(),
                milvus_field_ids.size(),
                load_info.load_priority,
                warmup_policy);
        auto chunked_column_group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));

        for (const auto& field_id : milvus_field_ids) {
            const auto& field_meta = field_metas.at(field_id);
            auto column = std::make_shared<ProxyChunkColumn>(
                chunked_column_group, field_id, field_meta);
            auto data_type = field_meta.get_data_type();
            std::optional<ParquetStatistics> statistics_opt;
            auto it = parquet_stats_by_field.find(field_id.get());
            if (it != parquet_stats_by_field.end()) {
                statistics_opt = std::move(it->second);
            }

            load_field_data_common(field_id,
                                   column,
                                   num_rows,
                                   data_type,
                                   info.enable_mmap,
                                   true,
                                   segment_load_info,
                                   schema_snapshot,
                                   nullptr,
                                   statistics_opt,
                                   op_ctx,
                                   is_replace,
                                   &committer);
            if (field_id == TimestampFieldID) {
                if (commit_ts_ != 0) {
                    std::vector<Timestamp> ts(num_rows, commit_ts_);
                    auto timestamp_index =
                        std::make_shared<const TimestampIndex>(
                            build_timestamp_index(ts.data(), num_rows));
                    auto timestamp_data = std::make_shared<TimestampData>();
                    timestamp_data->InitFromOwnedData(std::move(ts));
                    committer.Commit(
                        [this,
                         timestamp_data = std::move(timestamp_data),
                         timestamp_index = std::move(timestamp_index),
                         num_rows](RuntimeResourceState& runtime,
                                   PublishedSegmentState&) mutable {
                            runtime.timestamps = std::move(timestamp_data);
                            runtime.timestamp_index =
                                std::move(timestamp_index);
                            runtime.timestamp_index_slot.reset();
                            stats_.mem_size += sizeof(Timestamp) * num_rows;
                        });
                } else {
                    std::unique_ptr<
                        Translator<storagev2translator::TimestampIndexCell>>
                        translator = std::make_unique<
                            storagev2translator::TimestampIndexTranslator>(
                            id_, column, num_rows, info.warmup_policy);
                    auto slot = Manager::GetInstance().CreateCacheSlot(
                        std::move(translator));
                    auto cell_holder =
                        SemiInlineGet(slot->PinCells(nullptr, {0}));
                    auto* cell = cell_holder->get_cell_of(0);
                    AssertInfo(cell != nullptr,
                               "timestamp index cache is corrupted, segment {}",
                               id_);

                    auto timestamps = std::make_shared<TimestampData>();
                    auto pins = column->GetAllChunks(nullptr);
                    timestamps->InitFromPinnedChunks(column, std::move(pins));
                    auto timestamp_index =
                        std::make_shared<const TimestampIndex>(
                            cell->timestamp_index());
                    committer.Commit([timestamps = std::move(timestamps),
                                      timestamp_index =
                                          std::move(timestamp_index),
                                      slot = std::move(slot)](
                                         RuntimeResourceState& runtime,
                                         PublishedSegmentState&) mutable {
                        runtime.timestamps = std::move(timestamps);
                        runtime.timestamp_index = std::move(timestamp_index);
                        runtime.timestamp_index_slot = std::move(slot);
                    });
                }
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
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    bool is_replace,
    RuntimeResourceState* runtime) {
    SCOPE_CGO_CALL_METRIC();

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();

    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    auto visible_runtime_owner =
        runtime == nullptr ? CaptureRuntimeResourceState() : nullptr;
    auto visible_runtime =
        runtime != nullptr ? runtime : visible_runtime_owner.get();
    AssertInfo(
        visible_runtime == nullptr || visible_runtime->row_count == 0 ||
            visible_runtime->row_count == num_rows,
        "published row count is not equal to num_rows of LoadFieldDataInfo");

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
                          schema_snapshot->ShouldLoadField(field_id),
                          load_info.shard);
        LOG_INFO("segment {} loads field {} with num_rows {}, sorted by pk {}",
                 this->get_segment_id(),
                 field_id.get(),
                 num_rows,
                 is_sorted_by_pk_);

        if (SystemProperty::Instance().IsSystem(field_id)) {
            auto insert_files = info.insert_files;
            storage::SortByPath(insert_files);
            // field_data_info.arrow_reader_channel cannot have capacity
            // othersize deadlock could happen if result count is greater than cap
            // since this branch handles system only, we shall leave channel without cap for quick fix
            LoadArrowReaderFromRemote(insert_files,
                                      field_data_info.arrow_reader_channel,
                                      load_info.load_priority);

            LOG_INFO("segment {} submits load field {} task to thread pool",
                     this->get_segment_id(),
                     field_id.get());
            load_system_field_internal(field_id,
                                       field_data_info,
                                       load_info.load_priority,
                                       runtime,
                                       runtime == nullptr);
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

            auto field_meta = schema_snapshot->operator[](field_id);
            auto warmup_policy =
                resolve_field_data_warmup_policy(field_id,
                                                 segment_load_info,
                                                 schema_snapshot,
                                                 info.warmup_policy);
            std::unique_ptr<Translator<milvus::Chunk>> translator =
                std::make_unique<storagev1translator::ChunkTranslator>(
                    this->get_segment_id(),
                    field_meta,
                    field_data_info,
                    std::move(file_infos),
                    info.enable_mmap,
                    mmap_config.GetMmapPopulate(),
                    load_info.load_priority,
                    warmup_policy);

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
                                   segment_load_info,
                                   schema_snapshot,
                                   runtime,
                                   std::nullopt,
                                   op_ctx,
                                   is_replace);
        }
    }
}

void
ChunkedSegmentSealedImpl::load_field_data_internal(
    const LoadFieldDataInfo& load_info,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    bool is_replace,
    StagedStateCommitter& committer) {
    SCOPE_CGO_CALL_METRIC();

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();

    size_t num_rows = storage::GetNumRowsForLoadInfo(load_info);
    auto* staged_runtime = committer.runtime();
    AssertInfo(
        staged_runtime->row_count == 0 || staged_runtime->row_count == num_rows,
        "staged row count is not equal to num_rows of LoadFieldDataInfo");

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
                          schema_snapshot->ShouldLoadField(field_id),
                          load_info.shard);
        LOG_INFO("segment {} loads field {} with num_rows {}, sorted by pk {}",
                 this->get_segment_id(),
                 field_id.get(),
                 num_rows,
                 is_sorted_by_pk_);

        if (SystemProperty::Instance().IsSystem(field_id)) {
            auto insert_files = info.insert_files;
            storage::SortByPath(insert_files);
            LoadArrowReaderFromRemote(insert_files,
                                      field_data_info.arrow_reader_channel,
                                      load_info.load_priority);

            LOG_INFO("segment {} submits load field {} task to thread pool",
                     this->get_segment_id(),
                     field_id.get());
            auto system_field_type =
                SystemProperty::Instance().GetSystemFieldType(field_id);
            if (system_field_type == SystemFieldType::Timestamp) {
                std::vector<Timestamp> timestamps(num_rows);
                int64_t offset = 0;
                FieldMeta field_meta(FieldName(""),
                                     FieldId(0),
                                     DataType::INT64,
                                     false,
                                     std::nullopt);
                std::shared_ptr<milvus::ArrowDataWrapper> r;
                while (field_data_info.arrow_reader_channel->pop(r)) {
                    auto array_vec = read_single_column_batches(r->reader);
                    auto chunk = create_chunk(field_meta, array_vec);
                    auto chunk_ptr = static_cast<FixedWidthChunk*>(chunk.get());
                    milvus::fastmem::FastMemcpy(
                        timestamps.data() + offset,
                        static_cast<const Timestamp*>(chunk_ptr->Span().data()),
                        chunk_ptr->Span().row_count() *
                            sizeof(*timestamps.data()));
                    offset += chunk_ptr->Span().row_count();
                }

                if (commit_ts_ != 0) {
                    std::fill(timestamps.begin(), timestamps.end(), commit_ts_);
                }
                auto timestamp_index = std::make_shared<const TimestampIndex>(
                    build_timestamp_index(timestamps.data(), num_rows));
                auto timestamp_data = std::make_shared<TimestampData>();
                timestamp_data->InitFromOwnedData(std::move(timestamps));
                committer.Commit([this,
                                  timestamp_data = std::move(timestamp_data),
                                  timestamp_index = std::move(timestamp_index),
                                  num_rows](RuntimeResourceState& runtime,
                                            PublishedSegmentState&) mutable {
                    runtime.timestamps = std::move(timestamp_data);
                    runtime.timestamp_index = std::move(timestamp_index);
                    runtime.timestamp_index_slot.reset();
                    std::unique_lock lck(mutex_);
                    update_row_count(runtime, num_rows);
                    stats_.mem_size += sizeof(Timestamp) * num_rows;
                });
            } else {
                AssertInfo(system_field_type == SystemFieldType::RowId,
                           "System field type of id column is not RowId");
                std::shared_ptr<milvus::ArrowDataWrapper> r;
                while (field_data_info.arrow_reader_channel->pop(r)) {
                }
                committer.Commit([this, num_rows](RuntimeResourceState& runtime,
                                                  PublishedSegmentState&) {
                    std::unique_lock lck(mutex_);
                    update_row_count(runtime, num_rows);
                });
            }
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

            auto field_meta = schema_snapshot->operator[](field_id);
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
                                   segment_load_info,
                                   schema_snapshot,
                                   nullptr,
                                   std::nullopt,
                                   op_ctx,
                                   is_replace,
                                   &committer);
        }
    }
}

void
ChunkedSegmentSealedImpl::load_system_field_internal(
    FieldId field_id,
    FieldDataInfo& data,
    proto::common::LoadPriority load_priority,
    RuntimeResourceState* runtime,
    bool publish_ready) {
    SCOPE_CGO_CALL_METRIC();

    auto num_rows = data.row_count;
    std::shared_ptr<RuntimeResourceState> owned_runtime;
    auto* target_runtime = runtime;
    if (target_runtime == nullptr) {
        owned_runtime = CloneMutableRuntimeResourceState();
        target_runtime = owned_runtime.get();
    }
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
            milvus::fastmem::FastMemcpy(
                timestamps.data() + offset,
                static_cast<const Timestamp*>(chunk_ptr->Span().data()),
                chunk_ptr->Span().row_count() * sizeof(*timestamps.data()));
            offset += chunk_ptr->Span().row_count();
        }

        if (commit_ts_ != 0) {
            std::fill(timestamps.begin(), timestamps.end(), commit_ts_);
        }
        init_storage_v1_timestamp_index(
            std::move(timestamps), num_rows, target_runtime);
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
        update_row_count(*target_runtime, num_rows);
    }
    if (owned_runtime != nullptr && publish_ready) {
        PublishRuntimeStateLocked(
            ToConstRuntimeState(std::move(owned_runtime)));
    }
}

void
ChunkedSegmentSealedImpl::LoadDeletedRecord(const LoadDeletedRecordInfo& info) {
    SCOPE_CGO_CALL_METRIC();

    AssertInfo(info.row_count > 0, "The row count of deleted record is 0");
    AssertInfo(info.primary_keys, "Deleted primary keys is null");
    AssertInfo(info.timestamps, "Deleted timestamps is null");
    // step 1: get pks and timestamps
    auto schema_snapshot = CaptureSchemaSnapshot();
    auto field_id =
        schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_snapshot->operator[](field_id);
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
    auto snapshot = CapturePublishedState();
    if (!get_bit(snapshot->field_data_ready_bitset, field_id)) {
        return 0;
    }
    auto column = get_column(snapshot->runtime, field_id);
    return column ? column->num_chunks() : 1;
}

int64_t
ChunkedSegmentSealedImpl::num_chunk(FieldId field_id) const {
    auto snapshot = CapturePublishedState();
    if (!get_bit(snapshot->field_data_ready_bitset, field_id)) {
        return 1;
    }
    auto column = get_column(snapshot->runtime, field_id);
    return column ? column->num_chunks() : 1;
}

int64_t
ChunkedSegmentSealedImpl::size_per_chunk() const {
    return get_row_count();
}

int64_t
ChunkedSegmentSealedImpl::chunk_size(FieldId field_id, int64_t chunk_id) const {
    auto snapshot = CapturePublishedState();
    if (!get_bit(snapshot->field_data_ready_bitset, field_id)) {
        return 0;
    }
    auto column = get_column(snapshot->runtime, field_id);
    return column ? column->chunk_row_nums(chunk_id)
                  : snapshot->runtime->row_count;
}

std::pair<int64_t, int64_t>
ChunkedSegmentSealedImpl::get_chunk_by_offset(FieldId field_id,
                                              int64_t offset) const {
    auto snapshot = CapturePublishedState();
    auto column = get_column(snapshot->runtime, field_id);
    AssertInfo(column != nullptr,
               "field {} must exist when getting chunk by offset",
               field_id.get());
    return column->GetChunkIDByOffset(offset);
}

int64_t
ChunkedSegmentSealedImpl::num_rows_until_chunk(FieldId field_id,
                                               int64_t chunk_id) const {
    auto snapshot = CapturePublishedState();
    auto column = get_column(snapshot->runtime, field_id);
    AssertInfo(column != nullptr,
               "field {} must exist when getting rows until chunk",
               field_id.get());
    return column->GetNumRowsUntilChunk(chunk_id);
}

bool
ChunkedSegmentSealedImpl::is_mmap_field(FieldId field_id) const {
    auto runtime = CaptureRuntimeResourceState();
    return runtime != nullptr && runtime->mmap_field_ids.find(field_id) !=
                                     runtime->mmap_field_ids.end();
}

void
ChunkedSegmentSealedImpl::prefetch_chunks(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const std::vector<int64_t>& chunk_ids) const {
    auto snapshot = CapturePublishedState();
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(snapshot->runtime, field_id)) {
        column->PrefetchChunks(op_ctx, chunk_ids);
    }
}

void
ChunkedSegmentSealedImpl::prefetch_chunks_locked(milvus::OpContext* op_ctx,
                                                 FieldId field_id) const {
    auto snapshot = CapturePublishedState();
    if (auto column = get_column(snapshot->runtime, field_id)) {
        auto num_chunks = column->num_chunks();
        std::vector<int64_t> ids(num_chunks);
        std::iota(ids.begin(), ids.end(), 0);
        column->PrefetchChunks(op_ctx, ids);
    }
}

void
ChunkedSegmentSealedImpl::prefetch_chunks(milvus::OpContext* op_ctx,
                                          FieldId field_id) const {
    std::shared_lock lck(mutex_);
    prefetch_chunks_locked(op_ctx, field_id);
}

void
ChunkedSegmentSealedImpl::ApplyFieldValidData(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    int64_t chunk_id,
    int64_t offset,
    int64_t size,
    TargetBitmapView valid_result) const {
    if (size == 0) {
        return;
    }

    auto snapshot = CapturePublishedState();
    std::shared_ptr<ChunkedColumnInterface> column;
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    column = get_column(snapshot->runtime, field_id);
    AssertInfo(column != nullptr,
               "field {} column must exist when validity is requested",
               field_id.get());
    if (!column->IsNullable()) {
        return;
    }

    column->ApplyValidDataInChunk(op_ctx, chunk_id, offset, size, valid_result);
}

void
ChunkedSegmentSealedImpl::ApplyFieldValidDataByOffsets(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const int64_t* offsets,
    int64_t count,
    TargetBitmapView valid_result) const {
    if (count == 0) {
        return;
    }

    auto snapshot = CapturePublishedState();
    std::shared_ptr<ChunkedColumnInterface> column;
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    column = get_column(snapshot->runtime, field_id);
    AssertInfo(column != nullptr,
               "field {} column must exist when validity is requested",
               field_id.get());
    if (!column->IsNullable()) {
        return;
    }

    column->BulkIsValid(
        op_ctx,
        [&valid_result](bool is_valid, size_t i) {
            if (!is_valid) {
                valid_result[i] = false;
            }
        },
        offsets,
        count);
}

PinWrapper<SpanBase>
ChunkedSegmentSealedImpl::chunk_data_impl(milvus::OpContext* op_ctx,
                                          FieldId field_id,
                                          int64_t chunk_id) const {
    auto snapshot = CapturePublishedState();
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(snapshot->runtime, field_id)) {
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
    auto snapshot = CapturePublishedState();
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(snapshot->runtime, field_id)) {
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
    auto snapshot = CapturePublishedState();
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(snapshot->runtime, field_id)) {
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
    auto snapshot = CapturePublishedState();
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(snapshot->runtime, field_id)) {
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
    auto snapshot = CapturePublishedState();
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(snapshot->runtime, field_id)) {
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
    auto snapshot = CapturePublishedState();
    AssertInfo(get_bit(snapshot->field_data_ready_bitset, field_id),
               "Can't get bitset element at " + std::to_string(field_id.get()));
    if (auto column = get_column(snapshot->runtime, field_id)) {
        return column->ArrayViewsByOffsets(op_ctx, chunk_id, offsets);
    }
    ThrowInfo(ErrorCode::UnexpectedError,
              "chunk_array_views_by_offsets only used for variable column "
              "field ");
}

PinWrapper<index::NgramInvertedIndex*>
ChunkedSegmentSealedImpl::GetNgramIndex(milvus::OpContext* op_ctx,
                                        FieldId field_id) const {
    auto runtime = CaptureRuntimeResourceState();
    if (runtime->ngram_fields.find(field_id) == runtime->ngram_fields.end()) {
        return PinWrapper<index::NgramInvertedIndex*>(nullptr);
    }

    auto iter = runtime->scalar_indexings.find(field_id);
    if (iter == runtime->scalar_indexings.end()) {
        return PinWrapper<index::NgramInvertedIndex*>(nullptr);
    }

    auto ca = SemiInlineGet(iter->second->PinCells(op_ctx, {0}));
    auto index = dynamic_cast<index::NgramInvertedIndex*>(ca->get_cell_of(0));
    AssertInfo(index != nullptr,
               "ngram index cache is corrupted, field_id: {}",
               field_id.get());
    return PinWrapper<index::NgramInvertedIndex*>(std::move(ca), index);
}

PinWrapper<index::NgramInvertedIndex*>
ChunkedSegmentSealedImpl::GetNgramIndexForJson(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const std::string& nested_path) const {
    auto runtime = CaptureRuntimeResourceState();
    auto iter = runtime->ngram_indexings.find(field_id);
    if (iter == runtime->ngram_indexings.end()) {
        return PinWrapper<index::NgramInvertedIndex*>(nullptr);
    }
    auto nested_iter = iter->second.find(nested_path);
    if (nested_iter == iter->second.end()) {
        return PinWrapper<index::NgramInvertedIndex*>(nullptr);
    }

    auto ca = SemiInlineGet(nested_iter->second->PinCells(op_ctx, {0}));
    auto index = dynamic_cast<index::NgramInvertedIndex*>(ca->get_cell_of(0));
    AssertInfo(index != nullptr,
               "ngram index cache for json is corrupted, field_id: {}, "
               "nested_path: {}",
               field_id.get(),
               nested_path);
    return PinWrapper<index::NgramInvertedIndex*>(std::move(ca), index);
}

int64_t
ChunkedSegmentSealedImpl::get_row_count() const {
    auto runtime = CaptureRuntimeResourceState();
    return runtime != nullptr ? runtime->row_count : 0;
}

int64_t
ChunkedSegmentSealedImpl::get_field_avg_size(FieldId field_id) const {
    AssertInfo(field_id.get() >= 0,
               "invalid field id, should be greater than or equal to 0");
    if (SystemProperty::Instance().IsSystem(field_id)) {
        if (field_id == TimestampFieldID || field_id == RowFieldID) {
            return sizeof(int64_t);
        }
        ThrowInfo(FieldIDInvalid, "unsupported system field id");
    }
    auto snapshot = CapturePublishedState();
    auto& field_meta = snapshot->schema->operator[](field_id);
    if (!IsVariableDataType(field_meta.get_data_type())) {
        return field_meta.get_sizeof();
    }
    auto runtime = snapshot->runtime;
    if (runtime == nullptr) {
        return 0;
    }
    auto it = runtime->variable_fields_avg_size.find(field_id);
    return it != runtime->variable_fields_avg_size.end() ? it->second.second
                                                         : 0;
}

void
ChunkedSegmentSealedImpl::set_field_avg_size(FieldId field_id,
                                             int64_t num_rows,
                                             int64_t field_size) {
    AssertInfo(field_id.get() >= 0,
               "invalid field id, should be greater than or equal to 0");
    AssertInfo(num_rows > 0,
               "The num rows of field data should be greater than 0");
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    auto current = CapturePublishedState();
    auto& field_meta = current->schema->operator[](field_id);
    if (!IsVariableDataType(field_meta.get_data_type())) {
        return;
    }
    auto runtime = CloneRuntimeResourceState(current->runtime);
    auto& field_info = runtime->variable_fields_avg_size[field_id];
    auto size = field_info.first * field_info.second + field_size;
    field_info.first += num_rows;
    field_info.second = size / field_info.first;
    auto next = ClonePublishedState(current);
    next->runtime = ToConstRuntimeState(std::move(runtime));
    PublishStateOnline(std::move(next));
}

std::shared_ptr<const SkipIndex>
ChunkedSegmentSealedImpl::GetSkipIndexSnapshot() const {
    auto runtime = CaptureRuntimeResourceState();
    if (runtime == nullptr || runtime->skip_index == nullptr) {
        return std::make_shared<const SkipIndex>();
    }
    return runtime->skip_index;
}

int64_t
ChunkedSegmentSealedImpl::get_deleted_count() const {
    std::shared_lock lck(mutex_);
    return deleted_record_.size();
}

const Schema&
ChunkedSegmentSealedImpl::get_schema() const {
    return *CapturePublishedState()->schema;
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
    std::shared_lock vector_state_lck(mutex_);
    auto snapshot = CapturePublishedState();
    AssertInfo(snapshot->system_field_ready, "System field is not ready");
    auto field_id = search_info.field_id_;
    auto runtime = snapshot->runtime;
    auto& field_meta = snapshot->schema->operator[](field_id);

    AssertInfo(field_meta.is_vector(),
               "The meta type of vector field is not vector type");

    if (get_bit(snapshot->binlog_index_bitset, field_id)) {
        auto config_it = runtime->vec_binlog_config.find(field_id);
        AssertInfo(config_it != runtime->vec_binlog_config.end(),
                   "The binlog params is not generate.");
        auto binlog_search_info = config_it->second->GetSearchConf(search_info);

        auto vector_entry = GetVectorIndexing(runtime, field_id);
        AssertInfo(vector_entry != nullptr,
                   "vector indexes isn't ready for field " +
                       std::to_string(field_id.get()));
        query::SearchOnSealedIndex(*snapshot->schema,
                                   *vector_entry,
                                   binlog_search_info,
                                   query_data,
                                   query_offsets,
                                   query_count,
                                   bitset,
                                   op_context,
                                   output);
        milvus::tracer::AddEvent(
            "finish_searching_vector_temperate_binlog_index");
    } else if (get_bit(snapshot->index_ready_bitset, field_id)) {
        if (search_info.global_refine_enable_ &&
            IsIndexRefineEnabledLocked(op_context, field_id, runtime)) {
            search_info.topk_ = GetEffectiveSearchTopk(search_info);
        }
        auto vector_entry = GetVectorIndexing(runtime, field_id);
        AssertInfo(vector_entry != nullptr,
                   "vector indexes isn't ready for field " +
                       std::to_string(field_id.get()));
        query::SearchOnSealedIndex(*snapshot->schema,
                                   *vector_entry,
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
            get_bit(snapshot->field_data_ready_bitset, field_id),
            "Field Data is not loaded: " + std::to_string(field_id.get()));
        auto row_count = runtime != nullptr ? runtime->row_count : 0;
        AssertInfo(row_count > 0, "Can't get row count value");
        auto vec_data = get_column(snapshot->runtime, field_id);
        AssertInfo(
            vec_data != nullptr, "vector field {} not loaded", field_id.get());

        // get index params for bm25 and minhash brute force.
        // A field added by add_function_field is absent from this segment's
        // construction-time col_index_meta_ snapshot, so guard with HasField:
        // BM25 k1/b are delivered through the plan, MinHash falls back to
        // defaults for the brief window before the segment is reloaded.
        std::map<std::string, std::string> index_info;
        if ((search_info.metric_type_ == knowhere::metric::BM25 ||
             search_info.metric_type_ == knowhere::metric::MHJACCARD) &&
            col_index_meta_ != nullptr && col_index_meta_->HasField(field_id)) {
            index_info =
                col_index_meta_->GetFieldIndexMeta(field_id).GetIndexParams();
        }

        query::SearchOnSealedColumn(*snapshot->schema,
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
ChunkedSegmentSealedImpl::FilterVectorValidOffsetsFromIndex(
    milvus::OpContext* op_ctx,
    const SealedIndexingEntry& entry,
    const int64_t* seg_offsets,
    int64_t count) const {
    ValidResult result;
    result.valid_count = count;

    auto ca = SemiInlineGet(entry.indexing_->PinCells(op_ctx, {0}));
    auto vec_index = dynamic_cast<index::VectorIndex*>(ca->get_cell_of(0));
    AssertInfo(vec_index != nullptr, "invalid vector indexing");
    AssertInfo(vec_index->HasValidData(),
               "nullable vector index does not contain valid data");

    result.valid_data = std::make_unique<bool[]>(count);
    vec_index->GetOffsetMapping().FilterValidLogicalOffsets(
        seg_offsets, count, result.valid_data.get(), result.valid_offsets);
    result.valid_count = result.valid_offsets.size();
    return result;
}

ChunkedSegmentSealedImpl::ValidResult
ChunkedSegmentSealedImpl::FilterVectorValidOffsetsFromColumn(
    milvus::OpContext* op_ctx,
    const ChunkedColumnInterface* column,
    const int64_t* seg_offsets,
    int64_t count) const {
    ValidResult result;
    result.valid_data = std::make_unique<bool[]>(count);
    result.valid_offsets.reserve(count);

    column->BulkIsValid(
        op_ctx,
        [&](bool is_valid, size_t offset) {
            result.valid_data[offset] = is_valid;
            if (is_valid) {
                result.valid_offsets.push_back(seg_offsets[offset]);
            }
        },
        seg_offsets,
        count);
    result.valid_count = result.valid_offsets.size();
    return result;
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::get_vector(milvus::OpContext* op_ctx,
                                     FieldId field_id,
                                     const int64_t* ids,
                                     int64_t count) const {
    std::shared_lock vector_state_lck(mutex_);
    auto snapshot = CapturePublishedState();
    auto& field_meta = snapshot->schema->operator[](field_id);
    AssertInfo(field_meta.is_vector(), "vector field is not vector type");

    if (!get_bit(snapshot->index_ready_bitset, field_id) &&
        !get_bit(snapshot->binlog_index_bitset, field_id)) {
        return fill_with_empty(field_id, count);
    }

    auto vector_entry = GetVectorIndexing(snapshot->runtime, field_id);
    AssertInfo(vector_entry != nullptr, "vector index is not ready");
    auto ca = SemiInlineGet(vector_entry->indexing_->PinCells(op_ctx, {0}));
    auto vec_index = dynamic_cast<index::VectorIndex*>(ca->get_cell_of(0));
    AssertInfo(vec_index, "invalid vector indexing");

    auto has_raw_data = vec_index->HasRawData();

    if (has_raw_data) {
        // If index has raw data, get vector from memory.
        ValidResult filter_result;
        knowhere::DataSetPtr ids_ds;
        int64_t valid_count = count;
        const bool* valid_data = nullptr;
        if (field_meta.is_nullable()) {
            if (!vec_index->HasValidData()) {
                auto column = get_column(snapshot->runtime, field_id);
                if (column != nullptr) {
                    CheckVectorOutputCellsLoaded(
                        id_, field_id, field_meta, column.get(), ids, count);
                    return get_raw_data(
                        op_ctx, field_id, field_meta, ids, count);
                }
                ThrowInfo(ErrorCode::UnexpectedError,
                          "nullable vector index has raw data but no valid "
                          "data, and field data is unavailable");
            }
            filter_result = FilterVectorValidOffsetsFromIndex(
                op_ctx, *vector_entry, ids, count);
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

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::get_emb_list(milvus::OpContext* op_ctx,
                                       FieldId field_id,
                                       const FieldMeta& field_meta,
                                       const int64_t* seg_offsets,
                                       int64_t count) const {
    std::shared_lock vector_state_lck(mutex_);
    auto snapshot = CapturePublishedState();
    AssertInfo(field_meta.get_data_type() == DataType::VECTOR_ARRAY,
               "get_emb_list only supports VECTOR_ARRAY");

    if (!get_bit(snapshot->index_ready_bitset, field_id) &&
        !get_bit(snapshot->binlog_index_bitset, field_id)) {
        return fill_with_empty(field_id, count);
    }

    auto vector_entry = GetVectorIndexing(snapshot->runtime, field_id);
    AssertInfo(vector_entry != nullptr, "vector index is not ready");
    auto ca = SemiInlineGet(vector_entry->indexing_->PinCells(op_ctx, {0}));
    auto vec_index = dynamic_cast<index::VectorIndex*>(ca->get_cell_of(0));
    AssertInfo(vec_index, "invalid vector indexing");
    auto has_raw_data = vec_index->HasRawData();
    AssertInfo(has_raw_data,
               "get_emb_list called on vector index without raw data");

    auto metric_type = vec_index->GetMetricType();

    ValidResult filter_result;
    int64_t valid_count = count;
    const bool* valid_data = nullptr;
    const int64_t* valid_offsets = seg_offsets;
    if (field_meta.is_nullable()) {
        if (!vec_index->HasValidData()) {
            auto column = get_column(snapshot->runtime, field_id);
            if (column != nullptr) {
                CheckVectorOutputCellsLoaded(id_,
                                             field_id,
                                             field_meta,
                                             column.get(),
                                             seg_offsets,
                                             count);
                return get_raw_data(
                    op_ctx, field_id, field_meta, seg_offsets, count);
            }
            ThrowInfo(ErrorCode::UnexpectedError,
                      "nullable vector index has raw data but no valid "
                      "data, and field data is unavailable");
        }
        filter_result = FilterVectorValidOffsetsFromIndex(
            op_ctx, *vector_entry, seg_offsets, count);
        valid_count = filter_result.valid_count;
        valid_data = filter_result.valid_data.get();
        valid_offsets = filter_result.valid_offsets.data();
    }

    auto data_array =
        CreateEmptyVectorDataArray(count, valid_count, valid_data, field_meta);
    if (valid_count == 0) {
        return data_array;
    }

    // Build el_ids dataset from valid_offsets. For nullable VECTOR_ARRAY, the
    // index offset mapping maps logical row offsets to compact physical
    // embedding-list ids.
    auto ids_ds = GenIdsDataset(valid_count, valid_offsets);

    auto [raw_data, offsets] = vec_index->GetEmbListByIds(ids_ds, metric_type);
    AssertInfo(offsets.size() == static_cast<size_t>(valid_count + 1),
               "GetEmbListByIds returned invalid offsets size {}, expected {}",
               offsets.size(),
               valid_count + 1);

    auto dim = field_meta.get_dim();
    auto element_type = field_meta.get_element_type();
    const size_t vec_size_per_element =
        milvus::vector_bytes_per_element(element_type, dim);

    auto vector_array = data_array->mutable_vectors();
    auto obj = vector_array->mutable_vector_array();

    std::vector<int64_t> valid_logical_offsets;
    if (valid_data != nullptr) {
        valid_logical_offsets.reserve(valid_count);
        for (int64_t i = 0; i < count; ++i) {
            if (valid_data[i]) {
                valid_logical_offsets.push_back(i);
            }
        }
    }

    // Build a VectorFieldProto for each embedding list
    for (int64_t i = 0; i < valid_count; i++) {
        auto dst_index = valid_data != nullptr ? valid_logical_offsets[i] : i;
        auto* entry = obj->mutable_data()->Mutable(dst_index);
        entry->set_dim(dim);
        size_t vec_start = offsets[i];
        size_t vec_count = offsets[i + 1] - offsets[i];
        if (vec_count == 0) {
            continue;
        }
        size_t byte_offset = vec_start * vec_size_per_element;
        size_t byte_len = vec_count * vec_size_per_element;

        switch (element_type) {
            case DataType::VECTOR_FLOAT: {
                auto* src = reinterpret_cast<const float*>(raw_data.data() +
                                                           byte_offset);
                entry->mutable_float_vector()->mutable_data()->Add(
                    src, src + vec_count * dim);
                break;
            }
            case DataType::VECTOR_BINARY: {
                auto* src = reinterpret_cast<const char*>(raw_data.data() +
                                                          byte_offset);
                entry->mutable_binary_vector()->assign(src, byte_len);
                break;
            }
            case DataType::VECTOR_FLOAT16: {
                auto* src = reinterpret_cast<const char*>(raw_data.data() +
                                                          byte_offset);
                entry->mutable_float16_vector()->assign(src, byte_len);
                break;
            }
            case DataType::VECTOR_BFLOAT16: {
                auto* src = reinterpret_cast<const char*>(raw_data.data() +
                                                          byte_offset);
                entry->mutable_bfloat16_vector()->assign(src, byte_len);
                break;
            }
            case DataType::VECTOR_INT8: {
                auto* src = reinterpret_cast<const char*>(raw_data.data() +
                                                          byte_offset);
                entry->mutable_int8_vector()->assign(src, byte_len);
                break;
            }
            default:
                break;
        }
    }

    return data_array;
}

void
ChunkedSegmentSealedImpl::DropFieldData(const FieldId field_id) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    auto current = CapturePublishedState();
    DropFieldData(field_id, current->schema, nullptr, current);
}

void
ChunkedSegmentSealedImpl::DropFieldData(
    const FieldId field_id,
    const SchemaPtr& schema_snapshot,
    RuntimeResourceState* runtime,
    const std::shared_ptr<const PublishedSegmentState>& current_snapshot) {
    AssertInfo(!SystemProperty::Instance().IsSystem(field_id),
               "Dropping system field is not supported, field id: {}",
               field_id.get());
    auto snapshot = current_snapshot;
    if (snapshot == nullptr && runtime == nullptr) {
        snapshot = CapturePublishedState();
    }
    bool has_binlog_index =
        snapshot != nullptr &&
        has_bit_position(snapshot->binlog_index_bitset, field_id) &&
        get_bit(snapshot->binlog_index_bitset, field_id);
    auto schema_has_field = field_exists_in_schema(schema_snapshot, field_id);
    bool is_pk_field =
        schema_has_field &&
        schema_snapshot->get_primary_field_id().has_value() &&
        schema_snapshot->get_primary_field_id().value() == field_id;

    if (is_pk_field) {
        LOG_INFO(
            "Skip dropping pk field {} in segment {}", field_id.get(), id_);
        if (runtime == nullptr && has_binlog_index) {
            auto next_runtime = CloneRuntimeResourceState(snapshot->runtime);
            DropVectorIndexing(*next_runtime, field_id);
            next_runtime->vec_binlog_config.erase(field_id);
            auto published_runtime =
                ToConstRuntimeState(std::move(next_runtime));
            MutatePublishedStateLocked([&](PublishedSegmentState& state) {
                state.runtime = published_runtime;
                clear_bit_if_present(state.published_binlog_index_ready_bitset,
                                     field_id);
                clear_bit_if_present(state.binlog_index_bitset, field_id);
                if (!get_bit_if_present(state.index_ready_bitset, field_id)) {
                    ClearPublishedIndexRawDataInState(state, field_id);
                    ClearIndexRawDataInState(state, field_id);
                }
            });
        }
        return;
    }

    std::shared_ptr<ChunkedColumnInterface> old_column;
    if (runtime != nullptr) {
        auto it = runtime->fields.find(field_id);
        if (it != runtime->fields.end()) {
            old_column = it->second;
        }
    } else {
        old_column = get_column(snapshot->runtime, field_id);
    }
    if (old_column) {
        old_column->CancelWarmup();
    }
    if (runtime != nullptr) {
        runtime->fields.erase(field_id);
        runtime->array_offsets_map.erase(field_id);
        runtime->mmap_field_ids.erase(field_id);
        // Average size describes the retrievable field value, not the
        // resident raw column. Keep it when an index with raw data remains
        // responsible for retrieval.
        if (!schema_has_field) {
            runtime->variable_fields_avg_size.erase(field_id);
        }
        if (runtime->skip_index != nullptr) {
            runtime->skip_index->Erase(field_id);
        }
    } else {
        auto next_runtime = CloneRuntimeResourceState(snapshot->runtime);
        next_runtime->fields.erase(field_id);
        next_runtime->array_offsets_map.erase(field_id);
        next_runtime->mmap_field_ids.erase(field_id);
        // See the staged-runtime branch above: only schema removal retires
        // field-level size metadata.
        if (!schema_has_field) {
            next_runtime->variable_fields_avg_size.erase(field_id);
        }
        if (next_runtime->skip_index != nullptr) {
            next_runtime->skip_index->Erase(field_id);
        }
        if (has_binlog_index) {
            DropVectorIndexing(*next_runtime, field_id);
            next_runtime->vec_binlog_config.erase(field_id);
        }
        PublishFieldDroppedLocked(field_id,
                                  ToConstRuntimeState(std::move(next_runtime)));
    }
}

void
ChunkedSegmentSealedImpl::DropIndex(const FieldId field_id,
                                    milvus::OpContext* op_ctx) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    auto snapshot = CapturePublishedState();
    DropIndex(field_id, snapshot->schema, nullptr, op_ctx);
}

void
ChunkedSegmentSealedImpl::DropIndex(const FieldId field_id,
                                    const SchemaPtr& schema_snapshot,
                                    RuntimeResourceState* runtime,
                                    milvus::OpContext* op_ctx) {
    AssertInfo(!SystemProperty::Instance().IsSystem(field_id),
               "Field id:" + std::to_string(field_id.get()) +
                   " isn't one of system type when drop index");
    if (field_exists_in_schema(schema_snapshot, field_id)) {
        auto& field_meta = schema_snapshot->operator[](field_id);
        AssertInfo(!field_meta.is_vector(), "vector field cannot drop index");
    }

    if (runtime != nullptr) {
        cancel_and_erase_scalar_index(runtime->scalar_indexings, field_id);
        runtime->ngram_fields.erase(field_id);
    } else {
        auto next_runtime = CloneMutableRuntimeResourceState();
        cancel_and_erase_scalar_index(next_runtime->scalar_indexings, field_id);
        next_runtime->ngram_fields.erase(field_id);
        DropVectorIndexing(*next_runtime, field_id);
        next_runtime->vec_binlog_config.erase(field_id);
        PublishIndexDroppedLocked(
            field_id, ToConstRuntimeState(std::move(next_runtime)), op_ctx);
    }
}

void
ChunkedSegmentSealedImpl::DropJSONIndex(const FieldId field_id,
                                        const std::string& nested_path,
                                        milvus::OpContext* op_ctx) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    auto next_runtime = CloneMutableRuntimeResourceState();
    auto retired_indexings =
        EraseJsonIndexesAtPath(*next_runtime, field_id, nested_path);
    auto published_runtime = ToConstRuntimeState(std::move(next_runtime));
    MutatePublishedStateLocked(
        [&](PublishedSegmentState& state) {
            state.runtime = published_runtime;
            SyncJsonNgramIndexState(state, *published_runtime, field_id);
        },
        op_ctx);
    for (auto& indexing : retired_indexings) {
        if (indexing != nullptr) {
            indexing->CancelWarmup();
        }
    }
}

void
ChunkedSegmentSealedImpl::check_search(const query::Plan* plan) const {
    AssertInfo(plan, "Search plan is null");
    AssertInfo(plan->extra_info_opt_.has_value(),
               "Extra info of search plan doesn't have value");

    auto snapshot = CapturePublishedState();
    if (!snapshot->system_field_ready) {
        ThrowInfo(FieldNotLoaded,
                  "failed to load row ID or timestamp, potential missing "
                  "bin logs or "
                  "empty segments. Segment ID = " +
                      std::to_string(this->id_));
    }

    auto& request_fields = plan->extra_info_opt_.value().involved_fields_;
    auto field_ready_bitset = snapshot->field_data_ready_bitset |
                              snapshot->index_ready_bitset |
                              snapshot->binlog_index_bitset;

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
    auto snapshot = CapturePublishedState();
    auto runtime = snapshot->runtime;
    BitsetTypeView bitset_view(bitset);

    // See Contain() — same zero-storage pk2offset fast path.
    if (runtime != nullptr && runtime->virtual_pk2offset != nullptr) {
        for (auto& pk : pks) {
            runtime->virtual_pk2offset->find_range(
                pk, proto::plan::OpType::Equal, bitset_view, [](int64_t) {
                    return true;
                });
        }
        return;
    }

    if (!is_sorted_by_pk_) {
        auto pk_index = PinPkIndex(runtime, nullptr);
        auto* pk_cell = pk_index.get();
        AssertInfo(pk_cell != nullptr && pk_cell->has_pk2offset(),
                   "primary key index is not ready");
        for (auto& pk : pks) {
            pk_cell->pk2offset().find_range(
                pk,
                proto::plan::OpType::Equal,
                bitset_view,
                [](int64_t offset) { return true; });
        }
        return;
    }

    auto schema_snapshot = snapshot->schema;
    auto pk_field_id =
        schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = get_column(snapshot->runtime, pk_field_id);
    AssertInfo(pk_column != nullptr, "primary key column not loaded");

    switch (schema_snapshot->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64:
            search_pks_with_two_pointers_impl<int64_t>(
                bitset_view, pks, pk_column);
            break;
        case DataType::VARCHAR:
            search_pks_with_two_pointers_impl<std::string>(
                bitset_view, pks, pk_column);
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported type {}",
                                  schema_snapshot->get_fields()
                                      .at(pk_field_id)
                                      .get_data_type()));
    }
}

void
ChunkedSegmentSealedImpl::search_batch_pks(
    const std::vector<PkType>& pks,
    const std::function<Timestamp(const size_t idx)>& get_timestamp,
    bool include_same_ts,
    const std::function<void(const SegOffset offset, const Timestamp ts)>&
        callback) const {
    // Helper to read a single timestamp by segment offset.
    // For import/CDC segments with commit_ts_ set: every row carries commit_ts_,
    // so short-circuit without touching the raw timestamp column.
    // For StorageV2: pins the timestamp column and indexes into chunks.
    // PK lookup and timestamp data come from the same published runtime.
    auto snapshot = CapturePublishedState();
    auto runtime = snapshot->runtime;
    auto effective_commit_ts =
        snapshot->commit_ts != 0 ? std::optional<Timestamp>{snapshot->commit_ts}
                                 : std::nullopt;
    auto read_ts = [&](int64_t offset) -> Timestamp {
        return ReadTimestamp(offset, runtime, effective_commit_ts);
    };

    // Virtual PK offset maps can resolve pk -> offset directly by bit-extract.
    // Avoid the sorted-PK column scan below: external segments synthesize PKs
    // with VirtualPKChunkedColumn, which intentionally does not support
    // GetAllChunks().
    if (runtime != nullptr && runtime->virtual_pk2offset != nullptr) {
        auto timestamp_hit =
            include_same_ts
                ? [](Timestamp lhs, Timestamp rhs) { return lhs <= rhs; }
                : [](Timestamp lhs, Timestamp rhs) { return lhs < rhs; };
        for (size_t i = 0; i < pks.size(); i++) {
            auto timestamp = get_timestamp(i);
            for (auto offset : runtime->virtual_pk2offset->find(pks[i])) {
                auto insert_ts = read_ts(offset);
                if (timestamp_hit(insert_ts, timestamp)) {
                    callback(SegOffset(offset), timestamp);
                }
            }
        }
        return;
    }

    // handle unsorted case
    if (!is_sorted_by_pk_) {
        auto pk_index = PinPkIndex(runtime, nullptr);
        auto* pk_cell = pk_index.get();
        if (pk_cell == nullptr || !pk_cell->has_pk2offset()) {
            return;
        }
        auto timestamp_hit =
            include_same_ts
                ? [](Timestamp lhs, Timestamp rhs) { return lhs <= rhs; }
                : [](Timestamp lhs, Timestamp rhs) { return lhs < rhs; };
        for (size_t i = 0; i < pks.size(); i++) {
            auto timestamp = get_timestamp(i);
            auto offsets = pk_cell->pk2offset().find(pks[i]);
            for (auto offset : offsets) {
                auto insert_ts = read_ts(offset);
                if (timestamp_hit(insert_ts, timestamp)) {
                    callback(SegOffset(offset), timestamp);
                }
            }
        }
        return;
    }

    auto schema_snapshot = snapshot->schema;
    auto pk_field_id =
        schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = get_column(snapshot->runtime, pk_field_id);
    AssertInfo(pk_column != nullptr, "primary key column not loaded");

    auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

    auto timestamp_hit = include_same_ts
                             ? [](const Timestamp& ts1,
                                  const Timestamp& ts2) { return ts1 <= ts2; }
                             : [](const Timestamp& ts1, const Timestamp& ts2) {
                                   return ts1 < ts2;
                               };

    switch (schema_snapshot->get_fields().at(pk_field_id).get_data_type()) {
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
                        auto insert_ts = read_ts(offset);
                        if (timestamp_hit(insert_ts, timestamp)) {
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
                        auto insert_ts = read_ts(segment_offset);
                        if (timestamp_hit(insert_ts, timestamp)) {
                            callback(SegOffset(segment_offset), timestamp);
                        }
                    }
                }
            }
            break;
        }
        default: {
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported type {}",
                                  schema_snapshot->get_fields()
                                      .at(pk_field_id)
                                      .get_data_type()));
        }
    }
}

void
ChunkedSegmentSealedImpl::pk_range(milvus::OpContext* op_ctx,
                                   proto::plan::OpType op,
                                   const PkType& pk,
                                   BitsetTypeView& bitset) const {
    auto snapshot = CapturePublishedState();
    auto runtime = snapshot->runtime;
    // See Contain() — same zero-storage pk2offset fast path.
    if (runtime != nullptr && runtime->virtual_pk2offset != nullptr) {
        runtime->virtual_pk2offset->find_range(
            pk, op, bitset, [](int64_t) { return true; });
        return;
    }
    if (!is_sorted_by_pk_) {
        auto pk_index = PinPkIndex(runtime, op_ctx);
        auto* pk_cell = pk_index.get();
        AssertInfo(pk_cell != nullptr && pk_cell->has_pk2offset(),
                   "primary key index is not ready");
        pk_cell->pk2offset().find_range(
            pk, op, bitset, [](int64_t offset) { return true; });
        return;
    }

    search_sorted_pk_range(op_ctx, op, pk, bitset, snapshot);
}

void
ChunkedSegmentSealedImpl::search_sorted_pk_range(
    milvus::OpContext* op_ctx,
    proto::plan::OpType op,
    const PkType& pk,
    BitsetTypeView& bitset,
    const std::shared_ptr<const PublishedSegmentState>& snapshot) const {
    auto schema_snapshot = snapshot->schema;
    auto pk_field_id =
        schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = get_column(snapshot->runtime, pk_field_id);
    AssertInfo(pk_column != nullptr, "primary key column not loaded");

    switch (schema_snapshot->get_fields().at(pk_field_id).get_data_type()) {
        case DataType::INT64:
            search_sorted_pk_range_impl<int64_t>(
                op, std::get<int64_t>(pk), pk_column, bitset);
            break;
        case DataType::VARCHAR:
            search_sorted_pk_range_impl<std::string>(
                op, std::get<std::string>(pk), pk_column, bitset);
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported type {}",
                                  schema_snapshot->get_fields()
                                      .at(pk_field_id)
                                      .get_data_type()));
    }
}

void
ChunkedSegmentSealedImpl::pk_binary_range(milvus::OpContext* op_ctx,
                                          const PkType& lower_pk,
                                          bool lower_inclusive,
                                          const PkType& upper_pk,
                                          bool upper_inclusive,
                                          BitsetTypeView& bitset) const {
    auto snapshot = CapturePublishedState();
    auto runtime = snapshot->runtime;
    // See Contain() — same zero-storage pk2offset fast path.
    if (runtime != nullptr && runtime->virtual_pk2offset != nullptr) {
        auto lower_op = lower_inclusive ? proto::plan::OpType::GreaterEqual
                                        : proto::plan::OpType::GreaterThan;
        auto upper_op = upper_inclusive ? proto::plan::OpType::LessEqual
                                        : proto::plan::OpType::LessThan;
        BitsetType upper_result(bitset.size());
        auto upper_view = upper_result.view();
        runtime->virtual_pk2offset->find_range(
            lower_pk, lower_op, bitset, [](int64_t) { return true; });
        runtime->virtual_pk2offset->find_range(
            upper_pk, upper_op, upper_view, [](int64_t) { return true; });
        bitset &= upper_result;
        return;
    }
    if (!is_sorted_by_pk_) {
        auto pk_index = PinPkIndex(runtime, op_ctx);
        auto* pk_cell = pk_index.get();
        AssertInfo(pk_cell != nullptr && pk_cell->has_pk2offset(),
                   "primary key index is not ready");
        auto lower_op = lower_inclusive ? proto::plan::OpType::GreaterEqual
                                        : proto::plan::OpType::GreaterThan;
        auto upper_op = upper_inclusive ? proto::plan::OpType::LessEqual
                                        : proto::plan::OpType::LessThan;
        BitsetType upper_result(bitset.size());
        auto upper_view = upper_result.view();
        pk_cell->pk2offset().find_range(
            lower_pk, lower_op, bitset, [](int64_t offset) { return true; });
        pk_cell->pk2offset().find_range(
            upper_pk, upper_op, upper_view, [](int64_t offset) {
                return true;
            });
        bitset &= upper_result;
        return;
    }

    // For sorted segments, use binary search
    auto schema_snapshot = snapshot->schema;
    auto pk_field_id =
        schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
    auto pk_column = get_column(runtime, pk_field_id);
    AssertInfo(pk_column != nullptr, "primary key column not loaded");

    switch (schema_snapshot->get_fields().at(pk_field_id).get_data_type()) {
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
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported type {}",
                                  schema_snapshot->get_fields()
                                      .at(pk_field_id)
                                      .get_data_type()));
    }
}

std::pair<std::vector<OffsetMap::OffsetType>, bool>
ChunkedSegmentSealedImpl::find_first_n(int64_t limit,
                                       const BitsetTypeView& bitset) const {
    auto runtime = CaptureRuntimeResourceState();
    if (runtime != nullptr && runtime->virtual_pk2offset != nullptr) {
        return runtime->virtual_pk2offset->find_first_n(limit, bitset);
    }
    if (!is_sorted_by_pk_) {
        auto pk_index = PinPkIndex(runtime, nullptr);
        auto* pk_cell = pk_index.get();
        AssertInfo(pk_cell != nullptr && pk_cell->has_pk2offset(),
                   "primary key index is not ready");
        return pk_cell->pk2offset().find_first_n(limit, bitset);
    }
    if (limit == Unlimited || limit == NoLimit) {
        limit = runtime != nullptr ? runtime->row_count : 0;
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

std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
ChunkedSegmentSealedImpl::find_first_n_element(
    int64_t limit,
    const BitsetTypeView& element_bitset,
    const IArrayOffsets* array_offsets,
    const std::optional<QueryIteratorCursor>& cursor) const {
    auto snapshot = CapturePublishedState();
    auto runtime = snapshot->runtime;
    if (runtime != nullptr && runtime->virtual_pk2offset != nullptr) {
        return runtime->virtual_pk2offset->find_first_n_element(
            limit, element_bitset, array_offsets, cursor);
    }
    if (!is_sorted_by_pk_) {
        auto pk_index = PinPkIndex(runtime, nullptr);
        auto* pk_cell = pk_index.get();
        AssertInfo(pk_cell != nullptr && pk_cell->has_pk2offset(),
                   "primary key index is not ready");
        return pk_cell->pk2offset().find_first_n_element(
            limit, element_bitset, array_offsets, cursor);
    }

    // Sorted by PK, element_id order = (PK, element_index) order
    // Directly iterate element_bitset in order
    if (limit == Unlimited || limit == NoLimit) {
        limit = static_cast<int64_t>(element_bitset.size());
    }

    // We iterate matching elements by global element id, which maps back
    // to (doc_offset, element_offset). The cursor, however, only tells us the
    // last returned PK and element offset. Find the row for that PK first; the
    // scan can then skip only elements from that row whose element offset has
    // already been returned.
    std::optional<int64_t> cursor_doc_offset;
    if (cursor.has_value()) {
        auto schema_snapshot = snapshot->schema;
        auto pk_field_id =
            schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
        AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
        auto pk_column = get_column(runtime, pk_field_id);
        AssertInfo(pk_column != nullptr, "primary key column not loaded");
        switch (schema_snapshot->get_fields().at(pk_field_id).get_data_type()) {
            case DataType::INT64:
                cursor_doc_offset = find_sorted_pk_doc_offset<int64_t>(
                    std::get<int64_t>(cursor->last_pk), pk_column);
                break;
            case DataType::VARCHAR:
                cursor_doc_offset = find_sorted_pk_doc_offset<std::string>(
                    std::get<std::string>(cursor->last_pk), pk_column);
                break;
            default:
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported type {}",
                                      schema_snapshot->get_fields()
                                          .at(pk_field_id)
                                          .get_data_type()));
        }
    }

    int64_t hit_num = 0;
    auto element_size = static_cast<int64_t>(element_bitset.size());
    int64_t cnt = element_size - element_bitset.count();
    auto more_hit_than_limit = cnt > limit;
    limit = std::min(limit, cnt);

    std::vector<int64_t> doc_offsets;
    std::vector<std::vector<int32_t>> element_indices;

    int64_t current_doc_id = -1;
    std::optional<size_t> elem_opt = element_bitset.find_first(false);
    while (elem_opt.has_value() && hit_num < limit) {
        int64_t elem_id = static_cast<int64_t>(elem_opt.value());
        auto [doc_id, elem_idx] = array_offsets->ElementIDToRowID(elem_id);
        if (cursor_doc_offset.has_value() &&
            doc_id == cursor_doc_offset.value() &&
            elem_idx <= cursor->last_element_offset) {
            elem_opt = element_bitset.find_next(elem_id, false);
            continue;
        }

        if (doc_id != current_doc_id) {
            // New document - start a new entry
            doc_offsets.push_back(doc_id);
            element_indices.push_back({static_cast<int32_t>(elem_idx)});
            current_doc_id = doc_id;
        } else {
            // Same document - append to existing entry
            element_indices.back().push_back(static_cast<int32_t>(elem_idx));
        }
        hit_num++;
        elem_opt = element_bitset.find_next(elem_id, false);
    }

    return {std::move(doc_offsets),
            std::move(element_indices),
            more_hit_than_limit && elem_opt.has_value()};
}

ChunkedSegmentSealedImpl::ChunkedSegmentSealedImpl(
    SchemaPtr schema,
    IndexMetaPtr index_meta,
    const SegcoreConfig& segcore_config,
    int64_t segment_id,
    bool is_sorted_by_pk)
    : segcore_config_(segcore_config),
      mmap_descriptor_(storage::MmapManager::GetInstance()
                           .GetMmapChunkManager()
                           ->Register()),
      id_(segment_id),
      col_index_meta_(index_meta),
      is_sorted_by_pk_(is_sorted_by_pk),
      deleted_record_(
          nullptr,
          [this](const std::vector<PkType>& pks,
                 const Timestamp* timestamps,
                 const std::function<void(const SegOffset offset,
                                          const Timestamp ts)>& callback) {
              auto snapshot = CapturePublishedState();
              auto runtime = snapshot->runtime;
              auto schema = snapshot->schema;
              if (snapshot->commit_ts != 0) {
                  // For import segments with commit_ts, row timestamps are
                  // overwritten to commit_ts. Filter with the same published
                  // snapshot used for PK lookup, so pre-commit and
                  // same-timestamp deletes never reach DeletedRecord.
                  auto delete_is_after_insert = [&](size_t i) {
                      return timestamps[i] > snapshot->commit_ts;
                  };
                  if (!is_sorted_by_pk_) {
                      if (runtime != nullptr &&
                          runtime->virtual_pk2offset != nullptr) {
                          for (size_t i = 0; i < pks.size(); i++) {
                              if (!delete_is_after_insert(i)) {
                                  continue;
                              }
                              for (auto offset :
                                   runtime->virtual_pk2offset->find(pks[i])) {
                                  callback(SegOffset(offset), timestamps[i]);
                              }
                          }
                          return;
                      }
                      auto pk_index = PinPkIndex(runtime, nullptr);
                      auto* pk_cell = pk_index.get();
                      if (pk_cell == nullptr || !pk_cell->has_pk2offset()) {
                          return;
                      }
                      for (size_t i = 0; i < pks.size(); i++) {
                          if (!delete_is_after_insert(i)) {
                              continue;
                          }
                          auto offsets = pk_cell->pk2offset().find(pks[i]);
                          for (auto offset : offsets) {
                              callback(SegOffset(offset), timestamps[i]);
                          }
                      }
                  } else {
                      auto pk_field_id =
                          schema->get_primary_field_id().value_or(FieldId(-1));
                      AssertInfo(pk_field_id.get() != -1, "Primary key is -1");
                      auto pk_column = get_column(runtime, pk_field_id);
                      AssertInfo(pk_column != nullptr,
                                 "primary key column not loaded");
                      auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

                      switch (schema->get_fields()
                                  .at(pk_field_id)
                                  .get_data_type()) {
                          case DataType::INT64: {
                              auto num_chunk = pk_column->num_chunks();
                              for (int i = 0; i < num_chunk; ++i) {
                                  const auto& pw = all_chunk_pins[i];
                                  auto src = reinterpret_cast<const int64_t*>(
                                      pw.get()->RawData());
                                  auto chunk_row_num =
                                      pk_column->chunk_row_nums(i);
                                  auto num_rows_until_chunk =
                                      pk_column->GetNumRowsUntilChunk(i);
                                  for (size_t j = 0; j < pks.size(); j++) {
                                      if (!delete_is_after_insert(j)) {
                                          continue;
                                      }
                                      auto target = std::get<int64_t>(pks[j]);
                                      auto it = std::lower_bound(
                                          src, src + chunk_row_num, target);
                                      for (; it != src + chunk_row_num &&
                                             *it == target;
                                           ++it) {
                                          callback(
                                              SegOffset(it - src +
                                                        num_rows_until_chunk),
                                              timestamps[j]);
                                      }
                                  }
                              }
                              break;
                          }
                          case DataType::VARCHAR: {
                              auto num_chunk = pk_column->num_chunks();
                              for (int i = 0; i < num_chunk; ++i) {
                                  auto num_rows_until_chunk =
                                      pk_column->GetNumRowsUntilChunk(i);
                                  const auto& pw = all_chunk_pins[i];
                                  auto string_chunk =
                                      static_cast<StringChunk*>(pw.get());
                                  for (size_t j = 0; j < pks.size(); ++j) {
                                      if (!delete_is_after_insert(j)) {
                                          continue;
                                      }
                                      auto& target =
                                          std::get<std::string>(pks[j]);
                                      auto offset =
                                          string_chunk->binary_search_string(
                                              target);
                                      for (; offset != -1 &&
                                             offset < string_chunk->RowNums() &&
                                             string_chunk->operator[](offset) ==
                                                 target;
                                           ++offset) {
                                          callback(
                                              SegOffset(offset +
                                                        num_rows_until_chunk),
                                              timestamps[j]);
                                      }
                                  }
                              }
                              break;
                          }
                          default:
                              ThrowInfo(DataTypeInvalid,
                                        fmt::format("unsupported type {}",
                                                    schema->get_fields()
                                                        .at(pk_field_id)
                                                        .get_data_type()));
                      }
                  }
              } else {
                  this->search_batch_pks(
                      pks,
                      [&](const size_t idx) { return timestamps[idx]; },
                      false,
                      callback);
              }
          },
          segment_id) {
    auto load_info = std::make_shared<const SegmentLoadInfo>(
        milvus::proto::segcore::SegmentLoadInfo(), schema);
    std::atomic_store(&published_state_,
                      BuildPublishedState(schema, load_info, commit_ts_));
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
    auto snapshot = CapturePublishedState();
    AssertInfo(snapshot->system_field_ready,
               "System field isn't ready when do bulk_insert, segID:{}",
               id_);
    switch (system_type) {
        case SystemFieldType::Timestamp: {
            auto* dst = static_cast<Timestamp*>(output);
            // Import/CDC segments: every row carries commit_ts_, including
            // v2/v3 column-group segments where the raw timestamp column is
            // published in runtime but never overwritten. Short-circuit
            // before consulting the column.
            auto runtime = snapshot->runtime;
            auto effective_commit_ts =
                snapshot->commit_ts != 0
                    ? std::optional<Timestamp>{snapshot->commit_ts}
                    : std::nullopt;
            for (int64_t i = 0; i < count; ++i) {
                dst[i] =
                    ReadTimestamp(seg_offsets[i], runtime, effective_commit_ts);
            }
            break;
        }
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
    auto snapshot = CapturePublishedState();
    auto& field_meta = snapshot->schema->operator[](field_id);
    // Keep a shared column owner from the captured runtime instead of reading
    // through a mutable live map.
    // we have to clone the shared pointer, to make sure it won't get released
    // if segment released
    auto column = get_column(snapshot->runtime, field_id);
    AssertInfo(column != nullptr,
               "field {} must exist when doing bulk_subscript",
               field_id.get());
    AssertInfo(get_bit_if_present(snapshot->field_data_ready_bitset, field_id),
               "field {} must be ready when doing bulk_subscript",
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
                dst->at(offset) = std::string(json.data());
            },
            seg_offsets,
            count);
    } else {
        static_assert(std::is_same_v<S, std::string>);
        column->BulkRawStringAt(
            op_ctx,
            [dst](std::string_view value, size_t offset, bool is_valid) {
                dst->at(offset) = std::string(value);
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

static std::vector<std::string>
ReadTextLobBatch(
    const std::string& lob_base_path,
    const std::vector<milvus_storage::lob_column::EncodedRef>& encoded_refs) {
    if (encoded_refs.empty()) {
        return {};
    }

    auto properties = milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                          .GetProperties();
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();

    auto& cache = GetGlobalTextColumnCache();
    return cache.ReadBatch(lob_base_path, fs, *properties, encoded_refs);
}

void
ChunkedSegmentSealedImpl::bulk_subscript_text_impl(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const ChunkedColumnInterface* column,
    const int64_t* seg_offsets,
    int64_t count,
    google::protobuf::RepeatedPtrField<std::string>* dst) const {
    auto snapshot = CapturePublishedState();
    auto runtime = snapshot->runtime != nullptr ? snapshot->runtime
                                                : BuildRuntimeResourceState();
    auto it = runtime->text_lob_paths.find(field_id);
    AssertInfo(it != runtime->text_lob_paths.end(),
               "TEXT field {} has no LOB path. TEXT type requires StorageV3 "
               "with manifest. segment_id={}",
               field_id.get(),
               id_);
    const auto& lob_base_path = it->second;

    std::vector<milvus_storage::lob_column::EncodedRef> encoded_refs;
    std::vector<int64_t> valid_indices;
    encoded_refs.reserve(count);
    valid_indices.reserve(count);

    column->BulkRawStringAt(
        op_ctx,
        [&encoded_refs, &valid_indices](
            std::string_view value, size_t idx, bool is_valid) {
            if (!is_valid) {
                return;  // skip null values
            }
            encoded_refs.push_back(
                MakeTextLobEncodedRef(value.data(), value.size()));
            valid_indices.push_back(idx);
        },
        seg_offsets,
        count);

    if (encoded_refs.empty()) {
        return;
    }

    auto texts = ReadTextLobBatch(lob_base_path, encoded_refs);
    for (size_t i = 0; i < valid_indices.size() && i < texts.size(); i++) {
        *dst->Mutable(valid_indices[i]) = std::move(texts[i]);
    }
}

void
ChunkedSegmentSealedImpl::ClearData() {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    auto runtime_snapshot = CaptureRuntimeResourceState();
    if (runtime_snapshot != nullptr) {
        for (const auto& [_, indexing] : runtime_snapshot->scalar_indexings) {
            cancel_warmup(indexing);
        }
        for (const auto& [_, entry] : runtime_snapshot->vector_indexings) {
            if (entry != nullptr && entry->indexing_ != nullptr) {
                entry->indexing_->CancelWarmup();
            }
        }
        for (const auto& json_index : runtime_snapshot->json_indices) {
            cancel_warmup(json_index.index);
        }
        for (const auto& [_, path_indexings] :
             runtime_snapshot->ngram_indexings) {
            for (const auto& [__, indexing] : path_indexings) {
                cancel_warmup(indexing);
            }
        }
    }
    {
        std::unique_lock lck(mutex_);
        stats_.mem_size = 0;
    }
    ClearPublishedStateLocked();
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::fill_with_empty(FieldId field_id,
                                          int64_t count,
                                          int64_t valid_count,
                                          const void* valid_data) const {
    auto schema_snapshot = CaptureSchemaSnapshot();
    auto& field_meta = schema_snapshot->operator[](field_id);
    if (IsVectorDataType(field_meta.get_data_type())) {
        return CreateEmptyVectorDataArray(
            count, valid_count, valid_data, field_meta);
    }
    return CreateEmptyScalarDataArray(count, field_meta);
}

void
ChunkedSegmentSealedImpl::CreateTextIndex(FieldId field_id,
                                          milvus::OpContext* op_ctx) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    CreateTextIndexWithSchema(
        field_id, CaptureSchemaSnapshot(), op_ctx, true, nullptr);
}

void
ChunkedSegmentSealedImpl::ScopedTextIndexBuildGuard::Register() {
    std::unique_lock lck(segment_.mutex_);

    AssertInfo(segment_.pending_text_index_fields_.count(field_id_) == 0,
               "text index for field {} is already being built",
               field_id_.get());

    segment_.pending_text_index_fields_.insert(field_id_);
    registered_ = true;
}

void
ChunkedSegmentSealedImpl::ScopedTextIndexBuildGuard::Commit() {
    std::unique_lock lck(segment_.mutex_);

    AssertInfo(segment_.pending_text_index_fields_.count(field_id_) == 1,
               "text index for field {} lost pending build state",
               field_id_.get());

    segment_.pending_text_index_fields_.erase(field_id_);
    committed_ = true;
}

ChunkedSegmentSealedImpl::ScopedTextIndexBuildGuard::
    ~ScopedTextIndexBuildGuard() {
    if (!registered_ || committed_) {
        return;
    }
    std::unique_lock lck(segment_.mutex_);
    segment_.pending_text_index_fields_.erase(field_id_);
}

void
ChunkedSegmentSealedImpl::CreateTextIndexWithSchema(
    FieldId field_id,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    bool publish_marker,
    RuntimeResourceState* runtime) {
    CheckCancellation(op_ctx,
                      id_,
                      field_id.get(),
                      "ChunkedSegmentSealedImpl::CreateTextIndex()");

    std::shared_ptr<RuntimeResourceState> owned_runtime;
    auto* target_runtime = runtime;
    if (target_runtime == nullptr) {
        owned_runtime = CloneMutableRuntimeResourceState();
        target_runtime = owned_runtime.get();
    }
    AssertInfo(target_runtime->text_indexes.find(field_id) ==
                   target_runtime->text_indexes.end(),
               "text index for field {} already exists, refusing to rebuild",
               field_id.get());

    ScopedTextIndexBuildGuard build_guard(*this, field_id);
    build_guard.Register();

    const auto& field_meta = schema_snapshot->operator[](field_id);
    auto& cfg = storage::MmapManager::GetInstance().GetMmapConfig();
    std::unique_ptr<index::TextMatchIndex> index;
    std::string unique_id = GetUniqueFieldId(field_meta.get_id().get());
    if (!cfg.GetScalarIndexEnableMmap()) {
        // build text index in ram.
        // Sealed interim index: no background merge — finish() ends with an
        // explicit merge-all, and a racing policy merge (which finish()'s
        // NoMergePolicy cannot cancel once started) would reintroduce the
        // "segments could not be found in the SegmentManager" failure.
        index = std::make_unique<index::TextMatchIndex>(
            std::numeric_limits<int64_t>::max(),
            unique_id.c_str(),
            "milvus_tokenizer",
            field_meta.get_analyzer_params().c_str(),
            /*enable_background_merge=*/false);
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
        auto it = target_runtime->fields.find(field_id);
        std::shared_ptr<ChunkedColumnInterface> column =
            it != target_runtime->fields.end() ? it->second : nullptr;
        if (column) {
            // Check for cancellation before bulk operation
            CheckCancellation(op_ctx,
                              id_,
                              field_id.get(),
                              "ChunkedSegmentSealedImpl::CreateTextIndex()");
            if (field_meta.get_data_type() == DataType::TEXT) {
                const auto* path_map = &target_runtime->text_lob_paths;
                auto it = path_map->find(field_id);
                AssertInfo(it != path_map->end(),
                           "TEXT field {} has no LOB path. TEXT type "
                           "requires StorageV3 with manifest. segment_id={}",
                           field_id.get(),
                           id_);

                const auto& lob_base_path = it->second;

                struct TextIndexEntry {
                    size_t offset;
                    bool is_valid;
                    size_t text_index;
                };
                std::vector<TextIndexEntry> entries;
                std::vector<milvus_storage::lob_column::EncodedRef>
                    encoded_refs;
                auto flush_text_entries = [&]() {
                    if (entries.empty()) {
                        return;
                    }
                    auto texts = ReadTextLobBatch(lob_base_path, encoded_refs);
                    AssertInfo(texts.size() == encoded_refs.size(),
                               "TEXT field {} LOB batch read returned {} "
                               "texts for {} refs. segment_id={}",
                               field_id.get(),
                               texts.size(),
                               encoded_refs.size(),
                               id_);
                    for (const auto& entry : entries) {
                        if (!entry.is_valid) {
                            index->AddNullSealed(entry.offset);
                            continue;
                        }
                        index->AddTextSealed(
                            texts[entry.text_index], true, entry.offset);
                    }
                    entries.clear();
                    encoded_refs.clear();
                    CheckCancellation(
                        op_ctx,
                        id_,
                        field_id.get(),
                        "ChunkedSegmentSealedImpl::CreateTextIndex()");
                };
                column->BulkRawStringAt(
                    nullptr,
                    [&](std::string_view value, size_t offset, bool is_valid) {
                        if (!is_valid) {
                            entries.push_back({offset, false, 0});
                            if (entries.size() >= kTextLobIndexBuildBatchSize) {
                                flush_text_entries();
                            }
                            return;
                        }
                        entries.push_back({offset, true, encoded_refs.size()});
                        encoded_refs.push_back(
                            MakeTextLobEncodedRef(value.data(), value.size()));
                        if (encoded_refs.size() >=
                                kTextLobIndexBuildBatchSize ||
                            entries.size() >= kTextLobIndexBuildBatchSize) {
                            flush_text_entries();
                        }
                    });
                flush_text_entries();
            } else {
                column->BulkRawStringAt(
                    nullptr,
                    [&](std::string_view value, size_t offset, bool is_valid) {
                        index->AddTextSealed(
                            std::string(value), is_valid, offset);
                    });
            }
        } else {  // fetch raw data from index.
            auto field_index_iter =
                target_runtime->scalar_indexings.find(field_id);
            AssertInfo(
                field_index_iter != target_runtime->scalar_indexings.end(),
                "failed to create text index, neither raw data nor "
                "index are found");
            auto accessor =
                SemiInlineGet(field_index_iter->second->PinCells(op_ctx, {0}));
            auto ptr = accessor->get_cell_of(0);
            AssertInfo(ptr->HasRawData(),
                       "text raw data not found, trying to create text index "
                       "from index, but this index don't contain raw data");
            auto impl = dynamic_cast<index::ScalarIndex<std::string>*>(ptr);
            AssertInfo(impl != nullptr,
                       "failed to create text index, field index cannot be "
                       "converted to string index");
            auto n = impl->Count();
            for (size_t i = 0; i < n; i++) {
                auto raw = impl->Reverse_Lookup(i);
                if (!raw.has_value()) {
                    index->AddNullSealed(i);
                    continue;
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

    auto text_index_holder = std::make_shared<index::TextMatchIndexHolder>(
        std::move(index), cfg.GetScalarIndexEnableMmap());

    CheckCancellation(op_ctx,
                      id_,
                      field_id.get(),
                      "ChunkedSegmentSealedImpl::CreateTextIndex()");
    target_runtime->text_indexes.emplace(field_id,
                                         std::move(text_index_holder));
    build_guard.Commit();

    if (owned_runtime != nullptr) {
        auto current = CapturePublishedState();
        auto next = ClonePublishedState(current);
        next->runtime = ToConstRuntimeState(std::move(owned_runtime));
        if (publish_marker) {
            next->load_info =
                CloneLoadInfoWithTextIndexCreated(next->load_info, field_id);
        }
        NormalizePublishedState(*next);
        PublishStateOnline(std::move(next));
    }
}

void
ChunkedSegmentSealedImpl::CreateTextIndexWithSchema(
    FieldId field_id,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    StagedStateCommitter& committer) {
    CreateTextIndexWithSchema(
        field_id, schema_snapshot, op_ctx, false, committer.runtime());
}

void
ChunkedSegmentSealedImpl::RecordDefaultFieldsFilledLocked(
    const std::vector<FieldId>& field_ids) {
    if (field_ids.empty()) {
        return;
    }
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        state.load_info =
            CloneLoadInfoWithDefaultFilled(state.load_info, field_ids);
        state.use_take_for_output = state.load_info != nullptr &&
                                    state.load_info->GetUseTakeForOutput();
        ClearFieldBitsForAbsentLoadInfo(state);
        SetSystemFieldReadyInState(state, state.load_info.get());
    });
}

void
ChunkedSegmentSealedImpl::RecordDefaultFieldsFilled(
    const std::vector<FieldId>& field_ids) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    RecordDefaultFieldsFilledLocked(field_ids);
}

void
ChunkedSegmentSealedImpl::RecordTextIndexCreated(
    SegmentLoadInfo& segment_load_info, FieldId field_id) {
    segment_load_info.SetTextIndexCreated(field_id);
}
ChunkedSegmentSealedImpl::TextIndexVariant
ChunkedSegmentSealedImpl::BuildTextIndexFromFiles(
    milvus::OpContext* op_ctx,
    const std::shared_ptr<proto::indexcgo::LoadTextIndexInfo>& info_proto,
    const SegmentLoadInfo& segment_load_info) {
    // Check for cancellation before starting
    CheckCancellation(
        op_ctx, id_, "ChunkedSegmentSealedImpl::BuildTextIndexFromFiles()");

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
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    AssertInfo(fs != nullptr, "arrow file system is null");

    milvus::Config config;
    std::vector<std::string> files;
    for (const auto& f : info_proto->files()) {
        files.push_back(f);
    }
    config[milvus::index::INDEX_FILES] = files;
    config[milvus::LOAD_PRIORITY] = info_proto->load_priority();
    config[milvus::index::ENABLE_MMAP] = info_proto->enable_mmap();
    config[milvus::index::COLLECTION_ID] = info_proto->collectionid();
    if (info_proto->warmup_policy() != "") {
        config[milvus::index::WARMUP] = info_proto->warmup_policy();
    }
    if (!info_proto->base_path().empty()) {
        config[STATS_BASE_PATH_KEY] = info_proto->base_path();
    }
    milvus::storage::FileManagerContext file_ctx(
        field_data_meta, index_meta, remote_chunk_manager, fs);
    if (!info_proto->base_path().empty()) {
        file_ctx.set_stats_base_path(info_proto->base_path());
    }

    // const auto& field_meta = schema_->operator[](field_id);
    milvus::segcore::storagev1translator::TextMatchIndexLoadInfo load_info{
        info_proto->enable_mmap(),
        this->get_segment_id(),
        info_proto->fieldid(),
        field_meta.get_analyzer_params(),
        info_proto->index_size(),
        info_proto->warmup_policy(),
        segment_load_info.GetInsertChannel()};

    std::unique_ptr<
        milvus::cachinglayer::Translator<milvus::index::TextMatchIndex>>
        translator = std::make_unique<
            milvus::segcore::storagev1translator::TextMatchIndexTranslator>(
            load_info, file_ctx, config);
    auto cache_slot =
        milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
            std::move(translator), op_ctx);

    CheckCancellation(
        op_ctx, id_, "ChunkedSegmentSealedImpl::BuildTextIndexFromFiles()");
    return cache_slot;
}

std::shared_ptr<index::JsonKeyStats>
ChunkedSegmentSealedImpl::BuildJsonKeyStatsIndex(
    milvus::OpContext* op_ctx,
    const std::shared_ptr<milvus::proto::indexcgo::LoadJsonKeyIndexInfo>&
        info_proto) {
    auto field_id = milvus::FieldId(info_proto->fieldid());
    CheckCancellation(op_ctx,
                      id_,
                      field_id.get(),
                      "ChunkedSegmentSealedImpl::BuildJsonKeyStatsIndex()");

    if (!JSON_KEY_STATS_ENABLED.load()) {
        LOG_WARN(
            "skip load json key stats because json key stats is disabled, "
            "segment:{}, field:{}, build:{}, version:{}",
            id_,
            info_proto->fieldid(),
            info_proto->buildid(),
            info_proto->version());
        return nullptr;
    }

    LOG_INFO(
        "start load json key stats, segment:{}, field:{}, build:{}, "
        "version:{}, "
        "file_count:{}, base_path:{}, enable_mmap:{}, stats_size:{}",
        id_,
        info_proto->fieldid(),
        info_proto->buildid(),
        info_proto->version(),
        info_proto->files_size(),
        info_proto->base_path(),
        info_proto->enable_mmap(),
        info_proto->stats_size());

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
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    AssertInfo(fs != nullptr, "arrow file system is null");

    milvus::Config config;
    std::vector<std::string> files;
    files.reserve(info_proto->files_size());
    for (const auto& f : info_proto->files()) {
        files.push_back(f);
    }
    config[milvus::index::INDEX_FILES] = files;
    config[milvus::LOAD_PRIORITY] = info_proto->load_priority();
    config[milvus::index::ENABLE_MMAP] = info_proto->enable_mmap();
    if (info_proto->enable_mmap()) {
        config[milvus::index::MMAP_FILE_PATH] = info_proto->mmap_dir_path();
    }
    if (!info_proto->warmup_policy().empty()) {
        config[milvus::index::WARMUP] = info_proto->warmup_policy();
    }
    config[milvus::index::INDEX_SIZE] = info_proto->stats_size();
    if (!info_proto->base_path().empty()) {
        config[STATS_BASE_PATH_KEY] = info_proto->base_path();
    }
    auto load_info_snapshot = CaptureLoadInfoSnapshot();
    config[JSON_STATS_CACHE_SHARD_KEY] = load_info_snapshot->GetInsertChannel();

    milvus::storage::FileManagerContext file_ctx(
        field_data_meta, index_meta, remote_chunk_manager, fs);
    auto index = std::make_shared<milvus::index::JsonKeyStats>(file_ctx, true);
    milvus::tracer::TraceContext trace_ctx;
    try {
        milvus::ScopedTimer timer(
            "json_stats_load",
            [](double us) {
                milvus::monitor::internal_json_stats_latency_load.Observe(
                    us / 1000.0);
            },
            milvus::ScopedTimer::LogLevel::Info);
        index->Load(trace_ctx, config);
    } catch (std::exception& e) {
        LOG_WARN(
            "failed load json key stats, segment:{}, field:{}, build:{}, "
            "version:{}, error:{}",
            id_,
            info_proto->fieldid(),
            info_proto->buildid(),
            info_proto->version(),
            e.what());
        throw;
    }

    LOG_INFO(
        "load json key stats success, segment:{}, field:{}, build:{}, "
        "version:{}",
        id_,
        info_proto->fieldid(),
        info_proto->buildid(),
        info_proto->version());
    return index;
}

void
ChunkedSegmentSealedImpl::LoadBatchJsonKeyIndexes(
    milvus::OpContext* op_ctx,
    const std::unordered_map<
        FieldId,
        std::shared_ptr<milvus::proto::indexcgo::LoadJsonKeyIndexInfo>>& infos,
    const SchemaPtr& schema_snapshot,
    StagedStateCommitter& committer) {
    for (const auto& [field_id, info_proto] : infos) {
        AssertInfo(field_exists_in_schema(schema_snapshot, field_id),
                   "field {} not found in schema when loading json stats",
                   field_id.get());
        auto index = BuildJsonKeyStatsIndex(op_ctx, info_proto);
        if (index == nullptr) {
            continue;
        }
        committer.Commit(
            [field_id = field_id, index = std::move(index)](
                RuntimeResourceState& runtime, PublishedSegmentState&) mutable {
                runtime.json_stats[field_id] = std::move(index);
            });
    }
}

void
ChunkedSegmentSealedImpl::FillPrimaryKeys(const query::Plan* plan,
                                          SearchResult& results,
                                          milvus::OpContext* op_ctx) const {
    {
        std::shared_lock lck(mutex_);
        AssertInfo(plan, "empty plan");
        auto size = results.distances_.size();
        AssertInfo(results.seg_offsets_.size() == size,
                   "Size of result distances is not equal to size of ids");

        auto snapshot = CapturePublishedState();
        auto schema_snapshot = snapshot->schema;
        auto pk_field_id_opt = schema_snapshot->get_primary_field_id();
        AssertInfo(pk_field_id_opt.has_value(),
                   "Cannot get primary key offset from schema");
        auto pk_field_id = pk_field_id_opt.value();
        auto pk_type = schema_snapshot->operator[](pk_field_id).get_data_type();
        AssertInfo(IsPrimaryKeyDataType(pk_type),
                   "Primary key field is not INT64 or VARCHAR type");
        if (pk_type == DataType::VARCHAR) {
            auto column = get_column(snapshot->runtime, pk_field_id);
            if (column != nullptr) {
                Assert(results.primary_keys_.size() == 0);
                results.primary_keys_.resize(size);
                results.pk_type_ = pk_type;
                if (size == 0) {
                    return;
                }

                segcore::CheckCancellation(
                    op_ctx, get_segment_id(), "FillPrimaryKeys");
                milvus::OpContext local_ctx;
                if (op_ctx != nullptr) {
                    local_ctx.cancellation_token = op_ctx->cancellation_token;
                    local_ctx.runtime_load_priority =
                        op_ctx->runtime_load_priority;
                }
                // Intentionally bypass bulk_subscript's scalar-index raw-data
                // routing here: the sealed VARCHAR PK fast path reads the
                // loaded column directly to avoid DataArray materialization
                // and index reverse-lookup overhead. Storage-cost accounting
                // can differ if both index raw data and the column are
                // resident, but returned PK values are identical.
                column->BulkRawStringAt(
                    &local_ctx,
                    [&results](
                        std::string_view value, size_t offset, bool is_valid) {
                        AssertInfo(is_valid, "primary key must not be null");
                        results.primary_keys_[offset] = std::string(value);
                    },
                    results.seg_offsets_.data(),
                    size);
                results.search_storage_cost_.scanned_remote_bytes +=
                    local_ctx.storage_usage.scanned_cold_bytes.load();
                results.search_storage_cost_.scanned_total_bytes +=
                    local_ctx.storage_usage.scanned_total_bytes.load();
                return;
            }
        }
    }

    SegmentInternalInterface::FillPrimaryKeys(plan, results, op_ctx);
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::get_raw_data(milvus::OpContext* op_ctx,
                                       FieldId field_id,
                                       const FieldMeta& field_meta,
                                       const int64_t* seg_offsets,
                                       int64_t count) const {
    // Keep a shared column owner from the captured runtime so the column stays
    // alive even if a newer runtime is published.
    auto snapshot = CapturePublishedState();
    auto column = get_column(snapshot->runtime, field_id);
    AssertInfo(column != nullptr,
               "field {} must exist when getting raw data",
               field_id.get());
    int64_t valid_count = count;
    const bool* valid_data = nullptr;
    const int64_t* valid_offsets = seg_offsets;
    ValidResult filter_result;

    const bool nullable_vector =
        field_meta.is_vector() && field_meta.is_nullable();
    if (nullable_vector) {
        filter_result = FilterVectorValidOffsetsFromColumn(
            op_ctx, column.get(), seg_offsets, count);
        valid_count = filter_result.valid_count;
        valid_data = filter_result.valid_data.get();
        valid_offsets = filter_result.valid_offsets.data();
    }
    auto ret = fill_with_empty(field_id, count, valid_count, valid_data);
    if (field_meta.is_vector() && valid_count == 0) {
        return ret;
    }

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
        case DataType::STRING: {
            bulk_subscript_ptr_impl<std::string>(
                op_ctx,
                column.get(),
                seg_offsets,
                count,
                ret->mutable_scalars()->mutable_string_data()->mutable_data());
            break;
        }

        case DataType::TEXT: {
            // TEXT type is only supported in StorageV3 with LOB files.
            auto runtime = snapshot->runtime != nullptr
                               ? snapshot->runtime
                               : BuildRuntimeResourceState();
            auto it = runtime->text_lob_paths.find(field_id);
            AssertInfo(it != runtime->text_lob_paths.end(),
                       "TEXT field {} has no LOB path. TEXT type requires "
                       "StorageV3 with manifest. segment_id={}",
                       field_id.get(),
                       id_);
            bulk_subscript_text_impl(
                op_ctx,
                field_id,
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
            // Carry the element type into the response so the caller (and
            // SDK) can dispatch on it. Without this, callers parse a
            // headless ScalarField_ArrayData and reject it with
            // "unsupported element type None". Regression fix for #48619.
            ret->mutable_scalars()->mutable_array_data()->set_element_type(
                static_cast<milvus::proto::schema::DataType>(
                    field_meta.get_element_type()));
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
            auto dst =
                ret->mutable_vectors()->mutable_vector_array()->mutable_data();
            if (nullable_vector) {
                if (valid_count == 0) {
                    break;
                }
                std::vector<int64_t> valid_logical_offsets;
                valid_logical_offsets.reserve(valid_count);
                for (int64_t i = 0; i < count; ++i) {
                    if (valid_data[i]) {
                        valid_logical_offsets.push_back(i);
                    }
                }
                column->BulkVectorArrayAt(
                    op_ctx,
                    [dst, &valid_logical_offsets](VectorFieldProto&& array,
                                                  size_t i) {
                        dst->at(valid_logical_offsets[i]) = std::move(array);
                    },
                    valid_offsets,
                    valid_count);
            } else {
                bulk_subscript_vector_array_impl(
                    op_ctx, column.get(), seg_offsets, count, dst);
            }
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
    auto snapshot = CapturePublishedState();
    auto& field_meta = snapshot->schema->operator[](field_id);
    // if count == 0, return empty data array
    if (count == 0) {
        return fill_with_empty(field_id, count);
    }

    // Fast path for int64 PK field: use compressed offset2pk index
    auto pk_field_id = snapshot->schema->get_primary_field_id();
    auto pk_index = PinPkIndex(snapshot->runtime, op_ctx);
    if (pk_field_id.has_value() && pk_field_id.value() == field_id &&
        field_meta.get_data_type() == DataType::INT64 &&
        pk_index.get() != nullptr && pk_index.get()->has_int64_pk_index()) {
        auto ret = fill_with_empty(field_id, count);
        auto* output = ret->mutable_scalars()
                           ->mutable_long_data()
                           ->mutable_data()
                           ->mutable_data();
        pk_index.get()->bulk_get_int64_pks_by_offsets(
            seg_offsets, count, output);
        return ret;
    }

    // Decide once whether to serve this retrieve from column data instead of
    // the index-backed raw data. The flag is off by default, so short-circuit
    // before touching HasFieldData — that call takes a shared_lock and would
    // otherwise be paid on every retrieve regardless of the flag.
    bool use_field_data =
        SegcoreConfig::default_config()
            .get_prefer_field_data_when_index_has_raw_data() &&
        HasFieldData(field_id);

    if (!IsVectorDataType(field_meta.get_data_type())) {
        // === Scalar field ===
        if (!use_field_data) {
            // Try index first: if scalar index exists and has raw data, read from index
            PinWrapper<const index::IndexBase*> pin_scalar_index_ptr;
            auto scalar_indexes = PinIndex(op_ctx, field_id);
            if (!scalar_indexes.empty()) {
                pin_scalar_index_ptr = std::move(scalar_indexes[0]);
                if (IndexHasRawData(field_id)) {
                    return ReverseDataFromIndex(pin_scalar_index_ptr.get(),
                                                seg_offsets,
                                                count,
                                                field_meta);
                }
            }
        }
        return get_raw_data(op_ctx, field_id, field_meta, seg_offsets, count);
    }

    // === Vector field ===
    std::chrono::high_resolution_clock::time_point get_vector_start =
        std::chrono::high_resolution_clock::now();

    std::unique_ptr<DataArray> vector{nullptr};
    // Try index first: if vector index exists and has raw data, read from index
    if (!use_field_data && IndexHasRawData(field_id)) {
        if (IsVectorArrayDataType(field_meta.get_data_type())) {
            vector =
                get_emb_list(op_ctx, field_id, field_meta, seg_offsets, count);
        } else {
            vector = get_vector(op_ctx, field_id, seg_offsets, count);
        }
    } else {
        // retrieve data from column data instead of index
        // we could reject remote vector output if the vector is not loaded in local cache
        // for performance needs
        if (SegcoreConfig::default_config().get_reject_remote_vector_output()) {
            auto column = get_column(field_id);
            AssertInfo(column != nullptr,
                       "field {} must exist when getting raw data",
                       field_id.get());
            CheckVectorOutputCellsLoaded(
                id_, field_id, field_meta, column.get(), seg_offsets, count);
        }
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
    auto snapshot = CapturePublishedState();
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
            dst->at(offset) = ExtractSubJson(json.data(), dynamic_field_names);
        },
        seg_offsets,
        count);
    return ret;
}

bool
ChunkedSegmentSealedImpl::HasIndex(FieldId field_id) const {
    auto snapshot = CapturePublishedState();
    if (!SystemProperty::Instance().IsSystem(field_id) &&
        !field_exists_in_schema(snapshot->schema, field_id)) {
        return false;
    }
    return get_bit(snapshot->index_ready_bitset, field_id) ||
           get_bit(snapshot->binlog_index_bitset, field_id);
}

bool
ChunkedSegmentSealedImpl::HasJsonIndex(FieldId field_id) const {
    // JSON indexes (JsonFlatIndex + JSON-cast) remain distinct from the
    // scalar/vector/binlog readiness bitsets, but their ownership now follows
    // the published runtime snapshot.
    auto runtime = CaptureRuntimeResourceState();
    for (const auto& index : runtime->json_indices) {
        if (index.field_id == field_id) {
            return true;
        }
    }
    return false;
}

bool
ChunkedSegmentSealedImpl::HasFieldData(FieldId field_id) const {
    auto snapshot = CapturePublishedState();
    if (SystemProperty::Instance().IsSystem(field_id)) {
        return snapshot->system_field_ready;
    }
    if (!field_exists_in_schema(snapshot->schema, field_id)) {
        return false;
    }
    if (get_bit(snapshot->field_data_ready_bitset, field_id)) {
        return true;
    }
    return snapshot->load_info != nullptr &&
           snapshot->load_info->IsFieldFilledWithDefault(field_id);
}

// Checks the cached loaded manifest instead of field-data/index bitsets.
bool
ChunkedSegmentSealedImpl::HasColumnInLoadedManifest(
    const std::string& column_name) const {
    auto load_info = CaptureLoadInfoSnapshot();
    if (load_info == nullptr || !load_info->HasManifestPath()) {
        return true;
    }
    return load_info->HasManifestColumn(column_name);
}

std::pair<std::shared_ptr<ChunkedColumnInterface>, bool>
ChunkedSegmentSealedImpl::GetFieldDataIfExist(FieldId field_id) const {
    auto snapshot = CapturePublishedState();
    auto runtime = snapshot->runtime != nullptr ? snapshot->runtime
                                                : BuildRuntimeResourceState();
    auto it = runtime->fields.find(field_id);
    auto column = it != runtime->fields.end() ? it->second : nullptr;
    bool exists;
    if (SystemProperty::Instance().IsSystem(field_id)) {
        exists = snapshot->system_field_ready && column != nullptr;
    } else {
        exists =
            get_bit_if_present(snapshot->field_data_ready_bitset, field_id) &&
            column != nullptr;
    }
    if (!exists) {
        return {nullptr, false};
    }
    return {column, true};
}

bool
ChunkedSegmentSealedImpl::HasRawData(int64_t field_id) const {
    std::shared_lock lck(mutex_);
    auto snapshot = CapturePublishedState();
    auto fieldID = FieldId(field_id);
    const auto& field_meta = snapshot->schema->operator[](fieldID);

    if (IsVectorDataType(field_meta.get_data_type())) {
        if (get_bit(snapshot->index_ready_bitset, fieldID)) {
            AssertInfo(GetVectorIndexing(snapshot->runtime, fieldID) != nullptr,
                       "vector index is not ready");
        } else if (get_bit(snapshot->binlog_index_bitset, fieldID)) {
            AssertInfo(GetVectorIndexing(snapshot->runtime, fieldID) != nullptr,
                       "interim index is not ready");
        }
    }
    return HasRawDataFromState(*snapshot, fieldID);
}

bool
ChunkedSegmentSealedImpl::IndexHasRawData(FieldId field_id) const {
    auto snapshot = CapturePublishedState();
    return IndexHasRawDataFromState(*snapshot, field_id);
}

bool
ChunkedSegmentSealedImpl::CalcDistByIDs(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const knowhere::DataSetPtr& query_dataset,
    const int64_t* seg_offsets,
    size_t count,
    bool is_cosine,
    float* distances) const {
    std::shared_lock vector_state_lck(mutex_);
    auto runtime = CaptureRuntimeResourceState();
    auto vector_entry = GetVectorIndexing(runtime, field_id);
    if (vector_entry == nullptr) {
        return false;
    }
    auto accessor =
        SemiInlineGet(vector_entry->indexing_->PinCells(op_ctx, {0}));
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(accessor->get_cell_of(0));
    if (vec_index == nullptr) {
        return false;
    }
    // Callers pass logical offsets (already translated from physical by
    // SearchOnIndex). When the index carries an offset_mapping (nullable
    // vector), the underlying knowhere index operates on physical offsets,
    // so translate logical -> physical before the call.
    const auto& offset_mapping = vec_index->GetOffsetMapping();
    std::vector<int64_t> physical_offsets;
    const int64_t* labels = seg_offsets;
    if (offset_mapping.IsEnabled()) {
        physical_offsets.assign(seg_offsets, seg_offsets + count);
        offset_mapping.TransformLogicalOffsets(physical_offsets);
        labels = physical_offsets.data();
    }
    auto res = vec_index->CalcDistByIDs(
        query_dataset, BitsetView(), labels, count, is_cosine, op_ctx);
    if (!res.has_value()) {
        return false;
    }
    auto result_distances = res.value()->GetDistance();
    if (result_distances == nullptr) {
        return false;
    }
    milvus::fastmem::FastMemcpy(
        distances, result_distances, count * sizeof(float));
    return true;
}

bool
ChunkedSegmentSealedImpl::IsIndexRefineEnabledLocked(
    milvus::OpContext* op_ctx,
    FieldId field_id,
    const std::shared_ptr<const RuntimeResourceState>& runtime) const {
    auto vector_entry = GetVectorIndexing(runtime, field_id);
    if (vector_entry == nullptr) {
        return false;
    }
    auto accessor =
        SemiInlineGet(vector_entry->indexing_->PinCells(op_ctx, {0}));
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(accessor->get_cell_of(0));
    return vec_index != nullptr && vec_index->IsIndexRefineEnabled();
}

bool
ChunkedSegmentSealedImpl::IsIndexRefineEnabled(milvus::OpContext* op_ctx,
                                               FieldId field_id) const {
    std::shared_lock vector_state_lck(mutex_);
    return IsIndexRefineEnabledLocked(
        op_ctx, field_id, CaptureRuntimeResourceState());
}

DataType
ChunkedSegmentSealedImpl::GetFieldDataType(milvus::FieldId field_id) const {
    auto schema_snapshot = CaptureSchemaSnapshot();
    auto& field_meta = schema_snapshot->operator[](field_id);
    return field_meta.get_data_type();
}

void
ChunkedSegmentSealedImpl::search_ids(BitsetType& bitset,
                                     const IdArray& id_array) const {
    auto schema_snapshot = CaptureSchemaSnapshot();
    auto field_id =
        schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_snapshot->operator[](field_id);
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
    auto snapshot = CapturePublishedState();
    auto schema_snapshot = snapshot->schema;
    auto runtime = snapshot->runtime;
    auto field_id =
        schema_snapshot->get_primary_field_id().value_or(FieldId(-1));
    AssertInfo(field_id.get() != -1, "Primary key is -1");
    auto& field_meta = schema_snapshot->operator[](field_id);
    std::vector<PkType> pks(size);
    ParsePksFromIDs(pks, field_meta.get_data_type(), *ids);

    // filter out the deletions that the primary key not exists
    std::vector<std::tuple<Timestamp, PkType>> ordering(size);
    for (int i = 0; i < size; i++) {
        ordering[i] = std::make_tuple(timestamps_raw[i], pks[i]);
    }
    // If PK state is unavailable (for example, only metadata is loaded by the
    // Go-side cache), filtering could lose deletions, so preserve them all.
    auto pk_index = PinPkIndex(runtime, nullptr);
    auto virtual_pk2offset =
        runtime != nullptr ? runtime->virtual_pk2offset : nullptr;
    auto has_pk_index =
        virtual_pk2offset != nullptr ||
        (pk_index.get() != nullptr && !pk_index.get()->empty_pks());
    if (has_pk_index) {
        auto end = std::remove_if(
            ordering.begin(),
            ordering.end(),
            [&](const std::tuple<Timestamp, PkType>& record) {
                if (virtual_pk2offset != nullptr) {
                    return !virtual_pk2offset->contain(std::get<1>(record));
                }
                return !pk_index.get()->contain(std::get<1>(record));
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
    ThrowInfo(NotImplemented, "unimplemented");
}

int64_t
ChunkedSegmentSealedImpl::get_active_count(Timestamp ts) const {
    // TODO optimize here to reduce expr search range
    return this->get_row_count();
}

// Helper: apply a per-element timestamp scan over a range [beg, end),
// calling `pred(global_offset, ts_value)` for each row.
// Overload for TimestampData (StorageV1 / growing segment path).
template <typename Pred>
static void
scan_timestamp_range(const TimestampData& ts,
                     int64_t beg,
                     int64_t end,
                     Pred pred) {
    for (int64_t c = 0; c < ts.num_chunks(); c++) {
        auto chunk_start = ts.chunk_start_offset(c);
        auto chunk_end = chunk_start + ts.chunk_row_count(c);
        auto overlap_beg = std::max(beg, chunk_start);
        auto overlap_end = std::min(end, chunk_end);
        if (overlap_beg >= overlap_end) {
            continue;
        }
        auto* data = ts.chunk_data(c);
        auto local = overlap_beg - chunk_start;
        for (int64_t i = overlap_beg; i < overlap_end; ++i, ++local) {
            pred(i, data[local]);
        }
    }
}

// Overload for ChunkedColumnInterface (StorageV2 sealed segment path).
// Pins each chunk on demand and releases after scanning.
template <typename Pred>
static void
scan_timestamp_range(const ChunkedColumnInterface& column,
                     int64_t beg,
                     int64_t end,
                     Pred pred) {
    auto num_chunks = column.num_chunks();
    int64_t chunk_start = 0;
    for (int64_t c = 0; c < num_chunks; c++) {
        auto chunk_rows = column.chunk_row_nums(c);
        auto chunk_end = chunk_start + chunk_rows;
        auto overlap_beg = std::max(beg, chunk_start);
        auto overlap_end = std::min(end, chunk_end);
        if (overlap_beg >= overlap_end) {
            chunk_start = chunk_end;
            continue;
        }
        auto pw = column.DataOfChunk(nullptr, c);
        auto* data = reinterpret_cast<const Timestamp*>(pw.get());
        auto local = overlap_beg - chunk_start;
        for (int64_t i = overlap_beg; i < overlap_end; ++i, ++local) {
            pred(i, data[local]);
        }
        chunk_start = chunk_end;
    }
}

void
ChunkedSegmentSealedImpl::mask_with_timestamps(BitsetTypeView& bitset_chunk,
                                               Timestamp timestamp,
                                               Timestamp collection_ttl) const {
    auto snapshot = CapturePublishedState();
    auto schema_snapshot = snapshot->schema;
    // External collections have no timestamps; all data is always visible
    if (schema_snapshot->is_external_collection()) {
        return;
    }
    auto runtime = snapshot->runtime;
    AssertInfo(runtime != nullptr && runtime->timestamp_index != nullptr,
               "timestamp index is not ready");
    auto& ts_index_data = *runtime->timestamp_index;
    auto effective_commit_ts =
        snapshot->commit_ts != 0 ? std::optional<Timestamp>{snapshot->commit_ts}
                                 : std::nullopt;
    auto total_size = runtime->row_count;

    auto do_scan = [&](int64_t beg, int64_t end, auto pred) {
        for (int64_t i = beg; i < end; ++i) {
            pred(i, ReadTimestamp(i, runtime, effective_commit_ts));
        }
    };

    if (collection_ttl > 0) {
        auto range = ts_index_data.get_active_range(collection_ttl);
        if (range.first == range.second && range.first == total_size) {
            bitset_chunk.set();
            return;
        } else {
            // TTL bitset: [0, beg) = true, [beg, end) = check, [end, size) = false
            BitsetType ttl_mask;
            ttl_mask.reserve(total_size);
            ttl_mask.resize(range.first, true);
            ttl_mask.resize(total_size, false);
            do_scan(range.first, range.second, [&](int64_t i, Timestamp val) {
                ttl_mask[i] = val <= collection_ttl;
            });
            bitset_chunk |= ttl_mask;
        }
    }

    auto range = ts_index_data.get_active_range(timestamp);

    // range == (size_, size_): all data is useful, no filtering needed.
    if (range.first == range.second && range.first == total_size) {
        return;
    }
    // range == (0, 0): all data is too new, mask everything out.
    if (range.first == range.second && range.first == 0) {
        bitset_chunk.set();
        return;
    }
    // [0, beg) = false, [beg, end) = check, [end, size) = true
    BitsetType mask;
    mask.reserve(total_size);
    mask.resize(range.first, false);
    mask.resize(total_size, true);
    do_scan(range.first, range.second, [&](int64_t i, Timestamp val) {
        mask[i] = val > timestamp;
    });
    bitset_chunk |= mask;
}

std::string
ChunkedSegmentSealedImpl::resolve_field_data_warmup_policy(
    FieldId field_id,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    const std::string& explicit_warmup_policy) const {
    // System fields do not carry user-field warmup settings and are not
    // represented in user-field bitsets. They should not affect group warmup
    // aggregation.
    if (SystemProperty::Instance().IsSystem(field_id)) {
        return explicit_warmup_policy;
    }

    // "Has index" here means an index is usable now:
    // - SegmentLoadInfo covers DataCoord-built index files loaded with segment
    //   metadata.
    // - published_state_.binlog_index_bitset covers interim indexes already
    //   generated from raw binlog data.
    // Do not predict whether an interim index may be generated later. Before an
    // index exists, raw vector data is the search-critical representation and
    // follows vector-index warmup.
    bool has_index = segment_load_info.HasIndexInfo(field_id);
    if (!has_index) {
        auto snapshot = CapturePublishedState();
        has_index = snapshot != nullptr &&
                    get_bit_if_present(snapshot->binlog_index_bitset, field_id);
    }

    const auto& field_meta = schema_snapshot->operator[](field_id);
    auto is_vector = IsVectorDataType(field_meta.get_data_type());

    // explicit_warmup_policy is the field-data override carried by the current
    // load request. It is normally authoritative. The only exception is vector
    // raw data without a usable index: QueryCoord may have propagated
    // warmup.vectorField=disable into the field TypeParams, but in this state
    // the raw vector column is effectively the index/search path and must use
    // vector-index warmup instead.
    if (!explicit_warmup_policy.empty() && (!is_vector || has_index)) {
        return explicit_warmup_policy;
    }

    if (is_vector && !has_index) {
        auto [has_index_warmup, index_warmup_policy] =
            schema_snapshot->CollectionWarmupPolicy(/*is_vector=*/true,
                                                    /*is_index=*/true);
        if (has_index_warmup) {
            return index_warmup_policy;
        }

        switch (milvus::cachinglayer::Manager::GetInstance()
                    .getVectorIndexCacheWarmupPolicy()) {
            case CacheWarmupPolicy::CacheWarmupPolicy_Sync:
                return "sync";
            case CacheWarmupPolicy::CacheWarmupPolicy_Async:
                return "async";
            case CacheWarmupPolicy::CacheWarmupPolicy_Disable:
                return "disable";
            default:
                return "";
        }
    }

    auto [has_field_warmup, field_warmup_policy] =
        schema_snapshot->WarmupPolicy(field_id, is_vector, /*is_index=*/false);
    return has_field_warmup ? field_warmup_policy : "";
}

std::string
ChunkedSegmentSealedImpl::resolve_field_data_group_warmup_policy(
    const std::unordered_map<FieldId, FieldMeta>& field_metas,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    const std::string& explicit_warmup_policy) const {
    std::string aggregated_warmup_policy;
    for (const auto& field_meta_pair : field_metas) {
        AccumulateWarmupPolicyForGroup(
            resolve_field_data_warmup_policy(field_meta_pair.first,
                                             segment_load_info,
                                             schema_snapshot,
                                             explicit_warmup_policy),
            aggregated_warmup_policy);
    }
    return aggregated_warmup_policy;
}

bool
ChunkedSegmentSealedImpl::generate_interim_index(
    const FieldId field_id,
    int64_t num_rows,
    const std::shared_ptr<ChunkedColumnInterface>& loaded_column,
    milvus::OpContext* op_ctx,
    StagedStateCommitter* committer) {
    if (col_index_meta_ == nullptr || !col_index_meta_->HasField(field_id)) {
        return false;
    }
    auto snapshot = CapturePublishedState();
    auto schema_snapshot = snapshot->schema;
    auto& field_meta = schema_snapshot->operator[](field_id);
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
        if (committer != nullptr) {
            if (committer->IsVectorIndexReady(field_id)) {
                return false;
            }
        } else {
            if (RuntimeVectorIndexReady(snapshot->runtime.get(), field_id)) {
                return false;
            }
        }
        return true;
    };
    if (!enable_binlog_index()) {
        return false;
    }
    try {
        auto vec_data =
            loaded_column != nullptr ? loaded_column : get_column(field_id);
        AssertInfo(
            vec_data != nullptr, "vector field {} not loaded", field_id.get());
        int64_t row_count = num_rows;
        if (field_meta.is_nullable()) {
            vec_data->BuildValidRowIds(op_ctx);
            const auto& offset_mapping = vec_data->GetOffsetMapping();
            if (!offset_mapping.IsEnabled()) {
                return false;
            }
            row_count = offset_mapping.GetValidCount();
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
        auto dim = is_sparse ? std::numeric_limits<uint32_t>::max()
                             : field_meta.get_dim();
        auto interim_index_type = field_binlog_config->GetIndexType();
        auto build_config =
            field_binlog_config->GetBuildBaseParams(field_meta.get_data_type());
        build_config[knowhere::meta::DIM] = std::to_string(dim);
        build_config[knowhere::meta::NUM_BUILD_THREAD] = std::to_string(1);
        auto index_metric = field_binlog_config->GetMetricType();

        if (enable_binlog_index()) {
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
            auto index_version =
                knowhere::Version::GetCurrentVersion().VersionNumber();
            bool has_raw_data = false;
            if (is_sparse ||
                field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                has_raw_data = knowhere::IndexStaticFaced<float>::HasRawData(
                    interim_index_type, index_version, build_config);
            } else if (field_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
                has_raw_data = knowhere::IndexStaticFaced<float16>::HasRawData(
                    interim_index_type, index_version, build_config);
            } else if (field_meta.get_data_type() ==
                       DataType::VECTOR_BFLOAT16) {
                has_raw_data = knowhere::IndexStaticFaced<bfloat16>::HasRawData(
                    interim_index_type, index_version, build_config);
            }

            if (committer != nullptr) {
                committer->Commit(
                    [&,
                     interim_index_cache_slot =
                         std::move(interim_index_cache_slot),
                     field_binlog_config = std::move(field_binlog_config),
                     has_raw_data](
                        RuntimeResourceState&,
                        PublishedSegmentState& staged_state) mutable {
                        committer->StageInterimVectorIndexMutationLocked(
                            field_id,
                            index_metric,
                            std::move(interim_index_cache_slot),
                            std::move(field_binlog_config));
                        clear_bit_if_present(
                            staged_state.published_index_ready_bitset,
                            field_id);
                        set_bit(
                            staged_state.published_binlog_index_ready_bitset,
                            field_id,
                            true);
                        SetPublishedIndexRawDataInState(
                            staged_state, field_id, has_raw_data);
                    });
            } else {
                auto next_runtime = CloneMutableRuntimeResourceState();
                next_runtime->vector_indexings[field_id] =
                    BuildVectorIndexEntry(index_metric,
                                          std::move(interim_index_cache_slot));
                next_runtime->vec_binlog_config[field_id] =
                    std::shared_ptr<const VecIndexConfig>(
                        std::move(field_binlog_config));
                PublishBinlogIndexReadyLocked(
                    field_id,
                    has_raw_data,
                    ToConstRuntimeState(std::move(next_runtime)));
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
ChunkedSegmentSealedImpl::LazyCheckSchema(SchemaPtr sch,
                                          milvus::OpContext* op_ctx) {
    if (!sch) {
        return;
    }

    auto current_schema = CaptureSchemaSnapshot();
    auto current_schema_version = current_schema->get_schema_version();

    if (sch->get_schema_version() <= current_schema_version) {
        return;
    }

    if (op_ctx != nullptr &&
        op_ctx->cancellation_token.isCancellationRequested()) {
        ThrowInfo(ErrorCode::FollyCancel,
                  "lazy schema reopen cancelled for segment {}",
                  id_);
    }

    std::unique_lock<std::mutex> reopen_guard(reopen_mutex_, std::try_to_lock);
    if (!reopen_guard.owns_lock()) {
        ThrowInfo(ErrorCode::FollyOtherException,
                  "segment read gate busy for segment {} while another "
                  "schema reopen is in progress",
                  id_);
    }

    current_schema = CaptureSchemaSnapshot();
    current_schema_version = current_schema->get_schema_version();
    if (sch->get_schema_version() <= current_schema_version) {
        return;
    }

    // Avoid preparing a new snapshot while an old SearchResult is known to
    // hold a lease. This is only a preflight optimization; the final
    // fail-fast publish check is the linearization point.
    if (!operation_gate_.CanAcquirePublishImmediately()) {
        ThrowInfo(ErrorCode::FollyOtherException,
                  "segment read gate busy for segment {} during lazy schema "
                  "reopen",
                  id_);
    }

    LOG_INFO(
        "lazy check schema segment {} found newer schema version, current "
        "schema version {}, new schema version {}",
        id_,
        current_schema_version,
        sch->get_schema_version());
    ReopenSchemaLocked(op_ctx, std::move(sch), PublishMode::FailFast);
}

void
ChunkedSegmentSealedImpl::load_field_data_common(
    FieldId field_id,
    const std::shared_ptr<ChunkedColumnInterface>& column,
    size_t num_rows,
    DataType data_type,
    bool enable_mmap,
    bool is_proxy_column,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    RuntimeResourceState* runtime,
    std::optional<ParquetStatistics> statistics,
    milvus::OpContext* op_ctx,
    bool is_replace,
    StagedStateCommitter* committer) {
    std::shared_ptr<const PublishedSegmentState> snapshot;
    auto capture_snapshot =
        [&]() -> const std::shared_ptr<const PublishedSegmentState>& {
        if (snapshot == nullptr) {
            snapshot = CapturePublishedState();
        }
        return snapshot;
    };

    std::shared_ptr<CacheSlot<storagev2translator::PkIndexCell>> pk_index_slot;
    const bool is_primary_field =
        schema_snapshot->get_primary_field_id().value_or(FieldId(-1)) ==
        field_id;
    if (is_primary_field) {
        pk_index_slot =
            BuildPkIndexSlot(column,
                             data_type,
                             segment_load_info.GetStorageVersion() < STORAGE_V2,
                             op_ctx);
    }

    generate_interim_index(field_id, num_rows, column, op_ctx, committer);

    if (!SystemProperty::Instance().IsSystem(field_id) &&
        data_type == DataType::GEOMETRY &&
        segcore_config_.get_enable_geometry_cache()) {
        LoadGeometryCache(field_id, column);
    }

    auto& field_meta = schema_snapshot->operator[](field_id);
    auto prepare_array_offsets = [&](RuntimeResourceState& target_runtime) {
        if (auto parsed_struct_name = GetStructNameForArrayField(field_meta);
            parsed_struct_name.has_value()) {
            auto& struct_name = *parsed_struct_name;
            auto it = target_runtime.struct_to_array_offsets.find(struct_name);
            if (it != target_runtime.struct_to_array_offsets.end()) {
                target_runtime.array_offsets_map[field_id] = it->second;
                return;
            }

            auto new_offsets = ArrayOffsetsSealed::BuildFromColumn(
                *column, field_meta, num_rows);
            target_runtime.struct_to_array_offsets[struct_name] = new_offsets;
            target_runtime.array_offsets_map[field_id] = new_offsets;
        }
    };

    auto apply_loaded_column =
        [&](RuntimeResourceState& target_runtime,
            const std::shared_ptr<ChunkedColumnInterface>& old_column,
            const PublishedSegmentState& state_snapshot) {
            prepare_array_offsets(target_runtime);

            if (IsVariableDataType(data_type)) {
                if (enable_mmap) {
                    target_runtime.variable_fields_avg_size.erase(field_id);
                } else {
                    auto& field_info =
                        target_runtime.variable_fields_avg_size[field_id];
                    auto total_size = field_info.first * field_info.second +
                                      column->DataByteSize();
                    field_info.first += num_rows;
                    field_info.second = total_size / field_info.first;
                }
            }

            if (!IsVariableDataType(data_type) || IsStringDataType(data_type)) {
                if (target_runtime.skip_index == nullptr) {
                    target_runtime.skip_index = std::make_shared<SkipIndex>();
                }
                if (statistics) {
                    target_runtime.skip_index->LoadSkipFromStatistics(
                        id_, field_id, data_type, statistics.value());
                } else if (!is_proxy_column) {
                    target_runtime.skip_index->LoadSkip(
                        id_, field_id, data_type, column);
                }
            }

            if (is_primary_field) {
                target_runtime.pk_index_slot = pk_index_slot;
                target_runtime.virtual_pk2offset.reset();
            }

            if (is_replace) {
                if (old_column && !enable_mmap) {
                    if (!is_proxy_column ||
                        (is_proxy_column &&
                         field_id.get() != DEFAULT_SHORT_COLUMN_GROUP_ID)) {
                        stats_.mem_size -= old_column->DataByteSize();
                    }
                }
                target_runtime.fields.insert_or_assign(field_id, column);
                LOG_INFO("Replacing field {} data in segment {}",
                         field_id.get(),
                         id_);
            } else {
                AssertInfo(SystemProperty::Instance().IsSystem(field_id) ||
                               !get_bit(state_snapshot.field_data_ready_bitset,
                                        field_id),
                           "non system field {} data already loaded",
                           field_id.get());
                AssertInfo(target_runtime.fields.find(field_id) ==
                               target_runtime.fields.end(),
                           "field {} column already exists",
                           field_id.get());
                target_runtime.fields.emplace(field_id, column);
            }

            if (enable_mmap) {
                target_runtime.mmap_field_ids.insert(field_id);
            } else {
                target_runtime.mmap_field_ids.erase(field_id);
            }

            if (!SystemProperty::Instance().IsSystem(field_id)) {
                if (!enable_mmap) {
                    if (!is_proxy_column ||
                        (is_proxy_column &&
                         field_id.get() != DEFAULT_SHORT_COLUMN_GROUP_ID)) {
                        stats_.mem_size += column->DataByteSize();
                    }
                }
                if (!is_replace) {
                    AssertInfo(!get_bit(state_snapshot.field_data_ready_bitset,
                                        field_id),
                               "field {} data already loaded",
                               field_id.get());
                }
                update_row_count(target_runtime, num_rows);
            }
        };

    if (committer != nullptr) {
        committer->Commit([&](RuntimeResourceState& target_runtime,
                              PublishedSegmentState& staged_state) {
            std::shared_ptr<ChunkedColumnInterface> old_column;
            auto it = target_runtime.fields.find(field_id);
            if (it != target_runtime.fields.end()) {
                old_column = it->second;
            }
            if (old_column == nullptr) {
                old_column = get_column(capture_snapshot()->runtime, field_id);
            }

            std::unique_lock lck(mutex_);
            apply_loaded_column(target_runtime, old_column, staged_state);
        });
        return;
    }

    if (runtime != nullptr) {
        std::shared_ptr<ChunkedColumnInterface> old_column;
        auto it = runtime->fields.find(field_id);
        if (it != runtime->fields.end()) {
            old_column = it->second;
        }
        if (old_column == nullptr) {
            old_column = get_column(capture_snapshot()->runtime, field_id);
        }

        std::unique_lock lck(mutex_);
        apply_loaded_column(*runtime, old_column, *capture_snapshot());
        return;
    }

    std::unique_lock lck(mutex_);
    auto current = capture_snapshot();
    auto next_runtime = CloneRuntimeResourceState(current->runtime);
    auto old_column = get_column(current->runtime, field_id);

    apply_loaded_column(*next_runtime, old_column, *current);

    auto published_runtime = ToConstRuntimeState(std::move(next_runtime));
    lck.unlock();
    if (SystemProperty::Instance().IsSystem(field_id)) {
        PublishRuntimeStateLocked(published_runtime);
    } else {
        PublishFieldDataReadyLocked(field_id, published_runtime);
    }
}

TimestampIndex
ChunkedSegmentSealedImpl::build_timestamp_index(const Timestamp* data,
                                                size_t num_rows) {
    TimestampIndex index;
    auto min_slice_length = num_rows < 4096 ? 1 : 4096;
    auto meta = GenerateFakeSlices(data, num_rows, min_slice_length);
    index.set_length_meta(std::move(meta));
    index.build_with(data, num_rows);
    return index;
}

void
ChunkedSegmentSealedImpl::PrepareSchemaForReopen(const SchemaPtr& sch) {
    if (!sch) {
        return;
    }

    auto current_schema = CapturePublishedState()->schema;
    if (sch->get_schema_version() <= current_schema->get_schema_version()) {
        return;
    }

    ResizeStateBitsetsLocked(sch);
}

void
ChunkedSegmentSealedImpl::ApplySchemaForReopen(SchemaPtr sch) {
    PrepareSchemaForReopen(sch);
    PublishReopenState(sch, nullptr);
}

void
ChunkedSegmentSealedImpl::PrepareLoadDiffForReopen(
    milvus::OpContext* op_ctx,
    SegmentLoadInfo& segment_load_info,
    LoadDiff& diff,
    const SchemaPtr& schema_snapshot,
    StagedStateCommitter& committer) {
    milvus::tracer::TraceContext trace_ctx;

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.indexes_to_load.empty()) {
        LoadBatchIndexes(trace_ctx,
                         diff.indexes_to_load,
                         schema_snapshot,
                         op_ctx,
                         false,
                         committer);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.indexes_to_replace.empty()) {
        LoadBatchIndexes(trace_ctx,
                         diff.indexes_to_replace,
                         schema_snapshot,
                         op_ctx,
                         true,
                         committer);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.fields_to_reload.empty()) {
        ReloadColumns(diff.fields_to_reload, op_ctx);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (diff.load_external_manifest) {
        LoadColumnGroups(segment_load_info, schema_snapshot, op_ctx, committer);
    } else {
        bool has_cg_changes = !diff.column_groups_to_load.empty() ||
                              !diff.column_groups_to_replace.empty() ||
                              !diff.column_groups_to_lazyload.empty() ||
                              !diff.column_groups_to_lazyreplace.empty();
        if (has_cg_changes) {
            auto properties =
                milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                    .GetProperties();
            auto column_groups = segment_load_info.GetColumnGroups();
            auto arrow_schema = schema_snapshot->ConvertToLoonArrowSchema(
                /*text_lob_as_binary=*/true);
            auto needed_columns = std::make_shared<std::vector<std::string>>();
            for (const auto& field_id : schema_snapshot->get_field_ids()) {
                needed_columns->push_back(std::to_string(field_id.get()));
            }
            auto reader = std::shared_ptr<milvus_storage::api::Reader>(
                milvus_storage::api::Reader::create(
                    column_groups, arrow_schema, needed_columns, *properties)
                    .release());
            committer.Commit(
                [reader = std::move(reader)](RuntimeResourceState& runtime,
                                             PublishedSegmentState&) mutable {
                    runtime.reader = std::move(reader);
                });
            if (!diff.column_groups_to_load.empty()) {
                LoadColumnGroups(column_groups,
                                 properties,
                                 diff.column_groups_to_load,
                                 segment_load_info,
                                 schema_snapshot,
                                 true,
                                 op_ctx,
                                 false,
                                 committer);
            }
            if (!diff.column_groups_to_lazyload.empty()) {
                LoadColumnGroups(column_groups,
                                 properties,
                                 diff.column_groups_to_lazyload,
                                 segment_load_info,
                                 schema_snapshot,
                                 false,
                                 op_ctx,
                                 false,
                                 committer);
            }
            if (!diff.column_groups_to_replace.empty()) {
                LoadColumnGroups(column_groups,
                                 properties,
                                 diff.column_groups_to_replace,
                                 segment_load_info,
                                 schema_snapshot,
                                 true,
                                 op_ctx,
                                 true,
                                 committer);
            }
            if (!diff.column_groups_to_lazyreplace.empty()) {
                LoadColumnGroups(column_groups,
                                 properties,
                                 diff.column_groups_to_lazyreplace,
                                 segment_load_info,
                                 schema_snapshot,
                                 false,
                                 op_ctx,
                                 true,
                                 committer);
            }
        }
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (segment_load_info.HasManifestPath()) {
        InitTextLobPaths(
            segment_load_info.GetManifestPath(), schema_snapshot, committer);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.binlogs_to_load.empty()) {
        LoadBatchFieldData(trace_ctx,
                           diff.binlogs_to_load,
                           segment_load_info,
                           schema_snapshot,
                           op_ctx,
                           false,
                           &committer);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.binlogs_to_replace.empty()) {
        LoadBatchFieldData(trace_ctx,
                           diff.binlogs_to_replace,
                           segment_load_info,
                           schema_snapshot,
                           op_ctx,
                           true,
                           &committer);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.text_indexes_to_load.empty()) {
        LoadBatchTextIndexes(op_ctx,
                             diff.text_indexes_to_load,
                             schema_snapshot,
                             segment_load_info,
                             committer);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.json_stats_to_load.empty()) {
        LoadBatchJsonKeyIndexes(
            op_ctx, diff.json_stats_to_load, schema_snapshot, committer);
    }
    if (!diff.json_stats_to_replace.empty()) {
        LoadBatchJsonKeyIndexes(
            op_ctx, diff.json_stats_to_replace, schema_snapshot, committer);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.fields_to_fill_default.empty()) {
        FillDefaultValueFields(diff.fields_to_fill_default,
                               segment_load_info,
                               schema_snapshot,
                               committer);
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.text_indexes_to_create.empty()) {
        for (const auto& field_id : diff.text_indexes_to_create) {
            CreateTextIndexWithSchema(
                field_id, schema_snapshot, op_ctx, committer);
            RecordTextIndexCreated(segment_load_info, field_id);
            committer.Commit([&](RuntimeResourceState&,
                                 PublishedSegmentState& staged_state) {
                staged_state.load_info =
                    std::make_shared<const SegmentLoadInfo>(segment_load_info);
            });
        }
    }
}

void
ChunkedSegmentSealedImpl::FinalizeLoadDiffForReopen(
    milvus::OpContext* op_ctx,
    SegmentLoadInfo& segment_load_info,
    LoadDiff& diff,
    const SchemaPtr& schema_snapshot,
    StagedStateCommitter& committer) {
    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.indexes_to_drop.empty()) {
        for (auto field_id : diff.indexes_to_drop) {
            if (diff.indexes_to_replace.count(field_id) > 0 ||
                diff.indexes_to_load.count(field_id) > 0) {
                continue;
            }
            committer.Commit([&](RuntimeResourceState& runtime,
                                 PublishedSegmentState& staged_state) {
                DropIndex(field_id, schema_snapshot, &runtime);
                committer.StageVectorIndexDropLocked(field_id);
                DropIndexFromState(staged_state, field_id);
            });
        }
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    for (const auto& [field_id, nested_paths] : diff.json_indexes_to_drop) {
        for (const auto& nested_path : nested_paths) {
            committer.Commit([&, field_id = field_id, nested_path](
                                 RuntimeResourceState& runtime,
                                 PublishedSegmentState& staged_state) {
                for (auto& retired :
                     EraseJsonIndexesAtPath(runtime, field_id, nested_path)) {
                    committer.RetireCacheIndexingLocked(std::move(retired));
                }
                SyncJsonNgramIndexState(staged_state, runtime, field_id);
            });
        }
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.json_stats_to_drop.empty()) {
        for (auto field_id : diff.json_stats_to_drop) {
            if (diff.json_stats_to_load.count(field_id) > 0 ||
                diff.json_stats_to_replace.count(field_id) > 0) {
                LOG_INFO(
                    "skip drop json key stats because replacement is loaded, "
                    "segment:{}, field:{}",
                    id_,
                    field_id.get());
                continue;
            }
            LOG_INFO("drop json key stats, segment:{}, field:{}",
                     id_,
                     field_id.get());
            committer.Commit([field_id](RuntimeResourceState& runtime,
                                        PublishedSegmentState&) {
                runtime.json_stats.erase(field_id);
            });
        }
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.fields_to_fill_default.empty()) {
        for (auto field_id : diff.fields_to_fill_default) {
            segment_load_info.SetFieldFilledWithDefault(field_id);
        }
    }

    CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::ApplyLoadDiff()");
    if (!diff.field_data_to_drop.empty()) {
        for (auto field_id : diff.field_data_to_drop) {
            committer.Commit([&](RuntimeResourceState& runtime,
                                 PublishedSegmentState& staged_state) {
                bool drop_binlog_index = get_bit_if_present(
                    staged_state.binlog_index_bitset, field_id);
                DropFieldData(field_id, schema_snapshot, &runtime);
                if (drop_binlog_index) {
                    committer.StageVectorIndexDropLocked(field_id);
                }
                DropFieldFromState(staged_state, field_id);
            });
        }
    }
    committer.Commit(
        [&](RuntimeResourceState& runtime, PublishedSegmentState&) {
            for (auto it = runtime.text_indexes.begin();
                 it != runtime.text_indexes.end();) {
                if (!field_exists_in_schema(schema_snapshot, it->first)) {
                    it = runtime.text_indexes.erase(it);
                } else {
                    ++it;
                }
            }
        });
}

void
ChunkedSegmentSealedImpl::Reopen(SchemaPtr sch) {
    milvus::OpContext op_ctx;
    Reopen(&op_ctx, std::move(sch));
}

void
ChunkedSegmentSealedImpl::Reopen(milvus::OpContext* op_ctx, SchemaPtr sch) {
    if (!sch) {
        return;
    }

    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    ReopenSchemaLocked(op_ctx, std::move(sch), PublishMode::Drain);
}

void
ChunkedSegmentSealedImpl::ReopenSchemaLocked(milvus::OpContext* op_ctx,
                                             SchemaPtr sch,
                                             PublishMode publish_mode) {
    if (!sch) {
        return;
    }

    auto current = CapturePublishedState();
    auto current_schema = current->schema;
    if (sch->get_schema_version() <= current_schema->get_schema_version()) {
        return;
    }

    SegmentLoadInfo current_mutable(*current->load_info);
    SegmentLoadInfo new_local(*current->load_info);
    new_local.ReplaceSchemaForReopen(sch);

    auto diff = current_mutable.ComputeDiff(new_local);
    new_local.SetFieldsFilledWithDefault(
        current_mutable.GetDefaultFilledFieldsForNewInfo(new_local));
    // Populate manifest cache before publishing; readers do not take
    // reopen_mutex_.
    if (new_local.HasManifestPath() && sch->is_external_collection()) {
        CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::Reopen()");
        (void)new_local.GetColumnGroups();
    }
    LOG_INFO(
        "Schema-only reopen segment {} with diff {}", id_, diff.ToString());

    auto next_runtime = CloneMutableRuntimeResourceState();
    auto staged = ClonePublishedState(current);
    staged->schema = sch;
    staged->load_info = std::make_shared<const SegmentLoadInfo>(new_local);
    staged->runtime = ToConstRuntimeState(next_runtime);
    staged->commit_ts = current->commit_ts;
    NormalizePublishedState(*staged);
    StagedStateCommitter committer(*this, next_runtime.get(), staged.get());
    PrepareLoadDiffForReopen(op_ctx, new_local, diff, sch, committer);
    FinalizeLoadDiffForReopen(op_ctx, new_local, diff, sch, committer);
    new_local.CompactRuntimeInfoForManifest();
    auto published = std::make_shared<const SegmentLoadInfo>(new_local);
    auto delta = MakeStateDelta(sch,
                                published,
                                ToConstRuntimeState(std::move(next_runtime)),
                                current->commit_ts);
    delta.published_index_ready_bitset =
        staged->published_index_ready_bitset.clone();
    delta.published_binlog_index_ready_bitset =
        staged->published_binlog_index_ready_bitset.clone();
    delta.published_index_has_raw_data = staged->published_index_has_raw_data;
    committer.Publish(current, delta, op_ctx, publish_mode);

    LOG_INFO("Schema-only reopen segment {} done", id_);
}

void
ChunkedSegmentSealedImpl::Reopen(
    milvus::OpContext* op_ctx,
    const milvus::proto::segcore::SegmentLoadInfo& new_load_info) {
    Reopen(op_ctx, new_load_info, nullptr);
}

void
ChunkedSegmentSealedImpl::Reopen(
    milvus::OpContext* op_ctx,
    const milvus::proto::segcore::SegmentLoadInfo& new_load_info,
    SchemaPtr new_schema) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);

    auto current = CapturePublishedState();
    auto current_schema = current->schema;
    if (new_schema && new_schema->get_schema_version() <
                          current_schema->get_schema_version()) {
        ThrowInfo(kCollectionSchemaVersionNotReady,
                  "stale reopen segment {}, current schema version {}, "
                  "incoming schema version {}",
                  id_,
                  current_schema->get_schema_version(),
                  new_schema->get_schema_version());
    }

    auto target_schema = new_schema ? std::move(new_schema) : current_schema;

    SegmentLoadInfo current_mutable(*current->load_info);
    SegmentLoadInfo new_local(new_load_info, target_schema);
    new_local.InheritCachedColumnGroupsFrom(*current->load_info);
    for (auto fid : current->load_info->GetCreatedTextIndexes()) {
        if (field_exists_in_schema(target_schema, fid)) {
            new_local.SetTextIndexCreated(fid);
        }
    }

    auto diff = current_mutable.ComputeDiff(new_local);
    new_local.SetFieldsFilledWithDefault(
        current_mutable.GetDefaultFilledFieldsForNewInfo(new_local));
    // Populate manifest cache before publishing; readers do not take
    // reopen_mutex_.
    if (new_local.HasManifestPath() &&
        target_schema->is_external_collection()) {
        CheckCancellation(op_ctx, id_, "ChunkedSegmentSealedImpl::Reopen()");
        (void)new_local.GetColumnGroups();
    }
    LOG_INFO("Reopen segment {} with diff {}", id_, diff.ToString());

    auto next_runtime = CloneMutableRuntimeResourceState();
    auto staged = ClonePublishedState(current);
    staged->schema = target_schema;
    staged->load_info = std::make_shared<const SegmentLoadInfo>(new_local);
    staged->runtime = ToConstRuntimeState(next_runtime);
    staged->commit_ts = current->commit_ts;
    NormalizePublishedState(*staged);
    StagedStateCommitter committer(*this, next_runtime.get(), staged.get());
    PrepareLoadDiffForReopen(op_ctx, new_local, diff, target_schema, committer);
    FinalizeLoadDiffForReopen(
        op_ctx, new_local, diff, target_schema, committer);
    new_local.CompactRuntimeInfoForManifest();
    auto published = std::make_shared<const SegmentLoadInfo>(new_local);
    auto delta = MakeStateDelta(target_schema,
                                published,
                                ToConstRuntimeState(std::move(next_runtime)),
                                current->commit_ts);
    delta.published_index_ready_bitset =
        staged->published_index_ready_bitset.clone();
    delta.published_binlog_index_ready_bitset =
        staged->published_binlog_index_ready_bitset.clone();
    delta.published_index_has_raw_data = staged->published_index_has_raw_data;
    committer.Publish(current, delta, op_ctx);

    LOG_INFO("Reopen segment {} done", id_);
}

void
ChunkedSegmentSealedImpl::ApplyLoadDiff(milvus::OpContext* op_ctx,
                                        SegmentLoadInfo& segment_load_info,
                                        LoadDiff& diff,
                                        const SchemaPtr& schema_snapshot) {
    auto current = CapturePublishedState();
    auto next_runtime = CloneMutableRuntimeResourceState();
    auto staged = ClonePublishedState(current);
    staged->schema = schema_snapshot;
    staged->load_info =
        std::make_shared<const SegmentLoadInfo>(segment_load_info);
    staged->runtime = ToConstRuntimeState(next_runtime);
    staged->commit_ts = current->commit_ts;
    NormalizePublishedState(*staged);
    StagedStateCommitter committer(*this, next_runtime.get(), staged.get());
    PrepareLoadDiffForReopen(
        op_ctx, segment_load_info, diff, schema_snapshot, committer);
    FinalizeLoadDiffForReopen(
        op_ctx, segment_load_info, diff, schema_snapshot, committer);
    segment_load_info.CompactRuntimeInfoForManifest();
    auto published = std::make_shared<const SegmentLoadInfo>(segment_load_info);
    auto delta = MakeStateDelta(schema_snapshot,
                                published,
                                ToConstRuntimeState(std::move(next_runtime)),
                                current->commit_ts);
    delta.published_index_ready_bitset =
        staged->published_index_ready_bitset.clone();
    delta.published_binlog_index_ready_bitset =
        staged->published_binlog_index_ready_bitset.clone();
    delta.published_index_has_raw_data = staged->published_index_has_raw_data;
    committer.Publish(current, delta, op_ctx);
}

void
ChunkedSegmentSealedImpl::ApplyLoadDiff(milvus::OpContext* op_ctx,
                                        SegmentLoadInfo& segment_load_info,
                                        LoadDiff& diff) {
    ApplyLoadDiff(op_ctx, segment_load_info, diff, CaptureSchemaSnapshot());
}

void
ChunkedSegmentSealedImpl::fill_empty_field(
    const FieldMeta& field_meta,
    const SchemaPtr& schema_snapshot,
    const SegmentLoadInfo& segment_load_info,
    RuntimeResourceState& runtime) {
    auto field_id = field_meta.get_id();
    auto data_type = field_meta.get_data_type();
    LOG_INFO(
        "start fill empty field {} (data type {}) for sealed segment "
        "{}",
        data_type,
        field_id.get(),
        id_);
    auto [field_has_setting, field_mmap_enabled] =
        schema_snapshot->MmapEnabled(field_id);
    auto is_vector = IsVectorDataType(field_meta.get_data_type());
    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    bool global_use_mmap = is_vector ? mmap_config.GetVectorFieldEnableMmap()
                                     : mmap_config.GetScalarFieldEnableMmap();
    bool use_mmap = field_has_setting ? field_mmap_enabled : global_use_mmap;
    auto mmap_dir_path =
        milvus::storage::LocalChunkManagerSingleton::GetInstance()
            .GetChunkManager()
            ->GetRootPath();
    int64_t size = runtime.row_count;
    AssertInfo(size > 0, "Chunked Sealed segment must have more than 0 row");
    auto field_data_info = FieldDataInfo(field_id.get(),
                                         size,
                                         mmap_dir_path,
                                         false,
                                         segment_load_info.GetInsertChannel());

    auto warmup_policy = resolve_field_data_warmup_policy(
        field_id, segment_load_info, schema_snapshot);
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

    runtime.fields.emplace(field_id, column);
    if (use_mmap) {
        runtime.mmap_field_ids.insert(field_id);
    } else {
        runtime.mmap_field_ids.erase(field_id);
    }
    LOG_INFO(
        "fill empty field {} (data type {}) for growing segment {} "
        "done",
        field_meta.get_data_type(),
        field_id.get(),
        id_);
}

void
ChunkedSegmentSealedImpl::EnsureArrayOffsetsForStructField(
    const FieldMeta& field_meta,
    int64_t row_count,
    RuntimeResourceState& runtime) {
    auto struct_name = GetStructNameForArrayField(field_meta);
    if (!struct_name.has_value()) {
        return;
    }

    auto it = runtime.struct_to_array_offsets.find(*struct_name);
    if (it == runtime.struct_to_array_offsets.end()) {
        auto array_offsets = ArrayOffsetsSealed::BuildAllZeros(row_count);
        it =
            runtime.struct_to_array_offsets.emplace(*struct_name, array_offsets)
                .first;
    }

    runtime.array_offsets_map[field_meta.get_id()] = it->second;
}

void
ChunkedSegmentSealedImpl::FillDefaultValueFields(
    const std::vector<FieldId>& field_ids,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    RuntimeResourceState* runtime,
    PublishedSegmentState* staged_state) {
    auto snapshot = CapturePublishedState();
    const auto& visible_state =
        staged_state != nullptr ? *staged_state : *snapshot;
    RuntimeResourceState* target_runtime = runtime;
    std::shared_ptr<RuntimeResourceState> owned_runtime;
    if (target_runtime == nullptr) {
        owned_runtime = CloneMutableRuntimeResourceState();
        target_runtime = owned_runtime.get();
    }

    std::vector<FieldId> filled_fields;
    for (const auto& field_id : field_ids) {
        if (get_bit_if_present(visible_state.field_data_ready_bitset,
                               field_id)) {
            continue;
        }
        if (get_bit_if_present(visible_state.index_ready_bitset, field_id) &&
            HasIndexRawDataFromState(visible_state, field_id)) {
            continue;
        }
        if (schema_snapshot->is_function_output(field_id)) {
            continue;
        }
        const auto& field_meta = schema_snapshot->operator[](field_id);
        fill_empty_field(
            field_meta, schema_snapshot, segment_load_info, *target_runtime);
        EnsureArrayOffsetsForStructField(
            field_meta, target_runtime->row_count, *target_runtime);
        filled_fields.push_back(field_id);
    }

    if (filled_fields.empty()) {
        return;
    }

    if (owned_runtime != nullptr) {
        auto published_runtime = ToConstRuntimeState(std::move(owned_runtime));
        MutatePublishedStateLocked([&](PublishedSegmentState& state) {
            state.runtime = published_runtime;
            for (const auto& field_id : filled_fields) {
                set_bit(state.field_data_ready_bitset, field_id, true);
            }
        });
    }
}

void
ChunkedSegmentSealedImpl::FillDefaultValueFields(
    const std::vector<FieldId>& field_ids) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    auto snapshot = CapturePublishedState();
    FillDefaultValueFields(
        field_ids, *snapshot->load_info, snapshot->schema, nullptr);
}

void
ChunkedSegmentSealedImpl::FillDefaultValueFields(
    const std::vector<FieldId>& field_ids,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    StagedStateCommitter& committer) {
    const auto* staged_state = committer.staged_state();

    std::vector<std::pair<FieldMeta, std::shared_ptr<ChunkedColumnInterface>>>
        fields_to_commit;
    for (const auto& field_id : field_ids) {
        if (get_bit_if_present(staged_state->field_data_ready_bitset,
                               field_id)) {
            continue;
        }
        if (get_bit_if_present(staged_state->index_ready_bitset, field_id) &&
            HasIndexRawDataFromState(*staged_state, field_id)) {
            continue;
        }
        if (schema_snapshot->is_function_output(field_id)) {
            continue;
        }

        const auto& field_meta = schema_snapshot->operator[](field_id);
        auto data_type = field_meta.get_data_type();
        LOG_INFO(
            "start fill empty field {} (data type {}) for sealed segment "
            "{}",
            data_type,
            field_id.get(),
            id_);
        auto [field_has_setting, field_mmap_enabled] =
            schema_snapshot->MmapEnabled(field_id);
        auto is_vector = IsVectorDataType(field_meta.get_data_type());
        auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
        bool global_use_mmap = is_vector
                                   ? mmap_config.GetVectorFieldEnableMmap()
                                   : mmap_config.GetScalarFieldEnableMmap();
        bool use_mmap =
            field_has_setting ? field_mmap_enabled : global_use_mmap;
        auto mmap_dir_path =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager()
                ->GetRootPath();
        int64_t size = committer.runtime()->row_count;
        AssertInfo(size > 0,
                   "Chunked Sealed segment must have more than 0 row");
        auto field_data_info =
            FieldDataInfo(field_id.get(),
                          size,
                          mmap_dir_path,
                          false,
                          segment_load_info.GetInsertChannel());

        auto warmup_policy = resolve_field_data_warmup_policy(
            field_id, segment_load_info, schema_snapshot);
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
        auto column =
            MakeChunkedColumnBase(data_type, std::move(slot), field_meta);
        fields_to_commit.emplace_back(field_meta, std::move(column));
    }

    if (fields_to_commit.empty()) {
        return;
    }

    committer.Commit([&](RuntimeResourceState& runtime,
                         PublishedSegmentState&) {
        for (auto& [field_meta, column] : fields_to_commit) {
            auto field_id = field_meta.get_id();
            runtime.fields.emplace(field_id, std::move(column));
            auto [field_has_setting, field_mmap_enabled] =
                schema_snapshot->MmapEnabled(field_id);
            auto is_vector = IsVectorDataType(field_meta.get_data_type());
            auto& mmap_config =
                storage::MmapManager::GetInstance().GetMmapConfig();
            bool global_use_mmap = is_vector
                                       ? mmap_config.GetVectorFieldEnableMmap()
                                       : mmap_config.GetScalarFieldEnableMmap();
            bool use_mmap =
                field_has_setting ? field_mmap_enabled : global_use_mmap;
            if (use_mmap) {
                runtime.mmap_field_ids.insert(field_id);
            } else {
                runtime.mmap_field_ids.erase(field_id);
            }
            EnsureArrayOffsetsForStructField(
                field_meta, runtime.row_count, runtime);
            LOG_INFO(
                "fill empty field {} (data type {}) for growing segment {} "
                "done",
                field_meta.get_data_type(),
                field_id.get(),
                id_);
        }
    });
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
            "Successfully loaded geometry cache for segment {} field {} "
            "with "
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
ChunkedSegmentSealedImpl::SetCommitTimestamp(uint64_t ts) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    {
        std::unique_lock lck(mutex_);
        commit_ts_ = ts;
    }
    MutatePublishedStateLocked([&](PublishedSegmentState& state) {
        state.commit_ts = static_cast<Timestamp>(ts);
    });
}

uint64_t
ChunkedSegmentSealedImpl::GetCommitTimestamp() const {
    return CapturePublishedState()->commit_ts;
}

std::optional<Timestamp>
ChunkedSegmentSealedImpl::EffectiveCommitTs() const {
    auto commit_ts = CapturePublishedState()->commit_ts;
    return commit_ts != 0 ? std::optional<Timestamp>{commit_ts} : std::nullopt;
}

void
ChunkedSegmentSealedImpl::SetLoadInfo(
    proto::segcore::SegmentLoadInfo load_info) {
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
    auto current = CapturePublishedState();
    auto schema_snapshot = current->schema;
    auto commit_ts =
        static_cast<milvus::Timestamp>(load_info.commit_timestamp());
    {
        std::unique_lock lck(mutex_);
        commit_ts_ = commit_ts;
    }
    // Do not parse manifest here: Load() must be able to observe a
    // pre-cancelled OpContext before any storage/manifest IO happens.
    auto published = std::make_shared<const SegmentLoadInfo>(
        std::move(load_info), schema_snapshot);
    PublishStateOnline(BuildNextPublishedState(
        current,
        MakeStateDelta(
            schema_snapshot, published, static_cast<Timestamp>(commit_ts))));
    LOG_INFO(
        "SetLoadInfo for segment {}, num_rows: {}, index count: {}, "
        "storage_version: {}, use_take_for_output: {}, commit_ts: {}",
        id_,
        published->GetNumOfRows(),
        published->GetIndexInfoCount(),
        published->GetStorageVersion(),
        published->GetUseTakeForOutput(),
        commit_ts);
}

void
ChunkedSegmentSealedImpl::InitTextLobPaths(const std::string& manifest_path,
                                           const SchemaPtr& schema_snapshot,
                                           RuntimeResourceState* runtime) {
    AssertInfo(runtime != nullptr,
               "runtime must not be null when initializing TEXT LOB paths for "
               "segment {}",
               id_);

    std::vector<FieldId> text_field_ids;
    for (auto& [field_id, field_meta] : schema_snapshot->get_fields()) {
        if (field_meta.get_data_type() == DataType::TEXT) {
            text_field_ids.push_back(field_id);
        }
    }

    if (text_field_ids.empty()) {
        return;
    }

    std::string segment_base_path;
    try {
        nlohmann::json j = nlohmann::json::parse(manifest_path);
        segment_base_path = j.at("base_path").get<std::string>();
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Failed to parse manifest path for TEXT columns: {}",
                  e.what());
    }

    // segment_base_path format: {root}/{collectionID}/{partitionID}/{segmentID}
    // lob_base_path format: {root}/{collectionID}/{partitionID}/lobs/{field_id}
    std::filesystem::path segment_fs_path(segment_base_path);
    std::filesystem::path partition_path = segment_fs_path.parent_path();

    for (auto field_id : text_field_ids) {
        std::filesystem::path lob_base_path =
            partition_path / "lobs" / std::to_string(field_id.get());
        runtime->text_lob_paths[field_id] = lob_base_path.string();
        LOG_INFO("Initialized TEXT LOB path for segment {} field {}: {}",
                 id_,
                 field_id.get(),
                 lob_base_path.string());
    }
}

void
ChunkedSegmentSealedImpl::InitTextLobPaths(const std::string& manifest_path,
                                           const SchemaPtr& schema_snapshot,
                                           StagedStateCommitter& committer) {
    std::vector<FieldId> text_field_ids;
    for (auto& [field_id, field_meta] : schema_snapshot->get_fields()) {
        if (field_meta.get_data_type() == DataType::TEXT) {
            text_field_ids.push_back(field_id);
        }
    }

    if (text_field_ids.empty()) {
        return;
    }

    std::string segment_base_path;
    try {
        nlohmann::json j = nlohmann::json::parse(manifest_path);
        segment_base_path = j.at("base_path").get<std::string>();
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Failed to parse manifest path for TEXT columns: {}",
                  e.what());
    }

    std::filesystem::path segment_fs_path(segment_base_path);
    std::filesystem::path partition_path = segment_fs_path.parent_path();
    std::vector<std::pair<FieldId, std::string>> lob_paths;
    lob_paths.reserve(text_field_ids.size());
    for (auto field_id : text_field_ids) {
        std::filesystem::path lob_base_path =
            partition_path / "lobs" / std::to_string(field_id.get());
        lob_paths.emplace_back(field_id, lob_base_path.string());
    }

    committer.Commit([&](RuntimeResourceState& runtime,
                         PublishedSegmentState&) {
        for (const auto& [field_id, lob_path] : lob_paths) {
            runtime.text_lob_paths[field_id] = lob_path;
            LOG_INFO("Initialized TEXT LOB path for segment {} field {}: {}",
                     id_,
                     field_id.get(),
                     lob_path);
        }
    });
}

void
ChunkedSegmentSealedImpl::LoadColumnGroups(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    std::vector<std::pair<int, std::vector<FieldId>>>& cg_field_ids,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    bool eager_load,
    milvus::OpContext* op_ctx,
    bool is_replace,
    StagedStateCommitter& committer) {
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
                                   &segment_load_info,
                                   schema_snapshot,
                                   eager_load,
                                   op_ctx,
                                   is_replace,
                                   &committer]() {
            CheckCancellation(op_ctx,
                              id_,
                              cg_index,
                              "ChunkedSegmentSealedImpl::LoadColumnGroup()");
            LoadColumnGroup(column_groups,
                            properties,
                            cg_index,
                            field_ids,
                            segment_load_info,
                            schema_snapshot,
                            eager_load,
                            op_ctx,
                            is_replace,
                            committer);
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
    auto snapshot = CapturePublishedState();
    LoadColumnGroup(column_groups,
                    properties,
                    index,
                    milvus_field_ids,
                    *snapshot->load_info,
                    snapshot->schema,
                    eager_load,
                    op_ctx,
                    is_replace,
                    nullptr);
}

void
ChunkedSegmentSealedImpl::LoadColumnGroup(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    int64_t index,
    const std::vector<FieldId>& milvus_field_ids,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    bool eager_load,
    milvus::OpContext* op_ctx,
    bool is_replace,
    RuntimeResourceState* runtime) {
    AssertInfo(index < column_groups->size(),
               "load column group index out of range");
    AssertInfo(!milvus_field_ids.empty(),
               "load column group with empty field list");
    auto column_group = column_groups->at(index);

    for (const auto& field_id : milvus_field_ids) {
        AssertInfo(field_exists_in_schema(schema_snapshot, field_id),
                   "field {} not found in schema when loading column group",
                   field_id.get());
    }

    auto field_metas = schema_snapshot->get_field_metas(milvus_field_ids);
    auto aggregated_warmup_policy = resolve_field_data_group_warmup_policy(
        field_metas, segment_load_info, schema_snapshot);

    // assumption: vector field occupies whole column group
    bool is_vector = false;
    bool has_mmap_setting = false;
    bool mmap_enabled = false;
    for (auto& [field_id, field_meta] : field_metas) {
        if (IsVectorDataType(field_meta.get_data_type())) {
            is_vector = true;
        }

        // if field has mmap setting, use it
        // - mmap setting at collection level, then all field are the same
        // - mmap setting at field level, we define that as long as one field shall be mmap, then whole group shall be mmaped
        auto [field_has_setting, field_mmap_enabled] =
            schema_snapshot->MmapEnabled(field_id);
        has_mmap_setting = has_mmap_setting || field_has_setting;
        mmap_enabled = mmap_enabled || field_mmap_enabled;
    }

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    bool global_use_mmap = is_vector ? mmap_config.GetVectorFieldEnableMmap()
                                     : mmap_config.GetScalarFieldEnableMmap();
    auto use_mmap = has_mmap_setting ? mmap_enabled : global_use_mmap;

    // The set of columns this entry projects is exactly the field_ids the
    // diff handed us. For lazy entries, SegmentLoadInfo::ComputeDiffColumnGroups
    // emits one entry per field, so each lazy entry produces a single-column
    // projected ChunkReader — touching one lazy field will not co-load chunks
    // for sibling lazy fields in the same column group.
    auto needed_columns = std::make_shared<std::vector<std::string>>();
    needed_columns->reserve(milvus_field_ids.size());
    for (const auto& fid : milvus_field_ids) {
        needed_columns->push_back(
            schema_snapshot->get_storage_column_name(fid));
    }
    auto reader =
        runtime != nullptr ? runtime->reader : CaptureReaderSnapshot();
    AssertInfo(
        reader != nullptr,
        "reader must exist before loading manifest column group, segment {}",
        get_segment_id());
    auto chunk_reader_result = reader->get_chunk_reader(index, needed_columns);
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
    std::string warmup_policy = aggregated_warmup_policy;

    // Multiple lazy entries can share the same column-group index (one per
    // field), so the translator cache key must be disambiguated by the
    // field-id of this entry. Eager entries are still one-per-cg, so they
    // keep the unsuffixed key.
    std::string cache_key_suffix;
    if (!eager_load) {
        cache_key_suffix = std::to_string(milvus_field_ids.front().get());
    }

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
            milvus_field_ids.size(),
            segment_load_info.GetPriority(),
            eager_load,
            warmup_policy,
            cache_key_suffix,
            segment_load_info.GetEstimatedBytesPerRow(),
            segment_load_info.GetInsertChannel());
    auto chunked_column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    // Create ProxyChunkColumn for each field
    for (const auto& field_id : milvus_field_ids) {
        const auto& field_meta = field_metas.at(field_id);
        auto column = std::make_shared<ProxyChunkColumn>(
            chunked_column_group, field_id, field_meta);
        auto data_type = field_meta.get_data_type();
        load_field_data_common(
            field_id,
            column,
            segment_load_info.GetNumOfRows(),
            data_type,
            use_mmap,
            true,
            segment_load_info,
            schema_snapshot,
            runtime,
            std::
                nullopt,  // manifest cannot provide parquet skip index directly
            op_ctx,
            is_replace);
        if (field_id == TimestampFieldID) {
            int64_t num_rows = segment_load_info.GetNumOfRows();
            if (commit_ts_ != 0) {
                std::vector<Timestamp> ts(num_rows, commit_ts_);
                init_storage_v1_timestamp_index(
                    std::move(ts), num_rows, runtime);
            } else {
                init_storage_v2_timestamp_index(column, num_rows, "", runtime);
            }
            if (runtime == nullptr) {
                PublishSystemFieldStateLocked();
            }
        }
    }
}

void
ChunkedSegmentSealedImpl::LoadColumnGroup(
    const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    int64_t index,
    const std::vector<FieldId>& milvus_field_ids,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    bool eager_load,
    milvus::OpContext* op_ctx,
    bool is_replace,
    StagedStateCommitter& committer) {
    AssertInfo(index < column_groups->size(),
               "load column group index out of range");
    AssertInfo(!milvus_field_ids.empty(),
               "load column group with empty field list");

    for (const auto& field_id : milvus_field_ids) {
        AssertInfo(field_exists_in_schema(schema_snapshot, field_id),
                   "field {} not found in schema when loading column group",
                   field_id.get());
    }

    auto field_metas = schema_snapshot->get_field_metas(milvus_field_ids);
    auto aggregated_warmup_policy = resolve_field_data_group_warmup_policy(
        field_metas, segment_load_info, schema_snapshot);

    bool is_vector = false;
    bool has_mmap_setting = false;
    bool mmap_enabled = false;
    for (auto& [field_id, field_meta] : field_metas) {
        if (IsVectorDataType(field_meta.get_data_type())) {
            is_vector = true;
        }
        auto [field_has_setting, field_mmap_enabled] =
            schema_snapshot->MmapEnabled(field_id);
        has_mmap_setting = has_mmap_setting || field_has_setting;
        mmap_enabled = mmap_enabled || field_mmap_enabled;
    }

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    bool global_use_mmap = is_vector ? mmap_config.GetVectorFieldEnableMmap()
                                     : mmap_config.GetScalarFieldEnableMmap();
    auto use_mmap = has_mmap_setting ? mmap_enabled : global_use_mmap;

    auto needed_columns = std::make_shared<std::vector<std::string>>();
    needed_columns->reserve(milvus_field_ids.size());
    for (const auto& fid : milvus_field_ids) {
        needed_columns->push_back(
            schema_snapshot->get_storage_column_name(fid));
    }

    auto reader = committer.runtime()->reader;
    AssertInfo(
        reader != nullptr,
        "reader must exist before loading manifest column group, segment {}",
        get_segment_id());
    auto chunk_reader_result = reader->get_chunk_reader(index, needed_columns);
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

    std::string warmup_policy = aggregated_warmup_policy;

    std::string cache_key_suffix;
    if (!eager_load) {
        cache_key_suffix = std::to_string(milvus_field_ids.front().get());
    }

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
            milvus_field_ids.size(),
            segment_load_info.GetPriority(),
            eager_load,
            warmup_policy,
            cache_key_suffix,
            segment_load_info.GetEstimatedBytesPerRow(),
            segment_load_info.GetInsertChannel());
    auto chunked_column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    for (const auto& field_id : milvus_field_ids) {
        const auto& field_meta = field_metas.at(field_id);
        auto column = std::make_shared<ProxyChunkColumn>(
            chunked_column_group, field_id, field_meta);
        auto data_type = field_meta.get_data_type();
        load_field_data_common(field_id,
                               column,
                               segment_load_info.GetNumOfRows(),
                               data_type,
                               use_mmap,
                               true,
                               segment_load_info,
                               schema_snapshot,
                               nullptr,
                               std::nullopt,
                               op_ctx,
                               is_replace,
                               &committer);
        if (field_id == TimestampFieldID) {
            int64_t num_rows = segment_load_info.GetNumOfRows();
            if (commit_ts_ != 0) {
                std::vector<Timestamp> ts(num_rows, commit_ts_);
                auto timestamp_index = std::make_shared<const TimestampIndex>(
                    build_timestamp_index(ts.data(), num_rows));
                auto timestamp_data = std::make_shared<TimestampData>();
                timestamp_data->InitFromOwnedData(std::move(ts));
                committer.Commit([this,
                                  timestamp_data = std::move(timestamp_data),
                                  timestamp_index = std::move(timestamp_index),
                                  num_rows](RuntimeResourceState& runtime,
                                            PublishedSegmentState&) mutable {
                    runtime.timestamps = std::move(timestamp_data);
                    runtime.timestamp_index = std::move(timestamp_index);
                    runtime.timestamp_index_slot.reset();
                    stats_.mem_size += sizeof(Timestamp) * num_rows;
                });
            } else {
                std::unique_ptr<
                    Translator<storagev2translator::TimestampIndexCell>>
                    translator = std::make_unique<
                        storagev2translator::TimestampIndexTranslator>(
                        id_, column, num_rows, "");
                auto slot = Manager::GetInstance().CreateCacheSlot(
                    std::move(translator));
                auto cell_holder = SemiInlineGet(slot->PinCells(nullptr, {0}));
                auto* cell = cell_holder->get_cell_of(0);
                AssertInfo(cell != nullptr,
                           "timestamp index cache is corrupted, segment {}",
                           id_);

                auto timestamps = std::make_shared<TimestampData>();
                auto pins = column->GetAllChunks(nullptr);
                timestamps->InitFromPinnedChunks(column, std::move(pins));
                auto timestamp_index = std::make_shared<const TimestampIndex>(
                    cell->timestamp_index());
                committer.Commit(
                    [timestamps = std::move(timestamps),
                     timestamp_index = std::move(timestamp_index),
                     slot = std::move(slot)](RuntimeResourceState& runtime,
                                             PublishedSegmentState&) mutable {
                        runtime.timestamps = std::move(timestamps);
                        runtime.timestamp_index = std::move(timestamp_index);
                        runtime.timestamp_index_slot = std::move(slot);
                    });
            }
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
        text_indexes_to_load,
    const SchemaPtr& schema_snapshot,
    const SegmentLoadInfo& segment_load_info,
    StagedStateCommitter& committer) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> load_index_futures;
    load_index_futures.reserve(text_indexes_to_load.size());

    for (auto& [field_id, load_text_index_info] : text_indexes_to_load) {
        AssertInfo(field_exists_in_schema(schema_snapshot, field_id),
                   "field {} not found in schema when loading text index",
                   field_id.get());
        auto future = pool.Submit([this,
                                   op_ctx,
                                   field_id = field_id,
                                   info = std::move(load_text_index_info),
                                   &segment_load_info,
                                   &committer]() mutable -> void {
            ScopedTextIndexBuildGuard build_guard(*this, field_id);
            build_guard.Register();
            auto text_index =
                BuildTextIndexFromFiles(op_ctx, info, segment_load_info);
            committer.Commit([field_id, text_index = std::move(text_index)](
                                 RuntimeResourceState& runtime,
                                 PublishedSegmentState&) mutable {
                AssertInfo(runtime.text_indexes.find(field_id) ==
                               runtime.text_indexes.end(),
                           "text index for field {} already exists, "
                           "refusing to reload",
                           field_id.get());
                runtime.text_indexes.emplace(field_id, std::move(text_index));
            });
            build_guard.Commit();
        });
        load_index_futures.emplace_back(std::move(future));
    }

    storage::WaitAllFutures(load_index_futures);
}

void
ChunkedSegmentSealedImpl::LoadBatchIndexes(
    milvus::tracer::TraceContext& trace_ctx,
    std::unordered_map<FieldId, std::vector<LoadIndexInfo>>&
        field_id_to_index_info,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    bool is_replace,
    StagedStateCommitter& committer) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> load_index_futures;
    load_index_futures.reserve(field_id_to_index_info.size());

    for (auto& pair : field_id_to_index_info) {
        auto field_id = pair.first;
        AssertInfo(field_exists_in_schema(schema_snapshot, field_id),
                   "field {} not found in schema when loading index",
                   field_id.get());
        auto& index_infos = pair.second;
        for (auto& load_index_info : index_infos) {
            auto* load_index_info_ptr = &load_index_info;
            auto future = pool.Submit([this,
                                       trace_ctx,
                                       field_id,
                                       load_index_info_ptr,
                                       schema_snapshot,
                                       op_ctx,
                                       is_replace,
                                       &committer]() mutable -> void {
                // Early exit if cancelled while queued
                CheckCancellation(op_ctx, id_, field_id.get(), "LoadIndex");

                LOG_INFO("Loading index for segment {} field {} with {} files",
                         id_,
                         field_id.get(),
                         load_index_info_ptr->index_files.size());

                // Download & compose index
                LoadIndexData(trace_ctx, load_index_info_ptr, op_ctx);

                // RuntimeResourceState and staged PublishedSegmentState are
                // shared batch-private objects. Keep expensive IO/build work
                // parallel, then serialize the commit into staged state.
                committer.Commit([&](RuntimeResourceState& runtime,
                                     PublishedSegmentState& staged_state) {
                    LoadIndex(*load_index_info_ptr,
                              schema_snapshot,
                              is_replace,
                              &runtime,
                              &staged_state,
                              &committer);
                });
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
    auto snapshot = CapturePublishedState();
    LoadBatchFieldData(trace_ctx,
                       field_binlog_to_load,
                       *snapshot->load_info,
                       snapshot->schema,
                       op_ctx,
                       is_replace,
                       nullptr);
}

void
ChunkedSegmentSealedImpl::LoadBatchFieldData(
    milvus::tracer::TraceContext& trace_ctx,
    std::vector<std::pair<std::vector<FieldId>, proto::segcore::FieldBinlog>>&
        field_binlog_to_load,
    const SegmentLoadInfo& segment_load_info,
    const SchemaPtr& schema_snapshot,
    milvus::OpContext* op_ctx,
    bool is_replace,
    StagedStateCommitter* committer) {
    LOG_INFO("Loading field binlog for {} fields in segment {}",
             field_binlog_to_load.size(),
             id_);
    const auto* staged_state =
        committer != nullptr ? committer->staged_state() : nullptr;

    // When the flag is on, the loader must keep the column resident alongside
    // the index so bulk_subscript can serve retrieve from field data.
    auto prefer_field_data =
        SegcoreConfig::default_config()
            .get_prefer_field_data_when_index_has_raw_data();

    std::vector<std::pair<FieldId, LoadFieldDataInfo>> field_data_to_load;
    for (auto& [field_ids, field_binlog] : field_binlog_to_load) {
        LoadFieldDataInfo load_field_data_info;
        load_field_data_info.storage_version =
            segment_load_info.GetStorageVersion();
        load_field_data_info.shard = segment_load_info.GetInsertChannel();
        auto fields_to_load = field_ids;
        AssertInfo(!fields_to_load.empty(),
                   "load field data with empty field list");
        for (const auto& field_id : fields_to_load) {
            AssertInfo(field_exists_in_schema(schema_snapshot, field_id),
                       "field {} not found in schema when loading field data",
                       field_id.get());
        }

        auto snapshot = CapturePublishedState();
        const auto& visible_state =
            staged_state != nullptr ? *staged_state : *snapshot;
        bool index_has_raw_data = true;
        bool has_mmap_setting = false;
        bool mmap_enabled = false;
        bool is_vector = false;

        std::string aggregated_warmup_policy;
        for (const auto& child_field_id : fields_to_load) {
            auto& field_meta = schema_snapshot->operator[](child_field_id);
            if (IsVectorDataType(field_meta.get_data_type())) {
                is_vector = true;
            }

            // if field has mmap setting, use it
            // - mmap setting at collection level, then all field are the same
            // - mmap setting at field level, we define that as long as one field shall be mmap, then whole group shall be mmaped
            auto [field_has_setting, field_mmap_enabled] =
                schema_snapshot->MmapEnabled(child_field_id);
            has_mmap_setting = has_mmap_setting || field_has_setting;
            mmap_enabled = mmap_enabled || field_mmap_enabled;

            if (!SystemProperty::Instance().IsSystem(child_field_id) &&
                (get_bit_if_present(visible_state.index_ready_bitset,
                                    child_field_id) ||
                 get_bit_if_present(visible_state.binlog_index_bitset,
                                    child_field_id))) {
                index_has_raw_data =
                    index_has_raw_data &&
                    HasIndexRawDataFromState(visible_state, child_field_id);
            } else {
                index_has_raw_data = false;
            }

            AccumulateWarmupPolicyForGroup(
                resolve_field_data_warmup_policy(
                    child_field_id, segment_load_info, schema_snapshot),
                aggregated_warmup_policy);
        }

        auto group_id = field_binlog.fieldid();
        // Normally we skip loading field data when the index already carries
        // raw data, but prefer_field_data_when_index_has_raw_data opts into
        // keeping both resident so retrieve can read the column directly.
        if (index_has_raw_data && !prefer_field_data) {
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
        field_binlog_info.child_field_ids.resize(field_ids.size());
        std::transform(field_ids.begin(),
                       field_ids.end(),
                       field_binlog_info.child_field_ids.begin(),
                       [](FieldId field_id) { return field_id.get(); });

        auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
        auto global_use_mmap = is_vector
                                   ? mmap_config.GetVectorFieldEnableMmap()
                                   : mmap_config.GetScalarFieldEnableMmap();
        field_binlog_info.enable_mmap =
            has_mmap_setting ? mmap_enabled : global_use_mmap;

        // Determine group warmup policy: use per-field settings if any,
        // otherwise fall back to global warmup policy
        field_binlog_info.warmup_policy = aggregated_warmup_policy;

        // Store in map
        load_field_data_info.field_infos[group_id] = field_binlog_info;

        field_data_to_load.emplace_back(group_id, load_field_data_info);
    }

    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> load_field_futures;
    load_field_futures.reserve(field_data_to_load.size());

    for (const auto& [field_id, load_field_data_info] : field_data_to_load) {
        // Create local copies to capture in lambda (C++17 compatible)
        const auto field_data = load_field_data_info;
        const auto captured_field_id = field_id;
        auto future = pool.Submit([this,
                                   field_data,
                                   captured_field_id,
                                   &segment_load_info,
                                   schema_snapshot,
                                   op_ctx,
                                   is_replace,
                                   committer]() -> void {
            CheckCancellation(op_ctx,
                              id_,
                              captured_field_id.get(),
                              "ChunkedSegmentSealedImpl::LoadFieldData()");
            if (committer != nullptr) {
                LoadFieldData(field_data,
                              segment_load_info,
                              op_ctx,
                              is_replace,
                              schema_snapshot,
                              *committer);
            } else {
                LoadFieldData(field_data,
                              segment_load_info,
                              op_ctx,
                              is_replace,
                              schema_snapshot,
                              nullptr);
            }
        });

        load_field_futures.push_back(std::move(future));
    }

    storage::WaitAllFutures(load_field_futures);
}

void
ChunkedSegmentSealedImpl::Load(milvus::tracer::TraceContext& trace_ctx,
                               milvus::OpContext* op_ctx) {
    // Serialize with Reopen(pb)/SetLoadInfo. Runtime-only updates produced by
    // ApplyLoadDiff are committed through COW helpers after the data is loaded.
    std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);

    auto snapshot = CapturePublishedState();
    auto num_rows = snapshot->load_info->GetNumOfRows();
    LOG_INFO("Loading segment {} with {} rows", id_, num_rows);

    SegmentLoadInfo mutable_copy(snapshot->load_info->GetProto(),
                                 snapshot->schema);
    mutable_copy.SetFieldsFilledWithDefault(
        snapshot->load_info->GetFieldsFilledWithDefault());
    for (auto fid : snapshot->load_info->GetCreatedTextIndexes()) {
        mutable_copy.SetTextIndexCreated(fid);
    }
    auto diff = mutable_copy.GetLoadDiff();
    LOG_WARN("Load segment {} with diff {}", id_, diff.ToString());

    ApplyLoadDiff(op_ctx, mutable_copy, diff);

    LOG_INFO("Successfully loaded segment {} with {} rows", id_, num_rows);
}

void
ChunkedSegmentSealedImpl::FillTargetEntry(const query::Plan* plan,
                                          SearchResult& results,
                                          milvus::OpContext* op_ctx) const {
    auto snapshot = CapturePublishedState();
    AssertInfo(plan, "empty plan");
    auto size = results.distances_.size();
    AssertInfo(results.seg_offsets_.size() == size,
               "Size of result distances is not equal to size of ids");

    segcore::CheckCancellation(op_ctx, get_segment_id(), "FillTargetEntry");

    // Try take() for eligible output fields. Fields not filled by take still
    // go through bulk_subscript below.
    bool used_take = TryTakeForSearch(
        plan, results.seg_offsets_.data(), size, results, op_ctx);

    std::unique_ptr<DataArray> field_data;
    // Per-call OpContext keeps storage_usage scoped to this segment;
    // sharing op_ctx across segments would double-count bytes. See
    // SegmentInternalInterface::FillPrimaryKeys for the same pattern.
    milvus::OpContext local_ctx;
    if (op_ctx != nullptr) {
        local_ctx.cancellation_token = op_ctx->cancellation_token;
        local_ctx.runtime_load_priority = op_ctx->runtime_load_priority;
    }
    for (auto field_id : plan->target_entries_) {
        // Skip fields already filled by take
        if (used_take && results.output_fields_data_.count(field_id) > 0) {
            continue;
        }
        segcore::CheckCancellation(
            op_ctx, get_segment_id(), field_id.get(), "FillTargetEntry");
        auto& field_meta = plan->schema_->operator[](field_id);
        if (plan->schema_->get_dynamic_field_id().has_value() &&
            plan->schema_->get_dynamic_field_id().value() == field_id &&
            !plan->target_dynamic_fields_.empty()) {
            auto& target_dynamic_fields = plan->target_dynamic_fields_;
            field_data = bulk_subscript(&local_ctx,
                                        field_id,
                                        results.seg_offsets_.data(),
                                        size,
                                        target_dynamic_fields);
        } else if (!is_field_exist(field_id)) {
            field_data = bulk_subscript_not_exist_field(field_meta, size);
        } else {
            field_data = bulk_subscript(
                &local_ctx, field_id, results.seg_offsets_.data(), size);
        }
        results.output_fields_data_[field_id] = std::move(field_data);
    }
    results.search_storage_cost_.scanned_remote_bytes +=
        local_ctx.storage_usage.scanned_cold_bytes.load();
    results.search_storage_cost_.scanned_total_bytes +=
        local_ctx.storage_usage.scanned_total_bytes.load();
}

// ---- Shared helpers for TryTakeForRetrieve / TryTakeForSearch ----

static inline void
LogTakeFallback(const char* caller_tag,
                int64_t segment_id,
                int64_t rows,
                size_t unique_rows,
                size_t field_count,
                std::string_view reason) {
    LOG_INFO(
        "[TakeAPI] {} fallback to bulk_subscript for segment {}: "
        "reason={}, rows={}, unique_rows={}, fields={}",
        caller_tag,
        segment_id,
        reason,
        rows,
        unique_rows,
        field_count);
}

static bool
ShouldProjectInternalTakeDynamicField(
    const Schema& schema,
    FieldId field_id,
    const std::vector<std::string>& target_dynamic_fields) {
    if (schema.is_external_collection() || target_dynamic_fields.empty()) {
        return false;
    }
    auto dynamic_field_id = schema.get_dynamic_field_id();
    return dynamic_field_id.has_value() && dynamic_field_id.value() == field_id;
}

static void
CheckVectorOutputCellsLoaded(int64_t segment_id,
                             FieldId field_id,
                             const FieldMeta& field_meta,
                             const ChunkedColumnInterface* column,
                             const int64_t* offsets,
                             int64_t count) {
    if (!SegcoreConfig::default_config().get_reject_remote_vector_output() ||
        !IsVectorDataType(field_meta.get_data_type()) || count == 0) {
        return;
    }
    if (column == nullptr || !column->CellsLoaded(offsets, count)) {
        ThrowInfo(
            RetrieveError,
            "vector field '{}' is not loaded in local cache for "
            "output, segment id={}, please notice that vector field is by "
            "default resident in remote storage starting from milvus 3.0 to "
            "reduce local storage overhead, but performance will be degraded, "
            "you could manually set vector field warmup policy to 'sync' to "
            "load the vector data, please refer to "
            "https://milvus.io/docs/warm-up.md for more details",
            field_meta.get_name().get(),
            segment_id);
    }
}

ChunkedSegmentSealedImpl::TakeContext
ChunkedSegmentSealedImpl::BuildTakeContext(const int64_t* offsets,
                                           int64_t size) {
    struct OffsetEntry {
        int64_t offset;
        int64_t orig_pos;
    };
    std::vector<OffsetEntry> entries;
    entries.reserve(size);
    for (int64_t i = 0; i < size; i++) {
        entries.push_back({offsets[i], i});
    }
    std::sort(entries.begin(),
              entries.end(),
              [](const OffsetEntry& a, const OffsetEntry& b) {
                  return a.offset < b.offset;
              });

    TakeContext ctx;
    ctx.unique_offsets.reserve(size);
    ctx.result_mapping.resize(size);
    for (auto& e : entries) {
        if (ctx.unique_offsets.empty() ||
            ctx.unique_offsets.back() != e.offset) {
            ctx.unique_offsets.push_back(e.offset);
        }
        ctx.result_mapping[e.orig_pos] =
            static_cast<int64_t>(ctx.unique_offsets.size() - 1);
    }
    return ctx;
}

std::unique_ptr<DataArray>
ChunkedSegmentSealedImpl::ArrowToDataArray(
    const std::shared_ptr<arrow::Array>& arr,
    const FieldMeta& field_meta,
    const std::vector<int64_t>& result_mapping,
    int64_t size,
    const std::vector<std::string>* dynamic_field_names,
    const std::string* text_lob_path) {
    auto data_array = std::make_unique<DataArray>();
    data_array->set_type(
        static_cast<proto::schema::DataType>(field_meta.get_data_type()));

    switch (field_meta.get_data_type()) {
        case DataType::BOOL: {
            auto typed = std::static_pointer_cast<arrow::BooleanArray>(arr);
            auto obj = data_array->mutable_scalars()->mutable_bool_data();
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(typed->Value(result_mapping[i]));
            }
            break;
        }
        case DataType::INT8: {
            auto typed = std::static_pointer_cast<arrow::Int8Array>(arr);
            auto obj = data_array->mutable_scalars()->mutable_int_data();
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(
                    static_cast<int32_t>(typed->Value(result_mapping[i])));
            }
            break;
        }
        case DataType::INT16: {
            auto typed = std::static_pointer_cast<arrow::Int16Array>(arr);
            auto obj = data_array->mutable_scalars()->mutable_int_data();
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(
                    static_cast<int32_t>(typed->Value(result_mapping[i])));
            }
            break;
        }
        case DataType::INT32: {
            auto typed = std::static_pointer_cast<arrow::Int32Array>(arr);
            auto obj = data_array->mutable_scalars()->mutable_int_data();
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(typed->Value(result_mapping[i]));
            }
            break;
        }
        case DataType::INT64: {
            auto typed = std::static_pointer_cast<arrow::Int64Array>(arr);
            auto obj = data_array->mutable_scalars()->mutable_long_data();
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(typed->Value(result_mapping[i]));
            }
            break;
        }
        case DataType::FLOAT: {
            auto typed = std::static_pointer_cast<arrow::FloatArray>(arr);
            auto obj = data_array->mutable_scalars()->mutable_float_data();
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(typed->Value(result_mapping[i]));
            }
            break;
        }
        case DataType::DOUBLE: {
            auto typed = std::static_pointer_cast<arrow::DoubleArray>(arr);
            auto obj = data_array->mutable_scalars()->mutable_double_data();
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(typed->Value(result_mapping[i]));
            }
            break;
        }
        case DataType::TEXT: {
            auto obj = data_array->mutable_scalars()->mutable_string_data();
            if (text_lob_path != nullptr) {
                std::vector<milvus_storage::lob_column::EncodedRef>
                    encoded_refs;
                encoded_refs.reserve(size);
                std::vector<std::string> string_refs;
                string_refs.reserve(size);

                if (arr->type()->id() == arrow::Type::STRING) {
                    auto typed =
                        std::static_pointer_cast<arrow::StringArray>(arr);
                    for (int64_t i = 0; i < size; i++) {
                        auto idx = result_mapping[i];
                        if (typed->IsNull(idx)) {
                            encoded_refs.push_back(
                                MakeTextLobEncodedRef(nullptr, 0));
                            continue;
                        }
                        string_refs.emplace_back(typed->GetString(idx));
                        auto& ref = string_refs.back();
                        encoded_refs.push_back(
                            MakeTextLobEncodedRef(ref.data(), ref.size()));
                    }
                } else if (arr->type()->id() == arrow::Type::BINARY) {
                    auto typed =
                        std::static_pointer_cast<arrow::BinaryArray>(arr);
                    for (int64_t i = 0; i < size; i++) {
                        auto idx = result_mapping[i];
                        if (typed->IsNull(idx)) {
                            encoded_refs.push_back(
                                MakeTextLobEncodedRef(nullptr, 0));
                            continue;
                        }
                        auto val = typed->Value(idx);
                        encoded_refs.push_back(MakeTextLobEncodedRef(
                            val.data(), static_cast<size_t>(val.size())));
                    }
                } else {
                    return nullptr;
                }

                auto texts = ReadTextLobBatch(*text_lob_path, encoded_refs);
                for (auto& text : texts) {
                    obj->add_data(std::move(text));
                }
                break;
            }

            auto typed = std::static_pointer_cast<arrow::StringArray>(arr);
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(typed->GetString(result_mapping[i]));
            }
            break;
        }
        case DataType::VARCHAR:
        case DataType::STRING: {
            auto typed = std::static_pointer_cast<arrow::StringArray>(arr);
            auto obj = data_array->mutable_scalars()->mutable_string_data();
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(typed->GetString(result_mapping[i]));
            }
            break;
        }
        case DataType::JSON: {
            // NormalizeExternalArrow already converted String→Binary.
            auto obj = data_array->mutable_scalars()->mutable_json_data();
            auto typed = std::static_pointer_cast<arrow::BinaryArray>(arr);
            for (int64_t i = 0; i < size; i++) {
                auto val = typed->Value(result_mapping[i]);
                if (dynamic_field_names != nullptr &&
                    !dynamic_field_names->empty()) {
                    auto projected = ExtractSubJson(
                        std::string_view(
                            reinterpret_cast<const char*>(val.data()),
                            val.size()),
                        *dynamic_field_names);
                    obj->add_data(std::move(projected));
                } else {
                    obj->add_data(val.data(), val.size());
                }
            }
            break;
        }
        case DataType::GEOMETRY: {
            // NormalizeExternalArrow already converted WKT→WKB if needed.
            auto obj = data_array->mutable_scalars()->mutable_geometry_data();
            auto typed = std::static_pointer_cast<arrow::BinaryArray>(arr);
            for (int64_t i = 0; i < size; i++) {
                auto val = typed->Value(result_mapping[i]);
                obj->add_data(val.data(), val.size());
            }
            break;
        }
        case DataType::TIMESTAMPTZ: {
            // NormalizeExternalArrow already converted Timestamp→Int64.
            auto obj =
                data_array->mutable_scalars()->mutable_timestamptz_data();
            auto typed = std::static_pointer_cast<arrow::Int64Array>(arr);
            for (int64_t i = 0; i < size; i++) {
                obj->add_data(typed->Value(result_mapping[i]));
            }
            break;
        }
        case DataType::ARRAY: {
            // NormalizeExternalArrow already converted List→Binary(protobuf).
            auto obj = data_array->mutable_scalars()->mutable_array_data();
            // Same element_type carry-through as the chunked sealed path
            // above; without it the SDK rejects the response. Fix for #48619.
            obj->set_element_type(static_cast<milvus::proto::schema::DataType>(
                field_meta.get_element_type()));
            auto typed = std::static_pointer_cast<arrow::BinaryArray>(arr);
            for (int64_t i = 0; i < size; i++) {
                auto val = typed->Value(result_mapping[i]);
                auto* sf = obj->add_data();
                sf->ParseFromArray(val.data(), static_cast<int>(val.size()));
            }
            break;
        }
        case DataType::VECTOR_FLOAT: {
            int dim = field_meta.get_dim();
            auto vectors = data_array->mutable_vectors();
            vectors->set_dim(dim);
            auto float_data = vectors->mutable_float_vector();
            int64_t valid_count = size;
            if (field_meta.is_nullable()) {
                valid_count = 0;
                for (int64_t i = 0; i < size; i++) {
                    if (arr->IsValid(result_mapping[i])) {
                        valid_count++;
                    }
                }
            }
            float_data->mutable_data()->Resize(valid_count * dim, 0.0f);
            int64_t data_pos = 0;
            for (int64_t i = 0; i < size; i++) {
                auto idx = result_mapping[i];
                if (arr->IsNull(idx)) {
                    continue;
                }
                const uint8_t* val = nullptr;
                if (arr->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
                    val = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                              arr)
                              ->Value(idx);
                } else {
                    auto bin_val =
                        std::static_pointer_cast<arrow::BinaryArray>(arr)
                            ->Value(idx);
                    val = reinterpret_cast<const uint8_t*>(bin_val.data());
                }
                auto floats = reinterpret_cast<const float*>(val);
                milvus::fastmem::FastMemcpy(
                    float_data->mutable_data()->mutable_data() + data_pos * dim,
                    floats,
                    dim * sizeof(float));
                data_pos++;
            }
            break;
        }
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
        case DataType::VECTOR_INT8: {
            int dim = field_meta.get_dim();
            auto byte_width = field_meta.get_sizeof();
            auto vectors = data_array->mutable_vectors();
            vectors->set_dim(dim);
            std::string* vector_data = nullptr;
            switch (field_meta.get_data_type()) {
                case DataType::VECTOR_BINARY:
                    vector_data = vectors->mutable_binary_vector();
                    break;
                case DataType::VECTOR_FLOAT16:
                    vector_data = vectors->mutable_float16_vector();
                    break;
                case DataType::VECTOR_BFLOAT16:
                    vector_data = vectors->mutable_bfloat16_vector();
                    break;
                case DataType::VECTOR_INT8:
                    vector_data = vectors->mutable_int8_vector();
                    break;
                default:
                    break;
            }
            int64_t valid_count = size;
            if (field_meta.is_nullable()) {
                valid_count = 0;
                for (int64_t i = 0; i < size; i++) {
                    if (arr->IsValid(result_mapping[i])) {
                        valid_count++;
                    }
                }
            }
            vector_data->resize(valid_count * byte_width);
            int64_t data_pos = 0;
            for (int64_t i = 0; i < size; i++) {
                auto idx = result_mapping[i];
                if (arr->IsNull(idx)) {
                    continue;
                }
                const uint8_t* val = nullptr;
                if (arr->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
                    val = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                              arr)
                              ->Value(idx);
                } else {
                    auto bin_val =
                        std::static_pointer_cast<arrow::BinaryArray>(arr)
                            ->Value(idx);
                    AssertInfo(
                        static_cast<size_t>(bin_val.size()) == byte_width,
                        "vector byte width mismatch, expected {}, actual {}",
                        byte_width,
                        bin_val.size());
                    val = reinterpret_cast<const uint8_t*>(bin_val.data());
                }
                milvus::fastmem::FastMemcpy(
                    vector_data->data() + data_pos * byte_width,
                    val,
                    byte_width);
                data_pos++;
            }
            break;
        }
        case DataType::VECTOR_SPARSE_U32_F32: {
            auto vectors = data_array->mutable_vectors();
            auto sparse_data = vectors->mutable_sparse_float_vector();
            auto typed = std::static_pointer_cast<arrow::BinaryArray>(arr);
            int64_t max_dim = 0;
            for (int64_t i = 0; i < size; i++) {
                auto idx = result_mapping[i];
                if (arr->IsNull(idx)) {
                    continue;
                }
                auto val = typed->Value(idx);
                sparse_data->add_contents(val.data(), val.size());
                auto row = CopyAndWrapSparseRow(val.data(), val.size(), true);
                max_dim = std::max(max_dim, row.dim());
            }
            sparse_data->set_dim(max_dim);
            vectors->set_dim(sparse_data->dim());
            break;
        }
        case DataType::VECTOR_ARRAY: {
            // After normalize, arr is List<FixedSizeBinaryArray>.
            auto outer_list = std::static_pointer_cast<arrow::ListArray>(arr);
            auto inner_values =
                std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                    outer_list->values());
            int dim = field_meta.get_dim();
            auto element_type = field_meta.get_element_type();
            auto* va = data_array->mutable_vectors()
                           ->mutable_vector_array()
                           ->mutable_data();
            data_array->mutable_vectors()->set_dim(dim);
            for (int64_t i = 0; i < size; i++) {
                auto idx = result_mapping[i];
                int64_t start = outer_list->value_offset(idx);
                int64_t end = outer_list->value_offset(idx + 1);
                int64_t num_vectors = end - start;
                VectorArray vec_arr(inner_values->GetValue(start),
                                    num_vectors,
                                    dim,
                                    element_type);
                auto* vf = va->Add();
                *vf = vec_arr.output_data();
            }
            break;
        }
        default:
            return nullptr;  // unsupported type
    }

    // Populate valid_data for nullable fields so clients can identify nulls.
    if (field_meta.is_nullable()) {
        auto* vd = data_array->mutable_valid_data();
        vd->Reserve(size);
        for (int64_t i = 0; i < size; i++) {
            vd->Add(arr->IsValid(result_mapping[i]));
        }
    }

    return data_array;
}

std::shared_ptr<arrow::Table>
ChunkedSegmentSealedImpl::ExecuteTake(
    const std::shared_ptr<const PublishedSegmentState>& snapshot,
    const std::vector<int64_t>& unique_offsets,
    const std::shared_ptr<std::vector<std::string>>& needed_columns,
    const char* caller_tag,
    double& elapsed_ms,
    milvus::OpContext* op_ctx) const {
    // reader_->take() issues remote reads and can take seconds under slow
    // object storage. Bail out if the upstream reduce has already been
    // cancelled so we don't waste IO on a doomed request. caller_tag is a
    // short static string ("search" / "retrieve"); pass it directly to
    // avoid an extra fmt::format allocation on every call.
    segcore::CheckCancellation(op_ctx, id_, caller_tag);

    auto reader = snapshot != nullptr && snapshot->runtime != nullptr
                      ? snapshot->runtime->reader
                      : nullptr;
    if (!reader) {
        LOG_WARN("[TakeAPI] {} reader is null for segment {}", caller_tag, id_);
        return nullptr;
    }

    // Reader::take() itself is not thread-safe, so serialize only the call,
    // while the reader object lifetime is owned by the captured runtime snapshot.
    std::lock_guard<std::mutex> lock(reader_mutex_);
    auto take_start = std::chrono::high_resolution_clock::now();
    auto result = reader->take(unique_offsets, 1, needed_columns);
    elapsed_ms = std::chrono::duration<double, std::milli>(
                     std::chrono::high_resolution_clock::now() - take_start)
                     .count();
    if (!result.ok()) {
        LOG_WARN("[TakeAPI] {} take() failed for segment {}: {}",
                 caller_tag,
                 id_,
                 result.status().ToString());
        return nullptr;
    }
    return *result;
}

// ---- End shared helpers ----

bool
ChunkedSegmentSealedImpl::TryTakeForRetrieve(
    const query::RetrievePlan* plan,
    const std::unique_ptr<proto::segcore::RetrieveResults>& results,
    const int64_t* offsets,
    int64_t size,
    bool ignore_non_pk,
    bool fill_ids,
    milvus::OpContext* op_ctx) const {
    auto snapshot = CapturePublishedState();
    auto schema_snapshot = snapshot->schema;
    if (size == 0 || !snapshot->use_take_for_output) {
        return false;
    }
    const bool is_external_collection =
        schema_snapshot->is_external_collection();

    auto pk_field_id = plan->schema_->get_primary_field_id();
    auto is_pk_field = [&](const FieldId& fid) {
        return pk_field_id.has_value() && pk_field_id.value() == fid;
    };

    // Collect manifest-backed columns and their field IDs. Internal storage v2
    // uses field-id strings; external collections may use mapped source column
    // names, while milvus-table source fields use field-id strings.
    auto needed_columns = std::make_shared<std::vector<std::string>>();
    std::vector<FieldId> take_field_ids;
    std::vector<std::string> take_column_names;
    bool has_vector_output = false;
    for (auto field_id : plan->field_ids_) {
        if (SystemProperty::Instance().IsSystem(field_id)) {
            continue;
        }
        if (ignore_non_pk && !is_pk_field(field_id)) {
            continue;
        }
        auto& field_meta = schema_snapshot->operator[](field_id);
        if (is_external_collection &&
            !schema_snapshot->IsExternalManifestStoredField(field_id)) {
            continue;
        }
        has_vector_output =
            has_vector_output || IsVectorDataType(field_meta.get_data_type());
        auto column_name = schema_snapshot->get_storage_column_name(field_id);
        needed_columns->push_back(column_name);
        take_field_ids.push_back(field_id);
        take_column_names.push_back(std::move(column_name));
    }
    if (take_field_ids.empty()) {
        return false;
    }

    auto ctx = BuildTakeContext(offsets, size);
    if (SegcoreConfig::default_config().get_reject_remote_vector_output() &&
        has_vector_output) {
        LogTakeFallback("retrieve",
                        id_,
                        size,
                        ctx.unique_offsets.size(),
                        take_field_ids.size(),
                        "reject remote vector output enabled");
        return false;
    }

    double take_elapsed_ms = 0;
    auto table = ExecuteTake(snapshot,
                             ctx.unique_offsets,
                             needed_columns,
                             "retrieve",
                             take_elapsed_ms,
                             op_ctx);
    if (!table) {
        LogTakeFallback("retrieve",
                        id_,
                        size,
                        ctx.unique_offsets.size(),
                        take_field_ids.size(),
                        "take returned no table");
        return false;
    }

    // Cancellation can become observable between reader_->take() returning
    // and the Arrow Concatenate/NormalizeExternalArrow pass below, which
    // can itself be expensive on wide result sets.
    segcore::CheckCancellation(
        op_ctx, id_, "TryTakeForRetrieve(pre-arrow-convert)");

    // Convert Arrow Table columns to DataArray results
    auto fields_data = results->mutable_fields_data();
    auto ids = results->mutable_ids();

    // Build lookup from field_id to index in take_field_ids.
    std::unordered_map<int64_t, size_t> ext_field_idx;
    for (size_t fi = 0; fi < take_field_ids.size(); fi++) {
        ext_field_idx[take_field_ids[fi].get()] = fi;
    }

    // Pre-combine Arrow chunks for each external / function-output column
    std::vector<std::shared_ptr<arrow::Array>> combined_arrays(
        take_field_ids.size());
    for (size_t fi = 0; fi < take_field_ids.size(); fi++) {
        auto column_name = take_column_names[fi];
        auto col = table->GetColumnByName(column_name);
        if (!col || col->num_chunks() == 0) {
            LOG_WARN(
                "[TakeAPI] column '{}' not found in take result for "
                "segment {}",
                column_name,
                id_);
            LogTakeFallback("retrieve",
                            id_,
                            size,
                            ctx.unique_offsets.size(),
                            take_field_ids.size(),
                            fmt::format("missing column '{}'", column_name));
            return false;
        }
        if (col->num_chunks() == 1) {
            combined_arrays[fi] = col->chunk(0);
        } else {
            auto combined_result = arrow::Concatenate(col->chunks());
            if (!combined_result.ok()) {
                LOG_WARN("[TakeAPI] concatenate failed: {}",
                         combined_result.status().ToString());
                LogTakeFallback(
                    "retrieve",
                    id_,
                    size,
                    ctx.unique_offsets.size(),
                    take_field_ids.size(),
                    fmt::format("concatenate failed: {}",
                                combined_result.status().ToString()));
                return false;
            }
            combined_arrays[fi] = *combined_result;
        }
    }

    // Emit fields in plan->field_ids_ order so the positional index
    // matches outputFieldsID in the Proxy's afterReduce.
    for (auto field_id : plan->field_ids_) {
        if (SystemProperty::Instance().IsSystem(field_id)) {
            auto system_type =
                SystemProperty::Instance().GetSystemFieldType(field_id);
            FixedVector<int64_t> output(size);
            milvus::OpContext op_ctx;
            bulk_subscript(&op_ctx, system_type, offsets, size, output.data());
            auto data_array = std::make_unique<DataArray>();
            data_array->set_field_id(field_id.get());
            data_array->set_type(milvus::proto::schema::DataType::Int64);
            auto obj = data_array->mutable_scalars()->mutable_long_data();
            auto data = reinterpret_cast<const int64_t*>(output.data());
            obj->mutable_data()->Add(data, data + size);
            fields_data->AddAllocated(data_array.release());
            continue;
        }

        if (ignore_non_pk && !is_pk_field(field_id)) {
            continue;
        }

        auto& field_meta = schema_snapshot->operator[](field_id);

        // External virtual PK field (not manifest-stored, computed on the fly).
        if (is_external_collection &&
            !schema_snapshot->IsExternalManifestStoredField(field_id)) {
            if (is_pk_field(field_id) &&
                field_meta.get_data_type() == DataType::INT64) {
                auto data_array = std::make_unique<DataArray>();
                data_array->set_field_id(field_id.get());
                data_array->set_type(milvus::proto::schema::DataType::Int64);
                auto obj = data_array->mutable_scalars()->mutable_long_data();
                for (int64_t i = 0; i < size; i++) {
                    obj->add_data(GetVirtualPK(id_, offsets[i]));
                }
                if (!ignore_non_pk) {
                    fields_data->AddAllocated(data_array.release());
                }
                if (fill_ids) {
                    auto int_ids = ids->mutable_int_id();
                    for (int64_t i = 0; i < size; i++) {
                        int_ids->add_data(GetVirtualPK(id_, offsets[i]));
                    }
                }
            }
            continue;
        }

        // Convert from take() result
        auto it = ext_field_idx.find(field_id.get());
        if (it == ext_field_idx.end()) {
            continue;
        }
        size_t fi = it->second;
        auto arr = combined_arrays[fi];

        // Normalize external arrow types to Milvus internal format.
        if (is_external_collection) {
            arr = storage::NormalizeExternalArrow(arr, field_meta);
        }

        auto dynamic_field_names =
            ShouldProjectInternalTakeDynamicField(
                *schema_snapshot, field_id, plan->target_dynamic_fields_)
                ? &plan->target_dynamic_fields_
                : nullptr;
        const std::string* text_lob_path = nullptr;
        if (!is_external_collection &&
            field_meta.get_data_type() == DataType::TEXT) {
            auto path_it = snapshot->runtime->text_lob_paths.find(field_id);
            if (path_it == snapshot->runtime->text_lob_paths.end()) {
                LogTakeFallback(
                    "retrieve",
                    id_,
                    size,
                    ctx.unique_offsets.size(),
                    take_field_ids.size(),
                    fmt::format("missing TEXT LOB path for field {}",
                                field_id.get()));
                results->clear_fields_data();
                results->clear_ids();
                return false;
            }
            text_lob_path = &path_it->second;
        }
        auto data_array = ArrowToDataArray(arr,
                                           field_meta,
                                           ctx.result_mapping,
                                           size,
                                           dynamic_field_names,
                                           text_lob_path);
        if (!data_array) {
            LOG_WARN(
                "[TakeAPI] unsupported data type {} for field '{}', "
                "falling back",
                static_cast<int>(field_meta.get_data_type()),
                take_column_names[fi]);
            LogTakeFallback(
                "retrieve",
                id_,
                size,
                ctx.unique_offsets.size(),
                take_field_ids.size(),
                fmt::format("unsupported data type {} for column '{}'",
                            static_cast<int>(field_meta.get_data_type()),
                            take_column_names[fi]));
            results->clear_fields_data();
            results->clear_ids();
            return false;
        }
        data_array->set_field_id(field_id.get());

        if (fill_ids && is_pk_field(field_id)) {
            switch (field_meta.get_data_type()) {
                case DataType::INT64: {
                    auto int_ids = ids->mutable_int_id();
                    auto& src_data = data_array->scalars().long_data();
                    int_ids->mutable_data()->Add(src_data.data().begin(),
                                                 src_data.data().end());
                    break;
                }
                case DataType::VARCHAR: {
                    auto str_ids = ids->mutable_str_id();
                    auto& src_data = data_array->scalars().string_data();
                    for (auto i = 0; i < src_data.data_size(); ++i) {
                        *(str_ids->mutable_data()->Add()) = src_data.data(i);
                    }
                    break;
                }
                default: {
                    ThrowInfo(DataTypeInvalid,
                              fmt::format("unsupported datatype {}",
                                          field_meta.get_data_type()));
                }
            }
        }

        if (!ignore_non_pk) {
            fields_data->AddAllocated(data_array.release());
        }
    }

    LOG_DEBUG(
        "[TakeAPI] segment {} used take() for {} rows ({} unique), "
        "{} fields, elapsed={:.2f}ms",
        id_,
        size,
        ctx.unique_offsets.size(),
        take_field_ids.size(),
        take_elapsed_ms);
    return true;
}

bool
ChunkedSegmentSealedImpl::TryTakeForSearch(const query::Plan* plan,
                                           const int64_t* seg_offsets,
                                           int64_t size,
                                           SearchResult& results,
                                           milvus::OpContext* op_ctx) const {
    auto snapshot = CapturePublishedState();
    auto schema_snapshot = snapshot->schema;
    if (size == 0 || !snapshot->use_take_for_output) {
        return false;
    }
    const bool is_external_collection =
        schema_snapshot->is_external_collection();

    // Collect manifest-backed columns. Internal storage v2 uses field-id
    // strings; external collections may use mapped source column names, while
    // milvus-table source fields use field-id strings.
    auto needed_columns = std::make_shared<std::vector<std::string>>();
    std::vector<FieldId> take_field_ids;
    std::vector<const FieldMeta*> take_field_metas;
    std::vector<std::string> take_column_names;
    bool has_vector_output = false;
    for (auto field_id : plan->target_entries_) {
        auto& field_meta = schema_snapshot->operator[](field_id);
        if (is_external_collection &&
            !schema_snapshot->IsExternalManifestStoredField(field_id)) {
            continue;
        }
        has_vector_output =
            has_vector_output || IsVectorDataType(field_meta.get_data_type());
        auto column_name = schema_snapshot->get_storage_column_name(field_id);
        needed_columns->push_back(column_name);
        take_field_ids.push_back(field_id);
        take_field_metas.push_back(&field_meta);
        take_column_names.push_back(std::move(column_name));
    }
    if (take_field_ids.empty()) {
        return false;
    }

    auto ctx = BuildTakeContext(seg_offsets, size);
    if (SegcoreConfig::default_config().get_reject_remote_vector_output() &&
        has_vector_output) {
        LogTakeFallback("search",
                        id_,
                        size,
                        ctx.unique_offsets.size(),
                        take_field_ids.size(),
                        "reject remote vector output enabled");
        return false;
    }

    double take_elapsed_ms = 0;
    auto table = ExecuteTake(snapshot,
                             ctx.unique_offsets,
                             needed_columns,
                             "search",
                             take_elapsed_ms,
                             op_ctx);
    if (!table) {
        LogTakeFallback("search",
                        id_,
                        size,
                        ctx.unique_offsets.size(),
                        take_field_ids.size(),
                        "take returned no table");
        return false;
    }

    // Cancellation can become observable between reader_->take() returning
    // and the Arrow Concatenate/NormalizeExternalArrow pass below, which
    // can itself be expensive on wide result sets.
    segcore::CheckCancellation(
        op_ctx, id_, "TryTakeForSearch(pre-arrow-convert)");

    // Convert Arrow Table columns to DataArray and store in SearchResult
    for (size_t fi = 0; fi < take_field_ids.size(); fi++) {
        auto field_id = take_field_ids[fi];
        auto& field_meta = *take_field_metas[fi];
        auto column_name = take_column_names[fi];
        auto col = table->GetColumnByName(column_name);
        if (!col || col->num_chunks() == 0) {
            LOG_WARN("[TakeAPI] search column '{}' not found for segment {}",
                     column_name,
                     id_);
            LogTakeFallback("search",
                            id_,
                            size,
                            ctx.unique_offsets.size(),
                            take_field_ids.size(),
                            fmt::format("missing column '{}'", column_name));
            return false;
        }

        std::shared_ptr<arrow::Array> arr;
        if (col->num_chunks() == 1) {
            arr = col->chunk(0);
        } else {
            auto combined_result = arrow::Concatenate(col->chunks());
            if (!combined_result.ok()) {
                LogTakeFallback(
                    "search",
                    id_,
                    size,
                    ctx.unique_offsets.size(),
                    take_field_ids.size(),
                    fmt::format("concatenate failed: {}",
                                combined_result.status().ToString()));
                return false;
            }
            arr = *combined_result;
        }

        // Normalize external arrow types to Milvus internal format.
        if (is_external_collection) {
            arr = storage::NormalizeExternalArrow(arr, field_meta);
        }

        auto dynamic_field_names =
            ShouldProjectInternalTakeDynamicField(
                *schema_snapshot, field_id, plan->target_dynamic_fields_)
                ? &plan->target_dynamic_fields_
                : nullptr;
        const std::string* text_lob_path = nullptr;
        if (!is_external_collection &&
            field_meta.get_data_type() == DataType::TEXT) {
            auto path_it = snapshot->runtime->text_lob_paths.find(field_id);
            if (path_it == snapshot->runtime->text_lob_paths.end()) {
                LogTakeFallback(
                    "search",
                    id_,
                    size,
                    ctx.unique_offsets.size(),
                    take_field_ids.size(),
                    fmt::format("missing TEXT LOB path for field {}",
                                field_id.get()));
                results.output_fields_data_.clear();
                return false;
            }
            text_lob_path = &path_it->second;
        }
        auto data_array = ArrowToDataArray(arr,
                                           field_meta,
                                           ctx.result_mapping,
                                           size,
                                           dynamic_field_names,
                                           text_lob_path);
        if (!data_array) {
            LOG_WARN(
                "[TakeAPI] search: unsupported type {} for '{}', "
                "falling back",
                static_cast<int>(field_meta.get_data_type()),
                column_name);
            LogTakeFallback(
                "search",
                id_,
                size,
                ctx.unique_offsets.size(),
                take_field_ids.size(),
                fmt::format("unsupported data type {} for column '{}'",
                            static_cast<int>(field_meta.get_data_type()),
                            column_name));
            results.output_fields_data_.clear();
            return false;
        }
        data_array->set_field_id(field_id.get());
        results.output_fields_data_[field_id] = std::move(data_array);
    }

    LOG_DEBUG(
        "[TakeAPI] search: segment {} used take() for {} rows ({} unique), "
        "{} fields, elapsed={:.2f}ms",
        id_,
        size,
        ctx.unique_offsets.size(),
        take_field_ids.size(),
        take_elapsed_ms);
    return true;
}

void
ChunkedSegmentSealedImpl::prefetch_vector(milvus::OpContext* op_ctx,
                                          FieldId field_id) const {
    std::shared_lock vector_state_lck(mutex_);
    auto runtime = CaptureRuntimeResourceState();
    auto vector_entry = GetVectorIndexing(runtime, field_id);
    if (vector_entry != nullptr) {
        SemiInlineGet(vector_entry->indexing_->PinCells(op_ctx, {0}));
    } else {
        this->prefetch_chunks_locked(op_ctx, field_id);
    }
}
}  // namespace milvus::segcore
