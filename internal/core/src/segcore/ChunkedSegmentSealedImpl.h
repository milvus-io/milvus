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

#pragma once

#include <folly/ExceptionWrapper.h>
#include <stdint.h>
#include <any>
#include <atomic>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "ConcurrentVector.h"
#include "DeletedRecord.h"
#include "NamedType/underlying_functionalities.hpp"
#include "SealedIndexingRecord.h"
#include "SegmentSealed.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Utils.h"
#include "common/Array.h"
#include "common/ArrayOffsets.h"
#include "common/BitsetView.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/Json.h"
#include "common/LoadInfo.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/SystemProperty.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/VectorArray.h"
#include "common/protobuf_utils.h"
#include "fmt/core.h"
#include "folly/FBVector.h"
#include "google/protobuf/message.h"
#include "index/Index.h"
#include "index/NgramInvertedIndex.h"
#include "index/json_stats/JsonKeyStats.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"
#include "mmap/ChunkedColumnInterface.h"
#include "mmap/Types.h"
#include "parquet/statistics.h"
#include "pb/common.pb.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/plan.pb.h"
#include "pb/segcore.pb.h"
#include "query/PlanImpl.h"
#include "segcore/IndexConfigGenerator.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentLoadInfo.h"
#include "segcore/Types.h"
#include "storage/MmapChunkManager.h"
#include "segcore/TextColumnCache.h"

namespace milvus::segcore {

namespace storagev2translator {
class TimestampIndexCell;
class PkIndexCell;
}  // namespace storagev2translator

class TimestampData;
class TimestampIndex;

using namespace milvus::cachinglayer;

// Test-only accessor that simulates v2/v3 segment state (raw timestamp column
// published in runtime alongside an overwritten timestamp index). Defined in
// internal/core/unittest/test_commit_timestamp.cpp.
class CommitTimestampV2TestAccess;

class ChunkedSegmentSealedImpl : public SegmentSealed {
    friend class CommitTimestampV2TestAccess;

 public:
    using ParquetStatistics = std::vector<std::shared_ptr<parquet::Statistics>>;
    explicit ChunkedSegmentSealedImpl(SchemaPtr schema,
                                      IndexMetaPtr index_meta,
                                      const SegcoreConfig& segcore_config,
                                      int64_t segment_id,
                                      bool is_sorted_by_pk = false);
    ~ChunkedSegmentSealedImpl() override;
    void
    LoadIndex(LoadIndexInfo& info) override;
    void
    LoadFieldData(const LoadFieldDataInfo& info,
                  milvus::OpContext* op_ctx = nullptr) override;
    void
    LoadDeletedRecord(const LoadDeletedRecordInfo& info) override;
    void
    LoadSegmentMeta(
        const milvus::proto::segcore::LoadSegmentMeta& segment_meta) override;
    void
    DropIndex(const FieldId field_id) override;
    void
    DropJSONIndex(const FieldId field_id,
                  const std::string& nested_path) override;
    void
    DropFieldData(const FieldId field_id) override;
    bool
    HasIndex(FieldId field_id) const override;
    bool
    HasJsonIndex(FieldId field_id) const override;
    bool
    HasFieldData(FieldId field_id) const override;
    // Checks the loaded external manifest for a storage column.
    bool
    HasColumnInLoadedManifest(const std::string& column_name) const override;

    std::pair<std::shared_ptr<ChunkedColumnInterface>, bool>
    GetFieldDataIfExist(FieldId field_id) const;

    std::vector<PinWrapper<const index::IndexBase*>>
    PinIndex(milvus::OpContext* op_ctx,
             FieldId field_id,
             bool include_ngram = false) const override {
        auto snapshot = CapturePublishedState();
        if (snapshot != nullptr && snapshot->runtime != nullptr) {
            const auto& runtime = *snapshot->runtime;
            if (!include_ngram && runtime.ngram_fields.find(field_id) !=
                                      runtime.ngram_fields.end()) {
                return {};
            }

            auto iter = runtime.scalar_indexings.find(field_id);
            if (iter != runtime.scalar_indexings.end()) {
                auto ca = SemiInlineGet(iter->second->PinCells(op_ctx, {0}));
                auto index = ca->get_cell_of(0);
                return {
                    PinWrapper<const index::IndexBase*>(std::move(ca), index)};
            }
        }
        return {};
    }

    std::vector<PinWrapper<const index::IndexBase*>>
    PinJsonIndex(milvus::OpContext* op_ctx,
                 FieldId field_id,
                 const std::string& path,
                 DataType data_type,
                 bool any_type,
                 bool is_array) const override;

    std::string
    GetJsonFlatIndexNestedPath(FieldId field_id,
                               std::string_view query_path) const override;

    bool
    Contain(const PkType& pk) const override;

    void
    FillPrimaryKeys(const query::Plan* plan,
                    SearchResult& results,
                    milvus::OpContext* op_ctx = nullptr) const override;

    void
    AddFieldDataInfoForSealed(
        const LoadFieldDataInfo& field_data_info) override;

    int64_t
    get_segment_id() const override {
        return id_;
    }

    bool
    HasRawData(int64_t field_id) const override;

    // Returns true only if the index itself contains raw data,
    // without considering whether field data is loaded.
    bool
    IndexHasRawData(FieldId field_id) const;

    bool
    CalcDistByIDs(FieldId field_id,
                  const knowhere::DataSetPtr& query_dataset,
                  const int64_t* seg_offsets,
                  size_t count,
                  bool is_cosine,
                  float* distances) const override {
        return CalcDistByIDs(nullptr,
                             field_id,
                             query_dataset,
                             seg_offsets,
                             count,
                             is_cosine,
                             distances);
    }

    bool
    CalcDistByIDs(milvus::OpContext* op_ctx,
                  FieldId field_id,
                  const knowhere::DataSetPtr& query_dataset,
                  const int64_t* seg_offsets,
                  size_t count,
                  bool is_cosine,
                  float* distances) const override;

    bool
    IsIndexRefineEnabled(FieldId field_id) const override {
        return IsIndexRefineEnabled(nullptr, field_id);
    }

    bool
    IsIndexRefineEnabled(milvus::OpContext* op_ctx,
                         FieldId field_id) const override;

    DataType
    GetFieldDataType(FieldId fieldId) const override;

    void
    RemoveFieldFile(const FieldId field_id) override;

    void
    CreateTextIndex(FieldId field_id,
                    milvus::OpContext* op_ctx = nullptr) override;

    PinWrapper<index::TextMatchIndex*>
    GetTextIndex(milvus::OpContext* op_ctx, FieldId field_id) const override;

    std::shared_ptr<index::JsonKeyStats>
    GetJsonStats(milvus::OpContext* op_ctx, FieldId field_id) const override;

    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(milvus::OpContext* op_ctx, FieldId field_id) const override;

    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndexForJson(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         const std::string& nested_path) const override;

    std::shared_ptr<const IArrayOffsets>
    GetArrayOffsets(FieldId field_id) const override {
        auto runtime = CaptureRuntimeResourceState();
        auto it = runtime->array_offsets_map.find(field_id);
        if (it != runtime->array_offsets_map.end()) {
            return it->second;
        }
        return nullptr;
    }

    void
    BulkGetJsonData(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    const std::function<void(milvus::Json, size_t, bool)>& fn,
                    const int64_t* offsets,
                    int64_t count) const override {
        auto column = get_column(field_id);
        AssertInfo(column != nullptr,
                   "json field {} must exist when bulk reading raw data",
                   field_id.get());
        column->BulkRawJsonAt(op_ctx, fn, offsets, count);
    }

    void
    Reopen(SchemaPtr sch) override;

    void
    Reopen(
        milvus::OpContext* op_ctx,
        const milvus::proto::segcore::SegmentLoadInfo& new_load_info) override;

    void
    Reopen(milvus::OpContext* op_ctx,
           const milvus::proto::segcore::SegmentLoadInfo& new_load_info,
           SchemaPtr new_schema) override;

    void
    LazyCheckSchema(SchemaPtr sch, milvus::OpContext* op_ctx) override;

    void
    SetLoadInfo(milvus::proto::segcore::SegmentLoadInfo load_info) override;

    void
    SetCommitTimestamp(uint64_t ts) override;

    uint64_t
    GetCommitTimestamp() const override;

    struct RuntimeResourceState;

    using TextIndexVariant =
        std::variant<std::shared_ptr<milvus::index::TextMatchIndexHolder>,
                     std::shared_ptr<milvus::cachinglayer::CacheSlot<
                         milvus::index::TextMatchIndex>>>;

    struct JsonIndex {
        FieldId field_id;
        std::string nested_path;
        JsonCastType cast_type{JsonCastType::UNKNOWN};
        index::CacheIndexBasePtr index;
    };

    // When non-zero commit_ts is active, every row in this segment carries it
    // as its effective row timestamp (load-time overwrite). All timestamp
    // consumers must route through this so the override applies uniformly on
    // v1 AND v2/v3 storage paths.
    std::optional<Timestamp>
    EffectiveCommitTs() const;

    struct RuntimeResourceState {
        std::unordered_map<FieldId, std::shared_ptr<ChunkedColumnInterface>>
            fields;
        std::unordered_map<std::string, std::shared_ptr<ArrayOffsetsSealed>>
            struct_to_array_offsets;
        std::unordered_map<FieldId, std::shared_ptr<ArrayOffsetsSealed>>
            array_offsets_map;
        std::unordered_map<FieldId, index::CacheIndexBasePtr> scalar_indexings;
        std::unordered_map<FieldId, SealedIndexingEntryPtr> vector_indexings;
        std::unordered_map<FieldId, std::shared_ptr<const VecIndexConfig>>
            vec_binlog_config;
        std::unordered_set<FieldId> ngram_fields;
        std::unordered_map<
            FieldId,
            std::unordered_map<std::string, index::CacheIndexBasePtr>>
            ngram_indexings;
        std::unordered_map<FieldId, std::string> text_lob_paths;
        std::unordered_map<FieldId, TextIndexVariant> text_indexes;
        std::vector<JsonIndex> json_indices;
        std::unordered_map<FieldId, std::shared_ptr<index::JsonKeyStats>>
            json_stats;
        std::shared_ptr<milvus_storage::api::Reader> reader;
        std::shared_ptr<TimestampData> timestamps;
        std::shared_ptr<const TimestampIndex> timestamp_index;
        std::shared_ptr<CacheSlot<storagev2translator::TimestampIndexCell>>
            timestamp_index_slot;
        std::shared_ptr<CacheSlot<storagev2translator::PkIndexCell>>
            pk_index_slot;
        std::shared_ptr<const OffsetMap> virtual_pk2offset;
        std::shared_ptr<SkipIndex> skip_index;
        std::unordered_set<FieldId> mmap_field_ids;
        std::unordered_map<FieldId, std::pair<int64_t, int64_t>>
            variable_fields_avg_size;
        int64_t row_count{0};
    };

    struct PublishedSegmentState {
        SchemaPtr schema;
        std::shared_ptr<const SegmentLoadInfo> load_info;
        std::shared_ptr<const RuntimeResourceState> runtime;
        Timestamp commit_ts{0};
        bool use_take_for_output{false};
        bool system_field_ready{false};
        BitsetType published_index_ready_bitset;
        BitsetType published_binlog_index_ready_bitset;
        std::unordered_map<FieldId, bool> published_index_has_raw_data;
        BitsetType field_data_ready_bitset;
        BitsetType index_ready_bitset;
        BitsetType binlog_index_bitset;
        std::unordered_map<FieldId, bool> index_has_raw_data;
    };

    struct StateDelta {
        std::optional<SchemaPtr> schema;
        std::optional<std::shared_ptr<const SegmentLoadInfo>> load_info;
        std::optional<std::shared_ptr<const RuntimeResourceState>> runtime;
        std::optional<Timestamp> commit_ts;
        std::optional<BitsetType> published_index_ready_bitset;
        std::optional<BitsetType> published_binlog_index_ready_bitset;
        std::optional<std::unordered_map<FieldId, bool>>
            published_index_has_raw_data;
    };

    static std::shared_ptr<const RuntimeResourceState>
    BuildRuntimeResourceState();

    static std::shared_ptr<RuntimeResourceState>
    CloneRuntimeResourceState(
        const std::shared_ptr<const RuntimeResourceState>& current);

    static std::shared_ptr<const RuntimeResourceState>
    FreezeRuntimeResourceState(const RuntimeResourceState& current);

    void
    Load(milvus::tracer::TraceContext& trace_ctx,
         milvus::OpContext* op_ctx) override;

 public:
    size_t
    GetMemoryUsageInBytes() const override {
        return stats_.mem_size.load() + deleted_record_.mem_size();
    }

    int64_t
    get_row_count() const override;

    int64_t
    get_field_avg_size(FieldId field_id) const override;

    void
    set_field_avg_size(FieldId field_id,
                       int64_t num_rows,
                       int64_t field_size) override;

    std::shared_ptr<const SkipIndex>
    GetSkipIndexSnapshot() const;

    int64_t
    get_deleted_count() const override;

    Timestamp
    get_max_timestamp() const override {
        auto runtime = CaptureRuntimeResourceState();
        return runtime != nullptr && runtime->timestamp_index != nullptr
                   ? runtime->timestamp_index->get_max_timestamp()
                   : 0;
    }

    const Schema&
    get_schema() const override;

    void
    pk_range(milvus::OpContext* op_ctx,
             proto::plan::OpType op,
             const PkType& pk,
             BitsetTypeView& bitset) const override;

    void
    search_sorted_pk_range(
        milvus::OpContext* op_ctx,
        proto::plan::OpType op,
        const PkType& pk,
        BitsetTypeView& bitset,
        const std::shared_ptr<const PublishedSegmentState>& snapshot) const;

    void
    pk_binary_range(milvus::OpContext* op_ctx,
                    const PkType& lower_pk,
                    bool lower_inclusive,
                    const PkType& upper_pk,
                    bool upper_inclusive,
                    BitsetTypeView& bitset) const override;

    std::unique_ptr<DataArray>
    get_vector(milvus::OpContext* op_ctx,
               FieldId field_id,
               const int64_t* ids,
               int64_t count) const override;

    std::unique_ptr<DataArray>
    get_emb_list(milvus::OpContext* op_ctx,
                 FieldId field_id,
                 const FieldMeta& field_meta,
                 const int64_t* seg_offsets,
                 int64_t count) const;

    bool
    is_nullable(FieldId field_id) const override {
        auto schema_snapshot = CaptureSchemaSnapshot();
        auto& field_meta = schema_snapshot->operator[](field_id);
        return field_meta.is_nullable();
    };

    bool
    is_chunked() const override {
        return true;
    }

    void
    search_pks(BitsetType& bitset, const std::vector<PkType>& pks) const;

    void
    search_batch_pks(
        const std::vector<PkType>& pks,
        const std::function<Timestamp(const size_t idx)>& get_timestamp,
        bool include_same_ts,
        const std::function<void(const SegOffset offset, const Timestamp ts)>&
            callback) const;

 public:
    // Non-virtual helper called via dynamic_cast from SegmentInterface.
    // Must be public for cross-class access.
    bool
    TryTakeForRetrieve(
        const query::RetrievePlan* plan,
        const std::unique_ptr<proto::segcore::RetrieveResults>& results,
        const int64_t* offsets,
        int64_t size,
        bool ignore_non_pk,
        bool fill_ids,
        milvus::OpContext* op_ctx = nullptr) const;

    // count of chunk that has raw data
    int64_t
    num_chunk_data(FieldId field_id) const override;

    int64_t
    num_chunk(FieldId field_id) const override;

    // return size_per_chunk for each chunk, renaming against confusion
    int64_t
    size_per_chunk() const override;

    int64_t
    chunk_size(FieldId field_id, int64_t chunk_id) const override;

    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId field_id, int64_t offset) const override;

    int64_t
    num_rows_until_chunk(FieldId field_id, int64_t chunk_id) const override;

    SegcoreError
    Delete(int64_t size,
           const IdArray* pks,
           const Timestamp* timestamps) override;

    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n(int64_t limit, const BitsetTypeView& bitset) const override;

    std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
    find_first_n_element(
        int64_t limit,
        const BitsetTypeView& element_bitset,
        const IArrayOffsets* array_offsets,
        const std::optional<QueryIteratorCursor>& cursor) const override;

    // Calculate: output[i] = Vec[seg_offset[i]]
    // where Vec is determined from field_offset
    std::unique_ptr<DataArray>
    bulk_subscript(milvus::OpContext* op_ctx,
                   FieldId field_id,
                   const int64_t* seg_offsets,
                   int64_t count) const override;

    std::unique_ptr<DataArray>
    bulk_subscript(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        const int64_t* seg_offsets,
        int64_t count,
        const std::vector<std::string>& dynamic_field_names) const override;

    bool
    is_mmap_field(FieldId id) const override;

    void
    ClearData() override;

    bool
    is_field_exist(FieldId field_id) const override {
        auto schema_snapshot = CaptureSchemaSnapshot();
        return schema_snapshot->get_fields().find(field_id) !=
               schema_snapshot->get_fields().end();
    }

    void
    prefetch_chunks(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    const std::vector<int64_t>& chunk_ids) const override;

    void
    prefetch_chunks(milvus::OpContext* op_ctx, FieldId field_id) const override;

    void
    prefetch_vector(milvus::OpContext* op_ctx, FieldId field_id) const override;

    void
    ApplyFieldValidData(milvus::OpContext* op_ctx,
                        FieldId field_id,
                        int64_t chunk_id,
                        int64_t offset,
                        int64_t size,
                        TargetBitmapView valid_result) const override;

    void
    ApplyFieldValidDataByOffsets(milvus::OpContext* op_ctx,
                                 FieldId field_id,
                                 const int64_t* offsets,
                                 int64_t count,
                                 TargetBitmapView valid_result) const override;

 protected:
    // blob and row_count
    PinWrapper<SpanBase>
    chunk_data_impl(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    int64_t chunk_id) const override;

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    chunk_string_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    chunk_array_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<int64_t>, FixedVector<bool>>>
    chunk_array_lengths_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    chunk_vector_array_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<int64_t>, FixedVector<bool>>>
    chunk_vector_array_lengths_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override;

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    chunk_string_views_by_offsets(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        const FixedVector<int32_t>& offsets) const override;

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    chunk_array_views_by_offsets(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        const FixedVector<int32_t>& offsets) const override;

    // Calculate: output[i] = Vec[seg_offset[i]],
    // where Vec is determined from field_offset
    void
    bulk_subscript(milvus::OpContext* op_ctx,
                   SystemFieldType system_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* output) const override;

    void
    bulk_subscript(milvus::OpContext* op_ctx,
                   FieldId field_id,
                   DataType data_type,
                   const int64_t* seg_offsets,
                   int64_t count,
                   void* data,
                   TargetBitmap& valid_map,
                   bool small_int_raw_type = false) const override;

    // Override to inject take() fast path for Search output fields.
    // Uses existing vtable slot — no layout change.
    void
    FillTargetEntry(const query::Plan* plan,
                    SearchResult& results,
                    milvus::OpContext* op_ctx = nullptr) const override;

    bool
    TryTakeForSearch(const query::Plan* plan,
                     const int64_t* seg_offsets,
                     int64_t size,
                     SearchResult& results,
                     milvus::OpContext* op_ctx = nullptr) const;

    // Shared helpers for TryTakeForRetrieve / TryTakeForSearch
    struct TakeContext {
        std::vector<int64_t> unique_offsets;
        std::vector<int64_t> result_mapping;  // orig_pos → unique index
    };

    static TakeContext
    BuildTakeContext(const int64_t* offsets, int64_t size);

    // Converts a combined Arrow array into a proto DataArray using
    // result_mapping for reorder.  Returns nullptr on unsupported type.
    static std::unique_ptr<DataArray>
    ArrowToDataArray(
        const std::shared_ptr<arrow::Array>& arr,
        const FieldMeta& field_meta,
        const std::vector<int64_t>& result_mapping,
        int64_t size,
        const std::vector<std::string>* dynamic_field_names = nullptr,
        const std::string* text_lob_path = nullptr);

    // Calls reader_->take() with timing. Returns the table on success,
    // or nullptr on failure (logs a warning). Checks op_ctx for cancellation
    // before invoking reader_->take(), which can do remote reads.
    std::shared_ptr<arrow::Table>
    ExecuteTake(const std::shared_ptr<const PublishedSegmentState>& snapshot,
                const std::vector<int64_t>& unique_offsets,
                const std::shared_ptr<std::vector<std::string>>& needed_columns,
                const char* caller_tag,
                double& elapsed_ms,
                milvus::OpContext* op_ctx = nullptr) const;

    void
    check_search(const query::Plan* plan) const override;

    int64_t
    get_active_count(Timestamp ts) const override;

    const ConcurrentVector<Timestamp>&
    get_timestamps() const override {
        // Sealed segments no longer store timestamps in ConcurrentVector.
        // Only growing segments use this method.
        ThrowInfo(NotImplemented,
                  "sealed segment does not support get_timestamps()");
    }

    // Load Geometry cache for a field
    void
    LoadGeometryCache(FieldId field_id,
                      const std::shared_ptr<ChunkedColumnInterface>& column);

 private:
    void
    load_system_field_internal(
        FieldId field_id,
        FieldDataInfo& data,
        milvus::proto::common::LoadPriority load_priority,
        RuntimeResourceState* runtime = nullptr,
        bool publish_ready = true);

    // Initialize timestamp index with owned data (StorageV1 path)
    void
    init_storage_v1_timestamp_index(std::vector<Timestamp> timestamps,
                                    size_t num_rows);

    void
    init_storage_v1_timestamp_index(std::vector<Timestamp> timestamps,
                                    size_t num_rows,
                                    RuntimeResourceState* runtime);

    void
    init_storage_v2_timestamp_index(
        const std::shared_ptr<ChunkedColumnInterface>& column,
        size_t num_rows,
        const std::string& warmup_policy,
        RuntimeResourceState* runtime);

    static TimestampIndex
    build_timestamp_index(const Timestamp* data, size_t num_rows);

    template <typename PK>
    void
    search_sorted_pk_range_impl(
        proto::plan::OpType op,
        const PK& target,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column,
        BitsetTypeView& bitset) const {
        const auto num_chunk = pk_column->num_chunks();
        if (num_chunk == 0) {
            return;
        }
        auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

        if (op == proto::plan::OpType::Equal) {
            // find first occurrence
            auto [chunk_id, in_chunk_offset, exact_match] =
                this->pk_lower_bound<PK>(
                    target, pk_column.get(), all_chunk_pins, 0);

            if (exact_match) {
                // find last occurrence
                auto [last_chunk_id, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(target,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    chunk_id,
                                                    in_chunk_offset);

                auto start_idx =
                    pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
                auto end_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                               last_in_chunk_offset;

                bitset.set(start_idx, end_idx - start_idx + 1, true);
            }
        } else if (op == proto::plan::OpType::GreaterEqual ||
                   op == proto::plan::OpType::GreaterThan) {
            auto [chunk_id, in_chunk_offset, exact_match] =
                this->pk_lower_bound<PK>(
                    target, pk_column.get(), all_chunk_pins, 0);

            if (chunk_id != -1) {
                int64_t start_idx =
                    pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
                if (exact_match && op == proto::plan::OpType::GreaterThan) {
                    auto [last_chunk_id, last_in_chunk_offset] =
                        this->find_last_pk_position<PK>(target,
                                                        pk_column.get(),
                                                        all_chunk_pins,
                                                        chunk_id,
                                                        in_chunk_offset);
                    start_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                                last_in_chunk_offset + 1;
                }
                if (start_idx < bitset.size()) {
                    bitset.set(start_idx, bitset.size() - start_idx, true);
                }
            }
        } else if (op == proto::plan::OpType::LessEqual ||
                   op == proto::plan::OpType::LessThan) {
            auto [chunk_id, in_chunk_offset, exact_match] =
                this->pk_lower_bound<PK>(
                    target, pk_column.get(), all_chunk_pins, 0);

            int64_t end_idx;
            if (chunk_id == -1) {
                end_idx = bitset.size();
            } else if (op == proto::plan::OpType::LessEqual && exact_match) {
                auto [last_chunk_id, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(target,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    chunk_id,
                                                    in_chunk_offset);
                end_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                          last_in_chunk_offset + 1;
            } else {
                end_idx =
                    pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
            }

            if (end_idx > 0) {
                bitset.set(0, end_idx, true);
            }
        } else {
            ThrowInfo(ErrorCode::Unsupported,
                      fmt::format("unsupported op type {}", op));
        }
    }

    template <typename PK>
    void
    search_sorted_pk_binary_range_impl(
        const PK& lower_val,
        bool lower_inclusive,
        const PK& upper_val,
        bool upper_inclusive,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column,
        BitsetTypeView& bitset) const {
        const auto num_chunk = pk_column->num_chunks();
        if (num_chunk == 0) {
            return;
        }
        auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

        // Find the lower bound position (first value >= lower_val or > lower_val)
        auto [lower_chunk_id, lower_in_chunk_offset, lower_exact_match] =
            this->pk_lower_bound<PK>(
                lower_val, pk_column.get(), all_chunk_pins, 0);

        int64_t start_idx = 0;
        if (lower_chunk_id != -1) {
            start_idx = pk_column->GetNumRowsUntilChunk(lower_chunk_id) +
                        lower_in_chunk_offset;
            // If lower_inclusive is false and we found an exact match, skip all equal values
            if (!lower_inclusive && lower_exact_match) {
                auto [last_chunk_id, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(lower_val,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    lower_chunk_id,
                                                    lower_in_chunk_offset);
                start_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                            last_in_chunk_offset + 1;
            }
        } else {
            // lower_val is greater than all values, no results
            return;
        }

        // Find the upper bound position (first value >= upper_val or > upper_val)
        auto [upper_chunk_id, upper_in_chunk_offset, upper_exact_match] =
            this->pk_lower_bound<PK>(
                upper_val, pk_column.get(), all_chunk_pins, 0);

        int64_t end_idx = 0;
        if (upper_chunk_id == -1) {
            // upper_val is greater than all values, include all from start_idx to end
            end_idx = bitset.size();
        } else {
            // If upper_inclusive is true and we found an exact match, include all equal values
            if (upper_inclusive && upper_exact_match) {
                auto [last_chunk_id, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(upper_val,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    upper_chunk_id,
                                                    upper_in_chunk_offset);
                end_idx = pk_column->GetNumRowsUntilChunk(last_chunk_id) +
                          last_in_chunk_offset + 1;
            } else {
                // upper_inclusive is false or no exact match
                // In both cases, end at the position of first value >= upper_val
                end_idx = pk_column->GetNumRowsUntilChunk(upper_chunk_id) +
                          upper_in_chunk_offset;
            }
        }

        // Set bits from start_idx to end_idx - 1
        if (start_idx < end_idx) {
            bitset.set(start_idx, end_idx - start_idx, true);
        }
    }

    template <typename PK>
    std::optional<int64_t>
    find_sorted_pk_doc_offset(
        const PK& pk,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column) const {
        auto all_chunk_pins = pk_column->GetAllChunks(nullptr);
        auto [chunk_id, in_chunk_offset, exact_match] =
            this->pk_lower_bound<PK>(pk, pk_column.get(), all_chunk_pins, 0);
        if (!exact_match) {
            return std::nullopt;
        }
        return pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
    }

    template <typename PK>
    void
    search_pks_with_two_pointers_impl(
        BitsetTypeView& bitset,
        const std::vector<PkType>& pks,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column) const {
        // TODO: we should sort pks during plan generation
        std::vector<PK> sorted_pks;
        sorted_pks.reserve(pks.size());
        for (const auto& pk : pks) {
            sorted_pks.push_back(std::get<PK>(pk));
        }
        std::sort(sorted_pks.begin(), sorted_pks.end());

        auto all_chunk_pins = pk_column->GetAllChunks(nullptr);

        size_t pk_idx = 0;
        int last_chunk_id = 0;

        while (pk_idx < sorted_pks.size()) {
            const auto& target_pk = sorted_pks[pk_idx];

            // find the first occurrence of target_pk
            auto [chunk_id, in_chunk_offset, exact_match] =
                this->pk_lower_bound<PK>(
                    target_pk, pk_column.get(), all_chunk_pins, last_chunk_id);

            if (chunk_id == -1) {
                // All remaining PKs are greater than all values in pk_column
                break;
            }

            if (exact_match) {
                // Found exact match, find the last occurrence
                auto [last_chunk_id_found, last_in_chunk_offset] =
                    this->find_last_pk_position<PK>(target_pk,
                                                    pk_column.get(),
                                                    all_chunk_pins,
                                                    chunk_id,
                                                    in_chunk_offset);

                // Mark all occurrences from first to last position using global indices
                auto start_idx =
                    pk_column->GetNumRowsUntilChunk(chunk_id) + in_chunk_offset;
                auto end_idx =
                    pk_column->GetNumRowsUntilChunk(last_chunk_id_found) +
                    last_in_chunk_offset;

                bitset.set(start_idx, end_idx - start_idx + 1, true);
                last_chunk_id = last_chunk_id_found;
            }

            while (pk_idx < sorted_pks.size() &&
                   sorted_pks[pk_idx] == target_pk) {
                pk_idx++;
            }
        }
    }

    // Binary search to find lower_bound of pk in pk_column starting from from_chunk_id
    // Returns: (chunk_id, in_chunk_offset, exists)
    //   - chunk_id: the chunk containing the first value >= pk
    //   - in_chunk_offset: offset of the first value >= pk in that chunk
    //   - exists: true if found an exact match (value == pk), false otherwise
    //   - If pk doesn't exist, returns the position of first value > pk with exists=false
    //   - If pk is greater than all values, returns {-1, -1, false}
    template <typename PK>
    std::tuple<int, int, bool>
    pk_lower_bound(const PK& pk,
                   const ChunkedColumnInterface* pk_column,
                   const std::vector<PinWrapper<Chunk*>>& all_chunk_pins,
                   int from_chunk_id = 0) const {
        const auto num_chunk = pk_column->num_chunks();

        if (from_chunk_id >= num_chunk) {
            return {-1, -1, false};  // Invalid starting chunk
        }

        using PKViewType = std::conditional_t<std::is_same_v<PK, int64_t>,
                                              int64_t,
                                              std::string_view>;

        auto get_val_view = [&](int chunk_id,
                                int in_chunk_offset) -> PKViewType {
            auto& pw = all_chunk_pins[chunk_id];
            if constexpr (std::is_same_v<PK, int64_t>) {
                auto src =
                    reinterpret_cast<const int64_t*>(pw.get()->RawData());
                return src[in_chunk_offset];
            } else {
                auto string_chunk = static_cast<StringChunk*>(pw.get());
                return string_chunk->operator[](in_chunk_offset);
            }
        };

        // Binary search at chunk level to find the first chunk that might contain pk
        int left_chunk_id = from_chunk_id;
        int right_chunk_id = num_chunk - 1;
        int target_chunk_id = -1;

        while (left_chunk_id <= right_chunk_id) {
            int mid_chunk_id =
                left_chunk_id + (right_chunk_id - left_chunk_id) / 2;
            auto chunk_row_num = pk_column->chunk_row_nums(mid_chunk_id);

            PKViewType min_val = get_val_view(mid_chunk_id, 0);
            PKViewType max_val = get_val_view(mid_chunk_id, chunk_row_num - 1);

            if (pk >= min_val && pk <= max_val) {
                // pk might be in this chunk
                target_chunk_id = mid_chunk_id;
                break;
            } else if (pk < min_val) {
                // pk is before this chunk, could be in an earlier chunk
                target_chunk_id = mid_chunk_id;  // This chunk has values >= pk
                right_chunk_id = mid_chunk_id - 1;
            } else {
                // pk is after this chunk, search in later chunks
                left_chunk_id = mid_chunk_id + 1;
            }
        }

        // If no suitable chunk found, check if we need the first position after all chunks
        if (target_chunk_id == -1) {
            if (left_chunk_id >= num_chunk) {
                // pk is greater than all values
                return {-1, -1, false};
            }
            target_chunk_id = left_chunk_id;
        }

        // Binary search within the target chunk to find lower_bound position
        auto chunk_row_num = pk_column->chunk_row_nums(target_chunk_id);
        int left_offset = 0;
        int right_offset = chunk_row_num;

        while (left_offset < right_offset) {
            int mid_offset = left_offset + (right_offset - left_offset) / 2;
            PKViewType mid_val = get_val_view(target_chunk_id, mid_offset);

            if (mid_val < pk) {
                left_offset = mid_offset + 1;
            } else {
                right_offset = mid_offset;
            }
        }

        // Check if we found a valid position
        if (left_offset < chunk_row_num) {
            // Found position within current chunk
            PKViewType found_val = get_val_view(target_chunk_id, left_offset);
            bool exact_match = (found_val == pk);
            return {target_chunk_id, left_offset, exact_match};
        } else {
            // Position is beyond current chunk, try next chunk
            if (target_chunk_id + 1 < num_chunk) {
                // Next chunk exists, return its first position
                // Check if the first value in next chunk equals pk
                PKViewType next_val = get_val_view(target_chunk_id + 1, 0);
                bool exact_match = (next_val == pk);
                return {target_chunk_id + 1, 0, exact_match};
            } else {
                // No more chunks, pk is greater than all values
                return {-1, -1, false};
            }
        }
    }

    // Find the last occurrence position of pk starting from a known first occurrence
    // Parameters:
    //   - pk: the primary key to search for
    //   - pk_column: the primary key column
    //   - first_chunk_id: chunk id of the first occurrence (from pk_lower_bound)
    //   - first_in_chunk_offset: offset in chunk of the first occurrence (from pk_lower_bound)
    // Returns: (last_chunk_id, last_in_chunk_offset)
    //   - The position of the last occurrence of pk
    // Note: This function assumes pk exists and linearly scans forward.
    //       It's efficient when pk has few duplicates.
    template <typename PK>
    std::tuple<int, int>
    find_last_pk_position(const PK& pk,
                          const ChunkedColumnInterface* pk_column,
                          const std::vector<PinWrapper<Chunk*>>& all_chunk_pins,
                          int first_chunk_id,
                          int first_in_chunk_offset) const {
        const auto num_chunk = pk_column->num_chunks();

        using PKViewType = std::conditional_t<std::is_same_v<PK, int64_t>,
                                              int64_t,
                                              std::string_view>;

        auto get_val_view = [&](int chunk_id,
                                int in_chunk_offset) -> PKViewType {
            auto pw = all_chunk_pins[chunk_id];
            if constexpr (std::is_same_v<PK, int64_t>) {
                auto src =
                    reinterpret_cast<const int64_t*>(pw.get()->RawData());
                return src[in_chunk_offset];
            } else {
                auto string_chunk = static_cast<StringChunk*>(pw.get());
                return string_chunk->operator[](in_chunk_offset);
            }
        };

        int last_chunk_id = first_chunk_id;
        int last_offset = first_in_chunk_offset;

        // Linear scan forward in current chunk
        auto chunk_row_num = pk_column->chunk_row_nums(first_chunk_id);
        for (int offset = first_in_chunk_offset + 1; offset < chunk_row_num;
             offset++) {
            PKViewType curr_val = get_val_view(first_chunk_id, offset);
            if (curr_val == pk) {
                last_offset = offset;
            } else {
                // Found first value != pk, done
                return {last_chunk_id, last_offset};
            }
        }

        // Continue scanning in subsequent chunks
        for (int chunk_id = first_chunk_id + 1; chunk_id < num_chunk;
             chunk_id++) {
            auto curr_chunk_row_num = pk_column->chunk_row_nums(chunk_id);

            // Check first value in this chunk
            PKViewType first_val = get_val_view(chunk_id, 0);
            if (first_val != pk) {
                // This chunk doesn't contain pk anymore
                return {last_chunk_id, last_offset};
            }

            // Update last position and scan this chunk
            last_chunk_id = chunk_id;
            last_offset = 0;

            for (int offset = 1; offset < curr_chunk_row_num; offset++) {
                PKViewType curr_val = get_val_view(chunk_id, offset);
                if (curr_val == pk) {
                    last_offset = offset;
                } else {
                    // Found first value != pk
                    return {last_chunk_id, last_offset};
                }
            }
            // All values in this chunk equal pk, continue to next chunk
        }

        // Scanned all chunks, return last found position
        return {last_chunk_id, last_offset};
    }

    template <typename S, typename T = S>
    static void
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        const void* src_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        T* dst_raw);

    template <typename S, typename T = S>
    static void
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        ChunkedColumnInterface* field,
                        const int64_t* seg_offsets,
                        int64_t count,
                        T* dst_raw,
                        bool small_int_raw_type = false);

    static void
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        int64_t element_sizeof,
                        ChunkedColumnInterface* field,
                        const int64_t* seg_offsets,
                        int64_t count,
                        void* dst_raw);

    template <typename S>
    static void
    bulk_subscript_ptr_impl(
        milvus::OpContext* op_ctx,
        ChunkedColumnInterface* field,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<std::string>* dst_raw);

    template <typename S, typename T = S>
    static void
    bulk_subscript_ptr_impl(milvus::OpContext* op_ctx,
                            const ChunkedColumnInterface* field,
                            const int64_t* seg_offsets,
                            int64_t count,
                            T* dst);

    template <typename T>
    static void
    bulk_subscript_array_impl(milvus::OpContext* op_ctx,
                              ChunkedColumnInterface* column,
                              const int64_t* seg_offsets,
                              int64_t count,
                              google::protobuf::RepeatedPtrField<T>* dst);

    template <typename T>
    static void
    bulk_subscript_vector_array_impl(
        milvus::OpContext* op_ctx,
        const ChunkedColumnInterface* column,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<T>* dst);

    // TEXT column handling for StorageV2 (LOB reference resolution)
    void
    bulk_subscript_text_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        const ChunkedColumnInterface* column,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<std::string>* dst) const;

    std::unique_ptr<DataArray>
    fill_with_empty(FieldId field_id,
                    int64_t count,
                    int64_t valid_count = 0,
                    const void* valid_data = nullptr) const;

    std::unique_ptr<DataArray>
    get_raw_data(milvus::OpContext* op_ctx,
                 FieldId field_id,
                 const FieldMeta& field_meta,
                 const int64_t* seg_offsets,
                 int64_t count) const;

    struct ValidResult {
        int64_t valid_count = 0;
        std::unique_ptr<bool[]> valid_data;
        std::vector<int64_t> valid_offsets;
    };

    ValidResult
    FilterVectorValidOffsetsFromIndex(milvus::OpContext* op_ctx,
                                      const SealedIndexingEntry& entry,
                                      const int64_t* seg_offsets,
                                      int64_t count) const;

    ValidResult
    FilterVectorValidOffsetsFromColumn(milvus::OpContext* op_ctx,
                                       const ChunkedColumnInterface* column,
                                       const int64_t* seg_offsets,
                                       int64_t count) const;

    void
    update_row_count(RuntimeResourceState& runtime, int64_t row_count) {
        runtime.row_count = row_count;
        deleted_record_.set_sealed_row_count(row_count);
    }

    void
    mask_with_timestamps(BitsetTypeView& bitset_chunk,
                         Timestamp timestamp,
                         Timestamp collection_ttl) const override;

    void
    vector_search(SearchInfo& search_info,
                  const void* query_data,
                  const size_t* query_offsets,
                  int64_t query_count,
                  Timestamp timestamp,
                  const BitsetView& bitset,
                  milvus::OpContext* op_context,
                  SearchResult& output) const override;

    void
    mask_with_delete(BitsetTypeView& bitset,
                     int64_t ins_barrier,
                     Timestamp timestamp) const override;

    bool
    is_system_field_ready() const;

    void
    search_ids(BitsetType& bitset, const IdArray& id_array) const override;

    class StagedStateCommitter;

    void
    LoadVecIndex(LoadIndexInfo& info,
                 const SchemaPtr& schema_snapshot,
                 bool is_replace = false,
                 PublishedSegmentState* staged_state = nullptr,
                 StagedStateCommitter* committer = nullptr);

    void
    LoadScalarIndex(LoadIndexInfo& info,
                    const SchemaPtr& schema_snapshot,
                    bool is_replace = false,
                    RuntimeResourceState* runtime = nullptr,
                    PublishedSegmentState* staged_state = nullptr,
                    StagedStateCommitter* committer = nullptr);

    void
    LoadIndex(LoadIndexInfo& info, bool is_replace);

    void
    LoadIndex(LoadIndexInfo& info,
              const SchemaPtr& schema_snapshot,
              bool is_replace,
              RuntimeResourceState* runtime,
              PublishedSegmentState* staged_state = nullptr,
              StagedStateCommitter* committer = nullptr);

    void
    LoadIndex(LoadIndexInfo& info,
              bool is_replace,
              RuntimeResourceState* runtime,
              PublishedSegmentState* staged_state = nullptr,
              StagedStateCommitter* committer = nullptr);

    bool
    generate_interim_index(
        const FieldId field_id,
        int64_t num_rows,
        const std::shared_ptr<ChunkedColumnInterface>& loaded_column,
        milvus::OpContext* op_ctx,
        StagedStateCommitter* committer = nullptr);

    bool
    IsIndexRefineEnabledLocked(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        const std::shared_ptr<const RuntimeResourceState>& runtime) const;

    void
    prefetch_chunks_locked(milvus::OpContext* op_ctx, FieldId field_id) const;

    void
    fill_empty_field(const FieldMeta& field_meta,
                     const SchemaPtr& schema_snapshot,
                     const SegmentLoadInfo& segment_load_info,
                     RuntimeResourceState& runtime);

    std::string
    resolve_field_data_warmup_policy(
        FieldId field_id,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot,
        const std::string& explicit_warmup_policy = "") const;

    std::string
    resolve_field_data_group_warmup_policy(
        const std::unordered_map<FieldId, FieldMeta>& field_metas,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot,
        const std::string& explicit_warmup_policy = "") const;

    SchemaPtr
    CaptureSchemaSnapshot() const;

    std::shared_ptr<const SegmentLoadInfo>
    CaptureLoadInfoSnapshot() const;

    std::shared_ptr<const PublishedSegmentState>
    CapturePublishedState() const;

    std::shared_ptr<const RuntimeResourceState>
    CaptureRuntimeResourceState() const;

    std::shared_ptr<const PublishedSegmentState>
    BuildPublishedState(const SchemaPtr& schema,
                        const std::shared_ptr<const SegmentLoadInfo>& load_info,
                        Timestamp commit_ts) const;

    std::shared_ptr<PublishedSegmentState>
    ClonePublishedState(
        const std::shared_ptr<const PublishedSegmentState>& current) const;

    std::shared_ptr<RuntimeResourceState>
    CloneMutableRuntimeResourceState() const;

    std::shared_ptr<milvus_storage::api::Reader>
    CaptureReaderSnapshot() const;

    std::shared_ptr<const TimestampData>
    CaptureTimestampSnapshot() const;

    static SealedIndexingEntryPtr
    BuildVectorIndexEntry(const MetricType& metric_type,
                          index::CacheIndexBasePtr indexing);

    static bool
    RuntimeVectorIndexReady(const RuntimeResourceState* runtime,
                            FieldId field_id);

    static SealedIndexingEntryPtr
    GetVectorIndexing(
        const std::shared_ptr<const RuntimeResourceState>& runtime,
        FieldId field_id);

    static SealedIndexingEntryPtr
    EraseVectorIndexing(RuntimeResourceState& runtime, FieldId field_id);

    static void
    DropVectorIndexing(RuntimeResourceState& runtime, FieldId field_id);

    static std::vector<index::CacheIndexBasePtr>
    EraseJsonIndexings(RuntimeResourceState& runtime,
                       FieldId field_id,
                       std::string_view nested_path);

    static index::CacheIndexBasePtr
    EraseJsonNgramIndexing(RuntimeResourceState& runtime,
                           FieldId field_id,
                           std::string_view nested_path);

    static std::vector<index::CacheIndexBasePtr>
    EraseJsonIndexesAtPath(RuntimeResourceState& runtime,
                           FieldId field_id,
                           std::string_view nested_path);

    static bool
    RuntimeJsonNgramIndexReady(const RuntimeResourceState& runtime,
                               FieldId field_id);

    static void
    SyncJsonNgramIndexState(PublishedSegmentState& state,
                            const RuntimeResourceState& runtime,
                            FieldId field_id);

    std::shared_ptr<PublishedSegmentState>
    BuildNextPublishedState(
        const std::shared_ptr<const PublishedSegmentState>& current,
        const StateDelta& delta) const;

    void
    ApplyDeltaToState(PublishedSegmentState& state,
                      const StateDelta& delta) const;

    void
    NormalizePublishedState(PublishedSegmentState& state) const;

    class StagedStateCommitter {
     public:
        StagedStateCommitter(ChunkedSegmentSealedImpl& segment,
                             RuntimeResourceState* runtime,
                             PublishedSegmentState* staged_state)
            : segment_(segment),
              runtime_(runtime),
              staged_state_(staged_state) {
            AssertInfo(runtime_ != nullptr, "staged runtime must not be null");
            AssertInfo(staged_state_ != nullptr,
                       "staged published state must not be null");
        }

        RuntimeResourceState*
        runtime() const {
            return runtime_;
        }

        PublishedSegmentState*
        staged_state() const {
            return staged_state_;
        }

        template <typename Mutator>
        void
        Commit(Mutator&& mutator) {
            std::lock_guard<std::mutex> lock(mutex_);
            mutator(*runtime_, *staged_state_);
            segment_.NormalizePublishedState(*staged_state_);
        }

        bool
        IsVectorIndexReady(FieldId field_id);

        void
        StageVectorIndexMutationLocked(FieldId field_id,
                                       const MetricType& metric_type,
                                       index::CacheIndexBasePtr indexing,
                                       bool drop_existing) {
            if (drop_existing) {
                RetireVectorIndexingLocked(field_id);
                runtime_->vec_binlog_config.erase(field_id);
            }
            runtime_->vector_indexings[field_id] =
                BuildVectorIndexEntry(metric_type, std::move(indexing));
        }

        void
        StageVectorIndexDropLocked(FieldId field_id) {
            RetireVectorIndexingLocked(field_id);
            runtime_->vec_binlog_config.erase(field_id);
        }

        void
        StageInterimVectorIndexMutationLocked(
            FieldId field_id,
            const MetricType& metric_type,
            index::CacheIndexBasePtr indexing,
            std::unique_ptr<VecIndexConfig> binlog_config) {
            runtime_->vector_indexings[field_id] =
                BuildVectorIndexEntry(metric_type, std::move(indexing));
            runtime_->vec_binlog_config[field_id] =
                std::shared_ptr<const VecIndexConfig>(std::move(binlog_config));
        }

        void
        Publish(const std::shared_ptr<const PublishedSegmentState>& current,
                const StateDelta& delta) {
            std::vector<SealedIndexingEntryPtr> retired_indexings;
            std::vector<index::CacheIndexBasePtr> retired_cache_indexings;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                segment_.PublishState(
                    segment_.BuildNextPublishedState(current, delta));
                retired_indexings.swap(retired_vector_indexings_);
                retired_cache_indexings.swap(retired_cache_indexings_);
            }
            for (auto& entry : retired_indexings) {
                if (entry != nullptr && entry->indexing_ != nullptr) {
                    entry->indexing_->CancelWarmup();
                }
            }
            for (auto& indexing : retired_cache_indexings) {
                if (indexing != nullptr) {
                    indexing->CancelWarmup();
                }
            }
        }

        void
        RetireCacheIndexingLocked(index::CacheIndexBasePtr indexing) {
            if (indexing != nullptr) {
                retired_cache_indexings_.push_back(std::move(indexing));
            }
        }

     private:
        void
        RetireVectorIndexingLocked(FieldId field_id) {
            auto entry = EraseVectorIndexing(*runtime_, field_id);
            if (entry != nullptr) {
                retired_vector_indexings_.push_back(std::move(entry));
            }
        }

        ChunkedSegmentSealedImpl& segment_;
        RuntimeResourceState* runtime_;
        PublishedSegmentState* staged_state_;
        std::vector<SealedIndexingEntryPtr> retired_vector_indexings_;
        std::vector<index::CacheIndexBasePtr> retired_cache_indexings_;
        std::mutex mutex_;
    };

    void
    PublishState(const std::shared_ptr<const PublishedSegmentState>& state);

    void
    PublishState(std::shared_ptr<PublishedSegmentState> state);

    std::shared_ptr<index::JsonKeyStats>
    BuildJsonKeyStatsIndex(
        milvus::OpContext* op_ctx,
        const std::shared_ptr<milvus::proto::indexcgo::LoadJsonKeyIndexInfo>&
            info_proto);

    void
    LoadBatchJsonKeyIndexes(
        milvus::OpContext* op_ctx,
        const std::unordered_map<
            FieldId,
            std::shared_ptr<milvus::proto::indexcgo::LoadJsonKeyIndexInfo>>&
            infos,
        const SchemaPtr& schema_snapshot,
        StagedStateCommitter& committer);

    template <typename Mutator>
    void
    MutatePublishedStateLocked(Mutator&& mutator) {
        auto current = CapturePublishedState();
        auto next = ClonePublishedState(current);
        mutator(*next);
        NormalizePublishedState(*next);
        PublishState(std::move(next));
    }

    static bool
    HasIndexRawDataFromState(const PublishedSegmentState& state,
                             FieldId field_id);

    static void
    SetIndexRawDataInState(PublishedSegmentState& state,
                           FieldId field_id,
                           bool has_raw_data);

    static void
    ClearIndexRawDataInState(PublishedSegmentState& state, FieldId field_id);

    static bool
    HasPublishedIndexRawDataFromState(const PublishedSegmentState& state,
                                      FieldId field_id);

    static void
    SetPublishedIndexRawDataInState(PublishedSegmentState& state,
                                    FieldId field_id,
                                    bool has_raw_data);

    static void
    ClearPublishedIndexRawDataInState(PublishedSegmentState& state,
                                      FieldId field_id);

    static void
    ClearFieldBitsForAbsentLoadInfo(PublishedSegmentState& state);

    static void
    ResizeStateBitsets(PublishedSegmentState& state, const Schema& schema);

    void
    SetSystemFieldReadyInState(PublishedSegmentState& state,
                               const SegmentLoadInfo* load_info) const;

    static void
    DropFieldFromState(PublishedSegmentState& state, FieldId field_id);

    static void
    DropIndexFromState(PublishedSegmentState& state, FieldId field_id);

    static void
    ClearState(PublishedSegmentState& state);

    static std::shared_ptr<const SegmentLoadInfo>
    CloneLoadInfoWithDefaultFilled(
        const std::shared_ptr<const SegmentLoadInfo>& current,
        const std::vector<FieldId>& field_ids);

    static std::shared_ptr<const SegmentLoadInfo>
    CloneLoadInfoWithTextIndexCreated(
        const std::shared_ptr<const SegmentLoadInfo>& current,
        FieldId field_id);

    static std::shared_ptr<const SegmentLoadInfo>
    CloneLoadInfoForReopen(const SegmentLoadInfo& load_info,
                           const SchemaPtr& schema_snapshot);

    static StateDelta
    MakeStateDelta(const SchemaPtr& schema_snapshot,
                   const std::shared_ptr<const SegmentLoadInfo>& load_info,
                   Timestamp commit_ts);

    static StateDelta
    MakeStateDelta(const SchemaPtr& schema_snapshot,
                   const std::shared_ptr<const SegmentLoadInfo>& load_info,
                   const std::shared_ptr<const RuntimeResourceState>& runtime,
                   Timestamp commit_ts);

    void
    PublishReopenState(
        const std::shared_ptr<const PublishedSegmentState>& current,
        const StateDelta& delta);

    void
    PublishReopenState(
        const std::shared_ptr<const PublishedSegmentState>& current,
        const SchemaPtr& sch,
        const std::shared_ptr<const SegmentLoadInfo>& published);

    void
    PublishReopenState(const StateDelta& delta);

    void
    PublishReopenState(const SchemaPtr& sch,
                       const std::shared_ptr<const SegmentLoadInfo>& published);

    bool
    HasRawDataFromState(const PublishedSegmentState& state,
                        FieldId field_id) const;

    bool
    IndexHasRawDataFromState(const PublishedSegmentState& state,
                             FieldId field_id) const;

    void
    RecordDefaultFieldsFilledLocked(const std::vector<FieldId>& field_ids);

    void
    RecordDefaultFieldsFilled(const std::vector<FieldId>& field_ids);

    void
    SetUseTakeForOutputForTestingLocked(bool val);

    void
    SetUseTakeForOutputForTestingImpl(bool val);

    void
    MarkSystemFieldReadyLocked(bool value);

    void
    MarkFieldDataReadyLocked(FieldId field_id, bool value);

    void
    MarkIndexReadyLocked(FieldId field_id, bool value);

    void
    MarkBinlogIndexReadyLocked(FieldId field_id, bool value);

    void
    MarkIndexHasRawDataLocked(FieldId field_id, bool has_raw_data);

    void
    ClearIndexHasRawDataLocked(FieldId field_id);

    void
    ResizeStateBitsetsLocked(const SchemaPtr& schema_snapshot);

    void
    ClearPublishedStateLocked();

    void
    PublishSystemFieldStateLocked();

    void
    PublishFieldDataReadyLocked(
        FieldId field_id,
        const std::shared_ptr<const RuntimeResourceState>& runtime = nullptr);

    void
    PublishIndexReadyLocked(
        FieldId field_id,
        bool has_raw_data,
        const std::shared_ptr<const RuntimeResourceState>& runtime = nullptr);

    void
    PublishBinlogIndexReadyLocked(
        FieldId field_id,
        bool has_raw_data,
        const std::shared_ptr<const RuntimeResourceState>& runtime = nullptr);

    void
    PublishVectorIndexFactsLocked(FieldId field_id,
                                  bool ready,
                                  bool binlog_ready,
                                  std::optional<bool> has_raw_data);

    void
    PublishFieldDroppedLocked(
        FieldId field_id,
        const std::shared_ptr<const RuntimeResourceState>& runtime = nullptr);

    void
    PublishIndexDroppedLocked(
        FieldId field_id,
        const std::shared_ptr<const RuntimeResourceState>& runtime = nullptr);

    void
    PublishRuntimeStateLocked(
        const std::shared_ptr<const RuntimeResourceState>& runtime);

    static std::shared_ptr<const RuntimeResourceState>
    ToConstRuntimeState(std::shared_ptr<RuntimeResourceState> runtime);

    void
    RefreshPublishedLoadInfoLocked(
        const std::shared_ptr<const SegmentLoadInfo>& load_info,
        Timestamp commit_ts);

    void
    RefreshPublishedSchemaLocked(const SchemaPtr& schema_snapshot);

    void
    RefreshPublishedStateLocked(
        const SchemaPtr& schema_snapshot,
        const std::shared_ptr<const SegmentLoadInfo>& load_info,
        Timestamp commit_ts);

    void
    PrepareMutableStateForPublish(
        const SchemaPtr& schema_snapshot,
        Timestamp commit_ts,
        std::shared_ptr<PublishedSegmentState>& next) const;

    bool
    IsSystemFieldReadyFromState(const PublishedSegmentState& state,
                                const SegmentLoadInfo* load_info) const;

    void
    EnsureArrayOffsetsForStructField(const FieldMeta& field_meta,
                                     int64_t row_count,
                                     RuntimeResourceState& runtime);

    void
    FillDefaultValueFields(const std::vector<FieldId>& field_ids,
                           const SegmentLoadInfo& segment_load_info,
                           const SchemaPtr& schema_snapshot,
                           RuntimeResourceState* runtime = nullptr,
                           PublishedSegmentState* staged_state = nullptr);

    void
    FillDefaultValueFields(const std::vector<FieldId>& field_ids,
                           const SegmentLoadInfo& segment_load_info,
                           const SchemaPtr& schema_snapshot,
                           StagedStateCommitter& committer);

    void
    FillDefaultValueFields(const std::vector<FieldId>& field_ids);

    void
    DropIndex(const FieldId field_id,
              const SchemaPtr& schema_snapshot,
              RuntimeResourceState* runtime = nullptr);

    void
    DropFieldData(
        const FieldId field_id,
        const SchemaPtr& schema_snapshot,
        RuntimeResourceState* runtime = nullptr,
        const std::shared_ptr<const PublishedSegmentState>& current = nullptr);

    void
    LoadFieldData(const LoadFieldDataInfo& load_info,
                  const SegmentLoadInfo& segment_load_info,
                  milvus::OpContext* op_ctx,
                  bool is_replace,
                  const SchemaPtr& schema_snapshot,
                  RuntimeResourceState* runtime = nullptr);

    void
    LoadFieldData(const LoadFieldDataInfo& load_info,
                  const SegmentLoadInfo& segment_load_info,
                  milvus::OpContext* op_ctx,
                  bool is_replace,
                  const SchemaPtr& schema_snapshot,
                  StagedStateCommitter& committer);

    void
    LoadFieldData(const LoadFieldDataInfo& load_info,
                  milvus::OpContext* op_ctx,
                  bool is_replace);

    void
    load_field_data_internal(const LoadFieldDataInfo& load_info,
                             const SegmentLoadInfo& segment_load_info,
                             const SchemaPtr& schema_snapshot,
                             milvus::OpContext* op_ctx = nullptr,
                             bool is_replace = false,
                             RuntimeResourceState* runtime = nullptr);

    void
    load_field_data_internal(const LoadFieldDataInfo& load_info,
                             const SegmentLoadInfo& segment_load_info,
                             const SchemaPtr& schema_snapshot,
                             milvus::OpContext* op_ctx,
                             bool is_replace,
                             StagedStateCommitter& committer);

    void
    load_column_group_data_internal(const LoadFieldDataInfo& load_info,
                                    const SegmentLoadInfo& segment_load_info,
                                    const SchemaPtr& schema_snapshot,
                                    milvus::OpContext* op_ctx = nullptr,
                                    bool is_replace = false,
                                    RuntimeResourceState* runtime = nullptr);

    void
    load_column_group_data_internal(const LoadFieldDataInfo& load_info,
                                    const SegmentLoadInfo& segment_load_info,
                                    const SchemaPtr& schema_snapshot,
                                    milvus::OpContext* op_ctx,
                                    bool is_replace,
                                    StagedStateCommitter& committer);

    void
    LoadBatchIndexes(milvus::tracer::TraceContext& trace_ctx,
                     std::unordered_map<FieldId, std::vector<LoadIndexInfo>>&
                         field_id_to_index_info,
                     const SchemaPtr& schema_snapshot,
                     milvus::OpContext* op_ctx,
                     bool is_replace,
                     StagedStateCommitter& committer);

    void
    LoadBatchFieldData(milvus::tracer::TraceContext& trace_ctx,
                       std::vector<std::pair<std::vector<FieldId>,
                                             proto::segcore::FieldBinlog>>&
                           field_binlog_to_load,
                       const SegmentLoadInfo& segment_load_info,
                       const SchemaPtr& schema_snapshot,
                       milvus::OpContext* op_ctx = nullptr,
                       bool is_replace = false,
                       StagedStateCommitter* committer = nullptr);

    void
    LoadBatchFieldData(milvus::tracer::TraceContext& trace_ctx,
                       std::vector<std::pair<std::vector<FieldId>,
                                             proto::segcore::FieldBinlog>>&
                           field_binlog_to_load,
                       milvus::OpContext* op_ctx = nullptr,
                       bool is_replace = false);

    void
    InitTextLobPaths(const std::string& manifest_path,
                     const SchemaPtr& schema_snapshot,
                     RuntimeResourceState* runtime);

    void
    InitTextLobPaths(const std::string& manifest_path,
                     const SchemaPtr& schema_snapshot,
                     StagedStateCommitter& committer);

    void
    SynthesizeExternalSystemFields(const SegmentLoadInfo& segment_load_info,
                                   const SchemaPtr& schema_snapshot,
                                   RuntimeResourceState* runtime);

    void
    SynthesizeExternalSystemFields(RuntimeResourceState* runtime);

    void
    LoadColumnGroups(
        const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        std::vector<std::pair<int, std::vector<FieldId>>>& cg_field_ids,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot,
        bool eager_load,
        milvus::OpContext* op_ctx,
        bool is_replace,
        StagedStateCommitter& committer);

    // Load external collection column groups from staged segment load info.
    void
    LoadColumnGroups(const SegmentLoadInfo& segment_load_info,
                     const SchemaPtr& schema_snapshot,
                     milvus::OpContext* op_ctx,
                     StagedStateCommitter& committer);

    void
    LoadColumnGroup(
        const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        int64_t index,
        const std::vector<FieldId>& milvus_field_ids,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot,
        bool eager_load,
        milvus::OpContext* op_ctx = nullptr,
        bool is_replace = false,
        RuntimeResourceState* runtime = nullptr);

    void
    LoadColumnGroup(
        const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        int64_t index,
        const std::vector<FieldId>& milvus_field_ids,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot,
        bool eager_load,
        milvus::OpContext* op_ctx,
        bool is_replace,
        StagedStateCommitter& committer);

    void
    LoadColumnGroup(
        const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        int64_t index,
        const std::vector<FieldId>& milvus_field_ids,
        bool eager_load,
        milvus::OpContext* op_ctx = nullptr,
        bool is_replace = false);

    void
    ReloadColumns(const std::vector<FieldId>& field_ids_to_reload,
                  milvus::OpContext* op_ctx = nullptr);

    void
    LoadBatchTextIndexes(
        milvus::OpContext* op_ctx,
        std::unordered_map<FieldId,
                           std::shared_ptr<proto::indexcgo::LoadTextIndexInfo>>&
            text_indexes_to_load,
        const SchemaPtr& schema_snapshot,
        const SegmentLoadInfo& segment_load_info,
        StagedStateCommitter& committer);

    void
    CreateTextIndexWithSchema(FieldId field_id,
                              const SchemaPtr& schema_snapshot,
                              milvus::OpContext* op_ctx = nullptr,
                              bool publish_marker = true,
                              RuntimeResourceState* runtime = nullptr);

    void
    CreateTextIndexWithSchema(FieldId field_id,
                              const SchemaPtr& schema_snapshot,
                              milvus::OpContext* op_ctx,
                              StagedStateCommitter& committer);

    class ScopedTextIndexBuildGuard {
     public:
        ScopedTextIndexBuildGuard(ChunkedSegmentSealedImpl& segment,
                                  FieldId field_id)
            : segment_(segment), field_id_(field_id) {
        }

        void
        Register();

        void
        Commit();

        ~ScopedTextIndexBuildGuard();

     private:
        ChunkedSegmentSealedImpl& segment_;
        FieldId field_id_;
        bool registered_{false};
        bool committed_{false};
    };

    TextIndexVariant
    BuildTextIndexFromFiles(
        milvus::OpContext* op_ctx,
        const std::shared_ptr<proto::indexcgo::LoadTextIndexInfo>& info_proto,
        const SegmentLoadInfo& segment_load_info);

    void
    RecordTextIndexCreated(SegmentLoadInfo& segment_load_info,
                           FieldId field_id);

    void
    PrepareSchemaForReopen(const SchemaPtr& sch);

    void
    PrepareLoadDiffForReopen(milvus::OpContext* op_ctx,
                             SegmentLoadInfo& segment_load_info,
                             LoadDiff& load_diff,
                             const SchemaPtr& schema_snapshot,
                             StagedStateCommitter& committer);

    void
    FinalizeLoadDiffForReopen(milvus::OpContext* op_ctx,
                              SegmentLoadInfo& segment_load_info,
                              LoadDiff& load_diff,
                              const SchemaPtr& schema_snapshot,
                              StagedStateCommitter& committer);

    void
    ApplyLoadDiff(milvus::OpContext* op_ctx,
                  SegmentLoadInfo& segment_load_info,
                  LoadDiff& load_diff,
                  const SchemaPtr& schema_snapshot);

    void
    ApplyLoadDiff(milvus::OpContext* op_ctx,
                  SegmentLoadInfo& segment_load_info,
                  LoadDiff& load_diff);

    void
    Reopen(milvus::OpContext* op_ctx, SchemaPtr sch);

    void
    ApplySchemaForReopen(SchemaPtr sch);

    void
    load_field_data_common(
        FieldId field_id,
        const std::shared_ptr<ChunkedColumnInterface>& column,
        size_t num_rows,
        DataType data_type,
        bool enable_mmap,
        bool is_proxy_column,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot,
        RuntimeResourceState* runtime,
        std::optional<ParquetStatistics> statistics = {},
        milvus::OpContext* op_ctx = nullptr,
        bool is_replace = false,
        StagedStateCommitter* committer = nullptr);

    void
    CompactRuntimeLoadInfoForManifest();

    void
    load_field_data_common(
        FieldId field_id,
        const std::shared_ptr<ChunkedColumnInterface>& column,
        size_t num_rows,
        DataType data_type,
        bool enable_mmap,
        bool is_proxy_column,
        std::optional<ParquetStatistics> statistics = {},
        milvus::OpContext* op_ctx = nullptr,
        bool is_replace = false);

    std::shared_ptr<ChunkedColumnInterface>
    get_column(const std::shared_ptr<const RuntimeResourceState>& runtime,
               FieldId field_id) const {
        if (runtime == nullptr) {
            return nullptr;
        }
        auto it = runtime->fields.find(field_id);
        if (it != runtime->fields.end()) {
            return it->second;
        }
        return nullptr;
    }

    std::shared_ptr<ChunkedColumnInterface>
    get_column(FieldId field_id) const {
        return get_column(CaptureRuntimeResourceState(), field_id);
    }

    PinWrapper<const storagev2translator::TimestampIndexCell*>
    PinTimestampIndex(milvus::OpContext* op_ctx) const;

    PinWrapper<const storagev2translator::TimestampIndexCell*>
    PinTimestampIndex(
        const std::shared_ptr<const RuntimeResourceState>& runtime,
        milvus::OpContext* op_ctx) const;

    Timestamp
    ReadTimestamp(int64_t offset,
                  const std::shared_ptr<const RuntimeResourceState>& runtime,
                  std::optional<Timestamp> effective_commit_ts) const;

    PinWrapper<const storagev2translator::PkIndexCell*>
    PinPkIndex(const std::shared_ptr<const RuntimeResourceState>& runtime,
               milvus::OpContext* op_ctx) const;

    void
    init_storage_v2_timestamp_index(
        const std::shared_ptr<ChunkedColumnInterface>& column,
        size_t num_rows,
        const std::string& warmup_policy = "");

    std::shared_ptr<CacheSlot<storagev2translator::PkIndexCell>>
    BuildPkIndexSlot(const std::shared_ptr<ChunkedColumnInterface>& column,
                     DataType data_type,
                     bool eager,
                     milvus::OpContext* op_ctx) const;

 private:
    std::unique_ptr<SpanBase>
    chunk_data_impl_internal(FieldId field_id,
                             int64_t chunk_id,
                             int64_t start,
                             int64_t length) const;

    // deleted pks
    mutable DeletedRecord<true> deleted_record_;

    LoadFieldDataInfo field_data_info_;

    // Reader-visible published segment state. Readers MUST use atomic_load to
    // capture a single consistent snapshot per call. Published snapshots are
    // immutable; writers clone, mutate, normalize, then atomic_store a fully
    // built next state.
    std::shared_ptr<const PublishedSegmentState> published_state_;

    // Serializes all published_state_ writers so reopen/load/runtime-marker
    // copy-on-write publications never interleave. Does NOT block readers —
    // reader paths access published_state_ via atomic_load.
    // Lock order: reopen_mutex_ → mutex_ (outer → inner) only. Never inverse,
    // and reader paths that take mutex_ must not take reopen_mutex_.
    std::mutex reopen_mutex_;

    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
    int64_t id_;
    // commit_ts_ remains the writer-side source of truth for load-time data
    // preparation; published readers consume the value through
    // PublishedSegmentState.
    uint64_t commit_ts_{0};

    std::unordered_set<FieldId> pending_text_index_fields_;

    // only useful in binlog
    IndexMetaPtr col_index_meta_;
    SegcoreConfig segcore_config_;

    SegmentStats stats_{};

    // whether the segment is sorted by the pk
    // 1. will skip index loading for primary key field
    bool is_sorted_by_pk_ = false;

    // Query-time reader calls remain non-thread-safe and must be serialized.
    // The reader object itself now lives in RuntimeResourceState snapshots so
    // reopen/load can stage a replacement reader without exposing it early.
    mutable std::mutex reader_mutex_;

#ifdef MILVUS_UNIT_TEST
 public:
    // Test-only: inject a mock Reader for unit testing take() paths.
    void
    SetReaderForTesting(std::unique_ptr<milvus_storage::api::Reader> r) {
        auto runtime = CloneMutableRuntimeResourceState();
        runtime->reader =
            std::shared_ptr<milvus_storage::api::Reader>(std::move(r));
        PublishRuntimeStateLocked(ToConstRuntimeState(std::move(runtime)));
    }

    // Wrappers for protected methods to enable direct unit testing.
    void
    TestFillTargetEntry(const query::Plan* plan, SearchResult& results) const {
        FillTargetEntry(plan, results);
    }

    bool
    TestFieldAccessible(FieldId field_id) const {
        return FieldAccessible(field_id);
    }

    std::shared_ptr<const IArrayOffsets>
    TestGetArrayOffsets(FieldId field_id) const {
        return GetArrayOffsets(field_id);
    }

    std::shared_ptr<const SegmentLoadInfo>
    TestGetLoadInfoSnapshot() const {
        return CapturePublishedState()->load_info;
    }

    SchemaPtr
    TestGetSchemaSnapshot() const {
        return CapturePublishedState()->schema;
    }

    std::shared_ptr<const PublishedSegmentState>
    TestGetPublishedStateSnapshot() const {
        return CapturePublishedState();
    }

    std::string
    TestResolveFieldDataWarmupPolicy(
        FieldId field_id,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot,
        const std::string& explicit_warmup_policy = "") const {
        return resolve_field_data_warmup_policy(field_id,
                                                segment_load_info,
                                                schema_snapshot,
                                                explicit_warmup_policy);
    }

    std::string
    TestResolveFieldDataGroupWarmupPolicy(
        const std::vector<FieldId>& field_ids,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot) const {
        return resolve_field_data_group_warmup_policy(
            schema_snapshot->get_field_metas(field_ids),
            segment_load_info,
            schema_snapshot);
    }

    std::shared_ptr<ChunkedColumnInterface>
    TestStageLoadColumnGroupWithReader(
        const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        int64_t index,
        const std::vector<FieldId>& field_ids,
        const SegmentLoadInfo& segment_load_info,
        const SchemaPtr& schema_snapshot,
        std::shared_ptr<milvus_storage::api::Reader> reader,
        bool eager_load = true) {
        auto current = CapturePublishedState();
        auto runtime = CloneMutableRuntimeResourceState();
        runtime->reader = std::move(reader);
        auto staged = ClonePublishedState(current);
        staged->schema = schema_snapshot;
        staged->load_info =
            std::make_shared<const SegmentLoadInfo>(segment_load_info);
        staged->runtime = ToConstRuntimeState(runtime);
        staged->commit_ts = current->commit_ts;
        NormalizePublishedState(*staged);

        StagedStateCommitter committer(*this, runtime.get(), staged.get());
        LoadColumnGroup(column_groups,
                        properties,
                        index,
                        field_ids,
                        segment_load_info,
                        schema_snapshot,
                        eager_load,
                        nullptr,
                        false,
                        committer);

        auto it = runtime->fields.find(field_ids.front());
        AssertInfo(it != runtime->fields.end(), "test field was not loaded");
        return it->second;
    }

    void
    TestPublishRuntimeResourceState(
        std::shared_ptr<RuntimeResourceState> runtime) {
        std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
        PublishRuntimeStateLocked(ToConstRuntimeState(std::move(runtime)));
    }

    void
    TestPublishSystemFieldState() {
        PublishSystemFieldStateLocked();
    }

    std::shared_ptr<PublishedSegmentState>
    TestBuildNextPublishedState(
        const std::shared_ptr<const PublishedSegmentState>& current,
        const StateDelta& delta) const {
        return BuildNextPublishedState(current, delta);
    }

    std::shared_ptr<RuntimeResourceState>
    TestCloneMutableRuntimeResourceState() const {
        return CloneMutableRuntimeResourceState();
    }

    std::shared_ptr<const RuntimeResourceState>
    TestFreezeRuntimeResourceState(
        std::shared_ptr<RuntimeResourceState> runtime) const {
        return ToConstRuntimeState(std::move(runtime));
    }

    void
    TestLoadIndex(LoadIndexInfo& info,
                  bool is_replace,
                  RuntimeResourceState* runtime) {
        LoadIndex(info, is_replace, runtime);
    }

    void
    TestLoadIndex(LoadIndexInfo& info,
                  bool is_replace,
                  const SchemaPtr& schema_snapshot,
                  RuntimeResourceState* runtime,
                  PublishedSegmentState* staged_state,
                  StagedStateCommitter* committer = nullptr) {
        LoadIndex(info,
                  schema_snapshot,
                  is_replace,
                  runtime,
                  staged_state,
                  committer);
    }

    template <typename Verifier>
    void
    TestStageLoadIndexThenPublish(
        LoadIndexInfo& info,
        bool is_replace,
        const SchemaPtr& schema_snapshot,
        std::shared_ptr<RuntimeResourceState> runtime,
        PublishedSegmentState* staged_state,
        const std::shared_ptr<const PublishedSegmentState>& current,
        StateDelta& final_delta,
        Verifier&& verifier) {
        StagedStateCommitter committer(*this, runtime.get(), staged_state);
        committer.Commit([&](RuntimeResourceState& staged_runtime,
                             PublishedSegmentState& staged) {
            LoadIndex(info,
                      schema_snapshot,
                      is_replace,
                      &staged_runtime,
                      &staged,
                      &committer);
        });
        verifier();
        final_delta.runtime = ToConstRuntimeState(std::move(runtime));
        final_delta.published_index_ready_bitset =
            staged_state->published_index_ready_bitset.clone();
        final_delta.published_binlog_index_ready_bitset =
            staged_state->published_binlog_index_ready_bitset.clone();
        final_delta.published_index_has_raw_data =
            staged_state->published_index_has_raw_data;
        committer.Publish(current, final_delta);
    }

    template <typename Verifier>
    void
    TestStageLoadFieldDataThenPublish(
        FieldId field_id,
        const std::shared_ptr<ChunkedColumnInterface>& column,
        size_t num_rows,
        DataType data_type,
        const SchemaPtr& schema_snapshot,
        std::shared_ptr<RuntimeResourceState> runtime,
        PublishedSegmentState* staged_state,
        const std::shared_ptr<const PublishedSegmentState>& current,
        StateDelta& final_delta,
        Verifier&& verifier) {
        StagedStateCommitter committer(*this, runtime.get(), staged_state);
        load_field_data_common(field_id,
                               column,
                               num_rows,
                               data_type,
                               /*enable_mmap=*/false,
                               /*is_proxy_column=*/false,
                               *current->load_info,
                               schema_snapshot,
                               runtime.get(),
                               std::nullopt,
                               nullptr,
                               /*is_replace=*/true,
                               &committer);
        verifier();
        final_delta.runtime = ToConstRuntimeState(std::move(runtime));
        committer.Publish(current, final_delta);
    }

    template <typename Verifier>
    void
    TestStageLoadIndexGenerateInterimThenPublish(
        LoadIndexInfo& info,
        bool is_replace,
        const SchemaPtr& schema_snapshot,
        std::shared_ptr<RuntimeResourceState> runtime,
        PublishedSegmentState* staged_state,
        const std::shared_ptr<const PublishedSegmentState>& current,
        StateDelta& final_delta,
        const std::shared_ptr<ChunkedColumnInterface>& loaded_column,
        int64_t num_rows,
        milvus::OpContext* op_ctx,
        Verifier&& verifier) {
        StagedStateCommitter committer(*this, runtime.get(), staged_state);
        committer.Commit([&](RuntimeResourceState& staged_runtime,
                             PublishedSegmentState& staged) {
            LoadIndex(info,
                      schema_snapshot,
                      is_replace,
                      &staged_runtime,
                      &staged,
                      &committer);
        });
        auto generated_interim = generate_interim_index(FieldId(info.field_id),
                                                        num_rows,
                                                        loaded_column,
                                                        op_ctx,
                                                        &committer);
        verifier(generated_interim);
        final_delta.runtime = ToConstRuntimeState(std::move(runtime));
        final_delta.published_index_ready_bitset =
            staged_state->published_index_ready_bitset.clone();
        final_delta.published_binlog_index_ready_bitset =
            staged_state->published_binlog_index_ready_bitset.clone();
        final_delta.published_index_has_raw_data =
            staged_state->published_index_has_raw_data;
        committer.Publish(current, final_delta);
    }

    bool
    TestGenerateInterimIndex(
        FieldId field_id,
        int64_t num_rows,
        const std::shared_ptr<ChunkedColumnInterface>& loaded_column,
        milvus::OpContext* op_ctx = nullptr) {
        return generate_interim_index(
            field_id, num_rows, loaded_column, op_ctx, nullptr);
    }

    bool
    TestVectorIndexReady(FieldId field_id) const {
        auto runtime = CaptureRuntimeResourceState();
        return GetVectorIndexing(runtime, field_id) != nullptr;
    }

    void
    TestPublishVectorIndexFacts(FieldId field_id,
                                bool ready,
                                bool binlog_ready,
                                std::optional<bool> has_raw_data) {
        PublishVectorIndexFactsLocked(
            field_id, ready, binlog_ready, has_raw_data);
    }

    void
    TestDropFieldFromState(PublishedSegmentState& state,
                           FieldId field_id) const {
        DropFieldFromState(state, field_id);
    }

    void
    TestDropIndexFromState(PublishedSegmentState& state,
                           FieldId field_id) const {
        DropIndexFromState(state, field_id);
    }

    bool
    TestTryTakeForSearch(const query::Plan* plan,
                         const int64_t* seg_offsets,
                         int64_t size,
                         SearchResult& results) const {
        return TryTakeForSearch(plan, seg_offsets, size, results);
    }

    void
    SetUseTakeForOutputForTesting(bool val) {
        SetUseTakeForOutputForTestingImpl(val);
    }

    void
    SetTextLobPathForTesting(FieldId field_id, std::string lob_base_path) {
        std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
        auto current = CapturePublishedState();
        auto next = ClonePublishedState(current);
        auto runtime = CloneRuntimeResourceState(current->runtime);
        runtime->text_lob_paths[field_id] = std::move(lob_base_path);
        next->runtime = ToConstRuntimeState(std::move(runtime));
        PublishState(std::move(next));
    }

    void
    SetJsonStatsForTesting(FieldId field_id,
                           std::shared_ptr<index::JsonKeyStats> stats) {
        std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
        auto current = CapturePublishedState();
        auto next = ClonePublishedState(current);
        auto runtime = CloneRuntimeResourceState(current->runtime);
        runtime->json_stats[field_id] = std::move(stats);
        next->runtime = ToConstRuntimeState(std::move(runtime));
        NormalizePublishedState(*next);
        PublishState(std::move(next));
    }

    std::shared_ptr<const SegmentLoadInfo>
    TestGetSegmentLoadInfo() {
        return CapturePublishedState()->load_info;
    }

    void
    TestRecordTextIndexCreated(FieldId field_id) {
        std::lock_guard<std::mutex> reopen_guard(reopen_mutex_);
        MutatePublishedStateLocked([&](PublishedSegmentState& state) {
            state.load_info =
                CloneLoadInfoWithTextIndexCreated(state.load_info, field_id);
            state.use_take_for_output = state.load_info != nullptr &&
                                        state.load_info->GetUseTakeForOutput();
            ClearFieldBitsForAbsentLoadInfo(state);
            SetSystemFieldReadyInState(state, state.load_info.get());
        });
    }

    bool
    TestHasPendingTextIndex(FieldId field_id) const {
        return pending_text_index_fields_.count(field_id) > 0;
    }

    void
    TestRegisterPendingTextIndex(FieldId field_id) {
        ScopedTextIndexBuildGuard guard(*this, field_id);
        guard.Register();
    }

    void
    TestCreateTextIndexWithSchema(FieldId field_id,
                                  const SchemaPtr& schema_snapshot,
                                  milvus::OpContext* op_ctx = nullptr,
                                  bool publish_marker = true,
                                  RuntimeResourceState* runtime = nullptr) {
        CreateTextIndexWithSchema(
            field_id, schema_snapshot, op_ctx, publish_marker, runtime);
    }

    std::pair<std::shared_ptr<ChunkedColumnInterface>, bool>
    TestGetFieldDataIfExist(FieldId field_id) const {
        return GetFieldDataIfExist(field_id);
    }

    void
    TestClearPublishedState() {
        ClearPublishedStateLocked();
    }

    void
    TestSynthesizeExternalSystemFields(const SegmentLoadInfo& segment_load_info,
                                       const SchemaPtr& schema_snapshot,
                                       RuntimeResourceState* runtime) {
        SynthesizeExternalSystemFields(
            segment_load_info, schema_snapshot, runtime);
    }

    void
    TestSynthesizeExternalSystemFields(RuntimeResourceState* runtime) {
        SynthesizeExternalSystemFields(runtime);
    }
#endif
};

inline SegmentSealedUPtr
CreateSealedSegment(
    SchemaPtr schema,
    IndexMetaPtr index_meta = empty_index_meta,
    int64_t segment_id = 0,
    const SegcoreConfig& segcore_config = SegcoreConfig::default_config(),
    bool is_sorted_by_pk = false) {
    return std::make_unique<ChunkedSegmentSealedImpl>(
        schema, index_meta, segcore_config, segment_id, is_sorted_by_pk);
}

using ParquetStatisticsByField =
    std::map<int64_t, ChunkedSegmentSealedImpl::ParquetStatistics>;

struct LoadedGroupChunkMetadata {
    std::vector<milvus_storage::RowGroupMetadataVector> row_group_meta_list;
    ParquetStatisticsByField parquet_stats_by_field;
};

LoadedGroupChunkMetadata
LoadGroupChunkMetadata(const std::vector<std::string>& insert_files,
                       const std::vector<FieldId>& field_ids_for_stats,
                       const std::string& debug_key);

}  // namespace milvus::segcore
