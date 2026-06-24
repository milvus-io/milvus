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
#include <concepts>
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
#include "folly/Synchronized.h"
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

namespace storagev1translator {
class InsertRecordTranslator;
}

namespace storagev2translator {
class TimestampIndexCell;
class PkIndexCell;
}  // namespace storagev2translator

using namespace milvus::cachinglayer;

namespace sealed_segment_detail {

template <typename T>
concept PrimaryKey = std::same_as<T, int64_t> || std::same_as<T, std::string>;

template <PrimaryKey PK>
using PrimaryKeyView =
    std::conditional_t<std::same_as<PK, int64_t>, int64_t, std::string_view>;

template <typename T>
concept PrimitiveBulkSubscriptValue =
    std::integral<T> || std::floating_point<T>;

}  // namespace sealed_segment_detail

// Test-only accessor that pokes private members to simulate v2/v3 segment
// state (raw timestamp column emplaced into fields_ alongside an overwritten
// timestamp index). Defined in internal/core/unittest/test_commit_timestamp.cpp.
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

    std::pair<std::shared_ptr<ChunkedColumnInterface>, bool>
    GetFieldDataIfExist(FieldId field_id) const;

    std::vector<PinWrapper<const index::IndexBase*>>
    PinIndex(milvus::OpContext* op_ctx,
             FieldId field_id,
             bool include_ngram = false) const override {
        auto [scalar_indexings, ngram_fields] =
            lock(folly::rlock(scalar_indexings_), folly::rlock(ngram_fields_));
        if (!include_ngram) {
            if (ngram_fields->find(field_id) != ngram_fields->end()) {
                return {};
            }
        }

        auto iter = scalar_indexings->find(field_id);
        if (iter == scalar_indexings->end()) {
            return {};
        }
        auto ca = SemiInlineGet(iter->second->PinCells(op_ctx, {0}));
        auto index = ca->get_cell_of(0);
        return {PinWrapper<const index::IndexBase*>(std::move(ca), index)};
    }

    bool
    Contain(const PkType& pk) const override;

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

    void
    LoadTextIndex(milvus::OpContext* op_ctx,
                  std::shared_ptr<milvus::proto::indexcgo::LoadTextIndexInfo>
                      info_proto) override;

    void
    LoadJsonKeyIndex(
        milvus::OpContext* op_ctx,
        std::shared_ptr<milvus::proto::indexcgo::LoadJsonKeyIndexInfo>
            info_proto);

    void
    LoadBatchJsonKeyIndexes(
        milvus::OpContext* op_ctx,
        const std::unordered_map<
            FieldId,
            std::shared_ptr<milvus::proto::indexcgo::LoadJsonKeyIndexInfo>>&
            infos);

    void
    RemoveJsonStats(FieldId field_id) override {
        std::unique_lock lck(mutex_);
        json_stats_.erase(field_id);
    }

    void
    LoadJsonStats(FieldId field_id,
                  std::shared_ptr<index::JsonKeyStats> stats) override {
        std::unique_lock lck(mutex_);
        json_stats_[field_id] = stats;
    }

    std::shared_ptr<index::JsonKeyStats>
    GetJsonStats(milvus::OpContext* op_ctx, FieldId field_id) const override {
        std::shared_lock lck(mutex_);
        auto iter = json_stats_.find(field_id);
        if (iter == json_stats_.end()) {
            return nullptr;
        }
        return iter->second;
    }

    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(milvus::OpContext* op_ctx, FieldId field_id) const override;

    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndexForJson(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         const std::string& nested_path) const override;

    std::shared_ptr<const IArrayOffsets>
    GetArrayOffsets(FieldId field_id) const override {
        auto it = array_offsets_map_.find(field_id);
        if (it != array_offsets_map_.end()) {
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
        auto column = fields_.rlock()->at(field_id);
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

    // When non-zero commit_ts_ is active, every row in this segment carries
    // commit_ts_ as its effective row timestamp (load-time overwrite). Returns
    // nullopt otherwise. All timestamp consumers — read_ts (search_batch_pks),
    // bulk_subscript(Timestamp), mask_with_timestamps — must route through
    // this so the override applies uniformly on v1 AND v2/v3 storage paths.
    std::optional<Timestamp>
    EffectiveCommitTs() const {
        return commit_ts_ != 0 ? std::optional<Timestamp>{commit_ts_}
                               : std::nullopt;
    }

    void
    Load(milvus::tracer::TraceContext& trace_ctx,
         milvus::OpContext* op_ctx) override;

    void
    LoadManifest(const std::string& manifest_path);

 public:
    size_t
    GetMemoryUsageInBytes() const override {
        return stats_.mem_size.load() + deleted_record_.mem_size();
    }

    InsertRecord<true>&
    get_insert_record() override {
        return insert_record_;
    }

    int64_t
    get_row_count() const override;

    int64_t
    get_deleted_count() const override;

    Timestamp
    get_max_timestamp() const override {
        return insert_record_.timestamp_index_.get_max_timestamp();
    }

    const Schema&
    get_schema() const override;

    void
    pk_range(milvus::OpContext* op_ctx,
             proto::plan::OpType op,
             const PkType& pk,
             BitsetTypeView& bitset) const override;

    void
    search_sorted_pk_range(milvus::OpContext* op_ctx,
                           proto::plan::OpType op,
                           const PkType& pk,
                           BitsetTypeView& bitset) const;

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
        auto& field_meta = schema_->operator[](field_id);
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
        return schema_->get_fields().find(field_id) !=
               schema_->get_fields().end();
    }

    void
    prefetch_chunks(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    const std::vector<int64_t>& chunk_ids) const override;

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

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    chunk_vector_array_view_impl(
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
    ExecuteTake(const std::vector<int64_t>& unique_offsets,
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
        milvus::proto::common::LoadPriority load_priority);

    // Initialize timestamp index with owned data (StorageV1 path)
    void
    init_storage_v1_timestamp_index(std::vector<Timestamp> timestamps,
                                    size_t num_rows);

    template <sealed_segment_detail::PrimaryKey PK>
    void
    search_sorted_pk_range_impl(
        proto::plan::OpType op,
        const PK& target,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column,
        BitsetTypeView& bitset) const;

    template <sealed_segment_detail::PrimaryKey PK>
    void
    search_sorted_pk_binary_range_impl(
        const PK& lower_val,
        bool lower_inclusive,
        const PK& upper_val,
        bool upper_inclusive,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column,
        BitsetTypeView& bitset) const;

    template <sealed_segment_detail::PrimaryKey PK>
    std::optional<int64_t>
    find_sorted_pk_doc_offset(
        const PK& pk,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column) const;

    template <sealed_segment_detail::PrimaryKey PK>
    void
    search_pks_with_two_pointers_impl(
        BitsetTypeView& bitset,
        const std::vector<PkType>& pks,
        const std::shared_ptr<ChunkedColumnInterface>& pk_column) const;

    // Binary search to find lower_bound of pk in pk_column starting from from_chunk_id
    // Returns: (chunk_id, in_chunk_offset, exists)
    //   - chunk_id: the chunk containing the first value >= pk
    //   - in_chunk_offset: offset of the first value >= pk in that chunk
    //   - exists: true if found an exact match (value == pk), false otherwise
    //   - If pk doesn't exist, returns the position of first value > pk with exists=false
    //   - If pk is greater than all values, returns {-1, -1, false}
    struct PkLowerBoundResult {
        int chunk_id = -1;
        int in_chunk_offset = -1;
        bool exact_match = false;
    };

    template <sealed_segment_detail::PrimaryKey PK>
    PkLowerBoundResult
    pk_lower_bound(const PK& pk,
                   const ChunkedColumnInterface* pk_column,
                   const std::vector<PinWrapper<Chunk*>>& all_chunk_pins,
                   int from_chunk_id = 0) const;

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
    struct PkPosition {
        int chunk_id = -1;
        int in_chunk_offset = -1;
    };

    template <sealed_segment_detail::PrimaryKey PK>
    PkPosition
    find_last_pk_position(const PK& pk,
                          const ChunkedColumnInterface* pk_column,
                          const std::vector<PinWrapper<Chunk*>>& all_chunk_pins,
                          int first_chunk_id,
                          int first_in_chunk_offset) const;

    template <typename S, typename T = S>
    static void
    bulk_subscript_impl(milvus::OpContext* op_ctx,
                        const void* src_raw,
                        const int64_t* seg_offsets,
                        int64_t count,
                        T* dst_raw);

    template <sealed_segment_detail::PrimitiveBulkSubscriptValue T>
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

    static void
    bulk_subscript_string_impl(
        milvus::OpContext* op_ctx,
        const ChunkedColumnInterface* field,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<std::string>* dst_raw);

    static void
    bulk_subscript_json_impl(
        milvus::OpContext* op_ctx,
        const ChunkedColumnInterface* field,
        const int64_t* seg_offsets,
        int64_t count,
        google::protobuf::RepeatedPtrField<std::string>* dst_raw);

    static void
    bulk_subscript_string_impl(milvus::OpContext* op_ctx,
                               const ChunkedColumnInterface* field,
                               const int64_t* seg_offsets,
                               int64_t count,
                               std::string* dst);

    static void
    bulk_subscript_json_impl(milvus::OpContext* op_ctx,
                             const ChunkedColumnInterface* field,
                             const int64_t* seg_offsets,
                             int64_t count,
                             Json* dst);

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
                                      FieldId field_id,
                                      const int64_t* seg_offsets,
                                      int64_t count) const;

    ValidResult
    FilterVectorValidOffsetsFromColumn(milvus::OpContext* op_ctx,
                                       const ChunkedColumnInterface* column,
                                       const int64_t* seg_offsets,
                                       int64_t count) const;

    void
    update_row_count(int64_t row_count) {
        num_rows_ = row_count;
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

    void
    LoadVecIndex(LoadIndexInfo& info, bool is_replace = false);

    void
    LoadScalarIndex(LoadIndexInfo& info, bool is_replace = false);

    void
    LoadIndex(LoadIndexInfo& info, bool is_replace);

    bool
    generate_interim_index(const FieldId field_id,
                           int64_t num_rows,
                           milvus::OpContext* op_ctx);

    void
    fill_empty_field(const FieldMeta& field_meta);

    void
    EnsureArrayOffsetsForStructField(const FieldMeta& field_meta,
                                     int64_t row_count);

    /**
     * @brief Fill default values for fields without data sources
     *
     * This method fills default values for fields that exist in the schema
     * but have no data source (binlog, index with raw data, or column group).
     * Used during schema evolution when new fields are added.
     *
     * @param field_ids Vector of field IDs that need default value filling
     */
    void
    FillDefaultValueFields(const std::vector<FieldId>& field_ids);

    void
    LoadFieldData(const LoadFieldDataInfo& load_info,
                  milvus::OpContext* op_ctx,
                  bool is_replace);

    void
    load_field_data_internal(const LoadFieldDataInfo& load_info,
                             milvus::OpContext* op_ctx = nullptr,
                             bool is_replace = false);

    void
    load_column_group_data_internal(const LoadFieldDataInfo& load_info,
                                    milvus::OpContext* op_ctx = nullptr,
                                    bool is_replace = false);

    void
    LoadBatchIndexes(milvus::tracer::TraceContext& trace_ctx,
                     std::unordered_map<FieldId, std::vector<LoadIndexInfo>>&
                         field_id_to_index_info,
                     milvus::OpContext* op_ctx = nullptr,
                     bool is_replace = false);

    void
    LoadBatchFieldData(milvus::tracer::TraceContext& trace_ctx,
                       std::vector<std::pair<std::vector<FieldId>,
                                             proto::segcore::FieldBinlog>>&
                           field_binlog_to_load,
                       milvus::OpContext* op_ctx = nullptr,
                       bool is_replace = false);

    /**
     * @brief Initialize LOB base paths for TEXT fields
     *
     * This method parses the manifest path to extract the segment base path,
     * and computes lob_base_path for each TEXT type field.
     *
     * @param manifest_path JSON string containing base_path and version fields
     */
    void
    InitTextLobPaths(const std::string& manifest_path);

    void
    LoadColumnGroups(
        const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        std::vector<std::pair<int, std::vector<FieldId>>>& cg_field_ids,
        bool eager_load,
        milvus::OpContext* op_ctx = nullptr,
        bool is_replace = false);

    // Load column groups from a manifest file path (for external collections)
    void
    LoadColumnGroups(const std::string& manifest_path,
                     milvus::OpContext* op_ctx = nullptr);

    /**
     * @brief Load a single column group at the specified index
     *
     * Reads a specific column group from milvus storage, converts the data
     * to internal format, and stores it in the segment's field data structures.
     *
     * @param column_groups Metadata about all available column groups
     * @param properties Storage properties for accessing the data
     * @param index Index of the column group to load
     * @param milvus_field_ids A vector of field IDs to load
     * @param eager_load Whether to eagerly load provided columns
     * @param op_ctx The operation context
     */
    void
    LoadColumnGroup(
        const std::shared_ptr<milvus_storage::api::ColumnGroups>& column_groups,
        const std::shared_ptr<milvus_storage::api::Properties>& properties,
        int64_t index,
        const std::vector<FieldId>& milvus_field_ids,
        bool eager_load,
        milvus::OpContext* op_ctx = nullptr,
        bool is_replace = false);

    // Synthesize system fields (virtual PK, timestamps, PK index, row count)
    // for external collections. External collections don't have real PK or
    // timestamp fields in their parquet data, so these must be generated.
    void
    SynthesizeExternalSystemFields();

    /**
     * @brief Reloads columns from the specified field IDs
     *
     * @param field_ids_to_reload A vector of field IDs to reload
     * @param op_ctx The operation context
     */
    void
    ReloadColumns(const std::vector<FieldId>& field_ids_to_reload,
                  milvus::OpContext* op_ctx = nullptr);

    /**
     * @brief Load text indexes in batch
     *
     * @param op_ctx The operation context
     * @param text_indices_to_load a map from field id to text indexes to load
     */
    void
    LoadBatchTextIndexes(
        milvus::OpContext* op_ctx,
        std::unordered_map<FieldId,
                           std::shared_ptr<proto::indexcgo::LoadTextIndexInfo>>&
            text_indexes_to_load);

    /**
     * @brief Apply load differences to update segment load information
     *
     * This method processes the differences between current and new load states,
     * updating the segment's loaded fields and indexes accordingly. It handles
     * incremental updates during segment reopen operations.
     *
     * @param op_ctx The operation context
     * @param segment_load_info The segment load information to be updated
     * @param load_diff The differences to apply, containing fields and indexes to add/remove
     */
    void
    ApplyLoadDiff(milvus::OpContext* op_ctx,
                  SegmentLoadInfo& segment_load_info,
                  LoadDiff& load_diff);

    void
    Reopen(milvus::OpContext* op_ctx, SchemaPtr sch);

    void
    ApplySchemaForReopen(SchemaPtr sch);

    void
    RecordDefaultFieldsFilled(const std::vector<FieldId>& field_ids);

    // Atomically records that a text index has been created for `field_id` in
    // the published segment_load_info_. Uses a CAS loop so it is safe whether
    // or not the caller holds reopen_mutex_ (tests call CreateTextIndex
    // directly, outside a Reopen/Load chain).
    void
    RecordTextIndexCreated(FieldId field_id);

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
    get_column(FieldId field_id) const {
        std::shared_ptr<ChunkedColumnInterface> res;
        fields_.withRLock([&](auto& fields) {
            auto it = fields.find(field_id);
            if (it != fields.end()) {
                res = it->second;
            }
        });
        return res;
    }

    PinWrapper<const storagev2translator::TimestampIndexCell*>
    PinTimestampIndex(milvus::OpContext* op_ctx) const;

    PinWrapper<const storagev2translator::PkIndexCell*>
    PinPkIndex(milvus::OpContext* op_ctx) const;

    void
    init_storage_v2_timestamp_index(
        const std::shared_ptr<ChunkedColumnInterface>& column,
        size_t num_rows,
        const std::string& warmup_policy = "");

    void
    init_storage_v1_pk_index(
        FieldId field_id,
        const std::shared_ptr<ChunkedColumnInterface>& column,
        DataType data_type,
        bool is_replace);

    void
    init_storage_v2_pk_index(
        FieldId field_id,
        const std::shared_ptr<ChunkedColumnInterface>& column,
        DataType data_type);

#ifdef MILVUS_UNIT_TEST
 public:
    // Test-only: inject a mock Reader for unit testing take() paths.
    void
    SetReaderForTesting(std::unique_ptr<milvus_storage::api::Reader> r) {
        reader_ = std::move(r);
    }

    // Wrappers for protected methods to enable direct unit testing.
    void
    TestFillTargetEntry(const query::Plan* plan, SearchResult& results) const {
        FillTargetEntry(plan, results);
    }

    bool
    TestTryTakeForSearch(const query::Plan* plan,
                         const int64_t* seg_offsets,
                         int64_t size,
                         SearchResult& results) const {
        return TryTakeForSearch(plan, seg_offsets, size, results);
    }

    // Test-only: set use_take_for_output flag.
    void
    SetUseTakeForOutputForTesting(bool val) {
        use_take_for_output_.store(val, std::memory_order_relaxed);
    }

    // Test-only: inject TEXT LOB base path.
    void
    SetTextLobPathForTesting(FieldId field_id, std::string lob_base_path) {
        text_lob_paths_[field_id] = std::move(lob_base_path);
    }

    // Test-only: snapshot access to segment_load_info_ for asserting Reopen
    // preserves runtime-only state (e.g. created_text_indexes_).
    std::shared_ptr<const SegmentLoadInfo>
    TestGetSegmentLoadInfo() {
        return segment_load_info_.load();
    }

    // Test-only: stamp a field as having had its text index created, via the
    // same COW path production uses. Simulates a prior CreateTextIndex.
    void
    TestRecordTextIndexCreated(FieldId field_id) {
        RecordTextIndexCreated(field_id);
    }
#endif

 private:
    // InsertRecord needs to pin pk column.
    friend class storagev1translator::InsertRecordTranslator;

    // mmap descriptor, used in chunk cache
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
    // segment loading state
    BitsetType field_data_ready_bitset_;
    BitsetType index_ready_bitset_;
    BitsetType binlog_index_bitset_;

    // when index is ready (index_ready_bitset_/binlog_index_bitset_ is set to true), must also set index_has_raw_data_
    // to indicate whether the loaded index has raw data.
    std::unordered_map<FieldId, bool> index_has_raw_data_;

    // TODO: generate index for scalar
    std::optional<int64_t> num_rows_;

    // ngram indexings for json type
    folly::Synchronized<std::unordered_map<
        FieldId,
        std::unordered_map<std::string, index::CacheIndexBasePtr>>>
        ngram_indexings_;

    // fields that has ngram index
    folly::Synchronized<std::unordered_set<FieldId>> ngram_fields_;

    // scalar field index
    folly::Synchronized<std::unordered_map<FieldId, index::CacheIndexBasePtr>>
        scalar_indexings_;
    // vector field index
    SealedIndexingRecord vector_indexings_;

    // inserted fields data and row_ids, timestamps
    InsertRecord<true> insert_record_;
    folly::Synchronized<
        std::shared_ptr<CacheSlot<storagev2translator::TimestampIndexCell>>>
        timestamp_index_slot_;
    folly::Synchronized<
        std::shared_ptr<CacheSlot<storagev2translator::PkIndexCell>>>
        pk_index_slot_;

    // deleted pks
    mutable DeletedRecord<true> deleted_record_;

    LoadFieldDataInfo field_data_info_;

    // Load-info snapshot. Readers obtain a shared_ptr snapshot; writers publish
    // through copy-on-write. The pointee is const — mutations go through
    // RecordTextIndexCreated and the Reopen/SetLoadInfo/Load entry points.
    std::atomic<std::shared_ptr<const SegmentLoadInfo>> segment_load_info_;

    // Serializes top-level writers of segment_load_info_
    // (Reopen(pb), SetLoadInfo, Load) so their diff → publish → ApplyLoadDiff
    // sequences never interleave. Does NOT block readers — reader paths access
    // segment_load_info_ via atomic shared_ptr load.
    // Lock order: reopen_mutex_ → mutex_ (outer → inner) only. Never inverse,
    // and reader paths that take mutex_ must not take reopen_mutex_.
    std::mutex reopen_mutex_;

    SchemaPtr schema_;
    int64_t id_;
    // commit_ts_ is set for import segments to prevent rows with old historical
    // timestamps from being visible to queries before T_commit.
    uint64_t commit_ts_{0};
    mutable folly::Synchronized<
        std::unordered_map<FieldId, std::shared_ptr<ChunkedColumnInterface>>>
        fields_;
    std::unordered_set<FieldId> mmap_field_ids_;

    // only useful in binlog
    IndexMetaPtr col_index_meta_;
    SegcoreConfig segcore_config_;
    std::unordered_map<FieldId, std::unique_ptr<VecIndexConfig>>
        vec_binlog_config_;

    SegmentStats stats_{};

    // whether the segment is sorted by the pk
    // 1. will skip index loading for primary key field
    bool is_sorted_by_pk_ = false;

    // When true, use take() API for output field retrieval from storage.
    // Published alongside segment_load_info_ by the writer entry points; read
    // lock-free on query hot paths.
    std::atomic<bool> use_take_for_output_{false};

    // milvus storage internal api reader instance (NOT thread-safe).
    // Load-time access (get_chunk_reader, SetReader) is safe: single-threaded,
    // completes before the segment is visible to queries.
    // Query-time access (take) must be serialized via reader_mutex_ — concurrent
    // retrieve/search workers can hit the same segment at the same time.
    std::unique_ptr<milvus_storage::api::Reader> reader_;
    mutable std::mutex reader_mutex_;

    // ArrayOffsetsSealed for element-level filtering on array fields
    // field_id -> ArrayOffsetsSealed mapping
    std::unordered_map<FieldId, std::shared_ptr<ArrayOffsetsSealed>>
        array_offsets_map_;
    // struct_name -> ArrayOffsetsSealed mapping (temporary during load)
    std::unordered_map<std::string, std::shared_ptr<ArrayOffsetsSealed>>
        struct_to_array_offsets_;

    // LOB base paths for TEXT fields
    // field_id -> lob_base_path mapping (e.g., {partition_path}/lobs/{field_id})
    std::unordered_map<FieldId, std::string> text_lob_paths_;
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
