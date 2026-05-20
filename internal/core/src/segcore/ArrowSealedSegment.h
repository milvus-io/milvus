// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <arrow/record_batch.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/Schema.h"
#include "expr/ITypeExpr.h"
#include "segcore/SegmentSealed.h"

namespace milvus::cachinglayer {
template <typename CellT>
class CacheSlot;
template <typename CellT>
class CellAccessor;
}  // namespace milvus::cachinglayer

namespace milvus::segcore {

class ArrowSealedSegment : public SegmentSealed {
 public:
    ArrowSealedSegment(SchemaPtr schema,
                       std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
                       int64_t segment_id = 0);

    static std::shared_ptr<ArrowSealedSegment>
    FromRecordBatches(
        SchemaPtr schema,
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int64_t segment_id = 0);

    int64_t
    row_count() const;

    std::shared_ptr<arrow::Schema>
    arrow_schema() const;

    void
    SetSimulatedLoadLatency(std::chrono::milliseconds latency);

    bool
    EvictArrowRecordBatch(int64_t column_group_id, int64_t row_stripe_id) const;

    size_t
    ArrowRecordBatchLoadCount(int64_t column_group_id,
                              int64_t row_stripe_id) const;

    PinWrapper<std::shared_ptr<arrow::RecordBatch>>
    PinArrowRecordBatchForTest(milvus::OpContext* op_ctx,
                               int64_t column_group_id,
                               int64_t row_stripe_id) const;

    int64_t
    ArrowColumnGroupCountForTest() const;

    int64_t
    ArrowFieldColumnGroupForTest(FieldId field_id) const;

    size_t
    ArrowRecordBatchReaderCreatedCountForTest() const;

    size_t
    ArrowNativeExprExecutionCountForTest() const;

    BitsetType
    ExecuteArrowNativeExprForTest(milvus::OpContext* op_ctx,
                                  const expr::TypedExprPtr& expr) const;

    bool
    CanExecuteArrowNativeExpr(const expr::TypedExprPtr& expr) const override;

    BitsetType
    ExecuteArrowNativeExpr(milvus::OpContext* op_ctx,
                           const expr::TypedExprPtr& expr) const override;

    using RecordBatchView = ArrowRecordBatchView;

    class RecordBatchIterator : public ArrowRecordBatchReader {
     public:
        bool
        HasNext() const override;

        RecordBatchView
        Next() override;

     private:
        friend class ArrowSealedSegment;

        RecordBatchIterator(const ArrowSealedSegment* segment,
                            milvus::OpContext* op_ctx,
                            std::vector<FieldId> field_ids,
                            int64_t start_row_stripe_id = 0);

        const ArrowSealedSegment* segment_;
        milvus::OpContext* op_ctx_;
        std::vector<FieldId> field_ids_;
        int64_t next_row_stripe_id_{0};
    };

    RecordBatchIterator
    IterateRecordBatches(milvus::OpContext* op_ctx,
                         std::vector<FieldId> field_ids) const;

    RecordBatchIterator
    IterateRecordBatches(milvus::OpContext* op_ctx) const;

    bool
    CanUseArrowRecordBatchReader(FieldId field_id,
                                 DataType data_type) const override;

    std::unique_ptr<ArrowRecordBatchReader>
    CreateArrowRecordBatchReader(milvus::OpContext* op_ctx,
                                 std::vector<FieldId> field_ids,
                                 int64_t start_row_stripe_id) const override;

    void
    LoadIndex(LoadIndexInfo& info) override;
    void
    LoadSegmentMeta(
        const milvus::proto::segcore::LoadSegmentMeta& meta) override;
    void
    DropIndex(const FieldId field_id) override;
    void
    DropJSONIndex(const FieldId field_id,
                  const std::string& nested_path) override;
    void
    DropFieldData(const FieldId field_id) override;
    void
    AddFieldDataInfoForSealed(
        const LoadFieldDataInfo& field_data_info) override;
    void
    RemoveFieldFile(const FieldId field_id) override;
    void
    ClearData() override;
    std::unique_ptr<DataArray>
    get_vector(milvus::OpContext* op_ctx,
               FieldId field_id,
               const int64_t* ids,
               int64_t count) const override;
    void
    LoadTextIndex(milvus::OpContext* op_ctx,
                  std::shared_ptr<milvus::proto::indexcgo::LoadTextIndexInfo>
                      info_proto) override;
    InsertRecord<true>&
    get_insert_record() override;
    void
    LoadJsonStats(FieldId field_id,
                  std::shared_ptr<index::JsonKeyStats> stats) override;
    void
    RemoveJsonStats(FieldId field_id) override;
    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(milvus::OpContext* op_ctx, FieldId field_id) const override;
    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndexForJson(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         const std::string& nested_path) const override;

    bool
    Contain(const PkType& pk) const override;
    size_t
    GetMemoryUsageInBytes() const override;
    int64_t
    get_row_count() const override;
    const Schema&
    get_schema() const override;
    int64_t
    get_deleted_count() const override;
    SegcoreError
    Delete(int64_t size,
           const IdArray* pks,
           const Timestamp* timestamps) override;
    void
    LoadDeletedRecord(const LoadDeletedRecordInfo& info) override;
    void
    LoadFieldData(const LoadFieldDataInfo& info,
                  milvus::OpContext* op_ctx = nullptr) override;
    int64_t
    get_segment_id() const override;
    bool
    HasRawData(int64_t field_id) const override;
    bool
    HasFieldData(FieldId field_id) const override;
    bool
    is_nullable(FieldId field_id) const override;
    void
    CreateTextIndex(FieldId field_id,
                    milvus::OpContext* op_ctx = nullptr) override;
    void
    BulkGetJsonData(milvus::OpContext* op_ctx,
                    FieldId field_id,
                    const std::function<void(milvus::Json, size_t, bool)>& fn,
                    const int64_t* offsets,
                    int64_t count) const override;
    void
    LazyCheckSchema(SchemaPtr sch) override;
    void
    Reopen(SchemaPtr sch) override;
    void
    Reopen(
        milvus::OpContext* op_ctx,
        const milvus::proto::segcore::SegmentLoadInfo& new_load_info) override;
    void
    Load(milvus::tracer::TraceContext& trace_ctx,
         milvus::OpContext* op_ctx) override;
    std::shared_ptr<const IArrayOffsets>
    GetArrayOffsets(FieldId field_id) const override;

    bool
    HasIndex(FieldId field_id) const override;
    bool
    is_chunked() const override;
    DataType
    GetFieldDataType(FieldId fieldId) const override;
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
    int64_t
    num_chunk_data(FieldId field_id) const override;
    int64_t
    num_rows_until_chunk(FieldId field_id, int64_t chunk_id) const override;
    void
    mask_with_timestamps(BitsetTypeView& bitset_chunk,
                         Timestamp timestamp,
                         Timestamp collection_ttl) const override;
    int64_t
    num_chunk(FieldId field_id) const override;
    int64_t
    chunk_size(FieldId field_id, int64_t chunk_id) const override;
    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId field_id, int64_t offset) const override;
    int64_t
    size_per_chunk() const override;
    int64_t
    get_active_count(Timestamp ts) const override;
    Timestamp
    get_max_timestamp() const override;
    void
    search_ids(BitsetType& bitset, const IdArray& id_array) const override;
    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n(int64_t limit, const BitsetTypeView& bitset) const override;
    std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
    find_first_n_element(
        int64_t limit,
        const BitsetTypeView& element_bitset,
        const IArrayOffsets* array_offsets,
        const std::optional<QueryIteratorCursor>& cursor) const override;
    bool
    is_mmap_field(FieldId field_id) const override;
    bool
    is_field_exist(FieldId field_id) const override;
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
    void
    pk_range(milvus::OpContext* op_ctx,
             proto::plan::OpType op,
             const PkType& pk,
             BitsetTypeView& bitset) const override;
    void
    pk_binary_range(milvus::OpContext* op_ctx,
                    const PkType& lower_pk,
                    bool lower_inclusive,
                    const PkType& upper_pk,
                    bool upper_inclusive,
                    BitsetTypeView& bitset) const override;

 protected:
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
    void
    check_search(const query::Plan* plan) const override;
    const ConcurrentVector<Timestamp>&
    get_timestamps() const override;

 private:
    friend class ArrowRecordBatchTranslator;

    struct RecordBatchCell;
    struct RecordBatchCacheState;
    struct ColumnGroup;
    struct FieldLocation {
        int64_t column_group_id;
        int column_index;
    };
    struct PinnedRecordBatch {
        std::shared_ptr<cachinglayer::CellAccessor<RecordBatchCell>> accessor;
        std::shared_ptr<arrow::RecordBatch> batch;
    };

    struct ColumnGroup {
        std::vector<FieldId> field_ids;
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::shared_ptr<cachinglayer::CacheSlot<RecordBatchCell>> slot;
        std::shared_ptr<RecordBatchCacheState> state;
    };

    struct ChunkLocation {
        int64_t chunk_id;
        int64_t local_offset;
    };

    PinnedRecordBatch
    PinRecordBatch(milvus::OpContext* op_ctx,
                   int64_t column_group_id,
                   int64_t row_stripe_id) const;
    std::shared_ptr<arrow::Array>
    GetFieldArray(const arrow::RecordBatch& batch, FieldId field_id) const;
    ChunkLocation
    LocateOffset(int64_t offset) const;
    void
    BuildFieldMap();
    void
    ValidateFieldType(FieldId field_id, DataType data_type) const;
    std::string
    FieldName(FieldId field_id) const;
    FixedVector<bool>
    BuildValidity(const arrow::Array& array,
                  int64_t start,
                  int64_t length) const;

    SchemaPtr schema_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> backing_batches_;
    std::vector<int64_t> rows_until_batch_;
    int64_t row_count_{0};
    int64_t segment_id_;
    std::unordered_map<FieldId, FieldLocation> field_to_location_;
    std::vector<ColumnGroup> column_groups_;
    std::shared_ptr<std::atomic<int64_t>> simulated_load_latency_us_;
    mutable std::atomic<size_t> arrow_record_batch_reader_created_count_{0};
    mutable std::atomic<size_t> arrow_native_expr_execution_count_{0};
    InsertRecord<true> insert_record_;
    ConcurrentVector<Timestamp> timestamps_;
};

}  // namespace milvus::segcore
