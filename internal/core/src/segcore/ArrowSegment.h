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

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/record_batch.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/Types.h"
#include "milvus-storage/reader.h"
#include "segcore/SegmentSealed.h"

namespace milvus::segcore {

class ArrowSegment : public SegmentSealed {
 public:
    ArrowSegment(
        std::shared_ptr<milvus_storage::api::ChunkReader> reader,
        SchemaPtr schema,
        int64_t segment_id = 0,
        int64_t chunks_per_cell = 1);

    ~ArrowSegment() override = default;

    // ---------------------------------------------------------------
    // Trivial metadata
    // ---------------------------------------------------------------
    bool
    is_chunked() const override {
        return true;
    }
    int64_t
    get_row_count() const override {
        return num_rows_;
    }
    int64_t
    get_deleted_count() const override {
        return 0;
    }
    const Schema&
    get_schema() const override {
        return *schema_;
    }
    int64_t
    get_segment_id() const override {
        return segment_id_;
    }
    int64_t
    size_per_chunk() const override {
        return chunk_row_counts_.empty() ? 0 : chunk_row_counts_[0];
    }
    int64_t
    get_active_count(Timestamp) const override {
        return num_rows_;
    }
    Timestamp
    get_max_timestamp() const override {
        return MAX_TIMESTAMP;
    }

    // ---------------------------------------------------------------
    // Field info
    // ---------------------------------------------------------------
    bool
    HasFieldData(FieldId field_id) const override;

    bool
    HasRawData(int64_t field_id) const override;

    bool
    HasIndex(FieldId field_id) const override {
        return false;
    }

    bool
    is_nullable(FieldId field_id) const override;

    DataType
    GetFieldDataType(FieldId field_id) const override;

    // ---------------------------------------------------------------
    // Chunk metadata
    // ---------------------------------------------------------------
    int64_t
    num_chunk(FieldId field_id) const override;

    int64_t
    num_chunk_data(FieldId field_id) const override;

    int64_t
    chunk_size(FieldId field_id, int64_t chunk_id) const override;

    int64_t
    num_rows_until_chunk(FieldId field_id,
                         int64_t chunk_id) const override;

    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId field_id,
                        int64_t offset) const override;

    // ---------------------------------------------------------------
    // Misc metadata
    // ---------------------------------------------------------------
    bool
    is_field_exist(FieldId field_id) const override {
        return schema_->get_fields().find(field_id) !=
               schema_->get_fields().end();
    }

    bool
    is_mmap_field(FieldId field_id) const override {
        return false;
    }

    size_t
    GetMemoryUsageInBytes() const override {
        return 0;
    }

    bool
    Contain(const PkType& pk) const override;

    // ---------------------------------------------------------------
    // Stubs: SegmentInterface
    // ---------------------------------------------------------------
    SegcoreError
    Delete(int64_t size,
           const IdArray* pks,
           const Timestamp* timestamps) override;
    void
    LoadDeletedRecord(const LoadDeletedRecordInfo& info) override;
    void
    LoadFieldData(const LoadFieldDataInfo& info,
                  milvus::OpContext* op_ctx = nullptr) override;
    void
    CreateTextIndex(FieldId field_id,
                    milvus::OpContext* op_ctx = nullptr) override;
    void
    BulkGetJsonData(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        const std::function<void(milvus::Json, size_t, bool)>& fn,
        const int64_t* offsets,
        int64_t count) const override;
    void
    LazyCheckSchema(SchemaPtr sch) override;
    void
    Reopen(SchemaPtr sch) override;
    void
    Reopen(milvus::OpContext* op_ctx,
           const milvus::proto::segcore::SegmentLoadInfo& new_load_info) override;
    void
    Load(milvus::tracer::TraceContext& trace_ctx,
         milvus::OpContext* op_ctx = nullptr) override;
    std::shared_ptr<const IArrayOffsets>
    GetArrayOffsets(FieldId field_id) const override;

    // ---------------------------------------------------------------
    // Stubs: SegmentInternalInterface
    // ---------------------------------------------------------------
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
    void
    mask_with_timestamps(BitsetTypeView& bitset_chunk,
                         Timestamp timestamp,
                         Timestamp collection_ttl) const override;
    void
    check_search(const query::Plan* plan) const override;
    const ConcurrentVector<Timestamp>&
    get_timestamps() const override;
    void
    search_ids(BitsetType& bitset,
               const IdArray& id_array) const override;
    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_n(int64_t limit,
                 const BitsetTypeView& bitset) const override;
    std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
    find_first_n_element(int64_t limit,
                         const BitsetTypeView& element_bitset,
                         const IArrayOffsets* array_offsets) const override;
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

    // ---------------------------------------------------------------
    // Stubs: SegmentSealed
    // ---------------------------------------------------------------
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
    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(milvus::OpContext* op_ctx,
                  FieldId field_id) const override;
    PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndexForJson(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        const std::string& nested_path) const override;
    void
    LoadJsonStats(FieldId field_id,
                  std::shared_ptr<index::JsonKeyStats> stats) override;
    void
    RemoveJsonStats(FieldId field_id) override;

 protected:
    // ---------------------------------------------------------------
    // Core data access
    // ---------------------------------------------------------------
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

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    chunk_string_views_by_offsets(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        const FixedVector<int32_t>& offsets) const override;

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

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    chunk_array_views_by_offsets(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        const FixedVector<int32_t>& offsets) const override;

 private:
    std::shared_ptr<arrow::RecordBatch>
    GetOrLoadChunk(int64_t chunk_id) const;

    const FixedVector<bool>&
    GetOrExpandValidity(FieldId field_id,
                        int64_t chunk_id) const;

    std::shared_ptr<milvus_storage::api::ChunkReader> chunk_reader_;
    SchemaPtr schema_;
    int64_t segment_id_{0};
    int64_t num_rows_{0};
    std::vector<int64_t> chunk_row_counts_;
    std::vector<int64_t> prefix_row_counts_;
    std::vector<std::pair<size_t, size_t>> chunk_ranges_;
    mutable std::unordered_map<FieldId, int> field_to_col_;

    mutable std::unordered_map<int64_t,
                               std::shared_ptr<arrow::RecordBatch>>
        chunk_cache_;
    mutable std::map<std::pair<FieldId, int64_t>, FixedVector<bool>>
        validity_cache_;
};

}  // namespace milvus::segcore
