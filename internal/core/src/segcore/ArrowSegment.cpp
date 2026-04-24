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

#include "segcore/ArrowSegment.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/Types.h"
#include "segcore/ArrowUtil.h"

namespace milvus::segcore {

// =================================================================
// Constructor
// =================================================================

ArrowSegment::ArrowSegment(
    std::shared_ptr<milvus_storage::api::ChunkReader> reader,
    SchemaPtr schema,
    int64_t segment_id,
    int64_t chunks_per_cell)
    : chunk_reader_(std::move(reader)),
      schema_(std::move(schema)),
      segment_id_(segment_id) {
    auto chunk_rows = chunk_reader_->get_chunk_rows().ValueOrDie();
    size_t total = chunk_rows.size();
    size_t num_logical =
        (total + chunks_per_cell - 1) / chunks_per_cell;

    prefix_row_counts_.push_back(0);
    for (size_t cid = 0; cid < num_logical; ++cid) {
        size_t start = cid * chunks_per_cell;
        size_t end =
            std::min(start + static_cast<size_t>(chunks_per_cell), total);
        int64_t cell_rows = 0;
        for (size_t i = start; i < end; ++i) {
            cell_rows += static_cast<int64_t>(chunk_rows[i]);
        }
        chunk_row_counts_.push_back(cell_rows);
        num_rows_ += cell_rows;
        prefix_row_counts_.push_back(num_rows_);
        chunk_ranges_.push_back({start, end});
    }
    // field_to_col_ built lazily on first chunk load
}

// =================================================================
// GetOrLoadChunk
// =================================================================

std::shared_ptr<arrow::RecordBatch>
ArrowSegment::GetOrLoadChunk(int64_t chunk_id) const {
    auto it = chunk_cache_.find(chunk_id);
    if (it != chunk_cache_.end()) {
        return it->second;
    }

    auto [start, end] = chunk_ranges_[chunk_id];
    std::shared_ptr<arrow::RecordBatch> result;
    if (end - start == 1) {
        result =
            chunk_reader_->get_chunk(static_cast<int64_t>(start))
                .ValueOrDie();
    } else {
        std::vector<int64_t> indices;
        for (size_t i = start; i < end; ++i) {
            indices.push_back(static_cast<int64_t>(i));
        }
        auto batches =
            chunk_reader_->get_chunks(indices).ValueOrDie();
        auto table =
            arrow::Table::FromRecordBatches(batches).ValueOrDie();
        result = table->CombineChunksToBatch().ValueOrDie();
    }

    // Build field mapping from first loaded chunk
    if (field_to_col_.empty()) {
        auto arrow_schema = result->schema();
        for (const auto& [fid, meta] : schema_->get_fields()) {
            auto idx = arrow_schema->GetFieldIndex(
                meta.get_name().get());
            if (idx >= 0) {
                field_to_col_[fid] = idx;
            }
        }
    }

    chunk_cache_[chunk_id] = result;
    return result;
}

// =================================================================
// GetOrExpandValidity
// =================================================================

const FixedVector<bool>&
ArrowSegment::GetOrExpandValidity(FieldId field_id,
                                  int64_t chunk_id) const {
    auto key = std::make_pair(field_id, chunk_id);
    auto it = validity_cache_.find(key);
    if (it != validity_cache_.end()) {
        return it->second;
    }

    auto batch = GetOrLoadChunk(chunk_id);
    auto col = batch->column(field_to_col_.at(field_id));
    auto null_bitmap = col->null_bitmap_data();
    auto expanded = ExpandArrowValidity(
        null_bitmap, col->offset(), col->length());

    auto [inserted, _] = validity_cache_.emplace(key, std::move(expanded));
    return inserted->second;
}

// =================================================================
// Core data access: chunk_data_impl (scalars, zero-copy)
// =================================================================

PinWrapper<SpanBase>
ArrowSegment::chunk_data_impl(milvus::OpContext*,
                              FieldId field_id,
                              int64_t chunk_id) const {
    auto batch = GetOrLoadChunk(chunk_id);
    auto col = batch->column(field_to_col_.at(field_id));
    auto& validity = GetOrExpandValidity(field_id, chunk_id);
    auto data_type = schema_->operator[](field_id).get_data_type();
    auto elem_size = GetDataTypeSize(data_type);

    return PinWrapper<SpanBase>(SpanBase(
        col->data()->buffers[1]->data(),
        validity.empty() ? nullptr : validity.data(),
        chunk_row_counts_[chunk_id],
        elem_size));
}

// =================================================================
// Core data access: chunk_string_view_impl
// =================================================================

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
ArrowSegment::chunk_string_view_impl(
    milvus::OpContext*,
    FieldId field_id,
    int64_t chunk_id,
    std::optional<std::pair<int64_t, int64_t>> offset_len) const {
    auto batch = GetOrLoadChunk(chunk_id);
    auto str = std::static_pointer_cast<arrow::StringArray>(
        batch->column(field_to_col_.at(field_id)));
    auto& validity = GetOrExpandValidity(field_id, chunk_id);
    auto [start, len] = offset_len.value_or(
        std::make_pair(int64_t{0}, chunk_row_counts_[chunk_id]));

    std::vector<std::string_view> views;
    views.reserve(len);
    for (int64_t i = start; i < start + len; ++i) {
        auto sv = str->GetView(i);
        views.emplace_back(sv.data(), sv.size());
    }
    FixedVector<bool> valid;
    if (!validity.empty()) {
        valid.assign(
            validity.begin() + start,
            validity.begin() + start + len);
    }
    return PinWrapper(
        std::make_pair(std::move(views), std::move(valid)));
}

// =================================================================
// Core data access: chunk_string_views_by_offsets
// =================================================================

PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
ArrowSegment::chunk_string_views_by_offsets(
    milvus::OpContext*,
    FieldId field_id,
    int64_t chunk_id,
    const FixedVector<int32_t>& offsets) const {
    auto batch = GetOrLoadChunk(chunk_id);
    auto str = std::static_pointer_cast<arrow::StringArray>(
        batch->column(field_to_col_.at(field_id)));
    auto& validity = GetOrExpandValidity(field_id, chunk_id);

    std::vector<std::string_view> views;
    views.reserve(offsets.size());
    FixedVector<bool> valid;
    if (!validity.empty()) {
        valid.reserve(offsets.size());
    }
    for (auto off : offsets) {
        auto sv = str->GetView(off);
        views.emplace_back(sv.data(), sv.size());
        if (!validity.empty()) {
            valid.push_back(validity[off]);
        }
    }
    return PinWrapper(
        std::make_pair(std::move(views), std::move(valid)));
}

// =================================================================
// Field info methods
// =================================================================

bool
ArrowSegment::HasFieldData(FieldId field_id) const {
    // Ensure field mapping is available by loading chunk if needed
    if (field_to_col_.empty() && !chunk_ranges_.empty()) {
        GetOrLoadChunk(0);
    }
    return field_to_col_.count(field_id) > 0;
}

bool
ArrowSegment::HasRawData(int64_t field_id) const {
    return HasFieldData(FieldId(field_id));
}

bool
ArrowSegment::is_nullable(FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    return field_meta.is_nullable();
}

DataType
ArrowSegment::GetFieldDataType(FieldId field_id) const {
    auto& field_meta = schema_->operator[](field_id);
    return field_meta.get_data_type();
}

// =================================================================
// Chunk metadata methods
// =================================================================

int64_t
ArrowSegment::num_chunk(FieldId) const {
    return static_cast<int64_t>(chunk_row_counts_.size());
}

int64_t
ArrowSegment::num_chunk_data(FieldId) const {
    return static_cast<int64_t>(chunk_row_counts_.size());
}

int64_t
ArrowSegment::chunk_size(FieldId, int64_t chunk_id) const {
    return chunk_row_counts_[chunk_id];
}

int64_t
ArrowSegment::num_rows_until_chunk(FieldId,
                                   int64_t chunk_id) const {
    return prefix_row_counts_[chunk_id];
}

std::pair<int64_t, int64_t>
ArrowSegment::get_chunk_by_offset(FieldId, int64_t offset) const {
    // Binary search: find the chunk containing this offset
    auto it = std::upper_bound(prefix_row_counts_.begin(),
                               prefix_row_counts_.end(),
                               offset);
    int64_t chunk_id = std::max(
        int64_t{0},
        static_cast<int64_t>(
            std::distance(prefix_row_counts_.begin(), it)) -
            1);
    int64_t local_offset = offset - prefix_row_counts_[chunk_id];
    return {chunk_id, local_offset};
}

// =================================================================
// Phase 2 stubs (array/vector_array)
// =================================================================

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
ArrowSegment::chunk_array_view_impl(
    milvus::OpContext*,
    FieldId,
    int64_t,
    std::optional<std::pair<int64_t, int64_t>>) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::chunk_array_view_impl");
}

PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
ArrowSegment::chunk_vector_array_view_impl(
    milvus::OpContext*,
    FieldId,
    int64_t,
    std::optional<std::pair<int64_t, int64_t>>) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::chunk_vector_array_view_impl");
}

PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
ArrowSegment::chunk_array_views_by_offsets(
    milvus::OpContext*,
    FieldId,
    int64_t,
    const FixedVector<int32_t>&) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::chunk_array_views_by_offsets");
}

// =================================================================
// Stubs: SegmentInterface methods
// =================================================================

bool
ArrowSegment::Contain(const PkType&) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::Contain");
}

SegcoreError
ArrowSegment::Delete(int64_t, const IdArray*, const Timestamp*) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::Delete");
}

void
ArrowSegment::LoadDeletedRecord(const LoadDeletedRecordInfo&) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::LoadDeletedRecord");
}

void
ArrowSegment::LoadFieldData(const LoadFieldDataInfo&, milvus::OpContext*) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::LoadFieldData");
}

void
ArrowSegment::CreateTextIndex(FieldId, milvus::OpContext*) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::CreateTextIndex");
}

void
ArrowSegment::BulkGetJsonData(milvus::OpContext*,
                              FieldId,
                              const std::function<void(milvus::Json, size_t, bool)>&,
                              const int64_t*,
                              int64_t) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::BulkGetJsonData");
}

void
ArrowSegment::LazyCheckSchema(SchemaPtr) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::LazyCheckSchema");
}

void
ArrowSegment::Reopen(SchemaPtr) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::Reopen");
}

void
ArrowSegment::Reopen(milvus::OpContext*,
                     const milvus::proto::segcore::SegmentLoadInfo&) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::Reopen(OpContext)");
}

void
ArrowSegment::Load(milvus::tracer::TraceContext&, milvus::OpContext*) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::Load");
}

std::shared_ptr<const IArrayOffsets>
ArrowSegment::GetArrayOffsets(FieldId) const {
    return nullptr;
}

// =================================================================
// Stubs: SegmentInternalInterface methods
// =================================================================

void
ArrowSegment::vector_search(SearchInfo&,
                            const void*,
                            const size_t*,
                            int64_t,
                            Timestamp,
                            const BitsetView&,
                            milvus::OpContext*,
                            SearchResult&) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::vector_search");
}

void
ArrowSegment::mask_with_delete(BitsetTypeView&,
                               int64_t,
                               Timestamp) const {
    // No-op: ArrowSegment has no deletes
}

void
ArrowSegment::mask_with_timestamps(BitsetTypeView&,
                                   Timestamp,
                                   Timestamp) const {
    // No-op: ArrowSegment has no timestamps to filter
}

void
ArrowSegment::check_search(const query::Plan*) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::check_search");
}

const ConcurrentVector<Timestamp>&
ArrowSegment::get_timestamps() const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::get_timestamps");
}

void
ArrowSegment::search_ids(BitsetType&, const IdArray&) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::search_ids");
}

std::pair<std::vector<OffsetMap::OffsetType>, bool>
ArrowSegment::find_first_n(int64_t, const BitsetTypeView&) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::find_first_n");
}

std::tuple<std::vector<int64_t>, std::vector<std::vector<int32_t>>, bool>
ArrowSegment::find_first_n_element(int64_t,
                                   const BitsetTypeView&,
                                   const IArrayOffsets*) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::find_first_n_element");
}

void
ArrowSegment::pk_range(milvus::OpContext*,
                       proto::plan::OpType,
                       const PkType&,
                       BitsetTypeView&) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::pk_range");
}

void
ArrowSegment::pk_binary_range(milvus::OpContext*,
                              const PkType&,
                              bool,
                              const PkType&,
                              bool,
                              BitsetTypeView&) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::pk_binary_range");
}

void
ArrowSegment::bulk_subscript(milvus::OpContext*,
                             SystemFieldType,
                             const int64_t*,
                             int64_t,
                             void*) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::bulk_subscript");
}

void
ArrowSegment::bulk_subscript(milvus::OpContext*,
                             FieldId,
                             DataType,
                             const int64_t*,
                             int64_t,
                             void*,
                             TargetBitmap&,
                             bool) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::bulk_subscript");
}

std::unique_ptr<DataArray>
ArrowSegment::bulk_subscript(milvus::OpContext*,
                             FieldId,
                             const int64_t*,
                             int64_t) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::bulk_subscript");
}

std::unique_ptr<DataArray>
ArrowSegment::bulk_subscript(milvus::OpContext*,
                             FieldId,
                             const int64_t*,
                             int64_t,
                             const std::vector<std::string>&) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::bulk_subscript");
}

// =================================================================
// Stubs: SegmentSealed methods
// =================================================================

void
ArrowSegment::LoadIndex(LoadIndexInfo&) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::LoadIndex");
}

void
ArrowSegment::LoadSegmentMeta(
    const milvus::proto::segcore::LoadSegmentMeta&) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::LoadSegmentMeta");
}

void
ArrowSegment::DropIndex(const FieldId) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::DropIndex");
}

void
ArrowSegment::DropJSONIndex(const FieldId, const std::string&) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::DropJSONIndex");
}

void
ArrowSegment::DropFieldData(const FieldId) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::DropFieldData");
}

void
ArrowSegment::AddFieldDataInfoForSealed(const LoadFieldDataInfo&) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::AddFieldDataInfoForSealed");
}

void
ArrowSegment::RemoveFieldFile(const FieldId) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::RemoveFieldFile");
}

void
ArrowSegment::ClearData() {
    chunk_cache_.clear();
    validity_cache_.clear();
    field_to_col_.clear();
}

std::unique_ptr<DataArray>
ArrowSegment::get_vector(milvus::OpContext*, FieldId, const int64_t*, int64_t) const {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::get_vector");
}

void
ArrowSegment::LoadTextIndex(
    milvus::OpContext*,
    std::shared_ptr<milvus::proto::indexcgo::LoadTextIndexInfo>) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::LoadTextIndex");
}

InsertRecord<true>&
ArrowSegment::get_insert_record() {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::get_insert_record");
}

PinWrapper<index::NgramInvertedIndex*>
ArrowSegment::GetNgramIndex(milvus::OpContext*, FieldId) const {
    return nullptr;
}

PinWrapper<index::NgramInvertedIndex*>
ArrowSegment::GetNgramIndexForJson(milvus::OpContext*,
                                   FieldId,
                                   const std::string&) const {
    return nullptr;
}

void
ArrowSegment::LoadJsonStats(FieldId,
                            std::shared_ptr<index::JsonKeyStats>) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::LoadJsonStats");
}

void
ArrowSegment::RemoveJsonStats(FieldId) {
    ThrowInfo(ErrorCode::NotImplemented,
              "ArrowSegment::RemoveJsonStats");
}

}  // namespace milvus::segcore
