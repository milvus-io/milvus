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

#include <mutex>

#include "cachinglayer/CacheSlot.h"
#include "common/Chunk.h"
#include "common/OffsetMapping.h"
#include "common/bson_view.h"
namespace milvus {

using namespace milvus::cachinglayer;

class ChunkedColumnInterface {
 public:
    virtual ~ChunkedColumnInterface() = default;

    // Check if this column is part of a multi-field column group.
    // Used to guard DropFieldData from breaking shared storage.
    virtual bool
    IsInMultiFieldColumnGroup() const {
        return false;
    }

    // Default implementation does nothing.
    virtual void
    ManualEvictCache() const {
    }

    // Cancel any pending async warmup for this column's cache slot.
    // Default implementation does nothing.
    virtual void
    CancelWarmup() {
    }

    // Get raw data pointer of a specific chunk
    virtual cachinglayer::PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const = 0;

    // Check if the value at given offset is valid (not null)
    virtual bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const = 0;

    // fn: (bool is_valid, size_t offset) -> void
    // If offsets is nullptr, this function will iterate over all rows.
    // Only BulkRawStringAt and BulkIsValid allow offsets to be nullptr.
    // Other Bulk* methods can also support nullptr offsets, but not added at this moment.
    virtual void
    BulkIsValid(milvus::OpContext* ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const = 0;

    // Check if the column can contain null values
    virtual bool
    IsNullable() const = 0;

    // Get total number of rows in the column
    virtual size_t
    NumRows() const = 0;

    // Get total number of chunks in the column
    virtual int64_t
    num_chunks() const = 0;

    // Get total byte size of the column data
    virtual size_t
    DataByteSize() const = 0;

    // Get number of rows in a specific chunk
    virtual int64_t
    chunk_row_nums(int64_t chunk_id) const = 0;

    virtual PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const = 0;

    virtual void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const = 0;

    virtual PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(milvus::OpContext* op_ctx,
                int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const = 0;

    virtual PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(milvus::OpContext* op_ctx,
               int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len) const = 0;

    virtual PinWrapper<
        std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    VectorArrayViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const = 0;

    virtual PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext* op_ctx, int64_t chunk_id) const = 0;

    virtual PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const = 0;

    virtual PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const = 0;

    // Convert a global offset to (chunk_id, offset_in_chunk) pair
    virtual std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const = 0;

    virtual std::pair<std::vector<milvus::cachinglayer::cid_t>,
                      std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const = 0;

    virtual PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const = 0;

    virtual std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext* op_ctx) const = 0;

    // Get number of rows before a specific chunk
    virtual int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const = 0;

    // Get vector of row counts before each chunk
    virtual const std::vector<int64_t>&
    GetNumRowsUntilChunk() const = 0;

    // Get vector of valid (non-null) row counts before each chunk
    // For nullable columns, this tracks cumulative physical offsets
    // For non-nullable columns, this equals GetNumRowsUntilChunk()
    const std::vector<int64_t>&
    GetNumValidRowsUntilChunk() const {
        if (!num_valid_rows_until_chunk_.empty()) {
            return num_valid_rows_until_chunk_;
        }
        return GetNumRowsUntilChunk();
    }

    const FixedVector<bool>&
    GetValidData() const {
        return valid_data_;
    }

    const std::vector<int64_t>&
    GetValidCountPerChunk() const {
        return valid_count_per_chunk_;
    }

    const OffsetMapping&
    GetOffsetMapping() const {
        return offset_mapping_;
    }

    virtual void
    BuildValidRowIds(milvus::OpContext* op_ctx) {
        if (!IsNullable()) {
            return;
        }
        const auto total_chunks = num_chunks();
        const auto total_rows = NumRows();
        auto chunk_pws = GetAllChunks(op_ctx);

        valid_data_.resize(total_rows);
        valid_count_per_chunk_.assign(total_chunks, 0);

        int64_t logical_offset = 0;
        for (int64_t i = 0; i < total_chunks; i++) {
            auto chunk = chunk_pws[i].get();
            const auto rows = chunk_row_nums(i);
            int64_t valid_count = 0;
            for (int64_t j = 0; j < rows; j++) {
                const bool v = chunk->isValid(j);
                valid_data_[logical_offset + j] = v;
                valid_count += v ? 1 : 0;
            }
            valid_count_per_chunk_[i] = valid_count;
            logical_offset += rows;
        }

        num_valid_rows_until_chunk_.clear();
        num_valid_rows_until_chunk_.reserve(total_chunks + 1);
        num_valid_rows_until_chunk_.push_back(0);
        for (int64_t i = 0; i < total_chunks; i++) {
            num_valid_rows_until_chunk_.push_back(
                num_valid_rows_until_chunk_.back() + valid_count_per_chunk_[i]);
        }
        if (chunk_build_flags_.empty()) {
            BuildOffsetMapping();
        }
    }

    // Returns false if stats can't be aligned to chunks; caller falls back to eager.
    template <typename GetRgRows, typename GetRgNulls>
    bool
    TryInitValidRowIdsFromRowGroups(size_t num_row_groups,
                                    GetRgRows&& rg_rows,
                                    GetRgNulls&& rg_nulls) {
        const auto num_chunks = static_cast<int64_t>(this->num_chunks());
        if (num_chunks == 0) {
            return false;
        }
        const auto& chunk_bounds = GetNumRowsUntilChunk();
        if (static_cast<int64_t>(chunk_bounds.size()) != num_chunks + 1) {
            return false;
        }
        std::vector<int64_t> counts(num_chunks, 0);
        int64_t chunk_idx = 0;
        int64_t accumulated_rows = 0;
        for (size_t i = 0; i < num_row_groups; ++i) {
            const int64_t rows = rg_rows(i);
            const int64_t nulls = rg_nulls(i);
            if (rows < 0 || nulls < 0 || nulls > rows) {
                return false;
            }
            if (chunk_idx >= num_chunks) {
                return false;
            }
            counts[chunk_idx] += rows - nulls;
            accumulated_rows += rows;
            if (chunk_idx + 1 < num_chunks &&
                accumulated_rows >= chunk_bounds[chunk_idx + 1]) {
                if (accumulated_rows != chunk_bounds[chunk_idx + 1]) {
                    return false;
                }
                chunk_idx++;
            }
        }
        if (accumulated_rows != chunk_bounds[num_chunks]) {
            return false;
        }
        InitValidRowIds(counts);
        return true;
    }

    // Lazy counterpart of BuildValidRowIds: populates counts from metadata
    // without pinning any chunk.
    void
    InitValidRowIds(const std::vector<int64_t>& valid_count_per_chunk) {
        if (!IsNullable()) {
            return;
        }
        valid_count_per_chunk_ = valid_count_per_chunk;
        num_valid_rows_until_chunk_.clear();
        num_valid_rows_until_chunk_.reserve(valid_count_per_chunk.size() + 1);
        num_valid_rows_until_chunk_.push_back(0);
        int64_t total_valid = 0;
        for (auto c : valid_count_per_chunk) {
            total_valid += c;
            num_valid_rows_until_chunk_.push_back(total_valid);
        }
        offset_mapping_.Reserve(static_cast<int64_t>(NumRows()),
                                total_valid,
                                valid_count_per_chunk.size());
        chunk_build_flags_ =
            std::vector<std::once_flag>(valid_count_per_chunk.size());
    }

    void
    EnsureChunkOffsetMapping(int64_t chunk_id, milvus::OpContext* op_ctx) {
        if (!IsNullable() || chunk_build_flags_.empty()) {
            return;
        }
        if (chunk_id < 0 ||
            chunk_id >= static_cast<int64_t>(chunk_build_flags_.size())) {
            return;
        }
        if (offset_mapping_.IsChunkSet(chunk_id)) {
            return;
        }
        std::call_once(chunk_build_flags_[chunk_id], [&]() {
            auto pw = GetChunk(op_ctx, chunk_id);
            auto chunk = pw.get();
            const int64_t rows = chunk_row_nums(chunk_id);
            std::vector<uint8_t> valid_bytes(rows);
            for (int64_t j = 0; j < rows; ++j) {
                valid_bytes[j] = chunk->isValid(j) ? 1 : 0;
            }
            const int64_t start_logical = GetNumRowsUntilChunk()[chunk_id];
            const int64_t start_physical =
                num_valid_rows_until_chunk_[chunk_id];
            offset_mapping_.SetChunk(
                chunk_id,
                start_logical,
                start_physical,
                reinterpret_cast<const bool*>(valid_bytes.data()),
                rows);
        });
    }

    // Build offset mapping from valid_data
    void
    BuildOffsetMapping() {
        if (!valid_data_.empty()) {
            offset_mapping_.Build(valid_data_.data(), valid_data_.size());
        }
    }

    virtual void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) = 0;

    virtual void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count,
                         bool small_int_raw_type = false) = 0;

    virtual void
    BulkVectorValueAt(milvus::OpContext* op_ctx,
                      void* dst,
                      const int64_t* offsets,
                      int64_t element_sizeof,
                      int64_t count) = 0;

    // fn: (std::string_view value, size_t offset, bool is_valid) -> void
    // If offsets is nullptr, this function will iterate over all rows.
    // Only BulkRawStringAt and BulkIsValid allow offsets to be nullptr.
    // Other Bulk* methods can also support nullptr offsets, but not added at this moment.
    virtual void
    BulkRawStringAt(milvus::OpContext* op_ctx,
                    std::function<void(std::string_view, size_t, bool)> fn,
                    const int64_t* offsets = nullptr,
                    int64_t count = 0) const {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkRawStringAt only supported for ChunkColumnInterface of "
                  "variable length type");
    }

    virtual void
    BulkRawJsonAt(milvus::OpContext* op_ctx,
                  std::function<void(Json, size_t, bool)> fn,
                  const int64_t* offsets,
                  int64_t count) const {
        ThrowInfo(
            ErrorCode::Unsupported,
            "RawJsonAt only supported for ChunkColumnInterface of Json type");
    }

    virtual void
    BulkRawBsonAt(milvus::OpContext* op_ctx,
                  std::function<void(BsonView, uint32_t, uint32_t)> fn,
                  const uint32_t* row_offsets,
                  const uint32_t* value_offsets,
                  int64_t count) const {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkRawBsonAt only supported for ChunkColumnInterface of "
                  "Bson type");
    }

    virtual void
    BulkArrayAt(milvus::OpContext* op_ctx,
                std::function<void(const ArrayView&, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkArrayAt only supported for ChunkedArrayColumn");
    }

    virtual void
    BulkVectorArrayAt(milvus::OpContext* op_ctx,
                      std::function<void(VectorFieldProto&&, size_t)> fn,
                      const int64_t* offsets,
                      int64_t count) const {
        ThrowInfo(
            ErrorCode::Unsupported,
            "BulkVectorArrayAt only supported for ChunkedVectorArrayColumn");
    }

    static bool
    IsPrimitiveDataType(DataType data_type) {
        return data_type == DataType::INT8 || data_type == DataType::INT16 ||
               data_type == DataType::INT32 || data_type == DataType::INT64 ||
               data_type == DataType::FLOAT || data_type == DataType::DOUBLE ||
               data_type == DataType::BOOL ||
               data_type == DataType::TIMESTAMPTZ;
    }

    static bool
    IsChunkedVariableColumnDataType(DataType data_type) {
        return data_type == DataType::STRING ||
               data_type == DataType::VARCHAR || data_type == DataType::TEXT ||
               data_type == DataType::JSON || data_type == DataType::GEOMETRY ||
               data_type == DataType::MOL;
    }

    static bool
    IsChunkedArrayColumnDataType(DataType data_type) {
        return data_type == DataType::ARRAY;
    }

    static bool
    IsChunkedVectorArrayColumnDataType(DataType data_type) {
        return data_type == DataType::VECTOR_ARRAY;
    }

    static bool
    IsChunkedColumnDataType(DataType data_type) {
        return !IsChunkedVariableColumnDataType(data_type) &&
               !IsChunkedArrayColumnDataType(data_type);
    }

 protected:
    FixedVector<bool> valid_data_;
    std::vector<int64_t> valid_count_per_chunk_;
    std::vector<int64_t> num_valid_rows_until_chunk_;
    OffsetMapping offset_mapping_;
    std::vector<std::once_flag> chunk_build_flags_;

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    ToChunkIdAndOffset(const int64_t* offsets, int64_t count) const {
        AssertInfo(offsets != nullptr, "Offsets cannot be nullptr");
        auto num_rows = NumRows();
        for (int64_t i = 0; i < count; i++) {
            if (offsets[i] < 0 || offsets[i] >= num_rows) {
                ThrowInfo(ErrorCode::OutOfRange,
                          "offsets[{}] {} is out of range, num_rows: {}",
                          i,
                          offsets[i],
                          num_rows);
            }
        }
        return GetChunkIDsByOffsets(offsets, count);
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<uint32_t>>
    ToChunkIdAndOffset(const uint32_t* offsets, int64_t count) const {
        AssertInfo(offsets != nullptr, "Offsets cannot be nullptr");
        std::vector<milvus::cachinglayer::cid_t> cids;
        cids.reserve(count);
        std::vector<uint32_t> offsets_in_chunk;
        offsets_in_chunk.reserve(count);

        for (int64_t i = 0; i < count; i++) {
            auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offsets[i]);
            cids.push_back(chunk_id);
            offsets_in_chunk.push_back(offset_in_chunk);
        }
        return std::make_pair(std::move(cids), std::move(offsets_in_chunk));
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    ToChunkIdAndOffsetByPhysical(const int64_t* physical_offsets,
                                 int64_t count) const {
        AssertInfo(physical_offsets != nullptr,
                   "Physical offsets cannot be nullptr");
        const auto& num_valid_rows_until_chunk = GetNumValidRowsUntilChunk();
        std::vector<milvus::cachinglayer::cid_t> cids;
        cids.reserve(count);
        std::vector<int64_t> offsets_in_chunk;
        offsets_in_chunk.reserve(count);

        for (int64_t i = 0; i < count; i++) {
            auto offset = physical_offsets[i];
            auto iter = std::upper_bound(num_valid_rows_until_chunk.begin(),
                                         num_valid_rows_until_chunk.end(),
                                         offset);
            AssertInfo(iter != num_valid_rows_until_chunk.begin(),
                       "Physical offset {} is invalid",
                       offset);
            size_t chunk_idx =
                std::distance(num_valid_rows_until_chunk.begin(), iter) - 1;
            int64_t offset_in_chunk =
                offset - num_valid_rows_until_chunk[chunk_idx];
            cids.push_back(chunk_idx);
            offsets_in_chunk.push_back(offset_in_chunk);
        }
        return std::make_pair(std::move(cids), std::move(offsets_in_chunk));
    }
};

}  // namespace milvus
