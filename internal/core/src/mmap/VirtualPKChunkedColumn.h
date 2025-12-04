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

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "common/EasyAssert.h"
#include "common/VirtualPK.h"
#include "mmap/ChunkedColumnInterface.h"

namespace milvus {

// VirtualPKChunkedColumn generates virtual primary keys on-the-fly.
// Virtual PK format: (truncated_segment_id << 32) | offset
// This is used for external collections that don't have real PK fields.
class VirtualPKChunkedColumn : public ChunkedColumnInterface {
 public:
    explicit VirtualPKChunkedColumn(int64_t segment_id, int64_t num_rows)
        : segment_id_(segment_id),
          truncated_segment_id_(milvus::GetTruncatedSegmentID(segment_id)),
          num_rows_(num_rows) {
        num_rows_until_chunk_ = {0, num_rows_};
    }

    ~VirtualPKChunkedColumn() override = default;

    cachinglayer::PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "DataOfChunk not supported for VirtualPKChunkedColumn");
    }

    bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const override {
        return offset < num_rows_;
    }

    void
    BulkIsValid(milvus::OpContext* ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override {
        if (offsets == nullptr) {
            for (int64_t i = 0; i < num_rows_; i++) {
                fn(true, i);
            }
        } else {
            for (int64_t i = 0; i < count; i++) {
                fn(true, i);
            }
        }
    }

    bool
    IsNullable() const override {
        return false;
    }

    size_t
    NumRows() const override {
        return num_rows_;
    }

    int64_t
    num_chunks() const override {
        return 1;
    }

    size_t
    DataByteSize() const override {
        return num_rows_ * sizeof(int64_t);
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        AssertInfo(chunk_id == 0, "VirtualPKChunkedColumn has only 1 chunk");
        return num_rows_;
    }

    PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "Span not supported for VirtualPKChunkedColumn");
    }

    void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const override {
        // No-op - no data to prefetch
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(milvus::OpContext* op_ctx,
                int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "StringViews not supported for VirtualPKChunkedColumn");
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(milvus::OpContext* op_ctx,
               int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len)
        const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "ArrayViews not supported for VirtualPKChunkedColumn");
    }

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    VectorArrayViews(milvus::OpContext* op_ctx,
                     int64_t chunk_id,
                     std::optional<std::pair<int64_t, int64_t>> offset_len)
        const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "VectorArrayViews not supported for VirtualPKChunkedColumn");
    }

    PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext* op_ctx,
                       int64_t chunk_id) const override {
        ThrowInfo(
            ErrorCode::Unsupported,
            "VectorArrayOffsets not supported for VirtualPKChunkedColumn");
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const override {
        ThrowInfo(
            ErrorCode::Unsupported,
            "StringViewsByOffsets not supported for VirtualPKChunkedColumn");
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "ArrayViewsByOffsets not supported for VirtualPKChunkedColumn");
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        AssertInfo(offset >= 0 && offset < num_rows_,
                   "offset {} is out of range, num_rows: {}",
                   offset,
                   num_rows_);
        return {0, static_cast<size_t>(offset)};
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const override {
        std::vector<milvus::cachinglayer::cid_t> cids(count, 0);
        std::vector<int64_t> offsets_in_chunk(offsets, offsets + count);
        return std::make_pair(std::move(cids), std::move(offsets_in_chunk));
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "GetChunk not supported for VirtualPKChunkedColumn");
    }

    std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext* op_ctx) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "GetAllChunks not supported for VirtualPKChunkedColumn");
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        return num_rows_until_chunk_[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        return num_rows_until_chunk_;
    }

    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override {
        // Pre-compute all virtual PKs to ensure pointers remain valid
        std::vector<int64_t> virtual_pks(count);
        for (int64_t i = 0; i < count; i++) {
            virtual_pks[i] = milvus::GetVirtualPK(truncated_segment_id_, offsets[i]);
        }
        for (int64_t i = 0; i < count; i++) {
            fn(reinterpret_cast<const char*>(&virtual_pks[i]), i);
        }
    }

    void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count) override {
        auto typed_dst = static_cast<int64_t*>(dst);
        for (int64_t i = 0; i < count; i++) {
            typed_dst[i] = milvus::GetVirtualPK(truncated_segment_id_, offsets[i]);
        }
    }

    void
    BulkVectorValueAt(milvus::OpContext* op_ctx,
                      void* dst,
                      const int64_t* offsets,
                      int64_t element_sizeof,
                      int64_t count) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkVectorValueAt not supported for VirtualPKChunkedColumn");
    }

    // Get a virtual PK at a specific offset
    int64_t
    GetVirtualPKAt(int64_t offset) const {
        AssertInfo(offset >= 0 && offset < num_rows_,
                   "offset {} is out of range, num_rows: {}",
                   offset,
                   num_rows_);
        return milvus::GetVirtualPK(truncated_segment_id_, offset);
    }

    // Get the segment ID used for virtual PK generation
    int64_t
    GetSegmentID() const {
        return segment_id_;
    }

    // Get the truncated segment ID (lower 32 bits)
    int64_t
    GetTruncatedSegmentID() const {
        return truncated_segment_id_;
    }

 private:
    int64_t segment_id_;
    int64_t truncated_segment_id_;
    int64_t num_rows_;
    std::vector<int64_t> num_rows_until_chunk_;
};

}  // namespace milvus
