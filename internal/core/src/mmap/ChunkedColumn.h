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

#include <folly/io/IOBuf.h>
#include <sys/mman.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <math.h>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/Array.h"
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Span.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "cachinglayer/Translator.h"
#include "mmap/ChunkedColumnInterface.h"

namespace milvus {

using namespace milvus::cachinglayer;

std::pair<size_t, size_t> inline GetChunkIDByOffset(
    int64_t offset, const std::vector<int64_t>& num_rows_until_chunk) {
    // optimize for single chunk case
    if (num_rows_until_chunk.size() == 2) {
        return {0, offset};
    }
    auto iter = std::lower_bound(
        num_rows_until_chunk.begin(), num_rows_until_chunk.end(), offset + 1);
    size_t chunk_idx = std::distance(num_rows_until_chunk.begin(), iter) - 1;
    size_t offset_in_chunk = offset - num_rows_until_chunk[chunk_idx];
    return {chunk_idx, offset_in_chunk};
}

std::pair<std::vector<milvus::cachinglayer::cid_t>,
          std::vector<
              int64_t>> inline GetChunkIDsByOffsets(const int64_t* offsets,
                                                    int64_t count,
                                                    const std::vector<int64_t>&
                                                        num_rows_until_chunk) {
    std::vector<milvus::cachinglayer::cid_t> cids(count, 1);
    std::vector<int64_t> offsets_in_chunk(count);
    int64_t len = num_rows_until_chunk.size() - 1;
    while (len > 1) {
        const int64_t half = len / 2;
        len -= half;
        for (size_t i = 0; i < count; ++i) {
            const bool cmp =
                num_rows_until_chunk[cids[i] + half - 1] < offsets[i] + 1;
            cids[i] += static_cast<int64_t>(cmp) * half;
        }
    }

    for (size_t i = 0; i < count; ++i) {
        offsets_in_chunk[i] = offsets[i] - num_rows_until_chunk[--cids[i]];
    }

    return std::make_pair(std::move(cids), std::move(offsets_in_chunk));
}

std::pair<std::vector<milvus::cachinglayer::cid_t>,
          std::vector<
              int64_t>> inline GetChunkIDsByOffsets(const int64_t* offsets,
                                                    int64_t count,
                                                    const std::vector<int64_t>&
                                                        num_rows_until_chunk,
                                                    int64_t virt_chunk_order,
                                                    const std::vector<int64_t>&
                                                        vcid_to_cid_arr) {
    std::vector<milvus::cachinglayer::cid_t> cids(count, 0);
    std::vector<int64_t> offsets_in_chunk(count);
    // optimize for single chunk case
    if (num_rows_until_chunk.size() == 2) {
        for (int64_t i = 0; i < count; i++) {
            offsets_in_chunk[i] = offsets[i];
        }
        return std::make_pair(std::move(cids), std::move(offsets_in_chunk));
    }
    for (int64_t i = 0; i < count; i++) {
        auto offset = offsets[i];
        auto vcid = offset >> virt_chunk_order;
        auto scid = vcid_to_cid_arr[vcid];
        while (scid < num_rows_until_chunk.size() - 1 &&
               offset >= num_rows_until_chunk[scid + 1]) {
            scid++;
        }
        auto offset_in_chunk = offset - num_rows_until_chunk[scid];
        cids[i] = scid;
        offsets_in_chunk[i] = offset_in_chunk;
    }
    return std::make_pair(std::move(cids), std::move(offsets_in_chunk));
}

class ChunkedColumnBase : public ChunkedColumnInterface {
 public:
    explicit ChunkedColumnBase(std::unique_ptr<Translator<Chunk>> translator,
                               const FieldMeta& field_meta)
        : nullable_(field_meta.is_nullable()),
          data_type_(field_meta.get_data_type()),
          num_chunks_(translator->num_cells()),
          slot_(Manager::GetInstance().CreateCacheSlot(std::move(translator))) {
        num_rows_ = GetNumRowsUntilChunk().back();
    }

    virtual ~ChunkedColumnBase() = default;

    void
    ManualEvictCache() const override {
        slot_->ManualEvictAll();
    }

    PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const override {
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, {chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<const char*>(ca, chunk->Data());
    }

    bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const override {
        if (!nullable_) {
            return true;
        }
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
        auto ca = SemiInlineGet(
            slot_->PinCells(op_ctx, {static_cast<cid_t>(chunk_id)}));
        auto chunk = ca->get_cell_of(chunk_id);
        return chunk->isValid(offset_in_chunk);
    }

    void
    BulkIsValid(milvus::OpContext* op_ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets = nullptr,
                int64_t count = 0) const override {
        if (!nullable_) {
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
        // nullable:
        if (offsets == nullptr) {
            auto ca = SemiInlineGet(slot_->PinAllCells(op_ctx));
            for (int64_t i = 0; i < num_rows_; i++) {
                auto [cid, offset_in_chunk] = GetChunkIDByOffset(i);
                auto chunk = ca->get_cell_of(cid);
                auto valid = chunk->isValid(offset_in_chunk);
                fn(valid, i);
            }
        } else {
            auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
            auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
            for (int64_t i = 0; i < count; i++) {
                auto chunk = ca->get_cell_of(cids[i]);
                auto valid = chunk->isValid(offsets_in_chunk[i]);
                fn(valid, i);
            }
        }
    }

    bool
    IsNullable() const override {
        return nullable_;
    }

    size_t
    NumRows() const override {
        return num_rows_;
    };

    int64_t
    num_chunks() const override {
        return num_chunks_;
    }

    // This returns only memory byte size.
    size_t
    DataByteSize() const override {
        auto size = 0;
        for (auto i = 0; i < num_chunks_; i++) {
            size += slot_->size_of_cell(i).memory_bytes;
        }
        return size;
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        return GetNumRowsUntilChunk(chunk_id + 1) -
               GetNumRowsUntilChunk(chunk_id);
    }

    // TODO(tiered storage): make it async
    void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const override {
        SemiInlineGet(slot_->PinCells(op_ctx, chunk_ids));
    }

    PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "Span only supported for ChunkedColumn");
    }

    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkValueAt only supported for ChunkedColumn and "
                  "ProxyChunkColumn");
    }

    void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkPrimitiveValueAt only supported for ChunkedColumn");
    }

    void
    BulkVectorValueAt(milvus::OpContext* op_ctx,
                      void* dst,
                      const int64_t* offsets,
                      int64_t element_sizeof,
                      int64_t count) override {
        ThrowInfo(ErrorCode::Unsupported,
                  "BulkVectorValueAt only supported for ChunkedColumn");
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(milvus::OpContext* op_ctx,
                int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "StringViews only supported for VariableColumn");
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "ArrayViews only supported for ArrayChunkedColumn");
    }

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    VectorArrayViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        ThrowInfo(
            ErrorCode::Unsupported,
            "VectorArrayViews only supported for ChunkedVectorArrayColumn");
    }

    PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext* op_ctx,
                       int64_t chunk_id) const override {
        ThrowInfo(
            ErrorCode::Unsupported,
            "VectorArrayOffsets only supported for ChunkedVectorArrayColumn");
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "ViewsByOffsets only supported for VariableColumn");
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "viewsbyoffsets only supported for ArrayColumn");
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        AssertInfo(offset >= 0 && offset < num_rows_,
                   "offset {} is out of range, num_rows: {}",
                   offset,
                   num_rows_);
        auto& num_rows_until_chunk = GetNumRowsUntilChunk();
        return ::milvus::GetChunkIDByOffset(offset, num_rows_until_chunk);
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const override {
        auto& num_rows_until_chunk = GetNumRowsUntilChunk();
        auto meta = static_cast<milvus::segcore::storagev1translator::CTMeta*>(
            slot_->meta());
        auto& virt_chunk_order = meta->virt_chunk_order_;
        auto& vcid_to_cid_arr = meta->vcid_to_cid_arr_;
        return ::milvus::GetChunkIDsByOffsets(offsets,
                                              count,
                                              num_rows_until_chunk,
                                              virt_chunk_order,
                                              vcid_to_cid_arr);
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, {chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<Chunk*>(ca, chunk);
    }

    std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext* op_ctx) const override {
        auto ca = SemiInlineGet(slot_->PinAllCells(op_ctx));
        std::vector<PinWrapper<Chunk*>> ret;
        ret.reserve(num_chunks_);
        for (size_t i = 0; i < num_chunks_; i++) {
            auto chunk = ca->get_cell_of(i);
            ret.emplace_back(ca, chunk);
        }
        return ret;
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        return GetNumRowsUntilChunk()[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        auto meta = static_cast<milvus::segcore::storagev1translator::CTMeta*>(
            slot_->meta());
        return meta->num_rows_until_chunk_;
    }

 protected:
    bool nullable_{false};
    DataType data_type_{DataType::NONE};
    size_t num_rows_{0};
    size_t num_chunks_{0};
    mutable std::shared_ptr<CacheSlot<Chunk>> slot_;
};

class ChunkedColumn : public ChunkedColumnBase {
 public:
    // memory mode ctor
    explicit ChunkedColumn(std::unique_ptr<Translator<Chunk>> translator,
                           const FieldMeta& field_meta)
        : ChunkedColumnBase(std::move(translator), field_meta) {
    }

    // BulkValueAt() is used for custom data type in the future
    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override {
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
        for (int64_t i = 0; i < count; i++) {
            fn(ca->get_cell_of(cids[i])->ValueAt(offsets_in_chunk[i]), i);
        }
    }

    template <typename S, typename T>
    void
    BulkPrimitiveValueAtImpl(milvus::OpContext* op_ctx,
                             void* dst,
                             const int64_t* offsets,
                             int64_t count) {
        static_assert(std::is_fundamental_v<S> && std::is_fundamental_v<T>);
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
        auto typed_dst = static_cast<T*>(dst);
        for (int64_t i = 0; i < count; i++) {
            auto chunk = ca->get_cell_of(cids[i]);
            auto value = chunk->ValueAt(offsets_in_chunk[i]);
            typed_dst[i] =
                *static_cast<const S*>(static_cast<const void*>(value));
        }
    }

    void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count) override {
        switch (data_type_) {
            case DataType::INT8: {
                BulkPrimitiveValueAtImpl<int8_t, int32_t>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::INT16: {
                BulkPrimitiveValueAtImpl<int16_t, int32_t>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::INT32: {
                BulkPrimitiveValueAtImpl<int32_t, int32_t>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::INT64: {
                BulkPrimitiveValueAtImpl<int64_t, int64_t>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::TIMESTAMPTZ: {
                BulkPrimitiveValueAtImpl<int64_t, int64_t>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::FLOAT: {
                BulkPrimitiveValueAtImpl<float, float>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::DOUBLE: {
                BulkPrimitiveValueAtImpl<double, double>(
                    op_ctx, dst, offsets, count);
                break;
            }
            case DataType::BOOL: {
                BulkPrimitiveValueAtImpl<bool, bool>(
                    op_ctx, dst, offsets, count);
                break;
            }
            default: {
                ThrowInfo(
                    ErrorCode::Unsupported,
                    "BulkScalarValueAt is not supported for unknown scalar "
                    "data type: {}",
                    data_type_);
            }
        }
    }

    void
    BulkVectorValueAt(milvus::OpContext* op_ctx,
                      void* dst,
                      const int64_t* offsets,
                      int64_t element_sizeof,
                      int64_t count) override {
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
        auto dst_vec = reinterpret_cast<char*>(dst);
        for (int64_t i = 0; i < count; i++) {
            auto chunk = ca->get_cell_of(cids[i]);
            auto value = chunk->ValueAt(offsets_in_chunk[i]);
            memcpy(dst_vec + i * element_sizeof, value, element_sizeof);
        }
    }

    PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, {chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<SpanBase>(
            ca, static_cast<FixedWidthChunk*>(chunk)->Span());
    }
};

template <typename T>
class ChunkedVariableColumn : public ChunkedColumnBase {
 public:
    static_assert(
        std::is_same_v<T, std::string> || std::is_same_v<T, Json>,
        "ChunkedVariableColumn only supports std::string or Json types");

    // memory mode ctor
    explicit ChunkedVariableColumn(
        std::unique_ptr<Translator<Chunk>> translator,
        const FieldMeta& field_meta)
        : ChunkedColumnBase(std::move(translator), field_meta) {
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(milvus::OpContext* op_ctx,
                int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, {chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            ca, static_cast<StringChunk*>(chunk)->StringViews(offset_len));
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const override {
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, {chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            ca, static_cast<StringChunk*>(chunk)->ViewsByOffsets(offsets));
    }

    void
    BulkRawStringAt(milvus::OpContext* op_ctx,
                    std::function<void(std::string_view, size_t, bool)> fn,
                    const int64_t* offsets,
                    int64_t count) const override {
        if constexpr (!std::is_same_v<T, std::string>) {
            ThrowInfo(ErrorCode::Unsupported,
                      "BulkRawStringAt only supported for "
                      "ChunkedVariableColumn<std::string>");
        }
        if (offsets == nullptr) {
            auto ca = SemiInlineGet(slot_->PinAllCells(op_ctx));
            for (int64_t i = 0; i < num_rows_; i++) {
                auto [cid, offset_in_chunk] = GetChunkIDByOffset(i);
                auto chunk = ca->get_cell_of(cid);
                auto valid = nullable_ ? chunk->isValid(offset_in_chunk) : true;
                fn(static_cast<StringChunk*>(chunk)->operator[](
                       offset_in_chunk),
                   i,
                   valid);
            }
        } else {
            auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
            auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
            for (int64_t i = 0; i < count; i++) {
                auto chunk = ca->get_cell_of(cids[i]);
                auto valid =
                    nullable_ ? chunk->isValid(offsets_in_chunk[i]) : true;
                fn(static_cast<StringChunk*>(chunk)->operator[](
                       offsets_in_chunk[i]),
                   i,
                   valid);
            }
        }
    }

    void
    BulkRawJsonAt(milvus::OpContext* op_ctx,
                  std::function<void(Json, size_t, bool)> fn,
                  const int64_t* offsets,
                  int64_t count) const override {
        if constexpr (!std::is_same_v<T, Json>) {
            ThrowInfo(
                ErrorCode::Unsupported,
                "RawJsonAt only supported for ChunkedVariableColumn<Json>");
        }
        if (count == 0) {
            return;
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
        for (int64_t i = 0; i < count; i++) {
            auto chunk = ca->get_cell_of(cids[i]);
            auto valid = nullable_ ? chunk->isValid(offsets_in_chunk[i]) : true;
            auto str_view = static_cast<StringChunk*>(chunk)->operator[](
                offsets_in_chunk[i]);
            fn(Json(str_view.data(), str_view.size()), i, valid);
        }
    }

    void
    BulkRawBsonAt(milvus::OpContext* op_ctx,
                  std::function<void(BsonView, uint32_t, uint32_t)> fn,
                  const uint32_t* row_offsets,
                  const uint32_t* value_offsets,
                  int64_t count) const override {
        if (count == 0) {
            return;
        }
        AssertInfo(row_offsets != nullptr && value_offsets != nullptr,
                   "row_offsets and value_offsets must be provided");

        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(row_offsets, count);
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
        for (int64_t i = 0; i < count; i++) {
            auto chunk = ca->get_cell_of(cids[i]);
            auto str_view = static_cast<StringChunk*>(chunk)->operator[](
                offsets_in_chunk[i]);
            fn(BsonView(reinterpret_cast<const uint8_t*>(str_view.data()),
                        str_view.size()),
               row_offsets[i],
               value_offsets[i]);
        }
    }
};

class ChunkedArrayColumn : public ChunkedColumnBase {
 public:
    // memory mode ctor
    explicit ChunkedArrayColumn(std::unique_ptr<Translator<Chunk>> translator,
                                const FieldMeta& field_meta)
        : ChunkedColumnBase(std::move(translator), field_meta) {
    }

    void
    BulkArrayAt(milvus::OpContext* op_ctx,
                std::function<void(ScalarFieldProto&&, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override {
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
        for (int64_t i = 0; i < count; i++) {
            auto array = static_cast<ArrayChunk*>(ca->get_cell_of(cids[i]))
                             ->View(offsets_in_chunk[i])
                             .output_data();
            fn(std::move(array), i);
        }
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(milvus::OpContext* op_ctx,
               int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const override {
        auto ca = SemiInlineGet(
            slot_->PinCells(op_ctx, {static_cast<cid_t>(chunk_id)}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            ca, static_cast<ArrayChunk*>(chunk)->Views(offset_len));
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const override {
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, {chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            ca, static_cast<ArrayChunk*>(chunk)->ViewsByOffsets(offsets));
    }
};

class ChunkedVectorArrayColumn : public ChunkedColumnBase {
 public:
    explicit ChunkedVectorArrayColumn(
        std::unique_ptr<Translator<Chunk>> translator,
        const FieldMeta& field_meta)
        : ChunkedColumnBase(std::move(translator), field_meta) {
    }

    void
    BulkVectorArrayAt(milvus::OpContext* op_ctx,
                      std::function<void(VectorFieldProto&&, size_t)> fn,
                      const int64_t* offsets,
                      int64_t count) const override {
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, cids));
        for (int64_t i = 0; i < count; i++) {
            auto array =
                static_cast<VectorArrayChunk*>(ca->get_cell_of(cids[i]))
                    ->View(offsets_in_chunk[i])
                    .output_data();
            fn(std::move(array), i);
        }
    }

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    VectorArrayViews(milvus::OpContext* op_ctx,
                     int64_t chunk_id,
                     std::optional<std::pair<int64_t, int64_t>> offset_len =
                         std::nullopt) const override {
        auto ca = SemiInlineGet(
            slot_->PinCells(op_ctx, {static_cast<cid_t>(chunk_id)}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<
            std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>(
            ca, static_cast<VectorArrayChunk*>(chunk)->Views(offset_len));
    }

    PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext* op_ctx,
                       int64_t chunk_id) const override {
        auto ca = SemiInlineGet(
            slot_->PinCells(op_ctx, {static_cast<cid_t>(chunk_id)}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<const size_t*>(
            ca, static_cast<VectorArrayChunk*>(chunk)->Offsets());
    }
};

inline std::shared_ptr<ChunkedColumnInterface>
MakeChunkedColumnBase(DataType data_type,
                      std::unique_ptr<Translator<milvus::Chunk>> translator,
                      const FieldMeta& field_meta) {
    if (ChunkedColumnInterface::IsChunkedVariableColumnDataType(data_type)) {
        if (data_type == DataType::JSON) {
            return std::static_pointer_cast<ChunkedColumnInterface>(
                std::make_shared<ChunkedVariableColumn<milvus::Json>>(
                    std::move(translator), field_meta));
        }
        return std::static_pointer_cast<ChunkedColumnInterface>(
            std::make_shared<ChunkedVariableColumn<std::string>>(
                std::move(translator), field_meta));
    }

    if (ChunkedColumnInterface::IsChunkedArrayColumnDataType(data_type)) {
        return std::static_pointer_cast<ChunkedColumnInterface>(
            std::make_shared<ChunkedArrayColumn>(std::move(translator),
                                                 field_meta));
    }

    if (ChunkedColumnInterface::IsChunkedVectorArrayColumnDataType(data_type)) {
        return std::static_pointer_cast<ChunkedColumnInterface>(
            std::make_shared<ChunkedVectorArrayColumn>(std::move(translator),
                                                       field_meta));
    }

    return std::static_pointer_cast<ChunkedColumnInterface>(
        std::make_shared<ChunkedColumn>(std::move(translator), field_meta));
}

}  // namespace milvus
