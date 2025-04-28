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
#include "common/Array.h"
#include "segcore/storagev1translator/ChunkTranslator.h"

namespace milvus {

using namespace milvus::cachinglayer;

std::pair<size_t, size_t> inline GetChunkIDByOffset(
    int64_t offset, std::vector<int64_t>& num_rows_until_chunk) {
    AssertInfo(offset >= 0 && offset < num_rows_until_chunk.back(),
               "offset is out of range, offset: {}, num rows: {}",
               offset,
               num_rows_until_chunk.back());
    auto iter = std::lower_bound(
        num_rows_until_chunk.begin(), num_rows_until_chunk.end(), offset + 1);
    size_t chunk_idx = std::distance(num_rows_until_chunk.begin(), iter) - 1;
    size_t offset_in_chunk = offset - num_rows_until_chunk[chunk_idx];
    return {chunk_idx, offset_in_chunk};
}

class ChunkedColumnBase {
 public:
    // memory mode ctor
    explicit ChunkedColumnBase(std::unique_ptr<Translator<Chunk>> translator,
                               const FieldMeta& field_meta)
        : nullable_(field_meta.is_nullable()),
          num_chunks_(translator->num_cells()),
          slot_(Manager::GetInstance().CreateCacheSlot(std::move(translator))) {
        num_rows_ = GetNumRowsUntilChunk().back();
    }

    virtual ~ChunkedColumnBase() = default;

    PinWrapper<const char*>
    DataOfChunk(int chunk_id) const {
        auto ca = SemiInlineGet(slot_->PinCells({chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<const char*>(ca, chunk->Data());
    }

    bool
    IsValid(size_t offset) const {
        if (!nullable_) {
            return true;
        }
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
        return IsValid(chunk_id, offset_in_chunk);
    }

    bool
    IsValid(int64_t chunk_id, int64_t offset) const {
        if (nullable_) {
            auto ca =
                SemiInlineGet(slot_->PinCells({static_cast<cid_t>(chunk_id)}));
            auto chunk = ca->get_cell_of(chunk_id);
            return chunk->isValid(offset);
        }
        return true;
    }

    bool
    IsNullable() const {
        return nullable_;
    }

    size_t
    NumRows() const {
        return num_rows_;
    };

    int64_t
    num_chunks() const {
        return num_chunks_;
    }

    // This returns only memory byte size.
    size_t
    DataByteSize() const {
        auto size = 0;
        for (auto i = 0; i < num_chunks_; i++) {
            size += slot_->size_of_cell(i).memory_bytes;
        }
        return size;
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const {
        return GetNumRowsUntilChunk(chunk_id + 1) -
               GetNumRowsUntilChunk(chunk_id);
    }

    virtual PinWrapper<SpanBase>
    Span(int64_t chunk_id) const {
        PanicInfo(ErrorCode::Unsupported,
                  "Span only supported for ChunkedColumn");
    }

    virtual PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const {
        PanicInfo(ErrorCode::Unsupported,
                  "StringViews only supported for VariableColumn");
    }

    virtual PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len) const {
        PanicInfo(ErrorCode::Unsupported,
                  "ArrayViews only supported for ArrayChunkedColumn");
    }

    virtual PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    ViewsByOffsets(int64_t chunk_id,
                   const FixedVector<int32_t>& offsets) const {
        PanicInfo(ErrorCode::Unsupported,
                  "ViewsByOffsets only supported for VariableColumn");
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const {
        AssertInfo(offset < num_rows_,
                   "offset {} is out of range, num_rows: {}",
                   offset,
                   num_rows_);
        auto num_rows_until_chunk = GetNumRowsUntilChunk();
        return ::milvus::GetChunkIDByOffset(offset, num_rows_until_chunk);
    }

    PinWrapper<Chunk*>
    GetChunk(int64_t chunk_id) const {
        auto ca = SemiInlineGet(slot_->PinCells({chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<Chunk*>(ca, chunk);
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const {
        return GetNumRowsUntilChunk()[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const {
        auto meta = static_cast<milvus::segcore::storagev1translator::CTMeta*>(
            slot_->meta());
        return meta->num_rows_until_chunk_;
    }

 protected:
    bool nullable_{false};
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

    // TODO(tiered storage 1): this method should be replaced with a bulk access method.
    const char*
    ValueAt(int64_t offset) {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
        auto ca =
            SemiInlineGet(slot_->PinCells({static_cast<cid_t>(chunk_id)}));
        auto chunk = ca->get_cell_of(chunk_id);
        return chunk->ValueAt(offset_in_chunk);
    }

    PinWrapper<SpanBase>
    Span(int64_t chunk_id) const override {
        auto ca = SemiInlineGet(slot_->PinCells({chunk_id}));
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
    StringViews(int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        auto ca = SemiInlineGet(slot_->PinCells({chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            ca, static_cast<StringChunk*>(chunk)->StringViews(offset_len));
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    ViewsByOffsets(int64_t chunk_id,
                   const FixedVector<int32_t>& offsets) const override {
        auto ca = SemiInlineGet(slot_->PinCells({chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            ca, static_cast<StringChunk*>(chunk)->ViewsByOffsets(offsets));
    }

    // TODO(tiered storage 1): this method should be replaced with a bulk access method.
    // RawAt is called in three cases:
    // 1. bulk_subscript, pass in an offset array, access the specified rows.
    // 2. load, create skip index or text index, access all rows. (SkipIndex.h and CreateTextIndex)
    // 3. GetJsonData, json related index will use this, see if it can be modified to batch access or batch pin.
    T
    RawAt(const size_t i) const {
        if (i < 0 || i > num_rows_) {
            PanicInfo(ErrorCode::OutOfRange, "index out of range");
        }

        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(i);
        auto ca =
            SemiInlineGet(slot_->PinCells({static_cast<cid_t>(chunk_id)}));
        auto chunk = ca->get_cell_of(chunk_id);
        std::string_view str_view =
            static_cast<StringChunk*>(chunk)->operator[](offset_in_chunk);
        return T(str_view.data(), str_view.size());
    }
};

class ChunkedArrayColumn : public ChunkedColumnBase {
 public:
    // memory mode ctor
    explicit ChunkedArrayColumn(std::unique_ptr<Translator<Chunk>> translator,
                                const FieldMeta& field_meta)
        : ChunkedColumnBase(std::move(translator), field_meta) {
    }

    // TODO(tiered storage 1): this method should be replaced with a bulk access method.
    ScalarArray
    RawAt(const int i) const {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(i);
        auto ca =
            SemiInlineGet(slot_->PinCells({static_cast<cid_t>(chunk_id)}));
        auto chunk = ca->get_cell_of(chunk_id);
        return static_cast<ArrayChunk*>(chunk)
            ->View(offset_in_chunk)
            .output_data();
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const override {
        auto ca =
            SemiInlineGet(slot_->PinCells({static_cast<cid_t>(chunk_id)}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            ca, static_cast<ArrayChunk*>(chunk)->Views(offset_len));
    }
};
}  // namespace milvus
