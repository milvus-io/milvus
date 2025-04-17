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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>
#include <math.h>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"

#include "common/Array.h"
#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/Span.h"
#include "common/Array.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/storagev2translator/GroupCTMeta.h"

namespace milvus {

using GroupChunkVector = std::vector<std::shared_ptr<GroupChunk>>;

using namespace milvus::cachinglayer;

// ChunkedColumnGroup represents a collection of group chunks
class ChunkedColumnGroup {
 public:
    explicit ChunkedColumnGroup(
        std::unique_ptr<Translator<GroupChunk>> translator)
        : slot_(Manager::GetInstance().CreateCacheSlot(std::move(translator))) {
        num_chunks_ = slot_->num_cells();
        num_rows_ = GetNumRowsUntilChunk().back();
    }

    virtual ~ChunkedColumnGroup() = default;

    // Get the number of group chunks
    size_t
    num_chunks() const {
        return num_chunks_;
    }

    PinWrapper<GroupChunk*>
    GetGroupChunk(int64_t chunk_id) const {
        auto ca = SemiInlineGet(slot_->PinCells({chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<GroupChunk*>(ca, chunk);
    }

    int64_t
    NumRows() const {
        return num_rows_;
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const {
        return GetNumRowsUntilChunk()[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const {
        auto meta =
            static_cast<milvus::segcore::storagev2translator::GroupCTMeta*>(
                slot_->meta());
        return meta->num_rows_until_chunk_;
    }

 protected:
    mutable std::shared_ptr<CacheSlot<GroupChunk>> slot_;
    size_t num_chunks_{0};
    size_t num_rows_{0};
};

class ProxyChunkColumn : public ChunkedColumnInterface {
 public:
    explicit ProxyChunkColumn(std::shared_ptr<ChunkedColumnGroup> group,
                              FieldId field_id,
                              const FieldMeta& field_meta)
        : group_(group),
          field_id_(field_id),
          field_meta_(field_meta),
          data_type_(field_meta.get_data_type()) {
    }

    PinWrapper<const char*>
    DataOfChunk(int chunk_id) const override {
        auto group_chunk = group_->GetGroupChunk(chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return PinWrapper<const char*>(group_chunk, chunk->Data());
    }

    bool
    IsValid(size_t offset) const override {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
        auto group_chunk = group_->GetGroupChunk(chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return chunk->isValid(offset_in_chunk);
    }

    bool
    IsValid(int64_t chunk_id, int64_t offset) const override {
        auto group_chunk = group_->GetGroupChunk(chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return chunk->isValid(offset);
    }

    bool
    IsNullable() const override {
        return field_meta_.is_nullable();
    }

    size_t
    NumRows() const override {
        return group_->NumRows();
    }

    int64_t
    num_chunks() const override {
        return group_->num_chunks();
    }

    size_t
    DataByteSize() const override {
        size_t total_size = 0;
        for (int64_t i = 0; i < num_chunks(); ++i) {
            auto group_chunk = group_->GetGroupChunk(i);
            auto chunk = group_chunk.get()->GetChunk(field_id_);
            total_size += chunk->Size();
        }
        return total_size;
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        return group_->GetNumRowsUntilChunk(chunk_id + 1) -
               group_->GetNumRowsUntilChunk(chunk_id);
    }

    PinWrapper<SpanBase>
    Span(int64_t chunk_id) const override {
        if (!IsChunkedColumnDataType(data_type_)) {
            PanicInfo(ErrorCode::Unsupported,
                      "Span only supported for ChunkedColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<SpanBase>(
            chunk_wrapper, static_cast<FixedWidthChunk*>(chunk.get())->Span());
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        if (!IsChunkedVariableColumnDataType(data_type_)) {
            PanicInfo(ErrorCode::Unsupported,
                      "StringViews only supported for ChunkedVariableColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            chunk_wrapper,
            static_cast<StringChunk*>(chunk.get())->StringViews(offset_len));
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const override {
        if (!IsChunkedArrayColumnDataType(data_type_)) {
            PanicInfo(ErrorCode::Unsupported,
                      "ArrayViews only supported for ChunkedArrayColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            chunk_wrapper,
            static_cast<ArrayChunk*>(chunk.get())->Views(offset_len));
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    ViewsByOffsets(int64_t chunk_id,
                   const FixedVector<int32_t>& offsets) const override {
        if (!IsChunkedVariableColumnDataType(data_type_)) {
            PanicInfo(
                ErrorCode::Unsupported,
                "ViewsByOffsets only supported for ChunkedVariableColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            chunk_wrapper,
            static_cast<StringChunk*>(chunk.get())->ViewsByOffsets(offsets));
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        int64_t current_offset = 0;
        for (int64_t i = 0; i < num_chunks(); ++i) {
            auto rows = chunk_row_nums(i);
            if (current_offset + rows > offset) {
                return {i, offset - current_offset};
            }
            current_offset += rows;
        }
        return {num_chunks() - 1, chunk_row_nums(num_chunks() - 1) - 1};
    }

    PinWrapper<Chunk*>
    GetChunk(int64_t chunk_id) const override {
        auto group_chunk = group_->GetGroupChunk(chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return PinWrapper<Chunk*>(group_chunk, chunk.get());
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        return group_->GetNumRowsUntilChunk(chunk_id);
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        return group_->GetNumRowsUntilChunk();
    }

    const char*
    ValueAt(int64_t offset) override {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
        auto chunk = GetChunk(chunk_id);
        return chunk.get()->ValueAt(offset_in_chunk);
    }

    template <typename T>
    T
    RawAt(size_t i) const {
        if (!IsChunkedVariableColumnDataType(data_type_)) {
            PanicInfo(ErrorCode::Unsupported,
                      "RawAt only supported for ChunkedVariableColumn");
        }
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(i);
        auto chunk = group_->GetGroupChunk(chunk_id).get()->GetChunk(field_id_);
        std::string_view str_view =
            static_cast<StringChunk*>(chunk.get())->operator[](offset_in_chunk);
        return T(str_view.data(), str_view.size());
    }

    ScalarArray
    PrimitivieRawAt(const int i) const override {
        if (!IsChunkedArrayColumnDataType(data_type_)) {
            PanicInfo(ErrorCode::Unsupported,
                      "PrimitivieRawAt only supported for ChunkedArrayColumn");
        }
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(i);
        auto chunk = group_->GetGroupChunk(chunk_id).get()->GetChunk(field_id_);
        return static_cast<ArrayChunk*>(chunk.get())
            ->View(offset_in_chunk)
            .output_data();
    }

 private:
    std::shared_ptr<ChunkedColumnGroup> group_;
    FieldId field_id_;
    const FieldMeta& field_meta_;
    DataType data_type_;
};

}  // namespace milvus