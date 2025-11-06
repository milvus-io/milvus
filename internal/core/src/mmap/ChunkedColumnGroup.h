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
#include <cmath>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"

#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Span.h"
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

    void
    ManualEvictCache() const {
        slot_->ManualEvictAll();
    }

    // Get the number of group chunks
    size_t
    num_chunks() const {
        return num_chunks_;
    }

    PinWrapper<GroupChunk*>
    GetGroupChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const {
        auto ca = SemiInlineGet(slot_->PinCells(op_ctx, {chunk_id}));
        auto chunk = ca->get_cell_of(chunk_id);
        return PinWrapper<GroupChunk*>(ca, chunk);
    }

    std::shared_ptr<CellAccessor<GroupChunk>>
    GetGroupChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) {
        return SemiInlineGet(slot_->PinCells(op_ctx, chunk_ids));
    }

    std::vector<PinWrapper<GroupChunk*>>
    GetAllGroupChunks(milvus::OpContext* op_ctx) {
        auto ca = SemiInlineGet(slot_->PinAllCells(op_ctx));
        std::vector<PinWrapper<GroupChunk*>> ret;
        ret.reserve(num_chunks_);
        for (size_t i = 0; i < num_chunks_; i++) {
            auto chunk = ca->get_cell_of(i);
            ret.emplace_back(ca, chunk);
        }
        return ret;
    }

    int64_t
    NumRows() const {
        return num_rows_;
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const {
        AssertInfo(
            chunk_id >= 0 && chunk_id <= num_chunks_,
            "[StorageV2] chunk_id out of range: " + std::to_string(chunk_id));
        return GetNumRowsUntilChunk()[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const {
        auto meta =
            static_cast<milvus::segcore::storagev2translator::GroupCTMeta*>(
                slot_->meta());
        return meta->num_rows_until_chunk_;
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const {
        const auto& num_rows_until_chunk = GetNumRowsUntilChunk();
        auto iter = std::lower_bound(num_rows_until_chunk.begin(),
                                     num_rows_until_chunk.end(),
                                     offset + 1);
        size_t chunk_idx =
            std::distance(num_rows_until_chunk.begin(), iter) - 1;
        size_t offset_in_chunk = offset - num_rows_until_chunk[chunk_idx];
        return {chunk_idx, offset_in_chunk};
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) {
        const auto& num_rows_until_chunk = GetNumRowsUntilChunk();
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

    size_t
    NumFieldsInGroup() const {
        auto meta =
            static_cast<milvus::segcore::storagev2translator::GroupCTMeta*>(
                slot_->meta());
        return meta->num_fields_;
    }

    size_t
    memory_size() const {
        auto meta =
            static_cast<milvus::segcore::storagev2translator::GroupCTMeta*>(
                slot_->meta());
        size_t memory_size = 0;
        for (auto& size : meta->chunk_memory_size_) {
            memory_size += size;
        }
        return memory_size;
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

    void
    ManualEvictCache() const override {
        if (group_->NumFieldsInGroup() == 1) {
            group_->ManualEvictCache();
        }
    }

    PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const override {
        auto group_chunk = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return PinWrapper<const char*>(group_chunk, chunk->Data());
    }

    bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const override {
        auto [chunk_id, offset_in_chunk] = group_->GetChunkIDByOffset(offset);
        auto group_chunk = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return chunk->isValid(offset_in_chunk);
    }

    void
    BulkIsValid(milvus::OpContext* op_ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets = nullptr,
                int64_t count = 0) const override {
        if (!field_meta_.is_nullable()) {
            if (offsets == nullptr) {
                for (int64_t i = 0; i < group_->NumRows(); i++) {
                    fn(true, i);
                }
            } else {
                for (int64_t i = 0; i < count; i++) {
                    fn(true, i);
                }
            }
        }
        // nullable:
        if (count == 0) {
            return;
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto valid = chunk->isValid(offsets_in_chunk[i]);
            fn(valid, i);
        }
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
        return group_->memory_size();
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        return group_->GetNumRowsUntilChunk(chunk_id + 1) -
               group_->GetNumRowsUntilChunk(chunk_id);
    }

    // TODO(tiered storage): make it async
    void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const override {
        group_->GetGroupChunks(op_ctx, chunk_ids);
    }

    PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        if (!IsChunkedColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] Span only supported for ChunkedColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<SpanBase>(
            chunk_wrapper, static_cast<FixedWidthChunk*>(chunk.get())->Span());
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViews(milvus::OpContext* op_ctx,
                int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        if (!IsChunkedVariableColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] StringViews only supported for "
                      "ChunkedVariableColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            chunk_wrapper,
            static_cast<StringChunk*>(chunk.get())->StringViews(offset_len));
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViews(milvus::OpContext* op_ctx,
               int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const override {
        if (!IsChunkedArrayColumnDataType(data_type_)) {
            ThrowInfo(
                ErrorCode::Unsupported,
                "[StorageV2] ArrayViews only supported for ChunkedArrayColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            chunk_wrapper,
            static_cast<ArrayChunk*>(chunk.get())->Views(offset_len));
    }

    PinWrapper<std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>
    VectorArrayViews(milvus::OpContext* op_ctx,
                     int64_t chunk_id,
                     std::optional<std::pair<int64_t, int64_t>> offset_len =
                         std::nullopt) const override {
        if (!IsChunkedVectorArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] VectorArrayViews only supported for "
                      "ChunkedVectorArrayColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<
            std::pair<std::vector<VectorArrayView>, FixedVector<bool>>>(
            chunk_wrapper,
            static_cast<VectorArrayChunk*>(chunk.get())->Views(offset_len));
    }

    PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext* op_ctx,
                       int64_t chunk_id) const override {
        if (!IsChunkedVectorArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "VectorArrayOffsets only supported for "
                      "ChunkedVectorArrayColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<const size_t*>(
            chunk_wrapper,
            static_cast<VectorArrayChunk*>(chunk.get())->Offsets());
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const override {
        if (!IsChunkedVariableColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] ViewsByOffsets only supported for "
                      "ChunkedVariableColumn");
        }
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>(
            chunk_wrapper,
            static_cast<StringChunk*>(chunk.get())->ViewsByOffsets(offsets));
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const override {
        auto chunk_wrapper = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = chunk_wrapper.get()->GetChunk(field_id_);
        return PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>(
            chunk_wrapper,
            static_cast<ArrayChunk*>(chunk.get())->ViewsByOffsets(offsets));
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        return group_->GetChunkIDByOffset(offset);
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const override {
        return group_->GetChunkIDsByOffsets(offsets, count);
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        auto group_chunk = group_->GetGroupChunk(op_ctx, chunk_id);
        auto chunk = group_chunk.get()->GetChunk(field_id_);
        return PinWrapper<Chunk*>(group_chunk, chunk.get());
    }

    std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext* op_ctx) const override {
        std::vector<PinWrapper<Chunk*>> ret;
        auto group_chunks = group_->GetAllGroupChunks(op_ctx);
        ret.reserve(group_chunks.size());

        for (auto& group_chunk : group_chunks) {
            auto chunk = group_chunk.get()->GetChunk(field_id_);
            ret.emplace_back(group_chunk, chunk.get());
        }
        return ret;
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        return group_->GetNumRowsUntilChunk(chunk_id);
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        return group_->GetNumRowsUntilChunk();
    }

    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override {
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            fn(chunk->ValueAt(offsets_in_chunk[i]), i);
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
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        auto typed_dst = static_cast<T*>(dst);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
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
                ThrowInfo(ErrorCode::Unsupported,
                          "[StorageV2] BulkScalarValueAt is not supported for "
                          "unknown scalar "
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
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        auto dst_vec = reinterpret_cast<char*>(dst);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto value = chunk->ValueAt(offsets_in_chunk[i]);
            memcpy(dst_vec + i * element_sizeof, value, element_sizeof);
        }
    }

    void
    BulkRawStringAt(milvus::OpContext* op_ctx,
                    std::function<void(std::string_view, size_t, bool)> fn,
                    const int64_t* offsets = nullptr,
                    int64_t count = 0) const override {
        if (!IsChunkedVariableColumnDataType(data_type_) ||
            data_type_ == DataType::JSON) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] BulkRawStringAt only supported for "
                      "ProxyChunkColumn of "
                      "variable length type(except Json)");
        }
        if (offsets == nullptr) {
            int64_t current_offset = 0;
            for (cid_t cid = 0; cid < num_chunks(); ++cid) {
                auto group_chunk = group_->GetGroupChunk(op_ctx, cid);
                auto chunk = group_chunk.get()->GetChunk(field_id_);
                auto chunk_rows = chunk->RowNums();
                for (int64_t i = 0; i < chunk_rows; ++i) {
                    auto valid = chunk->isValid(i);
                    auto value =
                        static_cast<StringChunk*>(chunk.get())->operator[](i);
                    fn(value, current_offset + i, valid);
                }
                current_offset += chunk_rows;
            }
        } else {
            auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
            auto ca = group_->GetGroupChunks(op_ctx, cids);
            for (int64_t i = 0; i < count; i++) {
                auto* group_chunk = ca->get_cell_of(cids[i]);
                auto chunk = group_chunk->GetChunk(field_id_);
                auto valid = chunk->isValid(offsets_in_chunk[i]);
                auto value = static_cast<StringChunk*>(chunk.get())
                                 ->
                                 operator[](offsets_in_chunk[i]);
                fn(value, i, valid);
            }
        }
    }

    // TODO(tiered storage 2): replace with Bulk version
    void
    BulkRawJsonAt(milvus::OpContext* op_ctx,
                  std::function<void(Json, size_t, bool)> fn,
                  const int64_t* offsets,
                  int64_t count) const override {
        if (data_type_ != DataType::JSON) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] RawJsonAt only supported for "
                      "ProxyChunkColumn of Json type");
        }
        if (count == 0) {
            return;
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);

        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto valid = chunk->isValid(offsets_in_chunk[i]);
            auto str_view = static_cast<StringChunk*>(chunk.get())
                                ->
                                operator[](offsets_in_chunk[i]);
            fn(Json(str_view.data(), str_view.size()), i, valid);
        }
    }

    void
    BulkRawBsonAt(milvus::OpContext* op_ctx,
                  std::function<void(BsonView, uint32_t, uint32_t)> fn,
                  const uint32_t* row_offsets,
                  const uint32_t* value_offsets,
                  int64_t count) const override {
        if (data_type_ != DataType::STRING) {
            ThrowInfo(ErrorCode::Unsupported,
                      "BulkRawBsonAt only supported for ProxyChunkColumn of "
                      "Bson type");
        }
        if (count == 0) {
            return;
        }

        AssertInfo(row_offsets != nullptr, "row_offsets is nullptr");
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(row_offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);

        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto str_view = static_cast<StringChunk*>(chunk.get())
                                ->
                                operator[](offsets_in_chunk[i]);
            fn(BsonView(reinterpret_cast<const uint8_t*>(str_view.data()),
                        str_view.size()),
               row_offsets[i],
               value_offsets[i]);
        }
    }

    void
    BulkArrayAt(milvus::OpContext* op_ctx,
                std::function<void(ScalarFieldProto&&, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override {
        if (!IsChunkedArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] BulkArrayAt only supported for "
                      "ChunkedArrayColumn");
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto array = static_cast<ArrayChunk*>(chunk.get())
                             ->View(offsets_in_chunk[i])
                             .output_data();
            fn(std::move(array), i);
        }
    }

    void
    BulkVectorArrayAt(milvus::OpContext* op_ctx,
                      std::function<void(VectorFieldProto&&, size_t)> fn,
                      const int64_t* offsets,
                      int64_t count) const override {
        if (!IsChunkedVectorArrayColumnDataType(data_type_)) {
            ThrowInfo(ErrorCode::Unsupported,
                      "[StorageV2] BulkVectorArrayAt only supported for "
                      "ChunkedVectorArrayColumn");
        }
        auto [cids, offsets_in_chunk] = ToChunkIdAndOffset(offsets, count);
        auto ca = group_->GetGroupChunks(op_ctx, cids);
        for (int64_t i = 0; i < count; i++) {
            auto* group_chunk = ca->get_cell_of(cids[i]);
            auto chunk = group_chunk->GetChunk(field_id_);
            auto array = static_cast<VectorArrayChunk*>(chunk.get())
                             ->View(offsets_in_chunk[i])
                             .output_data();
            fn(std::move(array), i);
        }
    }

 private:
    std::shared_ptr<ChunkedColumnGroup> group_;
    FieldId field_id_;
    const FieldMeta field_meta_;
    DataType data_type_;
};

}  // namespace milvus