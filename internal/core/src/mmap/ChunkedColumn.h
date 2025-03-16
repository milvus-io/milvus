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
#include <filesystem>
#include <memory>
#include <queue>
#include <string>
#include <vector>
#include <math.h>

#include "common/Array.h"
#include "common/Chunk.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/File.h"
#include "common/FieldMeta.h"
#include "common/FieldData.h"
#include "common/Span.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "mmap/Utils.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Array.h"
#include "knowhere/dataset.h"
#include "monitor/prometheus_client.h"
#include "storage/MmapChunkManager.h"

#include "mmap/Column.h"
namespace milvus {

class ChunkedColumnBase : public ColumnBase {
 public:
    ChunkedColumnBase() = default;
    // memory mode ctor
    explicit ChunkedColumnBase(const FieldMeta& field_meta) {
        nullable_ = field_meta.is_nullable();
    }

    virtual ~ChunkedColumnBase() = default;

    void
    AppendBatch(const FieldDataPtr data) override {
        PanicInfo(ErrorCode::Unsupported, "AppendBatch not supported");
    }

    const char*
    Data(int chunk_id) const override {
        return chunks_[chunk_id]->Data();
    }

    virtual const char*
    ValueAt(int64_t offset) const {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
        return chunks_[chunk_id]->ValueAt(offset_in_chunk);
    };

    // MmappedData() returns the mmaped address
    const char*
    MmappedData() const override {
        AssertInfo(chunks_.size() == 1,
                   "only support one chunk, but got {} chunk(s)",
                   chunks_.size());
        return chunks_[0]->RawData();
    }

    bool
    IsValid(size_t offset) const {
        if (nullable_) {
            auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(offset);
            return chunks_[chunk_id]->isValid(offset_in_chunk);
        }
        return true;
    }

    bool
    IsValid(int64_t chunk_id, int64_t offset) const {
        if (nullable_) {
            return chunks_[chunk_id]->isValid(offset);
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
        return chunks_.size();
    }

    virtual void
    AddChunk(std::shared_ptr<Chunk> chunk) {
        num_rows_until_chunk_.push_back(num_rows_);
        num_rows_ += chunk->RowNums();
        chunks_.push_back(chunk);
    }

    size_t
    DataByteSize() const override {
        auto size = 0;
        for (auto& chunk : chunks_) {
            size += chunk->Size();
        }
        return size;
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const {
        return chunks_[chunk_id]->RowNums();
    }

    virtual SpanBase
    Span(int64_t chunk_id) const = 0;

    // used for sequential access for search
    virtual BufferView
    GetBatchBuffer(int64_t chunk_id, int64_t start_offset, int64_t length) {
        PanicInfo(ErrorCode::Unsupported,
                  "GetBatchBuffer only supported for VariableColumn");
    }

    virtual std::pair<std::vector<std::string_view>, FixedVector<bool>>
    StringViews(int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len) const {
        PanicInfo(ErrorCode::Unsupported,
                  "StringViews only supported for VariableColumn");
    }

    virtual std::pair<std::vector<ArrayView>, FixedVector<bool>>
    ArrayViews(int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len) const {
        PanicInfo(ErrorCode::Unsupported,
                  "ArrayViews only supported for ArrayChunkedColumn");
    }

    virtual std::pair<std::vector<std::string_view>, FixedVector<bool>>
    ViewsByOffsets(int64_t chunk_id,
                   const FixedVector<int32_t>& offsets) const {
        PanicInfo(ErrorCode::Unsupported,
                  "viewsbyoffsets only supported for VariableColumn");
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const {
        AssertInfo(offset < num_rows_,
                   "offset {} is out of range, num_rows: {}",
                   offset,
                   num_rows_);

        auto iter = std::lower_bound(num_rows_until_chunk_.begin(),
                                     num_rows_until_chunk_.end(),
                                     offset + 1);
        size_t chunk_idx =
            std::distance(num_rows_until_chunk_.begin(), iter) - 1;
        size_t offset_in_chunk = offset - num_rows_until_chunk_[chunk_idx];
        return {chunk_idx, offset_in_chunk};
    }

    std::shared_ptr<Chunk>
    GetChunk(int64_t chunk_id) const {
        return chunks_[chunk_id];
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const {
        return num_rows_until_chunk_[chunk_id];
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const {
        return num_rows_until_chunk_;
    }

 protected:
    bool nullable_{false};
    size_t num_rows_{0};
    std::vector<int64_t> num_rows_until_chunk_;

 private:
    // void
    // UpdateMetricWhenMmap(size_t mmaped_size) {
    //     UpdateMetricWhenMmap(mapping_type_, mmaped_size);
    // }

    // void
    // UpdateMetricWhenMmap(bool is_map_anonymous, size_t mapped_size) {
    //     if (mapping_type_ == MappingType::MAP_WITH_ANONYMOUS) {
    //         milvus::monitor::internal_mmap_allocated_space_bytes_anon.Observe(
    //             mapped_size);
    //         milvus::monitor::internal_mmap_in_used_space_bytes_anon.Increment(
    //             mapped_size);
    //     } else {
    //         milvus::monitor::internal_mmap_allocated_space_bytes_file.Observe(
    //             mapped_size);
    //         milvus::monitor::internal_mmap_in_used_space_bytes_file.Increment(
    //             mapped_size);
    //     }
    // }

    // void
    // UpdateMetricWhenMunmap(size_t mapped_size) {
    //     if (mapping_type_ == MappingType::MAP_WITH_ANONYMOUS) {
    //         milvus::monitor::internal_mmap_in_used_space_bytes_anon.Decrement(
    //             mapped_size);
    //     } else {
    //         milvus::monitor::internal_mmap_in_used_space_bytes_file.Decrement(
    //             mapped_size);
    //     }
    // }

 private:
    storage::MmapChunkManagerPtr mcm_ = nullptr;

 protected:
    std::vector<std::shared_ptr<Chunk>> chunks_;
};

class ChunkedColumn : public ChunkedColumnBase {
 public:
    ChunkedColumn() = default;
    // memory mode ctor
    explicit ChunkedColumn(const FieldMeta& field_meta)
        : ChunkedColumnBase(field_meta) {
    }

    explicit ChunkedColumn(const FieldMeta& field_meta,
                           const std::vector<std::shared_ptr<Chunk>>& chunks)
        : ChunkedColumnBase(field_meta) {
        for (auto& chunk : chunks) {
            AddChunk(chunk);
        }
    }

    ~ChunkedColumn() override = default;

    SpanBase
    Span(int64_t chunk_id) const override {
        return std::static_pointer_cast<FixedWidthChunk>(chunks_[chunk_id])
            ->Span();
    }
};

// when mmap is used, size_, data_ and num_rows_ of ColumnBase are used.
class ChunkedSparseFloatColumn : public ChunkedColumnBase {
 public:
    // memory mode ctor
    explicit ChunkedSparseFloatColumn(const FieldMeta& field_meta)
        : ChunkedColumnBase(field_meta) {
    }

    explicit ChunkedSparseFloatColumn(
        const FieldMeta& field_meta,
        const std::vector<std::shared_ptr<Chunk>>& chunks)
        : ChunkedColumnBase(field_meta) {
        for (auto& chunk : chunks) {
            AddChunk(chunk);
        }
    }

    ~ChunkedSparseFloatColumn() override = default;

    void
    AddChunk(std::shared_ptr<Chunk> chunk) override {
        num_rows_until_chunk_.push_back(num_rows_);
        num_rows_ += chunk->RowNums();
        chunks_.push_back(chunk);
        dim_ = std::max(
            dim_,
            std::static_pointer_cast<SparseFloatVectorChunk>(chunk)->Dim());
    }

    SpanBase
    Span(int64_t chunk_id) const override {
        PanicInfo(ErrorCode::Unsupported,
                  "Span not supported for sparse float column");
    }

    int64_t
    Dim() const {
        return dim_;
    }

 private:
    int64_t dim_ = 0;
};

template <typename T>
class ChunkedVariableColumn : public ChunkedColumnBase {
 public:
    using ViewType =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    // memory mode ctor
    explicit ChunkedVariableColumn(const FieldMeta& field_meta)
        : ChunkedColumnBase(field_meta) {
    }

    explicit ChunkedVariableColumn(
        const FieldMeta& field_meta,
        const std::vector<std::shared_ptr<Chunk>>& chunks)
        : ChunkedColumnBase(field_meta) {
        for (auto& chunk : chunks) {
            AddChunk(chunk);
        }
    }

    ~ChunkedVariableColumn() override = default;

    SpanBase
    Span(int64_t chunk_id) const override {
        PanicInfo(ErrorCode::NotImplemented,
                  "span() interface is not implemented for variable column");
    }

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    StringViews(int64_t chunk_id,
                std::optional<std::pair<int64_t, int64_t>> offset_len =
                    std::nullopt) const override {
        return std::static_pointer_cast<StringChunk>(chunks_[chunk_id])
            ->StringViews(offset_len);
    }

    std::pair<std::vector<std::string_view>, FixedVector<bool>>
    ViewsByOffsets(int64_t chunk_id,
                   const FixedVector<int32_t>& offsets) const override {
        return std::static_pointer_cast<StringChunk>(chunks_[chunk_id])
            ->ViewsByOffsets(offsets);
    }

    BufferView
    GetBatchBuffer(int64_t chunk_id,
                   int64_t start_offset,
                   int64_t length) override {
        BufferView buffer_view;
        std::vector<BufferView::Element> elements;
        elements.push_back(
            {chunks_[chunk_id]->Data(),
             std::static_pointer_cast<StringChunk>(chunks_[chunk_id])
                 ->Offsets(),
             static_cast<int>(start_offset),
             static_cast<int>(start_offset + length)});

        buffer_view.data_ = elements;
        return buffer_view;
    }

    ViewType
    operator[](const int i) const {
        if (i < 0 || i > num_rows_) {
            PanicInfo(ErrorCode::OutOfRange, "index out of range");
        }

        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(i);
        std::string_view str_view = std::static_pointer_cast<StringChunk>(chunks_[chunk_id])->operator[](offset_in_chunk);
        return ViewType(str_view.data(), str_view.size());
    }

    std::string_view
    RawAt(const int i) const {
        return std::string_view((*this)[i]);
    }
};
class ChunkedArrayColumn : public ChunkedColumnBase {
 public:
    // memory mode ctor
    explicit ChunkedArrayColumn(const FieldMeta& field_meta)
        : ChunkedColumnBase(field_meta) {
    }

    explicit ChunkedArrayColumn(
        const FieldMeta& field_meta,
        const std::vector<std::shared_ptr<Chunk>>& chunks)
        : ChunkedColumnBase(field_meta) {
        for (auto& chunk : chunks) {
            AddChunk(chunk);
        }
    }

    ~ChunkedArrayColumn() override = default;

    SpanBase
    Span(int64_t chunk_id) const override {
        PanicInfo(ErrorCode::NotImplemented,
                  "span() interface is not implemented for arr chunk column");
    }

    ArrayView
    operator[](const int i) const {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(i);
        return std::static_pointer_cast<ArrayChunk>(chunks_[chunk_id])
            ->View(offset_in_chunk);
    }

    ScalarArray
    RawAt(const int i) const {
        auto [chunk_id, offset_in_chunk] = GetChunkIDByOffset(i);
        return std::static_pointer_cast<ArrayChunk>(chunks_[chunk_id])
            ->View(offset_in_chunk)
            .output_data();
    }

    std::pair<std::vector<ArrayView>, FixedVector<bool>>
    ArrayViews(int64_t chunk_id,
               std::optional<std::pair<int64_t, int64_t>> offset_len =
                   std::nullopt) const override {
        return std::static_pointer_cast<ArrayChunk>(chunks_[chunk_id])
            ->Views(offset_len);
    }
};
}  // namespace milvus