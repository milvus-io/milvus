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

#include <gtest/gtest.h>

#include "mmap/ChunkedColumn.h"
#include "segcore/ChunkedSegmentSealedImpl.h"

namespace milvus {

// Synthetic, generation-owned summaries for Scan/Take contract tests. Real
// Storage V1 columns intentionally expose no metrics; this test column does.
class TestMetricsColumn : public ChunkedColumnInterface {
 public:
    TestMetricsColumn(std::shared_ptr<ChunkedColumnInterface> column,
                      SkipMetricsList metrics)
        : column_(std::move(column)), metrics_(std::move(metrics)) {
    }

    const index::FieldChunkMetrics*
    GetSkipMetrics(int64_t chunk_id) const override {
        return chunk_id >= 0 && static_cast<size_t>(chunk_id) < metrics_.size()
                   ? metrics_[chunk_id].get()
                   : nullptr;
    }

    std::optional<const SkipMetricsList*>
    GetSkipMetricsList() const override {
        return &metrics_;
    }

    cachinglayer::PinWrapper<const char*>
    DataOfChunk(milvus::OpContext* op_ctx, int chunk_id) const override {
        return column_->DataOfChunk(op_ctx, chunk_id);
    }

    bool
    IsValid(milvus::OpContext* op_ctx, size_t offset) const override {
        return column_->IsValid(op_ctx, offset);
    }

    void
    BulkIsValid(milvus::OpContext* ctx,
                std::function<void(bool, size_t)> fn,
                const int64_t* offsets,
                int64_t count) const override {
        return column_->BulkIsValid(ctx, fn, offsets, count);
    }

    bool
    IsNullable() const override {
        return column_->IsNullable();
    }

    size_t
    NumRows() const override {
        return column_->NumRows();
    }

    int64_t
    num_chunks() const override {
        return column_->num_chunks();
    }

    size_t
    DataByteSize() const override {
        return column_->DataByteSize();
    }

    int64_t
    chunk_row_nums(int64_t chunk_id) const override {
        return column_->chunk_row_nums(chunk_id);
    }

    PinWrapper<SpanBase>
    Span(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        return column_->Span(op_ctx, chunk_id);
    }

    void
    PrefetchChunks(milvus::OpContext* op_ctx,
                   const std::vector<int64_t>& chunk_ids) const override {
        return column_->PrefetchChunks(op_ctx, chunk_ids);
    }

    bool
    CellsLoaded(const int64_t* offsets, int64_t count) const override {
        return column_->CellsLoaded(offsets, count);
    }

    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    StringViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        return column_->StringViews(op_ctx, chunk_id, offset_len);
    }

    PinWrapper<std::pair<std::vector<ArrayView>, ValidityView>>
    ArrayViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        return column_->ArrayViews(op_ctx, chunk_id, offset_len);
    }

    PinWrapper<std::pair<std::vector<ArrayValueView>, ValidityView>>
    ArrayValueViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        return column_->ArrayValueViews(op_ctx, chunk_id, offset_len);
    }

    PinWrapper<std::pair<std::vector<VectorArrayView>, ValidityView>>
    VectorArrayViews(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        return column_->VectorArrayViews(op_ctx, chunk_id, offset_len);
    }

    PinWrapper<const size_t*>
    VectorArrayOffsets(milvus::OpContext* op_ctx,
                       int64_t chunk_id) const override {
        return column_->VectorArrayOffsets(op_ctx, chunk_id);
    }

    PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    StringViewsByOffsets(milvus::OpContext* op_ctx,
                         int64_t chunk_id,
                         const FixedVector<int32_t>& offsets) const override {
        return column_->StringViewsByOffsets(op_ctx, chunk_id, offsets);
    }

    PinWrapper<std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    ArrayViewsByOffsets(milvus::OpContext* op_ctx,
                        int64_t chunk_id,
                        const FixedVector<int32_t>& offsets) const override {
        return column_->ArrayViewsByOffsets(op_ctx, chunk_id, offsets);
    }

    PinWrapper<std::pair<std::vector<ArrayValueView>, FixedVector<bool>>>
    ArrayValueViewsByOffsets(
        milvus::OpContext* op_ctx,
        int64_t chunk_id,
        const FixedVector<int32_t>& offsets) const override {
        return column_->ArrayValueViewsByOffsets(op_ctx, chunk_id, offsets);
    }

    std::pair<size_t, size_t>
    GetChunkIDByOffset(int64_t offset) const override {
        return column_->GetChunkIDByOffset(offset);
    }

    std::pair<std::vector<milvus::cachinglayer::cid_t>, std::vector<int64_t>>
    GetChunkIDsByOffsets(const int64_t* offsets, int64_t count) const override {
        return column_->GetChunkIDsByOffsets(offsets, count);
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        return column_->GetChunk(op_ctx, chunk_id);
    }

    std::vector<PinWrapper<Chunk*>>
    GetAllChunks(milvus::OpContext* op_ctx) const override {
        return column_->GetAllChunks(op_ctx);
    }

    int64_t
    GetNumRowsUntilChunk(int64_t chunk_id) const override {
        return column_->GetNumRowsUntilChunk(chunk_id);
    }

    const std::vector<int64_t>&
    GetNumRowsUntilChunk() const override {
        return column_->GetNumRowsUntilChunk();
    }

    void
    BulkValueAt(milvus::OpContext* op_ctx,
                std::function<void(const char*, size_t)> fn,
                const int64_t* offsets,
                int64_t count) override {
        return column_->BulkValueAt(op_ctx, fn, offsets, count);
    }

    void
    BulkPrimitiveValueAt(milvus::OpContext* op_ctx,
                         void* dst,
                         const int64_t* offsets,
                         int64_t count,
                         bool small_int_raw_type) override {
        return column_->BulkPrimitiveValueAt(
            op_ctx, dst, offsets, count, small_int_raw_type);
    }

    void
    BulkVectorValueAt(milvus::OpContext* op_ctx,
                      void* dst,
                      const int64_t* offsets,
                      int64_t element_sizeof,
                      int64_t count) override {
        return column_->BulkVectorValueAt(
            op_ctx, dst, offsets, element_sizeof, count);
    }

    ScanResult
    Scan(milvus::OpContext* ctx, const ScanOptions& options) const override {
        return column_->Scan(ctx, options);
    }

    TakeResultPtr
    Take(milvus::OpContext* ctx, TakeOptions options) const override {
        return column_->Take(ctx, std::move(options));
    }

 private:
    std::shared_ptr<ChunkedColumnInterface> column_;
    SkipMetricsList metrics_;
};

inline void
InstallTestSkipMetrics(segcore::SegmentSealed* segment,
                       FieldId field_id,
                       SkipMetricsList metrics = {}) {
    auto* sealed = dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment);
    ASSERT_NE(sealed, nullptr);
    auto runtime = sealed->TestCloneMutableRuntimeResourceState();
    auto column = runtime->fields.at(field_id);
    if (metrics.empty()) {
        metrics.resize(column->num_chunks(),
                       std::make_shared<index::NoneFieldChunkMetrics>());
    }
    ASSERT_EQ(metrics.size(), column->num_chunks());
    runtime->fields[field_id] = std::make_shared<TestMetricsColumn>(
        std::move(column), std::move(metrics));
    sealed->TestPublishRuntimeResourceState(std::move(runtime));
}

}  // namespace milvus
