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
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/VectorArray.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/SegmentInterface.h"

namespace milvus::exec {

// Query aggregation reuses TargetBitmap as the selected-row mask because it is
// the existing pipeline selection primitive. RowContainer stores aggregation
// state, and std::vector<int64_t> is the current full-offset materialization;
// this chunk-local contract avoids reusing either as the input abstraction.
class AggSelectedChunk {
 public:
    AggSelectedChunk(int64_t chunk_id,
                     int64_t segment_offset,
                     int64_t row_count,
                     int64_t selected_count,
                     bool all_selected,
                     TargetBitmapView filter_mask)
        : chunk_id_(chunk_id),
          segment_offset_(segment_offset),
          row_count_(row_count),
          selected_count_(selected_count),
          all_selected_(all_selected),
          filter_mask_(filter_mask) {
        AssertInfo(chunk_id_ >= 0, "chunk id must be non-negative");
        AssertInfo(segment_offset_ >= 0, "segment offset must be non-negative");
        AssertInfo(row_count_ >= 0, "row count must be non-negative");
        AssertInfo(row_count_ <= std::numeric_limits<int32_t>::max(),
                   "chunk row count is too large for chunk-local offsets");
        AssertInfo(selected_count_ > 0,
                   "selected chunk must have at least one selected row");
        AssertInfo(selected_count_ <= row_count_,
                   "selected count must not exceed row count");
        if (all_selected_) {
            AssertInfo(selected_count_ == row_count_,
                       "all-selected chunk must select every row");
        } else {
            AssertInfo(filter_mask_.size() == static_cast<size_t>(row_count_),
                       "sparse selected chunk must keep its chunk filter view");
        }
    }

    int64_t
    chunk_id() const {
        return chunk_id_;
    }

    int64_t
    segment_offset() const {
        return segment_offset_;
    }

    int64_t
    row_count() const {
        return row_count_;
    }

    bool
    all_selected() const {
        return all_selected_;
    }

    int64_t
    selected_count() const {
        return selected_count_;
    }

    TargetBitmapView
    filter_mask() const {
        return filter_mask_;
    }

    FixedVector<int32_t>
    MaterializeSelectedOffsets() const {
        FixedVector<int32_t> offsets;
        offsets.reserve(selected_count_);
        ForEachSelectedOffset(
            [&](int32_t offset) { offsets.emplace_back(offset); });
        return offsets;
    }

    template <typename Fn>
    void
    ForEachSelectedOffset(Fn&& fn) const {
        if (all_selected_) {
            for (int32_t offset = 0; offset < row_count_; ++offset) {
                fn(offset);
            }
            return;
        }
        auto offset = filter_mask_.find_first(false);
        while (offset.has_value()) {
            fn(static_cast<int32_t>(*offset));
            offset = filter_mask_.find_next(*offset, false);
        }
    }

 private:
    int64_t chunk_id_;
    int64_t segment_offset_;
    int64_t row_count_;
    int64_t selected_count_;
    bool all_selected_;
    TargetBitmapView filter_mask_;
};

class AggChunkInput {
 public:
    AggChunkInput() = default;

    // The filter mask follows the query pipeline convention: true means the row
    // is filtered out, false means the row is selected for downstream work.
    static AggChunkInput
    FromChunkBounds(const std::vector<int64_t>& num_rows_until_chunk,
                    TargetBitmapView filter_mask) {
        AggChunkInput input;
        if (filter_mask.empty()) {
            return input;
        }

        AssertInfo(!num_rows_until_chunk.empty(),
                   "chunk bounds must not be empty");
        AssertInfo(num_rows_until_chunk.front() == 0,
                   "chunk bounds must start from 0");

        const auto total_rows = static_cast<int64_t>(filter_mask.size());
        AssertInfo(num_rows_until_chunk.back() == total_rows,
                   "chunk bounds must exactly cover the selected mask");
        for (int64_t i = 0;
             i + 1 < static_cast<int64_t>(num_rows_until_chunk.size());
             ++i) {
            const auto begin = num_rows_until_chunk[i];
            const auto end = num_rows_until_chunk[i + 1];
            AssertInfo(end >= begin, "chunk bounds must be monotonic");
            if (end <= begin) {
                continue;
            }

            auto chunk_filter = filter_mask.view(
                static_cast<size_t>(begin), static_cast<size_t>(end - begin));
            const auto row_count = end - begin;
            const auto filtered_count =
                static_cast<int64_t>(chunk_filter.count());
            const auto selected_count = row_count - filtered_count;
            if (selected_count == 0) {
                continue;
            }

            const bool all_selected = selected_count == row_count;
            input.selected_count_ += selected_count;
            input.chunks_.emplace_back(
                i,
                begin,
                row_count,
                selected_count,
                all_selected,
                all_selected ? TargetBitmapView{} : chunk_filter);
        }
        return input;
    }

    static AggChunkInput
    FromChunkedColumn(const ChunkedColumnInterface& column,
                      TargetBitmapView filter_mask) {
        return FromChunkBounds(column.GetNumRowsUntilChunk(), filter_mask);
    }

    static AggChunkInput
    FromSegment(const segcore::SegmentInternalInterface& segment,
                FieldId anchor_field,
                TargetBitmapView filter_mask) {
        std::vector<int64_t> bounds;
        const auto num_chunks = segment.num_chunk_data(anchor_field);
        bounds.reserve(num_chunks + 1);
        for (int64_t i = 0; i < num_chunks; ++i) {
            bounds.emplace_back(segment.num_rows_until_chunk(anchor_field, i));
        }
        bounds.emplace_back(segment.type() == SegmentType::Growing
                                ? static_cast<int64_t>(filter_mask.size())
                                : segment.num_rows_until_chunk(anchor_field,
                                                               num_chunks));
        return FromChunkBounds(bounds, filter_mask);
    }

    const std::vector<AggSelectedChunk>&
    chunks() const {
        return chunks_;
    }

    bool
    empty() const {
        return chunks_.empty();
    }

    int64_t
    selected_count() const {
        return selected_count_;
    }

 private:
    std::vector<AggSelectedChunk> chunks_;
    int64_t selected_count_{0};
};

class AggChunkedColumnView {
 public:
    AggChunkedColumnView(milvus::OpContext* op_ctx,
                         const ChunkedColumnInterface& column)
        : op_ctx_(op_ctx), column_(column) {
    }

    cachinglayer::PinWrapper<SpanBase>
    PinSpan(const AggSelectedChunk& chunk) const {
        return column_.Span(op_ctx_, chunk.chunk_id());
    }

    cachinglayer::PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    PinStringViews(const AggSelectedChunk& chunk) const {
        if (chunk.all_selected()) {
            return column_.StringViews(
                op_ctx_,
                chunk.chunk_id(),
                std::make_pair<int64_t, int64_t>(0, chunk.row_count()));
        }
        auto offsets = chunk.MaterializeSelectedOffsets();
        return column_.StringViewsByOffsets(op_ctx_, chunk.chunk_id(), offsets);
    }

    cachinglayer::PinWrapper<
        std::pair<std::vector<ArrayView>, FixedVector<bool>>>
    PinArrayViews(const AggSelectedChunk& chunk) const {
        if (chunk.all_selected()) {
            return column_.ArrayViews(
                op_ctx_,
                chunk.chunk_id(),
                std::make_pair<int64_t, int64_t>(0, chunk.row_count()));
        }
        auto offsets = chunk.MaterializeSelectedOffsets();
        return column_.ArrayViewsByOffsets(op_ctx_, chunk.chunk_id(), offsets);
    }

 private:
    milvus::OpContext* op_ctx_;
    const ChunkedColumnInterface& column_;
};

class AggSegmentChunkView {
 public:
    AggSegmentChunkView(milvus::OpContext* op_ctx,
                        const segcore::SegmentInternalInterface& segment)
        : op_ctx_(op_ctx), segment_(segment) {
    }

    template <typename T>
    cachinglayer::PinWrapper<Span<T>>
    PinSpan(FieldId field_id, const AggSelectedChunk& chunk) const {
        return segment_.chunk_data<T>(op_ctx_, field_id, chunk.chunk_id());
    }

    template <typename ViewType>
    cachinglayer::PinWrapper<
        std::pair<std::vector<ViewType>, FixedVector<bool>>>
    PinViews(FieldId field_id, const AggSelectedChunk& chunk) const {
        if (chunk.all_selected()) {
            return segment_.get_batch_views<ViewType>(
                op_ctx_, field_id, chunk.chunk_id(), 0, chunk.row_count());
        }
        auto offsets = chunk.MaterializeSelectedOffsets();
        return segment_.get_views_by_offsets<ViewType>(
            op_ctx_, field_id, chunk.chunk_id(), offsets);
    }

 private:
    milvus::OpContext* op_ctx_;
    const segcore::SegmentInternalInterface& segment_;
};

}  // namespace milvus::exec
