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

#include "cachinglayer/CacheSlot.h"
#include "common/Array.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorArray.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/SegmentInterface.h"

namespace milvus::exec {

// Shared selected-row raw input contract for query operators that consume the
// upstream bitmap directly. This promotes the former aggregation-only chunk/raw
// views so AggregationNode and QueryOrderByNode do not maintain parallel raw
// segment access paths.
class SelectedChunk {
 public:
    SelectedChunk(int64_t chunk_id,
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

class SelectedChunkInput {
 public:
    SelectedChunkInput() = default;

    static SelectedChunkInput
    FromChunkBounds(const std::vector<int64_t>& num_rows_until_chunk,
                    TargetBitmapView filter_mask) {
        SelectedChunkInput input;
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

    static SelectedChunkInput
    FromChunkedColumn(const ChunkedColumnInterface& column,
                      TargetBitmapView filter_mask) {
        return FromChunkBounds(column.GetNumRowsUntilChunk(), filter_mask);
    }

    static SelectedChunkInput
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

    const std::vector<SelectedChunk>&
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
    std::vector<SelectedChunk> chunks_;
    int64_t selected_count_{0};
};

class RawChunkedColumnView {
 public:
    RawChunkedColumnView(milvus::OpContext* op_ctx,
                         const ChunkedColumnInterface& column)
        : op_ctx_(op_ctx), column_(column) {
    }

    cachinglayer::PinWrapper<SpanBase>
    PinSpan(const SelectedChunk& chunk) const {
        return column_.Span(op_ctx_, chunk.chunk_id());
    }

    cachinglayer::PinWrapper<
        std::pair<std::vector<std::string_view>, FixedVector<bool>>>
    PinStringViews(const SelectedChunk& chunk) const {
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
    PinArrayViews(const SelectedChunk& chunk) const {
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

class RawSegmentChunkView {
 public:
    RawSegmentChunkView(milvus::OpContext* op_ctx,
                        const segcore::SegmentInternalInterface& segment)
        : op_ctx_(op_ctx), segment_(segment) {
    }

    template <typename T>
    cachinglayer::PinWrapper<Span<T>>
    PinSpan(FieldId field_id, const SelectedChunk& chunk) const {
        return segment_.chunk_data<T>(op_ctx_, field_id, chunk.chunk_id());
    }

    template <typename ViewType>
    cachinglayer::PinWrapper<
        std::pair<std::vector<ViewType>, FixedVector<bool>>>
    PinViews(FieldId field_id, const SelectedChunk& chunk) const {
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

class RawInput;

class RawColumnView {
 public:
    enum class Layout { kFixedWidth, kStringView };

    static RawColumnView
    FixedWidth(DataType type, const SpanBase& span) {
        RawColumnView view(type, Layout::kFixedWidth, false);
        view.span_ = span;
        return view;
    }

    static RawColumnView
    StringViews(DataType type,
                const std::vector<std::string_view>& views,
                const FixedVector<bool>& valid,
                bool compacted) {
        AssertInfo(type == DataType::VARCHAR || type == DataType::STRING,
                   "raw string view column must be VARCHAR or STRING");
        RawColumnView view(type, Layout::kStringView, compacted);
        view.string_views_ = &views;
        view.string_valid_ = &valid;
        return view;
    }

    DataType
    type() const {
        return type_;
    }

    Layout
    layout() const {
        return layout_;
    }

    bool
    ValidAt(vector_size_t row, const RawInput& input) const;

    template <typename T>
    T
    ValueAt(vector_size_t row, const RawInput& input) const {
        AssertInfo(
            layout_ == Layout::kFixedWidth,
            "raw primitive value can only be read from fixed-width span");
        AssertInfo(span_.has_value(), "raw fixed-width span is not set");
        AssertInfo(span_->element_sizeof() == static_cast<int64_t>(sizeof(T)),
                   "raw span element size does not match requested value type");
        const auto index = ValueIndex(row, input);
        AssertInfo(index < span_->row_count(), "raw span index out of range");
        return *(reinterpret_cast<const T*>(span_->data()) + index);
    }

    std::string_view
    StringViewAt(vector_size_t row, const RawInput& input) const;

 private:
    RawColumnView(DataType type, Layout layout, bool compacted)
        : type_(type), layout_(layout), compacted_(compacted) {
    }

    vector_size_t
    ValueIndex(vector_size_t row, const RawInput& input) const;

    DataType type_{DataType::NONE};
    Layout layout_{Layout::kFixedWidth};
    bool compacted_{false};
    std::optional<SpanBase> span_;
    const std::vector<std::string_view>* string_views_{nullptr};
    const FixedVector<bool>* string_valid_{nullptr};
};

class RawInput {
 public:
    explicit RawInput(const SelectedChunk& chunk) {
        AssertInfo(chunk.row_count() <=
                       static_cast<int64_t>(
                           std::numeric_limits<vector_size_t>::max()),
                   "chunk row count is too large for raw input");
        AssertInfo(chunk.selected_count() <=
                       static_cast<int64_t>(
                           std::numeric_limits<vector_size_t>::max()),
                   "selected row count is too large for raw input");
        row_count_ = static_cast<vector_size_t>(chunk.row_count());
        selected_count_ = static_cast<vector_size_t>(chunk.selected_count());
        if (!chunk.all_selected()) {
            filter_mask_ = chunk.filter_mask();
        }
    }

    vector_size_t
    selected_count() const {
        return selected_count_;
    }

    vector_size_t
    row_count() const {
        return row_count_;
    }

    size_t
    column_count() const {
        return columns_.size();
    }

    void
    AddFixedWidthColumn(DataType type, const SpanBase& span) {
        AssertInfo(span.row_count() >= row_count_,
                   "raw fixed-width span must cover chunk rows");
        columns_.emplace_back(RawColumnView::FixedWidth(type, span));
    }

    void
    AddStringColumn(DataType type,
                    const std::vector<std::string_view>& views,
                    const FixedVector<bool>& valid,
                    bool compacted) {
        if (compacted) {
            AssertInfo(views.size() == static_cast<size_t>(selected_count_),
                       "compacted raw string views must match selected rows");
            AssertInfo(
                valid.empty() ||
                    valid.size() == static_cast<size_t>(selected_count_),
                "compacted raw string validity must match selected rows");
        } else {
            AssertInfo(views.size() >= static_cast<size_t>(row_count_),
                       "raw string views must cover chunk rows");
            AssertInfo(valid.empty() ||
                           valid.size() >= static_cast<size_t>(row_count_),
                       "raw string validity must cover chunk rows");
        }
        columns_.emplace_back(
            RawColumnView::StringViews(type, views, valid, compacted));
    }

    const RawColumnView&
    child(column_index_t column_idx) const {
        AssertInfo(column_idx < columns_.size(),
                   "raw input column index out of range");
        return columns_[column_idx];
    }

    vector_size_t
    ValueIndex(vector_size_t row, bool compacted) const {
        AssertInfo(row < selected_count_, "raw selected row index out of range");
        if (compacted || filter_mask_.empty()) {
            return row;
        }
        if (!offset_cache_valid_ || row < cached_selected_index_) {
            auto first = filter_mask_.find_first(false);
            AssertInfo(first.has_value(),
                       "sparse raw input must contain selected rows");
            cached_selected_index_ = 0;
            cached_chunk_offset_ = static_cast<vector_size_t>(*first);
            offset_cache_valid_ = true;
        }
        while (cached_selected_index_ < row) {
            auto next = filter_mask_.find_next(cached_chunk_offset_, false);
            AssertInfo(next.has_value(),
                       "failed to locate selected row in raw input");
            cached_chunk_offset_ = static_cast<vector_size_t>(*next);
            ++cached_selected_index_;
        }
        return cached_chunk_offset_;
    }

 private:
    vector_size_t row_count_{0};
    vector_size_t selected_count_{0};
    TargetBitmapView filter_mask_;
    mutable bool offset_cache_valid_{false};
    mutable vector_size_t cached_selected_index_{0};
    mutable vector_size_t cached_chunk_offset_{0};
    std::vector<RawColumnView> columns_;
};

inline vector_size_t
RawColumnView::ValueIndex(vector_size_t row, const RawInput& input) const {
    return input.ValueIndex(row, compacted_);
}

inline bool
RawColumnView::ValidAt(vector_size_t row, const RawInput& input) const {
    const auto index = ValueIndex(row, input);
    if (layout_ == Layout::kFixedWidth) {
        AssertInfo(span_.has_value(), "raw fixed-width span is not set");
        AssertInfo(index < span_->row_count(), "raw span index out of range");
        const auto* valid = span_->valid_data();
        return valid == nullptr || valid[index];
    }

    AssertInfo(string_views_ != nullptr, "raw string views are not set");
    AssertInfo(index < static_cast<vector_size_t>(string_views_->size()),
               "raw string view index out of range");
    return string_valid_ == nullptr || string_valid_->empty() ||
           (*string_valid_)[index];
}

inline std::string_view
RawColumnView::StringViewAt(vector_size_t row, const RawInput& input) const {
    AssertInfo(layout_ == Layout::kStringView,
               "raw string value can only be read from string views");
    AssertInfo(string_views_ != nullptr, "raw string views are not set");
    const auto index = ValueIndex(row, input);
    AssertInfo(index < static_cast<vector_size_t>(string_views_->size()),
               "raw string view index out of range");
    return (*string_views_)[index];
}

struct RawColumnPinHolderBase {
    virtual ~RawColumnPinHolderBase() = default;
};

template <typename T>
struct RawColumnPinHolder final : RawColumnPinHolderBase {
    explicit RawColumnPinHolder(cachinglayer::PinWrapper<T>&& pin)
        : pin_(std::move(pin)) {
    }

    cachinglayer::PinWrapper<T> pin_;
};

template <typename T>
struct MissingFixedRawColumnHolder final : RawColumnPinHolderBase {
    explicit MissingFixedRawColumnHolder(int64_t row_count)
        : values_(row_count),
          valid_(row_count, false),
          span_(values_.data(), valid_.data(), row_count, sizeof(T)) {
    }

    std::vector<T> values_;
    FixedVector<bool> valid_;
    SpanBase span_;
};

struct MissingStringRawColumnHolder final : RawColumnPinHolderBase {
    explicit MissingStringRawColumnHolder(int64_t row_count)
        : views_(row_count), valid_(row_count, false) {
    }

    std::vector<std::string_view> views_;
    FixedVector<bool> valid_;
};

struct MissingBoolRawColumnHolder final : RawColumnPinHolderBase {
    explicit MissingBoolRawColumnHolder(int64_t row_count)
        : values_(std::make_unique<bool[]>(row_count)),
          valid_(row_count, false),
          span_(values_.get(), valid_.data(), row_count, sizeof(bool)) {
    }

    std::unique_ptr<bool[]> values_;
    FixedVector<bool> valid_;
    SpanBase span_;
};

struct GrowingStringRawColumnPinHolder final : RawColumnPinHolderBase {
    GrowingStringRawColumnPinHolder(
        cachinglayer::PinWrapper<Span<std::string>>&& pin, int64_t row_count)
        : pin_(std::move(pin)) {
        auto span = pin_.get();
        views_.reserve(row_count);
        for (int64_t i = 0; i < row_count; ++i) {
            views_.emplace_back(span.data()[i]);
        }
        if (span.valid_data() != nullptr) {
            valid_.reserve(row_count);
            for (int64_t i = 0; i < row_count; ++i) {
                valid_.emplace_back(span.valid_data()[i]);
            }
        }
    }

    cachinglayer::PinWrapper<Span<std::string>> pin_;
    std::vector<std::string_view> views_;
    FixedVector<bool> valid_;
};

struct SegmentOffsetRawColumnHolder final : RawColumnPinHolderBase {
    explicit SegmentOffsetRawColumnHolder(const SelectedChunk& chunk)
        : values_(chunk.row_count()),
          span_(values_.data(), nullptr, chunk.row_count(), sizeof(int64_t)) {
        for (int64_t i = 0; i < chunk.row_count(); ++i) {
            values_[i] = chunk.segment_offset() + i;
        }
    }

    std::vector<int64_t> values_;
    SpanBase span_;
};

template <DataType Type>
void
AddMissingRawColumn(RawInput& input,
                    std::vector<std::unique_ptr<RawColumnPinHolderBase>>& pins,
                    int64_t row_count) {
    if constexpr (Type == DataType::VARCHAR || Type == DataType::STRING) {
        auto holder = std::make_unique<MissingStringRawColumnHolder>(row_count);
        input.AddStringColumn(Type, holder->views_, holder->valid_, false);
        pins.emplace_back(std::move(holder));
    } else if constexpr (Type == DataType::BOOL) {
        auto holder = std::make_unique<MissingBoolRawColumnHolder>(row_count);
        input.AddFixedWidthColumn(Type, holder->span_);
        pins.emplace_back(std::move(holder));
    } else if constexpr (Type == DataType::INT8 || Type == DataType::INT16 ||
                         Type == DataType::INT32 || Type == DataType::INT64 ||
                         Type == DataType::TIMESTAMPTZ ||
                         Type == DataType::FLOAT || Type == DataType::DOUBLE) {
        using NativeType = typename TypeTraits<Type>::NativeType;
        auto holder =
            std::make_unique<MissingFixedRawColumnHolder<NativeType>>(row_count);
        input.AddFixedWidthColumn(Type, holder->span_);
        pins.emplace_back(std::move(holder));
    } else {
        ThrowInfo(DataTypeInvalid,
                  "raw input does not support input type {}",
                  Type);
    }
}

template <DataType Type>
void
AddRawColumn(RawInput& input,
             std::vector<std::unique_ptr<RawColumnPinHolderBase>>& pins,
             const segcore::SegmentInternalInterface& segment,
             OpContext* op_context,
             FieldId field_id,
             const SelectedChunk& chunk) {
    if (!segment.is_field_exist(field_id)) {
        AddMissingRawColumn<Type>(input, pins, chunk.row_count());
        return;
    }
    if constexpr (Type == DataType::VARCHAR || Type == DataType::STRING) {
        if (segment.type() == SegmentType::Growing) {
            auto pin = segment.chunk_data<std::string>(
                op_context, field_id, chunk.chunk_id());
            auto holder = std::make_unique<GrowingStringRawColumnPinHolder>(
                std::move(pin), chunk.row_count());
            input.AddStringColumn(Type, holder->views_, holder->valid_, false);
            pins.emplace_back(std::move(holder));
        } else {
            using PinValue =
                std::pair<std::vector<std::string_view>, FixedVector<bool>>;
            auto pin = RawSegmentChunkView(op_context, segment)
                           .PinViews<std::string_view>(field_id, chunk);
            auto holder =
                std::make_unique<RawColumnPinHolder<PinValue>>(std::move(pin));
            input.AddStringColumn(Type,
                                  holder->pin_.get().first,
                                  holder->pin_.get().second,
                                  !chunk.all_selected());
            pins.emplace_back(std::move(holder));
        }
    } else if constexpr (Type == DataType::BOOL) {
        auto pin =
            segment.chunk_data<bool>(op_context, field_id, chunk.chunk_id());
        auto holder =
            std::make_unique<RawColumnPinHolder<Span<bool>>>(std::move(pin));
        input.AddFixedWidthColumn(Type, holder->pin_.get());
        pins.emplace_back(std::move(holder));
    } else if constexpr (Type == DataType::INT8 || Type == DataType::INT16 ||
                         Type == DataType::INT32 || Type == DataType::INT64 ||
                         Type == DataType::TIMESTAMPTZ ||
                         Type == DataType::FLOAT || Type == DataType::DOUBLE) {
        using NativeType = typename TypeTraits<Type>::NativeType;
        auto pin = segment.chunk_data<NativeType>(
            op_context, field_id, chunk.chunk_id());
        auto holder = std::make_unique<RawColumnPinHolder<Span<NativeType>>>(
            std::move(pin));
        input.AddFixedWidthColumn(Type, holder->pin_.get());
        pins.emplace_back(std::move(holder));
    } else {
        ThrowInfo(DataTypeInvalid,
                  "raw input does not support input type {}",
                  Type);
    }
}

inline void
AddRawColumn(RawInput& input,
             std::vector<std::unique_ptr<RawColumnPinHolderBase>>& pins,
             const segcore::SegmentInternalInterface& segment,
             OpContext* op_context,
             FieldId field_id,
             DataType type,
             const SelectedChunk& chunk) {
    MILVUS_DYNAMIC_TYPE_DISPATCH(
        AddRawColumn, type, input, pins, segment, op_context, field_id, chunk);
}

inline void
AddSegmentOffsetRawColumn(
    RawInput& input,
    std::vector<std::unique_ptr<RawColumnPinHolderBase>>& pins,
    const SelectedChunk& chunk) {
    auto holder = std::make_unique<SegmentOffsetRawColumnHolder>(chunk);
    input.AddFixedWidthColumn(DataType::INT64, holder->span_);
    pins.emplace_back(std::move(holder));
}

}  // namespace milvus::exec
