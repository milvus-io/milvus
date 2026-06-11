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

#include <limits>
#include <optional>
#include <string_view>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Span.h"
#include "common/Types.h"
#include "exec/operator/query-agg/AggChunkInput.h"

namespace milvus::exec {

class AggRawInput;

// Existing ColumnVector is the execution vector abstraction we are bypassing,
// and SpanBase/StringViews do not carry selected-row indirection by themselves.
// This view keeps only chunk-local raw storage plus the selected-row mapping
// needed to hash, compare, store, and update aggregates without projecting a
// compact ColumnVector first.
class AggRawColumnView {
 public:
    enum class Layout { kFixedWidth, kStringView };

    static AggRawColumnView
    FixedWidth(DataType type, const SpanBase& span) {
        AggRawColumnView view(type, Layout::kFixedWidth, false);
        view.span_ = span;
        return view;
    }

    // The string views and validity vector are borrowed. They must outlive the
    // raw aggregation call that consumes this column.
    static AggRawColumnView
    StringViews(DataType type,
                const std::vector<std::string_view>& views,
                const FixedVector<bool>& valid,
                bool compacted) {
        AssertInfo(type == DataType::VARCHAR || type == DataType::STRING,
                   "raw string view column must be VARCHAR or STRING");
        AggRawColumnView view(type, Layout::kStringView, compacted);
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
    ValidAt(vector_size_t row, const AggRawInput& input) const;

    template <typename T>
    T
    ValueAt(vector_size_t row, const AggRawInput& input) const {
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
    StringViewAt(vector_size_t row, const AggRawInput& input) const;

 private:
    AggRawColumnView(DataType type, Layout layout, bool compacted)
        : type_(type), layout_(layout), compacted_(compacted) {
    }

    vector_size_t
    ValueIndex(vector_size_t row, const AggRawInput& input) const;

    DataType type_{DataType::NONE};
    Layout layout_{Layout::kFixedWidth};
    bool compacted_{false};
    std::optional<SpanBase> span_;
    const std::vector<std::string_view>* string_views_{nullptr};
    const FixedVector<bool>* string_valid_{nullptr};
};

class AggRawInput {
 public:
    explicit AggRawInput(const AggSelectedChunk& chunk) {
        AssertInfo(
            chunk.row_count() <= std::numeric_limits<vector_size_t>::max(),
            "chunk row count is too large for raw aggregation input");
        AssertInfo(
            chunk.selected_count() <= std::numeric_limits<vector_size_t>::max(),
            "selected row count is too large for raw aggregation input");
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

    void
    AddFixedWidthColumn(DataType type, const SpanBase& span) {
        AssertInfo(span.row_count() >= row_count_,
                   "raw fixed-width span must cover chunk rows");
        columns_.emplace_back(AggRawColumnView::FixedWidth(type, span));
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
            AggRawColumnView::StringViews(type, views, valid, compacted));
    }

    const AggRawColumnView&
    child(column_index_t column_idx) const {
        AssertInfo(column_idx < columns_.size(),
                   "raw input column index out of range");
        return columns_[column_idx];
    }

    vector_size_t
    ValueIndex(vector_size_t row, bool compacted) const {
        AssertInfo(row < selected_count_,
                   "raw selected row index out of range");
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
    std::vector<AggRawColumnView> columns_;
};

inline vector_size_t
AggRawColumnView::ValueIndex(vector_size_t row,
                             const AggRawInput& input) const {
    return input.ValueIndex(row, compacted_);
}

inline bool
AggRawColumnView::ValidAt(vector_size_t row, const AggRawInput& input) const {
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
AggRawColumnView::StringViewAt(vector_size_t row,
                               const AggRawInput& input) const {
    AssertInfo(layout_ == Layout::kStringView,
               "raw string value can only be read from string views");
    AssertInfo(string_views_ != nullptr, "raw string views are not set");
    const auto index = ValueIndex(row, input);
    AssertInfo(index < static_cast<vector_size_t>(string_views_->size()),
               "raw string view index out of range");
    return (*string_views_)[index];
}

}  // namespace milvus::exec
