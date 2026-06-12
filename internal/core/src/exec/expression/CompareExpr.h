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

#include <fmt/core.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/common.h"
#include "cachinglayer/CacheSlot.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "index/Index.h"
#include "mmap/ChunkedColumnInterface.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

template <typename T>
inline constexpr bool IsCompareStringViewType =
    std::is_same_v<std::remove_cv_t<T>, std::string_view>;

template <typename T, typename U, proto::plan::OpType op>
inline bool
CompareColumnValues(const T& left, const U& right) {
    if constexpr (op == proto::plan::OpType::Equal) {
        return left == right;
    } else if constexpr (op == proto::plan::OpType::NotEqual) {
        return left != right;
    } else if constexpr (op == proto::plan::OpType::GreaterThan) {
        return left > right;
    } else if constexpr (op == proto::plan::OpType::LessThan) {
        return left < right;
    } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
        return left >= right;
    } else if constexpr (op == proto::plan::OpType::LessEqual) {
        return left <= right;
    } else if constexpr (op == proto::plan::OpType::PrefixMatch) {
        if constexpr (IsCompareStringViewType<T> &&
                      IsCompareStringViewType<U>) {
            return PrefixMatch(left, right);
        } else {
            ThrowInfo(OpTypeInvalid,
                      "PrefixMatch only supports string compare expr");
        }
    } else {
        ThrowInfo(OpTypeInvalid,
                  fmt::format("unsupported op_type:{} for compare expr", op));
    }
    return false;
}

template <typename T,
          typename U,
          proto::plan::OpType op,
          FilterType filter_type>
struct CompareElementFunc {
    void
    operator()(const T* left,
               const U* right,
               size_t size,
               TargetBitmapView res,
               const TargetBitmap& bitmap_input,
               size_t start_cursor,
               const int32_t* offsets = nullptr) {
        // This is the original code, kept here for the documentation purposes
        // also, used for iterative filter
        if constexpr (filter_type == FilterType::random) {
            for (int i = 0; i < size; ++i) {
                auto offset = (offsets != nullptr) ? offsets[i] : i;
                res[i] =
                    CompareColumnValues<T, U, op>(left[offset], right[offset]);
            }
            return;
        }

        if (!bitmap_input.empty()) {
            for (int i = 0; i < size; ++i) {
                if (!bitmap_input[start_cursor + i]) {
                    continue;
                }
                res[i] = CompareColumnValues<T, U, op>(left[i], right[i]);
            }
            return;
        }

        if constexpr (IsCompareStringViewType<T> ||
                      IsCompareStringViewType<U>) {
            for (int i = 0; i < size; ++i) {
                res[i] = CompareColumnValues<T, U, op>(left[i], right[i]);
            }
            return;
        } else {
            if constexpr (op == proto::plan::OpType::Equal) {
                res.inplace_compare_column<T,
                                           U,
                                           milvus::bitset::CompareOpType::EQ>(
                    left, right, size);
            } else if constexpr (op == proto::plan::OpType::NotEqual) {
                res.inplace_compare_column<T,
                                           U,
                                           milvus::bitset::CompareOpType::NE>(
                    left, right, size);
            } else if constexpr (op == proto::plan::OpType::GreaterThan) {
                res.inplace_compare_column<T,
                                           U,
                                           milvus::bitset::CompareOpType::GT>(
                    left, right, size);
            } else if constexpr (op == proto::plan::OpType::LessThan) {
                res.inplace_compare_column<T,
                                           U,
                                           milvus::bitset::CompareOpType::LT>(
                    left, right, size);
            } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
                res.inplace_compare_column<T,
                                           U,
                                           milvus::bitset::CompareOpType::GE>(
                    left, right, size);
            } else if constexpr (op == proto::plan::OpType::LessEqual) {
                res.inplace_compare_column<T,
                                           U,
                                           milvus::bitset::CompareOpType::LE>(
                    left, right, size);
            } else {
                ThrowInfo(
                    OpTypeInvalid,
                    fmt::format(
                        "unsupported op_type:{} for CompareElementFunc", op));
            }
        }
    }
};

class PhyCompareFilterExpr : public Expr {
 public:
    PhyCompareFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::CompareExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size)
        : Expr(DataType::BOOL, std::move(input), name, op_ctx),
          left_field_(expr->left_field_id_),
          right_field_(expr->right_field_id_),
          segment_chunk_reader_(op_ctx, segment, active_count),
          batch_size_(batch_size),
          expr_(expr) {
        auto& schema = segment->get_schema();
        auto& left_field_meta = schema[left_field_];
        auto& right_field_meta = schema[right_field_];
        pinned_index_left_ = PinIndex(op_ctx_, segment, left_field_meta);
        pinned_index_right_ = PinIndex(op_ctx_, segment, right_field_meta);
        is_left_indexed_ = pinned_index_left_.size() > 0;
        is_right_indexed_ = pinned_index_right_.size() > 0;
        if (segment->is_chunked()) {
            left_num_chunk_ =
                is_left_indexed_ ? pinned_index_left_.size()
                : segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(left_field_);
            right_num_chunk_ =
                is_right_indexed_ ? pinned_index_right_.size()
                : segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(right_field_);
            num_chunk_ = left_num_chunk_;
        } else {
            num_chunk_ = is_left_indexed_
                             ? pinned_index_left_.size()
                             : upper_div(segment_chunk_reader_.active_count_,
                                         segment_chunk_reader_.SizePerChunk());
        }

        AssertInfo(
            batch_size_ > 0,
            fmt::format("expr batch size should greater than zero, but now: {}",
                        batch_size_));
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursorForIndexed(int64_t& pos) {
        pos = pos + batch_size_ >= segment_chunk_reader_.active_count_
                  ? segment_chunk_reader_.active_count_
                  : pos + batch_size_;
    }

    void
    MoveCursor() override {
        if (!has_offset_input_) {
            if (segment_chunk_reader_.segment_->is_chunked()) {
                if (is_left_indexed_) {
                    MoveCursorForIndexed(left_current_chunk_pos_);
                } else {
                    segment_chunk_reader_.MoveCursorForMultipleChunk(
                        left_current_chunk_id_,
                        left_current_chunk_pos_,
                        left_field_,
                        left_num_chunk_,
                        batch_size_);
                }
                if (is_right_indexed_) {
                    MoveCursorForIndexed(right_current_chunk_pos_);
                } else {
                    segment_chunk_reader_.MoveCursorForMultipleChunk(
                        right_current_chunk_id_,
                        right_current_chunk_pos_,
                        right_field_,
                        right_num_chunk_,
                        batch_size_);
                }
            } else {
                segment_chunk_reader_.MoveCursorForSingleChunk(
                    current_chunk_id_,
                    current_chunk_pos_,
                    num_chunk_,
                    batch_size_);
            }
        }
    }

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    bool
    IsSource() const override {
        return true;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    bool
    CanExecuteAllAtOnce() const override {
        return false;
    }

 private:
    int64_t
    GetCurrentRows() {
        if (segment_chunk_reader_.segment_->is_chunked()) {
            auto current_rows =
                is_left_indexed_ && segment_chunk_reader_.segment_->HasRawData(
                                        left_field_.get())
                    ? left_current_chunk_pos_
                    : segment_chunk_reader_.segment_->num_rows_until_chunk(
                          left_field_, left_current_chunk_id_) +
                          left_current_chunk_pos_;
            return current_rows;
        } else {
            return segment_chunk_reader_.segment_->type() ==
                           SegmentType::Growing
                       ? current_chunk_id_ *
                                 segment_chunk_reader_.SizePerChunk() +
                             current_chunk_pos_
                       : current_chunk_pos_;
        }
    }

    int64_t
    GetNextBatchSize();

    bool
    CanUseBothDataCompare(OffsetVector* input) const;

    template <typename T>
    static ChunkedColumnInterface::ScanValueKind
    DataScanValueKind() {
        if constexpr (std::is_same_v<T, std::string_view> ||
                      std::is_same_v<T, std::string>) {
            return ChunkedColumnInterface::ScanValueKind::StringView;
        } else if constexpr (std::is_same_v<T, Json>) {
            return ChunkedColumnInterface::ScanValueKind::JsonView;
        } else if constexpr (std::is_same_v<T, ArrayView>) {
            return ChunkedColumnInterface::ScanValueKind::ArrayView;
        } else {
            return ChunkedColumnInterface::ScanValueKind::FixedWidth;
        }
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataChunks(FUNC func,
                          OffsetVector* input,
                          TargetBitmapView res,
                          TargetBitmapView valid_res,
                          const ValTypes&... values) {
        (void)input;
        if (segment_chunk_reader_.segment_->is_chunked()) {
            return ProcessBothDataChunksForMultipleChunk<T,
                                                         U,
                                                         FUNC,
                                                         ValTypes...>(
                func, res, valid_res, values...);
        }
        return ProcessBothDataChunksForSingleChunk<T, U, FUNC, ValTypes...>(
            func, res, valid_res, values...);
    }

    // Process sorted offsets with one continuous Scan per column.
    // TODO: push the offset bitmap down into Scan when Vortex supports bitmap scan.
    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothSortedDataByOffsets(FUNC func,
                                   OffsetVector* input,
                                   TargetBitmapView res,
                                   TargetBitmapView valid_res,
                                   const ValTypes&... values) {
        auto left_column =
            segment_chunk_reader_.segment_->GetChunkedColumn(left_field_);
        auto right_column =
            segment_chunk_reader_.segment_->GetChunkedColumn(right_field_);
        if (left_column == nullptr || right_column == nullptr) {
            return -1;
        }
        if (input->empty()) {
            return 0;
        }

        const auto scan_start = static_cast<int64_t>((*input)[0]);
        const auto scan_end =
            static_cast<int64_t>((*input)[input->size() - 1]) + 1;
        const auto scan_length = scan_end - scan_start;
        AssertInfo(scan_length > 0,
                   "invalid compare offset scan range [{}, {})",
                   scan_start,
                   scan_end);

        auto left_options = ChunkedColumnInterface::ScanOptions::ForData(
            scan_start,
            scan_length,
            ChunkedColumnInterface::ScanProjection::Data,
            DataScanValueKind<T>());
        auto right_options = ChunkedColumnInterface::ScanOptions::ForData(
            scan_start,
            scan_length,
            ChunkedColumnInterface::ScanProjection::Data,
            DataScanValueKind<U>());
        auto left_cursor = left_column->Scan(op_ctx_, left_options);
        auto right_cursor = right_column->Scan(op_ctx_, right_options);
        if (left_cursor == nullptr || right_cursor == nullptr) {
            return -1;
        }

        ChunkedColumnInterface::ScanBatch left_batch;
        ChunkedColumnInterface::ScanBatch right_batch;
        int64_t left_batch_pos = 0;
        int64_t right_batch_pos = 0;
        size_t processed_offsets = 0;
        std::vector<int32_t> batch_offsets;
        batch_offsets.reserve(std::min<int64_t>(batch_size_, input->size()));

        while (processed_offsets < input->size()) {
            if (!EnsureDataScanBatch(left_cursor, left_batch, left_batch_pos) ||
                !EnsureDataScanBatch(
                    right_cursor, right_batch, right_batch_pos)) {
                break;
            }

            const auto left_row = left_batch.row_id_start + left_batch_pos;
            const auto right_row = right_batch.row_id_start + right_batch_pos;
            const auto interval_start = std::max(left_row, right_row);
            const auto interval_end =
                std::min(left_batch.row_id_start + left_batch.size,
                         right_batch.row_id_start + right_batch.size);
            AssertInfo(interval_start < interval_end,
                       "invalid compare offset scan interval [{}, {})",
                       interval_start,
                       interval_end);

            const auto group_start = processed_offsets;
            const auto left_base = interval_start - left_batch.row_id_start;
            const auto right_base = interval_start - right_batch.row_id_start;
            batch_offsets.clear();
            while (processed_offsets < input->size()) {
                const auto row =
                    static_cast<int64_t>((*input)[processed_offsets]);
                if (row >= interval_end) {
                    break;
                }
                AssertInfo(row >= interval_start,
                           "compare offset {} is before scan interval [{}, {})",
                           row,
                           interval_start,
                           interval_end);
                batch_offsets.push_back(
                    static_cast<int32_t>(row - interval_start));
                ++processed_offsets;
            }

            const auto group_size =
                static_cast<int64_t>(processed_offsets - group_start);
            if (group_size > 0) {
                func.template operator()<FilterType::random>(
                    left_batch.values.data_as<T>() + left_base,
                    right_batch.values.data_as<U>() + right_base,
                    batch_offsets.data(),
                    static_cast<int>(group_size),
                    res + group_start,
                    values...);

                for (int64_t i = 0; i < group_size; ++i) {
                    if (!left_batch.validity.IsValid(left_base +
                                                     batch_offsets[i]) ||
                        !right_batch.validity.IsValid(right_base +
                                                      batch_offsets[i])) {
                        res[group_start + i] = false;
                        valid_res[group_start + i] = false;
                    }
                }
            }

            left_batch_pos += interval_end - left_row;
            right_batch_pos += interval_end - right_row;
        }

        AssertInfo(processed_offsets == input->size(),
                   "compare offset scan processed {} offsets, expected {}",
                   processed_offsets,
                   input->size());
        return input->size();
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataByOffsetsByChunkFallback(FUNC func,
                                            OffsetVector* input,
                                            TargetBitmapView res,
                                            TargetBitmapView valid_res,
                                            const ValTypes&... values) {
        int64_t size = input->size();
        int64_t processed_size = 0;
        if (segment_chunk_reader_.segment_->is_chunked() ||
            segment_chunk_reader_.segment_->type() == SegmentType::Growing) {
            for (auto i = 0; i < size; ++i) {
                auto offset = (*input)[i];
                auto get_chunk_id_and_offset =
                    [&](const FieldId field) -> std::pair<int64_t, int64_t> {
                    if (segment_chunk_reader_.segment_->type() ==
                        SegmentType::Growing) {
                        auto size_per_chunk =
                            segment_chunk_reader_.SizePerChunk();
                        return {offset / size_per_chunk,
                                offset % size_per_chunk};
                    }
                    return segment_chunk_reader_.segment_->get_chunk_by_offset(
                        field, offset);
                };

                auto [left_chunk_id, left_chunk_offset] =
                    get_chunk_id_and_offset(left_field_);
                auto [right_chunk_id, right_chunk_offset] =
                    get_chunk_id_and_offset(right_field_);

                auto pw_left = segment_chunk_reader_.segment_->chunk_data<T>(
                    op_ctx_, left_field_, left_chunk_id);
                auto left_chunk = pw_left.get();
                auto pw_right = segment_chunk_reader_.segment_->chunk_data<U>(
                    op_ctx_, right_field_, right_chunk_id);
                auto right_chunk = pw_right.get();
                const bool* left_valid_data = left_chunk.valid_data();
                const bool* right_valid_data = right_chunk.valid_data();
                if (left_valid_data && !left_valid_data[left_chunk_offset]) {
                    res[processed_size] = false;
                    valid_res[processed_size] = false;
                    processed_size++;
                    continue;
                }
                if (right_valid_data && !right_valid_data[right_chunk_offset]) {
                    res[processed_size] = false;
                    valid_res[processed_size] = false;
                    processed_size++;
                    continue;
                }
                const T* left_data = left_chunk.data() + left_chunk_offset;
                const U* right_data = right_chunk.data() + right_chunk_offset;
                func.template operator()<FilterType::random>(
                    left_data,
                    right_data,
                    nullptr,
                    1,
                    res + processed_size,
                    values...);
                processed_size++;
            }
            return processed_size;
        }

        auto pw_left = segment_chunk_reader_.segment_->chunk_data<T>(
            op_ctx_, left_field_, 0);
        auto left_chunk = pw_left.get();
        auto pw_right = segment_chunk_reader_.segment_->chunk_data<U>(
            op_ctx_, right_field_, 0);
        auto right_chunk = pw_right.get();
        const T* left_data = left_chunk.data();
        const U* right_data = right_chunk.data();
        const bool* left_valid_data = left_chunk.valid_data();
        const bool* right_valid_data = right_chunk.valid_data();
        if (left_valid_data || right_valid_data) {
            for (int i = 0; i < size; ++i) {
                auto offset = (*input)[i];
                if (left_valid_data && !left_valid_data[offset]) {
                    res[i] = false;
                    valid_res[i] = false;
                    continue;
                }
                if (right_valid_data && !right_valid_data[offset]) {
                    res[i] = false;
                    valid_res[i] = false;
                    continue;
                }
                func.template operator()<FilterType::random>(
                    left_data + offset,
                    right_data + offset,
                    nullptr,
                    1,
                    res + i,
                    values...);
            }
            return size;
        }
        func.template operator()<FilterType::random>(
            left_data, right_data, input->data(), size, res, values...);
        return size;
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataByOffsets(FUNC func,
                             OffsetVector* input,
                             TargetBitmapView res,
                             TargetBitmapView valid_res,
                             const ValTypes&... values) {
        if constexpr (IsCompareStringViewType<T> ||
                      IsCompareStringViewType<U>) {
            if (IsDenseOffsetInputForScan(input, batch_size_)) {
                return ProcessBothSortedDataByOffsets<T, U>(
                    func, input, res, valid_res, values...);
            }
            return -1;
        } else {
            if (IsDenseOffsetInputForScan(input, batch_size_)) {
                const auto processed_size = ProcessBothSortedDataByOffsets<T, U>(
                    func, input, res, valid_res, values...);
                if (processed_size >= 0) {
                    return processed_size;
                }
            }
            return ProcessBothDataByOffsetsByChunkFallback<T, U>(
                func, input, res, valid_res, values...);
        }
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataChunksForSingleChunk(FUNC func,
                                        TargetBitmapView res,
                                        TargetBitmapView valid_res,
                                        const ValTypes&... values) {
        int64_t processed_size = 0;

        const auto active_count = segment_chunk_reader_.active_count_;
        for (size_t i = current_chunk_id_; i < num_chunk_; i++) {
            auto pw_left = segment_chunk_reader_.segment_->chunk_data<T>(
                op_ctx_, left_field_, i);
            auto left_chunk = pw_left.get();
            auto pw_right = segment_chunk_reader_.segment_->chunk_data<U>(
                op_ctx_, right_field_, i);
            auto right_chunk = pw_right.get();
            auto data_pos = (i == current_chunk_id_) ? current_chunk_pos_ : 0;
            auto size =
                (i == (num_chunk_ - 1))
                    ? (segment_chunk_reader_.segment_->type() ==
                               SegmentType::Growing
                           ? (active_count % segment_chunk_reader_
                                                 .SizePerChunk() ==
                                      0
                                  ? segment_chunk_reader_.SizePerChunk() -
                                        data_pos
                                  : active_count % segment_chunk_reader_
                                                       .SizePerChunk() -
                                        data_pos)
                           : active_count - data_pos)
                    : segment_chunk_reader_.SizePerChunk() - data_pos;

            if (processed_size + size >= batch_size_) {
                size = batch_size_ - processed_size;
            }

            const T* left_data = left_chunk.data() + data_pos;
            const U* right_data = right_chunk.data() + data_pos;
            func(left_data,
                 right_data,
                 nullptr,
                 size,
                 res + processed_size,
                 values...);
            const bool* left_valid_data = left_chunk.valid_data();
            const bool* right_valid_data = right_chunk.valid_data();
            for (int i = 0; i < size; ++i) {
                if (left_valid_data && !left_valid_data[i + data_pos]) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                    continue;
                }
                if (right_valid_data && !right_valid_data[i + data_pos]) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                }
            }
            processed_size += size;

            if (processed_size >= batch_size_) {
                current_chunk_id_ = i;
                current_chunk_pos_ = data_pos + size;
                break;
            }
        }

        return processed_size;
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataChunksForMultipleChunk(FUNC func,
                                          TargetBitmapView res,
                                          TargetBitmapView valid_res,
                                          const ValTypes&... values) {
        int64_t processed_size = 0;

        for (size_t i = left_current_chunk_id_; i < left_num_chunk_; i++) {
            auto pw_left = segment_chunk_reader_.segment_->chunk_data<T>(
                op_ctx_, left_field_, i);
            auto left_chunk = pw_left.get();
            auto pw_right = segment_chunk_reader_.segment_->chunk_data<U>(
                op_ctx_, right_field_, i);
            auto right_chunk = pw_right.get();
            auto data_pos =
                (i == left_current_chunk_id_) ? left_current_chunk_pos_ : 0;
            int64_t size = 0;
            if (segment_chunk_reader_.segment_->type() ==
                SegmentType::Growing) {
                size =
                    (i == (left_num_chunk_ - 1))
                        ? (segment_chunk_reader_.active_count_ %
                                       segment_chunk_reader_.SizePerChunk() ==
                                   0
                               ? segment_chunk_reader_.SizePerChunk() - data_pos
                               : segment_chunk_reader_.active_count_ %
                                         segment_chunk_reader_.SizePerChunk() -
                                     data_pos)
                        : segment_chunk_reader_.SizePerChunk() - data_pos;
            } else {
                size =
                    segment_chunk_reader_.segment_->chunk_size(left_field_, i) -
                    data_pos;
            }

            if (processed_size + size >= batch_size_) {
                size = batch_size_ - processed_size;
            }

            const T* left_data = left_chunk.data() + data_pos;
            const U* right_data = right_chunk.data() + data_pos;
            func(left_data,
                 right_data,
                 nullptr,
                 size,
                 res + processed_size,
                 values...);
            const bool* left_valid_data = left_chunk.valid_data();
            const bool* right_valid_data = right_chunk.valid_data();
            for (int i = 0; i < size; ++i) {
                if (left_valid_data && !left_valid_data[i + data_pos]) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                    continue;
                }
                if (right_valid_data && !right_valid_data[i + data_pos]) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                }
            }
            processed_size += size;

            if (processed_size >= batch_size_) {
                left_current_chunk_id_ = i;
                left_current_chunk_pos_ = data_pos + size;
                break;
            }
        }

        return processed_size;
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    TryProcessBothDataByScan(FUNC func,
                             int64_t real_batch_size,
                             TargetBitmapView res,
                             TargetBitmapView valid_res,
                             const ValTypes&... values) {
        if (!data_scan_initialized_) {
            data_scan_initialized_ = true;
            auto left_column =
                segment_chunk_reader_.segment_->GetChunkedColumn(left_field_);
            auto right_column =
                segment_chunk_reader_.segment_->GetChunkedColumn(right_field_);
            if (left_column != nullptr && right_column != nullptr) {
                const auto start = GetCurrentRows();
                const auto length = segment_chunk_reader_.active_count_ - start;
                auto left_options =
                    ChunkedColumnInterface::ScanOptions::ForData(
                        start,
                        length,
                        ChunkedColumnInterface::ScanProjection::Data,
                        DataScanValueKind<T>());
                auto right_options =
                    ChunkedColumnInterface::ScanOptions::ForData(
                        start,
                        length,
                        ChunkedColumnInterface::ScanProjection::Data,
                        DataScanValueKind<U>());
                left_data_cursor_ = left_column->Scan(op_ctx_, left_options);
                right_data_cursor_ = right_column->Scan(op_ctx_, right_options);
                if (left_data_cursor_ == nullptr ||
                    right_data_cursor_ == nullptr) {
                    left_data_cursor_.reset();
                    right_data_cursor_.reset();
                    return -1;
                }
            }
        }
        if (left_data_cursor_ == nullptr || right_data_cursor_ == nullptr) {
            return -1;
        }

        int64_t processed_size = 0;
        while (processed_size < real_batch_size) {
            if (!EnsureDataScanBatch(left_data_cursor_,
                                     left_data_batch_,
                                     left_data_batch_pos_) ||
                !EnsureDataScanBatch(right_data_cursor_,
                                     right_data_batch_,
                                     right_data_batch_pos_)) {
                break;
            }

            const auto left_row =
                left_data_batch_.row_id_start + left_data_batch_pos_;
            const auto right_row =
                right_data_batch_.row_id_start + right_data_batch_pos_;
            const auto expected_row = GetCurrentRows() + processed_size;
            AssertInfo(left_row == expected_row && right_row == expected_row,
                       "compare data scan row mismatch, left {}, right {}, "
                       "expected {}",
                       left_row,
                       right_row,
                       expected_row);

            auto size = std::min<int64_t>(
                {real_batch_size - processed_size,
                 left_data_batch_.size - left_data_batch_pos_,
                 right_data_batch_.size - right_data_batch_pos_});
            const auto* left_data =
                left_data_batch_.values.data_as<T>() + left_data_batch_pos_;
            const auto* right_data =
                right_data_batch_.values.data_as<U>() + right_data_batch_pos_;

            func(left_data,
                 right_data,
                 nullptr,
                 size,
                 res + processed_size,
                 values...);

            for (int64_t i = 0; i < size; ++i) {
                if (!left_data_batch_.validity.IsValid(left_data_batch_pos_ +
                                                       i)) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                    continue;
                }
                if (!right_data_batch_.validity.IsValid(right_data_batch_pos_ +
                                                        i)) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                }
            }

            processed_size += size;
            left_data_batch_pos_ += size;
            right_data_batch_pos_ += size;
        }

        AssertInfo(processed_size == real_batch_size,
                   "compare data scan processed {} rows, expected {}",
                   processed_size,
                   real_batch_size);
        MoveCursor();
        return processed_size;
    }

    template <typename OpType>
    VectorPtr
    ExecCompareExprDispatcher(OpType op, EvalCtx& context);

    VectorPtr
    ExecCompareExprDispatcherForHybridSegment(EvalCtx& context);

    VectorPtr
    ExecCompareExprDispatcherForBothDataSegment(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecCompareLeftType(EvalCtx& context);

    template <typename T, typename U>
    VectorPtr
    ExecCompareRightType(EvalCtx& context);

 private:
    const FieldId left_field_;
    const FieldId right_field_;
    bool is_left_indexed_;
    bool is_right_indexed_;
    int64_t num_chunk_{0};
    int64_t left_num_chunk_{0};
    int64_t right_num_chunk_{0};
    int64_t left_current_chunk_id_{0};
    int64_t left_current_chunk_pos_{0};
    int64_t right_current_chunk_id_{0};
    int64_t right_current_chunk_pos_{0};
    int64_t current_chunk_id_{0};
    int64_t current_chunk_pos_{0};

    const segcore::SegmentChunkReader segment_chunk_reader_;
    int64_t batch_size_;
    std::shared_ptr<const milvus::expr::CompareExpr> expr_;
    std::vector<PinWrapper<const index::IndexBase*>> pinned_index_left_;
    std::vector<PinWrapper<const index::IndexBase*>> pinned_index_right_;
    bool data_scan_initialized_{false};
    std::unique_ptr<ChunkedColumnInterface::ScanCursor> left_data_cursor_{
        nullptr};
    std::unique_ptr<ChunkedColumnInterface::ScanCursor> right_data_cursor_{
        nullptr};
    ChunkedColumnInterface::ScanBatch left_data_batch_;
    ChunkedColumnInterface::ScanBatch right_data_batch_;
    int64_t left_data_batch_pos_{0};
    int64_t right_data_batch_pos_{0};

    bool
    EnsureDataScanBatch(
        std::unique_ptr<ChunkedColumnInterface::ScanCursor>& cursor,
        ChunkedColumnInterface::ScanBatch& batch,
        int64_t& batch_pos) {
        while (batch_pos >= batch.size) {
            batch_pos = 0;
            if (!cursor->Next(&batch)) {
                return false;
            }
            AssertInfo(!batch.values.empty() && batch.size > 0,
                       "invalid compare data scan batch");
        }
        return true;
    }
};
}  //namespace exec
}  // namespace milvus
