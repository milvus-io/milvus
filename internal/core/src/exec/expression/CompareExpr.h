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
                    fmt::format("unsupported op_type:{} for CompareElementFunc",
                                op));
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
        auto schema = segment->get_schema_snapshot();
        auto& left_field_meta = (*schema)[left_field_];
        auto& right_field_meta = (*schema)[right_field_];
        pinned_index_left_ = PinIndex(op_ctx_, segment, left_field_meta);
        pinned_index_right_ = PinIndex(op_ctx_, segment, right_field_meta);
        is_left_indexed_ = pinned_index_left_.size() > 0;
        is_right_indexed_ = pinned_index_right_.size() > 0;
        left_use_index_data_ =
            is_left_indexed_ && segment->HasRawData(left_field_.get());
        right_use_index_data_ =
            is_right_indexed_ && segment->HasRawData(right_field_.get());
        if (segment->is_chunked()) {
            left_num_chunk_ =
                left_use_index_data_ ? pinned_index_left_.size()
                : segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(left_field_);
            right_num_chunk_ =
                right_use_index_data_ ? pinned_index_right_.size()
                : segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(right_field_);
            num_chunk_ = left_num_chunk_;
        } else {
            num_chunk_ = left_use_index_data_
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
    MoveCursorInternal() {
        if (!has_offset_input_) {
            if (segment_chunk_reader_.segment_->is_chunked()) {
                if (left_use_index_data_) {
                    MoveCursorForIndexed(left_current_chunk_pos_);
                } else {
                    segment_chunk_reader_.MoveCursorForMultipleChunk(
                        left_current_chunk_id_,
                        left_current_chunk_pos_,
                        left_field_,
                        left_num_chunk_,
                        batch_size_);
                }
                if (right_use_index_data_) {
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

    void
    MoveCursor() override {
        // Scan cursors receive the absolute segment position on every call.
        // A short-circuited compare window therefore advances only execution
        // state; the next evaluated call seeks both cursors forward.
        MoveCursorInternal();
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
    segcore::PinnedIndexView
    LeftPinnedIndexForRawLookup() const {
        if (!left_use_index_data_) {
            return {};
        }
        return {pinned_index_left_.data(), pinned_index_left_.size()};
    }

    segcore::PinnedIndexView
    RightPinnedIndexForRawLookup() const {
        if (!right_use_index_data_) {
            return {};
        }
        return {pinned_index_right_.data(), pinned_index_right_.size()};
    }

    int64_t
    GetCurrentRows() {
        if (segment_chunk_reader_.segment_->is_chunked()) {
            auto current_rows =
                left_use_index_data_
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
    CanUseBothDataCompare() const;

    template <typename T>
    static ChunkedColumnInterface::TargetType
    DataTargetType() {
        return milvus::TargetTypeOf<T>();
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataChunks(FUNC func,
                          TargetBitmapView res,
                          TargetBitmapView valid_res,
                          const ValTypes&... values) {
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

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataByOffsetsByTake(FUNC func,
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

        const auto offset_view = ChunkedColumnInterface::OffsetView::From(
            input->data(), static_cast<int64_t>(input->size()));
        auto left_take =
            left_column->Take(op_ctx_,
                              ChunkedColumnInterface::TakeOptions{
                                  offset_view, DataTargetType<T>()});
        auto right_take =
            right_column->Take(op_ctx_,
                               ChunkedColumnInterface::TakeOptions{
                                   offset_view, DataTargetType<U>()});
        if (left_take == nullptr || right_take == nullptr) {
            return -1;
        }
        const auto size = static_cast<int64_t>(input->size());
        AssertInfo(left_take->Size() == size && right_take->Size() == size,
                   "compare take sizes ({}, {}) do not match input {}",
                   left_take->Size(),
                   right_take->Size(),
                   size);

        const auto left_is_owned = left_take->IsOwned();
        const auto right_is_owned = right_take->IsOwned();
        if (left_is_owned && right_is_owned) {
            auto left_owned = left_take->GetOwn();
            auto right_owned = right_take->GetOwn();
            AssertInfo(left_owned.size == size && right_owned.size == size &&
                           !left_owned.values.empty() &&
                           !right_owned.values.empty(),
                       "invalid owned compare take results");
            func.template operator()<FilterType::random>(
                left_owned.values.data_as<T>(),
                right_owned.values.data_as<U>(),
                nullptr,
                static_cast<int>(size),
                res,
                values...);
            for (int64_t i = 0; i < size; ++i) {
                const auto left_valid =
                    !left_owned.validity || left_owned.validity[i];
                const auto right_valid =
                    !right_owned.validity || right_owned.validity[i];
                const auto left_skipped =
                    left_owned.data_skipped && left_owned.data_skipped[i];
                const auto right_skipped =
                    right_owned.data_skipped && right_owned.data_skipped[i];
                if (left_skipped || right_skipped) {
                    res[i] = false;
                    valid_res[i] = left_valid && right_valid;
                    continue;
                }
                if (!left_valid || !right_valid) {
                    res[i] = false;
                    valid_res[i] = false;
                }
            }
        } else {
            const auto left_items = left_take->template Access<T>();
            const auto right_items = right_take->template Access<U>();
            for (int64_t i = 0; i < size; ++i) {
                const auto left_item = left_items[i];
                const auto right_item = right_items[i];
                if (left_item.data_skipped || right_item.data_skipped) {
                    res[i] = false;
                    valid_res[i] = left_item.is_valid && right_item.is_valid;
                    continue;
                }
                if (!left_item.is_valid || !right_item.is_valid) {
                    res[i] = false;
                    valid_res[i] = false;
                    continue;
                }
                const auto& left = *left_item.value;
                const auto& right = *right_item.value;
                func.template operator()<FilterType::random>(
                    &left, &right, nullptr, 1, res + i, values...);
            }
        }
        return size;
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
            auto get_chunk_id_and_offset =
                [&](const FieldId field,
                    int64_t offset) -> std::pair<int64_t, int64_t> {
                if (segment_chunk_reader_.segment_->type() ==
                    SegmentType::Growing) {
                    auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
                    return {offset / size_per_chunk, offset % size_per_chunk};
                } else {
                    return segment_chunk_reader_.segment_->get_chunk_by_offset(
                        field, offset);
                }
            };

            // Consecutive offsets frequently fall in the same left/right chunk;
            // keep both pinned chunks across iterations and only re-pin/resolve
            // when a chunk id changes, avoiding a per-row GroupChunk pin +
            // shared_ptr lookup for each of the two columns. Safe on both
            // sealed and growing (data and the chunked validity storage have
            // stable per-chunk buffers).
            int64_t cached_left_chunk_id = -1;
            int64_t cached_right_chunk_id = -1;
            std::optional<PinWrapper<Span<T>>> pw_left;
            std::optional<PinWrapper<Span<U>>> pw_right;
            const T* left_base = nullptr;
            ValidityView left_validity;
            const U* right_base = nullptr;
            ValidityView right_validity;
            for (auto i = 0; i < size; ++i) {
                auto offset = (*input)[i];
                auto [left_chunk_id, left_chunk_offset] =
                    get_chunk_id_and_offset(left_field_, offset);
                auto [right_chunk_id, right_chunk_offset] =
                    get_chunk_id_and_offset(right_field_, offset);

                if (left_chunk_id != cached_left_chunk_id) {
                    pw_left.emplace(
                        segment_chunk_reader_.segment_->chunk_data<T>(
                            op_ctx_, left_field_, left_chunk_id));
                    auto left_chunk = pw_left->get();
                    left_base = left_chunk.data();
                    left_validity = left_chunk.validity();
                    cached_left_chunk_id = left_chunk_id;
                }
                if (right_chunk_id != cached_right_chunk_id) {
                    pw_right.emplace(
                        segment_chunk_reader_.segment_->chunk_data<U>(
                            op_ctx_, right_field_, right_chunk_id));
                    auto right_chunk = pw_right->get();
                    right_base = right_chunk.data();
                    right_validity = right_chunk.validity();
                    cached_right_chunk_id = right_chunk_id;
                }
                if (left_validity && !left_validity[left_chunk_offset]) {
                    res[processed_size] = false;
                    valid_res[processed_size] = false;
                    processed_size++;
                    continue;
                }
                if (right_validity && !right_validity[right_chunk_offset]) {
                    res[processed_size] = false;
                    valid_res[processed_size] = false;
                    processed_size++;
                    continue;
                }
                const T* left_data = left_base + left_chunk_offset;
                const U* right_data = right_base + right_chunk_offset;
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
        const auto left_validity = left_chunk.validity();
        const auto right_validity = right_chunk.validity();
        if (left_validity || right_validity) {
            for (int i = 0; i < size; ++i) {
                auto offset = (*input)[i];
                if (left_validity && !left_validity[offset]) {
                    res[i] = false;
                    valid_res[i] = false;
                    continue;
                }
                if (right_validity && !right_validity[offset]) {
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
        const auto processed_size = ProcessBothDataByOffsetsByTake<T, U>(
            func, input, res, valid_res, values...);
        if (processed_size >= 0) {
            return processed_size;
        }
        if constexpr (IsCompareStringViewType<T> ||
                      IsCompareStringViewType<U>) {
            return -1;
        }
        return ProcessBothDataByOffsetsByChunkFallback<T, U>(
            func, input, res, valid_res, values...);
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
            const auto left_validity = left_chunk.validity().Subview(data_pos);
            const auto right_validity =
                right_chunk.validity().Subview(data_pos);
            for (int i = 0; i < size; ++i) {
                if (left_validity && !left_validity[i]) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                    continue;
                }
                if (right_validity && !right_validity[i]) {
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
        while (processed_size < batch_size_ &&
               left_current_chunk_id_ < left_num_chunk_ &&
               right_current_chunk_id_ < right_num_chunk_) {
            auto pw_left = segment_chunk_reader_.segment_->chunk_data<T>(
                op_ctx_, left_field_, left_current_chunk_id_);
            auto left_chunk = pw_left.get();
            auto pw_right = segment_chunk_reader_.segment_->chunk_data<U>(
                op_ctx_, right_field_, right_current_chunk_id_);
            auto right_chunk = pw_right.get();
            int64_t left_chunk_size = 0;
            int64_t right_chunk_size = 0;
            if (segment_chunk_reader_.segment_->type() ==
                SegmentType::Growing) {
                const auto last_chunk_size =
                    segment_chunk_reader_.active_count_ %
                                segment_chunk_reader_.SizePerChunk() ==
                            0
                        ? segment_chunk_reader_.SizePerChunk()
                        : segment_chunk_reader_.active_count_ %
                              segment_chunk_reader_.SizePerChunk();
                left_chunk_size = left_current_chunk_id_ == left_num_chunk_ - 1
                                      ? last_chunk_size
                                      : segment_chunk_reader_.SizePerChunk();
                right_chunk_size =
                    right_current_chunk_id_ == right_num_chunk_ - 1
                        ? last_chunk_size
                        : segment_chunk_reader_.SizePerChunk();
            } else {
                left_chunk_size = segment_chunk_reader_.segment_->chunk_size(
                    left_field_, left_current_chunk_id_);
                right_chunk_size = segment_chunk_reader_.segment_->chunk_size(
                    right_field_, right_current_chunk_id_);
            }
            AssertInfo(left_current_chunk_pos_ < left_chunk_size &&
                           right_current_chunk_pos_ < right_chunk_size,
                       "compare chunk cursor out of range, left {}/{}, "
                       "right {}/{}",
                       left_current_chunk_pos_,
                       left_chunk_size,
                       right_current_chunk_pos_,
                       right_chunk_size);
            const auto size = std::min<int64_t>(
                {batch_size_ - processed_size,
                 left_chunk_size - left_current_chunk_pos_,
                 right_chunk_size - right_current_chunk_pos_});

            const T* left_data = left_chunk.data() + left_current_chunk_pos_;
            const U* right_data = right_chunk.data() + right_current_chunk_pos_;
            func(left_data,
                 right_data,
                 nullptr,
                 size,
                 res + processed_size,
                 values...);
            const auto left_validity =
                left_chunk.validity().Subview(left_current_chunk_pos_);
            const auto right_validity =
                right_chunk.validity().Subview(right_current_chunk_pos_);
            for (int i = 0; i < size; ++i) {
                if (left_validity && !left_validity[i]) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                    continue;
                }
                if (right_validity && !right_validity[i]) {
                    res[processed_size + i] = false;
                    valid_res[processed_size + i] = false;
                }
            }
            processed_size += size;
            left_current_chunk_pos_ += size;
            right_current_chunk_pos_ += size;
            if (left_current_chunk_pos_ == left_chunk_size) {
                ++left_current_chunk_id_;
                left_current_chunk_pos_ = 0;
            }
            if (right_current_chunk_pos_ == right_chunk_size) {
                ++right_current_chunk_id_;
                right_current_chunk_pos_ = 0;
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
            left_data_column_ =
                segment_chunk_reader_.segment_->GetChunkedColumn(left_field_);
            right_data_column_ =
                segment_chunk_reader_.segment_->GetChunkedColumn(right_field_);
        }
        if (left_data_column_ == nullptr || right_data_column_ == nullptr) {
            return -1;
        }

        const auto window_start = GetCurrentRows();
        if (left_data_scan_cursor_ == nullptr) {
            const auto pin_policy =
                segcore::SegcoreConfig::default_config()
                        .get_scan_cursor_owns_pin()
                    ? ChunkedColumnInterface::ScanPinPolicy::CursorOwned
                    : ChunkedColumnInterface::ScanPinPolicy::ResultOwned;
            auto left_cursor = left_data_column_->Scan(
                op_ctx_,
                ChunkedColumnInterface::ScanOptions::ForData(
                    window_start, DataTargetType<T>(), pin_policy));
            auto right_cursor = right_data_column_->Scan(
                op_ctx_,
                ChunkedColumnInterface::ScanOptions::ForData(
                    window_start, DataTargetType<U>(), pin_policy));
            if (left_cursor == nullptr || right_cursor == nullptr) {
                left_data_column_.reset();
                right_data_column_.reset();
                return -1;
            }
            left_data_scan_cursor_ = std::move(left_cursor);
            right_data_scan_cursor_ = std::move(right_cursor);
        }

        AssertInfo(right_data_scan_cursor_ != nullptr,
                   "compare scan cursors are not initialized together");
        try {
            ChunkedColumnInterface::ScanBatch left_data_batch;
            ChunkedColumnInterface::ScanBatch right_data_batch;
            int64_t left_data_batch_pos = 0;
            int64_t right_data_batch_pos = 0;
            int64_t processed_size = 0;
            while (processed_size < real_batch_size) {
                const auto expected_row = window_start + processed_size;
                const auto remaining = real_batch_size - processed_size;
                if (left_data_batch_pos == left_data_batch.size) {
                    if (left_data_scan_cursor_->Position() != expected_row) {
                        left_data_scan_cursor_->Seek(expected_row);
                    }
                    const auto returned = left_data_scan_cursor_->Next(
                        remaining,
                        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                        &left_data_batch);
                    AssertDataScanBatch(
                        returned, left_data_batch, expected_row, remaining);
                    left_data_batch_pos = 0;
                }
                if (right_data_batch_pos == right_data_batch.size) {
                    if (right_data_scan_cursor_->Position() != expected_row) {
                        right_data_scan_cursor_->Seek(expected_row);
                    }
                    const auto returned = right_data_scan_cursor_->Next(
                        remaining,
                        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                        &right_data_batch);
                    AssertDataScanBatch(
                        returned, right_data_batch, expected_row, remaining);
                    right_data_batch_pos = 0;
                }

                const auto left_row =
                    left_data_batch.row_id_start + left_data_batch_pos;
                const auto right_row =
                    right_data_batch.row_id_start + right_data_batch_pos;
                AssertInfo(
                    left_row == expected_row && right_row == expected_row,
                    "compare data scan row mismatch, left {}, right "
                    "{}, expected {}",
                    left_row,
                    right_row,
                    expected_row);

                auto size = std::min<int64_t>(
                    {real_batch_size - processed_size,
                     left_data_batch.size - left_data_batch_pos,
                     right_data_batch.size - right_data_batch_pos});
                const auto* left_data =
                    left_data_batch.values.data_as<T>() + left_data_batch_pos;
                const auto* right_data =
                    right_data_batch.values.data_as<U>() + right_data_batch_pos;

                func(left_data,
                     right_data,
                     nullptr,
                     size,
                     res + processed_size,
                     values...);

                for (int64_t i = 0; i < size; ++i) {
                    if (left_data_batch.validity &&
                        !left_data_batch.validity[left_data_batch_pos + i]) {
                        res[processed_size + i] = false;
                        valid_res[processed_size + i] = false;
                        continue;
                    }
                    if (right_data_batch.validity &&
                        !right_data_batch.validity[right_data_batch_pos + i]) {
                        res[processed_size + i] = false;
                        valid_res[processed_size + i] = false;
                    }
                }

                processed_size += size;
                left_data_batch_pos += size;
                right_data_batch_pos += size;
            }
        } catch (...) {
            left_data_scan_cursor_.reset();
            right_data_scan_cursor_.reset();
            throw;
        }

        MoveCursorInternal();
        return real_batch_size;
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
    bool left_use_index_data_;
    bool right_use_index_data_;
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
    // Keep both published column generations and their forward-only cursors
    // alive for the expression lifetime. Each ScanBatch owns only its current
    // transient pin and decoded data.
    std::shared_ptr<ChunkedColumnInterface> left_data_column_{nullptr};
    std::shared_ptr<ChunkedColumnInterface> right_data_column_{nullptr};
    std::unique_ptr<ChunkedColumnInterface::ScanCursor> left_data_scan_cursor_;
    std::unique_ptr<ChunkedColumnInterface::ScanCursor> right_data_scan_cursor_;

    static void
    AssertDataScanBatch(bool returned,
                        const ChunkedColumnInterface::ScanBatch& batch,
                        int64_t expected_row,
                        int64_t max_rows) {
        AssertInfo(returned && !batch.values.empty() && batch.size > 0 &&
                       batch.row_id_start == expected_row &&
                       batch.size <= max_rows,
                   "invalid compare data scan batch [{}, {}) for request "
                   "[{}, {})",
                   batch.row_id_start,
                   batch.row_id_start + batch.size,
                   expected_row,
                   expected_row + max_rows);
    }
};
}  //namespace exec
}  // namespace milvus
