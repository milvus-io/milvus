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
#include <boost/variant.hpp>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/type_c.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentChunkReader.h"

namespace milvus {
namespace exec {

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
                if constexpr (op == proto::plan::OpType::Equal) {
                    res[i] = left[offset] == right[offset];
                } else if constexpr (op == proto::plan::OpType::NotEqual) {
                    res[i] = left[offset] != right[offset];
                } else if constexpr (op == proto::plan::OpType::GreaterThan) {
                    res[i] = left[offset] > right[offset];
                } else if constexpr (op == proto::plan::OpType::LessThan) {
                    res[i] = left[offset] < right[offset];
                } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
                    res[i] = left[offset] >= right[offset];
                } else if constexpr (op == proto::plan::OpType::LessEqual) {
                    res[i] = left[offset] <= right[offset];
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported op_type:{} for CompareElementFunc",
                            op));
                }
            }
            return;
        }

        if (!bitmap_input.empty()) {
            for (int i = 0; i < size; ++i) {
                if (!bitmap_input[start_cursor + i]) {
                    continue;
                }
                if constexpr (op == proto::plan::OpType::Equal) {
                    res[i] = left[i] == right[i];
                } else if constexpr (op == proto::plan::OpType::NotEqual) {
                    res[i] = left[i] != right[i];
                } else if constexpr (op == proto::plan::OpType::GreaterThan) {
                    res[i] = left[i] > right[i];
                } else if constexpr (op == proto::plan::OpType::LessThan) {
                    res[i] = left[i] < right[i];
                } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
                    res[i] = left[i] >= right[i];
                } else if constexpr (op == proto::plan::OpType::LessEqual) {
                    res[i] = left[i] <= right[i];
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported op_type:{} for CompareElementFunc",
                            op));
                }
            }
            return;
        }

        if constexpr (op == proto::plan::OpType::Equal) {
            res.inplace_compare_column<T, U, milvus::bitset::CompareOpType::EQ>(
                left, right, size);
        } else if constexpr (op == proto::plan::OpType::NotEqual) {
            res.inplace_compare_column<T, U, milvus::bitset::CompareOpType::NE>(
                left, right, size);
        } else if constexpr (op == proto::plan::OpType::GreaterThan) {
            res.inplace_compare_column<T, U, milvus::bitset::CompareOpType::GT>(
                left, right, size);
        } else if constexpr (op == proto::plan::OpType::LessThan) {
            res.inplace_compare_column<T, U, milvus::bitset::CompareOpType::LT>(
                left, right, size);
        } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
            res.inplace_compare_column<T, U, milvus::bitset::CompareOpType::GE>(
                left, right, size);
        } else if constexpr (op == proto::plan::OpType::LessEqual) {
            res.inplace_compare_column<T, U, milvus::bitset::CompareOpType::LE>(
                left, right, size);
        } else {
            PanicInfo(OpTypeInvalid,
                      fmt::format(
                          "unsupported op_type:{} for CompareElementFunc", op));
        }
    }
};

class PhyCompareFilterExpr : public Expr {
 public:
    PhyCompareFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::CompareExpr>& expr,
        const std::string& name,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size)
        : Expr(DataType::BOOL, std::move(input), name),
          left_field_(expr->left_field_id_),
          right_field_(expr->right_field_id_),
          segment_chunk_reader_(segment, active_count),
          batch_size_(batch_size),
          expr_(expr) {
        is_left_indexed_ = segment->HasIndex(left_field_);
        is_right_indexed_ = segment->HasIndex(right_field_);
        if (segment->is_chunked()) {
            left_num_chunk_ =
                is_left_indexed_
                    ? segment->num_chunk_index(expr_->left_field_id_)
                : segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(left_field_);
            right_num_chunk_ =
                is_right_indexed_
                    ? segment->num_chunk_index(expr_->right_field_id_)
                : segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(right_field_);
            num_chunk_ = left_num_chunk_;
        } else {
            num_chunk_ = is_left_indexed_
                             ? segment->num_chunk_index(expr_->left_field_id_)
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
    MoveCursor() override {
        if (!has_offset_input_) {
            if (segment_chunk_reader_.segment_->is_chunked()) {
                segment_chunk_reader_.MoveCursorForMultipleChunk(
                    left_current_chunk_id_,
                    left_current_chunk_pos_,
                    left_field_,
                    left_num_chunk_,
                    batch_size_);
                segment_chunk_reader_.MoveCursorForMultipleChunk(
                    right_current_chunk_id_,
                    right_current_chunk_pos_,
                    right_field_,
                    right_num_chunk_,
                    batch_size_);
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
    ToString() const {
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
    IsStringExpr();

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataChunks(FUNC func,
                          OffsetVector* input,
                          TargetBitmapView res,
                          TargetBitmapView valid_res,
                          ValTypes... values) {
        if (segment_chunk_reader_.segment_->is_chunked()) {
            return ProcessBothDataChunksForMultipleChunk<T,
                                                         U,
                                                         FUNC,
                                                         ValTypes...>(
                func, res, valid_res, values...);
        } else {
            return ProcessBothDataChunksForSingleChunk<T, U, FUNC, ValTypes...>(
                func, res, valid_res, values...);
        }
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataByOffsets(FUNC func,
                             OffsetVector* input,
                             TargetBitmapView res,
                             TargetBitmapView valid_res,
                             ValTypes... values) {
        int64_t size = input->size();
        int64_t processed_size = 0;
        const auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
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
                    } else {
                        return segment_chunk_reader_.segment_
                            ->get_chunk_by_offset(field, offset);
                    }
                };

                auto [left_chunk_id, left_chunk_offset] =
                    get_chunk_id_and_offset(left_field_);
                auto [right_chunk_id, right_chunk_offset] =
                    get_chunk_id_and_offset(right_field_);

                auto left_chunk = segment_chunk_reader_.segment_->chunk_data<T>(
                    left_field_, left_chunk_id);

                auto right_chunk =
                    segment_chunk_reader_.segment_->chunk_data<U>(
                        right_field_, right_chunk_id);
                const T* left_data = left_chunk.data() + left_chunk_offset;
                const U* right_data = right_chunk.data() + right_chunk_offset;
                func.template operator()<FilterType::random>(
                    left_data,
                    right_data,
                    nullptr,
                    1,
                    res + processed_size,
                    values...);
                const bool* left_valid_data = left_chunk.valid_data();
                const bool* right_valid_data = right_chunk.valid_data();
                // mask with valid_data
                if (left_valid_data && !left_valid_data[left_chunk_offset]) {
                    res[processed_size] = false;
                    valid_res[processed_size] = false;
                    continue;
                }
                if (right_valid_data && !right_valid_data[right_chunk_offset]) {
                    res[processed_size] = false;
                    valid_res[processed_size] = false;
                }
                processed_size++;
            }
            return processed_size;
        } else {
            auto left_chunk =
                segment_chunk_reader_.segment_->chunk_data<T>(left_field_, 0);
            auto right_chunk =
                segment_chunk_reader_.segment_->chunk_data<U>(right_field_, 0);
            const T* left_data = left_chunk.data();
            const U* right_data = right_chunk.data();
            func.template operator()<FilterType::random>(
                left_data, right_data, input->data(), size, res, values...);
            const bool* left_valid_data = left_chunk.valid_data();
            const bool* right_valid_data = right_chunk.valid_data();
            // mask with valid_data
            for (int i = 0; i < size; ++i) {
                if (left_valid_data && !left_valid_data[(*input)[i]]) {
                    res[i] = false;
                    valid_res[i] = false;
                    continue;
                }
                if (right_valid_data && !right_valid_data[(*input)[i]]) {
                    res[i] = false;
                    valid_res[i] = false;
                }
            }
            processed_size += size;
            return processed_size;
        }
    }

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataChunksForSingleChunk(FUNC func,
                                        TargetBitmapView res,
                                        TargetBitmapView valid_res,
                                        ValTypes... values) {
        int64_t processed_size = 0;

        const auto active_count = segment_chunk_reader_.active_count_;
        for (size_t i = current_chunk_id_; i < num_chunk_; i++) {
            auto left_chunk =
                segment_chunk_reader_.segment_->chunk_data<T>(left_field_, i);
            auto right_chunk =
                segment_chunk_reader_.segment_->chunk_data<U>(right_field_, i);
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
            // mask with valid_data
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
                                          ValTypes... values) {
        int64_t processed_size = 0;

        // only call this function when left and right are not indexed, so they have the same number of chunks
        for (size_t i = left_current_chunk_id_; i < left_num_chunk_; i++) {
            auto left_chunk =
                segment_chunk_reader_.segment_->chunk_data<T>(left_field_, i);
            auto right_chunk =
                segment_chunk_reader_.segment_->chunk_data<U>(right_field_, i);
            auto data_pos =
                (i == left_current_chunk_id_) ? left_current_chunk_pos_ : 0;
            auto size = 0;
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
            // mask with valid_data
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
};
}  //namespace exec
}  // namespace milvus
