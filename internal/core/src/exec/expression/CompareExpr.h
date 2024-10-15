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
#include <optional>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/type_c.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

using number_type = boost::variant<bool,
                                   int8_t,
                                   int16_t,
                                   int32_t,
                                   int64_t,
                                   float,
                                   double,
                                   std::string>;

using number = std::optional<number_type>;

using ChunkDataAccessor = std::function<const number(int)>;
using MultipleChunkDataAccessor = std::function<const number()>;

template <typename T, typename U, proto::plan::OpType op>
struct CompareElementFunc {
    void
    operator()(const T* left,
               const U* right,
               size_t size,
               TargetBitmapView res) {
        /*
        // This is the original code, kept here for the documentation purposes
        for (int i = 0; i < size; ++i) {
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
                    fmt::format("unsupported op_type:{} for CompareElementFunc",
                                op));
            }
        }
        */

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
          segment_(segment),
          active_count_(active_count),
          batch_size_(batch_size),
          expr_(expr) {
        is_left_indexed_ = segment_->HasIndex(left_field_);
        is_right_indexed_ = segment_->HasIndex(right_field_);
        size_per_chunk_ = segment_->size_per_chunk();
        if (segment_->is_chunked()) {
            left_num_chunk_ =
                is_left_indexed_
                    ? segment_->num_chunk_index(expr_->left_field_id_)
                : segment_->type() == SegmentType::Growing
                    ? upper_div(active_count_, size_per_chunk_)
                    : segment_->num_chunk_data(left_field_);
            right_num_chunk_ =
                is_right_indexed_
                    ? segment_->num_chunk_index(expr_->right_field_id_)
                : segment_->type() == SegmentType::Growing
                    ? upper_div(active_count_, size_per_chunk_)
                    : segment_->num_chunk_data(right_field_);
            num_chunk_ = left_num_chunk_;
        } else {
            num_chunk_ = is_left_indexed_
                             ? segment_->num_chunk_index(expr_->left_field_id_)
                             : upper_div(active_count_, size_per_chunk_);
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
        if (segment_->is_chunked()) {
            MoveCursorForMultipleChunk();
        } else {
            MoveCursorForSingleChunk();
        }
    }

    void
    MoveCursorForMultipleChunk() {
        int64_t processed_rows = 0;
        for (int64_t chunk_id = left_current_chunk_id_;
             chunk_id < left_num_chunk_;
             ++chunk_id) {
            auto chunk_size = 0;
            if (segment_->type() == SegmentType::Growing) {
                chunk_size = chunk_id == left_num_chunk_ - 1
                                 ? active_count_ - chunk_id * size_per_chunk_
                                 : size_per_chunk_;
            } else {
                chunk_size = segment_->chunk_size(left_field_, chunk_id);
            }

            for (int i = chunk_id == left_current_chunk_id_
                             ? left_current_chunk_pos_
                             : 0;
                 i < chunk_size;
                 ++i) {
                if (++processed_rows >= batch_size_) {
                    left_current_chunk_id_ = chunk_id;
                    left_current_chunk_pos_ = i + 1;
                }
            }
        }
        processed_rows = 0;
        for (int64_t chunk_id = right_current_chunk_id_;
             chunk_id < right_num_chunk_;
             ++chunk_id) {
            auto chunk_size = 0;
            if (segment_->type() == SegmentType::Growing) {
                chunk_size = chunk_id == right_num_chunk_ - 1
                                 ? active_count_ - chunk_id * size_per_chunk_
                                 : size_per_chunk_;
            } else {
                chunk_size = segment_->chunk_size(right_field_, chunk_id);
            }

            for (int i = chunk_id == right_current_chunk_id_
                             ? right_current_chunk_pos_
                             : 0;
                 i < chunk_size;
                 ++i) {
                if (++processed_rows >= batch_size_) {
                    right_current_chunk_id_ = chunk_id;
                    right_current_chunk_pos_ = i + 1;
                }
            }
        }
    }

    void
    MoveCursorForSingleChunk() {
        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id_; chunk_id < num_chunk_;
             ++chunk_id) {
            auto chunk_size = chunk_id == num_chunk_ - 1
                                  ? active_count_ - chunk_id * size_per_chunk_
                                  : size_per_chunk_;

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                if (++processed_rows >= batch_size_) {
                    current_chunk_id_ = chunk_id;
                    current_chunk_pos_ = i + 1;
                }
            }
        }
    }

    int64_t
    GetCurrentRows() {
        if (segment_->is_chunked()) {
            auto current_rows =
                is_left_indexed_ && segment_->type() == SegmentType::Sealed
                    ? left_current_chunk_pos_
                    : segment_->num_rows_until_chunk(left_field_,
                                                     left_current_chunk_id_) +
                          left_current_chunk_pos_;
            return current_rows;
        } else {
            return segment_->type() == SegmentType::Growing
                       ? current_chunk_id_ * size_per_chunk_ +
                             current_chunk_pos_
                       : current_chunk_pos_;
        }
    }

 private:
    int64_t
    GetNextBatchSize();

    bool
    IsStringExpr();

    template <typename T>
    MultipleChunkDataAccessor
    GetChunkData(FieldId field_id,
                 bool index,
                 int64_t& current_chunk_id,
                 int64_t& current_chunk_pos);

    template <typename T>
    ChunkDataAccessor
    GetChunkData(FieldId field_id, int chunk_id, int data_barrier);

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataChunks(FUNC func,
                          TargetBitmapView res,
                          TargetBitmapView valid_res,
                          ValTypes... values) {
        if (segment_->is_chunked()) {
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
    ProcessBothDataChunksForSingleChunk(FUNC func,
                                        TargetBitmapView res,
                                        TargetBitmapView valid_res,
                                        ValTypes... values) {
        int64_t processed_size = 0;

        for (size_t i = current_chunk_id_; i < num_chunk_; i++) {
            auto left_chunk = segment_->chunk_data<T>(left_field_, i);
            auto right_chunk = segment_->chunk_data<U>(right_field_, i);
            auto data_pos = (i == current_chunk_id_) ? current_chunk_pos_ : 0;
            auto size =
                (i == (num_chunk_ - 1))
                    ? (segment_->type() == SegmentType::Growing
                           ? (active_count_ % size_per_chunk_ == 0
                                  ? size_per_chunk_ - data_pos
                                  : active_count_ % size_per_chunk_ - data_pos)
                           : active_count_ - data_pos)
                    : size_per_chunk_ - data_pos;

            if (processed_size + size >= batch_size_) {
                size = batch_size_ - processed_size;
            }

            const T* left_data = left_chunk.data() + data_pos;
            const U* right_data = right_chunk.data() + data_pos;
            func(left_data, right_data, size, res + processed_size, values...);
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
            auto left_chunk = segment_->chunk_data<T>(left_field_, i);
            auto right_chunk = segment_->chunk_data<U>(right_field_, i);
            auto data_pos =
                (i == left_current_chunk_id_) ? left_current_chunk_pos_ : 0;
            auto size = 0;
            if (segment_->type() == SegmentType::Growing) {
                size = (i == (left_num_chunk_ - 1))
                           ? (active_count_ % size_per_chunk_ == 0
                                  ? size_per_chunk_ - data_pos
                                  : active_count_ % size_per_chunk_ - data_pos)
                           : size_per_chunk_ - data_pos;
            } else {
                size = segment_->chunk_size(left_field_, i) - data_pos;
            }

            if (processed_size + size >= batch_size_) {
                size = batch_size_ - processed_size;
            }

            const T* left_data = left_chunk.data() + data_pos;
            const U* right_data = right_chunk.data() + data_pos;
            func(left_data, right_data, size, res + processed_size, values...);
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

    MultipleChunkDataAccessor
    GetChunkData(DataType data_type,
                 FieldId field_id,
                 bool index,
                 int64_t& current_chunk_id,
                 int64_t& current_chunk_pos);

    ChunkDataAccessor
    GetChunkData(DataType data_type,
                 FieldId field_id,
                 int chunk_id,
                 int data_barrier);

    template <typename OpType>
    VectorPtr
    ExecCompareExprDispatcher(OpType op);

    VectorPtr
    ExecCompareExprDispatcherForHybridSegment();

    VectorPtr
    ExecCompareExprDispatcherForBothDataSegment();

    template <typename T>
    VectorPtr
    ExecCompareLeftType();

    template <typename T, typename U>
    VectorPtr
    ExecCompareRightType();

 private:
    const FieldId left_field_;
    const FieldId right_field_;
    bool is_left_indexed_;
    bool is_right_indexed_;
    int64_t active_count_{0};
    int64_t num_chunk_{0};
    int64_t left_num_chunk_{0};
    int64_t right_num_chunk_{0};
    int64_t left_current_chunk_id_{0};
    int64_t left_current_chunk_pos_{0};
    int64_t right_current_chunk_id_{0};
    int64_t right_current_chunk_pos_{0};
    int64_t current_chunk_id_{0};
    int64_t current_chunk_pos_{0};
    int64_t size_per_chunk_{0};

    const segcore::SegmentInternalInterface* segment_;
    int64_t batch_size_;
    std::shared_ptr<const milvus::expr::CompareExpr> expr_;
};
}  //namespace exec
}  // namespace milvus
