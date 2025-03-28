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
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

using number = boost::variant<bool,
                              int8_t,
                              int16_t,
                              int32_t,
                              int64_t,
                              float,
                              double,
                              std::string>;
using ChunkDataAccessor = std::function<const number(int)>;

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
        num_chunk_ = is_left_indexed_
                         ? segment_->num_chunk_index(expr_->left_field_id_)
                         : upper_div(active_count_, size_per_chunk_);
        AssertInfo(
            batch_size_ > 0,
            fmt::format("expr batch size should greater than zero, but now: {}",
                        batch_size_));
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override {
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
                    break;
                }
            }
        }
    }

 private:
    int64_t
    GetNextBatchSize();

    bool
    IsStringExpr();

    template <typename T>
    ChunkDataAccessor
    GetChunkData(FieldId field_id, int chunk_id, int data_barrier);

    template <typename T, typename U, typename FUNC, typename... ValTypes>
    int64_t
    ProcessBothDataChunks(FUNC func, TargetBitmapView res, ValTypes... values) {
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
            processed_size += size;

            if (processed_size >= batch_size_) {
                current_chunk_id_ = i;
                current_chunk_pos_ = data_pos + size;
                break;
            }
        }

        return processed_size;
    }

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
    int64_t current_chunk_id_{0};
    int64_t current_chunk_pos_{0};
    int64_t size_per_chunk_{0};

    const segcore::SegmentInternalInterface* segment_;
    int64_t batch_size_;
    std::shared_ptr<const milvus::expr::CompareExpr> expr_;
};
}  //namespace exec
}  // namespace milvus
