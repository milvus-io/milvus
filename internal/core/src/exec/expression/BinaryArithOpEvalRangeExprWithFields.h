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

#include <cmath>
#include <fmt/core.h>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentChunkReader.h"

namespace milvus {
namespace exec {

// Helper function for safe modulo operation
template <typename T, typename U>
decltype(auto)
safe_mod_fields(T a, U b) {
    if constexpr (std::is_floating_point_v<T> || std::is_floating_point_v<U>) {
        return std::fmod(a, b);
    } else {
        return a % b;
    }
}

// Element function for arithmetic operations between two fields
template <typename T,
          typename U,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op,
          FilterType filter_type = FilterType::sequential>
struct ArithOpTwoFieldsElementFunc {
    typedef std::conditional_t<
        std::is_floating_point_v<T> || std::is_floating_point_v<U>,
        double,
        int64_t>
        ResultType;

    void
    operator()(const T* left,
               const U* right,
               size_t size,
               ResultType val,
               TargetBitmapView res,
               const int32_t* offsets = nullptr) {
        for (int i = 0; i < size; ++i) {
            auto offset = (offsets != nullptr) ? offsets[i] : i;
            ResultType arith_result;

            // Perform arithmetic operation
            if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                arith_result = static_cast<ResultType>(left[offset]) +
                               static_cast<ResultType>(right[offset]);
            } else if constexpr (arith_op == proto::plan::ArithOpType::Sub) {
                arith_result = static_cast<ResultType>(left[offset]) -
                               static_cast<ResultType>(right[offset]);
            } else if constexpr (arith_op == proto::plan::ArithOpType::Mul) {
                arith_result = static_cast<ResultType>(left[offset]) *
                               static_cast<ResultType>(right[offset]);
            } else if constexpr (arith_op == proto::plan::ArithOpType::Div) {
                auto right_val = static_cast<ResultType>(right[offset]);
                // For division by zero: set result to false (element doesn't match filter)
                if (right_val == static_cast<ResultType>(0)) {
                    res[i] = false;
                    continue;
                }
                arith_result = static_cast<ResultType>(left[offset]) / right_val;
            } else if constexpr (arith_op == proto::plan::ArithOpType::Mod) {
                auto right_val = static_cast<ResultType>(right[offset]);
                // For modulo by zero: set result to false (element doesn't match filter)
                if (right_val == static_cast<ResultType>(0)) {
                    res[i] = false;
                    continue;
                }
                arith_result =
                    safe_mod_fields(static_cast<ResultType>(left[offset]),
                                    right_val);
            } else {
                ThrowInfo(OpTypeInvalid,
                          fmt::format("unsupported arith type:{} for "
                                      "ArithOpTwoFieldsElementFunc",
                                      arith_op));
            }

            // Perform comparison operation
            if constexpr (cmp_op == proto::plan::OpType::Equal) {
                res[i] = arith_result == val;
            } else if constexpr (cmp_op == proto::plan::OpType::NotEqual) {
                res[i] = arith_result != val;
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterThan) {
                res[i] = arith_result > val;
            } else if constexpr (cmp_op == proto::plan::OpType::LessThan) {
                res[i] = arith_result < val;
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterEqual) {
                res[i] = arith_result >= val;
            } else if constexpr (cmp_op == proto::plan::OpType::LessEqual) {
                res[i] = arith_result <= val;
            } else {
                ThrowInfo(OpTypeInvalid,
                          fmt::format("unsupported cmp type:{} for "
                                      "ArithOpTwoFieldsElementFunc",
                                      cmp_op));
            }
        }
    }
};

class PhyBinaryArithOpEvalRangeExprWithFields : public Expr {
 public:
    PhyBinaryArithOpEvalRangeExprWithFields(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::BinaryArithOpEvalRangeExprWithFields>&
            expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size)
        : Expr(DataType::BOOL, std::move(input), name, op_ctx),
          left_field_(expr->left_column_.field_id_),
          right_field_(expr->right_column_.field_id_),
          segment_chunk_reader_(op_ctx, segment, active_count),
          batch_size_(batch_size),
          expr_(expr) {
        auto& schema = segment->get_schema();
        auto& left_field_meta = schema[left_field_];
        auto& right_field_meta = schema[right_field_];

        if (segment->is_chunked()) {
            left_num_chunk_ =
                segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(left_field_);
            right_num_chunk_ =
                segment->type() == SegmentType::Growing
                    ? upper_div(segment_chunk_reader_.active_count_,
                                segment_chunk_reader_.SizePerChunk())
                    : segment->num_chunk_data(right_field_);
            num_chunk_ = left_num_chunk_;
        } else {
            num_chunk_ = upper_div(segment_chunk_reader_.active_count_,
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
                segment_chunk_reader_.MoveCursor(current_chunk_id_,
                                                 current_chunk_pos_,
                                                 num_chunk_,
                                                 batch_size_);
            }
        }
    }

 private:
    int64_t
    GetNextBatchSize();

    VectorPtr
    EvalForBothDataSegment();

    template <typename T, typename U>
    VectorPtr
    ExecArithOpEvalRangeForTwoFields();

    template <typename T, typename U, proto::plan::OpType cmp_op>
    VectorPtr
    ExecArithOpEvalRangeForTwoFieldsWithCmpOp();

    template <typename T,
              typename U,
              proto::plan::OpType cmp_op,
              proto::plan::ArithOpType arith_op>
    VectorPtr
    ExecArithOpEvalRangeForTwoFieldsImpl();

 private:
    const FieldId left_field_;
    const FieldId right_field_;
    int64_t left_num_chunk_{0};
    int64_t right_num_chunk_{0};
    int64_t num_chunk_{0};
    int64_t current_chunk_id_{0};
    int64_t current_chunk_pos_{0};
    int64_t left_current_chunk_id_{0};
    int64_t left_current_chunk_pos_{0};
    int64_t right_current_chunk_id_{0};
    int64_t right_current_chunk_pos_{0};
    SegmentChunkReader segment_chunk_reader_;
    int64_t batch_size_;
    const std::shared_ptr<const milvus::expr::BinaryArithOpEvalRangeExprWithFields>
        expr_;
};

}  // namespace exec
}  // namespace milvus
