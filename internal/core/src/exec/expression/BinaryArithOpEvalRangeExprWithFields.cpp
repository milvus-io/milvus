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

#include "BinaryArithOpEvalRangeExprWithFields.h"

#include <algorithm>
#include <utility>

#include "exec/expression/Utils.h"

namespace milvus {
namespace exec {

void
PhyBinaryArithOpEvalRangeExprWithFields::Eval(EvalCtx& context,
                                              VectorPtr& result) {
    SetHasOffsetInput(context.get_offset_input() != nullptr);
    result = EvalForBothDataSegment(context);
}

int64_t
PhyBinaryArithOpEvalRangeExprWithFields::GetNextBatchSize() {
    auto* segment = segment_chunk_reader_.segment_;
    if (segment->is_chunked()) {
        auto left_rows =
            segment->num_rows_until_chunk(left_field_, left_current_chunk_id_) +
            left_current_chunk_pos_;
        auto right_rows = segment->num_rows_until_chunk(
                              right_field_, right_current_chunk_id_) +
                          right_current_chunk_pos_;
        auto left_batch_size =
            left_rows + batch_size_ >= segment_chunk_reader_.active_count_
                ? segment_chunk_reader_.active_count_ - left_rows
                : batch_size_;
        auto right_batch_size =
            right_rows + batch_size_ >= segment_chunk_reader_.active_count_
                ? segment_chunk_reader_.active_count_ - right_rows
                : batch_size_;
        auto left_chunk_remaining =
            segment->chunk_size(left_field_, left_current_chunk_id_) -
            left_current_chunk_pos_;
        auto right_chunk_remaining =
            segment->chunk_size(right_field_, right_current_chunk_id_) -
            right_current_chunk_pos_;
        return std::min({left_batch_size,
                         right_batch_size,
                         left_chunk_remaining,
                         right_chunk_remaining});
    } else {
        auto current_rows =
            segment->type() == SegmentType::Growing
                ? current_chunk_id_ * segment_chunk_reader_.SizePerChunk() +
                      current_chunk_pos_
                : current_chunk_pos_;
        auto batch_size =
            current_rows + batch_size_ >= segment_chunk_reader_.active_count_
                ? segment_chunk_reader_.active_count_ - current_rows
                : batch_size_;
        if (segment->type() == SegmentType::Growing) {
            auto chunk_remaining =
                segment_chunk_reader_.SizePerChunk() - current_chunk_pos_;
            return std::min(batch_size, chunk_remaining);
        }
        return batch_size;
    }
}

void
PhyBinaryArithOpEvalRangeExprWithFields::MoveCursorBy(int64_t size) {
    if (size <= 0) {
        return;
    }

    auto* segment = segment_chunk_reader_.segment_;
    auto active_count = segment_chunk_reader_.active_count_;
    if (segment->is_chunked()) {
        auto move_field_cursor = [segment, active_count](FieldId field,
                                                         int64_t num_chunk,
                                                         int64_t& chunk_id,
                                                         int64_t& chunk_pos,
                                                         int64_t move_size) {
            auto current_offset =
                segment->num_rows_until_chunk(field, chunk_id) + chunk_pos;
            auto target_offset =
                std::min(active_count, current_offset + move_size);
            if (target_offset >= active_count) {
                chunk_id = num_chunk - 1;
                chunk_pos = active_count -
                            segment->num_rows_until_chunk(field, chunk_id);
                return;
            }
            auto [next_chunk_id, next_chunk_pos] =
                segment->get_chunk_by_offset(field, target_offset);
            chunk_id = next_chunk_id;
            chunk_pos = next_chunk_pos;
        };

        move_field_cursor(left_field_,
                          left_num_chunk_,
                          left_current_chunk_id_,
                          left_current_chunk_pos_,
                          size);
        move_field_cursor(right_field_,
                          right_num_chunk_,
                          right_current_chunk_id_,
                          right_current_chunk_pos_,
                          size);
        return;
    }

    auto current_rows =
        segment->type() == SegmentType::Growing
            ? current_chunk_id_ * segment_chunk_reader_.SizePerChunk() +
                  current_chunk_pos_
            : current_chunk_pos_;
    auto target_rows = std::min(active_count, current_rows + size);
    if (segment->type() == SegmentType::Growing) {
        auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
        if (target_rows >= active_count) {
            current_chunk_id_ = num_chunk_ - 1;
            current_chunk_pos_ =
                active_count - current_chunk_id_ * size_per_chunk;
            return;
        }
        current_chunk_id_ = target_rows / size_per_chunk;
        current_chunk_pos_ = target_rows % size_per_chunk;
    } else {
        current_chunk_pos_ = target_rows;
    }
}

VectorPtr
PhyBinaryArithOpEvalRangeExprWithFields::EvalForBothDataSegment(
    EvalCtx& context) {
    auto& left_column = expr_->left_column_;
    auto& right_column = expr_->right_column_;
    auto left_type = left_column.data_type_;
    auto right_type = right_column.data_type_;

    // Dispatch based on left and right types - supporting numeric types
    switch (left_type) {
        case DataType::INT8:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, int8_t>(
                        context);
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, int16_t>(
                        context);
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, int32_t>(
                        context);
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, int64_t>(
                        context);
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, float>(
                        context);
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, double>(
                        context);
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::INT16:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, int8_t>(
                        context);
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, int16_t>(
                        context);
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, int32_t>(
                        context);
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, int64_t>(
                        context);
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, float>(
                        context);
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, double>(
                        context);
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::INT32:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, int8_t>(
                        context);
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, int16_t>(
                        context);
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, int32_t>(
                        context);
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, int64_t>(
                        context);
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, float>(
                        context);
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, double>(
                        context);
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::INT64:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, int8_t>(
                        context);
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, int16_t>(
                        context);
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, int32_t>(
                        context);
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, int64_t>(
                        context);
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, float>(
                        context);
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, double>(
                        context);
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::FLOAT:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<float, int8_t>(
                        context);
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<float, int16_t>(
                        context);
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<float, int32_t>(
                        context);
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<float, int64_t>(
                        context);
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<float, float>(
                        context);
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<float, double>(
                        context);
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::DOUBLE:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<double, int8_t>(
                        context);
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<double, int16_t>(
                        context);
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<double, int32_t>(
                        context);
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<double, int64_t>(
                        context);
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<double, float>(
                        context);
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<double, double>(
                        context);
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported left data type: " +
                          std::to_string(static_cast<int>(left_type)));
    }
}

template <typename T, typename U>
VectorPtr
PhyBinaryArithOpEvalRangeExprWithFields::ExecArithOpEvalRangeForTwoFields(
    EvalCtx& context) {
    auto cmp_op = expr_->op_type_;
    switch (cmp_op) {
        case proto::plan::OpType::Equal:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::Equal>(context);
        case proto::plan::OpType::NotEqual:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::NotEqual>(context);
        case proto::plan::OpType::GreaterThan:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::GreaterThan>(context);
        case proto::plan::OpType::LessThan:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::LessThan>(context);
        case proto::plan::OpType::GreaterEqual:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::GreaterEqual>(context);
        case proto::plan::OpType::LessEqual:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::LessEqual>(context);
        default:
            ThrowInfo(
                OpTypeInvalid,
                fmt::format("unsupported comparison op type: {}", cmp_op));
    }
}

template <typename T, typename U, proto::plan::OpType cmp_op>
VectorPtr
PhyBinaryArithOpEvalRangeExprWithFields::
    ExecArithOpEvalRangeForTwoFieldsWithCmpOp(EvalCtx& context) {
    auto arith_op = expr_->arith_op_type_;
    switch (arith_op) {
        case proto::plan::ArithOpType::Add:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Add>(context);
        case proto::plan::ArithOpType::Sub:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Sub>(context);
        case proto::plan::ArithOpType::Mul:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Mul>(context);
        case proto::plan::ArithOpType::Div:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Div>(context);
        case proto::plan::ArithOpType::Mod:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Mod>(context);
        default:
            ThrowInfo(OpTypeInvalid,
                      fmt::format("unsupported arith op type: {}", arith_op));
    }
}

template <typename T,
          typename U,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op>
VectorPtr
PhyBinaryArithOpEvalRangeExprWithFields::ExecArithOpEvalRangeForTwoFieldsImpl(
    EvalCtx& context) {
    typedef std::conditional_t<std::is_floating_point_v<T> ||
                                   std::is_floating_point_v<U>,
                               double,
                               int64_t>
        ResultType;

    // Get the comparison value
    auto& value = expr_->value_;
    ResultType val;
    if constexpr (std::is_floating_point_v<ResultType>) {
        val = GetValueWithCastNumber<double>(value);
    } else {
        val = GetValueFromProto<int64_t>(value);
    }

    auto* input = context.get_offset_input();
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto* segment = segment_chunk_reader_.segment_;
    const auto& bitmap_input = context.get_bitmap_input();

    if (has_offset_input_) {
        auto get_chunk_position =
            [this, segment](FieldId field,
                            int64_t offset) -> std::pair<int64_t, int64_t> {
            if (segment->is_chunked()) {
                return segment->get_chunk_by_offset(field, offset);
            }
            if (segment->type() == SegmentType::Growing) {
                auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
                return std::make_pair(offset / size_per_chunk,
                                      offset % size_per_chunk);
            }
            return std::pair<int64_t, int64_t>{0, offset};
        };

        ArithOpTwoFieldsElementFunc<T,
                                    U,
                                    cmp_op,
                                    arith_op,
                                    FilterType::sequential>
            func;
        for (size_t i = 0; i < input->size(); ++i) {
            auto offset = (*input)[i];
            auto [left_chunk_id, left_chunk_pos] =
                get_chunk_position(left_field_, offset);
            auto [right_chunk_id, right_chunk_pos] =
                get_chunk_position(right_field_, offset);
            auto left_pw =
                segment->chunk_data<T>(op_ctx_, left_field_, left_chunk_id);
            auto right_pw =
                segment->chunk_data<U>(op_ctx_, right_field_, right_chunk_id);
            auto left_span = left_pw.get();
            auto right_span = right_pw.get();
            AssertInfo(left_chunk_pos >= 0 &&
                           left_chunk_pos <
                               static_cast<int64_t>(left_span.row_count()),
                       "Left field: accessing beyond chunk boundary");
            AssertInfo(right_chunk_pos >= 0 &&
                           right_chunk_pos <
                               static_cast<int64_t>(right_span.row_count()),
                       "Right field: accessing beyond chunk boundary");
            const T* left_data = left_span.data() + left_chunk_pos;
            const U* right_data = right_span.data() + right_chunk_pos;
            const bool* left_valid_data = left_span.valid_data();
            const bool* right_valid_data = right_span.valid_data();
            if (left_valid_data != nullptr) {
                left_valid_data += left_chunk_pos;
            }
            if (right_valid_data != nullptr) {
                right_valid_data += right_chunk_pos;
            }
            func(left_data,
                 right_data,
                 left_valid_data,
                 right_valid_data,
                 1,
                 val,
                 res + i,
                 valid_res + i,
                 bitmap_input,
                 nullptr,
                 i);
        }
        return res_vec;
    }

    // Get data from both columns
    if (segment->is_chunked()) {
        // For chunked segments, we need to handle the data differently
        auto left_pw = segment->chunk_data<T>(
            op_ctx_, left_field_, left_current_chunk_id_);
        auto right_pw = segment->chunk_data<U>(
            op_ctx_, right_field_, right_current_chunk_id_);
        auto left_span = left_pw.get();
        auto right_span = right_pw.get();

        AssertInfo(left_current_chunk_pos_ >= 0 &&
                       left_current_chunk_pos_ + real_batch_size <=
                           static_cast<int64_t>(left_span.row_count()),
                   "Left field: accessing beyond chunk boundary");
        AssertInfo(right_current_chunk_pos_ >= 0 &&
                       right_current_chunk_pos_ + real_batch_size <=
                           static_cast<int64_t>(right_span.row_count()),
                   "Right field: accessing beyond chunk boundary");
        const T* left_data = left_span.data() + left_current_chunk_pos_;
        const U* right_data = right_span.data() + right_current_chunk_pos_;
        const bool* left_valid_data = left_span.valid_data();
        const bool* right_valid_data = right_span.valid_data();
        if (left_valid_data != nullptr) {
            left_valid_data += left_current_chunk_pos_;
        }
        if (right_valid_data != nullptr) {
            right_valid_data += right_current_chunk_pos_;
        }

        ArithOpTwoFieldsElementFunc<T,
                                    U,
                                    cmp_op,
                                    arith_op,
                                    FilterType::sequential>
            func;
        func(left_data,
             right_data,
             left_valid_data,
             right_valid_data,
             real_batch_size,
             val,
             res,
             valid_res,
             bitmap_input);

        MoveCursorBy(real_batch_size);
    } else {
        auto left_pw =
            segment->chunk_data<T>(op_ctx_, left_field_, current_chunk_id_);
        auto right_pw =
            segment->chunk_data<U>(op_ctx_, right_field_, current_chunk_id_);
        auto left_span = left_pw.get();
        auto right_span = right_pw.get();

        AssertInfo(current_chunk_pos_ >= 0 &&
                       current_chunk_pos_ + real_batch_size <=
                           static_cast<int64_t>(left_span.row_count()),
                   "Left field: accessing beyond chunk boundary");
        AssertInfo(current_chunk_pos_ >= 0 &&
                       current_chunk_pos_ + real_batch_size <=
                           static_cast<int64_t>(right_span.row_count()),
                   "Right field: accessing beyond chunk boundary");
        const T* left_data = left_span.data() + current_chunk_pos_;
        const U* right_data = right_span.data() + current_chunk_pos_;
        const bool* left_valid_data = left_span.valid_data();
        const bool* right_valid_data = right_span.valid_data();
        if (left_valid_data != nullptr) {
            left_valid_data += current_chunk_pos_;
        }
        if (right_valid_data != nullptr) {
            right_valid_data += current_chunk_pos_;
        }

        ArithOpTwoFieldsElementFunc<T,
                                    U,
                                    cmp_op,
                                    arith_op,
                                    FilterType::sequential>
            func;
        func(left_data,
             right_data,
             left_valid_data,
             right_valid_data,
             real_batch_size,
             val,
             res,
             valid_res,
             bitmap_input);

        MoveCursorBy(real_batch_size);
    }

    return res_vec;
}

}  // namespace exec
}  // namespace milvus
