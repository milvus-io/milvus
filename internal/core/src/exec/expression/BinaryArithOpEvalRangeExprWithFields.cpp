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

#include "query/Utils.h"

namespace milvus {
namespace exec {

void
PhyBinaryArithOpEvalRangeExprWithFields::Eval(EvalCtx& context,
                                               VectorPtr& result) {
    result = EvalForBothDataSegment();
    AssertInfo(result != nullptr,
               "PhyBinaryArithOpEvalRangeExprWithFields::Eval result is nullptr");
}

int64_t
PhyBinaryArithOpEvalRangeExprWithFields::GetNextBatchSize() {
    auto* segment = segment_chunk_reader_.segment_;
    if (segment->is_chunked()) {
        // For chunked segments, we need to get the minimum batch size from both fields
        // to ensure we don't read beyond chunk boundaries for either field
        auto left_batch_size = segment_chunk_reader_.GetNextBatchSize(
            left_current_chunk_id_, left_current_chunk_pos_, left_num_chunk_, batch_size_);
        auto right_batch_size = segment_chunk_reader_.GetNextBatchSize(
            right_current_chunk_id_, right_current_chunk_pos_, right_num_chunk_, batch_size_);
        return std::min(left_batch_size, right_batch_size);
    } else {
        return segment_chunk_reader_.GetNextBatchSize(
            current_chunk_id_, current_chunk_pos_, num_chunk_, batch_size_);
    }
}

VectorPtr
PhyBinaryArithOpEvalRangeExprWithFields::EvalForBothDataSegment() {
    auto& left_column = expr_->left_column_;
    auto& right_column = expr_->right_column_;
    auto left_type = left_column.data_type_;
    auto right_type = right_column.data_type_;

    // Dispatch based on left and right types - supporting numeric types
    switch (left_type) {
        case DataType::INT8:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, int8_t>();
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, int16_t>();
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, int32_t>();
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, int64_t>();
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, float>();
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<int8_t, double>();
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::INT16:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, int8_t>();
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, int16_t>();
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, int32_t>();
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, int64_t>();
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, float>();
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<int16_t, double>();
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::INT32:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, int8_t>();
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, int16_t>();
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, int32_t>();
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, int64_t>();
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, float>();
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<int32_t, double>();
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::INT64:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, int8_t>();
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, int16_t>();
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, int32_t>();
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, int64_t>();
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, float>();
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<int64_t, double>();
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::FLOAT:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<float, int8_t>();
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<float, int16_t>();
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<float, int32_t>();
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<float, int64_t>();
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<float, float>();
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<float, double>();
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported right data type: " +
                                  std::to_string(static_cast<int>(right_type)));
            }
        case DataType::DOUBLE:
            switch (right_type) {
                case DataType::INT8:
                    return ExecArithOpEvalRangeForTwoFields<double, int8_t>();
                case DataType::INT16:
                    return ExecArithOpEvalRangeForTwoFields<double, int16_t>();
                case DataType::INT32:
                    return ExecArithOpEvalRangeForTwoFields<double, int32_t>();
                case DataType::INT64:
                    return ExecArithOpEvalRangeForTwoFields<double, int64_t>();
                case DataType::FLOAT:
                    return ExecArithOpEvalRangeForTwoFields<double, float>();
                case DataType::DOUBLE:
                    return ExecArithOpEvalRangeForTwoFields<double, double>();
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
PhyBinaryArithOpEvalRangeExprWithFields::ExecArithOpEvalRangeForTwoFields() {
    auto cmp_op = expr_->op_type_;
    switch (cmp_op) {
        case proto::plan::OpType::Equal:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::Equal>();
        case proto::plan::OpType::NotEqual:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::NotEqual>();
        case proto::plan::OpType::GreaterThan:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::GreaterThan>();
        case proto::plan::OpType::LessThan:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::LessThan>();
        case proto::plan::OpType::GreaterEqual:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::GreaterEqual>();
        case proto::plan::OpType::LessEqual:
            return ExecArithOpEvalRangeForTwoFieldsWithCmpOp<
                T,
                U,
                proto::plan::OpType::LessEqual>();
        default:
            ThrowInfo(OpTypeInvalid,
                      fmt::format("unsupported comparison op type: {}", cmp_op));
    }
}

template <typename T, typename U, proto::plan::OpType cmp_op>
VectorPtr
PhyBinaryArithOpEvalRangeExprWithFields::ExecArithOpEvalRangeForTwoFieldsWithCmpOp() {
    auto arith_op = expr_->arith_op_type_;
    switch (arith_op) {
        case proto::plan::ArithOpType::Add:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Add>();
        case proto::plan::ArithOpType::Sub:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Sub>();
        case proto::plan::ArithOpType::Mul:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Mul>();
        case proto::plan::ArithOpType::Div:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Div>();
        case proto::plan::ArithOpType::Mod:
            return ExecArithOpEvalRangeForTwoFieldsImpl<
                T,
                U,
                cmp_op,
                proto::plan::ArithOpType::Mod>();
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
PhyBinaryArithOpEvalRangeExprWithFields::ExecArithOpEvalRangeForTwoFieldsImpl() {
    typedef std::conditional_t<
        std::is_floating_point_v<T> || std::is_floating_point_v<U>,
        double,
        int64_t>
        ResultType;

    // Get the comparison value
    auto& value = expr_->value_;
    ResultType val;
    if constexpr (std::is_floating_point_v<ResultType>) {
        val = GetValueFromProto<double>(value);
    } else {
        val = GetValueFromProto<int64_t>(value);
    }

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    res.reset();

    auto* segment = segment_chunk_reader_.segment_;

    // Get data from both columns
    if (segment->is_chunked()) {
        // For chunked segments, we need to handle the data differently
        auto left_span = segment->chunk_data<T>(left_field_, left_current_chunk_id_);
        auto right_span = segment->chunk_data<U>(right_field_, right_current_chunk_id_);

        const T* left_data = left_span.data() + left_current_chunk_pos_;
        const U* right_data = right_span.data() + right_current_chunk_pos_;

        ArithOpTwoFieldsElementFunc<T, U, cmp_op, arith_op, FilterType::sequential>
            func;
        func(left_data, right_data, real_batch_size, val, res);
    } else {
        auto left_span = segment->chunk_data<T>(left_field_, current_chunk_id_);
        auto right_span = segment->chunk_data<U>(right_field_, current_chunk_id_);

        const T* left_data = left_span.data() + current_chunk_pos_;
        const U* right_data = right_span.data() + current_chunk_pos_;

        ArithOpTwoFieldsElementFunc<T, U, cmp_op, arith_op, FilterType::sequential>
            func;
        func(left_data, right_data, real_batch_size, val, res);
    }

    return res_vec;
}

}  // namespace exec
}  // namespace milvus
