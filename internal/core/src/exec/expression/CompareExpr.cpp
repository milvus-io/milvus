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

#include "CompareExpr.h"
#include <optional>
#include "query/Relational.h"

namespace milvus {
namespace exec {

bool
PhyCompareFilterExpr::IsStringExpr() {
    return expr_->left_data_type_ == DataType::VARCHAR ||
           expr_->right_data_type_ == DataType::VARCHAR;
}

int64_t
PhyCompareFilterExpr::GetNextBatchSize() {
    auto current_rows = GetCurrentRows();

    return current_rows + batch_size_ >= segment_chunk_reader_.active_count_
               ? segment_chunk_reader_.active_count_ - current_rows
               : batch_size_;
}

template <typename OpType>
VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcher(OpType op) {
    if (segment_chunk_reader_.segment_->is_chunked()) {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
        TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
        valid_res.set();

        auto left =
            segment_chunk_reader_.GetChunkDataAccessor(expr_->left_data_type_,
                                                       expr_->left_field_id_,
                                                       is_left_indexed_,
                                                       left_current_chunk_id_,
                                                       left_current_chunk_pos_);
        auto right = segment_chunk_reader_.GetChunkDataAccessor(
            expr_->right_data_type_,
            expr_->right_field_id_,
            is_right_indexed_,
            right_current_chunk_id_,
            right_current_chunk_pos_);
        for (int i = 0; i < real_batch_size; ++i) {
            auto left_value = left(), right_value = right();
            if (!left_value.has_value() || !right_value.has_value()) {
                res[i] = false;
                valid_res[i] = false;
                continue;
            }
            res[i] =
                boost::apply_visitor(milvus::query::Relational<decltype(op)>{},
                                     left_value.value(),
                                     right_value.value());
        }
        return res_vec;
    } else {
        auto real_batch_size = GetNextBatchSize();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec = std::make_shared<ColumnVector>(
            TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
        TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
        valid_res.set();

        auto left_data_barrier = segment_chunk_reader_.segment_->num_chunk_data(
            expr_->left_field_id_);
        auto right_data_barrier =
            segment_chunk_reader_.segment_->num_chunk_data(
                expr_->right_field_id_);

        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id_; chunk_id < num_chunk_;
             ++chunk_id) {
            auto chunk_size =
                chunk_id == num_chunk_ - 1
                    ? segment_chunk_reader_.active_count_ -
                          chunk_id * segment_chunk_reader_.SizePerChunk()
                    : segment_chunk_reader_.SizePerChunk();
            auto left = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->left_data_type_,
                expr_->left_field_id_,
                chunk_id,
                left_data_barrier);
            auto right = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->right_data_type_,
                expr_->right_field_id_,
                chunk_id,
                right_data_barrier);

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                if (!left(i).has_value() || !right(i).has_value()) {
                    res[processed_rows] = false;
                    valid_res[processed_rows] = false;
                } else {
                    res[processed_rows] = boost::apply_visitor(
                        milvus::query::Relational<decltype(op)>{},
                        left(i).value(),
                        right(i).value());
                }
                processed_rows++;

                if (processed_rows >= batch_size_) {
                    current_chunk_id_ = chunk_id;
                    current_chunk_pos_ = i + 1;
                    return res_vec;
                }
            }
        }
        return res_vec;
    }
}

void
PhyCompareFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    // For segment both fields has no index, can use SIMD to speed up.
    // Avoiding too much call stack that blocks SIMD.
    if (!is_left_indexed_ && !is_right_indexed_ && !IsStringExpr()) {
        result = ExecCompareExprDispatcherForBothDataSegment();
        return;
    }
    result = ExecCompareExprDispatcherForHybridSegment();
}

VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcherForHybridSegment() {
    switch (expr_->op_type_) {
        case OpType::Equal: {
            return ExecCompareExprDispatcher(std::equal_to<>{});
        }
        case OpType::NotEqual: {
            return ExecCompareExprDispatcher(std::not_equal_to<>{});
        }
        case OpType::GreaterEqual: {
            return ExecCompareExprDispatcher(std::greater_equal<>{});
        }
        case OpType::GreaterThan: {
            return ExecCompareExprDispatcher(std::greater<>{});
        }
        case OpType::LessEqual: {
            return ExecCompareExprDispatcher(std::less_equal<>{});
        }
        case OpType::LessThan: {
            return ExecCompareExprDispatcher(std::less<>{});
        }
        case OpType::PrefixMatch: {
            return ExecCompareExprDispatcher(
                milvus::query::MatchOp<OpType::PrefixMatch>{});
        }
            // case OpType::PostfixMatch: {
            // }
        default: {
            PanicInfo(OpTypeInvalid, "unsupported optype: {}", expr_->op_type_);
        }
    }
}

VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcherForBothDataSegment() {
    switch (expr_->left_data_type_) {
        case DataType::BOOL:
            return ExecCompareLeftType<bool>();
        case DataType::INT8:
            return ExecCompareLeftType<int8_t>();
        case DataType::INT16:
            return ExecCompareLeftType<int16_t>();
        case DataType::INT32:
            return ExecCompareLeftType<int32_t>();
        case DataType::INT64:
            return ExecCompareLeftType<int64_t>();
        case DataType::FLOAT:
            return ExecCompareLeftType<float>();
        case DataType::DOUBLE:
            return ExecCompareLeftType<double>();
        default:
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported left datatype:{} of compare expr",
                            expr_->left_data_type_));
    }
}

template <typename T>
VectorPtr
PhyCompareFilterExpr::ExecCompareLeftType() {
    switch (expr_->right_data_type_) {
        case DataType::BOOL:
            return ExecCompareRightType<T, bool>();
        case DataType::INT8:
            return ExecCompareRightType<T, int8_t>();
        case DataType::INT16:
            return ExecCompareRightType<T, int16_t>();
        case DataType::INT32:
            return ExecCompareRightType<T, int32_t>();
        case DataType::INT64:
            return ExecCompareRightType<T, int64_t>();
        case DataType::FLOAT:
            return ExecCompareRightType<T, float>();
        case DataType::DOUBLE:
            return ExecCompareRightType<T, double>();
        default:
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported right datatype:{} of compare expr",
                            expr_->right_data_type_));
    }
}

template <typename T, typename U>
VectorPtr
PhyCompareFilterExpr::ExecCompareRightType() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto expr_type = expr_->op_type_;
    auto execute_sub_batch = [expr_type](const T* left,
                                         const U* right,
                                         const int size,
                                         TargetBitmapView res) {
        switch (expr_type) {
            case proto::plan::GreaterThan: {
                CompareElementFunc<T, U, proto::plan::GreaterThan> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::GreaterEqual: {
                CompareElementFunc<T, U, proto::plan::GreaterEqual> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::LessThan: {
                CompareElementFunc<T, U, proto::plan::LessThan> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::LessEqual: {
                CompareElementFunc<T, U, proto::plan::LessEqual> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::Equal: {
                CompareElementFunc<T, U, proto::plan::Equal> func;
                func(left, right, size, res);
                break;
            }
            case proto::plan::NotEqual: {
                CompareElementFunc<T, U, proto::plan::NotEqual> func;
                func(left, right, size, res);
                break;
            }
            default:
                PanicInfo(OpTypeInvalid,
                          fmt::format("unsupported operator type for "
                                      "compare column expr: {}",
                                      expr_type));
        }
    };
    int64_t processed_size =
        ProcessBothDataChunks<T, U>(execute_sub_batch, res, valid_res);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
};

}  //namespace exec
}  // namespace milvus
