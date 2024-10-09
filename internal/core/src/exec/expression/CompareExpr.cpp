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
PhyCompareFilterExpr::ExecCompareExprDispatcher(OpType op,
                                                OffsetVector* input) {
    // take offsets as input
    if (has_offset_input_) {
        auto real_batch_size = input->size();
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
        const auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
        for (auto i = 0; i < real_batch_size; ++i) {
            auto offset = (*input)[i];
            auto get_chunk_id_and_offset =
                [&](const FieldId field,
                    const int64_t data_barrier) -> std::pair<int64_t, int64_t> {
                if (segment_chunk_reader_.segment_->type() ==
                    SegmentType::Growing) {
                    return {offset / size_per_chunk, offset % size_per_chunk};
                } else if (segment_chunk_reader_.segment_->is_chunked() &&
                           data_barrier > 0) {
                    return segment_chunk_reader_.segment_->get_chunk_by_offset(
                        field, offset);
                } else {
                    return {0, offset};
                }
            };

            auto [left_chunk_id, left_chunk_offset] =
                get_chunk_id_and_offset(left_field_, left_data_barrier);
            auto [right_chunk_id, right_chunk_offset] =
                get_chunk_id_and_offset(right_field_, right_data_barrier);
            auto left = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->left_data_type_,
                expr_->left_field_id_,
                left_chunk_id,
                left_data_barrier);
            auto right = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->right_data_type_,
                expr_->right_field_id_,
                right_chunk_id,
                right_data_barrier);
            auto left_opt = left(left_chunk_offset);
            auto right_opt = right(right_chunk_offset);
            if (!left_opt.has_value() || !right_opt.has_value()) {
                res[processed_rows] = false;
                valid_res[processed_rows] = false;
            } else {
                res[processed_rows] = boost::apply_visitor(
                    milvus::query::Relational<decltype(op)>{},
                    left_opt.value(),
                    right_opt.value());
            }
            processed_rows++;
        }
        return res_vec;
    }

    // normal path
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
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    // For segment both fields has no index, can use SIMD to speed up.
    // Avoiding too much call stack that blocks SIMD.
    if (!is_left_indexed_ && !is_right_indexed_ && !IsStringExpr()) {
        result = ExecCompareExprDispatcherForBothDataSegment(input);
        return;
    }
    result = ExecCompareExprDispatcherForHybridSegment(input);
}

VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcherForHybridSegment(
    OffsetVector* input) {
    switch (expr_->op_type_) {
        case OpType::Equal: {
            return ExecCompareExprDispatcher(std::equal_to<>{}, input);
        }
        case OpType::NotEqual: {
            return ExecCompareExprDispatcher(std::not_equal_to<>{}, input);
        }
        case OpType::GreaterEqual: {
            return ExecCompareExprDispatcher(std::greater_equal<>{}, input);
        }
        case OpType::GreaterThan: {
            return ExecCompareExprDispatcher(std::greater<>{}, input);
        }
        case OpType::LessEqual: {
            return ExecCompareExprDispatcher(std::less_equal<>{}, input);
        }
        case OpType::LessThan: {
            return ExecCompareExprDispatcher(std::less<>{}, input);
        }
        case OpType::PrefixMatch: {
            return ExecCompareExprDispatcher(
                milvus::query::MatchOp<OpType::PrefixMatch>{}, input);
        }
            // case OpType::PostfixMatch: {
            // }
        default: {
            PanicInfo(OpTypeInvalid, "unsupported optype: {}", expr_->op_type_);
        }
    }
}

VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcherForBothDataSegment(
    OffsetVector* input) {
    switch (expr_->left_data_type_) {
        case DataType::BOOL:
            return ExecCompareLeftType<bool>(input);
        case DataType::INT8:
            return ExecCompareLeftType<int8_t>(input);
        case DataType::INT16:
            return ExecCompareLeftType<int16_t>(input);
        case DataType::INT32:
            return ExecCompareLeftType<int32_t>(input);
        case DataType::INT64:
            return ExecCompareLeftType<int64_t>(input);
        case DataType::FLOAT:
            return ExecCompareLeftType<float>(input);
        case DataType::DOUBLE:
            return ExecCompareLeftType<double>(input);
        default:
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported left datatype:{} of compare expr",
                            expr_->left_data_type_));
    }
}

template <typename T>
VectorPtr
PhyCompareFilterExpr::ExecCompareLeftType(OffsetVector* input) {
    switch (expr_->right_data_type_) {
        case DataType::BOOL:
            return ExecCompareRightType<T, bool>(input);
        case DataType::INT8:
            return ExecCompareRightType<T, int8_t>(input);
        case DataType::INT16:
            return ExecCompareRightType<T, int16_t>(input);
        case DataType::INT32:
            return ExecCompareRightType<T, int32_t>(input);
        case DataType::INT64:
            return ExecCompareRightType<T, int64_t>(input);
        case DataType::FLOAT:
            return ExecCompareRightType<T, float>(input);
        case DataType::DOUBLE:
            return ExecCompareRightType<T, double>(input);
        default:
            PanicInfo(
                DataTypeInvalid,
                fmt::format("unsupported right datatype:{} of compare expr",
                            expr_->right_data_type_));
    }
}

template <typename T, typename U>
VectorPtr
PhyCompareFilterExpr::ExecCompareRightType(OffsetVector* input) {
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto expr_type = expr_->op_type_;
    auto execute_sub_batch = [expr_type]<FilterType filter_type =
                                             FilterType::sequential>(
        const T* left,
        const U* right,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res) {
        switch (expr_type) {
            case proto::plan::GreaterThan: {
                CompareElementFunc<T, U, proto::plan::GreaterThan, filter_type>
                    func;
                func(left, right, size, res, offsets);
                break;
            }
            case proto::plan::GreaterEqual: {
                CompareElementFunc<T, U, proto::plan::GreaterEqual, filter_type>
                    func;
                func(left, right, size, res, offsets);
                break;
            }
            case proto::plan::LessThan: {
                CompareElementFunc<T, U, proto::plan::LessThan, filter_type>
                    func;
                func(left, right, size, res, offsets);
                break;
            }
            case proto::plan::LessEqual: {
                CompareElementFunc<T, U, proto::plan::LessEqual, filter_type>
                    func;
                func(left, right, size, res, offsets);
                break;
            }
            case proto::plan::Equal: {
                CompareElementFunc<T, U, proto::plan::Equal, filter_type> func;
                func(left, right, size, res, offsets);
                break;
            }
            case proto::plan::NotEqual: {
                CompareElementFunc<T, U, proto::plan::NotEqual, filter_type>
                    func;
                func(left, right, size, res, offsets);
                break;
            }
            default:
                PanicInfo(OpTypeInvalid,
                          fmt::format("unsupported operator type for "
                                      "compare column expr: {}",
                                      expr_type));
        }
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessBothDataByOffsets<T, U>(
            execute_sub_batch, input, res, valid_res);
    } else {
        processed_size = ProcessBothDataChunks<T, U>(
            execute_sub_batch, input, res, valid_res);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
};

}  //namespace exec
}  // namespace milvus
