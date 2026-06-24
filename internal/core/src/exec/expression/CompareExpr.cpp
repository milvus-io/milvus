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

#include <functional>
#include <optional>

#include "boost/variant/detail/apply_visitor_binary.hpp"
#include "common/Tracer.h"
#include "fmt/core.h"
#include "folly/FBVector.h"
#include "opentelemetry/trace/span.h"
#include "query/Relational.h"

namespace milvus {
namespace exec {

bool
PhyCompareFilterExpr::CanUseBothDataCompare(OffsetVector* input) const {
    const auto is_supported_compare_op = [&]() {
        switch (expr_->op_type_) {
            case OpType::Equal:
            case OpType::NotEqual:
            case OpType::GreaterEqual:
            case OpType::GreaterThan:
            case OpType::LessEqual:
            case OpType::LessThan:
            case OpType::PrefixMatch:
                return true;
            default:
                return false;
        }
    }();
    if (!is_supported_compare_op) {
        return false;
    }

    const auto is_left_string = expr_->left_data_type_ == DataType::VARCHAR ||
                                expr_->left_data_type_ == DataType::TEXT;
    const auto is_right_string = expr_->right_data_type_ == DataType::VARCHAR ||
                                 expr_->right_data_type_ == DataType::TEXT;
    if (is_left_string || is_right_string) {
        if (!is_left_string || !is_right_string) {
            return false;
        }
        if (input != nullptr &&
            !IsDenseOffsetInputForScan(input, batch_size_)) {
            return false;
        }
        return segment_chunk_reader_.segment_->GetChunkedColumn(left_field_) !=
                   nullptr &&
               segment_chunk_reader_.segment_->GetChunkedColumn(right_field_) !=
                   nullptr;
    }

    return expr_->op_type_ != OpType::PrefixMatch;
}

bool
PhyCompareFilterExpr::CanUseBothDataFastPath() {
    if (is_left_indexed_ || is_right_indexed_ ||
        IsStringDataType(expr_->left_data_type_) ||
        IsStringDataType(expr_->right_data_type_)) {
        return false;
    }

    // Offset-input path resolves each field's chunk from the row offset
    // independently, so it does not require left/right chunk boundaries to
    // align.
    if (has_offset_input_) {
        return true;
    }

    if (can_use_both_data_sequential_fast_path_.has_value()) {
        return can_use_both_data_sequential_fast_path_.value();
    }

    auto segment = segment_chunk_reader_.segment_;
    if (!segment->is_chunked() || segment->type() == SegmentType::Growing) {
        can_use_both_data_sequential_fast_path_ = true;
        return true;
    }

    auto left_chunks = segment->num_chunk_data(left_field_);
    auto right_chunks = segment->num_chunk_data(right_field_);
    if (left_chunks <= 0 || right_chunks <= 0 || left_chunks != right_chunks) {
        can_use_both_data_sequential_fast_path_ = false;
        return false;
    }

    for (int64_t i = 0; i <= left_chunks; ++i) {
        if (segment->num_rows_until_chunk(left_field_, i) !=
            segment->num_rows_until_chunk(right_field_, i)) {
            can_use_both_data_sequential_fast_path_ = false;
            return false;
        }
    }
    can_use_both_data_sequential_fast_path_ = true;
    return true;
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
PhyCompareFilterExpr::ExecCompareExprDispatcher(OpType op, EvalCtx& context) {
    // take offsets as input
    auto input = context.get_offset_input();
    if (has_offset_input_) {
        auto real_batch_size = input->size();
        if (real_batch_size == 0) {
            return nullptr;
        }

        auto res_vec =
            std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                           TargetBitmap(real_batch_size, true));
        TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
        TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

        auto left_raw_data_chunk_count =
            segment_chunk_reader_.segment_->num_chunk_data(
                expr_->left_field_id_);
        auto right_raw_data_chunk_count =
            segment_chunk_reader_.segment_->num_chunk_data(
                expr_->right_field_id_);

        int64_t processed_rows = 0;
        const auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
        for (auto i = 0; i < real_batch_size; ++i) {
            auto offset = (*input)[i];
            auto get_chunk_id_and_offset =
                [&](const FieldId field, const int64_t raw_data_chunk_count)
                -> std::pair<int64_t, int64_t> {
                if (segment_chunk_reader_.segment_->type() ==
                    SegmentType::Growing) {
                    return {offset / size_per_chunk, offset % size_per_chunk};
                } else if (segment_chunk_reader_.segment_->is_chunked() &&
                           raw_data_chunk_count > 0) {
                    return segment_chunk_reader_.segment_->get_chunk_by_offset(
                        field, offset);
                } else {
                    return {0, offset};
                }
            };

            auto [left_chunk_id, left_chunk_offset] =
                get_chunk_id_and_offset(left_field_, left_raw_data_chunk_count);
            auto [right_chunk_id, right_chunk_offset] = get_chunk_id_and_offset(
                right_field_, right_raw_data_chunk_count);
            auto left = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->left_data_type_,
                expr_->left_field_id_,
                left_chunk_id,
                LeftPinnedIndexForRawLookup());
            auto right = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->right_data_type_,
                expr_->right_field_id_,
                right_chunk_id,
                RightPinnedIndexForRawLookup());
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

        auto left = segment_chunk_reader_.GetMultipleChunkDataAccessor(
            expr_->left_data_type_,
            expr_->left_field_id_,
            left_current_chunk_id_,
            left_current_chunk_pos_,
            LeftPinnedIndexForRawLookup());
        auto right = segment_chunk_reader_.GetMultipleChunkDataAccessor(
            expr_->right_data_type_,
            expr_->right_field_id_,
            right_current_chunk_id_,
            right_current_chunk_pos_,
            RightPinnedIndexForRawLookup());
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
                LeftPinnedIndexForRawLookup());
            auto right = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->right_data_type_,
                expr_->right_field_id_,
                chunk_id,
                RightPinnedIndexForRawLookup());

            for (int i = chunk_id == current_chunk_id_ ? current_chunk_pos_ : 0;
                 i < chunk_size;
                 ++i) {
                auto left_opt = left(i);
                auto right_opt = right(i);
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
    tracer::AutoSpan span(
        "PhyCompareFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("op_type", static_cast<int>(expr_->op_type_));
    span.GetSpan()->SetAttribute("left_indexed", is_left_indexed_);
    span.GetSpan()->SetAttribute("right_indexed", is_right_indexed_);

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    // For unindexed field-field compare, use the direct data path. Fixed-width
    // values use DataScan when available and fall back to chunk SIMD. String
    // values require Scan-provided string_view batches.
    if (!is_left_indexed_ && !is_right_indexed_ &&
        CanUseBothDataCompare(input)) {
        result = ExecCompareExprDispatcherForBothDataSegment(context);
        return;
    }
    result = ExecCompareExprDispatcherForHybridSegment(context);
}

VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcherForHybridSegment(
    EvalCtx& context) {
    switch (expr_->op_type_) {
        case OpType::Equal: {
            return ExecCompareExprDispatcher(std::equal_to<>{}, context);
        }
        case OpType::NotEqual: {
            return ExecCompareExprDispatcher(std::not_equal_to<>{}, context);
        }
        case OpType::GreaterEqual: {
            return ExecCompareExprDispatcher(std::greater_equal<>{}, context);
        }
        case OpType::GreaterThan: {
            return ExecCompareExprDispatcher(std::greater<>{}, context);
        }
        case OpType::LessEqual: {
            return ExecCompareExprDispatcher(std::less_equal<>{}, context);
        }
        case OpType::LessThan: {
            return ExecCompareExprDispatcher(std::less<>{}, context);
        }
        case OpType::PrefixMatch: {
            return ExecCompareExprDispatcher(
                milvus::query::MatchOp<OpType::PrefixMatch>{}, context);
        }
            // case OpType::PostfixMatch: {
            // }
        default: {
            ThrowInfo(OpTypeInvalid, "unsupported optype: {}", expr_->op_type_);
        }
    }
}

VectorPtr
PhyCompareFilterExpr::ExecCompareExprDispatcherForBothDataSegment(
    EvalCtx& context) {
    switch (expr_->left_data_type_) {
        case DataType::BOOL:
            return ExecCompareLeftType<bool>(context);
        case DataType::INT8:
            return ExecCompareLeftType<int8_t>(context);
        case DataType::INT16:
            return ExecCompareLeftType<int16_t>(context);
        case DataType::INT32:
            return ExecCompareLeftType<int32_t>(context);
        case DataType::INT64:
            return ExecCompareLeftType<int64_t>(context);
        case DataType::FLOAT:
            return ExecCompareLeftType<float>(context);
        case DataType::DOUBLE:
            return ExecCompareLeftType<double>(context);
        case DataType::VARCHAR:
        case DataType::TEXT:
            return ExecCompareLeftType<std::string_view>(context);
        default:
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("unsupported left datatype:{} of compare expr",
                            expr_->left_data_type_));
    }
}

template <typename T>
VectorPtr
PhyCompareFilterExpr::ExecCompareLeftType(EvalCtx& context) {
    const auto right_type = expr_->right_data_type_;
    switch (right_type) {
        case DataType::BOOL:
            if constexpr (!IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, bool>(context);
            }
            break;
        case DataType::INT8:
            if constexpr (!IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, int8_t>(context);
            }
            break;
        case DataType::INT16:
            if constexpr (!IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, int16_t>(context);
            }
            break;
        case DataType::INT32:
            if constexpr (!IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, int32_t>(context);
            }
            break;
        case DataType::INT64:
            if constexpr (!IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, int64_t>(context);
            }
            break;
        case DataType::FLOAT:
            if constexpr (!IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, float>(context);
            }
            break;
        case DataType::DOUBLE:
            if constexpr (!IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, double>(context);
            }
            break;
        case DataType::VARCHAR:
        case DataType::TEXT:
            if constexpr (IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, std::string_view>(context);
            }
            break;
        default:
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("unsupported right datatype:{} of compare expr",
                            right_type));
    }
    ThrowInfo(DataTypeInvalid,
              fmt::format("unsupported right datatype:{} of compare expr",
                          right_type));
}

template <typename T, typename U>
VectorPtr
PhyCompareFilterExpr::ExecCompareRightType(EvalCtx& context) {
    auto input = context.get_offset_input();
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    const auto& bitmap_input = context.get_bitmap_input();
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto expr_type = expr_->op_type_;
    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [ expr_type, &bitmap_input, &
          processed_cursor ]<FilterType filter_type = FilterType::sequential>(
            const T* left,
            const U* right,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res) {
        switch (expr_type) {
            case proto::plan::GreaterThan: {
                CompareElementFunc<T, U, proto::plan::GreaterThan, filter_type>
                    func;
                func(left,
                     right,
                     size,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::GreaterEqual: {
                CompareElementFunc<T, U, proto::plan::GreaterEqual, filter_type>
                    func;
                func(left,
                     right,
                     size,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::LessThan: {
                CompareElementFunc<T, U, proto::plan::LessThan, filter_type>
                    func;
                func(left,
                     right,
                     size,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::LessEqual: {
                CompareElementFunc<T, U, proto::plan::LessEqual, filter_type>
                    func;
                func(left,
                     right,
                     size,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::Equal: {
                CompareElementFunc<T, U, proto::plan::Equal, filter_type> func;
                func(left,
                     right,
                     size,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::NotEqual: {
                CompareElementFunc<T, U, proto::plan::NotEqual, filter_type>
                    func;
                func(left,
                     right,
                     size,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::PrefixMatch: {
                CompareElementFunc<T, U, proto::plan::PrefixMatch, filter_type>
                    func;
                func(left,
                     right,
                     size,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            default:
                ThrowInfo(OpTypeInvalid,
                          fmt::format("unsupported operator type for "
                                      "compare column expr: {}",
                                      expr_type));
        }
        processed_cursor += size;
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessBothDataByOffsets<T, U>(
            execute_sub_batch, input, res, valid_res);
    } else {
        processed_size = TryProcessBothDataByScan<T, U>(
            execute_sub_batch, real_batch_size, res, valid_res);
        if (processed_size < 0) {
            if constexpr (!IsCompareStringViewType<T> &&
                          !IsCompareStringViewType<U>) {
                processed_size = ProcessBothDataChunks<T, U>(
                    execute_sub_batch, input, res, valid_res);
            }
        }
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
