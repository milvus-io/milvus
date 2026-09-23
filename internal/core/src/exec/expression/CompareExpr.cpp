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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "boost/variant/detail/apply_visitor_binary.hpp"
#include "common/Tracer.h"
#include "fmt/core.h"
#include "folly/FBVector.h"
#include "opentelemetry/trace/span.h"
#include "query/Relational.h"

namespace milvus {
namespace exec {

bool
PhyCompareFilterExpr::GatherValues(
    const PinnedValueLookup& source,
    DataType data_type,
    const int64_t* offsets,
    int64_t count,
    std::vector<segcore::data_access_type>& values) const {
    if (!source.Covers(offsets, count)) {
        return false;
    }

    values.assign(static_cast<size_t>(count), std::nullopt);
    std::vector<uint8_t> seen(static_cast<size_t>(count), 0);
    const auto gather = [&]<typename T>() {
        const auto* selected = source.Reader<T>();
        AssertInfo(selected != nullptr,
                   "cached value reader does not match data type {}",
                   data_type);
        selected->Gather(
            offsets,
            count,
            [&](int64_t i, const T* value, bool valid) {
                AssertInfo(i >= 0 && i < count,
                           "value gather output {} exceeds batch size {}",
                           i,
                           count);
                const auto pos = static_cast<size_t>(i);
                AssertInfo(seen[pos] == 0,
                           "value gather produced output {} twice",
                           i);
                seen[pos] = 1;
                if (!valid) {
                    return;
                }
                AssertInfo(value != nullptr,
                           "value reader returned a null value for row {}",
                           offsets[pos]);
                if constexpr (std::is_same_v<T, std::string_view>) {
                    values[pos] = std::string(value->data(), value->size());
                } else {
                    values[pos] = *value;
                }
            });
    };
    switch (data_type) {
        case DataType::BOOL:
            gather.template operator()<bool>();
            break;
        case DataType::INT8:
            gather.template operator()<int8_t>();
            break;
        case DataType::INT16:
            gather.template operator()<int16_t>();
            break;
        case DataType::INT32:
            gather.template operator()<int32_t>();
            break;
        case DataType::INT64:
        // #52689: TIMESTAMPTZ is stored and indexed as int64.
        case DataType::TIMESTAMPTZ:
            gather.template operator()<int64_t>();
            break;
        case DataType::FLOAT:
            gather.template operator()<float>();
            break;
        case DataType::DOUBLE:
            gather.template operator()<double>();
            break;
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            gather.template operator()<std::string_view>();
            break;
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported value lookup data type {}",
                      data_type);
    }
    for (int64_t i = 0; i < count; ++i) {
        AssertInfo(seen[static_cast<size_t>(i)] != 0,
                   "value gather omitted row {}",
                   offsets[i]);
    }
    return true;
}

bool
PhyCompareFilterExpr::CanUseBothDataCompare() {
    if (can_use_both_data_compare_.has_value()) {
        return *can_use_both_data_compare_;
    }

    can_use_both_data_compare_ = [&]() {
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
        const auto can_compare_string_type = [](DataType data_type) {
            return data_type == DataType::VARCHAR ||
                   data_type == DataType::STRING;
        };
        if (IsStringDataType(expr_->left_data_type_) ||
            IsStringDataType(expr_->right_data_type_)) {
            if (!can_compare_string_type(expr_->left_data_type_) ||
                !can_compare_string_type(expr_->right_data_type_)) {
                return false;
            }
            return CaptureDataColumn(left_field_) != nullptr &&
                   CaptureDataColumn(right_field_) != nullptr;
        }
        if (expr_->op_type_ == OpType::PrefixMatch) {
            return false;
        }
        return true;
    }();
    return *can_use_both_data_compare_;
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
    if (is_left_indexed_ || is_right_indexed_) {
        return ExecCompareWithValueLookup(op, context);
    }

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
            segment_chunk_reader_.NumChunkData(expr_->left_field_id_);
        auto right_raw_data_chunk_count =
            segment_chunk_reader_.NumChunkData(expr_->right_field_id_);

        int64_t processed_rows = 0;
        const auto size_per_chunk = segment_chunk_reader_.SizePerChunk();
        auto get_chunk_id_and_offset =
            [&](const FieldId field,
                const int64_t raw_data_chunk_count,
                int64_t offset) -> std::pair<int64_t, int64_t> {
            if (segment_chunk_reader_.segment_->type() ==
                SegmentType::Growing) {
                return {offset / size_per_chunk, offset % size_per_chunk};
            } else if (segment_chunk_reader_.segment_->is_chunked() &&
                       raw_data_chunk_count > 0) {
                return segment_chunk_reader_.GetChunkByOffset(field, offset);
            } else {
                return {0, offset};
            }
        };
        // Consecutive offsets frequently fall in the same left/right chunk;
        // keep each column's data accessor (which pins its chunk) across
        // iterations and rebuild it only when that column's chunk id changes.
        // Safe on both sealed and growing (data and the chunked validity
        // storage have stable per-chunk buffers).
        int64_t cached_left_chunk_id = -1;
        int64_t cached_right_chunk_id = -1;
        segcore::ChunkDataAccessor left;
        segcore::ChunkDataAccessor right;
        // Finite offset input uses one Take per sealed string column,
        // preserving order, duplicates and nulls, instead of one Cell pin
        // and view per row.
        segcore::ChunkDataAccessor left_by_offsets;
        segcore::ChunkDataAccessor right_by_offsets;
        if (segment_chunk_reader_.segment_->type() == SegmentType::Sealed) {
            auto offsets = OffsetView::From(input->data(), real_batch_size);
            if (expr_->left_data_type_ == DataType::VARCHAR) {
                left_by_offsets =
                    segment_chunk_reader_.GetStringDataAccessorByOffsets(
                        left_field_, offsets);
            }
            if (expr_->right_data_type_ == DataType::VARCHAR) {
                right_by_offsets =
                    segment_chunk_reader_.GetStringDataAccessorByOffsets(
                        right_field_, offsets);
            }
        }
        for (auto i = 0; i < real_batch_size; ++i) {
            auto offset = (*input)[i];
            auto read = [&](FieldId field,
                            DataType type,
                            int64_t raw_chunk_count,
                            const segcore::ChunkDataAccessor& by_offsets,
                            int64_t& cached_chunk_id,
                            segcore::ChunkDataAccessor& accessor) {
                if (by_offsets) {
                    return by_offsets(i);
                }
                auto [chunk_id, chunk_offset] =
                    get_chunk_id_and_offset(field, raw_chunk_count, offset);
                if (chunk_id != cached_chunk_id) {
                    accessor = segment_chunk_reader_.GetChunkDataAccessor(
                        type, field, chunk_id);
                    cached_chunk_id = chunk_id;
                }
                return accessor(chunk_offset);
            };
            auto left_opt = read(left_field_,
                                 expr_->left_data_type_,
                                 left_raw_data_chunk_count,
                                 left_by_offsets,
                                 cached_left_chunk_id,
                                 left);
            auto right_opt = read(right_field_,
                                  expr_->right_data_type_,
                                  right_raw_data_chunk_count,
                                  right_by_offsets,
                                  cached_right_chunk_id,
                                  right);
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
            real_batch_size,
            &left_string_scan_state_);
        auto right = segment_chunk_reader_.GetMultipleChunkDataAccessor(
            expr_->right_data_type_,
            expr_->right_field_id_,
            right_current_chunk_id_,
            right_current_chunk_pos_,
            real_batch_size,
            &right_string_scan_state_);
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
                chunk_id);
            auto right = segment_chunk_reader_.GetChunkDataAccessor(
                expr_->right_data_type_,
                expr_->right_field_id_,
                chunk_id);

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

template <typename OpType>
VectorPtr
PhyCompareFilterExpr::ExecCompareWithValueLookup(OpType op,
                                                 EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto real_batch_size =
        input != nullptr ? static_cast<int64_t>(input->size())
                         : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    std::vector<int64_t> offsets(static_cast<size_t>(real_batch_size));
    if (input != nullptr) {
        for (int64_t i = 0; i < real_batch_size; ++i) {
            offsets[static_cast<size_t>(i)] = (*input)[i];
        }
    } else {
        const auto row_begin = GetCurrentRows();
        for (int64_t i = 0; i < real_batch_size; ++i) {
            offsets[static_cast<size_t>(i)] = row_begin + i;
        }
    }

    std::vector<segcore::data_access_type> left_values;
    std::vector<segcore::data_access_type> right_values;
    const bool left_gathered = GatherValues(left_value_lookup_,
                                            expr_->left_data_type_,
                                            offsets.data(),
                                            real_batch_size,
                                            left_values);
    const bool right_gathered = GatherValues(right_value_lookup_,
                                             expr_->right_data_type_,
                                             offsets.data(),
                                             real_batch_size,
                                             right_values);

    const auto left_raw_chunk_count =
        segment_chunk_reader_.NumChunkData(left_field_);
    const auto right_raw_chunk_count =
        segment_chunk_reader_.NumChunkData(right_field_);
    int64_t cached_left_chunk_id = -1;
    int64_t cached_right_chunk_id = -1;
    segcore::ChunkDataAccessor left_accessor;
    segcore::ChunkDataAccessor right_accessor;
    const auto read_raw = [&](FieldId field_id,
                              DataType data_type,
                              int64_t raw_chunk_count,
                              int64_t offset,
                              int64_t& cached_chunk_id,
                              segcore::ChunkDataAccessor& accessor)
        -> segcore::data_access_type {
        const auto [chunk_id, chunk_offset] = [&]() {
            if (segment_chunk_reader_.segment_->type() ==
                SegmentType::Growing) {
                const auto chunk_size = segment_chunk_reader_.SizePerChunk();
                return std::pair{offset / chunk_size, offset % chunk_size};
            }
            if (segment_chunk_reader_.segment_->is_chunked() &&
                raw_chunk_count > 0) {
                return segment_chunk_reader_.GetChunkByOffset(field_id, offset);
            }
            return std::pair<int64_t, int64_t>{0, offset};
        }();
        if (chunk_id != cached_chunk_id) {
            accessor = segment_chunk_reader_.GetChunkDataAccessor(
                data_type, field_id, chunk_id);
            cached_chunk_id = chunk_id;
        }
        return accessor(chunk_offset);
    };

    auto result =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView bits(result->GetRawData(), real_batch_size);
    TargetBitmapView valid(result->GetValidRawData(), real_batch_size);
    for (int64_t i = 0; i < real_batch_size; ++i) {
        const auto offset = offsets[static_cast<size_t>(i)];
        segcore::data_access_type left_raw;
        segcore::data_access_type right_raw;
        const segcore::data_access_type* left = nullptr;
        const segcore::data_access_type* right = nullptr;
        if (left_gathered) {
            left = &left_values[static_cast<size_t>(i)];
        } else {
            left_raw = read_raw(left_field_,
                                expr_->left_data_type_,
                                left_raw_chunk_count,
                                offset,
                                cached_left_chunk_id,
                                left_accessor);
            left = &left_raw;
        }
        if (right_gathered) {
            right = &right_values[static_cast<size_t>(i)];
        } else {
            right_raw = read_raw(right_field_,
                                 expr_->right_data_type_,
                                 right_raw_chunk_count,
                                 offset,
                                 cached_right_chunk_id,
                                 right_accessor);
            right = &right_raw;
        }
        if (!left->has_value() || !right->has_value()) {
            bits[static_cast<size_t>(i)] = false;
            valid[static_cast<size_t>(i)] = false;
            continue;
        }
        bits[static_cast<size_t>(i)] = boost::apply_visitor(
            milvus::query::Relational<decltype(op)>{},
            left->value(),
            right->value());
    }
    // The sequential cursor (current_data_global_pos_) is advanced once per
    // Eval by the caller, so this path must not move it again.
    return result;
}

void
PhyCompareFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span(
        "PhyCompareFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.SetAttribute("op_type", static_cast<int>(expr_->op_type_));
    span.SetAttribute("left_indexed", is_left_indexed_);
    span.SetAttribute("right_indexed", is_right_indexed_);

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    const auto sequential_batch_size =
        has_offset_input_ ? 0 : GetNextBatchSize();
    // An index without raw row values cannot serve this field-field compare;
    // keep using the Column path when both operands still come from data.
    // For segments where both fields have no index, this also lets SIMD run
    // without a deep call stack in between.
    if (!is_left_indexed_ && !is_right_indexed_ && CanUseBothDataCompare()) {
        result = ExecCompareExprDispatcherForBothDataSegment(context);
        current_data_global_pos_ += sequential_batch_size;
        return;
    }
    result = ExecCompareExprDispatcherForHybridSegment(context);
    current_data_global_pos_ += sequential_batch_size;
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
            ThrowInfo(
                UnexpectedError, "unsupported optype: {}", expr_->op_type_);
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
        case DataType::TIMESTAMPTZ:
            return ExecCompareLeftType<int64_t>(context);
        case DataType::FLOAT:
            return ExecCompareLeftType<float>(context);
        case DataType::DOUBLE:
            return ExecCompareLeftType<double>(context);
        case DataType::STRING:
        case DataType::VARCHAR:
            return ExecCompareLeftType<std::string_view>(context);
        default:
            ThrowInfo(
                UnexpectedError,
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
        case DataType::TIMESTAMPTZ:
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
        case DataType::STRING:
        case DataType::VARCHAR:
            if constexpr (IsCompareStringViewType<T>) {
                return ExecCompareRightType<T, std::string_view>(context);
            }
            break;
        default:
            ThrowInfo(
                UnexpectedError,
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
                ThrowInfo(UnexpectedError,
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
        processed_size = TryProcessBothDataByScan<T, U>(execute_sub_batch,
                                                        real_batch_size,
                                                        res,
                                                        valid_res,
                                                        processed_cursor);
        if (processed_size < 0) {
            if constexpr (IsCompareStringViewType<T> ||
                          IsCompareStringViewType<U>) {
                ThrowInfo(
                    UnexpectedError,
                    "sealed string compare Column Scan is unavailable for "
                    "fields {} and {}",
                    left_field_.get(),
                    right_field_.get());
            } else {
                AssertInfo(
                    segment_chunk_reader_.segment_->type() ==
                        SegmentType::Growing,
                    "sealed compare Column Scan is unavailable for fields {} "
                    "and {}",
                    left_field_.get(),
                    right_field_.get());
                processed_size = ProcessBothDataChunks<T, U>(
                    execute_sub_batch, res, valid_res);
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
