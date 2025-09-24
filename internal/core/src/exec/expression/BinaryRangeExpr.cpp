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

#include "BinaryRangeExpr.h"
#include <utility>

#include "query/Utils.h"
#include "index/json_stats/JsonKeyStats.h"
namespace milvus {
namespace exec {

void
PhyBinaryRangeFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    switch (expr_->column_.data_type_) {
        case DataType::BOOL: {
            result = ExecRangeVisitorImpl<bool>(context);
            break;
        }
        case DataType::INT8: {
            result = ExecRangeVisitorImpl<int8_t>(context);
            break;
        }
        case DataType::INT16: {
            result = ExecRangeVisitorImpl<int16_t>(context);
            break;
        }
        case DataType::INT32: {
            result = ExecRangeVisitorImpl<int32_t>(context);
            break;
        }
        case DataType::INT64: {
            result = ExecRangeVisitorImpl<int64_t>(context);
            break;
        }
        case DataType::FLOAT: {
            result = ExecRangeVisitorImpl<float>(context);
            break;
        }
        case DataType::DOUBLE: {
            result = ExecRangeVisitorImpl<double>(context);
            break;
        }
        case DataType::VARCHAR: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                result = ExecRangeVisitorImpl<std::string>(context);
            } else {
                result = ExecRangeVisitorImpl<std::string_view>(context);
            }
            break;
        }
        case DataType::JSON: {
            auto value_type = expr_->lower_val_.val_case();
            if (SegmentExpr::CanUseIndex() && !has_offset_input_) {
                switch (value_type) {
                    case proto::plan::GenericValue::ValCase::kInt64Val: {
                        proto::plan::GenericValue double_lower_val;
                        double_lower_val.set_float_val(
                            static_cast<double>(expr_->lower_val_.int64_val()));
                        proto::plan::GenericValue double_upper_val;
                        double_upper_val.set_float_val(
                            static_cast<double>(expr_->upper_val_.int64_val()));

                        lower_arg_.SetValue<double>(double_lower_val);
                        upper_arg_.SetValue<double>(double_upper_val);
                        arg_inited_ = true;

                        result = ExecRangeVisitorImplForIndex<double>();
                        break;
                    }
                    case proto::plan::GenericValue::ValCase::kFloatVal: {
                        result = ExecRangeVisitorImplForIndex<double>();
                        break;
                    }
                    case proto::plan::GenericValue::ValCase::kStringVal: {
                        result =
                            ExecRangeVisitorImplForJson<std::string>(context);
                        break;
                    }
                    default: {
                        ThrowInfo(DataTypeInvalid,
                                  fmt::format(
                                      "unsupported value type {} in expression",
                                      value_type));
                    }
                }
            } else {
                switch (value_type) {
                    case proto::plan::GenericValue::ValCase::kInt64Val: {
                        result = ExecRangeVisitorImplForJson<int64_t>(context);
                        break;
                    }
                    case proto::plan::GenericValue::ValCase::kFloatVal: {
                        result = ExecRangeVisitorImplForJson<double>(context);
                        break;
                    }
                    case proto::plan::GenericValue::ValCase::kStringVal: {
                        result =
                            ExecRangeVisitorImplForJson<std::string>(context);
                        break;
                    }
                    default: {
                        ThrowInfo(DataTypeInvalid,
                                  fmt::format(
                                      "unsupported value type {} in expression",
                                      value_type));
                    }
                }
            }
            break;
        }
        case DataType::ARRAY: {
            auto value_type = expr_->lower_val_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<int64_t>(context);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<double>(context);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kStringVal: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<std::string>(context);
                    break;
                }
                default: {
                    ThrowInfo(
                        DataTypeInvalid,
                        fmt::format("unsupported value type {} in expression",
                                    value_type));
                }
            }
            break;
        }
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImpl(EvalCtx& context) {
    if (SegmentExpr::CanUseIndex() && !has_offset_input_) {
        return ExecRangeVisitorImplForIndex<T>();
    } else {
        return ExecRangeVisitorImplForData<T>(context);
    }
}

template <typename T, typename IndexInnerType, typename HighPrecisionType>
ColumnVectorPtr
PhyBinaryRangeFilterExpr::PreCheckOverflow(HighPrecisionType& val1,
                                           HighPrecisionType& val2,
                                           bool& lower_inclusive,
                                           bool& upper_inclusive,
                                           OffsetVector* input) {
    lower_inclusive = expr_->lower_inclusive_;
    upper_inclusive = expr_->upper_inclusive_;

    if (!arg_inited_) {
        lower_arg_.SetValue<HighPrecisionType>(expr_->lower_val_);
        upper_arg_.SetValue<HighPrecisionType>(expr_->upper_val_);
        arg_inited_ = true;
    }
    val1 = lower_arg_.GetValue<HighPrecisionType>();
    val2 = upper_arg_.GetValue<HighPrecisionType>();
    auto get_next_overflow_batch =
        [this](OffsetVector* input) -> ColumnVectorPtr {
        int64_t batch_size;
        if (input != nullptr) {
            batch_size = input->size();
        } else {
            batch_size = overflow_check_pos_ + batch_size_ >= active_count_
                             ? active_count_ - overflow_check_pos_
                             : batch_size_;
            overflow_check_pos_ += batch_size;
        }
        auto valid_res =
            (input != nullptr)
                ? ProcessChunksForValidByOffsets<T>(SegmentExpr::CanUseIndex(),
                                                    *input)
                : ProcessChunksForValid<T>(SegmentExpr::CanUseIndex());

        auto res_vec = std::make_shared<ColumnVector>(TargetBitmap(batch_size),
                                                      std::move(valid_res));
        return res_vec;
    };

    if constexpr (std::is_integral_v<T> && !std::is_same_v<bool, T>) {
        if (milvus::query::gt_ub<T>(val1)) {
            return get_next_overflow_batch(input);
        } else if (milvus::query::lt_lb<T>(val1)) {
            val1 = std::numeric_limits<T>::min();
            lower_inclusive = true;
        }

        if (milvus::query::gt_ub<T>(val2)) {
            val2 = std::numeric_limits<T>::max();
            upper_inclusive = true;
        } else if (milvus::query::lt_lb<T>(val2)) {
            return get_next_overflow_batch(input);
        }
    }
    return nullptr;
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForIndex() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    typedef std::conditional_t<std::is_integral_v<IndexInnerType> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               IndexInnerType>
        HighPrecisionType;

    HighPrecisionType val1;
    HighPrecisionType val2;
    bool lower_inclusive = false;
    bool upper_inclusive = false;
    if (auto res =
            PreCheckOverflow<T>(val1, val2, lower_inclusive, upper_inclusive)) {
        return res;
    }

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto execute_sub_batch =
        [lower_inclusive, upper_inclusive](
            Index* index_ptr, HighPrecisionType val1, HighPrecisionType val2) {
            BinaryRangeIndexFunc<T> func;
            return std::move(
                func(index_ptr, val1, val2, lower_inclusive, upper_inclusive));
        };
    auto res = ProcessIndexChunks<T>(execute_sub_batch, val1, val2);
    AssertInfo(res->size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res->size(),
               real_batch_size);
    return res;
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForData(EvalCtx& context) {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    typedef std::conditional_t<std::is_integral_v<IndexInnerType> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               IndexInnerType>
        HighPrecisionType;

    const auto& bitmap_input = context.get_bitmap_input();
    auto* input = context.get_offset_input();
    HighPrecisionType val1;
    HighPrecisionType val2;
    bool lower_inclusive = false;
    bool upper_inclusive = false;
    if (auto res = PreCheckOverflow<T>(
            val1, val2, lower_inclusive, upper_inclusive, input)) {
        return res;
    }

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

    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [ lower_inclusive, upper_inclusive, &processed_cursor, &
          bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            HighPrecisionType val1,
            HighPrecisionType val2) {
        if (lower_inclusive && upper_inclusive) {
            BinaryRangeElementFunc<T, true, true, filter_type> func;
            func(val1,
                 val2,
                 data,
                 size,
                 res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        } else if (lower_inclusive && !upper_inclusive) {
            BinaryRangeElementFunc<T, true, false, filter_type> func;
            func(val1,
                 val2,
                 data,
                 size,
                 res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        } else if (!lower_inclusive && upper_inclusive) {
            BinaryRangeElementFunc<T, false, true, filter_type> func;
            func(val1,
                 val2,
                 data,
                 size,
                 res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        } else {
            BinaryRangeElementFunc<T, false, false, filter_type> func;
            func(val1,
                 val2,
                 data,
                 size,
                 res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        }
        // there is a batch operation in BinaryRangeElementFunc,
        // so not divide data again for the reason that it may reduce performance if the null distribution is scattered
        // but to mask res with valid_data after the batch operation.
        if (valid_data != nullptr) {
            for (int i = 0; i < size; i++) {
                auto offset = i;
                if constexpr (filter_type == FilterType::random) {
                    offset = (offsets) ? offsets[i] : i;
                }
                if (!valid_data[offset]) {
                    res[i] = valid_res[i] = false;
                }
            }
        }
        processed_cursor += size;
    };

    auto skip_index_func =
        [val1, val2, lower_inclusive, upper_inclusive](
            const SkipIndex& skip_index, FieldId field_id, int64_t chunk_id) {
            if (lower_inclusive && upper_inclusive) {
                return skip_index.CanSkipBinaryRange<T>(
                    field_id, chunk_id, val1, val2, true, true);
            } else if (lower_inclusive && !upper_inclusive) {
                return skip_index.CanSkipBinaryRange<T>(
                    field_id, chunk_id, val1, val2, true, false);
            } else if (!lower_inclusive && upper_inclusive) {
                return skip_index.CanSkipBinaryRange<T>(
                    field_id, chunk_id, val1, val2, false, true);
            } else {
                return skip_index.CanSkipBinaryRange<T>(
                    field_id, chunk_id, val1, val2, false, false);
            }
        };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<T>(execute_sub_batch,
                                                 skip_index_func,
                                                 input,
                                                 res,
                                                 valid_res,
                                                 val1,
                                                 val2);
    } else {
        processed_size = ProcessDataChunks<T>(
            execute_sub_batch, skip_index_func, res, valid_res, val1, val2);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForJson(EvalCtx& context) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    const auto& bitmap_input = context.get_bitmap_input();
    auto* input = context.get_offset_input();
    FieldId field_id = expr_->column_.field_id_;
    if (!has_offset_input_ && CanUseJsonStats(context, field_id)) {
        return ExecRangeVisitorImplForJsonStats<ValueType>();
    }
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

    bool lower_inclusive = expr_->lower_inclusive_;
    bool upper_inclusive = expr_->upper_inclusive_;
    if (!arg_inited_) {
        lower_arg_.SetValue<ValueType>(expr_->lower_val_);
        upper_arg_.SetValue<ValueType>(expr_->upper_val_);
        arg_inited_ = true;
    }
    ValueType val1 = lower_arg_.GetValue<ValueType>();
    ValueType val2 = upper_arg_.GetValue<ValueType>();
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [
            lower_inclusive,
            upper_inclusive,
            pointer,
            &bitmap_input,
            &processed_cursor
        ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            ValueType val1,
            ValueType val2) {
        if (lower_inclusive && upper_inclusive) {
            BinaryRangeElementFuncForJson<ValueType, true, true, filter_type>
                func;
            func(val1,
                 val2,
                 pointer,
                 data,
                 valid_data,
                 size,
                 res,
                 valid_res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        } else if (lower_inclusive && !upper_inclusive) {
            BinaryRangeElementFuncForJson<ValueType, true, false, filter_type>
                func;
            func(val1,
                 val2,
                 pointer,
                 data,
                 valid_data,
                 size,
                 res,
                 valid_res,
                 bitmap_input,
                 processed_cursor,
                 offsets);

        } else if (!lower_inclusive && upper_inclusive) {
            BinaryRangeElementFuncForJson<ValueType, false, true, filter_type>
                func;
            func(val1,
                 val2,
                 pointer,
                 data,
                 valid_data,
                 size,
                 res,
                 valid_res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        } else {
            BinaryRangeElementFuncForJson<ValueType, false, false, filter_type>
                func;
            func(val1,
                 val2,
                 pointer,
                 data,
                 valid_data,
                 size,
                 res,
                 valid_res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        }
        processed_cursor += size;
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<milvus::Json>(execute_sub_batch,
                                                            std::nullptr_t{},
                                                            input,
                                                            res,
                                                            valid_res,
                                                            val1,
                                                            val2);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, val1, val2);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForJsonStats() {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto pointer = milvus::index::JsonPointer(expr_->column_.nested_path_);
    bool lower_inclusive = expr_->lower_inclusive_;
    bool upper_inclusive = expr_->upper_inclusive_;
    ValueType val1 = GetValueFromProto<ValueType>(expr_->lower_val_);
    ValueType val2 = GetValueFromProto<ValueType>(expr_->upper_val_);

    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        pinned_json_stats_ = segment->GetJsonStats(op_ctx_, field_id);
        auto* index = pinned_json_stats_.get();
        Assert(index != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data
        auto try_execute = [&](milvus::index::JSONType json_type,
                               TargetBitmapView& res_view,
                               TargetBitmapView& valid_res_view,
                               auto GetType) {
            auto target_field = index->GetShreddingField(pointer, json_type);
            if (!target_field.empty()) {
                using ColType = decltype(GetType);
                auto shredding_executor =
                    [val1, val2, lower_inclusive, upper_inclusive](
                        const ColType* src,
                        const bool* valid,
                        size_t size,
                        TargetBitmapView res,
                        TargetBitmapView valid_res) {
                        for (size_t i = 0; i < size; ++i) {
                            if (valid != nullptr && !valid[i]) {
                                res[i] = valid_res[i] = false;
                                continue;
                            }
                            if (lower_inclusive && upper_inclusive) {
                                res[i] = src[i] >= val1 && src[i] <= val2;
                            } else if (lower_inclusive && !upper_inclusive) {
                                res[i] = src[i] >= val1 && src[i] < val2;
                            } else if (!lower_inclusive && upper_inclusive) {
                                res[i] = src[i] > val1 && src[i] <= val2;
                            } else {
                                res[i] = src[i] > val1 && src[i] < val2;
                            }
                        }
                    };
                index->ExecutorForShreddingData<ColType>(op_ctx_,
                                                         target_field,
                                                         shredding_executor,
                                                         nullptr,
                                                         res_view,
                                                         valid_res_view);
                LOG_DEBUG("using shredding data's field: {} count {}",
                          target_field,
                          res_view.count());
            }
        };

        if constexpr (std::is_same_v<GetType, int64_t>) {
            // int64 compare
            try_execute(milvus::index::JSONType::INT64,
                        res_view,
                        valid_res_view,
                        int64_t{});
            // and double compare
            TargetBitmap res_double(active_count_, false);
            TargetBitmapView res_double_view(res_double);
            TargetBitmap res_double_valid(active_count_, true);
            TargetBitmapView valid_res_double_view(res_double_valid);
            try_execute(milvus::index::JSONType::DOUBLE,
                        res_double_view,
                        valid_res_double_view,
                        double{});
            res_view.inplace_or_with_count(res_double_view, active_count_);
            valid_res_view.inplace_or_with_count(valid_res_double_view,
                                                 active_count_);

        } else if constexpr (std::is_same_v<GetType, double>) {
            try_execute(milvus::index::JSONType::DOUBLE,
                        res_view,
                        valid_res_view,
                        double{});
            // and int64 compare
            TargetBitmap res_int64(active_count_, false);
            TargetBitmapView res_int64_view(res_int64);
            TargetBitmap res_int64_valid(active_count_, true);
            TargetBitmapView valid_res_int64_view(res_int64_valid);
            try_execute(milvus::index::JSONType::INT64,
                        res_int64_view,
                        valid_res_int64_view,
                        int64_t{});
            res_view.inplace_or_with_count(res_int64_view, active_count_);
            valid_res_view.inplace_or_with_count(valid_res_int64_view,
                                                 active_count_);
        } else if constexpr (std::is_same_v<GetType, std::string_view> ||
                             std::is_same_v<GetType, std::string>) {
            try_execute(milvus::index::JSONType::STRING,
                        res_view,
                        valid_res_view,
                        std::string_view{});
        }

        // process shared data
        auto shared_executor =
            [val1, val2, lower_inclusive, upper_inclusive, &res_view](
                milvus::BsonView bson, uint32_t row_id, uint32_t value_offset) {
                if constexpr (std::is_same_v<GetType, int64_t> ||
                              std::is_same_v<GetType, double>) {
                    auto val = bson.ParseAsValueAtOffset<double>(value_offset);
                    if (!val.has_value()) {
                        res_view[row_id] = false;
                        return;
                    }
                    if (lower_inclusive && upper_inclusive) {
                        res_view[row_id] =
                            val.value() >= val1 && val.value() <= val2;
                    } else if (lower_inclusive && !upper_inclusive) {
                        res_view[row_id] =
                            val.value() >= val1 && val.value() < val2;
                    } else if (!lower_inclusive && upper_inclusive) {
                        res_view[row_id] =
                            val.value() > val1 && val.value() <= val2;
                    } else {
                        res_view[row_id] =
                            val.value() > val1 && val.value() < val2;
                    }
                } else {
                    auto val = bson.ParseAsValueAtOffset<GetType>(value_offset);
                    if (!val.has_value()) {
                        res_view[row_id] = false;
                        return;
                    }
                    if (lower_inclusive && upper_inclusive) {
                        res_view[row_id] =
                            val.value() >= val1 && val.value() <= val2;
                    } else if (lower_inclusive && !upper_inclusive) {
                        res_view[row_id] =
                            val.value() >= val1 && val.value() < val2;
                    } else if (!lower_inclusive && upper_inclusive) {
                        res_view[row_id] =
                            val.value() > val1 && val.value() <= val2;
                    } else {
                        res_view[row_id] =
                            val.value() > val1 && val.value() < val2;
                    }
                }
            };
        if (!index->CanSkipShared(pointer)) {
            index->ExecuteForSharedData(op_ctx_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
    }

    TargetBitmap result;
    result.append(
        *cached_index_chunk_res_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}  // namespace exec

template <typename ValueType>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForArray(EvalCtx& context) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    const auto& bitmap_input = context.get_bitmap_input();
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

    bool lower_inclusive = expr_->lower_inclusive_;
    bool upper_inclusive = expr_->upper_inclusive_;

    if (!arg_inited_) {
        lower_arg_.SetValue<ValueType>(expr_->lower_val_);
        upper_arg_.SetValue<ValueType>(expr_->upper_val_);
        arg_inited_ = true;
    }
    ValueType val1 = lower_arg_.GetValue<ValueType>();
    ValueType val2 = upper_arg_.GetValue<ValueType>();

    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }

    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [ lower_inclusive, upper_inclusive, &processed_cursor, &
          bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            ValueType val1,
            ValueType val2,
            int index) {
        if (lower_inclusive && upper_inclusive) {
            BinaryRangeElementFuncForArray<ValueType, true, true, filter_type>
                func;
            func(val1,
                 val2,
                 index,
                 data,
                 valid_data,
                 size,
                 res,
                 valid_res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        } else if (lower_inclusive && !upper_inclusive) {
            BinaryRangeElementFuncForArray<ValueType, true, false, filter_type>
                func;
            func(val1,
                 val2,
                 index,
                 data,
                 valid_data,
                 size,
                 res,
                 valid_res,
                 bitmap_input,
                 processed_cursor,
                 offsets);

        } else if (!lower_inclusive && upper_inclusive) {
            BinaryRangeElementFuncForArray<ValueType, false, true, filter_type>
                func;
            func(val1,
                 val2,
                 index,
                 data,
                 valid_data,
                 size,
                 res,
                 valid_res,
                 bitmap_input,
                 processed_cursor,
                 offsets);

        } else {
            BinaryRangeElementFuncForArray<ValueType, false, false, filter_type>
                func;
            func(val1,
                 val2,
                 index,
                 data,
                 valid_data,
                 size,
                 res,
                 valid_res,
                 bitmap_input,
                 processed_cursor,
                 offsets);
        }
        processed_cursor += size;
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size =
            ProcessDataByOffsets<milvus::ArrayView>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    val1,
                                                    val2,
                                                    index);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(execute_sub_batch,
                                                              std::nullptr_t{},
                                                              res,
                                                              valid_res,
                                                              val1,
                                                              val2,
                                                              index);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

}  // namespace exec
}  // namespace milvus
