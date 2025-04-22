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
            if (is_index_mode_ && !has_offset_input_) {
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
                        PanicInfo(DataTypeInvalid,
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
                        PanicInfo(DataTypeInvalid,
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
                    PanicInfo(
                        DataTypeInvalid,
                        fmt::format("unsupported value type {} in expression",
                                    value_type));
                }
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImpl(EvalCtx& context) {
    if (is_index_mode_ && !has_offset_input_) {
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
                ? ProcessChunksForValidByOffsets<T>(is_index_mode_, *input)
                : ProcessChunksForValid<T>(is_index_mode_);

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
    auto execute_sub_batch = [lower_inclusive,
                              upper_inclusive,
                              &processed_cursor,
                              &bitmap_input]<FilterType filter_type =
                                                 FilterType::sequential>(
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
    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return ExecRangeVisitorImplForJsonForIndex<ValueType>();
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
    auto execute_sub_batch = [lower_inclusive,
                              upper_inclusive,
                              pointer,
                              &bitmap_input,
                              &processed_cursor]<FilterType filter_type =
                                                     FilterType::sequential>(
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
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForJsonForIndex() {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size = current_data_chunk_pos_ + batch_size_ > active_count_
                               ? active_count_ - current_data_chunk_pos_
                               : batch_size_;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
#define BinaryRangeJSONIndexCompare(cmp)                      \
    do {                                                      \
        auto val = json.at<GetType>(offset, size);            \
        if (val.error()) {                                    \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto val = json.at<double>(offset, size);     \
                return !val.error() && (cmp);                 \
            }                                                 \
            return false;                                     \
        }                                                     \
        return (cmp);                                         \
    } while (false)
#define BinaryRangeJSONTypeCompare(cmp)                                    \
    do {                                                                   \
        if constexpr (std::is_same_v<GetType, std::string_view>) {         \
            if (type == uint8_t(milvus::index::JSONType::STRING)) {        \
                auto val = json.at_string(offset, size);                   \
                return (cmp);                                              \
            } else {                                                       \
                return false;                                              \
            }                                                              \
        } else if constexpr (std::is_same_v<GetType, double>) {            \
            if (type == uint8_t(milvus::index::JSONType::INT64)) {         \
                auto val =                                                 \
                    std::stoll(std::string(json.at_string(offset, size))); \
                return (cmp);                                              \
            } else if (type == uint8_t(milvus::index::JSONType::DOUBLE)) { \
                auto val =                                                 \
                    std::stod(std::string(json.at_string(offset, size)));  \
                return (cmp);                                              \
            } else {                                                       \
                return false;                                              \
            }                                                              \
        } else if constexpr (std::is_same_v<GetType, int64_t>) {           \
            if (type == uint8_t(milvus::index::JSONType::INT64)) {         \
                auto val =                                                 \
                    std::stoll(std::string(json.at_string(offset, size))); \
                return (cmp);                                              \
            } else if (type == uint8_t(milvus::index::JSONType::DOUBLE)) { \
                auto val =                                                 \
                    std::stod(std::string(json.at_string(offset, size)));  \
                return (cmp);                                              \
            } else {                                                       \
                return false;                                              \
            }                                                              \
        }                                                                  \
    } while (false)

#define BinaryRangeJSONTypeCompareWithValue(cmp)                   \
    do {                                                           \
        if constexpr (std::is_same_v<GetType, int64_t>) {          \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) { \
                float val = *reinterpret_cast<float*>(&value);     \
                return (cmp);                                      \
            } else {                                               \
                int64_t val = value;                               \
                return (cmp);                                      \
            }                                                      \
        } else if constexpr (std::is_same_v<GetType, double>) {    \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) { \
                float val = *reinterpret_cast<float*>(&value);     \
                return (cmp);                                      \
            } else {                                               \
                int64_t val = value;                               \
                return (cmp);                                      \
            }                                                      \
        } else if constexpr (std::is_same_v<GetType, bool>) {      \
            bool val = *reinterpret_cast<bool*>(&value);           \
            return (cmp);                                          \
        }                                                          \
    } while (false)
    bool lower_inclusive = expr_->lower_inclusive_;
    bool upper_inclusive = expr_->upper_inclusive_;
    ValueType val1 = GetValueFromProto<ValueType>(expr_->lower_val_);
    ValueType val2 = GetValueFromProto<ValueType>(expr_->upper_val_);
    if (cached_index_chunk_id_ != 0) {
        const segcore::SegmentInternalInterface* segment = nullptr;
        if (segment_->type() == SegmentType::Growing) {
            segment =
                dynamic_cast<const segcore::SegmentGrowingImpl*>(segment_);
        } else if (segment_->type() == SegmentType::Sealed) {
            segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        }
        auto field_id = expr_->column_.field_id_;
        auto* index = segment->GetJsonKeyIndex(field_id);
        Assert(index != nullptr);
        auto filter_func = [segment,
                            &field_id,
                            val1,
                            val2,
                            lower_inclusive,
                            upper_inclusive](bool valid,
                                             uint8_t type,
                                             uint32_t row_id,
                                             uint16_t offset,
                                             uint16_t size,
                                             int32_t value) {
            if (valid) {
                if constexpr (std::is_same_v<GetType, int64_t>) {
                    if (type != uint8_t(milvus::index::JSONType::INT32) &&
                        type != uint8_t(milvus::index::JSONType::INT64) &&
                        type != uint8_t(milvus::index::JSONType::FLOAT) &&
                        type != uint8_t(milvus::index::JSONType::DOUBLE)) {
                        return false;
                    }
                } else if constexpr (std::is_same_v<GetType,
                                                    std::string_view>) {
                    if (type != uint8_t(milvus::index::JSONType::STRING) &&
                        type !=
                            uint8_t(milvus::index::JSONType::STRING_ESCAPE)) {
                        return false;
                    }
                } else if constexpr (std::is_same_v<GetType, double>) {
                    if (type != uint8_t(milvus::index::JSONType::INT32) &&
                        type != uint8_t(milvus::index::JSONType::INT64) &&
                        type != uint8_t(milvus::index::JSONType::FLOAT) &&
                        type != uint8_t(milvus::index::JSONType::DOUBLE)) {
                        return false;
                    }
                } else if constexpr (std::is_same_v<GetType, bool>) {
                    if (type != uint8_t(milvus::index::JSONType::BOOL)) {
                        return false;
                    }
                }
                if (lower_inclusive && upper_inclusive) {
                    if (type == uint8_t(milvus::index::JSONType::FLOAT)) {
                        BinaryRangeJSONTypeCompareWithValue(
                            static_cast<float>(val1) <= val &&
                            val <= static_cast<float>(val2));
                    } else {
                        BinaryRangeJSONTypeCompareWithValue(val1 <= val &&
                                                            val <= val2);
                    }
                } else if (lower_inclusive && !upper_inclusive) {
                    if (type == uint8_t(milvus::index::JSONType::FLOAT)) {
                        BinaryRangeJSONTypeCompareWithValue(
                            static_cast<float>(val1) <= val &&
                            val < static_cast<float>(val2));
                    } else {
                        BinaryRangeJSONTypeCompareWithValue(val1 <= val &&
                                                            val < val2);
                    }
                } else if (!lower_inclusive && upper_inclusive) {
                    if (type == uint8_t(milvus::index::JSONType::FLOAT)) {
                        BinaryRangeJSONTypeCompareWithValue(
                            static_cast<float>(val1) < val &&
                            val <= static_cast<float>(val2));
                    } else {
                        BinaryRangeJSONTypeCompareWithValue(val1 < val &&
                                                            val <= val2);
                    }
                } else {
                    if (type == uint8_t(milvus::index::JSONType::FLOAT)) {
                        BinaryRangeJSONTypeCompareWithValue(
                            static_cast<float>(val1) < val &&
                            val < static_cast<float>(val2));
                    } else {
                        BinaryRangeJSONTypeCompareWithValue(val1 < val &&
                                                            val < val2);
                    }
                }
            } else {
                auto json_pair = segment->GetJsonData(field_id, row_id);
                if (!json_pair.second) {
                    return false;
                }
                auto json = milvus::Json(json_pair.first.data(),
                                         json_pair.first.size());
                if (lower_inclusive && upper_inclusive) {
                    if (type == uint8_t(milvus::index::JSONType::STRING) ||
                        type == uint8_t(milvus::index::JSONType::DOUBLE) ||
                        type == uint8_t(milvus::index::JSONType::INT64)) {
                        BinaryRangeJSONTypeCompare(val1 <= val && val <= val2);
                    } else {
                        BinaryRangeJSONIndexCompare(
                            val1 <= ValueType(val.value()) &&
                            ValueType(val.value()) <= val2);
                    }
                } else if (lower_inclusive && !upper_inclusive) {
                    if (type == uint8_t(milvus::index::JSONType::STRING) ||
                        type == uint8_t(milvus::index::JSONType::DOUBLE) ||
                        type == uint8_t(milvus::index::JSONType::INT64)) {
                        BinaryRangeJSONTypeCompare(val1 <= val && val < val2);
                    } else {
                        BinaryRangeJSONIndexCompare(
                            val1 <= ValueType(val.value()) &&
                            ValueType(val.value()) < val2);
                    }
                } else if (!lower_inclusive && upper_inclusive) {
                    if (type == uint8_t(milvus::index::JSONType::STRING) ||
                        type == uint8_t(milvus::index::JSONType::DOUBLE) ||
                        type == uint8_t(milvus::index::JSONType::INT64)) {
                        BinaryRangeJSONTypeCompare(val1 < val && val <= val2);
                    } else {
                        BinaryRangeJSONIndexCompare(
                            val1 < ValueType(val.value()) &&
                            ValueType(val.value()) <= val2);
                    }
                } else {
                    if (type == uint8_t(milvus::index::JSONType::STRING) ||
                        type == uint8_t(milvus::index::JSONType::DOUBLE) ||
                        type == uint8_t(milvus::index::JSONType::INT64)) {
                        BinaryRangeJSONTypeCompare(val1 < val && val < val2);
                    } else {
                        BinaryRangeJSONIndexCompare(
                            val1 < ValueType(val.value()) &&
                            ValueType(val.value()) < val2);
                    }
                }
            }
        };
        bool is_growing = segment_->type() == SegmentType::Growing;
        bool is_strong_consistency = consistency_level_ == 0;
        cached_index_chunk_res_ = index
                                      ->FilterByPath(pointer,
                                                     active_count_,
                                                     is_growing,
                                                     is_strong_consistency,
                                                     filter_func)
                                      .clone();
        cached_index_chunk_id_ = 0;
    }
    int total_data_chunk_pos_ =  GetProcessedRows();
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, total_data_chunk_pos_, real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

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
    auto execute_sub_batch = [lower_inclusive,
                              upper_inclusive,
                              &processed_cursor,
                              &bitmap_input]<FilterType filter_type =
                                                 FilterType::sequential>(
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

}  //namespace exec
}  // namespace milvus
