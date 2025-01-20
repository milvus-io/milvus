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

#include "BinaryArithOpEvalRangeExpr.h"

namespace milvus {
namespace exec {

void
PhyBinaryArithOpEvalRangeExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    switch (expr_->column_.data_type_) {
        case DataType::BOOL: {
            result = ExecRangeVisitorImpl<bool>(input);
            break;
        }
        case DataType::INT8: {
            result = ExecRangeVisitorImpl<int8_t>(input);
            break;
        }
        case DataType::INT16: {
            result = ExecRangeVisitorImpl<int16_t>(input);
            break;
        }
        case DataType::INT32: {
            result = ExecRangeVisitorImpl<int32_t>(input);
            break;
        }
        case DataType::INT64: {
            result = ExecRangeVisitorImpl<int64_t>(input);
            break;
        }
        case DataType::FLOAT: {
            result = ExecRangeVisitorImpl<float>(input);
            break;
        }
        case DataType::DOUBLE: {
            result = ExecRangeVisitorImpl<double>(input);
            break;
        }
        case DataType::JSON: {
            auto value_type = expr_->value_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kBoolVal: {
                    result = ExecRangeVisitorImplForJson<bool>(input);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    result = ExecRangeVisitorImplForJson<int64_t>(input);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    result = ExecRangeVisitorImplForJson<double>(input);
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
        case DataType::ARRAY: {
            auto value_type = expr_->value_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<int64_t>(input);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<double>(input);
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

template <typename ValueType>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForJson(
    OffsetVector* input) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
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

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto value = GetValueFromProto<ValueType>(expr_->value_);
    auto right_operand =
        arith_type != proto::plan::ArithOpType::ArrayLength
            ? GetValueFromProto<ValueType>(expr_->right_operand_)
            : ValueType();

#define BinaryArithRangeJSONCompare(cmp)                                \
    do {                                                                \
        for (size_t i = 0; i < size; ++i) {                             \
            auto offset = i;                                            \
            if constexpr (filter_type == FilterType::random) {          \
                offset = (offsets) ? offsets[i] : i;                    \
            }                                                           \
            if (valid_data != nullptr && !valid_data[offset]) {         \
                res[i] = false;                                         \
                valid_res[i] = false;                                   \
                continue;                                               \
            }                                                           \
            auto x = data[offset].template at<GetType>(pointer);        \
            if (x.error()) {                                            \
                if constexpr (std::is_same_v<GetType, int64_t>) {       \
                    auto x = data[offset].template at<double>(pointer); \
                    res[i] = !x.error() && (cmp);                       \
                    continue;                                           \
                }                                                       \
                res[i] = false;                                         \
                continue;                                               \
            }                                                           \
            res[i] = (cmp);                                             \
        }                                                               \
    } while (false)

#define BinaryArithRangeJSONCompareNotEqual(cmp)                        \
    do {                                                                \
        for (size_t i = 0; i < size; ++i) {                             \
            auto offset = i;                                            \
            if constexpr (filter_type == FilterType::random) {          \
                offset = (offsets) ? offsets[i] : i;                    \
            }                                                           \
            if (valid_data != nullptr && !valid_data[offset]) {         \
                res[i] = false;                                         \
                valid_res[i] = false;                                   \
                continue;                                               \
            }                                                           \
            auto x = data[offset].template at<GetType>(pointer);        \
            if (x.error()) {                                            \
                if constexpr (std::is_same_v<GetType, int64_t>) {       \
                    auto x = data[offset].template at<double>(pointer); \
                    res[i] = x.error() || (cmp);                        \
                    continue;                                           \
                }                                                       \
                res[i] = true;                                          \
                continue;                                               \
            }                                                           \
            res[i] = (cmp);                                             \
        }                                                               \
    } while (false)

#define BinaryArithRangeJONCompareArrayLength(cmp)              \
    do {                                                        \
        for (size_t i = 0; i < size; ++i) {                     \
            auto offset = i;                                    \
            if constexpr (filter_type == FilterType::random) {  \
                offset = (offsets) ? offsets[i] : i;            \
            }                                                   \
            if (valid_data != nullptr && !valid_data[offset]) { \
                res[i] = false;                                 \
                valid_res[i] = false;                           \
                continue;                                       \
            }                                                   \
            int array_length = 0;                               \
            auto doc = data[offset].doc();                      \
            auto array = doc.at_pointer(pointer).get_array();   \
            if (!array.error()) {                               \
                array_length = array.count_elements();          \
            }                                                   \
            res[i] = (cmp);                                     \
        }                                                       \
    } while (false)

    auto execute_sub_batch =
        [ op_type,
          arith_type ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            ValueType val,
            ValueType right_operand,
            const std::string& pointer) {
        switch (op_type) {
            case proto::plan::OpType::Equal: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(x.value() + right_operand ==
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(x.value() - right_operand ==
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(x.value() * right_operand ==
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(x.value() / right_operand ==
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            static_cast<ValueType>(
                                fmod(x.value(), right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length ==
                                                              val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::NotEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompareNotEqual(
                            x.value() + right_operand != val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompareNotEqual(
                            x.value() - right_operand != val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompareNotEqual(
                            x.value() * right_operand != val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompareNotEqual(
                            x.value() / right_operand != val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompareNotEqual(
                            static_cast<ValueType>(
                                fmod(x.value(), right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length !=
                                                              val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(x.value() + right_operand >
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(x.value() - right_operand >
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(x.value() * right_operand >
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(x.value() / right_operand >
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            static_cast<ValueType>(
                                fmod(x.value(), right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length >
                                                              val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(x.value() + right_operand >=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(x.value() - right_operand >=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(x.value() * right_operand >=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(x.value() / right_operand >=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            static_cast<ValueType>(
                                fmod(x.value(), right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length >=
                                                              val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(x.value() + right_operand <
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(x.value() - right_operand <
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(x.value() * right_operand <
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(x.value() / right_operand <
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            static_cast<ValueType>(
                                fmod(x.value(), right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length <
                                                              val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(x.value() + right_operand <=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(x.value() - right_operand <=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(x.value() * right_operand <=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(x.value() / right_operand <=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            static_cast<ValueType>(
                                fmod(x.value(), right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length <=
                                                              val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            default:
                PanicInfo(OpTypeInvalid,
                          "unsupported operator type for binary "
                          "arithmetic eval expr: {}",
                          op_type);
        }
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<milvus::Json>(execute_sub_batch,
                                                            std::nullptr_t{},
                                                            input,
                                                            res,
                                                            valid_res,
                                                            value,
                                                            right_operand,
                                                            pointer);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(execute_sub_batch,
                                                         std::nullptr_t{},
                                                         res,
                                                         valid_res,
                                                         value,
                                                         right_operand,
                                                         pointer);
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
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForArray(
    OffsetVector* input) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
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

    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto value = GetValueFromProto<ValueType>(expr_->value_);
    auto right_operand =
        arith_type != proto::plan::ArithOpType::ArrayLength
            ? GetValueFromProto<ValueType>(expr_->right_operand_)
            : ValueType();

#define BinaryArithRangeArrayCompare(cmp)                       \
    do {                                                        \
        for (size_t i = 0; i < size; ++i) {                     \
            auto offset = i;                                    \
            if constexpr (filter_type == FilterType::random) {  \
                offset = (offsets) ? offsets[i] : i;            \
            }                                                   \
            if (valid_data != nullptr && !valid_data[offset]) { \
                res[i] = false;                                 \
                valid_res[i] = false;                           \
                continue;                                       \
            }                                                   \
            if (index >= data[offset].length()) {               \
                res[i] = false;                                 \
                continue;                                       \
            }                                                   \
            auto value = data[offset].get_data<GetType>(index); \
            res[i] = (cmp);                                     \
        }                                                       \
    } while (false)

#define BinaryArithRangeArrayLengthCompate(cmp)                 \
    do {                                                        \
        for (size_t i = 0; i < size; ++i) {                     \
            auto offset = i;                                    \
            if constexpr (filter_type == FilterType::random) {  \
                offset = (offsets) ? offsets[i] : i;            \
            }                                                   \
            if (valid_data != nullptr && !valid_data[offset]) { \
                res[i] = valid_res[i] = false;                  \
                continue;                                       \
            }                                                   \
            res[i] = (cmp);                                     \
        }                                                       \
    } while (false)

    auto execute_sub_batch =
        [ op_type,
          arith_type ]<FilterType filter_type = FilterType::sequential>(
            const ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            ValueType val,
            ValueType right_operand,
            int index) {
        switch (op_type) {
            case proto::plan::OpType::Equal: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeArrayCompare(value + right_operand ==
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeArrayCompare(value - right_operand ==
                                                     val);

                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeArrayCompare(value * right_operand ==
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeArrayCompare(value / right_operand ==
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeArrayCompare(
                            static_cast<ValueType>(
                                fmod(value, right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeArrayLengthCompate(
                            data[offset].length() == val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::NotEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeArrayCompare(value + right_operand !=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeArrayCompare(value - right_operand !=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeArrayCompare(value * right_operand !=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeArrayCompare(value / right_operand !=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeArrayCompare(
                            static_cast<ValueType>(
                                fmod(value, right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeArrayLengthCompate(
                            data[offset].length() != val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeArrayCompare(value + right_operand >
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeArrayCompare(value - right_operand >
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeArrayCompare(value * right_operand >
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeArrayCompare(value / right_operand >
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeArrayCompare(
                            static_cast<ValueType>(fmod(value, right_operand)) >
                            val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeArrayLengthCompate(
                            data[offset].length() > val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeArrayCompare(value + right_operand >=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeArrayCompare(value - right_operand >=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeArrayCompare(value * right_operand >=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeArrayCompare(value / right_operand >=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeArrayCompare(
                            static_cast<ValueType>(
                                fmod(value, right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeArrayLengthCompate(
                            data[offset].length() >= val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeArrayCompare(value + right_operand <
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeArrayCompare(value - right_operand <
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeArrayCompare(value * right_operand <
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeArrayCompare(value / right_operand <
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeArrayCompare(
                            static_cast<ValueType>(fmod(value, right_operand)) <
                            val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeArrayLengthCompate(
                            data[offset].length() < val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeArrayCompare(value + right_operand <=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeArrayCompare(value - right_operand <=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeArrayCompare(value * right_operand <=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeArrayCompare(value / right_operand <=
                                                     val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeArrayCompare(
                            static_cast<ValueType>(
                                fmod(value, right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeArrayLengthCompate(
                            data[offset].length() <= val);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            default:
                PanicInfo(OpTypeInvalid,
                          "unsupported operator type for binary "
                          "arithmetic eval expr: {}",
                          op_type);
        }
    };

    int64_t processed_size;
    if (has_offset_input_) {
        processed_size =
            ProcessDataByOffsets<milvus::ArrayView>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    value,
                                                    right_operand,
                                                    index);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(execute_sub_batch,
                                                              std::nullptr_t{},
                                                              res,
                                                              valid_res,
                                                              value,
                                                              right_operand,
                                                              index);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImpl(OffsetVector* input) {
    if (is_index_mode_ && IndexHasRawData<T>()) {
        return ExecRangeVisitorImplForIndex<T>(input);
    } else {
        return ExecRangeVisitorImplForData<T>(input);
    }
}

template <typename T>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForIndex(
    OffsetVector* input) {
    LOG_INFO("debug_for_sample: ExecRangeVisitorImplForIndex");
    using Index = index::ScalarIndex<T>;
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisionType;
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto value = GetValueFromProto<HighPrecisionType>(expr_->value_);
    auto right_operand =
        GetValueFromProto<HighPrecisionType>(expr_->right_operand_);
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto sub_batch_size = has_offset_input_ ? input->size() : size_per_chunk_;

    auto execute_sub_batch =
        [ op_type, arith_type,
          sub_batch_size ]<FilterType filter_type = FilterType::sequential>(
            Index * index_ptr,
            HighPrecisionType value,
            HighPrecisionType right_operand,
            const int32_t* offsets = nullptr) {
        TargetBitmap res;
        switch (op_type) {
            case proto::plan::OpType::Equal: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Add,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Sub,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Mul,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Div,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Mod,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::NotEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Add,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Sub,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Mul,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Div,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Mod,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Add,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Sub,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Mul,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Div,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Mod,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Add,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Sub,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Mul,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Div,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Mod,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Add,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Sub,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Mul,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Div,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Mod,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Add,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Sub,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Mul,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Div,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Mod,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            default:
                PanicInfo(OpTypeInvalid,
                          "unsupported operator type for binary "
                          "arithmetic eval expr: {}",
                          op_type);
        }
        return res;
    };
    if (has_offset_input_) {
        auto res = ProcessIndexChunksByOffsets<T>(
            execute_sub_batch, input, value, right_operand);

        AssertInfo(res->size() == real_batch_size,
                   "internal error: expr processed rows {} not equal "
                   "expect batch size {}",
                   res->size(),
                   real_batch_size);
        return res;
    } else {
        auto res =
            ProcessIndexChunks<T>(execute_sub_batch, value, right_operand);
        AssertInfo(res->size() == real_batch_size,
                   "internal error: expr processed rows {} not equal "
                   "expect batch size {}",
                   res->size(),
                   real_batch_size);
        return res;
    }
}

template <typename T>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForData(
    OffsetVector* input) {
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisionType;
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto value = GetValueFromProto<HighPrecisionType>(expr_->value_);
    auto right_operand =
        GetValueFromProto<HighPrecisionType>(expr_->right_operand_);
    auto res_vec = std::make_shared<ColumnVector>(
        TargetBitmap(real_batch_size), TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    valid_res.set();

    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;

    auto execute_sub_batch =
        [ op_type,
          arith_type ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            HighPrecisionType value,
            HighPrecisionType right_operand) {
        switch (op_type) {
            case proto::plan::OpType::Equal: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Add,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Sub,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Mul,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Div,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Mod,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::NotEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Add,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Sub,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Mul,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Div,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Mod,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Add,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Sub,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Mul,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Div,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Mod,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Add,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Sub,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Mul,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Div,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Mod,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Add,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Sub,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Mul,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Div,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Mod,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Add,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Sub,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Mul,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Div,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Mod,
                                           filter_type>
                            func;
                        func(data, size, value, right_operand, res, offsets);
                        break;
                    }
                    default:
                        PanicInfo(
                            OpTypeInvalid,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            default:
                PanicInfo(OpTypeInvalid,
                          "unsupported operator type for binary "
                          "arithmetic eval expr: {}",
                          op_type);
        }
        // there is a batch operation in ArithOpElementFunc,
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
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<T>(execute_sub_batch,
                                                 std::nullptr_t{},
                                                 input,
                                                 res,
                                                 valid_res,
                                                 value,
                                                 right_operand);
    } else {
        processed_size = ProcessDataChunks<T>(execute_sub_batch,
                                              std::nullptr_t{},
                                              res,
                                              valid_res,
                                              value,
                                              right_operand);
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
