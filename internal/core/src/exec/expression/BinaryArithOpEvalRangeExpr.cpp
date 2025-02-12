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
    switch (expr_->column_.data_type_) {
        case DataType::BOOL: {
            result = ExecRangeVisitorImpl<bool>();
            break;
        }
        case DataType::INT8: {
            result = ExecRangeVisitorImpl<int8_t>();
            break;
        }
        case DataType::INT16: {
            result = ExecRangeVisitorImpl<int16_t>();
            break;
        }
        case DataType::INT32: {
            result = ExecRangeVisitorImpl<int32_t>();
            break;
        }
        case DataType::INT64: {
            result = ExecRangeVisitorImpl<int64_t>();
            break;
        }
        case DataType::FLOAT: {
            result = ExecRangeVisitorImpl<float>();
            break;
        }
        case DataType::DOUBLE: {
            result = ExecRangeVisitorImpl<double>();
            break;
        }
        case DataType::JSON: {
            auto value_type = expr_->value_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kBoolVal: {
                    result = ExecRangeVisitorImplForJson<bool>();
                    break;
                }
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    result = ExecRangeVisitorImplForJson<int64_t>();
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    result = ExecRangeVisitorImplForJson<double>();
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
                    result = ExecRangeVisitorImplForArray<int64_t>();
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<double>();
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
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForJson() {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto value = GetValueFromProto<ValueType>(expr_->value_);
    auto right_operand =
        arith_type != proto::plan::ArithOpType::ArrayLength
            ? GetValueFromProto<ValueType>(expr_->right_operand_)
            : ValueType();

#define BinaryArithRangeJSONCompare(cmp)                           \
    do {                                                           \
        for (size_t i = 0; i < size; ++i) {                        \
            auto x = data[i].template at<GetType>(pointer);        \
            if (x.error()) {                                       \
                if constexpr (std::is_same_v<GetType, int64_t>) {  \
                    auto x = data[i].template at<double>(pointer); \
                    res[i] = !x.error() && (cmp);                  \
                    continue;                                      \
                }                                                  \
                res[i] = false;                                    \
                continue;                                          \
            }                                                      \
            res[i] = (cmp);                                        \
        }                                                          \
    } while (false)

#define BinaryArithRangeJSONCompareNotEqual(cmp)                   \
    do {                                                           \
        for (size_t i = 0; i < size; ++i) {                        \
            auto x = data[i].template at<GetType>(pointer);        \
            if (x.error()) {                                       \
                if constexpr (std::is_same_v<GetType, int64_t>) {  \
                    auto x = data[i].template at<double>(pointer); \
                    res[i] = x.error() || (cmp);                   \
                    continue;                                      \
                }                                                  \
                res[i] = true;                                     \
                continue;                                          \
            }                                                      \
            res[i] = (cmp);                                        \
        }                                                          \
    } while (false)

    auto execute_sub_batch = [op_type, arith_type](const milvus::Json* data,
                                                   const int size,
                                                   TargetBitmapView res,
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
                            static_cast<ValueType>(long(x.value()) %
                                                   long(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            int array_length = 0;
                            auto doc = data[i].doc();
                            auto array = doc.at_pointer(pointer).get_array();
                            if (!array.error()) {
                                array_length = array.count_elements();
                            }
                            res[i] = array_length == val;
                        }
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
                            static_cast<ValueType>(long(x.value()) %
                                                   long(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            int array_length = 0;
                            auto doc = data[i].doc();
                            auto array = doc.at_pointer(pointer).get_array();
                            if (!array.error()) {
                                array_length = array.count_elements();
                            }
                            res[i] = array_length != val;
                        }
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
                            static_cast<ValueType>(long(x.value()) %
                                                   long(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            int array_length = 0;
                            auto doc = data[i].doc();
                            auto array = doc.at_pointer(pointer).get_array();
                            if (!array.error()) {
                                array_length = array.count_elements();
                            }
                            res[i] = array_length > val;
                        }
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
                            static_cast<ValueType>(long(x.value()) %
                                                   long(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            int array_length = 0;
                            auto doc = data[i].doc();
                            auto array = doc.at_pointer(pointer).get_array();
                            if (!array.error()) {
                                array_length = array.count_elements();
                            }
                            res[i] = array_length >= val;
                        }
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
                            static_cast<ValueType>(long(x.value()) %
                                                   long(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            int array_length = 0;
                            auto doc = data[i].doc();
                            auto array = doc.at_pointer(pointer).get_array();
                            if (!array.error()) {
                                array_length = array.count_elements();
                            }
                            res[i] = array_length < val;
                        }
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
                            static_cast<ValueType>(long(x.value()) %
                                                   long(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            int array_length = 0;
                            auto doc = data[i].doc();
                            auto array = doc.at_pointer(pointer).get_array();
                            if (!array.error()) {
                                array_length = array.count_elements();
                            }
                            res[i] = array_length <= val;
                        }
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
    int64_t processed_size = ProcessDataChunks<milvus::Json>(execute_sub_batch,
                                                             std::nullptr_t{},
                                                             res,
                                                             value,
                                                             right_operand,
                                                             pointer);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForArray() {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

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

#define BinaryArithRangeArrayCompare(cmp)                  \
    do {                                                   \
        for (size_t i = 0; i < size; ++i) {                \
            if (index >= data[i].length()) {               \
                res[i] = false;                            \
                continue;                                  \
            }                                              \
            auto value = data[i].get_data<GetType>(index); \
            res[i] = (cmp);                                \
        }                                                  \
    } while (false)

    auto execute_sub_batch = [op_type, arith_type](const ArrayView* data,
                                                   const int size,
                                                   TargetBitmapView res,
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
                            static_cast<ValueType>(long(value) %
                                                   long(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            res[i] = data[i].length() == val;
                        }
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
                            static_cast<ValueType>(long(value) %
                                                   long(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            res[i] = data[i].length() != val;
                        }
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
                            static_cast<ValueType>(long(value) %
                                                   long(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            res[i] = data[i].length() > val;
                        }
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
                            static_cast<ValueType>(long(value) %
                                                   long(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            res[i] = data[i].length() >= val;
                        }
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
                            static_cast<ValueType>(long(value) %
                                                   long(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            res[i] = data[i].length() < val;
                        }
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
                            static_cast<ValueType>(long(value) %
                                                   long(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        for (size_t i = 0; i < size; ++i) {
                            res[i] = data[i].length() <= val;
                        }
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

    int64_t processed_size = ProcessDataChunks<milvus::ArrayView>(
        execute_sub_batch, std::nullptr_t{}, res, value, right_operand, index);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImpl() {
    if (is_index_mode_ && IndexHasRawData<T>()) {
        return ExecRangeVisitorImplForIndex<T>();
    } else {
        return ExecRangeVisitorImplForData<T>();
    }
}

template <typename T>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForIndex() {
    using Index = index::ScalarIndex<T>;
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisionType;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto value = GetValueFromProto<HighPrecisionType>(expr_->value_);
    auto right_operand =
        GetValueFromProto<HighPrecisionType>(expr_->right_operand_);
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto sub_batch_size = size_per_chunk_;

    auto execute_sub_batch = [op_type, arith_type, sub_batch_size](
                                 Index* index_ptr,
                                 HighPrecisionType value,
                                 HighPrecisionType right_operand) {
        TargetBitmap res;
        switch (op_type) {
            case proto::plan::OpType::Equal: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Add>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Sub>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Mul>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Div>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Mod>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
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
                                         proto::plan::ArithOpType::Add>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Sub>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Mul>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Div>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Mod>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
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
                                         proto::plan::ArithOpType::Add>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Sub>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Mul>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Div>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Mod>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
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
                                         proto::plan::ArithOpType::Add>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Sub>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Mul>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Div>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Mod>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
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
                                         proto::plan::ArithOpType::Add>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Sub>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Mul>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Div>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Mod>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
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
                                         proto::plan::ArithOpType::Add>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Sub>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Mul>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Div>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Mod>
                            func;
                        res = std::move(func(
                            index_ptr, sub_batch_size, value, right_operand));
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
    auto res = ProcessIndexChunks<T>(execute_sub_batch, value, right_operand);
    AssertInfo(res.size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res.size(),
               real_batch_size);
    return std::make_shared<ColumnVector>(std::move(res));
}

template <typename T>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForData() {
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisionType;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto value = GetValueFromProto<HighPrecisionType>(expr_->value_);
    auto right_operand =
        GetValueFromProto<HighPrecisionType>(expr_->right_operand_);
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto execute_sub_batch = [op_type, arith_type](
                                 const T* data,
                                 const int size,
                                 TargetBitmapView res,
                                 HighPrecisionType value,
                                 HighPrecisionType right_operand) {
        switch (op_type) {
            case proto::plan::OpType::Equal: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Add>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Sub>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Mul>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Div>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::Equal,
                                           proto::plan::ArithOpType::Mod>
                            func;
                        func(data, size, value, right_operand, res);
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
                                           proto::plan::ArithOpType::Add>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Sub>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Mul>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Div>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::NotEqual,
                                           proto::plan::ArithOpType::Mod>
                            func;
                        func(data, size, value, right_operand, res);
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
                                           proto::plan::ArithOpType::Add>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Sub>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Mul>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Div>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterThan,
                                           proto::plan::ArithOpType::Mod>
                            func;
                        func(data, size, value, right_operand, res);
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
                                           proto::plan::ArithOpType::Add>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Sub>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Mul>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Div>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::GreaterEqual,
                                           proto::plan::ArithOpType::Mod>
                            func;
                        func(data, size, value, right_operand, res);
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
                                           proto::plan::ArithOpType::Add>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Sub>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Mul>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Div>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessThan,
                                           proto::plan::ArithOpType::Mod>
                            func;
                        func(data, size, value, right_operand, res);
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
                                           proto::plan::ArithOpType::Add>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Sub>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Mul>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Div>
                            func;
                        func(data, size, value, right_operand, res);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        ArithOpElementFunc<T,
                                           proto::plan::OpType::LessEqual,
                                           proto::plan::ArithOpType::Mod>
                            func;
                        func(data, size, value, right_operand, res);
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
    int64_t processed_size = ProcessDataChunks<T>(
        execute_sub_batch, std::nullptr_t{}, res, value, right_operand);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

}  //namespace exec
}  // namespace milvus
