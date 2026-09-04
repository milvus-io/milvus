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

#include <simdjson.h>
#include <cstdint>
#include <string_view>
#include <variant>
#include <vector>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Tracer.h"
#include "common/VectorArray.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Utils.h"
#include "fmt/core.h"
#include "opentelemetry/trace/span.h"

namespace milvus {
class SkipIndex;

namespace exec {

void
PhyBinaryArithOpEvalRangeExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    tracer::AutoSpan span(
        "PhyBinaryArithOpEvalRangeExpr::Eval", tracer::GetRootSpan(), true);
    span.SetAttribute("data_type", static_cast<int>(expr_->column_.data_type_));
    span.SetAttribute("op_type", static_cast<int>(expr_->op_type_));

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    auto data_type = expr_->column_.data_type_;
    const bool is_nested_array =
        data_type == DataType::ARRAY &&
        expr_->column_.element_type_ == DataType::ARRAY;
    if (expr_->column_.element_level_ && !is_nested_array) {
        data_type = expr_->column_.element_type_;
    }
    switch (data_type) {
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
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format("unsupported value type {} in expression",
                                    value_type));
                }
            }
            break;
        }
        case DataType::ARRAY: {
            const auto is_array_length =
                expr_->arith_op_type_ == proto::plan::ArithOpType::ArrayLength;
            if (is_nested_array && !is_array_length) {
                ThrowInfo(UnexpectedError,
                          "unsupported arith type for recursive ARRAY: {}",
                          expr_->arith_op_type_);
            }
            auto value_type = expr_->value_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    if (is_array_length) {
                        if (is_nested_array) {
                            if (expr_->column_.element_level_) {
                                result = ExecArrayLength<ArrayValueView,
                                                         int64_t,
                                                         true>(input);
                            } else {
                                result = ExecArrayLength<ArrayValueView,
                                                         int64_t,
                                                         false>(input);
                            }
                        } else {
                            result = ExecArrayLength<ArrayView, int64_t, false>(
                                input);
                        }
                    } else {
                        result = ExecRangeVisitorImplForArray<int64_t>(input);
                    }
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    if (is_array_length) {
                        if (is_nested_array) {
                            if (expr_->column_.element_level_) {
                                result = ExecArrayLength<ArrayValueView,
                                                         double,
                                                         true>(input);
                            } else {
                                result = ExecArrayLength<ArrayValueView,
                                                         double,
                                                         false>(input);
                            }
                        } else {
                            result = ExecArrayLength<ArrayView, double, false>(
                                input);
                        }
                    } else {
                        result = ExecRangeVisitorImplForArray<double>(input);
                    }
                    break;
                }
                default: {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format("unsupported value type {} in expression",
                                    value_type));
                }
            }
            break;
        }
        case DataType::VECTOR_ARRAY: {
            if (expr_->arith_op_type_ !=
                proto::plan::ArithOpType::ArrayLength) {
                ThrowInfo(UnexpectedError,
                          "unsupported arith type for VECTOR_ARRAY: {}",
                          expr_->arith_op_type_);
            }
            auto value_type = expr_->value_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    result =
                        ExecArrayLength<VectorArrayView, int64_t, false>(input);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    result =
                        ExecArrayLength<VectorArrayView, double, false>(input);
                    break;
                }
                default: {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format("unsupported value type {} in expression",
                                    value_type));
                }
            }
            break;
        }
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

// Applies a single arithmetic op to a JSON/array-extracted value, mirroring
// the per-branch arithmetic already inlined in BinaryArithRangeJSONCompare /
// BinaryArithRangeArrayCompare's cmp expressions (e.g. json_v + operand).
// Used to pre-transform the extracted value through op1 so the *existing*
// switch(arith_type) dispatch below can run unchanged for op2. This is a
// plain runtime switch, matching this file's existing dispatch style for
// the JSON/Array paths (there is no SIMD kernel for per-element simdjson /
// array access to begin with, so nothing is lost relative to today).
template <typename JsonValueType, typename OperandType>
JsonValueType
ApplyJsonArithOp(proto::plan::ArithOpType op,
                 JsonValueType json_v,
                 OperandType right_operand) {
    switch (op) {
        case proto::plan::ArithOpType::Add:
            return json_v + right_operand;
        case proto::plan::ArithOpType::Sub:
            return json_v - right_operand;
        case proto::plan::ArithOpType::Mul:
            return json_v * right_operand;
        case proto::plan::ArithOpType::Div:
            return json_v / right_operand;
        case proto::plan::ArithOpType::Mod:
            return safe_mod(json_v, right_operand);
        case proto::plan::ArithOpType::BitAnd:
            return JsonValueType(int64_t(json_v) & int64_t(right_operand));
        case proto::plan::ArithOpType::BitOr:
            return JsonValueType(int64_t(json_v) | int64_t(right_operand));
        case proto::plan::ArithOpType::BitXor:
            return JsonValueType(int64_t(json_v) ^ int64_t(right_operand));
        case proto::plan::ArithOpType::Shl:
            return JsonValueType(int64_t(json_v) << int64_t(right_operand));
        case proto::plan::ArithOpType::Shr:
            return JsonValueType(int64_t(json_v) >> int64_t(right_operand));
        default:
            ThrowInfo(UnexpectedError,
                      fmt::format("unsupported first arith type for binary "
                                  "arithmetic eval expr: {}",
                                  op));
            return JsonValueType();
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
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    if (!arg_inited_) {
        value_arg_.SetValue<ValueType>(expr_->value_);
        if (expr_->arith_op_type_ == proto::plan::ArithOpType::ArrayLength) {
            right_operand_arg_.SetValue(ValueType());
        } else {
            right_operand_arg_.SetValue<ValueType>(expr_->right_operand_);
        }
        if (expr_->has_second_op()) {
            right_operand2_arg_.SetValue<ValueType>(expr_->right_operand2_);
        }
        arg_inited_ = true;
    }

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto arith_type2 = expr_->arith_op_type2_;
    auto has_second_op = expr_->has_second_op();
    auto value = value_arg_.GetValue<ValueType>();
    auto right_operand = right_operand_arg_.GetValue<ValueType>();
    auto right_operand2 =
        has_second_op ? right_operand2_arg_.GetValue<ValueType>() : ValueType();

    // Validate divisor for division/modulo operations
    if ((arith_type == proto::plan::ArithOpType::Div ||
         arith_type == proto::plan::ArithOpType::Mod) &&
        right_operand == 0) {
        ThrowInfo(
            ErrorCode::ExprInvalid,
            "division or modulus by zero in JSON field arithmetic expression");
    }
    if (has_second_op &&
        (arith_type2 == proto::plan::ArithOpType::Div ||
         arith_type2 == proto::plan::ArithOpType::Mod) &&
        right_operand2 == 0) {
        ThrowInfo(
            ErrorCode::ExprInvalid,
            "division or modulus by zero in JSON field arithmetic expression");
    }

// For int64_t GetType, uses at_numeric() to extract any JSON number in one
// parse.  int64 values preserve precision; uint64/double fall back to double.
// 'cmp' must reference 'json_v' (auto-typed as int64_t or double).
//
// When has_second_op is set, json_v is pre-transformed through op1
// (arith_type1 / right_operand1, captured by the enclosing lambda) right
// after extraction, before 'cmp' (which by then references op2's dispatch
// via the arith_type/right_operand aliasing done at the top of the lambda
// body) runs — see ApplyJsonArithOp above.
#define BinaryArithRangeJSONCompare(cmp)                                       \
    do {                                                                       \
        for (size_t i = 0; i < size; ++i) {                                    \
            auto offset = i;                                                   \
            if constexpr (filter_type == FilterType::random) {                 \
                offset = (offsets) ? offsets[i] : i;                           \
            }                                                                  \
            if (valid_data && !valid_data[offset]) {                           \
                res[i] = false;                                                \
                valid_res[i] = false;                                          \
                continue;                                                      \
            }                                                                  \
            if constexpr (std::is_same_v<GetType, int64_t>) {                  \
                auto x_num = data[offset].at_numeric(pointer);                 \
                if (x_num.error()) {                                           \
                    res[i] = false;                                            \
                    valid_res[i] = false;                                      \
                    continue;                                                  \
                }                                                              \
                auto n = x_num.value();                                        \
                if (n.is_int64()) {                                            \
                    auto json_v = n.get_int64();                               \
                    if (has_second_op) {                                       \
                        json_v = ApplyJsonArithOp(                             \
                            arith_type1, json_v, right_operand1);              \
                    }                                                          \
                    res[i] = (cmp);                                            \
                } else {                                                       \
                    auto json_v = n.is_uint64()                                \
                                      ? static_cast<double>(n.get_uint64())    \
                                      : n.get_double();                        \
                    if (has_second_op) {                                       \
                        json_v = ApplyJsonArithOp(                             \
                            arith_type1, json_v, right_operand1);              \
                    }                                                          \
                    res[i] = (cmp);                                            \
                }                                                              \
            } else {                                                           \
                auto x = data[offset].template at<GetType>(pointer);           \
                if (x.error()) {                                               \
                    res[i] = false;                                            \
                    valid_res[i] = false;                                      \
                    continue;                                                  \
                }                                                              \
                auto json_v = x.value();                                       \
                if (has_second_op) {                                           \
                    json_v =                                                   \
                        ApplyJsonArithOp(arith_type1, json_v, right_operand1); \
                }                                                              \
                res[i] = (cmp);                                                \
            }                                                                  \
        }                                                                      \
    } while (false)

#define BinaryArithRangeJONCompareArrayLength(cmp)             \
    do {                                                       \
        for (size_t i = 0; i < size; ++i) {                    \
            auto offset = i;                                   \
            if constexpr (filter_type == FilterType::random) { \
                offset = (offsets) ? offsets[i] : i;           \
            }                                                  \
            if (valid_data && !valid_data[offset]) {           \
                res[i] = false;                                \
                valid_res[i] = false;                          \
                continue;                                      \
            }                                                  \
            int array_length = 0;                              \
            auto doc = data[offset].doc();                     \
            auto array = doc.at_pointer(pointer).get_array();  \
            if (array.error()) {                               \
                res[i] = false;                                \
                valid_res[i] = false;                          \
                continue;                                      \
            }                                                  \
            array_length = array.count_elements();             \
            res[i] = (cmp);                                    \
        }                                                      \
    } while (false)

    auto execute_sub_batch =
        [ op_type, arith_type1 = arith_type, arith_type2,
          has_second_op ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            ValidityView valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            ValueType val,
            ValueType right_operand1,
            ValueType right_operand2,
            const std::string& pointer) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // Nothing to do here since the caller has already handled valid_res.
        if (data == nullptr) {
            return;
        }
        // When has_second_op, dispatch below operates on op2 against a
        // json_v already pre-transformed through op1 (see
        // BinaryArithRangeJSONCompare / ApplyJsonArithOp above) — so
        // arith_type/right_operand alias op2's values in that case, op1's
        // otherwise (the single-op case, unchanged).
        auto arith_type = has_second_op ? arith_type2 : arith_type1;
        auto right_operand = has_second_op ? right_operand2 : right_operand1;
        switch (op_type) {
            case proto::plan::OpType::Equal: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(json_v + right_operand ==
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(json_v - right_operand ==
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(json_v * right_operand ==
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(json_v / right_operand ==
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            safe_mod(json_v, right_operand) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length ==
                                                              val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) & int64_t(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) | int64_t(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) ^ int64_t(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) << int64_t(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) >> int64_t(right_operand)) == val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::NotEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(json_v + right_operand !=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(json_v - right_operand !=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(json_v * right_operand !=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(json_v / right_operand !=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            safe_mod(json_v, right_operand) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length !=
                                                              val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) & int64_t(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) | int64_t(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) ^ int64_t(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) << int64_t(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) >> int64_t(right_operand)) != val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(json_v + right_operand >
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(json_v - right_operand >
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(json_v * right_operand >
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(json_v / right_operand >
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            safe_mod(json_v, right_operand) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length >
                                                              val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) & int64_t(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) | int64_t(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) ^ int64_t(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) << int64_t(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) >> int64_t(right_operand)) > val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::GreaterEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(json_v + right_operand >=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(json_v - right_operand >=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(json_v * right_operand >=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(json_v / right_operand >=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            safe_mod(json_v, right_operand) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length >=
                                                              val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) & int64_t(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) | int64_t(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) ^ int64_t(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) << int64_t(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) >> int64_t(right_operand)) >= val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessThan: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(json_v + right_operand <
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(json_v - right_operand <
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(json_v * right_operand <
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(json_v / right_operand <
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            safe_mod(json_v, right_operand) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length <
                                                              val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) & int64_t(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) | int64_t(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) ^ int64_t(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) << int64_t(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) >> int64_t(right_operand)) < val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            case proto::plan::OpType::LessEqual: {
                switch (arith_type) {
                    case proto::plan::ArithOpType::Add: {
                        BinaryArithRangeJSONCompare(json_v + right_operand <=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Sub: {
                        BinaryArithRangeJSONCompare(json_v - right_operand <=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mul: {
                        BinaryArithRangeJSONCompare(json_v * right_operand <=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Div: {
                        BinaryArithRangeJSONCompare(json_v / right_operand <=
                                                    val);
                        break;
                    }
                    case proto::plan::ArithOpType::Mod: {
                        BinaryArithRangeJSONCompare(
                            safe_mod(json_v, right_operand) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::ArrayLength: {
                        BinaryArithRangeJONCompareArrayLength(array_length <=
                                                              val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) & int64_t(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) | int64_t(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) ^ int64_t(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) << int64_t(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeJSONCompare(
                            (int64_t(json_v) >> int64_t(right_operand)) <= val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            default:
                ThrowInfo(UnexpectedError,
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
                                                            right_operand2,
                                                            pointer);
    } else {
        processed_size = ProcessDataChunks<milvus::Json>(execute_sub_batch,
                                                         std::nullptr_t{},
                                                         res,
                                                         valid_res,
                                                         value,
                                                         right_operand,
                                                         right_operand2,
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
    AssertInfo(expr_->arith_op_type_ != proto::plan::ArithOpType::ArrayLength,
               "ARRAY length must use ExecArrayLength");
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();

    if (!arg_inited_) {
        value_arg_.SetValue<ValueType>(expr_->value_);
        if (expr_->arith_op_type_ == proto::plan::ArithOpType::ArrayLength) {
            right_operand_arg_.SetValue(ValueType());
        } else {
            right_operand_arg_.SetValue<ValueType>(expr_->right_operand_);
        }
        if (expr_->has_second_op()) {
            right_operand2_arg_.SetValue<ValueType>(expr_->right_operand2_);
        }
        arg_inited_ = true;
    }

    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto arith_type2 = expr_->arith_op_type2_;
    auto has_second_op = expr_->has_second_op();
    auto value = value_arg_.GetValue<ValueType>();
    auto right_operand = right_operand_arg_.GetValue<ValueType>();
    auto right_operand2 =
        has_second_op ? right_operand2_arg_.GetValue<ValueType>() : ValueType();

    // Validate divisor for division/modulo operations
    if ((arith_type == proto::plan::ArithOpType::Div ||
         arith_type == proto::plan::ArithOpType::Mod) &&
        right_operand == 0) {
        ThrowInfo(
            ErrorCode::ExprInvalid,
            "division or modulus by zero in Array field arithmetic expression");
    }
    if (has_second_op &&
        (arith_type2 == proto::plan::ArithOpType::Div ||
         arith_type2 == proto::plan::ArithOpType::Mod) &&
        right_operand2 == 0) {
        ThrowInfo(
            ErrorCode::ExprInvalid,
            "division or modulus by zero in Array field arithmetic expression");
    }

// See BinaryArithRangeJSONCompare above for the has_second_op pre-transform.
#define BinaryArithRangeArrayCompare(cmp)                                     \
    do {                                                                      \
        for (size_t i = 0; i < size; ++i) {                                   \
            auto offset = i;                                                  \
            if constexpr (filter_type == FilterType::random) {                \
                offset = (offsets) ? offsets[i] : i;                          \
            }                                                                 \
            if (valid_data && !valid_data[offset]) {                          \
                res[i] = false;                                               \
                valid_res[i] = false;                                         \
                continue;                                                     \
            }                                                                 \
            if (index >= data[offset].length()) {                             \
                res[i] = false;                                               \
                valid_res[i] = false;                                         \
                continue;                                                     \
            }                                                                 \
            auto value = data[offset].get_data<GetType>(index);               \
            if (has_second_op) {                                              \
                value = ApplyJsonArithOp(arith_type1, value, right_operand1); \
            }                                                                 \
            res[i] = (cmp);                                                   \
        }                                                                     \
    } while (false)

    auto execute_sub_batch =
        [ op_type, arith_type1 = arith_type, arith_type2,
          has_second_op ]<FilterType filter_type = FilterType::sequential>(
            const ArrayView* data,
            ValidityView valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            ValueType val,
            ValueType right_operand1,
            ValueType right_operand2,
            int index) {
        if (arith_type1 != proto::plan::ArithOpType::ArrayLength) {
            AssertInfo(index >= 0,
                       "array arithmetic predicate requires nested path");
        }
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // Nothing to do here since the caller has already handled valid_res.
        if (data == nullptr) {
            return;
        }
        // See ExecRangeVisitorImplForJson's execute_sub_batch for why this
        // aliasing exists.
        auto arith_type = has_second_op ? arith_type2 : arith_type1;
        auto right_operand = has_second_op ? right_operand2 : right_operand1;
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
                            safe_mod(value, right_operand) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) & int64_t(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) | int64_t(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) ^ int64_t(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) << int64_t(right_operand)) == val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) >> int64_t(right_operand)) == val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
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
                            safe_mod(value, right_operand) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) & int64_t(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) | int64_t(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) ^ int64_t(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) << int64_t(right_operand)) != val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) >> int64_t(right_operand)) != val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
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
                            safe_mod(value, right_operand) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) & int64_t(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) | int64_t(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) ^ int64_t(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) << int64_t(right_operand)) > val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) >> int64_t(right_operand)) > val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
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
                            safe_mod(value, right_operand) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) & int64_t(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) | int64_t(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) ^ int64_t(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) << int64_t(right_operand)) >= val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) >> int64_t(right_operand)) >= val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
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
                            safe_mod(value, right_operand) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) & int64_t(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) | int64_t(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) ^ int64_t(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) << int64_t(right_operand)) < val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) >> int64_t(right_operand)) < val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
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
                            safe_mod(value, right_operand) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitAnd: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) & int64_t(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) | int64_t(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) ^ int64_t(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) << int64_t(right_operand)) <= val);
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        BinaryArithRangeArrayCompare(
                            (int64_t(value) >> int64_t(right_operand)) <= val);
                        break;
                    }
                    default:
                        ThrowInfo(
                            UnexpectedError,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            default:
                ThrowInfo(UnexpectedError,
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
                                                    right_operand2,
                                                    index);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(execute_sub_batch,
                                                              std::nullptr_t{},
                                                              res,
                                                              valid_res,
                                                              value,
                                                              right_operand,
                                                              right_operand2,
                                                              index);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ArrayType, typename ValueType, bool ElementLevel>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecArrayLength(OffsetVector* input) {
    if (expr_->arith_op_type_ != proto::plan::ArithOpType::ArrayLength) {
        ThrowInfo(UnexpectedError,
                  "unsupported arith type for ARRAY length expression: {}",
                  expr_->arith_op_type_);
    }
    AssertInfo(expr_->column_.element_level_ == ElementLevel,
               "ARRAY length element-level mismatch: plan={}, executor={}",
               expr_->column_.element_level_,
               ElementLevel);
    if constexpr (std::is_same_v<ArrayType, ArrayValueView>) {
        AssertInfo(expr_->column_.nested_path_.empty(),
                   "recursive ARRAY length does not support nested path now");
    }

    auto next_batch_size = GetNextRealBatchSize(input, ElementLevel);
    if (!next_batch_size.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *next_batch_size;
    if (auto res =
            AdvanceEmptyElementBatch(input, ElementLevel, real_batch_size)) {
        return res;
    }

    if (!arg_inited_) {
        value_arg_.SetValue<ValueType>(expr_->value_);
        arg_inited_ = true;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    auto op_type = expr_->op_type_;
    auto value = value_arg_.GetValue<ValueType>();

    auto compare_length = [op_type, value](int64_t length) {
        switch (op_type) {
            case proto::plan::OpType::Equal:
                return length == value;
            case proto::plan::OpType::NotEqual:
                return length != value;
            case proto::plan::OpType::GreaterThan:
                return length > value;
            case proto::plan::OpType::GreaterEqual:
                return length >= value;
            case proto::plan::OpType::LessThan:
                return length < value;
            case proto::plan::OpType::LessEqual:
                return length <= value;
            default:
                ThrowInfo(UnexpectedError,
                          "unsupported operator type for ARRAY length "
                          "expression: {}",
                          op_type);
        }
        return false;
    };

    auto execute_sub_batch = [compare_length]<FilterType filter_type =
                                                  FilterType::sequential>(
        const ArrayType* data,
        ValidityView valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        if (data == nullptr) {
            return;
        }
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = compare_length(
                static_cast<int64_t>(GetArrayRowSize(data[offset])));
        }
    };

    int64_t processed_size = 0;
    if (has_offset_input_) {
        if constexpr (ElementLevel) {
            processed_size = ProcessElementLevelByOffsets<ArrayType>(
                execute_sub_batch, std::nullptr_t{}, input, res, valid_res);
        } else {
            processed_size = ProcessDataByOffsets<ArrayType>(
                execute_sub_batch, std::nullptr_t{}, input, res, valid_res);
        }
    } else {
        if constexpr (ElementLevel) {
            processed_size = ProcessDataChunksForElementLevel<ArrayType>(
                execute_sub_batch, std::nullptr_t{}, res, valid_res);
        } else {
            processed_size = ProcessDataChunks<ArrayType>(
                execute_sub_batch, std::nullptr_t{}, res, valid_res);
        }
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
    if (exec_path_ == ExprExecPath::ScalarIndex) {
        return ExecRangeVisitorImplForIndex<T>(input);
    } else {
        return ExecRangeVisitorImplForData<T>(input);
    }
}

template <typename T>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForIndex(
    OffsetVector* input) {
    using Index = index::ScalarIndex<T>;
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisionType;
    auto next_batch_size =
        GetNextRealBatchSize(input, expr_->column_.element_level_);
    if (!next_batch_size.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *next_batch_size;
    if (auto res = AdvanceEmptyElementBatch(
            input, expr_->column_.element_level_, real_batch_size)) {
        return res;
    }
    if (!arg_inited_) {
        value_arg_.SetValue<HighPrecisionType>(expr_->value_);
        right_operand_arg_.SetValue<HighPrecisionType>(expr_->right_operand_);
        if (expr_->has_second_op()) {
            right_operand2_arg_.SetValue<HighPrecisionType>(
                expr_->right_operand2_);
        }
        arg_inited_ = true;
    }

    auto value = value_arg_.GetValue<HighPrecisionType>();
    auto right_operand = right_operand_arg_.GetValue<HighPrecisionType>();
    auto has_second_op = expr_->has_second_op();
    auto right_operand2 =
        has_second_op ? right_operand2_arg_.GetValue<HighPrecisionType>()
                      : HighPrecisionType();
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto arith_type2 = expr_->arith_op_type2_;
    auto sub_batch_size = has_offset_input_ ? input->size() : size_per_chunk_;

    auto execute_sub_batch =
        [
            op_type,
            arith_type,
            arith_type2,
            has_second_op,
            right_operand2,
            sub_batch_size
        ]<FilterType filter_type = FilterType::sequential>(
            Index * index_ptr,
            HighPrecisionType value,
            HighPrecisionType right_operand,
            const int32_t* offsets = nullptr) {
        TargetBitmap res;
        if (has_second_op) {
            // op_type/arith_type/arith_type2 are resolved at runtime inside
            // ArithOpIndexFunc2 rather than via a (cmp x arith x arith)
            // combinatorial template switch — see ApplyArithOp's comment in
            // the header for why.
            ArithOpIndexFunc2<T, filter_type> func;
            res = std::move(func(index_ptr,
                                 sub_batch_size,
                                 value,
                                 right_operand,
                                 right_operand2,
                                 op_type,
                                 arith_type,
                                 arith_type2,
                                 offsets));
            return res;
        }
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
                    case proto::plan::ArithOpType::BitAnd: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::BitAnd,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::BitOr,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::BitXor,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Shl,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::Equal,
                                         proto::plan::ArithOpType::Shr,
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
                        ThrowInfo(
                            UnexpectedError,
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
                    case proto::plan::ArithOpType::BitAnd: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::BitAnd,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::BitOr,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::BitXor,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Shl,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::NotEqual,
                                         proto::plan::ArithOpType::Shr,
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
                        ThrowInfo(
                            UnexpectedError,
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
                    case proto::plan::ArithOpType::BitAnd: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::BitAnd,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::BitOr,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::BitXor,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Shl,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Shr,
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
                        ThrowInfo(
                            UnexpectedError,
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
                    case proto::plan::ArithOpType::BitAnd: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::BitAnd,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::BitOr,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::BitXor,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Shl,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::GreaterEqual,
                                         proto::plan::ArithOpType::Shr,
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
                        ThrowInfo(
                            UnexpectedError,
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
                    case proto::plan::ArithOpType::BitAnd: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::BitAnd,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::BitOr,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::BitXor,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Shl,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessThan,
                                         proto::plan::ArithOpType::Shr,
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
                        ThrowInfo(
                            UnexpectedError,
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
                    case proto::plan::ArithOpType::BitAnd: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::BitAnd,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitOr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::BitOr,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::BitXor: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::BitXor,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shl: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Shl,
                                         filter_type>
                            func;
                        res = std::move(func(index_ptr,
                                             sub_batch_size,
                                             value,
                                             right_operand,
                                             offsets));
                        break;
                    }
                    case proto::plan::ArithOpType::Shr: {
                        ArithOpIndexFunc<T,
                                         proto::plan::OpType::LessEqual,
                                         proto::plan::ArithOpType::Shr,
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
                        ThrowInfo(
                            UnexpectedError,
                            fmt::format("unsupported arith type for binary "
                                        "arithmetic eval expr: {}",
                                        arith_type));
                }
                break;
            }
            default:
                ThrowInfo(UnexpectedError,
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

    auto next_batch_size =
        GetNextRealBatchSize(input, expr_->column_.element_level_);
    if (!next_batch_size.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *next_batch_size;
    if (auto res = AdvanceEmptyElementBatch(
            input, expr_->column_.element_level_, real_batch_size)) {
        return res;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    if (!arg_inited_) {
        value_arg_.SetValue<HighPrecisionType>(expr_->value_);
        right_operand_arg_.SetValue<HighPrecisionType>(expr_->right_operand_);
        if (expr_->has_second_op()) {
            right_operand2_arg_.SetValue<HighPrecisionType>(
                expr_->right_operand2_);
        }
        arg_inited_ = true;
    }

    auto value = value_arg_.GetValue<HighPrecisionType>();
    auto right_operand = right_operand_arg_.GetValue<HighPrecisionType>();
    auto has_second_op = expr_->has_second_op();
    auto right_operand2 =
        has_second_op ? right_operand2_arg_.GetValue<HighPrecisionType>()
                      : HighPrecisionType();
    auto op_type = expr_->op_type_;
    auto arith_type = expr_->arith_op_type_;
    auto arith_type2 = expr_->arith_op_type2_;

    auto execute_sub_batch =
        [ op_type, arith_type, arith_type2, has_second_op,
          right_operand2 ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            ValidityView valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            HighPrecisionType value,
            HighPrecisionType right_operand) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // Nothing to do here since the caller has already handled valid_res.
        if (data == nullptr) {
            return;
        }
        if (has_second_op) {
            // op_type/arith_type/arith_type2 are resolved at runtime inside
            // ArithOpElementFunc2 rather than via a (cmp x arith x arith)
            // combinatorial template switch — see ApplyArithOp's comment in
            // the header for why.
            ArithOpElementFunc2<T, filter_type> func;
            func(data,
                 size,
                 value,
                 right_operand,
                 right_operand2,
                 op_type,
                 arith_type,
                 arith_type2,
                 res,
                 offsets);
            // Shared tail (valid-mask application) below still needs to run;
            // fall through instead of returning early.
        } else {
            switch (op_type) {
                case proto::plan::OpType::Equal: {
                    switch (arith_type) {
                        case proto::plan::ArithOpType::Add: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::Add,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Sub: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::Sub,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mul: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::Mul,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Div: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::Div,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mod: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::Mod,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitAnd: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::BitAnd,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitOr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::BitOr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitXor: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::BitXor,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shl: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::Shl,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::Equal,
                                               proto::plan::ArithOpType::Shr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        default:
                            ThrowInfo(
                                UnexpectedError,
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
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Sub: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::Sub,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mul: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::Mul,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Div: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::Div,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mod: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::Mod,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitAnd: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::BitAnd,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitOr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::BitOr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitXor: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::BitXor,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shl: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::Shl,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::NotEqual,
                                               proto::plan::ArithOpType::Shr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        default:
                            ThrowInfo(
                                UnexpectedError,
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
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Sub: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::Sub,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mul: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::Mul,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Div: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::Div,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mod: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::Mod,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitAnd: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::BitAnd,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitOr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::BitOr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitXor: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::BitXor,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shl: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::Shl,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::GreaterThan,
                                               proto::plan::ArithOpType::Shr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        default:
                            ThrowInfo(
                                UnexpectedError,
                                fmt::format("unsupported arith type for binary "
                                            "arithmetic eval expr: {}",
                                            arith_type));
                    }
                    break;
                }
                case proto::plan::OpType::GreaterEqual: {
                    switch (arith_type) {
                        case proto::plan::ArithOpType::Add: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::Add,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Sub: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::Sub,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mul: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::Mul,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Div: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::Div,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mod: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::Mod,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitAnd: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::BitAnd,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitOr: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::BitOr,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitXor: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::BitXor,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shl: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::Shl,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shr: {
                            ArithOpElementFunc<
                                T,
                                proto::plan::OpType::GreaterEqual,
                                proto::plan::ArithOpType::Shr,
                                filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        default:
                            ThrowInfo(
                                UnexpectedError,
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
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Sub: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::Sub,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mul: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::Mul,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Div: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::Div,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mod: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::Mod,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitAnd: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::BitAnd,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitOr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::BitOr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitXor: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::BitXor,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shl: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::Shl,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessThan,
                                               proto::plan::ArithOpType::Shr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        default:
                            ThrowInfo(
                                UnexpectedError,
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
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Sub: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::Sub,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mul: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::Mul,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Div: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::Div,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Mod: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::Mod,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitAnd: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::BitAnd,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitOr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::BitOr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::BitXor: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::BitXor,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shl: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::Shl,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        case proto::plan::ArithOpType::Shr: {
                            ArithOpElementFunc<T,
                                               proto::plan::OpType::LessEqual,
                                               proto::plan::ArithOpType::Shr,
                                               filter_type>
                                func;
                            func(
                                data, size, value, right_operand, res, offsets);
                            break;
                        }
                        default:
                            ThrowInfo(
                                UnexpectedError,
                                fmt::format("unsupported arith type for binary "
                                            "arithmetic eval expr: {}",
                                            arith_type));
                    }
                    break;
                }
                default:
                    ThrowInfo(UnexpectedError,
                              "unsupported operator type for binary "
                              "arithmetic eval expr: {}",
                              op_type);
            }
        }
        // there is a batch operation in ArithOpElementFunc,
        // so not divide data again for the reason that it may reduce performance if the null distribution is scattered
        // but to mask res with valid_data after the batch operation.
        if constexpr (filter_type == FilterType::sequential) {
            // contiguous rows: reuse the vectorized shared helper
            ApplyValidMask(valid_data, res, valid_res, size);
        } else if (valid_data) {
            // scattered by offsets: gather, keep the per-row loop
            for (int i = 0; i < size; i++) {
                auto offset = (offsets) ? offsets[i] : i;
                if (!valid_data[offset]) {
                    res[i] = valid_res[i] = false;
                }
            }
        }
    };

    auto skip_index_func = [op_ctx = op_ctx_,
                            op_type,
                            arith_type,
                            value,
                            right_operand,
                            has_second_op](const SkipIndex& skip_index,
                                           FieldId field_id,
                                           int64_t chunk_id) {
        // CanSkipBinaryArithRange only reasons about a single arithmetic
        // op against per-chunk min/max stats; it has no notion of a
        // second chained op. Extending it is out of scope here, so
        // conservatively never skip a chunk for a depth-2 expression
        // rather than risk incorrectly pruning rows that would still
        // match after the second op is applied.
        if (has_second_op) {
            return false;
        }
        return skip_index.CanSkipBinaryArithRange<T>(op_ctx,
                                                     field_id,
                                                     chunk_id,
                                                     op_type,
                                                     arith_type,
                                                     value,
                                                     right_operand);
    };

    int64_t processed_size;
    if (has_offset_input_) {
        if (expr_->column_.element_level_) {
            // For element-level filtering with offset input
            processed_size = ProcessElementLevelByOffsets<T>(execute_sub_batch,
                                                             skip_index_func,
                                                             input,
                                                             res,
                                                             valid_res,
                                                             value,
                                                             right_operand);
        } else {
            processed_size = ProcessDataByOffsets<T>(execute_sub_batch,
                                                     skip_index_func,
                                                     input,
                                                     res,
                                                     valid_res,
                                                     value,
                                                     right_operand);
        }
    } else {
        if (expr_->column_.element_level_) {
            // For element-level filtering without offset input (brute force)
            processed_size =
                ProcessDataChunksForElementLevel<T>(execute_sub_batch,
                                                    skip_index_func,
                                                    res,
                                                    valid_res,
                                                    value,
                                                    right_operand);
        } else {
            processed_size = ProcessDataChunks<T>(execute_sub_batch,
                                                  skip_index_func,
                                                  res,
                                                  valid_res,
                                                  value,
                                                  right_operand);
        }
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

void
PhyBinaryArithOpEvalRangeExpr::PrefetchRawData() {
    auto datatype = expr_->column_.data_type_;
    if (expr_->column_.element_level_) {
        datatype = expr_->column_.element_type_;
    }

    switch (datatype) {
        case DataType::BOOL: {
            PrefetchRawData<bool>();
            break;
        }
        case DataType::INT8: {
            PrefetchRawData<int8_t>();
            break;
        }
        case DataType::INT16: {
            PrefetchRawData<int16_t>();
            break;
        }
        case DataType::INT32: {
            PrefetchRawData<int32_t>();
            break;
        }
        case DataType::INT64: {
            PrefetchRawData<int64_t>();
            break;
        }
        case DataType::FLOAT: {
            PrefetchRawData<float>();
            break;
        }
        case DataType::DOUBLE: {
            PrefetchRawData<double>();
            break;
        }
        default: {
            SegmentExpr::PrefetchRawData(expr_->column_.field_id_);
            break;
        }
    }
}

template <typename T>
void
PhyBinaryArithOpEvalRangeExpr::PrefetchRawData() {
    using H =
        std::conditional_t<std::is_integral_v<T> && !std::is_same_v<bool, T>,
                           int64_t,
                           T>;
    auto skip_index = segment_->GetSkipIndex();
    auto value = GetValueWithCastNumber<H>(expr_->value_);
    auto right_value = GetValueWithCastNumber<H>(expr_->right_operand_);

    std::vector<int64_t> chunks_may_hit;
    for (size_t i = RawDataPrefetchStartChunk(); i < num_data_chunk_; ++i) {
        auto skip =
            skip_index->CanSkipBinaryArithRange<T>(field_id_,
                                                   i,
                                                   expr_->op_type_,
                                                   expr_->arith_op_type_,
                                                   value,
                                                   right_value);
        if (!skip) {
            chunks_may_hit.push_back(i);
        }
    }
    segment_->prefetch_chunks(op_ctx_, field_id_, chunks_may_hit);
}

template VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImpl<int64_t>(
    OffsetVector*);  // add this for macos

}  //namespace exec
}  // namespace milvus

#undef BinaryArithRangeJSONCompare
#undef BinaryArithRangeJONCompareArrayLength
#undef BinaryArithRangeArrayCompare
