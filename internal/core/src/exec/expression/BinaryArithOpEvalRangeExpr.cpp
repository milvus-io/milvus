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
        case DataType::JSON: {
            auto value_type = expr_->value_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kBoolVal: {
                    result = ExecRangeVisitorImplForJson<bool>(context);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    result = ExecRangeVisitorImplForJson<int64_t>(context);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    result = ExecRangeVisitorImplForJson<double>(context);
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
                                                         true>(context);
                            } else {
                                result = ExecArrayLength<ArrayValueView,
                                                         int64_t,
                                                         false>(context);
                            }
                        } else {
                            result = ExecArrayLength<ArrayView, int64_t, false>(
                                context);
                        }
                    } else {
                        result = ExecRangeVisitorImplForArray<int64_t>(context);
                    }
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    if (is_array_length) {
                        if (is_nested_array) {
                            if (expr_->column_.element_level_) {
                                result = ExecArrayLength<ArrayValueView,
                                                         double,
                                                         true>(context);
                            } else {
                                result = ExecArrayLength<ArrayValueView,
                                                         double,
                                                         false>(context);
                            }
                        } else {
                            result = ExecArrayLength<ArrayView, double, false>(
                                context);
                        }
                    } else {
                        result = ExecRangeVisitorImplForArray<double>(context);
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
                    result = ExecArrayLength<VectorArrayView, int64_t, false>(
                        context);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    result = ExecArrayLength<VectorArrayView, double, false>(
                        context);
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

template <typename ValueType>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForJson(EvalCtx& context) {
    if (!arg_inited_) {
        value_arg_.SetValue<ValueType>(expr_->value_);
        if (expr_->arith_op_type_ == proto::plan::ArithOpType::ArrayLength) {
            right_operand_arg_.SetValue(ValueType());
        } else {
            right_operand_arg_.SetValue<ValueType>(expr_->right_operand_);
        }
        arg_inited_ = true;
    }
    return EvalKernel<milvus::Json>(
        context,
        BinaryArithJsonKernel<ValueType>{
            .cmp_op = expr_->op_type_,
            .arith_op = expr_->arith_op_type_,
            .value = value_arg_.GetValue<ValueType>(),
            .right_operand = right_operand_arg_.GetValue<ValueType>(),
            .pointer = milvus::Json::pointer(expr_->column_.nested_path_),
        },
        /*element_level=*/false);
}

template <typename ValueType>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForArray(EvalCtx& context) {
    AssertInfo(expr_->arith_op_type_ != proto::plan::ArithOpType::ArrayLength,
               "ARRAY length must use ExecArrayLength");
    if (!arg_inited_) {
        value_arg_.SetValue<ValueType>(expr_->value_);
        right_operand_arg_.SetValue<ValueType>(expr_->right_operand_);
        arg_inited_ = true;
    }
    int index = -1;
    if (!expr_->column_.nested_path_.empty()) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }
    return EvalKernel<milvus::ArrayView>(
        context,
        BinaryArithArrayKernel<ValueType>{
            .cmp_op = expr_->op_type_,
            .arith_op = expr_->arith_op_type_,
            .value = value_arg_.GetValue<ValueType>(),
            .right_operand = right_operand_arg_.GetValue<ValueType>(),
            .index = index,
        },
        /*element_level=*/false);
}

template <typename ArrayType, typename ValueType, bool ElementLevel>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecArrayLength(EvalCtx& context) {
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
    if (!arg_inited_) {
        value_arg_.SetValue<ValueType>(expr_->value_);
        arg_inited_ = true;
    }
    return EvalKernel<ArrayType>(context,
                                 ArrayLengthKernel<ArrayType, ValueType>{
                                     .cmp_op = expr_->op_type_,
                                     .value = value_arg_.GetValue<ValueType>(),
                                 },
                                 ElementLevel);
}

template <typename T>
VectorPtr
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImpl(EvalCtx& context) {
    if (exec_path_ == ExprExecPath::ScalarIndex) {
        return ExecRangeVisitorImplForIndex<T>(context.get_offset_input());
    } else {
        return ExecRangeVisitorImplForData<T>(context);
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
        arg_inited_ = true;
    }

    auto value = value_arg_.GetValue<HighPrecisionType>();
    auto right_operand = right_operand_arg_.GetValue<HighPrecisionType>();
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
PhyBinaryArithOpEvalRangeExpr::ExecRangeVisitorImplForData(EvalCtx& context) {
    using HighPrecisionType = milvus::bitset::ArithHighPrecisionType<T>;
    if (!arg_inited_) {
        value_arg_.SetValue<HighPrecisionType>(expr_->value_);
        right_operand_arg_.SetValue<HighPrecisionType>(expr_->right_operand_);
        arg_inited_ = true;
    }
    return EvalKernel<T>(
        context,
        BinaryArithScalarKernel<T>{
            .cmp_op = expr_->op_type_,
            .arith_op = expr_->arith_op_type_,
            .value = value_arg_.GetValue<HighPrecisionType>(),
            .right_operand = right_operand_arg_.GetValue<HighPrecisionType>(),
            .op_ctx = op_ctx_,
        },
        expr_->column_.element_level_);
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
    EvalCtx&);  // add this for macos

}  //namespace exec
}  // namespace milvus
