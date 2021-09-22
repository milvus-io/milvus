// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "query/Plan.h"
#include <utility>
#include "query/generated/ShowExprVisitor.h"
#include "query/ExprImpl.h"

namespace milvus::query {
using Json = nlohmann::json;

#if 1
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR
namespace impl {
class ShowExprNodeVisitor : ExprVisitor {
 public:
    using RetType = Json;

 public:
    RetType
    call_child(Expr& expr) {
        assert(!ret_.has_value());
        expr.accept(*this);
        assert(ret_.has_value());
        auto ret = std::move(ret_);
        ret_ = std::nullopt;
        return std::move(ret.value());
    }

    Json
    combine(Json&& extra, UnaryExprBase& expr) {
        auto result = std::move(extra);
        result["child"] = call_child(*expr.child_);
        return result;
    }

    Json
    combine(Json&& extra, BinaryExprBase& expr) {
        auto result = std::move(extra);
        result["left_child"] = call_child(*expr.left_);
        result["right_child"] = call_child(*expr.right_);
        return result;
    }

 private:
    std::optional<RetType> ret_;
};
}  // namespace impl
#endif

void
ShowExprVisitor::visit(LogicalUnaryExpr& expr) {
    AssertInfo(!ret_.has_value(), "[ShowExprVisitor]Ret json already has value before visit");
    using OpType = LogicalUnaryExpr::OpType;

    // TODO: use magic_enum if available
    AssertInfo(expr.op_type_ == OpType::LogicalNot, "[ShowExprVisitor]Expr op type isn't LogicNot");
    auto op_name = "LogicalNot";

    Json extra{
        {"expr_type", "BoolUnary"},
        {"op", op_name},
    };
    ret_ = this->combine(std::move(extra), expr);
}

void
ShowExprVisitor::visit(LogicalBinaryExpr& expr) {
    AssertInfo(!ret_.has_value(), "[ShowExprVisitor]Ret json already has value before visit");
    using OpType = LogicalBinaryExpr::OpType;

    // TODO: use magic_enum if available
    auto op_name = [](OpType op) {
        switch (op) {
            case OpType::LogicalAnd:
                return "LogicalAnd";
            case OpType::LogicalOr:
                return "LogicalOr";
            case OpType::LogicalXor:
                return "LogicalXor";
            default:
                PanicInfo("unsupported op");
        }
    }(expr.op_type_);

    Json extra{
        {"expr_type", "BoolBinary"},
        {"op", op_name},
    };
    ret_ = this->combine(std::move(extra), expr);
}

template <typename T>
static Json
TermExtract(const TermExpr& expr_raw) {
    auto expr = dynamic_cast<const TermExprImpl<T>*>(&expr_raw);
    AssertInfo(expr, "[ShowExprVisitor]TermExpr cast to TermExprImpl failed");
    return Json{expr->terms_};
}

void
ShowExprVisitor::visit(TermExpr& expr) {
    AssertInfo(!ret_.has_value(), "[ShowExprVisitor]Ret json already has value before visit");
    AssertInfo(datatype_is_vector(expr.data_type_) == false, "[ShowExprVisitor]Data type of expr isn't vector type");
    auto terms = [&] {
        switch (expr.data_type_) {
            case DataType::BOOL:
                return TermExtract<bool>(expr);
            case DataType::INT8:
                return TermExtract<int8_t>(expr);
            case DataType::INT16:
                return TermExtract<int16_t>(expr);
            case DataType::INT32:
                return TermExtract<int32_t>(expr);
            case DataType::INT64:
                return TermExtract<int64_t>(expr);
            case DataType::DOUBLE:
                return TermExtract<double>(expr);
            case DataType::FLOAT:
                return TermExtract<float>(expr);
            default:
                PanicInfo("unsupported type");
        }
    }();

    Json res{{"expr_type", "Term"},
             {"field_offset", expr.field_offset_.get()},
             {"data_type", datatype_name(expr.data_type_)},
             {"terms", std::move(terms)}};

    ret_ = res;
}

template <typename T>
static Json
UnaryRangeExtract(const UnaryRangeExpr& expr_raw) {
    using proto::plan::OpType;
    using proto::plan::OpType_Name;
    auto expr = dynamic_cast<const UnaryRangeExprImpl<T>*>(&expr_raw);
    AssertInfo(expr, "[ShowExprVisitor]UnaryRangeExpr cast to UnaryRangeExprImpl failed");
    Json res{{"expr_type", "UnaryRange"},
             {"field_offset", expr->field_offset_.get()},
             {"data_type", datatype_name(expr->data_type_)},
             {"op", OpType_Name(static_cast<OpType>(expr->op_type_))},
             {"value", expr->value_}};
    return res;
}

void
ShowExprVisitor::visit(UnaryRangeExpr& expr) {
    AssertInfo(!ret_.has_value(), "[ShowExprVisitor]Ret json already has value before visit");
    AssertInfo(datatype_is_vector(expr.data_type_) == false, "[ShowExprVisitor]Data type of expr isn't vector type");
    switch (expr.data_type_) {
        case DataType::BOOL:
            ret_ = UnaryRangeExtract<bool>(expr);
            return;
        case DataType::INT8:
            ret_ = UnaryRangeExtract<int8_t>(expr);
            return;
        case DataType::INT16:
            ret_ = UnaryRangeExtract<int16_t>(expr);
            return;
        case DataType::INT32:
            ret_ = UnaryRangeExtract<int32_t>(expr);
            return;
        case DataType::INT64:
            ret_ = UnaryRangeExtract<int64_t>(expr);
            return;
        case DataType::DOUBLE:
            ret_ = UnaryRangeExtract<double>(expr);
            return;
        case DataType::FLOAT:
            ret_ = UnaryRangeExtract<float>(expr);
            return;
        default:
            PanicInfo("unsupported type");
    }
}

template <typename T>
static Json
BinaryRangeExtract(const BinaryRangeExpr& expr_raw) {
    using proto::plan::OpType;
    using proto::plan::OpType_Name;
    auto expr = dynamic_cast<const BinaryRangeExprImpl<T>*>(&expr_raw);
    AssertInfo(expr, "[ShowExprVisitor]BinaryRangeExpr cast to BinaryRangeExprImpl failed");
    Json res{{"expr_type", "BinaryRange"},
             {"field_offset", expr->field_offset_.get()},
             {"data_type", datatype_name(expr->data_type_)},
             {"lower_inclusive", expr->lower_inclusive_},
             {"upper_inclusive", expr->upper_inclusive_},
             {"lower_value", expr->lower_value_},
             {"upper_value", expr->upper_value_}};
    return res;
}

void
ShowExprVisitor::visit(BinaryRangeExpr& expr) {
    AssertInfo(!ret_.has_value(), "[ShowExprVisitor]Ret json already has value before visit");
    AssertInfo(datatype_is_vector(expr.data_type_) == false, "[ShowExprVisitor]Data type of expr isn't vector type");
    switch (expr.data_type_) {
        case DataType::BOOL:
            ret_ = BinaryRangeExtract<bool>(expr);
            return;
        case DataType::INT8:
            ret_ = BinaryRangeExtract<int8_t>(expr);
            return;
        case DataType::INT16:
            ret_ = BinaryRangeExtract<int16_t>(expr);
            return;
        case DataType::INT32:
            ret_ = BinaryRangeExtract<int32_t>(expr);
            return;
        case DataType::INT64:
            ret_ = BinaryRangeExtract<int64_t>(expr);
            return;
        case DataType::DOUBLE:
            ret_ = BinaryRangeExtract<double>(expr);
            return;
        case DataType::FLOAT:
            ret_ = BinaryRangeExtract<float>(expr);
            return;
        default:
            PanicInfo("unsupported type");
    }
}

void
ShowExprVisitor::visit(CompareExpr& expr) {
    using proto::plan::OpType;
    using proto::plan::OpType_Name;
    AssertInfo(!ret_.has_value(), "[ShowExprVisitor]Ret json already has value before visit");

    Json res{{"expr_type", "Compare"},
             {"left_field_offset", expr.left_field_offset_.get()},
             {"left_data_type", datatype_name(expr.left_data_type_)},
             {"right_field_offset", expr.right_field_offset_.get()},
             {"right_data_type", datatype_name(expr.right_data_type_)},
             {"op", OpType_Name(static_cast<OpType>(expr.op_type_))}};
    ret_ = res;
}
}  // namespace milvus::query
