#include "query/Plan.h"
#include "utils/EasyAssert.h"
#include "utils/Json.h"
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
    combine(Json&& extra, UnaryExpr& expr) {
        auto result = std::move(extra);
        result["child"] = call_child(*expr.child_);
        return result;
    }

    Json
    combine(Json&& extra, BinaryExpr& expr) {
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
ShowExprVisitor::visit(BoolUnaryExpr& expr) {
    Assert(!ret_.has_value());
    using OpType = BoolUnaryExpr::OpType;

    // TODO: use magic_enum if available
    Assert(expr.op_type_ == OpType::LogicalNot);
    auto op_name = "LogicalNot";

    Json extra{
        {"expr_type", "BoolUnary"},
        {"op", op_name},
    };
    ret_ = this->combine(std::move(extra), expr);
}

void
ShowExprVisitor::visit(BoolBinaryExpr& expr) {
    Assert(!ret_.has_value());
    using OpType = BoolBinaryExpr::OpType;

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
    Assert(expr);
    return Json{expr->terms_};
}

void
ShowExprVisitor::visit(TermExpr& expr) {
    Assert(!ret_.has_value());
    Assert(field_is_vector(expr.data_type_) == false);
    auto terms = [&] {
        switch (expr.data_type_) {
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
            case DataType::BOOL:
                return TermExtract<bool>(expr);
            default:
                PanicInfo("unsupported type");
        }
    }();

    Json res{{"expr_type", "Term"},
             {"field_id", expr.field_id_},
             {"data_type", datatype_name(expr.data_type_)},
             {"terms", std::move(terms)}};

    ret_ = res;
}

template <typename T>
static Json
CondtionExtract(const RangeExpr& expr_raw) {
    auto expr = dynamic_cast<const TermExprImpl<T>*>(&expr_raw);
    Assert(expr);
    return Json{expr->terms_};
}

void
ShowExprVisitor::visit(RangeExpr& expr) {
    Assert(!ret_.has_value());
    Assert(field_is_vector(expr.data_type_) == false);
    auto conditions = [&] {
        switch (expr.data_type_) {
            case DataType::BOOL:
                return CondtionExtract<bool>(expr);
            case DataType::INT8:
                return CondtionExtract<int8_t>(expr);
            case DataType::INT16:
                return CondtionExtract<int16_t>(expr);
            case DataType::INT32:
                return CondtionExtract<int32_t>(expr);
            case DataType::INT64:
                return CondtionExtract<int64_t>(expr);
            case DataType::DOUBLE:
                return CondtionExtract<double>(expr);
            case DataType::FLOAT:
                return CondtionExtract<float>(expr);
            default:
                PanicInfo("unsupported type");
        }
    }();

    Json res{{"expr_type", "Range"},
             {"field_id", expr.field_id_},
             {"data_type", datatype_name(expr.data_type_)},
             {"conditions", std::move(conditions)}};
}
}  // namespace milvus::query
