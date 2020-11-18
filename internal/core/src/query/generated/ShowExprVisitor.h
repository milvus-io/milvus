#pragma once
// Generated File
// DO NOT EDIT
#include "query/Plan.h"
#include "utils/EasyAssert.h"
#include "utils/Json.h"
#include "ExprVisitor.h"

namespace milvus::query {
class ShowExprVisitor : ExprVisitor {
 public:
    void
    visit(BoolUnaryExpr& expr) override;

    void
    visit(BoolBinaryExpr& expr) override;

    void
    visit(TermExpr& expr) override;

    void
    visit(RangeExpr& expr) override;

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
}  // namespace milvus::query
