#pragma once
// Generated File
// DO NOT EDIT
#include "ExprVisitor.h"
namespace milvus::query {
class ShowExprVisitor : ExprVisitor {
 public:
    virtual void
    visit(BoolUnaryExpr& expr) override;

    virtual void
    visit(BoolBinaryExpr& expr) override;

    virtual void
    visit(TermExpr& expr) override;

    virtual void
    visit(RangeExpr& expr) override;

 public:
};
}  // namespace milvus::query
