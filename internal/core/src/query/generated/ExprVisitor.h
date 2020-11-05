#pragma once
// Generated File
// DO NOT EDIT
#include "query/Expr.h"
namespace milvus::query {
class ExprVisitor {
 public:
    virtual ~ExprVisitor() = 0;

 public:
    virtual void
    visit(BoolUnaryExpr&) = 0;

    virtual void
    visit(BoolBinaryExpr&) = 0;

    virtual void
    visit(TermExpr&) = 0;

    virtual void
    visit(RangeExpr&) = 0;
};
}  // namespace milvus::query
