// Generated File
// DO NOT EDIT
#include "query/Expr.h"
#include "ExprVisitor.h"

namespace milvus::query {
void
BoolUnaryExpr::accept(ExprVisitor& visitor) {
    visitor.visit(*this);
}

void
BoolBinaryExpr::accept(ExprVisitor& visitor) {
    visitor.visit(*this);
}

void
TermExpr::accept(ExprVisitor& visitor) {
    visitor.visit(*this);
}

void
RangeExpr::accept(ExprVisitor& visitor) {
    visitor.visit(*this);
}

}  // namespace milvus::query
