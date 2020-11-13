#pragma once
#include "Expr.h"

namespace milvus::query {
template <typename T>
struct TermExprImpl : TermExpr {
    std::vector<T> terms_;
};

template <typename T>
struct RangeExprImpl : RangeExpr {
    enum class OpType { LessThan, LessEqual, GreaterThan, GreaterEqual, Equal, NotEqual };
    std::vector<std::tuple<OpType, T>> conditions_;
};

}  // namespace milvus::query