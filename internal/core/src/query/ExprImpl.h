#pragma once
#include "Expr.h"

namespace milvus::query {
template <typename T>
struct TermExprImpl : TermExpr {
    std::vector<T> terms_;
};

template <typename T>
struct RangeExprImpl : RangeExpr {
    std::vector<std::tuple<OpType, T>> conditions_;
};

}  // namespace milvus::query