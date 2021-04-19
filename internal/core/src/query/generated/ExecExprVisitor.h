#pragma once
// Generated File
// DO NOT EDIT
#include "segcore/SegmentSmallIndex.h"
#include <optional>
#include "query/ExprImpl.h"
#include "boost/dynamic_bitset.hpp"
#include "ExprVisitor.h"

namespace milvus::query {
class ExecExprVisitor : ExprVisitor {
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
    using RetType = std::deque<boost::dynamic_bitset<>>;
    explicit ExecExprVisitor(segcore::SegmentSmallIndex& segment) : segment_(segment) {
    }
    RetType
    call_child(Expr& expr) {
        Assert(!ret_.has_value());
        expr.accept(*this);
        Assert(ret_.has_value());
        auto ret = std::move(ret_);
        ret_ = std::nullopt;
        return std::move(ret.value());
    }

 public:
    template <typename T, typename IndexFunc, typename ElementFunc>
    auto
    ExecRangeVisitorImpl(RangeExprImpl<T>& expr, IndexFunc func, ElementFunc element_func) -> RetType;

    template <typename T>
    auto
    ExecRangeVisitorDispatcher(RangeExpr& expr_raw) -> RetType;

 private:
    segcore::SegmentSmallIndex& segment_;
    std::optional<RetType> ret_;
};
}  // namespace milvus::query
