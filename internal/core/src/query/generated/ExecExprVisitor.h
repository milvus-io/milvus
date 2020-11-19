#pragma once
// Generated File
// DO NOT EDIT
#include "segcore/SegmentNaive.h"
#include <optional>
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
    using RetType = faiss::ConcurrentBitsetPtr;
    explicit ExecExprVisitor(segcore::SegmentNaive& segment) : segment_(segment) {
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

 private:
    segcore::SegmentNaive& segment_;
    std::optional<RetType> ret_;
};
}  // namespace milvus::query
