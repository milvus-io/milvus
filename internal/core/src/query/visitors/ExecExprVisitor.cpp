#include "segcore/SegmentNaive.h"
#include <optional>
#include "query/generated/ExecExprVisitor.h"

namespace milvus::query {
#if 1
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR
namespace impl {
class ExecExprVisitor : ExprVisitor {
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
}  // namespace impl
#endif

void
ExecExprVisitor::visit(BoolUnaryExpr& expr) {
    PanicInfo("unimplemented");
}

void
ExecExprVisitor::visit(BoolBinaryExpr& expr) {
    PanicInfo("unimplemented");
}

void
ExecExprVisitor::visit(TermExpr& expr) {
    PanicInfo("unimplemented");
}

void
ExecExprVisitor::visit(RangeExpr& expr) {
}

}  // namespace milvus::query
