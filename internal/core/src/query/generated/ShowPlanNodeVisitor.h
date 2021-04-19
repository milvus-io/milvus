#pragma once
// Generated File
// DO NOT EDIT
#include "utils/EasyAssert.h"
#include "utils/Json.h"
#include <optional>

#include "PlanNodeVisitor.h"

namespace milvus::query {
class ShowPlanNodeVisitor : PlanNodeVisitor {
 public:
    virtual void
    visit(FloatVectorANNS& node) override;

    virtual void
    visit(BinaryVectorANNS& node) override;

 public:
    using RetType = nlohmann::json;

 public:
    RetType
    call_child(PlanNode& node) {
        assert(!ret_.has_value());
        node.accept(*this);
        assert(ret_.has_value());
        auto ret = std::move(ret_);
        ret_ = std::nullopt;
        return std::move(ret.value());
    }

 private:
    std::optional<RetType> ret_;
};
}  // namespace milvus::query
