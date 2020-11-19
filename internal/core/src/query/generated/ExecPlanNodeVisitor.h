#pragma once
// Generated File
// DO NOT EDIT
#include "utils/Json.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentBase.h"
#include "PlanNodeVisitor.h"

namespace milvus::query {
class ExecPlanNodeVisitor : PlanNodeVisitor {
 public:
    void
    visit(FloatVectorANNS& node) override;

    void
    visit(BinaryVectorANNS& node) override;

 public:
    using RetType = segcore::QueryResult;
    ExecPlanNodeVisitor(segcore::SegmentBase& segment, Timestamp timestamp, const PlaceholderGroup& placeholder_group)
        : segment_(segment), timestamp_(timestamp), placeholder_group_(placeholder_group) {
    }
    // using RetType = nlohmann::json;

    RetType
    get_moved_result(PlanNode& node) {
        assert(!ret_.has_value());
        node.accept(*this);
        assert(ret_.has_value());
        auto ret = std::move(ret_).value();
        ret_ = std::nullopt;
        return ret;
    }

 private:
    // std::optional<RetType> ret_;
    segcore::SegmentBase& segment_;
    Timestamp timestamp_;
    const PlaceholderGroup& placeholder_group_;

    std::optional<RetType> ret_;
};
}  // namespace milvus::query
