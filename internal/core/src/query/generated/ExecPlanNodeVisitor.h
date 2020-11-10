#pragma once
// Generated File
// DO NOT EDIT
#include "PlanNodeVisitor.h"
namespace milvus::query {
class ExecPlanNodeVisitor : PlanNodeVisitor {
 public:
    virtual void
    visit(FloatVectorANNS& node) override;

    virtual void
    visit(BinaryVectorANNS& node) override;

 public:
    using RetType = segcore::QueryResult;
    ExecPlanNodeVisitor(segcore::SegmentBase& segment, segcore::Timestamp timestamp, const float* src_data)
        : segment_(segment), timestamp_(timestamp), src_data_(src_data) {
    }
    // using RetType = nlohmann::json;

    RetType get_moved_result(){
        assert(ret_.has_value());
        auto ret = std::move(ret_).value();
        ret_ = std::nullopt;
        return ret;
    }
 private:
    // std::optional<RetType> ret_;
    segcore::SegmentBase& segment_;
    segcore::Timestamp timestamp_;
    const float* src_data_;

    std::optional<RetType> ret_;
};
}  // namespace milvus::query
