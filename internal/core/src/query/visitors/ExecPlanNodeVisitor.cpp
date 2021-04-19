#include "utils/Json.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentBase.h"
#include "query/generated/ExecPlanNodeVisitor.h"
#include "segcore/SegmentSmallIndex.h"
#include "query/generated/ExecExprVisitor.h"
#include "query/Search.h"

namespace milvus::query {

#if 1
namespace impl {
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR UNDER suvlim/core_gen/
class ExecPlanNodeVisitor : PlanNodeVisitor {
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
}  // namespace impl
#endif

void
ExecPlanNodeVisitor::visit(FloatVectorANNS& node) {
    // TODO: optimize here, remove the dynamic cast
    assert(!ret_.has_value());
    auto segment = dynamic_cast<segcore::SegmentSmallIndex*>(&segment_);
    AssertInfo(segment, "support SegmentSmallIndex Only");
    RetType ret;
    auto& ph = placeholder_group_.at(0);
    auto src_data = ph.get_blob<float>();
    auto num_queries = ph.num_of_queries_;
    if (node.predicate_.has_value()) {
        auto bitmap = ExecExprVisitor(*segment).call_child(*node.predicate_.value());
        auto ptr = &bitmap;
        QueryBruteForceImpl(*segment, node.query_info_, src_data, num_queries, timestamp_, ptr, ret);
    } else {
        QueryBruteForceImpl(*segment, node.query_info_, src_data, num_queries, timestamp_, std::nullopt, ret);
    }
    ret_ = ret;
}

void
ExecPlanNodeVisitor::visit(BinaryVectorANNS& node) {
    // TODO
}

}  // namespace milvus::query
