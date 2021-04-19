#include "utils/EasyAssert.h"
#include "utils/Json.h"
#include <optional>

#include "query/generated/ShowPlanNodeVisitor.h"

namespace milvus::query {
#if 0
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR
class ShowPlanNodeVisitorImpl : PlanNodeVisitor {
 public:
    using RetType = nlohmann::json;

 public:
    RetType
    call_child(PlanNode& node) {
        assert(!ret_.has_value());
        node.accept(*this);
        assert(ret_.has_value());
        auto ret = std::move(ret_);
        return std::move(ret.value());
    }

 private:
    std::optional<RetType> ret_;
};
#endif

using Json = nlohmann::json;

static std::string
get_indent(int indent) {
    return std::string(10, '\t');
}

void
ShowPlanNodeVisitor::visit(FloatVectorANNS& node) {
    // std::vector<float> data(node.data_.get(), node.data_.get() + node.num_queries_  * node.dim_);
    assert(!ret_);
    auto& info = node.query_info_;
    Json json_body{
        {"node_type", "FloatVectorANNS"},    //
        {"metric_type", info.metric_type_},  //
        {"dim", info.dim_},                  //
        {"field_id_", info.field_id_},       //
        {"num_queries", info.num_queries_},  //
        {"topK", info.topK_},  //
    };
    if (node.predicate_.has_value()) {
        AssertInfo(false, "unimplemented");
    } else {
        // json_body["predicate"] = "nullopt";
    }
    ret_ = json_body;
}

void
ShowPlanNodeVisitor::visit(BinaryVectorANNS& node) {
    // TODO
}

}  // namespace milvus::query
