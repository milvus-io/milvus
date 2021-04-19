#include "PlanImpl.h"
#include "utils/Json.h"
#include "PlanNode.h"
#include "utils/EasyAssert.h"
#include "pb/service_msg.pb.h"
#include <vector>
#include <boost/align/aligned_allocator.hpp>

namespace milvus::query {

static std::unique_ptr<VectorPlanNode>
CreateVecNode(const std::string& field_name, const Json& vec_info) {
    // TODO add binary info
    auto vec_node = std::make_unique<FloatVectorANNS>();
    auto topK = vec_info["topk"];
    vec_node->query_info_.topK_ = topK;
    vec_node->query_info_.metric_type_ = vec_info["metric_type"];
    vec_node->query_info_.search_params_ = vec_info["params"];
    vec_node->query_info_.field_id_ = field_name;
    vec_node->placeholder_tag_ = vec_info["query"];
    return vec_node;
}

static std::unique_ptr<Plan>
CreatePlanImplNaive(const std::string& dsl_str) {
    auto plan = std::make_unique<Plan>();
    auto dsl = nlohmann::json::parse(dsl_str);
    nlohmann::json vec_pack;

    auto& bool_dsl = dsl["bool"];
    if (bool_dsl.contains("must")) {
        auto& packs = bool_dsl["must"];
        for (auto& pack : packs) {
            if (pack.contains("vector")) {
                auto iter = pack["vector"].begin();
                auto key = iter.key();
                auto& body = iter.value();
                plan->plan_node_ = CreateVecNode(key, body);
                return plan;
            }
        }
        PanicInfo("Unsupported DSL: vector node not detected");
    } else if (bool_dsl.contains("vector")) {
        auto iter = bool_dsl["vector"].begin();
        auto key = iter.key();
        auto& body = iter.value();
        plan->plan_node_ = CreateVecNode(key, body);
        return plan;
    } else {
        PanicInfo("Unsupported DSL: vector node not detected");
    }
}

static std::unique_ptr<Plan>
CreateRangeNode(const Json& range_group) {
    return nullptr;
}

void
CheckNull(const Json& json) {
    Assert(!json.is_null());
}

class PlanParser {
 public:
    void
    ParseBoolBody(const Json& dsl) {
        CheckNull(dsl);
        for (const auto& item : dsl.items()) {
            PanicInfo("unimplemented");
        }
    }

    void
    CreatePlanImpl(const Json& dsl) {
        if (dsl.empty()) {
            PanicInfo("DSL Is Empty or Invalid");
        }
        if (!dsl.contains("bool")) {
            auto bool_dsl = dsl["bool"];
            ParseBoolBody(bool_dsl);
        }
        PanicInfo("unimplemented");
    }

 private:
};

std::unique_ptr<Plan>
CreatePlan(const Schema& schema, const std::string& dsl_str) {
    (void)schema;
    auto plan = CreatePlanImplNaive(dsl_str);
    return plan;
}

std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan, const std::string& blob) {
    (void)plan;
    namespace ser = milvus::proto::service;
    auto result = std::make_unique<PlaceholderGroup>();
    ser::PlaceholderGroup ph_group;
    auto ok = ph_group.ParseFromString(blob);
    Assert(ok);
    for (auto& info : ph_group.placeholders()) {
        Placeholder element;
        element.tag_ = info.tag();
        element.num_of_queries_ = info.values_size();
        AssertInfo(element.num_of_queries_, "must have queries");
        element.line_sizeof_ = info.values().Get(0).size();
        auto& target = element.blob_;
        target.reserve(element.line_sizeof_ * element.num_of_queries_);
        for (auto& line : info.values()) {
            Assert(element.line_sizeof_ == line.size());
            target.insert(target.end(), line.begin(), line.end());
        }
        result->emplace_back(std::move(element));
    }
    return result;
}

int64_t
GetTopK(const Plan* plan) {
    return plan->plan_node_->query_info_.topK_;
}

int64_t
GetNumOfQueries(const PlaceholderGroup* group) {
    return group->at(0).num_of_queries_;
}

}  // namespace milvus::query
