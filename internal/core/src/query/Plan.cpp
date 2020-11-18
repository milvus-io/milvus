#include "PlanImpl.h"
#include "utils/Json.h"
#include "PlanNode.h"
#include "utils/EasyAssert.h"
#include "pb/service_msg.pb.h"
#include "ExprImpl.h"
#include <vector>
#include <memory>
#include <boost/align/aligned_allocator.hpp>

namespace milvus::query {

static std::unique_ptr<VectorPlanNode>
ParseVecNode(Plan* plan, const Json& out_body) {
    // TODO add binary info
    auto vec_node = std::make_unique<FloatVectorANNS>();
    Assert(out_body.size() == 1);
    auto iter = out_body.begin();
    std::string field_name = iter.key();
    auto& vec_info = iter.value();
    auto topK = vec_info["topk"];
    vec_node->query_info_.topK_ = topK;
    vec_node->query_info_.metric_type_ = vec_info["metric_type"];
    vec_node->query_info_.search_params_ = vec_info["params"];
    vec_node->query_info_.field_id_ = field_name;
    vec_node->placeholder_tag_ = vec_info["query"];
    auto tag = vec_node->placeholder_tag_;
    AssertInfo(!plan->tag2field_.count(tag), "duplicated placeholder tag");
    plan->tag2field_.emplace(tag, field_name);
    return vec_node;
}

/// initialize RangeExpr::mapping_
const std::map<std::string, RangeExpr::OpType> RangeExpr::mapping_ = {
    {"lt", OpType::LessThan},    {"le", OpType::LessEqual},    {"lte", OpType::LessEqual},
    {"gt", OpType::GreaterThan}, {"ge", OpType::GreaterEqual}, {"gte", OpType::GreaterEqual},
    {"eq", OpType::Equal},       {"ne", OpType::NotEqual},
};

static inline std::string
to_lower(const std::string& raw) {
    auto data = raw;
    std::transform(data.begin(), data.end(), data.begin(), [](unsigned char c) { return std::tolower(c); });
    return data;
};

template <typename T>
std::unique_ptr<Expr>
ParseRangeNodeImpl(const Schema& schema, const std::string& field_name, const Json& body) {
    auto expr = std::make_unique<RangeExprImpl<T>>();
    auto data_type = schema[field_name].get_data_type();
    expr->data_type_ = data_type;
    expr->field_id_ = field_name;
    for (auto& item : body.items()) {
        auto& op_name = item.key();
        auto op = RangeExpr::mapping_.at(to_lower(op_name));
        T value = item.value();
        expr->conditions_.emplace_back(op, value);
    }
    return expr;
}

std::unique_ptr<Expr>
ParseRangeNode(const Schema& schema, const Json& out_body) {
    Assert(out_body.size() == 1);
    auto out_iter = out_body.begin();
    auto field_name = out_iter.key();
    auto body = out_iter.value();
    auto data_type = schema[field_name].get_data_type();
    Assert(!field_is_vector(data_type));
    switch (data_type) {
        case DataType::BOOL:
            return ParseRangeNodeImpl<bool>(schema, field_name, body);
        case DataType::INT8:
            return ParseRangeNodeImpl<int8_t>(schema, field_name, body);
        case DataType::INT16:
            return ParseRangeNodeImpl<int16_t>(schema, field_name, body);
        case DataType::INT32:
            return ParseRangeNodeImpl<int32_t>(schema, field_name, body);
        case DataType::INT64:
            return ParseRangeNodeImpl<int64_t>(schema, field_name, body);
        case DataType::FLOAT:
            return ParseRangeNodeImpl<float>(schema, field_name, body);
        case DataType::DOUBLE:
            return ParseRangeNodeImpl<double>(schema, field_name, body);
        default:
            PanicInfo("unsupported");
    }
}

static std::unique_ptr<Plan>
CreatePlanImplNaive(const Schema& schema, const std::string& dsl_str) {
    auto plan = std::make_unique<Plan>(schema);
    auto dsl = nlohmann::json::parse(dsl_str);
    nlohmann::json vec_pack;
    std::optional<std::unique_ptr<Expr>> predicate;

    auto& bool_dsl = dsl["bool"];
    if (bool_dsl.contains("must")) {
        auto& packs = bool_dsl["must"];
        for (auto& pack : packs) {
            if (pack.contains("vector")) {
                auto& out_body = pack["vector"];
                plan->plan_node_ = ParseVecNode(plan.get(), out_body);
            } else if (pack.contains("range")) {
                AssertInfo(!predicate, "unsupported complex DSL");
                auto& out_body = pack["range"];
                predicate = ParseRangeNode(schema, out_body);
            } else {
                PanicInfo("unsupported node");
            }
        }
        AssertInfo(plan->plan_node_, "vector node not found");
    } else if (bool_dsl.contains("vector")) {
        auto& out_body = bool_dsl["vector"];
        plan->plan_node_ = ParseVecNode(plan.get(), out_body);
        Assert(plan->plan_node_);
    } else {
        PanicInfo("Unsupported DSL");
    }
    plan->plan_node_->predicate_ = std::move(predicate);
    return plan;
}

std::unique_ptr<Plan>
CreatePlan(const Schema& schema, const std::string& dsl_str) {
    auto plan = CreatePlanImplNaive(schema, dsl_str);
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
