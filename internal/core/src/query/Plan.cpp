// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/ExprImpl.h"
#include "utils/Json.h"
#include "utils/EasyAssert.h"
#include "pb/service_msg.pb.h"
#include <vector>
#include <memory>
#include <boost/align/aligned_allocator.hpp>
#include <boost/algorithm/string.hpp>
#include <algorithm>
#include "query/generated/VerifyPlanNodeVisitor.h"

namespace milvus::query {

/// initialize RangeExpr::mapping_
const std::map<std::string, RangeExpr::OpType> RangeExpr::mapping_ = {
    {"lt", OpType::LessThan},    {"le", OpType::LessEqual},    {"lte", OpType::LessEqual},
    {"gt", OpType::GreaterThan}, {"ge", OpType::GreaterEqual}, {"gte", OpType::GreaterEqual},
    {"eq", OpType::Equal},       {"ne", OpType::NotEqual},
};

// static inline std::string
// to_lower(const std::string& raw) {
//    auto data = raw;
//    std::transform(data.begin(), data.end(), data.begin(), [](unsigned char c) { return std::tolower(c); });
//    return data;
//}

class Parser {
 public:
    friend std::unique_ptr<Plan>
    CreatePlan(const Schema& schema, const std::string& dsl_str);

 private:
    std::unique_ptr<Plan>
    CreatePlanImpl(const std::string& dsl_str);

    explicit Parser(const Schema& schema) : schema(schema) {
    }

    // vector node parser, should be called exactly once per pass.
    std::unique_ptr<VectorPlanNode>
    ParseVecNode(const Json& out_body);

    // Dispatcher of all parse function
    // NOTE: when nullptr, it is a pure vector node
    ExprPtr
    ParseAnyNode(const Json& body);

    ExprPtr
    ParseMustNode(const Json& body);

    ExprPtr
    ParseShouldNode(const Json& body);

    ExprPtr
    ParseShouldNotNode(const Json& body);

    // parse the value of "should"/"must"/"should_not" entry
    std::vector<ExprPtr>
    ParseItemList(const Json& body);

    // parse the value of "range" entry
    ExprPtr
    ParseRangeNode(const Json& out_body);

    // parse the value of "term" entry
    ExprPtr
    ParseTermNode(const Json& out_body);

 private:
    // template implementation of leaf parser
    // used by corresponding parser

    template <typename T>
    ExprPtr
    ParseRangeNodeImpl(const FieldName& field_name, const Json& body);

    template <typename T>
    ExprPtr
    ParseTermNodeImpl(const FieldName& field_name, const Json& body);

 private:
    const Schema& schema;
    std::map<std::string, FieldOffset> tag2field_;  // PlaceholderName -> field offset
    std::optional<std::unique_ptr<VectorPlanNode>> vector_node_opt_;
};

ExprPtr
Parser::ParseRangeNode(const Json& out_body) {
    Assert(out_body.is_object());
    Assert(out_body.size() == 1);
    auto out_iter = out_body.begin();
    auto field_name = FieldName(out_iter.key());
    auto body = out_iter.value();
    auto data_type = schema[field_name].get_data_type();
    Assert(!datatype_is_vector(data_type));

    switch (data_type) {
        case DataType::BOOL:
            return ParseRangeNodeImpl<bool>(field_name, body);
        case DataType::INT8:
            return ParseRangeNodeImpl<int8_t>(field_name, body);
        case DataType::INT16:
            return ParseRangeNodeImpl<int16_t>(field_name, body);
        case DataType::INT32:
            return ParseRangeNodeImpl<int32_t>(field_name, body);
        case DataType::INT64:
            return ParseRangeNodeImpl<int64_t>(field_name, body);
        case DataType::FLOAT:
            return ParseRangeNodeImpl<float>(field_name, body);
        case DataType::DOUBLE:
            return ParseRangeNodeImpl<double>(field_name, body);
        default:
            PanicInfo("unsupported");
    }
}

std::unique_ptr<Plan>
Parser::CreatePlanImpl(const std::string& dsl_str) {
    auto dsl = Json::parse(dsl_str);
    auto bool_dsl = dsl.at("bool");
    auto predicate = ParseAnyNode(bool_dsl);
    Assert(vector_node_opt_.has_value());
    auto vec_node = std::move(vector_node_opt_).value();
    if (predicate != nullptr) {
        vec_node->predicate_ = std::move(predicate);
    }
    VerifyPlanNodeVisitor verifier;
    vec_node->accept(verifier);

    auto plan = std::make_unique<Plan>(schema);
    plan->tag2field_ = std::move(tag2field_);
    plan->plan_node_ = std::move(vec_node);
    return plan;
}

ExprPtr
Parser::ParseTermNode(const Json& out_body) {
    Assert(out_body.size() == 1);
    auto out_iter = out_body.begin();
    auto field_name = FieldName(out_iter.key());
    auto body = out_iter.value();
    auto data_type = schema[field_name].get_data_type();
    Assert(!datatype_is_vector(data_type));
    switch (data_type) {
        case DataType::BOOL: {
            return ParseTermNodeImpl<bool>(field_name, body);
        }
        case DataType::INT8: {
            return ParseTermNodeImpl<int8_t>(field_name, body);
        }
        case DataType::INT16: {
            return ParseTermNodeImpl<int16_t>(field_name, body);
        }
        case DataType::INT32: {
            return ParseTermNodeImpl<int32_t>(field_name, body);
        }
        case DataType::INT64: {
            return ParseTermNodeImpl<int64_t>(field_name, body);
        }
        case DataType::FLOAT: {
            return ParseTermNodeImpl<float>(field_name, body);
        }
        case DataType::DOUBLE: {
            return ParseTermNodeImpl<double>(field_name, body);
        }
        default: {
            PanicInfo("unsupported data_type");
        }
    }
}

std::unique_ptr<VectorPlanNode>
Parser::ParseVecNode(const Json& out_body) {
    Assert(out_body.is_object());
    Assert(out_body.size() == 1);
    auto iter = out_body.begin();
    auto field_name = FieldName(iter.key());

    auto& vec_info = iter.value();
    Assert(vec_info.is_object());
    auto topK = vec_info["topk"];
    AssertInfo(topK > 0, "topK must greater than 0");
    AssertInfo(topK < 16384, "topK is too large");

    auto field_offset = schema.get_offset(field_name);

    auto vec_node = [&]() -> std::unique_ptr<VectorPlanNode> {
        auto field_meta = schema.operator[](field_name);
        auto data_type = field_meta.get_data_type();
        if (data_type == DataType::VECTOR_FLOAT) {
            return std::make_unique<FloatVectorANNS>();
        } else {
            return std::make_unique<BinaryVectorANNS>();
        }
    }();
    vec_node->query_info_.topK_ = topK;
    vec_node->query_info_.metric_type_ = vec_info.at("metric_type");
    vec_node->query_info_.search_params_ = vec_info.at("params");
    vec_node->query_info_.field_offset_ = field_offset;
    vec_node->placeholder_tag_ = vec_info.at("query");
    auto tag = vec_node->placeholder_tag_;
    AssertInfo(!tag2field_.count(tag), "duplicated placeholder tag");
    tag2field_.emplace(tag, field_offset);
    return vec_node;
}

template <typename T>
ExprPtr
Parser::ParseTermNodeImpl(const FieldName& field_name, const Json& body) {
    auto expr = std::make_unique<TermExprImpl<T>>();
    auto field_offset = schema.get_offset(field_name);
    auto data_type = schema[field_name].get_data_type();
    Assert(body.is_object());
    auto values = body["values"];

    expr->field_offset_ = field_offset;
    expr->data_type_ = data_type;
    for (auto& value : values) {
        if constexpr (std::is_same_v<T, bool>) {
            Assert(value.is_boolean());
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value.is_number_integer());
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value.is_number());
        } else {
            static_assert(always_false<T>, "unsupported type");
            __builtin_unreachable();
        }
        T real_value = value;
        expr->terms_.push_back(real_value);
    }
    std::sort(expr->terms_.begin(), expr->terms_.end());
    return expr;
}

template <typename T>
ExprPtr
Parser::ParseRangeNodeImpl(const FieldName& field_name, const Json& body) {
    auto expr = std::make_unique<RangeExprImpl<T>>();
    auto field_meta = schema[field_name];
    auto data_type = field_meta.get_data_type();
    expr->data_type_ = data_type;
    expr->field_offset_ = schema.get_offset(field_name);
    Assert(body.is_object());
    for (auto& item : body.items()) {
        auto op_name = boost::algorithm::to_lower_copy(std::string(item.key()));

        AssertInfo(RangeExpr::mapping_.count(op_name), "op(" + op_name + ") not found");
        auto op = RangeExpr::mapping_.at(op_name);
        if constexpr (std::is_same_v<T, bool>) {
            Assert(item.value().is_boolean());
        } else if constexpr (std::is_integral_v<T>) {
            Assert(item.value().is_number_integer());
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(item.value().is_number());
        } else {
            static_assert(always_false<T>, "unsupported type");
            __builtin_unreachable();
        }
        T value = item.value();
        expr->conditions_.emplace_back(op, value);
    }
    std::sort(expr->conditions_.begin(), expr->conditions_.end());
    return expr;
}

std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan, const std::string& blob) {
    namespace ser = milvus::proto::service;
    auto result = std::make_unique<PlaceholderGroup>();
    ser::PlaceholderGroup ph_group;
    auto ok = ph_group.ParseFromString(blob);
    Assert(ok);
    for (auto& info : ph_group.placeholders()) {
        Placeholder element;
        element.tag_ = info.tag();
        Assert(plan->tag2field_.count(element.tag_));
        auto field_offset = plan->tag2field_.at(element.tag_);
        auto& field_meta = plan->schema_[field_offset];
        element.num_of_queries_ = info.values_size();
        AssertInfo(element.num_of_queries_, "must have queries");
        Assert(element.num_of_queries_ > 0);
        element.line_sizeof_ = info.values().Get(0).size();
        Assert(field_meta.get_sizeof() == element.line_sizeof_);
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

std::unique_ptr<Plan>
CreatePlan(const Schema& schema, const std::string& dsl_str) {
    auto plan = Parser(schema).CreatePlanImpl(dsl_str);
    return plan;
}

std::vector<ExprPtr>
Parser::ParseItemList(const Json& body) {
    std::vector<ExprPtr> results;
    if (body.is_object()) {
        // only one item;
        auto new_entry = ParseAnyNode(body);
        results.emplace_back(std::move(new_entry));
    } else {
        // item array
        Assert(body.is_array());
        for (auto& item : body) {
            auto new_entry = ParseAnyNode(item);
            results.emplace_back(std::move(new_entry));
        }
    }
    auto old_size = results.size();

    auto new_end = std::remove_if(results.begin(), results.end(), [](const ExprPtr& x) { return x == nullptr; });

    results.resize(new_end - results.begin());

    return results;
}

ExprPtr
Parser::ParseAnyNode(const Json& out_body) {
    Assert(out_body.is_object());
    Assert(out_body.size() == 1);

    auto out_iter = out_body.begin();

    auto key = out_iter.key();
    auto body = out_iter.value();

    if (key == "must") {
        return ParseMustNode(body);
    } else if (key == "should") {
        return ParseShouldNode(body);
    } else if (key == "should_not") {
        return ParseShouldNotNode(body);
    } else if (key == "range") {
        return ParseRangeNode(body);
    } else if (key == "term") {
        return ParseTermNode(body);
    } else if (key == "vector") {
        auto vec_node = ParseVecNode(body);
        Assert(!vector_node_opt_.has_value());
        vector_node_opt_ = std::move(vec_node);
        return nullptr;
    } else {
        PanicInfo("unsupported key: " + key);
    }
}

template <typename Merger>
static ExprPtr
ConstructTree(Merger merger, std::vector<ExprPtr> item_list) {
    if (item_list.size() == 0) {
        return nullptr;
    }

    if (item_list.size() == 1) {
        return std::move(item_list[0]);
    }

    // Note: use deque to construct a binary tree
    //     Op
    //   /    \
    // Op     Op
    // | \    | \
    // A  B   C D
    std::deque<ExprPtr> binary_queue;
    for (auto& item : item_list) {
        Assert(item != nullptr);
        binary_queue.push_back(std::move(item));
    }
    while (binary_queue.size() > 1) {
        auto left = std::move(binary_queue.front());
        binary_queue.pop_front();
        auto right = std::move(binary_queue.front());
        binary_queue.pop_front();
        binary_queue.push_back(merger(std::move(left), std::move(right)));
    }
    Assert(binary_queue.size() == 1);
    return std::move(binary_queue.front());
}

ExprPtr
Parser::ParseMustNode(const Json& body) {
    auto item_list = ParseItemList(body);
    auto merger = [](ExprPtr left, ExprPtr right) {
        using OpType = BoolBinaryExpr::OpType;
        auto res = std::make_unique<BoolBinaryExpr>();
        res->op_type_ = OpType::LogicalAnd;
        res->left_ = std::move(left);
        res->right_ = std::move(right);
        return res;
    };
    return ConstructTree(merger, std::move(item_list));
}

ExprPtr
Parser::ParseShouldNode(const Json& body) {
    auto item_list = ParseItemList(body);
    Assert(item_list.size() >= 1);
    auto merger = [](ExprPtr left, ExprPtr right) {
        using OpType = BoolBinaryExpr::OpType;
        auto res = std::make_unique<BoolBinaryExpr>();
        res->op_type_ = OpType::LogicalOr;
        res->left_ = std::move(left);
        res->right_ = std::move(right);
        return res;
    };
    return ConstructTree(merger, std::move(item_list));
}

ExprPtr
Parser::ParseShouldNotNode(const Json& body) {
    auto item_list = ParseItemList(body);
    Assert(item_list.size() >= 1);
    auto merger = [](ExprPtr left, ExprPtr right) {
        using OpType = BoolBinaryExpr::OpType;
        auto res = std::make_unique<BoolBinaryExpr>();
        res->op_type_ = OpType::LogicalAnd;
        res->left_ = std::move(left);
        res->right_ = std::move(right);
        return res;
    };
    auto subtree = ConstructTree(merger, std::move(item_list));

    using OpType = BoolUnaryExpr::OpType;
    auto res = std::make_unique<BoolUnaryExpr>();
    res->op_type_ = OpType::LogicalNot;
    res->child_ = std::move(subtree);

    return res;
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
