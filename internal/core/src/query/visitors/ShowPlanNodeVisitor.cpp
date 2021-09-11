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

#include "exceptions/EasyAssert.h"
#include "utils/Json.h"
#include <optional>
#include <utility>

#include "query/generated/ShowPlanNodeVisitor.h"
#include "query/generated/ShowExprVisitor.h"

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
        ret_ = std::nullopt;
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
    auto& info = node.search_info_;
    Json json_body{
        {"node_type", "FloatVectorANNS"},                      //
        {"metric_type", MetricTypeToName(info.metric_type_)},  //
        {"field_offset_", info.field_offset_.get()},           //
        {"topk", info.topk_},                                  //
        {"search_params", info.search_params_},                //
        {"placeholder_tag", node.placeholder_tag_},            //
    };
    if (node.predicate_.has_value()) {
        ShowExprVisitor expr_show;
        Assert(node.predicate_.value());
        json_body["predicate"] = expr_show.call_child(node.predicate_->operator*());
    } else {
        json_body["predicate"] = "None";
    }
    ret_ = json_body;
}

void
ShowPlanNodeVisitor::visit(BinaryVectorANNS& node) {
    assert(!ret_);
    auto& info = node.search_info_;
    Json json_body{
        {"node_type", "BinaryVectorANNS"},                     //
        {"metric_type", MetricTypeToName(info.metric_type_)},  //
        {"field_offset_", info.field_offset_.get()},           //
        {"topk", info.topk_},                                  //
        {"search_params", info.search_params_},                //
        {"placeholder_tag", node.placeholder_tag_},            //
    };
    if (node.predicate_.has_value()) {
        ShowExprVisitor expr_show;
        Assert(node.predicate_.value());
        json_body["predicate"] = expr_show.call_child(node.predicate_->operator*());
    } else {
        json_body["predicate"] = "None";
    }
    ret_ = json_body;
}

void
ShowPlanNodeVisitor::visit(RetrievePlanNode& node) {
}

}  // namespace milvus::query
