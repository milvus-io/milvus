// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "Plan.h"
#include "PlanProto.h"
#include "generated/ShowPlanNodeVisitor.h"

namespace milvus::query {

// deprecated
std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan,
                      const std::string_view placeholder_group_blob) {
    return ParsePlaceholderGroup(
        plan,
        reinterpret_cast<const uint8_t*>(placeholder_group_blob.data()),
        placeholder_group_blob.size());
}

std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan,
                      const uint8_t* blob,
                      const int64_t blob_len) {
    namespace set = milvus::proto::common;
    auto result = std::make_unique<PlaceholderGroup>();
    set::PlaceholderGroup ph_group;
    auto ok = ph_group.ParseFromArray(blob, blob_len);
    Assert(ok);
    for (auto& info : ph_group.placeholders()) {
        Placeholder element;
        element.tag_ = info.tag();
        Assert(plan->tag2field_.count(element.tag_));
        auto field_id = plan->tag2field_.at(element.tag_);
        auto& field_meta = plan->schema_[field_id];
        element.num_of_queries_ = info.values_size();
        AssertInfo(element.num_of_queries_, "must have queries");
        Assert(element.num_of_queries_ > 0);
        element.line_sizeof_ = info.values().Get(0).size();
        AssertInfo(field_meta.get_sizeof() == element.line_sizeof_,
                   "vector dimension mismatch");
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
CreateSearchPlanByExpr(const Schema& schema,
                       const void* serialized_expr_plan,
                       const int64_t size) {
    // Note: serialized_expr_plan is of binary format
    proto::plan::PlanNode plan_node;
    plan_node.ParseFromArray(serialized_expr_plan, size);
    return ProtoParser(schema).CreatePlan(plan_node);
}

std::unique_ptr<RetrievePlan>
CreateRetrievePlanByExpr(const Schema& schema,
                         const void* serialized_expr_plan,
                         const int64_t size) {
    proto::plan::PlanNode plan_node;
    plan_node.ParseFromArray(serialized_expr_plan, size);
    return ProtoParser(schema).CreateRetrievePlan(plan_node);
}

int64_t
GetTopK(const Plan* plan) {
    return plan->plan_node_->search_info_.topk_;
}

int64_t
GetFieldID(const Plan* plan) {
    return plan->plan_node_->search_info_.field_id_.get();
}

int64_t
GetNumOfQueries(const PlaceholderGroup* group) {
    return group->at(0).num_of_queries_;
}

// std::unique_ptr<RetrievePlan>
// CreateRetrievePlan(const Schema& schema, proto::segcore::RetrieveRequest&& request) {
//    auto plan = std::make_unique<RetrievePlan>();
//    plan->seg_offsets_ = std::unique_ptr<proto::schema::IDs>(request.release_ids());
//    for (auto& field_id : request.output_fields_id()) {
//        plan->field_ids_.push_back(schema.get_offset(FieldId(field_id)));
//    }
//    return plan;
//}

void
Plan::check_identical(Plan& other) {
    Assert(&schema_ == &other.schema_);
    auto json = ShowPlanNodeVisitor().call_child(*this->plan_node_);
    auto other_json = ShowPlanNodeVisitor().call_child(*other.plan_node_);
    Assert(json.dump(2) == other_json.dump(2));
    Assert(this->extra_info_opt_.has_value() ==
           other.extra_info_opt_.has_value());
    if (this->extra_info_opt_.has_value()) {
        Assert(this->extra_info_opt_->involved_fields_ ==
               other.extra_info_opt_->involved_fields_);
    }
    Assert(this->tag2field_ == other.tag2field_);
    Assert(this->target_entries_ == other.target_entries_);
}

}  // namespace milvus::query
