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
#include "query/generated/ExtractInfoPlanNodeVisitor.h"
#include "query/generated/ExtractInfoExprVisitor.h"

namespace milvus::query {

#if 1
namespace impl {
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR UNDER suvlim/core_gen/
class ExtractInfoPlanNodeVisitor : PlanNodeVisitor {
 public:
    explicit ExtractInfoPlanNodeVisitor(ExtractedPlanInfo& plan_info) : plan_info_(plan_info) {
    }

 private:
    ExtractedPlanInfo& plan_info_;
};
}  // namespace impl
#endif

void
ExtractInfoPlanNodeVisitor::visit(FloatVectorANNS& node) {
    plan_info_.add_involved_field(node.search_info_.field_offset_);
    if (node.predicate_.has_value()) {
        ExtractInfoExprVisitor expr_visitor(plan_info_);
        node.predicate_.value()->accept(expr_visitor);
    }
}

void
ExtractInfoPlanNodeVisitor::visit(BinaryVectorANNS& node) {
    plan_info_.add_involved_field(node.search_info_.field_offset_);
    if (node.predicate_.has_value()) {
        ExtractInfoExprVisitor expr_visitor(plan_info_);
        node.predicate_.value()->accept(expr_visitor);
    }
}

void
ExtractInfoPlanNodeVisitor::visit(RetrievePlanNode& node) {
    // Assert(node.predicate_.has_value());
    ExtractInfoExprVisitor expr_visitor(plan_info_);
    node.predicate_->accept(expr_visitor);
}

}  // namespace milvus::query
