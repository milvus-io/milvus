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
#include "query/generated/ExtractInfoExprVisitor.h"

namespace milvus::query {

namespace impl {
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR UNDER suvlim/core_gen/
class ExtractInfoExprVisitor : ExprVisitor {
 public:
    explicit ExtractInfoExprVisitor(ExtractedPlanInfo& plan_info) : plan_info_(plan_info) {
    }

 private:
    ExtractedPlanInfo& plan_info_;
};
}  // namespace impl

void
ExtractInfoExprVisitor::visit(LogicalUnaryExpr& expr) {
    expr.child_->accept(*this);
}

void
ExtractInfoExprVisitor::visit(LogicalBinaryExpr& expr) {
    expr.left_->accept(*this);
    expr.right_->accept(*this);
}

void
ExtractInfoExprVisitor::visit(TermExpr& expr) {
    plan_info_.add_involved_field(expr.field_id_);
}

void
ExtractInfoExprVisitor::visit(UnaryRangeExpr& expr) {
    plan_info_.add_involved_field(expr.field_id_);
}

void
ExtractInfoExprVisitor::visit(BinaryRangeExpr& expr) {
    plan_info_.add_involved_field(expr.field_id_);
}

void
ExtractInfoExprVisitor::visit(CompareExpr& expr) {
    plan_info_.add_involved_field(expr.left_field_id_);
    plan_info_.add_involved_field(expr.right_field_id_);
}

void
ExtractInfoExprVisitor::visit(BinaryArithOpEvalRangeExpr& expr) {
    plan_info_.add_involved_field(expr.field_id_);
}

}  // namespace milvus::query
