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

#pragma once

#include <boost/dynamic_bitset.hpp>
#include <memory>

#include "Plan.h"
#include "PlanNode.h"
#include "common/Schema.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"

namespace milvus::query {

class ProtoParser {
 public:
    using TypeCheckFunction = std::function<bool(const DataType)>;
    static bool
    TypeIsBool(const DataType type) {
        return type == DataType::BOOL;
    }
    static bool
    TypeIsAny(const DataType) {
        return true;
    }

 public:
    explicit ProtoParser(SchemaPtr schema) : schema(std::move(schema)) {
    }

    std::unique_ptr<VectorPlanNode>
    PlanNodeFromProto(const proto::plan::PlanNode& plan_node_proto);

    std::unique_ptr<RetrievePlanNode>
    RetrievePlanNodeFromProto(const proto::plan::PlanNode& plan_node_proto);

    std::unique_ptr<Plan>
    CreatePlan(const proto::plan::PlanNode& plan_node_proto);

    std::unique_ptr<RetrievePlan>
    CreateRetrievePlan(const proto::plan::PlanNode& plan_node_proto);

    expr::TypedExprPtr
    ParseExprs(const proto::plan::Expr& expr_pb,
               TypeCheckFunction type_check = TypeIsBool);

    std::shared_ptr<rescores::Scorer>
    ParseScorer(const proto::plan::ScoreFunction& function);

    // Shared-filter search, phase 1: return VectorSearchNode's source subtree
    // (the filter prefix) from a plan whose chain above VectorSearchNode is
    // exactly what RebindToPrecomputedBitset can rebuild -- nothing, or the
    // post-search nodes this branch builds (GroupByNode, RescoresNode). Any
    // other shape throws.
    //
    // An iterative-filter plan puts its predicate *above* VectorSearchNode
    // (FilterNode -> VectorSearchNode -> MvccNode), so a lenient walk would
    // return a bare MvccNode and phase 1 would compute an MVCC-only bitset
    // that carries no predicate at all. Rejecting here fails the group before
    // any work is done and says why. A plan with no predicate is a different
    // case: its prefix is legitimately just MvccNode and is accepted.
    static std::shared_ptr<plan::PlanNode>
    ExtractSharedFilterPrefix(const std::shared_ptr<plan::PlanNode>& root_node);

    // Shared-filter search, phase 2: rebuild `root_node` with VectorSearchNode's
    // source subtree replaced by a PrecomputedBitsetNode, so the branch
    // executes only its vector search against a bitset computed earlier.
    //
    // The replacement point is exactly the extraction point of
    // ExtractSharedFilterPrefix, and both accept exactly the same chain above
    // VectorSearchNode. Anything else means the caller grouped a plan shape
    // that must not have been grouped, and throws.
    static std::shared_ptr<plan::PlanNode>
    RebindToPrecomputedBitset(const std::shared_ptr<plan::PlanNode>& root_node);

 private:
    expr::TypedExprPtr
    CreateAlwaysTrueExprs();

    expr::TypedExprPtr
    ParseBinaryExprs(const proto::plan::BinaryExpr& expr_pb);

    expr::TypedExprPtr
    ParseBinaryArithOpEvalRangeExprs(
        const proto::plan::BinaryArithOpEvalRangeExpr& expr_pb);

    expr::TypedExprPtr
    ParseBinaryRangeExprs(const proto::plan::BinaryRangeExpr& expr_pb);

    expr::TypedExprPtr
    ParseCallExprs(const proto::plan::CallExpr& expr_pb);

    expr::TypedExprPtr
    ParseColumnExprs(const proto::plan::ColumnExpr& expr_pb);

    expr::TypedExprPtr
    ParseCompareExprs(const proto::plan::CompareExpr& expr_pb);

    expr::TypedExprPtr
    ParseExistExprs(const proto::plan::ExistsExpr& expr_pb);

    expr::TypedExprPtr
    ParseNullExprs(const proto::plan::NullExpr& expr_pb);

    expr::TypedExprPtr
    ParseJsonContainsExprs(const proto::plan::JSONContainsExpr& expr_pb);

    expr::TypedExprPtr
    ParseGISFunctionFilterExprs(
        const proto::plan::GISFunctionFilterExpr& expr_pb);

    expr::TypedExprPtr
    ParseTermExprs(const proto::plan::TermExpr& expr_pb);

    expr::TypedExprPtr
    ParseUnaryExprs(const proto::plan::UnaryExpr& expr_pb);

    expr::TypedExprPtr
    ParseUnaryRangeExprs(const proto::plan::UnaryRangeExpr& expr_pb);

    expr::TypedExprPtr
    ParseTimestamptzArithCompareExprs(
        const proto::plan::TimestamptzArithCompareExpr& expr_pb);

    expr::TypedExprPtr
    ParseValueExprs(const proto::plan::ValueExpr& expr_pb);

    void
    PlanOptionsFromProto(const proto::plan::PlanOption& plan_option_proto,
                         PlanOptions& plan_options);

 private:
    const SchemaPtr schema;
};

}  // namespace milvus::query

template <>
struct fmt::formatter<milvus::proto::plan::GenericValue::ValCase>
    : formatter<string_view> {
    auto
    format(milvus::proto::plan::GenericValue::ValCase c,
           format_context& ctx) const {
        string_view name = "unknown";
        switch (c) {
            case milvus::proto::plan::GenericValue::ValCase::kBoolVal:
                name = "kBoolVal";
                break;
            case milvus::proto::plan::GenericValue::ValCase::kInt64Val:
                name = "kInt64Val";
                break;
            case milvus::proto::plan::GenericValue::ValCase::kFloatVal:
                name = "kFloatVal";
                break;
            case milvus::proto::plan::GenericValue::ValCase::kStringVal:
                name = "kStringVal";
                break;
            case milvus::proto::plan::GenericValue::ValCase::kArrayVal:
                name = "kArrayVal";
                break;
            case milvus::proto::plan::GenericValue::ValCase::VAL_NOT_SET:
                name = "VAL_NOT_SET";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
