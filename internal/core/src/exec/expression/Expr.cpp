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

#include "Expr.h"

#include "common/EasyAssert.h"
#include "exec/expression/AlwaysTrueExpr.h"
#include "exec/expression/BinaryArithOpEvalRangeExpr.h"
#include "exec/expression/BinaryRangeExpr.h"
#include "exec/expression/CallExpr.h"
#include "exec/expression/ColumnExpr.h"
#include "exec/expression/CompareExpr.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/ExistsExpr.h"
#include "exec/expression/JsonContainsExpr.h"
#include "exec/expression/LogicalBinaryExpr.h"
#include "exec/expression/LogicalUnaryExpr.h"
#include "exec/expression/NullExpr.h"
#include "exec/expression/TermExpr.h"
#include "exec/expression/UnaryExpr.h"
#include "exec/expression/ValueExpr.h"
#include "expr/ITypeExpr.h"

#include <memory>

namespace milvus {
namespace exec {

void
ExprSet::Eval(int32_t begin,
              int32_t end,
              bool initialize,
              EvalCtx& context,
              std::vector<VectorPtr>& results) {
    results.resize(exprs_.size());

    for (size_t i = begin; i < end; ++i) {
        exprs_[i]->Eval(context, results[i]);
    }
}

std::vector<ExprPtr>
CompileExpressions(const std::vector<expr::TypedExprPtr>& sources,
                   ExecContext* context,
                   const std::unordered_set<std::string>& flatten_candidate,
                   bool enable_constant_folding) {
    std::vector<std::shared_ptr<Expr>> exprs;
    exprs.reserve(sources.size());

    for (auto& source : sources) {
        exprs.emplace_back(CompileExpression(source,
                                             context->get_query_context(),
                                             flatten_candidate,
                                             enable_constant_folding));
    }

    OptimizeCompiledExprs(context, exprs);

    return exprs;
}

static std::optional<std::string>
ShouldFlatten(const expr::TypedExprPtr& expr,
              const std::unordered_set<std::string>& flat_candidates = {}) {
    if (auto call =
            std::dynamic_pointer_cast<const expr::LogicalBinaryExpr>(expr)) {
        if (call->op_type_ == expr::LogicalBinaryExpr::OpType::And ||
            call->op_type_ == expr::LogicalBinaryExpr::OpType::Or) {
            return call->name();
        }
    }
    return std::nullopt;
}

static bool
IsCall(const expr::TypedExprPtr& expr, const std::string& name) {
    if (auto call =
            std::dynamic_pointer_cast<const expr::LogicalBinaryExpr>(expr)) {
        return call->name() == name;
    }
    return false;
}

static bool
AllInputTypeEqual(const expr::TypedExprPtr& expr) {
    const auto& inputs = expr->inputs();
    for (int i = 1; i < inputs.size(); i++) {
        if (inputs[0]->type() != inputs[i]->type()) {
            return false;
        }
    }
    return true;
}

static void
FlattenInput(const expr::TypedExprPtr& input,
             const std::string& flatten_call,
             std::vector<expr::TypedExprPtr>& flat) {
    if (IsCall(input, flatten_call) && AllInputTypeEqual(input)) {
        for (auto& child : input->inputs()) {
            FlattenInput(child, flatten_call, flat);
        }
    } else {
        flat.emplace_back(input);
    }
}

std::vector<ExprPtr>
CompileInputs(const expr::TypedExprPtr& expr,
              QueryContext* context,
              const std::unordered_set<std::string>& flatten_cadidates) {
    std::vector<ExprPtr> compiled_inputs;
    auto flatten = ShouldFlatten(expr);
    for (auto& input : expr->inputs()) {
        if (dynamic_cast<const expr::InputTypeExpr*>(input.get())) {
            AssertInfo(
                dynamic_cast<const expr::FieldAccessTypeExpr*>(expr.get()),
                "An InputReference can only occur under a FieldReference");
        } else {
            if (flatten.has_value()) {
                std::vector<expr::TypedExprPtr> flat_exprs;
                FlattenInput(input, flatten.value(), flat_exprs);
                for (auto& input : flat_exprs) {
                    compiled_inputs.push_back(CompileExpression(
                        input, context, flatten_cadidates, false));
                }
            } else {
                compiled_inputs.push_back(CompileExpression(
                    input, context, flatten_cadidates, false));
            }
        }
    }
    return compiled_inputs;
}

ExprPtr
CompileExpression(const expr::TypedExprPtr& expr,
                  QueryContext* context,
                  const std::unordered_set<std::string>& flatten_candidates,
                  bool enable_constant_folding) {
    ExprPtr result;

    auto compiled_inputs = CompileInputs(expr, context, flatten_candidates);

    auto GetTypes = [](const std::vector<ExprPtr>& exprs) {
        std::vector<DataType> types;
        types.reserve(exprs.size());
        for (auto& expr : exprs) {
            types.push_back(expr->type());
        }
        return types;
    };
    auto input_types = GetTypes(compiled_inputs);

    if (auto call = std::dynamic_pointer_cast<const expr::CallExpr>(expr)) {
        result = std::make_shared<PhyCallExpr>(
            compiled_inputs,
            call,
            "PhyCallExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::UnaryRangeFilterExpr>(expr)) {
        result = std::make_shared<PhyUnaryRangeFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyUnaryRangeFilterExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::LogicalUnaryExpr>(expr)) {
        result = std::make_shared<PhyLogicalUnaryExpr>(
            compiled_inputs, casted_expr, "PhyLogicalUnaryExpr");
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::TermFilterExpr>(expr)) {
        result = std::make_shared<PhyTermFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyTermFilterExpr",
            context->get_segment(),
            context->get_active_count(),
            context->get_query_timestamp(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::LogicalBinaryExpr>(expr)) {
        if (casted_expr->op_type_ ==
                milvus::expr::LogicalBinaryExpr::OpType::And ||
            casted_expr->op_type_ ==
                milvus::expr::LogicalBinaryExpr::OpType::Or) {
            result = std::make_shared<PhyConjunctFilterExpr>(
                std::move(compiled_inputs),
                casted_expr->op_type_ ==
                    milvus::expr::LogicalBinaryExpr::OpType::And);
        } else {
            result = std::make_shared<PhyLogicalBinaryExpr>(
                compiled_inputs, casted_expr, "PhyLogicalBinaryExpr");
        }
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::BinaryRangeFilterExpr>(expr)) {
        result = std::make_shared<PhyBinaryRangeFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyBinaryRangeFilterExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::AlwaysTrueExpr>(expr)) {
        result = std::make_shared<PhyAlwaysTrueExpr>(
            compiled_inputs,
            casted_expr,
            "PhyAlwaysTrueExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::BinaryArithOpEvalRangeExpr>(expr)) {
        result = std::make_shared<PhyBinaryArithOpEvalRangeExpr>(
            compiled_inputs,
            casted_expr,
            "PhyBinaryArithOpEvalRangeExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr =
                   std::dynamic_pointer_cast<const milvus::expr::CompareExpr>(
                       expr)) {
        result = std::make_shared<PhyCompareFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyCompareFilterExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr =
                   std::dynamic_pointer_cast<const milvus::expr::ExistsExpr>(
                       expr)) {
        result = std::make_shared<PhyExistsFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyExistsFilterExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::JsonContainsExpr>(expr)) {
        result = std::make_shared<PhyJsonContainsFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyJsonContainsFilterExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto value_expr =
                   std::dynamic_pointer_cast<const milvus::expr::ValueExpr>(
                       expr)) {
        // used for function call arguments, may emit any type
        result = std::make_shared<PhyValueExpr>(
            compiled_inputs,
            value_expr,
            "PhyValueExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto column_expr =
                   std::dynamic_pointer_cast<const milvus::expr::ColumnExpr>(
                       expr)) {
        result = std::make_shared<PhyColumnExpr>(
            compiled_inputs,
            column_expr,
            "PhyColumnExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto column_expr =
                   std::dynamic_pointer_cast<const milvus::expr::NullExpr>(
                       expr)) {
        result = std::make_shared<PhyNullExpr>(
            compiled_inputs,
            column_expr,
            "PhyNullExpr",
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else {
        PanicInfo(ExprInvalid, "unsupport expr: ", expr->ToString());
    }
    return result;
}

inline void
ReorderConjunctExpr(std::shared_ptr<milvus::exec::PhyConjunctFilterExpr>& expr,
                    ExecContext* context,
                    bool& has_heavy_operation) {
    auto* segment = context->get_query_context()->get_segment();
    if (!segment || !expr) {
        return;
    }
    std::vector<size_t> reorder;
    std::vector<size_t> numeric_expr;
    std::vector<size_t> indexed_expr;
    std::vector<size_t> string_expr;
    std::vector<size_t> json_expr;
    std::vector<size_t> compare_expr;
    std::vector<size_t> other_expr;
    std::vector<size_t> heavy_conjunct_expr;
    std::vector<size_t> light_conjunct_expr;

    auto& inputs = expr->GetInputsRef();
    for (int i = 0; i < inputs.size(); i++) {
        auto input = inputs[i];

        if (input->IsSource() && input->GetColumnInfo().has_value()) {
            auto column = input->GetColumnInfo().value();
            if (IsNumericDataType(column.data_type_)) {
                numeric_expr.push_back(i);
                continue;
            }
            if (segment->HasIndex(column.field_id_)) {
                indexed_expr.push_back(i);
                continue;
            }
            if (IsStringDataType(column.data_type_)) {
                string_expr.push_back(i);
                continue;
            }
            if (IsJsonDataType(column.data_type_)) {
                json_expr.push_back(i);
                has_heavy_operation = true;
                continue;
            }
        }

        if (input->name() == "PhyConjunctFilterExpr") {
            bool sub_expr_heavy = false;
            auto expr = std::dynamic_pointer_cast<PhyConjunctFilterExpr>(input);
            ReorderConjunctExpr(expr, context, sub_expr_heavy);
            has_heavy_operation |= sub_expr_heavy;
            if (sub_expr_heavy) {
                heavy_conjunct_expr.push_back(i);
            } else {
                light_conjunct_expr.push_back(i);
            }
        }

        if (input->name() == "PhyCompareFilterExpr") {
            compare_expr.push_back(i);
            has_heavy_operation = true;
            continue;
        }

        other_expr.push_back(i);
    }

    reorder.insert(reorder.end(), numeric_expr.begin(), numeric_expr.end());
    reorder.insert(reorder.end(), indexed_expr.begin(), indexed_expr.end());
    reorder.insert(reorder.end(), string_expr.begin(), string_expr.end());
    reorder.insert(
        reorder.end(), light_conjunct_expr.begin(), light_conjunct_expr.end());
    reorder.insert(reorder.end(), other_expr.begin(), other_expr.end());
    reorder.insert(reorder.end(), json_expr.begin(), json_expr.end());
    reorder.insert(
        reorder.end(), heavy_conjunct_expr.begin(), heavy_conjunct_expr.end());
    reorder.insert(reorder.end(), compare_expr.begin(), compare_expr.end());

    Assert(reorder.size() == inputs.size());

    expr->Reorder(reorder);
}

inline void
OptimizeCompiledExprs(ExecContext* context, const std::vector<ExprPtr>& exprs) {
    // Rule 1: reoder conjucts expr's subexpr, postpone heavy operations
    // sequence: int(column) -> index(column) -> string(column) -> light conjuct
    // ...... -> json(column) -> heavy conjuct -> two_column_compare
    for (const auto& expr : exprs) {
        if (expr->name() == "PhyConjunctFilterExpr") {
            auto conjunct_expr =
                std::dynamic_pointer_cast<PhyConjunctFilterExpr>(expr);
            bool has_heavy_operation = false;
            ReorderConjunctExpr(conjunct_expr, context, has_heavy_operation);
        }
    }
}

}  // namespace exec
}  // namespace milvus
