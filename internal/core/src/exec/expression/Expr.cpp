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

    if (OPTIMIZE_EXPR_ENABLED) {
        OptimizeCompiledExprs(context, exprs);
    }

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

bool
IsLikeExpr(std::shared_ptr<Expr> input) {
    if (input->name() == "PhyUnaryRangeFilterExpr") {
        auto optype = std::static_pointer_cast<PhyUnaryRangeFilterExpr>(input)
                          ->GetLogicalExpr()
                          ->op_type_;
        switch (optype) {
            case proto::plan::PrefixMatch:
            case proto::plan::PostfixMatch:
            case proto::plan::Match:
                return true;
            default:
                return false;
        }
    }
    return false;
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
    std::vector<size_t> str_like_expr;
    std::vector<size_t> json_expr;
    std::vector<size_t> json_like_expr;
    std::vector<size_t> array_expr;
    std::vector<size_t> array_like_expr;
    std::vector<size_t> compare_expr;
    std::vector<size_t> other_expr;
    std::vector<size_t> heavy_conjunct_expr;
    std::vector<size_t> light_conjunct_expr;

    const auto& inputs = expr->GetInputsRef();
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
                auto is_like_expr = IsLikeExpr(input);
                if (is_like_expr) {
                    str_like_expr.push_back(i);
                    has_heavy_operation = true;
                } else {
                    string_expr.push_back(i);
                }
                continue;
            }

            if (IsArrayDataType(column.data_type_)) {
                auto is_like_expr = IsLikeExpr(input);
                if (is_like_expr) {
                    array_like_expr.push_back(i);
                    has_heavy_operation = true;
                } else {
                    array_expr.push_back(i);
                }
                continue;
            }

            if (IsJsonDataType(column.data_type_)) {
                auto is_like_expr = IsLikeExpr(input);
                if (is_like_expr) {
                    json_like_expr.push_back(i);
                } else {
                    json_expr.push_back(i);
                }
                has_heavy_operation = true;
                continue;
            }
        }

        if (input->name() == "PhyConjunctFilterExpr") {
            bool sub_expr_heavy = false;
            auto expr = std::static_pointer_cast<PhyConjunctFilterExpr>(input);
            ReorderConjunctExpr(expr, context, sub_expr_heavy);
            has_heavy_operation |= sub_expr_heavy;
            if (sub_expr_heavy) {
                heavy_conjunct_expr.push_back(i);
            } else {
                light_conjunct_expr.push_back(i);
            }
            continue;
        }

        if (input->name() == "PhyCompareFilterExpr") {
            compare_expr.push_back(i);
            has_heavy_operation = true;
            continue;
        }

        other_expr.push_back(i);
    }

    reorder.reserve(inputs.size());
    // Final reorder sequence:
    // 1. Numeric column expressions (fastest to evaluate)
    // 2. Indexed column expressions (can use index for efficient filtering)
    // 3. String column expressions
    // 4. Light conjunct expressions (conjunctions without heavy operations)
    // 5. Other expressions
    // 6. Array column expression
    // 7. String like expression
    // 8. Array like expression
    // 9. JSON column expressions (expensive to evaluate)
    // 10. JSON like expression (more expensive than common json compare)
    // 11. Heavy conjunct expressions (conjunctions with heavy operations)
    // 12. Compare filter expressions (most expensive, comparing two columns)
    reorder.insert(reorder.end(), numeric_expr.begin(), numeric_expr.end());
    reorder.insert(reorder.end(), indexed_expr.begin(), indexed_expr.end());
    reorder.insert(reorder.end(), string_expr.begin(), string_expr.end());
    reorder.insert(
        reorder.end(), light_conjunct_expr.begin(), light_conjunct_expr.end());
    reorder.insert(reorder.end(), other_expr.begin(), other_expr.end());
    reorder.insert(reorder.end(), array_expr.begin(), array_expr.end());
    reorder.insert(reorder.end(), str_like_expr.begin(), str_like_expr.end());
    reorder.insert(
        reorder.end(), array_like_expr.begin(), array_like_expr.end());
    reorder.insert(reorder.end(), json_expr.begin(), json_expr.end());
    reorder.insert(reorder.end(), json_like_expr.begin(), json_like_expr.end());
    reorder.insert(
        reorder.end(), heavy_conjunct_expr.begin(), heavy_conjunct_expr.end());
    reorder.insert(reorder.end(), compare_expr.begin(), compare_expr.end());

    AssertInfo(reorder.size() == inputs.size(),
               "reorder size:{} but input size:{}",
               reorder.size(),
               inputs.size());

    expr->Reorder(reorder);
}

inline std::shared_ptr<PhyTermFilterExpr>
ConvertMultiOrToInExpr(std::vector<std::shared_ptr<Expr>>& exprs,
                       std::vector<size_t> indices,
                       ExecContext* context) {
    std::vector<proto::plan::GenericValue> values;
    auto type = proto::plan::GenericValue::ValCase::VAL_NOT_SET;
    for (auto& i : indices) {
        auto expr = std::static_pointer_cast<PhyUnaryRangeFilterExpr>(exprs[i])
                        ->GetLogicalExpr();
        if (type == proto::plan::GenericValue::ValCase::VAL_NOT_SET) {
            type = expr->val_.val_case();
        }
        if (type != expr->val_.val_case()) {
            return nullptr;
        }
        values.push_back(expr->val_);
    }
    auto logical_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        exprs[indices[0]]->GetColumnInfo().value(), values);
    auto query_context = context->get_query_context();
    return std::make_shared<PhyTermFilterExpr>(
        std::vector<std::shared_ptr<Expr>>{},
        logical_expr,
        "PhyTermFilterExpr",
        query_context->get_segment(),
        query_context->get_active_count(),
        query_context->get_query_timestamp(),
        query_context->query_config()->get_expr_batch_size());
}

inline void
RewriteConjunctExpr(std::shared_ptr<milvus::exec::PhyConjunctFilterExpr>& expr,
                    ExecContext* context) {
    if (expr->IsOr()) {
        auto& inputs = expr->GetInputsRef();
        std::map<expr::ColumnInfo, std::vector<size_t>> expr_indices;
        for (size_t i = 0; i < inputs.size(); i++) {
            auto input = inputs[i];
            if (input->name() == "PhyUnaryRangeFilterExpr") {
                auto phy_expr =
                    std::static_pointer_cast<PhyUnaryRangeFilterExpr>(input);
                if (phy_expr->GetOpType() == proto::plan::OpType::Equal) {
                    auto column = phy_expr->GetColumnInfo().value();
                    if (expr_indices.find(column) != expr_indices.end()) {
                        expr_indices[column].push_back(i);
                    } else {
                        expr_indices[column] = {i};
                    }
                }
            }

            if (input->name() == "PhyConjunctFilterExpr") {
                auto expr = std::static_pointer_cast<
                    milvus::exec::PhyConjunctFilterExpr>(input);
                RewriteConjunctExpr(expr, context);
            }
        }

        for (auto& [column, indices] : expr_indices) {
            // For numeric type, if or column greater than 150, then using in expr replace.
            // For other type, all convert to in expr.
            if ((IsNumericDataType(column.data_type_) &&
                 indices.size() > DEFAULT_CONVERT_OR_TO_IN_NUMERIC_LIMIT) ||
                (!IsNumericDataType(column.data_type_) && indices.size() > 1)) {
                auto new_expr =
                    ConvertMultiOrToInExpr(inputs, indices, context);
                if (new_expr) {
                    inputs[indices[0]] = new_expr;
                    for (size_t j = 1; j < indices.size(); j++) {
                        inputs[indices[j]] = nullptr;
                    }
                }
            }
        }
        inputs.erase(std::remove(inputs.begin(), inputs.end(), nullptr),
                     inputs.end());
    }
}

inline void
OptimizeCompiledExprs(ExecContext* context, const std::vector<ExprPtr>& exprs) {
    std::chrono::high_resolution_clock::time_point start =
        std::chrono::high_resolution_clock::now();
    for (const auto& expr : exprs) {
        if (expr->name() == "PhyConjunctFilterExpr") {
            LOG_DEBUG("before reoder filter expression: {}", expr->ToString());
            auto conjunct_expr =
                std::static_pointer_cast<PhyConjunctFilterExpr>(expr);
            RewriteConjunctExpr(conjunct_expr, context);
            bool has_heavy_operation = false;
            ReorderConjunctExpr(conjunct_expr, context, has_heavy_operation);
            LOG_DEBUG("after reorder filter expression: {}", expr->ToString());
        }
    }
    std::chrono::high_resolution_clock::time_point end =
        std::chrono::high_resolution_clock::now();
    double cost =
        std::chrono::duration<double, std::micro>(end - start).count();
    monitor::internal_core_optimize_expr_latency.Observe(cost / 1000);
}

}  // namespace exec
}  // namespace milvus
