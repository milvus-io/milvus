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

#include <atomic>
#include <chrono>
#include <memory>
#include <ratio>

#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/Tracer.h"
#include "exec/expression/AlwaysTrueExpr.h"
#include "exec/expression/BinaryArithOpEvalRangeExpr.h"
#include "exec/expression/BinaryRangeExpr.h"
#include "exec/expression/CallExpr.h"
#include "exec/expression/ColumnExpr.h"
#include "exec/expression/CompareExpr.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/ExistsExpr.h"
#include "exec/expression/GISFunctionFilterExpr.h"
#include "exec/expression/JsonContainsExpr.h"
#include "exec/expression/LogicalBinaryExpr.h"
#include "exec/expression/LogicalUnaryExpr.h"
#include "exec/expression/MatchExpr.h"
#include "exec/expression/NullExpr.h"
#include "exec/expression/TermExpr.h"
#include "exec/expression/TimestamptzArithCompareExpr.h"
#include "exec/expression/UnaryExpr.h"
#include "exec/expression/ValueExpr.h"
#include "expr/ITypeExpr.h"
#include "glog/logging.h"
#include "index/SkipIndex.h"
#include "log/Log.h"
#include "monitor/Monitor.h"
#include "pb/plan.pb.h"
#include "prometheus/histogram.h"
#include "segcore/Utils.h"

namespace milvus {
namespace exec {

SegmentExpr::~SegmentExpr() {
    // record accumulated json filter latencies as segment-level metrics.
    // latencies are accumulated in microseconds and converted to milliseconds for Observe.
    // this avoids per-batch metric overhead and provides more meaningful
    // segment-level measurements.
    if (json_filter_bruteforce_latency_us_ > 0) {
        milvus::monitor::internal_json_filter_latency_bruteforce.Observe(
            json_filter_bruteforce_latency_us_ / 1000.0);
    }
    if (json_filter_stats_latency_us_ > 0) {
        milvus::monitor::internal_json_filter_latency_json_stats.Observe(
            json_filter_stats_latency_us_ / 1000.0);
    }
    if (json_stats_shredding_latency_us_ > 0) {
        milvus::monitor::internal_json_stats_latency_shredding.Observe(
            json_stats_shredding_latency_us_ / 1000.0);
    }
    if (json_stats_shared_latency_us_ > 0) {
        milvus::monitor::internal_json_stats_latency_shared.Observe(
            json_stats_shared_latency_us_ / 1000.0);
    }
}

void
ExprSet::Eval(int32_t begin,
              int32_t end,
              bool initialize,
              EvalCtx& context,
              std::vector<VectorPtr>& results) {
    tracer::AutoSpan span("ExprSet::Eval", tracer::GetRootSpan(), true);

    results.resize(exprs_.size());
    auto* exec_ctx = context.get_exec_context();
    auto* query_ctx =
        exec_ctx != nullptr ? exec_ctx->get_query_context() : nullptr;
    for (size_t i = begin; i < end; ++i) {
        milvus::exec::checkCancellation(query_ctx);
        exprs_[i]->Eval(context, results[i]);
    }
}

// Create TTL field filtering expression if schema has TTL field configured
// Returns a single OR expression: ttl_field is null OR ttl_field > physical_us
// This means: keep entities with null TTL (never expire) OR entities with TTL > current time (not expired)
inline expr::TypedExprPtr
CreateTTLFieldFilterExpression(QueryContext* query_context) {
    auto segment = query_context->get_segment();
    auto& schema = segment->get_schema();
    if (!schema.get_ttl_field_id().has_value()) {
        return nullptr;
    }

    auto ttl_field_id = schema.get_ttl_field_id().value();
    auto& ttl_field_meta = schema[ttl_field_id];

    // Convert query_timestamp to physical microseconds for TTL comparison
    auto query_timestamp = query_context->get_query_timestamp();
    int64_t physical_us =
        static_cast<int64_t>(
            milvus::segcore::TimestampToPhysicalMs(query_timestamp)) *
        1000;

    expr::ColumnInfo ttl_column_info(ttl_field_id,
                                     ttl_field_meta.get_data_type(),
                                     {},
                                     ttl_field_meta.is_nullable());

    auto ttl_is_null_expr = std::make_shared<expr::NullExpr>(
        ttl_column_info, proto::plan::NullExpr_NullOp_IsNull);

    proto::plan::GenericValue ttl_threshold;
    ttl_threshold.set_int64_val(physical_us);
    auto ttl_greater_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        ttl_column_info, proto::plan::OpType::GreaterThan, ttl_threshold);

    auto ttl_or_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::Or,
        ttl_is_null_expr,
        ttl_greater_expr);

    return ttl_or_expr;
}

std::vector<ExprPtr>
CompileExpressions(const std::vector<expr::TypedExprPtr>& sources,
                   ExecContext* context,
                   const std::unordered_set<std::string>& flatten_candidate,
                   bool enable_constant_folding) {
    std::vector<std::shared_ptr<Expr>> exprs;
    exprs.reserve(sources.size());

    // Create TTL filter expression if schema has TTL field
    auto ttl_expr =
        CreateTTLFieldFilterExpression(context->get_query_context());

    // Merge TTL expression with the first source expression if TTL exists
    for (size_t i = 0; i < sources.size(); ++i) {
        expr::TypedExprPtr expr_to_compile = sources[i];
        if (i == 0 && ttl_expr != nullptr) {
            // Merge TTL expression with the first expression using AND
            expr_to_compile = std::make_shared<expr::LogicalBinaryExpr>(
                expr::LogicalBinaryExpr::OpType::And, sources[i], ttl_expr);
        }
        exprs.emplace_back(CompileExpression(expr_to_compile,
                                             context->get_query_context(),
                                             flatten_candidate,
                                             enable_constant_folding));
    }

    if (OPTIMIZE_EXPR_ENABLED.load()) {
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
                for (auto& flat_input : flat_exprs) {
                    compiled_inputs.push_back(CompileExpression(
                        flat_input, context, flatten_cadidates, false));
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
    auto op_ctx = context->get_op_context();

    if (auto call = std::dynamic_pointer_cast<const expr::CallExpr>(expr)) {
        result = std::make_shared<PhyCallExpr>(
            compiled_inputs,
            call,
            "PhyCallExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::UnaryRangeFilterExpr>(expr)) {
        result = std::make_shared<PhyUnaryRangeFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyUnaryRangeFilterExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::LogicalUnaryExpr>(expr)) {
        result = std::make_shared<PhyLogicalUnaryExpr>(
            compiled_inputs, casted_expr, "PhyLogicalUnaryExpr", op_ctx);
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::TermFilterExpr>(expr)) {
        result = std::make_shared<PhyTermFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyTermFilterExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->get_query_timestamp(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::LogicalBinaryExpr>(expr)) {
        if (casted_expr->op_type_ ==
                milvus::expr::LogicalBinaryExpr::OpType::And ||
            casted_expr->op_type_ ==
                milvus::expr::LogicalBinaryExpr::OpType::Or) {
            result = std::make_shared<PhyConjunctFilterExpr>(
                std::move(compiled_inputs),
                casted_expr->op_type_ ==
                    milvus::expr::LogicalBinaryExpr::OpType::And,
                op_ctx);
        } else {
            result = std::make_shared<PhyLogicalBinaryExpr>(
                compiled_inputs, casted_expr, "PhyLogicalBinaryExpr", op_ctx);
        }
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::BinaryRangeFilterExpr>(expr)) {
        result = std::make_shared<PhyBinaryRangeFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyBinaryRangeFilterExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::AlwaysTrueExpr>(expr)) {
        result = std::make_shared<PhyAlwaysTrueExpr>(
            compiled_inputs,
            casted_expr,
            "PhyAlwaysTrueExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::BinaryArithOpEvalRangeExpr>(expr)) {
        result = std::make_shared<PhyBinaryArithOpEvalRangeExpr>(
            compiled_inputs,
            casted_expr,
            "PhyBinaryArithOpEvalRangeExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::TimestamptzArithCompareExpr>(expr)) {
        result = std::make_shared<PhyTimestamptzArithCompareExpr>(
            compiled_inputs,
            casted_expr,
            "PhyTimestamptzArithCompareExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto casted_expr =
                   std::dynamic_pointer_cast<const milvus::expr::CompareExpr>(
                       expr)) {
        result = std::make_shared<PhyCompareFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyCompareFilterExpr",
            op_ctx,
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
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::JsonContainsExpr>(expr)) {
        result = std::make_shared<PhyJsonContainsFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyJsonContainsFilterExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto value_expr =
                   std::dynamic_pointer_cast<const milvus::expr::ValueExpr>(
                       expr)) {
        // used for function call arguments, may emit any type
        result = std::make_shared<PhyValueExpr>(
            compiled_inputs,
            value_expr,
            "PhyValueExpr",
            op_ctx,
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
            op_ctx,
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
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto casted_expr = std::dynamic_pointer_cast<
                   const milvus::expr::GISFunctionFilterExpr>(expr)) {
        result = std::make_shared<PhyGISFunctionFilterExpr>(
            compiled_inputs,
            casted_expr,
            "PhyGISFunctionFilterExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size(),
            context->get_consistency_level());
    } else if (auto match_expr =
                   std::dynamic_pointer_cast<const milvus::expr::MatchExpr>(
                       expr)) {
        result = std::make_shared<PhyMatchFilterExpr>(
            compiled_inputs,
            match_expr,
            "PhyMatchFilterExpr",
            op_ctx,
            context->get_segment(),
            context->get_active_count(),
            context->query_config()->get_expr_batch_size());
    } else {
        ThrowInfo(ExprInvalid, "unsupport expr: ", expr->ToString());
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
            case proto::plan::InnerMatch:
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
    // Record all LIKE expression indices for potential batch ngram optimization
    std::vector<size_t> like_indices;

    const auto& inputs = expr->GetInputsRef();
    bool and_conjunction = expr->IsAnd();
    for (int i = 0; i < inputs.size(); i++) {
        auto input = inputs[i];

        if (input->IsSource() && input->GetColumnInfo().has_value()) {
            auto column = input->GetColumnInfo().value();
            if (IsNumericDataType(column.data_type_)) {
                numeric_expr.push_back(i);
                continue;
            }
            if (segment->HasIndex(column.field_id_) && !IsLikeExpr(input)) {
                indexed_expr.push_back(i);
                continue;
            }

            if (IsStringDataType(column.data_type_)) {
                if (IsLikeExpr(input)) {
                    has_heavy_operation = true;
                    str_like_expr.push_back(i);
                    if (and_conjunction) {
                        like_indices.push_back(i);
                    }
                } else {
                    string_expr.push_back(i);
                }
                continue;
            }

            if (IsArrayDataType(column.data_type_)) {
                if (IsLikeExpr(input)) {
                    has_heavy_operation = true;
                    array_like_expr.push_back(i);
                    if (and_conjunction) {
                        like_indices.push_back(i);
                    }
                } else {
                    array_expr.push_back(i);
                }
                continue;
            }

            if (IsJsonDataType(column.data_type_)) {
                if (IsLikeExpr(input)) {
                    json_like_expr.push_back(i);
                    if (and_conjunction) {
                        like_indices.push_back(i);
                    }
                } else {
                    json_expr.push_back(i);
                }
                has_heavy_operation = true;
                continue;
            }
        }

        if (input->name() == "PhyConjunctFilterExpr") {
            bool sub_expr_heavy = false;
            auto sub_expr =
                std::static_pointer_cast<PhyConjunctFilterExpr>(input);
            ReorderConjunctExpr(sub_expr, context, sub_expr_heavy);
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

    // Reserve position for like_conjunct (will be added to inputs_ at runtime)
    bool has_batch_like = like_indices.size() > 1;
    if (has_batch_like) {
        reorder.push_back(
            inputs.size());  // inputs.size() will be like_conjunct's index
        expr->SetLikeIndices(std::move(like_indices));
    }
    reorder.insert(
        reorder.end(), heavy_conjunct_expr.begin(), heavy_conjunct_expr.end());
    reorder.insert(reorder.end(), compare_expr.begin(), compare_expr.end());

    size_t expected_size = inputs.size() + (has_batch_like ? 1 : 0);
    AssertInfo(reorder.size() == expected_size,
               "reorder size:{} but expected size:{}",
               reorder.size(),
               expected_size);

    expr->Reorder(reorder);
}

inline void
SetNamespaceSkipIndex(std::shared_ptr<PhyConjunctFilterExpr> conjunct_expr,
                      ExecContext* context) {
    auto schema = context->get_query_context()->get_segment()->get_schema();
    auto namespace_field_id = schema.get_namespace_field_id();
    auto inputs = conjunct_expr->GetInputsRef();
    std::shared_ptr<PhyUnaryRangeFilterExpr> namespace_expr = nullptr;
    for (const auto& input : inputs) {
        auto unary = std::dynamic_pointer_cast<PhyUnaryRangeFilterExpr>(input);
        if (!unary) {
            continue;
        }
        if (unary->GetColumnInfo().value().field_id_ ==
                namespace_field_id.value() &&
            unary->GetOpType() == proto::plan::OpType::Equal) {
            namespace_expr = unary;
        }
    }
    if (!namespace_expr) {
        return;
    }
    auto namespace_field_meta = schema[namespace_field_id.value()];
    auto& skip_index =
        context->get_query_context()->get_segment()->GetSkipIndex();
    if (namespace_field_meta.get_data_type() == DataType::INT64) {
        auto skip_namespace_func = [&](int64_t chunk_id) -> bool {
            return skip_index.CanSkipUnaryRange<int64_t>(
                namespace_field_id.value(),
                chunk_id,
                proto::plan::OpType::Equal,
                namespace_expr->GetLogicalExpr()->GetValue().int64_val());
        };
        namespace_expr->SetNamespaceSkipFunc(skip_namespace_func);
    } else {
        auto skip_namespace_func = [&](int64_t chunk_id) -> bool {
            return skip_index.CanSkipUnaryRange<std::string>(
                namespace_field_id.value(),
                chunk_id,
                proto::plan::OpType::Equal,
                namespace_expr->GetLogicalExpr()->GetValue().string_val());
        };
        namespace_expr->SetNamespaceSkipFunc(skip_namespace_func);
    }
}

inline void
OptimizeCompiledExprs(ExecContext* context, const std::vector<ExprPtr>& exprs) {
    auto schema = context->get_query_context()->get_segment()->get_schema();
    auto namespace_field_id = schema.get_namespace_field_id();
    std::chrono::high_resolution_clock::time_point start =
        std::chrono::high_resolution_clock::now();
    for (const auto& expr : exprs) {
        if (expr->name() == "PhyConjunctFilterExpr") {
            LOG_DEBUG("before reoder filter expression: {}", expr->ToString());
            auto conjunct_expr =
                std::static_pointer_cast<PhyConjunctFilterExpr>(expr);
            bool has_heavy_operation = false;
            ReorderConjunctExpr(conjunct_expr, context, has_heavy_operation);
            LOG_DEBUG("after reorder filter expression: {}", expr->ToString());
            if (namespace_field_id.has_value()) {
                SetNamespaceSkipIndex(conjunct_expr, context);
            }
        }
    }
    std::chrono::high_resolution_clock::time_point end =
        std::chrono::high_resolution_clock::now();
    double cost =
        std::chrono::duration<double, std::micro>(end - start).count();
    milvus::monitor::internal_core_optimize_expr_latency.Observe(cost / 1000);
}

}  // namespace exec
}  // namespace milvus
