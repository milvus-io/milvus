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

#include <memory>
#include <vector>

#include "common/Consts.h"
#include "expr/ITypeExpr.h"
#include "exec/expression/Expr.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"

namespace milvus::test {
inline auto
GenColumnInfo(
    int64_t field_id,
    proto::schema::DataType field_type,
    bool auto_id,
    bool is_pk,
    proto::schema::DataType element_type = proto::schema::DataType::None) {
    auto column_info = new proto::plan::ColumnInfo();
    column_info->set_field_id(field_id);
    column_info->set_data_type(field_type);
    column_info->set_is_autoid(auto_id);
    column_info->set_is_primary_key(is_pk);
    column_info->set_element_type(element_type);
    return column_info;
}

template <typename T>
auto
GenGenericValue(T value) {
    auto generic = new proto::plan::GenericValue();
    if constexpr (std::is_same_v<T, bool>) {
        generic->set_bool_val(static_cast<bool>(value));
    } else if constexpr (std::is_integral_v<T>) {
        generic->set_int64_val(static_cast<int64_t>(value));
    } else if constexpr (std::is_floating_point_v<T>) {
        generic->set_float_val(static_cast<float>(value));
    } else if constexpr (std::is_same_v<T, std::string>) {
        generic->set_string_val(static_cast<std::string>(value));
    } else {
        static_assert(always_false<T>);
    }
    return generic;
}

template <typename T>
auto
GenUnaryRangeExpr(proto::plan::OpType op, T& value) {
    auto unary_range_expr = new proto::plan::UnaryRangeExpr();
    unary_range_expr->set_op(op);
    auto generic = GenGenericValue(value);
    unary_range_expr->set_allocated_value(generic);
    return unary_range_expr;
}

inline auto
GenExpr() {
    return std::make_unique<proto::plan::Expr>();
}

inline std::shared_ptr<milvus::plan::PlanNode>
CreateRetrievePlanByExpr(std::shared_ptr<milvus::expr::ITypeExpr> expr) {
    auto init_plannode_id = std::stoi(DEFAULT_PLANNODE_ID);
    milvus::plan::PlanNodePtr plannode;
    std::vector<milvus::plan::PlanNodePtr> sources;

    plannode =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

    plannode = std::make_shared<milvus::plan::MvccNode>(
        std::to_string(init_plannode_id++), sources);
    return plannode;
}

inline std::shared_ptr<milvus::plan::PlanNode>
CreateSearchPlanByExpr(std::shared_ptr<milvus::expr::ITypeExpr> expr) {
    auto init_plannode_id = std::stoi(DEFAULT_PLANNODE_ID);
    milvus::plan::PlanNodePtr plannode;
    std::vector<milvus::plan::PlanNodePtr> sources;

    plannode =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

    plannode = std::make_shared<milvus::plan::MvccNode>(
        std::to_string(init_plannode_id++), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

    plannode = std::make_shared<milvus::plan::VectorSearchNode>(
        std::to_string(init_plannode_id++), sources);

    return plannode;
}

inline ColumnVectorPtr
gen_filter_res(milvus::plan::PlanNode* plan_node,
               const milvus::segcore::SegmentInternalInterface* segment,
               uint64_t active_count,
               uint64_t timestamp,
               FixedVector<int32_t>* offsets = nullptr) {
    auto filter_node = dynamic_cast<milvus::plan::FilterBitsNode*>(plan_node);
    assert(filter_node != nullptr);
    std::vector<milvus::expr::TypedExprPtr> filters;
    filters.emplace_back(filter_node->filter());
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, timestamp);

    std::unique_ptr<milvus::exec::ExecContext> exec_context =
        std::make_unique<milvus::exec::ExecContext>(query_context.get());
    auto exprs_ =
        std::make_unique<milvus::exec::ExprSet>(filters, exec_context.get());
    std::vector<VectorPtr> results_;
    milvus::exec::EvalCtx eval_ctx(exec_context.get(), exprs_.get());
    eval_ctx.set_offset_input(offsets);
    exprs_->Eval(0, 1, true, eval_ctx, results_);

    auto col_vec = std::dynamic_pointer_cast<milvus::ColumnVector>(results_[0]);
    return col_vec;
}

}  // namespace milvus::test
