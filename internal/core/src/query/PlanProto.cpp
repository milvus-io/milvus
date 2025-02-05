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

#include "PlanProto.h"

#include <google/protobuf/text_format.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/VectorTrait.h"
#include "common/EasyAssert.h"
#include "exec/expression/function/FunctionFactory.h"
#include "pb/plan.pb.h"
#include "query/Utils.h"
#include "knowhere/comp/materialized_view.h"
#include "plan/PlanNode.h"

namespace milvus::query {
namespace planpb = milvus::proto::plan;

std::unique_ptr<VectorPlanNode>
ProtoParser::PlanNodeFromProto(const planpb::PlanNode& plan_node_proto) {
    // TODO: add more buffs
    Assert(plan_node_proto.has_vector_anns());
    auto& anns_proto = plan_node_proto.vector_anns();

    auto expr_parser = [&]() -> plan::PlanNodePtr {
        auto expr = ParseExprs(anns_proto.predicates());
        return std::make_shared<plan::FilterBitsNode>(
            milvus::plan::GetNextPlanNodeId(), expr);
    };

    auto search_info_parser = [&]() -> SearchInfo {
        SearchInfo search_info;
        auto& query_info_proto = anns_proto.query_info();
        auto field_id = FieldId(anns_proto.field_id());
        search_info.field_id_ = field_id;

        search_info.metric_type_ = query_info_proto.metric_type();
        search_info.topk_ = query_info_proto.topk();
        search_info.round_decimal_ = query_info_proto.round_decimal();
        search_info.search_params_ =
            nlohmann::json::parse(query_info_proto.search_params());
        search_info.materialized_view_involved =
            query_info_proto.materialized_view_involved();
        // currently, iterative filter does not support range search
        if (!search_info.search_params_.contains(RADIUS)) {
            if (query_info_proto.hints() != "") {
                if (query_info_proto.hints() == "disable") {
                    search_info.iterative_filter_execution = false;
                } else if (query_info_proto.hints() == ITERATIVE_FILTER) {
                    search_info.iterative_filter_execution = true;
                } else {
                    // check if hints is valid
                    PanicInfo(ConfigInvalid,
                              "hints: {} not supported",
                              query_info_proto.hints());
                }
            } else if (search_info.search_params_.contains(HINTS)) {
                if (search_info.search_params_[HINTS] == ITERATIVE_FILTER) {
                    search_info.iterative_filter_execution = true;
                } else {
                    // check if hints is valid
                    PanicInfo(ConfigInvalid,
                              "hints: {} not supported",
                              search_info.search_params_[HINTS]);
                }
            }
        }

        if (query_info_proto.bm25_avgdl() > 0) {
            search_info.search_params_[knowhere::meta::BM25_AVGDL] =
                query_info_proto.bm25_avgdl();
        }

        if (query_info_proto.group_by_field_id() > 0) {
            auto group_by_field_id =
                FieldId(query_info_proto.group_by_field_id());
            search_info.group_by_field_id_ = group_by_field_id;
            search_info.group_size_ = query_info_proto.group_size() > 0
                                          ? query_info_proto.group_size()
                                          : 1;
            search_info.strict_group_size_ =
                query_info_proto.strict_group_size();
        }

        if (query_info_proto.has_search_iterator_v2_info()) {
            auto& iterator_v2_info_proto =
                query_info_proto.search_iterator_v2_info();
            search_info.iterator_v2_info_ = SearchIteratorV2Info{
                .token = iterator_v2_info_proto.token(),
                .batch_size = iterator_v2_info_proto.batch_size(),
            };
            if (iterator_v2_info_proto.has_last_bound()) {
                search_info.iterator_v2_info_->last_bound =
                    iterator_v2_info_proto.last_bound();
            }
        }

        return search_info;
    };

    auto plan_node = [&]() -> std::unique_ptr<VectorPlanNode> {
        if (anns_proto.vector_type() ==
            milvus::proto::plan::VectorType::BinaryVector) {
            return std::make_unique<BinaryVectorANNS>();
        } else if (anns_proto.vector_type() ==
                   milvus::proto::plan::VectorType::Float16Vector) {
            return std::make_unique<Float16VectorANNS>();
        } else if (anns_proto.vector_type() ==
                   milvus::proto::plan::VectorType::BFloat16Vector) {
            return std::make_unique<BFloat16VectorANNS>();
        } else if (anns_proto.vector_type() ==
                   milvus::proto::plan::VectorType::SparseFloatVector) {
            return std::make_unique<SparseFloatVectorANNS>();
        } else if (anns_proto.vector_type() ==
                   milvus::proto::plan::VectorType::Int8Vector) {
            return std::make_unique<Int8VectorANNS>();
        } else {
            return std::make_unique<FloatVectorANNS>();
        }
    }();
    plan_node->placeholder_tag_ = anns_proto.placeholder_tag();
    plan_node->search_info_ = std::move(search_info_parser());

    milvus::plan::PlanNodePtr plannode;
    std::vector<milvus::plan::PlanNodePtr> sources;

    // mvcc node -> vector search node -> iterative filter node
    auto iterative_filter_plan = [&]() {
        plannode = std::make_shared<milvus::plan::MvccNode>(
            milvus::plan::GetNextPlanNodeId());
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
        plannode = std::make_shared<milvus::plan::VectorSearchNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

        auto expr = ParseExprs(anns_proto.predicates());
        plannode = std::make_shared<plan::FilterNode>(
            milvus::plan::GetNextPlanNodeId(), expr, sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
    };

    // pre filter node -> mvcc node -> vector search node
    auto pre_filter_plan = [&]() {
        plannode = std::move(expr_parser());
        if (plan_node->search_info_.materialized_view_involved) {
            const auto expr_info = plannode->GatherInfo();
            knowhere::MaterializedViewSearchInfo materialized_view_search_info;
            for (const auto& [expr_field_id, vals] :
                 expr_info.field_id_to_values) {
                materialized_view_search_info
                    .field_id_to_touched_categories_cnt[expr_field_id] =
                    vals.size();
            }
            materialized_view_search_info.is_pure_and = expr_info.is_pure_and;
            materialized_view_search_info.has_not = expr_info.has_not;

            plan_node->search_info_
                .search_params_[knowhere::meta::MATERIALIZED_VIEW_SEARCH_INFO] =
                materialized_view_search_info;
        }
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
        plannode = std::make_shared<milvus::plan::MvccNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

        plannode = std::make_shared<milvus::plan::VectorSearchNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
    };

    if (anns_proto.has_predicates()) {
        // currently limit iterative filter scope to search only
        if (plan_node->search_info_.iterative_filter_execution &&
            plan_node->search_info_.group_by_field_id_ == std::nullopt) {
            iterative_filter_plan();
        } else {
            pre_filter_plan();
        }
    } else {
        // no filter, force set iterative filter hint to false, go with normal vector search path
        plan_node->search_info_.iterative_filter_execution = false;
        plannode = std::make_shared<milvus::plan::MvccNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

        plannode = std::make_shared<milvus::plan::VectorSearchNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
    }

    if (plan_node->search_info_.group_by_field_id_ != std::nullopt) {
        plannode = std::make_shared<milvus::plan::GroupByNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
    }

    plan_node->plannodes_ = plannode;

    return plan_node;
}

std::unique_ptr<RetrievePlanNode>
ProtoParser::RetrievePlanNodeFromProto(
    const planpb::PlanNode& plan_node_proto) {
    Assert(plan_node_proto.has_predicates() || plan_node_proto.has_query());

    milvus::plan::PlanNodePtr plannode;
    std::vector<milvus::plan::PlanNodePtr> sources;

    auto plan_node = [&]() -> std::unique_ptr<RetrievePlanNode> {
        auto node = std::make_unique<RetrievePlanNode>();
        if (plan_node_proto.has_predicates()) {  // version before 2023.03.30.
            node->is_count_ = false;
            auto& predicate_proto = plan_node_proto.predicates();
            auto expr_parser = [&]() -> plan::PlanNodePtr {
                auto expr = ParseExprs(predicate_proto);
                return std::make_shared<plan::FilterBitsNode>(
                    milvus::plan::GetNextPlanNodeId(), expr);
            }();
            plannode = std::move(expr_parser);
            sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
            plannode = std::make_shared<milvus::plan::MvccNode>(
                milvus::plan::GetNextPlanNodeId(), sources);
            node->plannodes_ = std::move(plannode);
        } else {
            auto& query = plan_node_proto.query();
            if (query.has_predicates()) {
                auto* predicate_proto = &query.predicates();
                bool has_predicate = true;
                if (predicate_proto->expr_case() ==
                    proto::plan::Expr::kRandomSampleExpr) {
                    has_predicate = false;
                    auto& sample_expr = predicate_proto->random_sample_expr();
                    plannode = std::make_shared<milvus::plan::RandomSampleNode>(
                        milvus::plan::GetNextPlanNodeId(),
                        sample_expr.sample_factor());
                    sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
                    if (sample_expr.has_predicate()) {
                        predicate_proto = &sample_expr.predicate();
                        has_predicate = true;
                    }
                }

                if (has_predicate) {
                    auto expr_parser = [&]() -> plan::PlanNodePtr {
                        auto expr = ParseExprs(*predicate_proto);
                        return std::make_shared<plan::FilterBitsNode>(
                            milvus::plan::GetNextPlanNodeId(), expr, sources);
                    }();
                    plannode = std::move(expr_parser);
                    sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
                }
            }

            plannode = std::make_shared<milvus::plan::MvccNode>(
                milvus::plan::GetNextPlanNodeId(), sources);
            sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

            node->is_count_ = query.is_count();
            node->limit_ = query.limit();
            if (node->is_count_) {
                plannode = std::make_shared<milvus::plan::CountNode>(
                    milvus::plan::GetNextPlanNodeId(), sources);
                sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
            }
            node->plannodes_ = plannode;
        }
        return node;
    }();

    return plan_node;
}

std::unique_ptr<Plan>
ProtoParser::CreatePlan(const proto::plan::PlanNode& plan_node_proto) {
    LOG_DEBUG("create search plan from proto: {}",
              plan_node_proto.DebugString());
    auto plan = std::make_unique<Plan>(schema);

    auto plan_node = PlanNodeFromProto(plan_node_proto);
    plan->tag2field_["$0"] = plan_node->search_info_.field_id_;
    plan->plan_node_ = std::move(plan_node);
    ExtractedPlanInfo extra_info(schema.size());
    extra_info.add_involved_field(plan->plan_node_->search_info_.field_id_);
    plan->extra_info_opt_ = std::move(extra_info);

    for (auto field_id_raw : plan_node_proto.output_field_ids()) {
        auto field_id = FieldId(field_id_raw);
        plan->target_entries_.push_back(field_id);
    }
    for (auto dynamic_field : plan_node_proto.dynamic_fields()) {
        plan->target_dynamic_fields_.push_back(dynamic_field);
    }

    return plan;
}

std::unique_ptr<RetrievePlan>
ProtoParser::CreateRetrievePlan(const proto::plan::PlanNode& plan_node_proto) {
    LOG_DEBUG("create retrieve plan from proto: {}",
              plan_node_proto.DebugString());
    auto retrieve_plan = std::make_unique<RetrievePlan>(schema);

    auto plan_node = RetrievePlanNodeFromProto(plan_node_proto);

    retrieve_plan->plan_node_ = std::move(plan_node);
    for (auto field_id_raw : plan_node_proto.output_field_ids()) {
        auto field_id = FieldId(field_id_raw);
        retrieve_plan->field_ids_.push_back(field_id);
    }
    for (auto dynamic_field : plan_node_proto.dynamic_fields()) {
        retrieve_plan->target_dynamic_fields_.push_back(dynamic_field);
    }

    return retrieve_plan;
}

expr::TypedExprPtr
ProtoParser::ParseUnaryRangeExprs(const proto::plan::UnaryRangeExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));
    std::vector<::milvus::proto::plan::GenericValue> extra_values;
    for (auto val : expr_pb.extra_values()) {
        extra_values.emplace_back(val);
    }
    return std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(column_info),
        expr_pb.op(),
        expr_pb.value(),
        extra_values);
}

expr::TypedExprPtr
ProtoParser::ParseNullExprs(const proto::plan::NullExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));
    return std::make_shared<milvus::expr::NullExpr>(
        expr::ColumnInfo(column_info), expr_pb.op());
}

expr::TypedExprPtr
ProtoParser::ParseBinaryRangeExprs(
    const proto::plan::BinaryRangeExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == (DataType)columnInfo.data_type());
    return std::make_shared<expr::BinaryRangeFilterExpr>(
        columnInfo,
        expr_pb.lower_value(),
        expr_pb.upper_value(),
        expr_pb.lower_inclusive(),
        expr_pb.upper_inclusive());
}

expr::TypedExprPtr
ProtoParser::ParseCallExprs(const proto::plan::CallExpr& expr_pb) {
    std::vector<expr::TypedExprPtr> parameters;
    std::vector<DataType> func_param_type_list;
    for (auto& param_expr : expr_pb.function_parameters()) {
        // function parameter can be any type
        auto e = this->ParseExprs(param_expr, TypeIsAny);
        parameters.push_back(e);
        func_param_type_list.push_back(e->type());
    }
    auto& factory = exec::expression::FunctionFactory::Instance();
    exec::expression::FilterFunctionRegisterKey func_sig{
        expr_pb.function_name(), std::move(func_param_type_list)};

    auto function = factory.GetFilterFunction(func_sig);
    if (function == nullptr) {
        PanicInfo(ExprInvalid,
                  "function " + func_sig.ToString() + " not found. ");
    }
    return std::make_shared<expr::CallExpr>(
        expr_pb.function_name(), parameters, function);
}

expr::TypedExprPtr
ProtoParser::ParseCompareExprs(const proto::plan::CompareExpr& expr_pb) {
    auto& left_column_info = expr_pb.left_column_info();
    auto left_field_id = FieldId(left_column_info.field_id());
    auto left_data_type = schema[left_field_id].get_data_type();
    Assert(left_data_type ==
           static_cast<DataType>(left_column_info.data_type()));

    auto& right_column_info = expr_pb.right_column_info();
    auto right_field_id = FieldId(right_column_info.field_id());
    auto right_data_type = schema[right_field_id].get_data_type();
    Assert(right_data_type ==
           static_cast<DataType>(right_column_info.data_type()));

    return std::make_shared<expr::CompareExpr>(left_field_id,
                                               right_field_id,
                                               left_data_type,
                                               right_data_type,
                                               expr_pb.op());
}

expr::TypedExprPtr
ProtoParser::ParseTermExprs(const proto::plan::TermExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == (DataType)columnInfo.data_type());
    std::vector<::milvus::proto::plan::GenericValue> values;
    for (size_t i = 0; i < expr_pb.values_size(); i++) {
        values.emplace_back(expr_pb.values(i));
    }
    return std::make_shared<expr::TermFilterExpr>(
        columnInfo, values, expr_pb.is_in_field());
}

expr::TypedExprPtr
ProtoParser::ParseUnaryExprs(const proto::plan::UnaryExpr& expr_pb) {
    auto op = static_cast<expr::LogicalUnaryExpr::OpType>(expr_pb.op());
    Assert(op == expr::LogicalUnaryExpr::OpType::LogicalNot);
    auto child_expr = this->ParseExprs(expr_pb.child());
    return std::make_shared<expr::LogicalUnaryExpr>(op, child_expr);
}

expr::TypedExprPtr
ProtoParser::ParseBinaryExprs(const proto::plan::BinaryExpr& expr_pb) {
    auto op = static_cast<expr::LogicalBinaryExpr::OpType>(expr_pb.op());
    auto left_expr = this->ParseExprs(expr_pb.left());
    auto right_expr = this->ParseExprs(expr_pb.right());
    return std::make_shared<expr::LogicalBinaryExpr>(op, left_expr, right_expr);
}

expr::TypedExprPtr
ProtoParser::ParseBinaryArithOpEvalRangeExprs(
    const proto::plan::BinaryArithOpEvalRangeExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));
    return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        column_info,
        expr_pb.op(),
        expr_pb.arith_op(),
        expr_pb.value(),
        expr_pb.right_operand());
}

expr::TypedExprPtr
ProtoParser::ParseExistExprs(const proto::plan::ExistsExpr& expr_pb) {
    auto& column_info = expr_pb.info();
    auto field_id = FieldId(column_info.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));
    return std::make_shared<expr::ExistsExpr>(column_info);
}

expr::TypedExprPtr
ProtoParser::ParseJsonContainsExprs(
    const proto::plan::JSONContainsExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == (DataType)columnInfo.data_type());
    std::vector<::milvus::proto::plan::GenericValue> values;
    for (size_t i = 0; i < expr_pb.elements_size(); i++) {
        values.emplace_back(expr_pb.elements(i));
    }
    return std::make_shared<expr::JsonContainsExpr>(
        columnInfo,
        expr_pb.op(),
        expr_pb.elements_same_type(),
        std::move(values));
}

expr::TypedExprPtr
ProtoParser::ParseColumnExprs(const proto::plan::ColumnExpr& expr_pb) {
    return std::make_shared<expr::ColumnExpr>(expr_pb.info());
}

expr::TypedExprPtr
ProtoParser::ParseValueExprs(const proto::plan::ValueExpr& expr_pb) {
    return std::make_shared<expr::ValueExpr>(expr_pb.value());
}

expr::TypedExprPtr
ProtoParser::CreateAlwaysTrueExprs() {
    return std::make_shared<expr::AlwaysTrueExpr>();
}

expr::TypedExprPtr
ProtoParser::ParseExprs(const proto::plan::Expr& expr_pb,
                        TypeCheckFunction type_check) {
    using ppe = proto::plan::Expr;
    expr::TypedExprPtr result;
    switch (expr_pb.expr_case()) {
        case ppe::kUnaryRangeExpr: {
            result = ParseUnaryRangeExprs(expr_pb.unary_range_expr());
            break;
        }
        case ppe::kBinaryExpr: {
            result = ParseBinaryExprs(expr_pb.binary_expr());
            break;
        }
        case ppe::kUnaryExpr: {
            result = ParseUnaryExprs(expr_pb.unary_expr());
            break;
        }
        case ppe::kTermExpr: {
            result = ParseTermExprs(expr_pb.term_expr());
            break;
        }
        case ppe::kBinaryRangeExpr: {
            result = ParseBinaryRangeExprs(expr_pb.binary_range_expr());
            break;
        }
        case ppe::kCompareExpr: {
            result = ParseCompareExprs(expr_pb.compare_expr());
            break;
        }
        case ppe::kBinaryArithOpEvalRangeExpr: {
            result = ParseBinaryArithOpEvalRangeExprs(
                expr_pb.binary_arith_op_eval_range_expr());
            break;
        }
        case ppe::kExistsExpr: {
            result = ParseExistExprs(expr_pb.exists_expr());
            break;
        }
        case ppe::kAlwaysTrueExpr: {
            result = CreateAlwaysTrueExprs();
            break;
        }
        case ppe::kJsonContainsExpr: {
            result = ParseJsonContainsExprs(expr_pb.json_contains_expr());
            break;
        }
        case ppe::kCallExpr: {
            result = ParseCallExprs(expr_pb.call_expr());
            break;
        }
            // may emit various types
        case ppe::kColumnExpr: {
            result = ParseColumnExprs(expr_pb.column_expr());
            break;
        }
        case ppe::kValueExpr: {
            result = ParseValueExprs(expr_pb.value_expr());
            break;
        }
        case ppe::kNullExpr: {
            result = ParseNullExprs(expr_pb.null_expr());
            break;
        }
        default: {
            std::string s;
            google::protobuf::TextFormat::PrintToString(expr_pb, &s);
            PanicInfo(ExprInvalid,
                      std::string("unsupported expr proto node: ") + s);
        }
    }
    if (type_check(result->type())) {
        return result;
    }
    PanicInfo(
        ExprInvalid, "expr type check failed, actual type: {}", result->type());
}

}  // namespace milvus::query
