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

#include "ExprImpl.h"
#include "common/VectorTrait.h"
#include "common/EasyAssert.h"
#include "generated/ExtractInfoExprVisitor.h"
#include "generated/ExtractInfoPlanNodeVisitor.h"
#include "pb/plan.pb.h"
#include "query/Utils.h"
#include "knowhere/comp/materialized_view.h"

namespace milvus::query {
namespace planpb = milvus::proto::plan;

template <typename T>
std::unique_ptr<TermExprImpl<T>>
ExtractTermExprImpl(FieldId field_id,
                    DataType data_type,
                    const planpb::TermExpr& expr_proto) {
    static_assert(IsScalar<T>);
    auto size = expr_proto.values_size();
    std::vector<T> terms;
    terms.reserve(size);
    auto val_case = proto::plan::GenericValue::ValCase::VAL_NOT_SET;
    for (int i = 0; i < size; ++i) {
        auto& value_proto = expr_proto.values(i);
        if constexpr (std::is_same_v<T, bool>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kBoolVal);
            terms.push_back(static_cast<T>(value_proto.bool_val()));
            val_case = proto::plan::GenericValue::ValCase::kBoolVal;
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kInt64Val);
            auto value = value_proto.int64_val();
            if (out_of_range<T>(value)) {
                continue;
            }
            terms.push_back(static_cast<T>(value));
            val_case = proto::plan::GenericValue::ValCase::kInt64Val;
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kFloatVal);
            terms.push_back(static_cast<T>(value_proto.float_val()));
            val_case = proto::plan::GenericValue::ValCase::kFloatVal;
        } else if constexpr (std::is_same_v<T, std::string>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kStringVal);
            terms.push_back(static_cast<T>(value_proto.string_val()));
            val_case = proto::plan::GenericValue::ValCase::kStringVal;
        } else {
            static_assert(always_false<T>);
        }
    }
    std::sort(terms.begin(), terms.end());
    return std::make_unique<TermExprImpl<T>>(
        expr_proto.column_info(), terms, val_case, expr_proto.is_in_field());
}

template <typename T>
std::unique_ptr<UnaryRangeExprImpl<T>>
ExtractUnaryRangeExprImpl(FieldId field_id,
                          DataType data_type,
                          const planpb::UnaryRangeExpr& expr_proto) {
    static_assert(IsScalar<T>);
    auto getValue = [&](const auto& value_proto) -> T {
        if constexpr (std::is_same_v<T, bool>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kBoolVal);
            return static_cast<T>(value_proto.bool_val());
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kInt64Val);
            return static_cast<T>(value_proto.int64_val());
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kFloatVal);
            return static_cast<T>(value_proto.float_val());
        } else if constexpr (std::is_same_v<T, std::string>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kStringVal);
            return static_cast<T>(value_proto.string_val());
        } else if constexpr (std::is_same_v<T, proto::plan::Array>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kArrayVal);
            return static_cast<proto::plan::Array>(value_proto.array_val());
        } else {
            static_assert(always_false<T>);
        }
    };
    return std::make_unique<UnaryRangeExprImpl<T>>(
        expr_proto.column_info(),
        static_cast<OpType>(expr_proto.op()),
        getValue(expr_proto.value()),
        expr_proto.value().val_case());
}

template <typename T>
std::unique_ptr<BinaryRangeExprImpl<T>>
ExtractBinaryRangeExprImpl(FieldId field_id,
                           DataType data_type,
                           const planpb::BinaryRangeExpr& expr_proto) {
    static_assert(IsScalar<T>);
    auto getValue = [&](const auto& value_proto) -> T {
        if constexpr (std::is_same_v<T, bool>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kBoolVal);
            return static_cast<T>(value_proto.bool_val());
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kInt64Val);
            return static_cast<T>(value_proto.int64_val());
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kFloatVal);
            return static_cast<T>(value_proto.float_val());
        } else if constexpr (std::is_same_v<T, std::string>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kStringVal);
            return static_cast<T>(value_proto.string_val());
        } else {
            static_assert(always_false<T>);
        }
    };
    return std::make_unique<BinaryRangeExprImpl<T>>(
        expr_proto.column_info(),
        expr_proto.lower_value().val_case(),
        expr_proto.lower_inclusive(),
        expr_proto.upper_inclusive(),
        getValue(expr_proto.lower_value()),
        getValue(expr_proto.upper_value()));
}

template <typename T>
std::unique_ptr<BinaryArithOpEvalRangeExprImpl<T>>
ExtractBinaryArithOpEvalRangeExprImpl(
    FieldId field_id,
    DataType data_type,
    const planpb::BinaryArithOpEvalRangeExpr& expr_proto) {
    static_assert(std::is_fundamental_v<T>);
    auto getValue = [&](const auto& value_proto) -> T {
        if constexpr (std::is_same_v<T, bool>) {
            // Handle bool here. Otherwise, it can go in `is_integral_v<T>`
            static_assert(always_false<T>);
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kInt64Val);
            return static_cast<T>(value_proto.int64_val());
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kFloatVal);
            return static_cast<T>(value_proto.float_val());
        } else {
            static_assert(always_false<T>);
        }
    };
    if (expr_proto.arith_op() == proto::plan::ArrayLength) {
        return std::make_unique<BinaryArithOpEvalRangeExprImpl<T>>(
            expr_proto.column_info(),
            expr_proto.value().val_case(),
            expr_proto.arith_op(),
            0,
            expr_proto.op(),
            getValue(expr_proto.value()));
    }
    return std::make_unique<BinaryArithOpEvalRangeExprImpl<T>>(
        expr_proto.column_info(),
        expr_proto.value().val_case(),
        expr_proto.arith_op(),
        getValue(expr_proto.right_operand()),
        expr_proto.op(),
        getValue(expr_proto.value()));
}

std::unique_ptr<VectorPlanNode>
ProtoParser::PlanNodeFromProto(const planpb::PlanNode& plan_node_proto) {
    // TODO: add more buffs
    Assert(plan_node_proto.has_vector_anns());
    auto& anns_proto = plan_node_proto.vector_anns();
    auto expr_opt = [&]() -> std::optional<ExprPtr> {
        if (!anns_proto.has_predicates()) {
            return std::nullopt;
        } else {
            return ParseExpr(anns_proto.predicates());
        }
    }();

    auto expr_parser = [&]() -> plan::PlanNodePtr {
        auto expr = ParseExprs(anns_proto.predicates());
        return std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                      expr);
    };

    auto& query_info_proto = anns_proto.query_info();

    SearchInfo search_info;
    auto field_id = FieldId(anns_proto.field_id());
    search_info.field_id_ = field_id;

    search_info.metric_type_ = query_info_proto.metric_type();
    search_info.topk_ = query_info_proto.topk();
    search_info.round_decimal_ = query_info_proto.round_decimal();
    search_info.search_params_ =
        nlohmann::json::parse(query_info_proto.search_params());
    search_info.materialized_view_involved =
        query_info_proto.materialized_view_involved();

    if (query_info_proto.group_by_field_id() > 0) {
        auto group_by_field_id = FieldId(query_info_proto.group_by_field_id());
        search_info.group_by_field_id_ = group_by_field_id;
        search_info.group_size_ = query_info_proto.group_size() > 0
                                      ? query_info_proto.group_size()
                                      : 1;
        search_info.group_strict_size_ = query_info_proto.group_strict_size();
    }

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
        } else {
            return std::make_unique<FloatVectorANNS>();
        }
    }();
    plan_node->placeholder_tag_ = anns_proto.placeholder_tag();
    plan_node->predicate_ = std::move(expr_opt);
    if (anns_proto.has_predicates()) {
        plan_node->filter_plannode_ = std::move(expr_parser());
    }
    plan_node->search_info_ = std::move(search_info);

    if (plan_node->search_info_.materialized_view_involved &&
        plan_node->filter_plannode_.has_value()) {
        const auto expr_info =
            plan_node->filter_plannode_.value()->GatherInfo();
        knowhere::MaterializedViewSearchInfo materialized_view_search_info;
        for (const auto& [expr_field_id, vals] : expr_info.field_id_to_values) {
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

    return plan_node;
}

std::unique_ptr<RetrievePlanNode>
ProtoParser::RetrievePlanNodeFromProto(
    const planpb::PlanNode& plan_node_proto) {
    Assert(plan_node_proto.has_predicates() || plan_node_proto.has_query());

    auto plan_node = [&]() -> std::unique_ptr<RetrievePlanNode> {
        auto node = std::make_unique<RetrievePlanNode>();
        if (plan_node_proto.has_predicates()) {  // version before 2023.03.30.
            node->is_count_ = false;
            auto& predicate_proto = plan_node_proto.predicates();
            auto expr_opt = [&]() -> ExprPtr {
                return ParseExpr(predicate_proto);
            }();
            auto expr_parser = [&]() -> plan::PlanNodePtr {
                auto expr = ParseExprs(predicate_proto);
                return std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, expr);
            }();
            node->predicate_ = std::move(expr_opt);
            node->filter_plannode_ = std::move(expr_parser);
        } else {
            auto& query = plan_node_proto.query();
            if (query.has_predicates()) {
                auto& predicate_proto = query.predicates();
                auto expr_opt = [&]() -> ExprPtr {
                    return ParseExpr(predicate_proto);
                }();
                auto expr_parser = [&]() -> plan::PlanNodePtr {
                    auto expr = ParseExprs(predicate_proto);
                    return std::make_shared<plan::FilterBitsNode>(
                        DEFAULT_PLANNODE_ID, expr);
                }();
                node->predicate_ = std::move(expr_opt);
                node->filter_plannode_ = std::move(expr_parser);
            }
            node->is_count_ = query.is_count();
            node->limit_ = query.limit();
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
    ExtractedPlanInfo plan_info(schema.size());
    ExtractInfoPlanNodeVisitor extractor(plan_info);
    plan_node->accept(extractor);

    plan->tag2field_["$0"] = plan_node->search_info_.field_id_;
    plan->plan_node_ = std::move(plan_node);
    plan->extra_info_opt_ = std::move(plan_info);

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
    ExtractedPlanInfo plan_info(schema.size());
    ExtractInfoPlanNodeVisitor extractor(plan_info);
    plan_node->accept(extractor);

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
    return std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(column_info), expr_pb.op(), expr_pb.value());
}

ExprPtr
ProtoParser::ParseUnaryRangeExpr(const proto::plan::UnaryRangeExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));

    auto result = [&]() -> ExprPtr {
        switch (data_type) {
            case DataType::BOOL: {
                return ExtractUnaryRangeExprImpl<bool>(
                    field_id, data_type, expr_pb);
            }

            // see also: https://github.com/milvus-io/milvus/issues/23646.
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64: {
                return ExtractUnaryRangeExprImpl<int64_t>(
                    field_id, data_type, expr_pb);
            }

            case DataType::FLOAT: {
                return ExtractUnaryRangeExprImpl<float>(
                    field_id, data_type, expr_pb);
            }
            case DataType::DOUBLE: {
                return ExtractUnaryRangeExprImpl<double>(
                    field_id, data_type, expr_pb);
            }
            case DataType::VARCHAR: {
                return ExtractUnaryRangeExprImpl<std::string>(
                    field_id, data_type, expr_pb);
            }
            case DataType::JSON:
            case DataType::ARRAY: {
                switch (expr_pb.value().val_case()) {
                    case proto::plan::GenericValue::ValCase::kBoolVal:
                        return ExtractUnaryRangeExprImpl<bool>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        return ExtractUnaryRangeExprImpl<double>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        return ExtractUnaryRangeExprImpl<int64_t>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kStringVal:
                        return ExtractUnaryRangeExprImpl<std::string>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kArrayVal:
                        return ExtractUnaryRangeExprImpl<proto::plan::Array>(
                            field_id, data_type, expr_pb);
                    default:
                        PanicInfo(
                            DataTypeInvalid,
                            fmt::format("unknown data type: {} in expression",
                                        expr_pb.value().val_case()));
                }
            }
            default: {
                PanicInfo(DataTypeInvalid, "unsupported data type");
            }
        }
    }();
    return result;
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

ExprPtr
ProtoParser::ParseBinaryRangeExpr(const proto::plan::BinaryRangeExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == (DataType)columnInfo.data_type());

    auto result = [&]() -> ExprPtr {
        switch (data_type) {
            case DataType::BOOL: {
                return ExtractBinaryRangeExprImpl<bool>(
                    field_id, data_type, expr_pb);
            }

            // see also: https://github.com/milvus-io/milvus/issues/23646.
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64: {
                return ExtractBinaryRangeExprImpl<int64_t>(
                    field_id, data_type, expr_pb);
            }

            case DataType::FLOAT: {
                return ExtractBinaryRangeExprImpl<float>(
                    field_id, data_type, expr_pb);
            }
            case DataType::DOUBLE: {
                return ExtractBinaryRangeExprImpl<double>(
                    field_id, data_type, expr_pb);
            }
            case DataType::VARCHAR: {
                return ExtractBinaryRangeExprImpl<std::string>(
                    field_id, data_type, expr_pb);
            }
            case DataType::JSON: {
                switch (expr_pb.lower_value().val_case()) {
                    case proto::plan::GenericValue::ValCase::kBoolVal:
                        return ExtractBinaryRangeExprImpl<bool>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        return ExtractBinaryRangeExprImpl<int64_t>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        return ExtractBinaryRangeExprImpl<double>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kStringVal:
                        return ExtractBinaryRangeExprImpl<std::string>(
                            field_id, data_type, expr_pb);
                    default:
                        PanicInfo(
                            DataTypeInvalid,
                            fmt::format("unknown data type in expression {}",
                                        data_type));
                }
            }
            case DataType::ARRAY: {
                switch (expr_pb.lower_value().val_case()) {
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        return ExtractBinaryRangeExprImpl<int64_t>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        return ExtractBinaryRangeExprImpl<double>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kStringVal:
                        return ExtractBinaryRangeExprImpl<std::string>(
                            field_id, data_type, expr_pb);
                    default:
                        PanicInfo(
                            DataTypeInvalid,
                            fmt::format("unknown data type in expression {}",
                                        data_type));
                }
            }

            default: {
                PanicInfo(
                    DataTypeInvalid, "unsupported data type {}", data_type);
            }
        }
    }();
    return result;
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

ExprPtr
ProtoParser::ParseCompareExpr(const proto::plan::CompareExpr& expr_pb) {
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

    return [&]() -> ExprPtr {
        auto result = std::make_unique<CompareExpr>();
        result->left_field_id_ = left_field_id;
        result->left_data_type_ = left_data_type;
        result->right_field_id_ = right_field_id;
        result->right_data_type_ = right_data_type;
        result->op_type_ = static_cast<OpType>(expr_pb.op());
        return result;
    }();
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

ExprPtr
ProtoParser::ParseTermExpr(const proto::plan::TermExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == (DataType)columnInfo.data_type());

    // auto& field_meta = schema[field_offset];
    auto result = [&]() -> ExprPtr {
        switch (data_type) {
            case DataType::BOOL: {
                return ExtractTermExprImpl<bool>(field_id, data_type, expr_pb);
            }
            case DataType::INT8: {
                return ExtractTermExprImpl<int8_t>(
                    field_id, data_type, expr_pb);
            }
            case DataType::INT16: {
                return ExtractTermExprImpl<int16_t>(
                    field_id, data_type, expr_pb);
            }
            case DataType::INT32: {
                return ExtractTermExprImpl<int32_t>(
                    field_id, data_type, expr_pb);
            }
            case DataType::INT64: {
                return ExtractTermExprImpl<int64_t>(
                    field_id, data_type, expr_pb);
            }
            case DataType::FLOAT: {
                return ExtractTermExprImpl<float>(field_id, data_type, expr_pb);
            }
            case DataType::DOUBLE: {
                return ExtractTermExprImpl<double>(
                    field_id, data_type, expr_pb);
            }
            case DataType::VARCHAR: {
                return ExtractTermExprImpl<std::string>(
                    field_id, data_type, expr_pb);
            }
            case DataType::JSON: {
                if (expr_pb.values().size() == 0) {
                    return ExtractTermExprImpl<bool>(
                        field_id, data_type, expr_pb);
                }
                switch (expr_pb.values()[0].val_case()) {
                    case proto::plan::GenericValue::ValCase::kBoolVal:
                        return ExtractTermExprImpl<bool>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        return ExtractTermExprImpl<double>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        return ExtractTermExprImpl<int64_t>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kStringVal:
                        return ExtractTermExprImpl<std::string>(
                            field_id, data_type, expr_pb);
                    default:
                        PanicInfo(
                            DataTypeInvalid,
                            fmt::format("unknown data type: {} in expression",
                                        expr_pb.values()[0].val_case()));
                }
            }
            case DataType::ARRAY: {
                if (expr_pb.values().size() == 0) {
                    return ExtractTermExprImpl<bool>(
                        field_id, data_type, expr_pb);
                }
                switch (expr_pb.values()[0].val_case()) {
                    case proto::plan::GenericValue::ValCase::kBoolVal:
                        return ExtractTermExprImpl<bool>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        return ExtractTermExprImpl<double>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        return ExtractTermExprImpl<int64_t>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kStringVal:
                        return ExtractTermExprImpl<std::string>(
                            field_id, data_type, expr_pb);
                    default:
                        PanicInfo(
                            DataTypeInvalid,
                            fmt::format("unknown data type: {} in expression",
                                        expr_pb.values()[0].val_case()));
                }
            }
            default: {
                PanicInfo(
                    DataTypeInvalid, "unsupported data type {}", data_type);
            }
        }
    }();
    return result;
}

ExprPtr
ProtoParser::ParseUnaryExpr(const proto::plan::UnaryExpr& expr_pb) {
    auto op = static_cast<LogicalUnaryExpr::OpType>(expr_pb.op());
    Assert(op == LogicalUnaryExpr::OpType::LogicalNot);
    auto expr = this->ParseExpr(expr_pb.child());
    return std::make_unique<LogicalUnaryExpr>(op, expr);
}

expr::TypedExprPtr
ProtoParser::ParseUnaryExprs(const proto::plan::UnaryExpr& expr_pb) {
    auto op = static_cast<expr::LogicalUnaryExpr::OpType>(expr_pb.op());
    Assert(op == expr::LogicalUnaryExpr::OpType::LogicalNot);
    auto child_expr = this->ParseExprs(expr_pb.child());
    return std::make_shared<expr::LogicalUnaryExpr>(op, child_expr);
}

ExprPtr
ProtoParser::ParseBinaryExpr(const proto::plan::BinaryExpr& expr_pb) {
    auto op = static_cast<LogicalBinaryExpr::OpType>(expr_pb.op());
    auto left_expr = this->ParseExpr(expr_pb.left());
    auto right_expr = this->ParseExpr(expr_pb.right());
    return std::make_unique<LogicalBinaryExpr>(op, left_expr, right_expr);
}

expr::TypedExprPtr
ProtoParser::ParseBinaryExprs(const proto::plan::BinaryExpr& expr_pb) {
    auto op = static_cast<expr::LogicalBinaryExpr::OpType>(expr_pb.op());
    auto left_expr = this->ParseExprs(expr_pb.left());
    auto right_expr = this->ParseExprs(expr_pb.right());
    return std::make_shared<expr::LogicalBinaryExpr>(op, left_expr, right_expr);
}

ExprPtr
ProtoParser::ParseBinaryArithOpEvalRangeExpr(
    const proto::plan::BinaryArithOpEvalRangeExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));

    auto result = [&]() -> ExprPtr {
        switch (data_type) {
            // see also: https://github.com/milvus-io/milvus/issues/23646.
            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64: {
                return ExtractBinaryArithOpEvalRangeExprImpl<int64_t>(
                    field_id, data_type, expr_pb);
            }

            case DataType::FLOAT: {
                return ExtractBinaryArithOpEvalRangeExprImpl<float>(
                    field_id, data_type, expr_pb);
            }
            case DataType::DOUBLE: {
                return ExtractBinaryArithOpEvalRangeExprImpl<double>(
                    field_id, data_type, expr_pb);
            }
            case DataType::JSON: {
                switch (expr_pb.value().val_case()) {
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        return ExtractBinaryArithOpEvalRangeExprImpl<int64_t>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        return ExtractBinaryArithOpEvalRangeExprImpl<double>(
                            field_id, data_type, expr_pb);
                    default:
                        PanicInfo(DataTypeInvalid,
                                  "unsupported data type {} in expression",
                                  expr_pb.value().val_case());
                }
            }
            case DataType::ARRAY: {
                switch (expr_pb.value().val_case()) {
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        return ExtractBinaryArithOpEvalRangeExprImpl<int64_t>(
                            field_id, data_type, expr_pb);
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        return ExtractBinaryArithOpEvalRangeExprImpl<double>(
                            field_id, data_type, expr_pb);
                    default:
                        PanicInfo(DataTypeInvalid,
                                  "unsupported data type {} in expression",
                                  expr_pb.value().val_case());
                }
            }
            default: {
                PanicInfo(
                    DataTypeInvalid, "unsupported data type {}", data_type);
            }
        }
    }();
    return result;
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

std::unique_ptr<ExistsExprImpl>
ExtractExistsExprImpl(const proto::plan::ExistsExpr& expr_proto) {
    return std::make_unique<ExistsExprImpl>(expr_proto.info());
}

expr::TypedExprPtr
ProtoParser::ParseExistExprs(const proto::plan::ExistsExpr& expr_pb) {
    auto& column_info = expr_pb.info();
    auto field_id = FieldId(column_info.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));
    return std::make_shared<expr::ExistsExpr>(column_info);
}

ExprPtr
ProtoParser::ParseExistExpr(const proto::plan::ExistsExpr& expr_pb) {
    auto& column_info = expr_pb.info();
    auto field_id = FieldId(column_info.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));

    auto result = [&]() -> ExprPtr {
        switch (data_type) {
            case DataType::JSON: {
                return ExtractExistsExprImpl(expr_pb);
            }
            default: {
                PanicInfo(
                    DataTypeInvalid, "unsupported data type {}", data_type);
            }
        }
    }();
    return result;
}

template <typename T>
std::unique_ptr<JsonContainsExprImpl<T>>
ExtractJsonContainsExprImpl(const proto::plan::JSONContainsExpr& expr_proto) {
    static_assert(IsScalar<T> or std::is_same_v<T, proto::plan::GenericValue> or
                  std::is_same_v<T, proto::plan::Array>);
    auto size = expr_proto.elements_size();
    std::vector<T> terms;
    terms.reserve(size);
    auto val_case = proto::plan::GenericValue::VAL_NOT_SET;
    for (int i = 0; i < size; ++i) {
        auto& value_proto = expr_proto.elements(i);
        if constexpr (std::is_same_v<T, bool>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kBoolVal);
            terms.push_back(static_cast<T>(value_proto.bool_val()));
            val_case = proto::plan::GenericValue::ValCase::kBoolVal;
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kInt64Val);
            auto value = value_proto.int64_val();
            if (out_of_range<T>(value)) {
                continue;
            }
            terms.push_back(static_cast<T>(value));
            val_case = proto::plan::GenericValue::ValCase::kInt64Val;
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kFloatVal);
            terms.push_back(static_cast<T>(value_proto.float_val()));
            val_case = proto::plan::GenericValue::ValCase::kFloatVal;
        } else if constexpr (std::is_same_v<T, std::string>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kStringVal);
            terms.push_back(static_cast<T>(value_proto.string_val()));
            val_case = proto::plan::GenericValue::ValCase::kStringVal;
        } else if constexpr (std::is_same_v<T, proto::plan::Array>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kArrayVal);
            terms.push_back(static_cast<T>(value_proto.array_val()));
            val_case = proto::plan::GenericValue::ValCase::kArrayVal;
        } else if constexpr (std::is_same_v<T, proto::plan::GenericValue>) {
            terms.push_back(value_proto);
        } else {
            static_assert(always_false<T>);
        }
    }

    return std::make_unique<JsonContainsExprImpl<T>>(
        expr_proto.column_info(),
        terms,
        expr_proto.elements_same_type(),
        expr_proto.op(),
        val_case);
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

ExprPtr
ProtoParser::ParseJsonContainsExpr(
    const proto::plan::JSONContainsExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto data_type = schema[field_id].get_data_type();
    Assert(data_type == (DataType)columnInfo.data_type());

    // auto& field_meta = schema[field_offset];
    auto result = [&]() -> ExprPtr {
        if (expr_pb.elements_size() == 0) {
            PanicInfo(DataIsEmpty, "no elements in expression");
        }
        if (expr_pb.elements_same_type()) {
            switch (expr_pb.elements(0).val_case()) {
                case proto::plan::GenericValue::kBoolVal:
                    return ExtractJsonContainsExprImpl<bool>(expr_pb);
                case proto::plan::GenericValue::kInt64Val:
                    return ExtractJsonContainsExprImpl<int64_t>(expr_pb);
                case proto::plan::GenericValue::kFloatVal:
                    return ExtractJsonContainsExprImpl<double>(expr_pb);
                case proto::plan::GenericValue::kStringVal:
                    return ExtractJsonContainsExprImpl<std::string>(expr_pb);
                case proto::plan::GenericValue::kArrayVal:
                    return ExtractJsonContainsExprImpl<proto::plan::Array>(
                        expr_pb);
                default:
                    PanicInfo(
                        DataTypeInvalid,
                        fmt::format("unsupported data type {}", data_type));
            }
        }
        return ExtractJsonContainsExprImpl<proto::plan::GenericValue>(expr_pb);
    }();
    return result;
}

expr::TypedExprPtr
ProtoParser::CreateAlwaysTrueExprs() {
    return std::make_shared<expr::AlwaysTrueExpr>();
}

expr::TypedExprPtr
ProtoParser::ParseExprs(const proto::plan::Expr& expr_pb) {
    using ppe = proto::plan::Expr;
    switch (expr_pb.expr_case()) {
        case ppe::kUnaryRangeExpr: {
            return ParseUnaryRangeExprs(expr_pb.unary_range_expr());
        }
        case ppe::kBinaryExpr: {
            return ParseBinaryExprs(expr_pb.binary_expr());
        }
        case ppe::kUnaryExpr: {
            return ParseUnaryExprs(expr_pb.unary_expr());
        }
        case ppe::kTermExpr: {
            return ParseTermExprs(expr_pb.term_expr());
        }
        case ppe::kBinaryRangeExpr: {
            return ParseBinaryRangeExprs(expr_pb.binary_range_expr());
        }
        case ppe::kCompareExpr: {
            return ParseCompareExprs(expr_pb.compare_expr());
        }
        case ppe::kBinaryArithOpEvalRangeExpr: {
            return ParseBinaryArithOpEvalRangeExprs(
                expr_pb.binary_arith_op_eval_range_expr());
        }
        case ppe::kExistsExpr: {
            return ParseExistExprs(expr_pb.exists_expr());
        }
        case ppe::kAlwaysTrueExpr: {
            return CreateAlwaysTrueExprs();
        }
        case ppe::kJsonContainsExpr: {
            return ParseJsonContainsExprs(expr_pb.json_contains_expr());
        }
        default: {
            std::string s;
            google::protobuf::TextFormat::PrintToString(expr_pb, &s);
            PanicInfo(ExprInvalid,
                      std::string("unsupported expr proto node: ") + s);
        }
    }
}

ExprPtr
ProtoParser::ParseExpr(const proto::plan::Expr& expr_pb) {
    using ppe = proto::plan::Expr;
    switch (expr_pb.expr_case()) {
        case ppe::kBinaryExpr: {
            return ParseBinaryExpr(expr_pb.binary_expr());
        }
        case ppe::kUnaryExpr: {
            return ParseUnaryExpr(expr_pb.unary_expr());
        }
        case ppe::kTermExpr: {
            return ParseTermExpr(expr_pb.term_expr());
        }
        case ppe::kUnaryRangeExpr: {
            return ParseUnaryRangeExpr(expr_pb.unary_range_expr());
        }
        case ppe::kBinaryRangeExpr: {
            return ParseBinaryRangeExpr(expr_pb.binary_range_expr());
        }
        case ppe::kCompareExpr: {
            return ParseCompareExpr(expr_pb.compare_expr());
        }
        case ppe::kBinaryArithOpEvalRangeExpr: {
            return ParseBinaryArithOpEvalRangeExpr(
                expr_pb.binary_arith_op_eval_range_expr());
        }
        case ppe::kExistsExpr: {
            return ParseExistExpr(expr_pb.exists_expr());
        }
        case ppe::kAlwaysTrueExpr: {
            return CreateAlwaysTrueExpr();
        }
        case ppe::kJsonContainsExpr: {
            return ParseJsonContainsExpr(expr_pb.json_contains_expr());
        }
        default: {
            std::string s;
            google::protobuf::TextFormat::PrintToString(expr_pb, &s);
            PanicInfo(ExprInvalid, "unsupported expr proto node: {}", s);
        }
    }
}

}  // namespace milvus::query
