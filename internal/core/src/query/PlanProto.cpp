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

#include "query/PlanProto.h"
#include "ExprImpl.h"
#include <google/protobuf/text_format.h>
#include <query/generated/ExtractInfoPlanNodeVisitor.h>
#include "query/generated/ExtractInfoExprVisitor.h"

namespace milvus::query {
namespace planpb = milvus::proto::plan;

template <typename T>
std::unique_ptr<TermExprImpl<T>>
ExtractTermExprImpl(FieldOffset field_offset, DataType data_type, const planpb::TermExpr& expr_proto) {
    static_assert(std::is_fundamental_v<T>);
    auto result = std::make_unique<TermExprImpl<T>>();
    result->field_offset_ = field_offset;
    result->data_type_ = data_type;
    auto size = expr_proto.values_size();
    for (int i = 0; i < size; ++i) {
        auto& value_proto = expr_proto.values(i);
        if constexpr (std::is_same_v<T, bool>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kBoolVal);
            result->terms_.emplace_back(static_cast<T>(value_proto.bool_val()));
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kInt64Val);
            result->terms_.emplace_back(static_cast<T>(value_proto.int64_val()));
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kFloatVal);
            result->terms_.emplace_back(static_cast<T>(value_proto.float_val()));
        } else {
            static_assert(always_false<T>);
        }
    }
    return result;
}

template <typename T>
std::unique_ptr<UnaryRangeExprImpl<T>>
ExtractUnaryRangeExprImpl(FieldOffset field_offset, DataType data_type, const planpb::UnaryRangeExpr& expr_proto) {
    static_assert(std::is_fundamental_v<T>);
    auto result = std::make_unique<UnaryRangeExprImpl<T>>();
    result->field_offset_ = field_offset;
    result->data_type_ = data_type;
    result->op_type_ = static_cast<OpType>(expr_proto.op());

    auto setValue = [&](T& v, const auto& value_proto) {
        if constexpr (std::is_same_v<T, bool>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kBoolVal);
            v = static_cast<T>(value_proto.bool_val());
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kInt64Val);
            v = static_cast<T>(value_proto.int64_val());
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kFloatVal);
            v = static_cast<T>(value_proto.float_val());
        } else {
            static_assert(always_false<T>);
        }
    };
    setValue(result->value_, expr_proto.value());
    return result;
}

template <typename T>
std::unique_ptr<BinaryRangeExprImpl<T>>
ExtractBinaryRangeExprImpl(FieldOffset field_offset, DataType data_type, const planpb::BinaryRangeExpr& expr_proto) {
    static_assert(std::is_fundamental_v<T>);
    auto result = std::make_unique<BinaryRangeExprImpl<T>>();
    result->field_offset_ = field_offset;
    result->data_type_ = data_type;

    auto setValue = [&](T& v, const auto& value_proto) {
        if constexpr (std::is_same_v<T, bool>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kBoolVal);
            v = static_cast<T>(value_proto.bool_val());
        } else if constexpr (std::is_integral_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kInt64Val);
            v = static_cast<T>(value_proto.int64_val());
        } else if constexpr (std::is_floating_point_v<T>) {
            Assert(value_proto.val_case() == planpb::GenericValue::kFloatVal);
            v = static_cast<T>(value_proto.float_val());
        } else {
            static_assert(always_false<T>);
        }
    };
    setValue(result->lower_value_, expr_proto.lower_value());
    setValue(result->upper_value_, expr_proto.upper_value());
    result->lower_inclusive_ = expr_proto.lower_inclusive();
    result->upper_inclusive_ = expr_proto.upper_inclusive();
    return result;
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

    auto& query_info_proto = anns_proto.query_info();

    SearchInfo search_info;
    auto field_id = FieldId(anns_proto.field_id());
    auto field_offset = schema.get_offset(field_id);
    search_info.field_offset_ = field_offset;

    search_info.metric_type_ = GetMetricType(query_info_proto.metric_type());
    search_info.topk_ = query_info_proto.topk();
    search_info.round_decimal_ = query_info_proto.round_decimal();
    search_info.search_params_ = json::parse(query_info_proto.search_params());

    auto plan_node = [&]() -> std::unique_ptr<VectorPlanNode> {
        if (anns_proto.is_binary()) {
            return std::make_unique<BinaryVectorANNS>();
        } else {
            return std::make_unique<FloatVectorANNS>();
        }
    }();
    plan_node->placeholder_tag_ = anns_proto.placeholder_tag();
    plan_node->predicate_ = std::move(expr_opt);
    plan_node->search_info_ = std::move(search_info);
    return plan_node;
}

std::unique_ptr<RetrievePlanNode>
ProtoParser::RetrievePlanNodeFromProto(const planpb::PlanNode& plan_node_proto) {
    Assert(plan_node_proto.has_predicates());
    auto& predicate_proto = plan_node_proto.predicates();
    auto expr_opt = [&]() -> ExprPtr { return ParseExpr(predicate_proto); }();

    auto plan_node = [&]() -> std::unique_ptr<RetrievePlanNode> { return std::make_unique<RetrievePlanNode>(); }();
    plan_node->predicate_ = std::move(expr_opt);
    return plan_node;
}

std::unique_ptr<Plan>
ProtoParser::CreatePlan(const proto::plan::PlanNode& plan_node_proto) {
    auto plan = std::make_unique<Plan>(schema);

    auto plan_node = PlanNodeFromProto(plan_node_proto);
    ExtractedPlanInfo plan_info(schema.size());
    ExtractInfoPlanNodeVisitor extractor(plan_info);
    plan_node->accept(extractor);

    plan->tag2field_["$0"] = plan_node->search_info_.field_offset_;
    plan->plan_node_ = std::move(plan_node);
    plan->extra_info_opt_ = std::move(plan_info);

    for (auto field_id_raw : plan_node_proto.output_field_ids()) {
        auto field_id = FieldId(field_id_raw);
        auto offset = schema.get_offset(field_id);
        plan->target_entries_.push_back(offset);
    }

    return plan;
}

std::unique_ptr<RetrievePlan>
ProtoParser::CreateRetrievePlan(const proto::plan::PlanNode& plan_node_proto) {
    auto retrieve_plan = std::make_unique<RetrievePlan>(schema);

    auto plan_node = RetrievePlanNodeFromProto(plan_node_proto);
    ExtractedPlanInfo plan_info(schema.size());
    ExtractInfoPlanNodeVisitor extractor(plan_info);
    plan_node->accept(extractor);

    retrieve_plan->plan_node_ = std::move(plan_node);
    for (auto field_id_raw : plan_node_proto.output_field_ids()) {
        auto field_id = FieldId(field_id_raw);
        auto offset = schema.get_offset(field_id);
        retrieve_plan->field_offsets_.push_back(offset);
    }
    return retrieve_plan;
}

ExprPtr
ProtoParser::ParseUnaryRangeExpr(const proto::plan::UnaryRangeExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto field_offset = schema.get_offset(field_id);
    auto data_type = schema[field_offset].get_data_type();
    Assert(data_type == static_cast<DataType>(column_info.data_type()));

    auto result = [&]() -> ExprPtr {
        switch (data_type) {
            case DataType::BOOL: {
                return ExtractUnaryRangeExprImpl<bool>(field_offset, data_type, expr_pb);
            }
            case DataType::INT8: {
                return ExtractUnaryRangeExprImpl<int8_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT16: {
                return ExtractUnaryRangeExprImpl<int16_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT32: {
                return ExtractUnaryRangeExprImpl<int32_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT64: {
                return ExtractUnaryRangeExprImpl<int64_t>(field_offset, data_type, expr_pb);
            }
            case DataType::FLOAT: {
                return ExtractUnaryRangeExprImpl<float>(field_offset, data_type, expr_pb);
            }
            case DataType::DOUBLE: {
                return ExtractUnaryRangeExprImpl<double>(field_offset, data_type, expr_pb);
            }
            default: {
                PanicInfo("unsupported data type");
            }
        }
    }();
    return result;
}

ExprPtr
ProtoParser::ParseBinaryRangeExpr(const proto::plan::BinaryRangeExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto field_offset = schema.get_offset(field_id);
    auto data_type = schema[field_offset].get_data_type();
    Assert(data_type == (DataType)columnInfo.data_type());

    auto result = [&]() -> ExprPtr {
        switch (data_type) {
            case DataType::BOOL: {
                return ExtractBinaryRangeExprImpl<bool>(field_offset, data_type, expr_pb);
            }
            case DataType::INT8: {
                return ExtractBinaryRangeExprImpl<int8_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT16: {
                return ExtractBinaryRangeExprImpl<int16_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT32: {
                return ExtractBinaryRangeExprImpl<int32_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT64: {
                return ExtractBinaryRangeExprImpl<int64_t>(field_offset, data_type, expr_pb);
            }
            case DataType::FLOAT: {
                return ExtractBinaryRangeExprImpl<float>(field_offset, data_type, expr_pb);
            }
            case DataType::DOUBLE: {
                return ExtractBinaryRangeExprImpl<double>(field_offset, data_type, expr_pb);
            }
            default: {
                PanicInfo("unsupported data type");
            }
        }
    }();
    return result;
}

ExprPtr
ProtoParser::ParseCompareExpr(const proto::plan::CompareExpr& expr_pb) {
    auto& left_column_info = expr_pb.left_column_info();
    auto left_field_id = FieldId(left_column_info.field_id());
    auto left_field_offset = schema.get_offset(left_field_id);
    auto left_data_type = schema[left_field_offset].get_data_type();
    Assert(left_data_type == static_cast<DataType>(left_column_info.data_type()));

    auto& right_column_info = expr_pb.right_column_info();
    auto right_field_id = FieldId(right_column_info.field_id());
    auto right_field_offset = schema.get_offset(right_field_id);
    auto right_data_type = schema[right_field_offset].get_data_type();
    Assert(right_data_type == static_cast<DataType>(right_column_info.data_type()));

    return [&]() -> ExprPtr {
        auto result = std::make_unique<CompareExpr>();
        result->left_field_offset_ = left_field_offset;
        result->left_data_type_ = left_data_type;
        result->right_field_offset_ = right_field_offset;
        result->right_data_type_ = right_data_type;
        result->op_type_ = static_cast<OpType>(expr_pb.op());
        return result;
    }();
}

ExprPtr
ProtoParser::ParseTermExpr(const proto::plan::TermExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto field_offset = schema.get_offset(field_id);
    auto data_type = schema[field_offset].get_data_type();
    Assert(data_type == (DataType)columnInfo.data_type());

    // auto& field_meta = schema[field_offset];
    auto result = [&]() -> ExprPtr {
        switch (data_type) {
            case DataType::BOOL: {
                return ExtractTermExprImpl<bool>(field_offset, data_type, expr_pb);
            }
            case DataType::INT8: {
                return ExtractTermExprImpl<int8_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT16: {
                return ExtractTermExprImpl<int16_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT32: {
                return ExtractTermExprImpl<int32_t>(field_offset, data_type, expr_pb);
            }
            case DataType::INT64: {
                return ExtractTermExprImpl<int64_t>(field_offset, data_type, expr_pb);
            }
            case DataType::FLOAT: {
                return ExtractTermExprImpl<float>(field_offset, data_type, expr_pb);
            }
            case DataType::DOUBLE: {
                return ExtractTermExprImpl<double>(field_offset, data_type, expr_pb);
            }
            default: {
                PanicInfo("unsupported data type");
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
    auto result = std::make_unique<LogicalUnaryExpr>();
    result->child_ = std::move(expr);
    result->op_type_ = op;
    return result;
}

ExprPtr
ProtoParser::ParseBinaryExpr(const proto::plan::BinaryExpr& expr_pb) {
    auto op = static_cast<LogicalBinaryExpr::OpType>(expr_pb.op());
    auto left_expr = this->ParseExpr(expr_pb.left());
    auto right_expr = this->ParseExpr(expr_pb.right());
    auto result = std::make_unique<LogicalBinaryExpr>();
    result->op_type_ = op;
    result->left_ = std::move(left_expr);
    result->right_ = std::move(right_expr);
    return result;
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
        default:
            PanicInfo("unsupported expr proto node");
    }
}

}  // namespace milvus::query
