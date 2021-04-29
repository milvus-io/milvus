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
#include "PlanNode.h"
#include "ExprImpl.h"
#include "pb/plan.pb.h"
#include <google/protobuf/text_format.h>
#include "query/generated/ExtractInfoExprVisitor.h"

namespace milvus::query {
namespace planpb = milvus::proto::plan;

ExprPtr
ProtoParser::ExprFromProto(const planpb::Expr& expr_proto) {
    // TODO: make naive works
    Assert(expr_proto.has_range_expr());

    auto& range_expr = expr_proto.range_expr();
    auto& columen_info = range_expr.column_info();
    auto field_id = FieldId(columen_info.field_id());
    auto field_offset = schema.get_offset(field_id);
    auto data_type = schema[field_offset].get_data_type();
    involved_fields.set(field_offset.get(), true);

    // auto& field_meta = schema[field_offset];
    ExprPtr result = [&]() {
        switch ((DataType)columen_info.data_type()) {
            case DataType::INT64: {
                auto result = std::make_unique<RangeExprImpl<int64_t>>();
                result->field_offset_ = field_offset;
                result->data_type_ = data_type;
                Assert(range_expr.ops_size() == range_expr.values_size());
                auto sz = range_expr.ops_size();
                // TODO simplify this
                for (int i = 0; i < sz; ++i) {
                    result->conditions_.emplace_back((RangeExpr::OpType)range_expr.ops(i),
                                                     range_expr.values(i).int64_val());
                }
                return result;
            }
            default: {
                PanicInfo("unsupported yet");
            }
        }
    }();
    return result;
}

std::unique_ptr<VectorPlanNode>
ProtoParser::PlanNodeFromProto(const planpb::PlanNode& plan_node_proto) {
    // TODO: add more buffs
    Assert(plan_node_proto.has_vector_anns());
    auto& anns_proto = plan_node_proto.vector_anns();
    AssertInfo(anns_proto.is_binary() == false, "unimplemented");
    auto expr_opt = [&]() -> std::optional<ExprPtr> {
        if (!anns_proto.has_predicates()) {
            return std::nullopt;
        } else {
            return ExprFromProto(anns_proto.predicates());
        }
    }();

    auto& query_info_proto = anns_proto.query_info();

    QueryInfo query_info;
    auto field_id = FieldId(anns_proto.field_id());
    auto field_offset = schema.get_offset(field_id);
    query_info.field_offset_ = field_offset;
    this->involved_fields.set(field_offset.get(), true);

    query_info.metric_type_ = GetMetricType(query_info_proto.metric_type());
    query_info.topK_ = query_info_proto.topk();
    query_info.search_params_ = json::parse(query_info_proto.search_params());

    auto plan_node = [&]() -> std::unique_ptr<VectorPlanNode> {
        if (anns_proto.is_binary()) {
            return std::make_unique<BinaryVectorANNS>();
        } else {
            return std::make_unique<FloatVectorANNS>();
        }
    }();
    plan_node->placeholder_tag_ = anns_proto.placeholder_tag();
    plan_node->predicate_ = std::move(expr_opt);
    plan_node->query_info_ = std::move(query_info);
    return plan_node;
}

std::unique_ptr<Plan>
ProtoParser::CreatePlan(const proto::plan::PlanNode& plan_node_proto) {
    auto plan = std::make_unique<Plan>(schema);

    auto plan_node = PlanNodeFromProto(plan_node_proto);
    plan->tag2field_["$0"] = plan_node->query_info_.field_offset_;
    plan->plan_node_ = std::move(plan_node);
    ExtractedPlanInfo extract_info(schema.size());
    extract_info.involved_fields_ = std::move(involved_fields);
    plan->extra_info_opt_ = std::move(extract_info);
    return plan;
}

}  // namespace milvus::query
