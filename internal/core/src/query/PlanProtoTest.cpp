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

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "query/PlanProto.h"

namespace {

milvus::SchemaPtr
BuildSchema() {
    auto schema = std::make_shared<milvus::Schema>();
    schema->AddDebugField(
        "fakevec", milvus::DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", milvus::DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    schema->AddDebugField("score", milvus::DataType::INT64);
    return schema;
}

milvus::proto::plan::PlanNode
BuildSearchPlanNode(float search_topk_ratio,
                    float refine_topk_ratio,
                    milvus::FieldId vector_field_id) {
    milvus::proto::plan::PlanNode plan_node;
    auto* vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(vector_field_id.get());

    auto* query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(-1);
    query_info->set_metric_type(knowhere::metric::L2);
    query_info->set_search_params("{}");
    query_info->set_search_topk_ratio(search_topk_ratio);
    query_info->set_refine_topk_ratio(refine_topk_ratio);

    return plan_node;
}

}  // namespace

TEST(PlanProto, NotSetUnsupported) {
    using namespace milvus;
    using namespace milvus::query;
    auto schema = BuildSchema();

    proto::plan::Expr expr_pb;
    ProtoParser parser(schema);
    ASSERT_ANY_THROW(parser.ParseExprs(expr_pb));
}

TEST(PlanProto, RejectsGlobalRefineRatiosBelowOne) {
    using namespace milvus::query;

    auto schema = BuildSchema();
    auto vector_field_id = schema->get_field_id(milvus::FieldName("fakevec"));
    ProtoParser parser(schema);

    EXPECT_ANY_THROW(parser.PlanNodeFromProto(
        BuildSearchPlanNode(0.5f, 1.5f, vector_field_id)));
    EXPECT_ANY_THROW(parser.PlanNodeFromProto(
        BuildSearchPlanNode(1.5f, 0.5f, vector_field_id)));
}

TEST(PlanProto, VectorArrayFieldIdGapInStructArray) {
    namespace planpb = milvus::proto::plan;
    namespace schemapb = milvus::proto::schema;

    schemapb::CollectionSchema schema_proto;
    auto pk = schema_proto.add_fields();
    pk->set_name("id");
    pk->set_fieldid(100);
    pk->set_is_primary_key(true);
    pk->set_data_type(schemapb::DataType::Int64);

    auto struct_array = schema_proto.add_struct_array_fields();
    struct_array->set_name("evidence");
    struct_array->set_fieldid(146);

    auto evidence_item = struct_array->add_fields();
    evidence_item->set_name("evidence[evidence_item]");
    evidence_item->set_fieldid(147);
    evidence_item->set_data_type(schemapb::DataType::Array);
    evidence_item->set_element_type(schemapb::DataType::VarChar);
    auto max_length = evidence_item->add_type_params();
    max_length->set_key("max_length");
    max_length->set_value("512");
    auto max_capacity = evidence_item->add_type_params();
    max_capacity->set_key("max_capacity");
    max_capacity->set_value("200");

    auto evidence_vector = struct_array->add_fields();
    evidence_vector->set_name("evidence[evidence_vector]");
    evidence_vector->set_fieldid(148);
    evidence_vector->set_data_type(schemapb::DataType::ArrayOfVector);
    evidence_vector->set_element_type(schemapb::DataType::FloatVector);
    auto dim = evidence_vector->add_type_params();
    dim->set_key("dim");
    dim->set_value("1024");
    auto vector_max_capacity = evidence_vector->add_type_params();
    vector_max_capacity->set_key("max_capacity");
    vector_max_capacity->set_value("200");

    auto schema = milvus::Schema::ParseFrom(schema_proto);
    ASSERT_EQ(schema->size(), 3);
    ASSERT_EQ(schema->get_field_id_bitset_size(), 49);

    planpb::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(planpb::VectorType::EmbListFloatVector);
    vector_anns->set_field_id(148);
    vector_anns->set_placeholder_tag("$0");
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_metric_type("MAX_SIM_COSINE");
    query_info->set_topk(10);
    query_info->set_round_decimal(-1);
    query_info->set_search_params(R"({"ef": 200})");

    auto plan = milvus::query::CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_TRUE(plan->extra_info_opt_.has_value());
    const auto& involved_fields = plan->extra_info_opt_->involved_fields_;
    ASSERT_EQ(involved_fields.size(), 49);
    EXPECT_TRUE(involved_fields[48]);
}

TEST(PlanProto, SearchPlanCollectsFieldAccessInfo) {
    namespace planpb = milvus::proto::plan;

    auto schema = BuildSchema();
    auto vector_field_id = schema->get_field_id(milvus::FieldName("fakevec"));
    auto predicate_field_id = schema->get_field_id(milvus::FieldName("age"));
    auto output_field_id = schema->get_field_id(milvus::FieldName("score"));
    auto plan_node = BuildSearchPlanNode(0.0f, 0.0f, vector_field_id);
    plan_node.add_output_field_ids(output_field_id.get());

    auto* query_info = plan_node.mutable_vector_anns()->mutable_query_info();
    query_info->set_query_field_id(predicate_field_id.get());
    query_info->add_group_by_field_ids(predicate_field_id.get());

    auto* predicates = plan_node.mutable_vector_anns()->mutable_predicates();
    auto* term_expr = predicates->mutable_term_expr();
    auto* column_info = term_expr->mutable_column_info();
    column_info->set_field_id(predicate_field_id.get());
    column_info->set_data_type(milvus::proto::schema::DataType::Int64);

    auto plan = milvus::query::CreateSearchPlanFromPlanNode(schema, plan_node);

    EXPECT_EQ(plan->target_entries_,
              std::vector<milvus::FieldId>({output_field_id}));
    EXPECT_EQ(plan->access_entries_,
              std::vector<milvus::FieldId>(
                  {vector_field_id, predicate_field_id, output_field_id}));
}

TEST(PlanProto, RetrievePlanCollectsFieldAccessInfo) {
    namespace planpb = milvus::proto::plan;

    auto schema = BuildSchema();
    auto predicate_field_id = schema->get_field_id(milvus::FieldName("age"));
    auto output_field_id = schema->get_field_id(milvus::FieldName("score"));

    planpb::PlanNode plan_node;
    plan_node.add_output_field_ids(output_field_id.get());

    auto* query = plan_node.mutable_query();
    query->set_limit(10);
    query->add_group_by_field_ids(predicate_field_id.get());
    auto* aggregate = query->add_aggregates();
    aggregate->set_op(planpb::sum);
    aggregate->set_field_id(predicate_field_id.get());
    auto* order_by = query->add_order_by_fields();
    order_by->set_field_id(predicate_field_id.get());

    auto* predicates = query->mutable_predicates();
    auto* term_expr = predicates->mutable_term_expr();
    auto* column_info = term_expr->mutable_column_info();
    column_info->set_field_id(predicate_field_id.get());
    column_info->set_data_type(milvus::proto::schema::DataType::Int64);

    auto plan = milvus::query::CreateRetrievePlanByExpr(
        schema, plan_node.SerializeAsString().data(), plan_node.ByteSizeLong());

    EXPECT_EQ(plan->field_ids_,
              std::vector<milvus::FieldId>({output_field_id}));
    EXPECT_EQ(
        plan->access_entries_,
        std::vector<milvus::FieldId>({predicate_field_id, output_field_id}));
}

TEST(PlanProto, SearchPlanRejectsDroppedPredicateField) {
    // Regression: a filter predicate that references a since-dropped field id must be
    // rejected with FieldIDInvalid (input error) BEFORE the expression is parsed. If the
    // check runs after PlanNodeFromProto, ParseExprs reaches Schema::operator[] and throws
    // the default UnexpectedError(2001), which the proxy LB mistakes for a node fault and
    // blacklists a healthy shard leader.
    auto schema = BuildSchema();
    auto vector_field_id = schema->get_field_id(milvus::FieldName("fakevec"));
    const milvus::FieldId dropped_field_id(9999);  // absent from schema

    auto plan_node = BuildSearchPlanNode(0.0f, 0.0f, vector_field_id);
    auto* predicates = plan_node.mutable_vector_anns()->mutable_predicates();
    auto* term_expr = predicates->mutable_term_expr();
    auto* column_info = term_expr->mutable_column_info();
    column_info->set_field_id(dropped_field_id.get());
    column_info->set_data_type(milvus::proto::schema::DataType::Int64);

    try {
        milvus::query::CreateSearchPlanFromPlanNode(schema, plan_node);
        FAIL() << "expected CreateSearchPlanFromPlanNode to throw on dropped "
                  "field";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::FieldIDInvalid);
    }
}

TEST(PlanProto, RetrievePlanRejectsDroppedPredicateField) {
    // Regression, retrieve path (see SearchPlanRejectsDroppedPredicateField).
    auto schema = BuildSchema();
    const milvus::FieldId dropped_field_id(9999);  // absent from schema

    milvus::proto::plan::PlanNode plan_node;
    auto* query = plan_node.mutable_query();
    query->set_limit(10);
    auto* predicates = query->mutable_predicates();
    auto* term_expr = predicates->mutable_term_expr();
    auto* column_info = term_expr->mutable_column_info();
    column_info->set_field_id(dropped_field_id.get());
    column_info->set_data_type(milvus::proto::schema::DataType::Int64);

    try {
        milvus::query::CreateRetrievePlanByExpr(
            schema,
            plan_node.SerializeAsString().data(),
            plan_node.ByteSizeLong());
        FAIL() << "expected CreateRetrievePlanByExpr to throw on dropped field";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::FieldIDInvalid);
    }
}
