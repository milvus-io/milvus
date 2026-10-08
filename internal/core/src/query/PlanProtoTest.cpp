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
#include <regex>
#include <vector>
#include <chrono>

#include "common/Schema.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "query/PlanProto.h"

TEST(PlanProto, NotSetUnsupported) {
    using namespace milvus;
    using namespace milvus::query;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    proto::plan::Expr expr_pb;
    ProtoParser parser(schema);
    ASSERT_ANY_THROW(parser.ParseExprs(expr_pb));
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

TEST(PlanProto, StrictGroupSettings) {
    using namespace milvus;
    auto schema = std::make_shared<Schema>();
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    proto::plan::PlanNode node;
    auto* anns = node.mutable_vector_anns();
    anns->set_vector_type(proto::plan::VectorType::FloatVector);
    anns->set_field_id(vec.get());
    anns->set_placeholder_tag("$0");
    auto* info = anns->mutable_query_info();
    info->set_metric_type("L2");
    info->set_topk(10);
    info->set_round_decimal(-1);
    for (int64_t weight :
         {int64_t(0), int64_t(50), std::numeric_limits<int64_t>::max()}) {
        for (bool skip : {false, true}) {
            info->set_search_params(knowhere::Json{
                {kStrictGroupPhase1CandidateWeight, weight},
                {kStrictGroupSkipRefine,
                 skip}}.dump());
            auto parsed = query::ProtoParser(schema).PlanNodeFromProto(node);
            EXPECT_EQ(
                parsed->search_info_.strict_group_phase1_candidate_weight_,
                weight);
            EXPECT_EQ(parsed->search_info_.strict_group_skip_refine_, skip);
            EXPECT_TRUE(parsed->search_info_.search_params_.empty());
        }
    }
    for (const auto& value : {knowhere::Json(-1),
                              knowhere::Json(1.0),
                              knowhere::Json(uint64_t(1) << 63),
                              knowhere::Json("50"),
                              knowhere::Json(nullptr),
                              knowhere::Json(true)}) {
        info->set_search_params(
            knowhere::Json{{kStrictGroupPhase1CandidateWeight, value}}.dump());
        EXPECT_THROW(query::ProtoParser(schema).PlanNodeFromProto(node),
                     SegcoreError);
    }
    for (const auto& value :
         {knowhere::Json("true"), knowhere::Json(1), knowhere::Json(nullptr)}) {
        info->set_search_params(
            knowhere::Json{{kStrictGroupSkipRefine, value}}.dump());
        EXPECT_THROW(query::ProtoParser(schema).PlanNodeFromProto(node),
                     SegcoreError);
    }
    info->set_search_params(R"({"nprobe":128})");
    auto defaults = query::ProtoParser(schema).PlanNodeFromProto(node);
    EXPECT_EQ(defaults->search_info_.strict_group_strategy_,
              StrictGroupStrategy::PerGroup);
    EXPECT_EQ(defaults->search_info_.search_params_["nprobe"], 128);
    for (const auto& [strategy, expected] :
         std::vector<std::pair<std::string, StrictGroupStrategy>>{
             {"original", StrictGroupStrategy::Original},
             {"per_group", StrictGroupStrategy::PerGroup}}) {
        info->set_search_params(
            knowhere::Json{{kStrictGroupStrategy, strategy}}.dump());
        auto parsed = query::ProtoParser(schema).PlanNodeFromProto(node);
        EXPECT_EQ(parsed->search_info_.strict_group_strategy_, expected);
        EXPECT_FALSE(
            parsed->search_info_.search_params_.contains(kStrictGroupStrategy));
    }
    for (const auto& value : {knowhere::Json("invalid"),
                              knowhere::Json("sampling"),
                              knowhere::Json("filtered_iterator"),
                              knowhere::Json(1),
                              knowhere::Json(nullptr),
                              knowhere::Json(true)}) {
        info->set_search_params(
            knowhere::Json{{kStrictGroupStrategy, value}}.dump());
        EXPECT_THROW(query::ProtoParser(schema).PlanNodeFromProto(node),
                     SegcoreError);
    }
}
