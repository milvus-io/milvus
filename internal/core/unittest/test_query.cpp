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

#include "pb/schema.pb.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/DataGen.h"

using json = nlohmann::json;
using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {
const int64_t ROW_COUNT = 100 * 1000;
}

TEST(Query, ParsePlaceholderGroup) {
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                query_info: <
                                  topk: 10
                                  round_decimal: 3
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    int64_t num_queries = 100000;
    int dim = 16;
    auto raw_group = CreatePlaceholderGroup(num_queries, dim);
    auto blob = raw_group.SerializeAsString();
    auto placeholder = ParsePlaceholderGroup(plan.get(), blob);
}

TEST(Query, ExecWithPredicateLoader) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto counter_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(counter_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Float
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          float_val: -1
                                        >
                                        upper_value: <
                                          float_val: 1
                                        >
                                      >
                                    >
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    milvus::Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);

    query::Json json = SearchResultToJson(*sr);
#ifdef __linux__
    auto ref = json::parse(R"(
[
  [
        ["982->0.000000", "25315->4.742000", "57893->4.758000", "1499->6.066000", "48201->6.075000"],
	["41772->10.111000", "42126->11.532000", "80693->11.712000", "74859->11.790000", "79777->11.842000"],
	["59251->2.543000", "65551->4.454000", "21617->5.144000", "50037->5.267000", "72204->5.332000"],
	["59219->5.458000", "21995->6.078000", "97922->6.764000", "80887->6.898000", "61367->7.029000"],
	["66353->5.696000", "30664->5.881000", "41087->5.917000", "34625->6.109000", "10393->6.633000"]
  ]
])");
#else  // for mac
    auto ref = json::parse(R"(
[
  [
    ["982->0.000000", "31864->4.270000", "18916->4.651000", "71547->5.125000", "86706->5.991000"],
    ["96984->4.192000", "65514->6.011000", "89328->6.138000", "80284->6.526000", "68218->6.563000"],
    ["30119->2.464000", "52595->4.323000", "82365->4.725000", "32673->4.851000", "74834->5.009000"],
    ["99625->6.129000", "86582->6.900000", "10069->7.388000", "89982->7.672000", "85934->7.792000"],
    ["37759->3.581000", "97019->5.557000", "92444->5.681000", "31292->5.780000", "53543->5.844000"]
  ]
])");
#endif
    std::cout << json.dump(2);
    ASSERT_EQ(json.dump(2), ref.dump(2));
}

TEST(Query, ExecWithPredicateSmallN) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 7, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Float
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          float_val: -1
                                        >
                                        upper_value: <
                                          float_val: 1
                                        >
                                      >
                                    >
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    int64_t N = 177;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 7, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    milvus::Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}

TEST(Query, ExecWithPredicate) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Float
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          float_val: -1
                                        >
                                        upper_value: <
                                          float_val: 1
                                        >
                                      >
                                    >
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    milvus::Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);

    query::Json json = SearchResultToJson(*sr);
#ifdef __linux__
    auto ref = json::parse(R"(
[
	[
		["982->0.000000", "25315->4.742000", "57893->4.758000", "1499->6.066000", "48201->6.075000"],
		["41772->10.111000", "42126->11.532000", "80693->11.712000", "74859->11.790000", "79777->11.842000"],
		["59251->2.543000", "65551->4.454000", "21617->5.144000", "50037->5.267000", "72204->5.332000"],
		["59219->5.458000", "21995->6.078000", "97922->6.764000", "80887->6.898000", "61367->7.029000"],
		["66353->5.696000", "30664->5.881000", "41087->5.917000", "34625->6.109000", "10393->6.633000"]
	]
])");
#else  // for mac
    auto ref = json::parse(R"(
[
	[
        ["982->0.000000", "31864->4.270000", "18916->4.651000", "71547->5.125000", "86706->5.991000"],
        ["96984->4.192000", "65514->6.011000", "89328->6.138000", "80284->6.526000", "68218->6.563000"],
        ["30119->2.464000", "52595->4.323000", "82365->4.725000", "32673->4.851000", "74834->5.009000"],
        ["99625->6.129000", "86582->6.900000", "10069->7.388000", "89982->7.672000", "85934->7.792000"],
        ["37759->3.581000", "97019->5.557000", "92444->5.681000", "31292->5.780000", "53543->5.844000"]
    ]
])");
#endif
    std::cout << json.dump(2);
    ASSERT_EQ(json.dump(2), ref.dump(2));
}

TEST(Query, ExecTerm) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      term_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Int64
                                        >
                                        values: <
                                          int64_val: 1
                                        >
                                        values: <
                                          int64_val: 2
                                        >
                                      >
                                    >
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 3;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    milvus::Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    int topk = 5;
    auto json = SearchResultToJson(*sr);
    ASSERT_EQ(sr->total_nq_, num_queries);
    ASSERT_EQ(sr->unity_topK_, topk);
}

TEST(Query, ExecEmpty) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("age", DataType::FLOAT);
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    const char* raw_plan = R"(vector_anns: <
                                field_id: 101
                                query_info: <
                                  topk: 5
                                  round_decimal: 3
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
        >)";
    int64_t N = ROW_COUNT;
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    milvus::Timestamp timestamp = 1000000;
    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    std::cout << SearchResultToJson(*sr);
    ASSERT_EQ(sr->unity_topK_, 0);

    for (auto i : sr->seg_offsets_) {
        ASSERT_EQ(i, -1);
    }

    for (auto v : sr->distances_) {
        ASSERT_EQ(v, std::numeric_limits<float>::max());
    }
}

TEST(Query, ExecWithoutPredicateFlat) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, std::nullopt);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    milvus::Timestamp timestamp = 1000000;
    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    std::vector<std::vector<std::string>> results;
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}

TEST(Query, ExecWithoutPredicate) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    milvus::Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    assert_order(*sr, "l2");
    std::vector<std::vector<std::string>> results;
    auto json = SearchResultToJson(*sr);
#ifdef __linux__
    auto ref = json::parse(R"(
[
	[
		["982->0.000000", "25315->4.742000", "57893->4.758000", "1499->6.066000", "48201->6.075000"],
		["41772->10.111000", "42126->11.532000", "80693->11.712000", "74859->11.790000", "79777->11.842000"],
		["59251->2.543000", "68714->4.356000", "65551->4.454000", "21617->5.144000", "50037->5.267000"],
		["33572->5.432000", "59219->5.458000", "21995->6.078000", "97922->6.764000", "17913->6.831000"],
		["66353->5.696000", "30664->5.881000", "41087->5.917000", "34625->6.109000", "24554->6.195000"]
	]
])");
#else  // for mac
    auto ref = json::parse(R"(
[
	[
        ["982->0.000000", "31864->4.270000", "18916->4.651000", "78227->4.808000", "71547->5.125000"],
        ["96984->4.192000", "45733->4.912000", "32891->5.016000", "65514->6.011000", "89328->6.138000"],
        ["30119->2.464000", "23782->3.724000", "52595->4.323000", "82365->4.725000", "32673->4.851000"],
        ["99625->6.129000", "86582->6.900000", "60608->7.285000", "10069->7.388000", "89982->7.672000"],
        ["37759->3.581000", "50907->4.776000", "45814->4.872000", "97019->5.557000", "92444->5.681000"]
    ]
])");
#endif
    std::cout << json.dump(2);
    ASSERT_EQ(json.dump(2), ref.dump(2));
}

TEST(Query, InnerProduct) {
    int64_t N = 100000;
    constexpr auto dim = 16;
    constexpr auto topk = 10;
    auto num_queries = 5;
    auto schema = std::make_shared<Schema>();
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "IP"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
        >)";
    auto vec_fid = schema->AddDebugField(
        "normalized", DataType::VECTOR_FLOAT, dim, knowhere::metric::IP);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto col = dataset.get_col<float>(vec_fid);

    auto ph_group_raw =
        CreatePlaceholderGroupFromBlob(num_queries, 16, col.data());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    milvus::Timestamp ts = N * 2;
    auto sr = segment->Search(plan.get(), ph_group.get(), ts);
    assert_order(*sr, "ip");
}

TEST(Query, FillSegment) {
    namespace pb = milvus::proto;
    pb::schema::CollectionSchema proto;
    proto.set_name("col");
    proto.set_description("asdfhsalkgfhsadg");
    auto dim = 16;
    bool bool_default_value = true;
    int32_t int_default_value = 20;
    int64_t long_default_value = 20;
    float float_default_value = 20;
    double double_default_value = 20;
    string varchar_dafualt_vlaue = "20";

    {
        auto field = proto.add_fields();
        field->set_name("fakevec");
        field->set_nullable(false);
        field->set_is_primary_key(false);
        field->set_description("asdgfsagf");
        field->set_fieldid(100);
        field->set_data_type(pb::schema::DataType::FloatVector);
        auto param = field->add_type_params();
        param->set_key("dim");
        param->set_value("16");
        auto iparam = field->add_index_params();
        iparam->set_key("metric_type");
        iparam->set_value("L2");
    }

    {
        auto field = proto.add_fields();
        field->set_name("the_key");
        field->set_nullable(false);
        field->set_fieldid(101);
        field->set_is_primary_key(true);
        field->set_description("asdgfsagf");
        field->set_data_type(pb::schema::DataType::Int64);
    }

    {
        auto field = proto.add_fields();
        field->set_name("the_value");
        field->set_nullable(true);
        field->set_fieldid(102);
        field->set_is_primary_key(false);
        field->set_description("asdgfsagf");
        field->set_data_type(pb::schema::DataType::Int32);
    }

    auto schema = Schema::ParseFrom(proto);

    // dispatch here
    int N = 100000;
    auto dataset = DataGen(schema, N);
    const auto std_vec = dataset.get_col<int64_t>(FieldId(101));  // ids field
    const auto std_vfloat_vec =
        dataset.get_col<float>(FieldId(100));  // vector field
    const auto std_i32_vec =
        dataset.get_col<int32_t>(FieldId(102));  // scalar field
    const auto i32_vec_valid_data = dataset.get_col_valid(FieldId(102));

    std::vector<std::unique_ptr<SegmentInternalInterface>> segments;
    segments.emplace_back([&] {
        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        return segment;
    }());
    segments.emplace_back([&] {
        auto segment = CreateSealedSegment(schema);
        SealedLoadFieldData(dataset, *segment);
        return segment;
    }());

    // add field
    {
        auto field = proto.add_fields();
        field->set_name("lack_null_binlog");
        field->set_nullable(true);
        field->set_fieldid(103);
        field->set_is_primary_key(false);
        field->set_description("lack null binlog");
        field->set_data_type(pb::schema::DataType::Float);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_bool");
        field->set_nullable(true);
        field->set_fieldid(104);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(pb::schema::DataType::Bool);
        field->mutable_default_value()->set_bool_data(bool_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_int");
        field->set_nullable(true);
        field->set_fieldid(105);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(pb::schema::DataType::Int32);
        field->mutable_default_value()->set_int_data(int_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_int64");
        field->set_nullable(true);
        field->set_fieldid(106);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(pb::schema::DataType::Int64);
        field->mutable_default_value()->set_int_data(long_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_float");
        field->set_nullable(true);
        field->set_fieldid(107);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(pb::schema::DataType::Float);
        field->mutable_default_value()->set_float_data(float_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_double");
        field->set_nullable(true);
        field->set_fieldid(108);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(pb::schema::DataType::Double);
        field->mutable_default_value()->set_double_data(double_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_varchar");
        field->set_nullable(true);
        field->set_fieldid(109);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(pb::schema::DataType::VarChar);
        auto str_type_params = field->add_type_params();
        str_type_params->set_key(MAX_LENGTH);
        str_type_params->set_value(std::to_string(64));
        field->mutable_default_value()->set_string_data(varchar_dafualt_vlaue);
    }

    schema = Schema::ParseFrom(proto);

    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto ph_proto = CreatePlaceholderGroup(10, 16, 443);
    auto ph = ParsePlaceholderGroup(plan.get(), ph_proto.SerializeAsString());
    milvus::Timestamp ts = N * 2UL;

    auto topk = 5;
    auto num_queries = 10;
    for (auto& segment : segments) {
        plan->target_entries_.clear();
        plan->target_entries_.push_back(
            schema->get_field_id(FieldName("fakevec")));
        plan->target_entries_.push_back(
            schema->get_field_id(FieldName("the_value")));
        plan->target_entries_.push_back(
            schema->get_field_id(FieldName("lack_null_binlog")));
        plan->target_entries_.push_back(
            schema->get_field_id(FieldName("lack_default_value_binlog_bool")));
        plan->target_entries_.push_back(
            schema->get_field_id(FieldName("lack_default_value_binlog_int")));
        plan->target_entries_.push_back(
            schema->get_field_id(FieldName("lack_default_value_binlog_int64")));
        plan->target_entries_.push_back(
            schema->get_field_id(FieldName("lack_default_value_binlog_float")));
        plan->target_entries_.push_back(schema->get_field_id(
            FieldName("lack_default_value_binlog_double")));
        plan->target_entries_.push_back(schema->get_field_id(
            FieldName("lack_default_value_binlog_varchar")));
        auto result = segment->Search(plan.get(), ph.get(), ts);
        result->result_offsets_.resize(topk * num_queries);
        segment->FillTargetEntry(plan.get(), *result);
        segment->FillPrimaryKeys(plan.get(), *result);

        auto& fields_data = result->output_fields_data_;
        ASSERT_EQ(fields_data.size(), 9);
        for (auto field_id : plan->target_entries_) {
            ASSERT_EQ(fields_data.count(field_id), true);
        }

        auto vec_field_id = schema->get_field_id(FieldName("fakevec"));
        auto output_vec_field_data =
            fields_data.at(vec_field_id)->vectors().float_vector().data();
        ASSERT_EQ(output_vec_field_data.size(), topk * num_queries * dim);

        auto i32_field_id = schema->get_field_id(FieldName("the_value"));
        auto output_i32_field_data =
            fields_data.at(i32_field_id)->scalars().int_data().data();
        ASSERT_EQ(output_i32_field_data.size(), topk * num_queries);
        auto output_i32_valid_data = fields_data.at(i32_field_id)->valid_data();
        ASSERT_EQ(output_i32_valid_data.size(), topk * num_queries);
        auto float_field_id =
            schema->get_field_id(FieldName("lack_null_binlog"));
        auto output_float_field_data =
            fields_data.at(float_field_id)->scalars().float_data().data();
        ASSERT_EQ(output_float_field_data.size(), topk * num_queries);
        auto output_float_valid_data =
            fields_data.at(float_field_id)->valid_data();
        ASSERT_EQ(output_float_valid_data.size(), topk * num_queries);
        auto double_field_id =
            schema->get_field_id(FieldName("lack_default_value_binlog_double"));
        auto output_double_field_data =
            fields_data.at(double_field_id)->scalars().double_data().data();
        ASSERT_EQ(output_double_field_data.size(), topk * num_queries);
        auto output_double_valid_data =
            fields_data.at(double_field_id)->valid_data();
        ASSERT_EQ(output_double_valid_data.size(), topk * num_queries);

        auto bool_field_id =
            schema->get_field_id(FieldName("lack_default_value_binlog_bool"));
        auto output_bool_field_data =
            fields_data.at(bool_field_id)->scalars().bool_data().data();
        ASSERT_EQ(output_bool_field_data.size(), topk * num_queries);
        auto output_bool_valid_data =
            fields_data.at(bool_field_id)->valid_data();
        ASSERT_EQ(output_bool_valid_data.size(), topk * num_queries);

        auto int_field_id =
            schema->get_field_id(FieldName("lack_default_value_binlog_int"));
        auto output_int_field_data =
            fields_data.at(int_field_id)->scalars().int_data().data();
        ASSERT_EQ(output_int_field_data.size(), topk * num_queries);
        auto output_int_valid_data = fields_data.at(int_field_id)->valid_data();
        ASSERT_EQ(output_int_valid_data.size(), topk * num_queries);

        auto int64_field_id =
            schema->get_field_id(FieldName("lack_default_value_binlog_int64"));
        auto output_int64_field_data =
            fields_data.at(int64_field_id)->scalars().long_data().data();
        ASSERT_EQ(output_int64_field_data.size(), topk * num_queries);
        auto output_int64_valid_data =
            fields_data.at(int64_field_id)->valid_data();
        ASSERT_EQ(output_int64_valid_data.size(), topk * num_queries);

        auto float_field_id_default_value =
            schema->get_field_id(FieldName("lack_default_value_binlog_float"));
        auto output_float_field_data_default_value =
            fields_data.at(float_field_id_default_value)
                ->scalars()
                .float_data()
                .data();
        ASSERT_EQ(output_float_field_data_default_value.size(),
                  topk * num_queries);
        auto output_float_valid_data_default_value =
            fields_data.at(float_field_id_default_value)->valid_data();
        ASSERT_EQ(output_float_valid_data_default_value.size(),
                  topk * num_queries);

        auto varchar_field_id = schema->get_field_id(
            FieldName("lack_default_value_binlog_varchar"));
        auto output_varchar_field_data =
            fields_data.at(varchar_field_id)->scalars().string_data().data();
        ASSERT_EQ(output_varchar_field_data.size(), topk * num_queries);
        auto output_varchar_valid_data =
            fields_data.at(varchar_field_id)->valid_data();
        ASSERT_EQ(output_varchar_valid_data.size(), topk * num_queries);

        for (int i = 0; i < topk * num_queries; i++) {
            int64_t val = std::get<int64_t>(result->primary_keys_[i]);

            auto internal_offset = result->seg_offsets_[i];
            auto std_val = std_vec[internal_offset];
            auto std_i32 = std_i32_vec[internal_offset];
            auto std_i32_valid = i32_vec_valid_data[internal_offset];
            auto std_float_valid = false;
            auto std_double = double_default_value;
            auto std_double_valid = true;
            std::vector<float> std_vfloat(dim);
            std::copy_n(std_vfloat_vec.begin() + dim * internal_offset,
                        dim,
                        std_vfloat.begin());

            ASSERT_EQ(val, std_val) << "io:" << internal_offset;
            if (val != -1) {
                // check vector field
                std::vector<float> vfloat(dim);
                memcpy(vfloat.data(),
                       &output_vec_field_data[i * dim],
                       dim * sizeof(float));
                ASSERT_EQ(vfloat, std_vfloat);

                // check int32 field
                int i32;
                memcpy(&i32, &output_i32_field_data[i], sizeof(int32_t));
                ASSERT_EQ(i32, std_i32);
                // check int32 valid field
                bool i32_valid;
                memcpy(&i32_valid, &output_i32_valid_data[i], sizeof(bool));
                ASSERT_EQ(i32_valid, std_i32_valid);

                // check float field lack null field binlog valid field
                bool f_valid;
                memcpy(&f_valid, &output_float_valid_data[i], sizeof(bool));
                ASSERT_EQ(f_valid, std_float_valid);

                // check double field lack default value field binlog
                double d;
                memcpy(&d, &output_double_field_data[i], sizeof(double));
                ASSERT_EQ(d, std_double);
                // check double field lack default value field binlog valid field
                bool d_valid;
                memcpy(&d_valid, &output_double_valid_data[i], sizeof(bool));
                ASSERT_EQ(d_valid, std_double_valid);
            }
        }
    }
}

TEST(Query, ExecWithPredicateBinary) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_BINARY, 512, knowhere::metric::JACCARD);
    auto float_fid = schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    const char* raw_plan = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Float
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          float_val: -1
                                        >
                                        upper_value: <
                                          float_val: 1
                                        >
                                      >
                                    >
                                    query_info: <
                                      topk: 5
                                      round_decimal: 3
                                      metric_type: "JACCARD"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto vec_ptr = dataset.get_col<uint8_t>(vec_fid);

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, 512, vec_ptr.data() + 1024 * 512 / 8);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    milvus::Timestamp timestamp = 1000000;
    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
    // ASSERT_EQ(json.dump(2), ref.dump(2));
}
