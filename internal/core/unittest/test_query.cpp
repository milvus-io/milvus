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

#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <string.h>
#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "futures/Future.h"
#include "futures/future_c.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "segcore/Collection.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/segment_c.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using json = nlohmann::json;
using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {
const int64_t ROW_COUNT = 100 * 1000;
}

TEST(Query, ParsePlaceholderGroup) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseSearch("",         // no filter expression
                                       "fakevec",  // vector field name
                                       10,         // topk
                                       "L2",       // metric_type
                                       "{\"nprobe\": 10}",  // search_params
                                       3                    // round_decimal
    );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
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

    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseSearch("age >= -1 AND age < 1",  // filter expression
                           "fakevec",                // vector field name
                           5,                        // topk
                           "L2",                     // metric_type
                           "{\"nprobe\": 10}",       // search_params
                           3                         // round_decimal
        );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);

    query::Json json = SearchResultToJson(*sr, 3);
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

    int64_t N = 177;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseSearch("age >= -1 AND age < 1",  // filter expression
                           "fakevec",                // vector field name
                           5,                        // topk
                           "L2",                     // metric_type
                           "{\"nprobe\": 10}",       // search_params
                           3                         // round_decimal
        );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 7, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    Timestamp timestamp = 1000000;

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

    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseSearch("age >= -1 AND age < 1",  // filter expression
                           "fakevec",                // vector field name
                           5,                        // topk
                           "L2",                     // metric_type
                           "{\"nprobe\": 10}",       // search_params
                           3                         // round_decimal
        );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);

    query::Json json = SearchResultToJson(*sr, 3);
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

    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseSearch("counter in [1, 2]",  // filter expression
                           "fakevec",            // vector field name
                           5,                    // topk
                           "L2",                 // metric_type
                           "{\"nprobe\": 10}",   // search_params
                           3                     // round_decimal
        );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto num_queries = 3;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp timestamp = 1000000;

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

    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseSearch("",         // no filter expression
                                       "fakevec",  // vector field name
                                       5,          // topk
                                       "L2",       // metric_type
                                       "{\"nprobe\": 10}",  // search_params
                                       3                    // round_decimal
    );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    Timestamp timestamp = 1000000;
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

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseSearch("",         // no filter expression
                                       "fakevec",  // vector field name
                                       5,          // topk
                                       "L2",       // metric_type
                                       "{\"nprobe\": 10}",  // search_params
                                       3                    // round_decimal
    );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
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
    Timestamp timestamp = 1000000;
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

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseSearch("",         // no filter expression
                                       "fakevec",  // vector field name
                                       5,          // topk
                                       "L2",       // metric_type
                                       "{\"nprobe\": 10}",  // search_params
                                       3                    // round_decimal
    );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
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
    Timestamp timestamp = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    assert_order(*sr, "l2");
    std::vector<std::vector<std::string>> results;
    auto json = SearchResultToJson(*sr, 3);
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
    auto num_queries = 5;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "normalized", DataType::VECTOR_FLOAT, dim, knowhere::metric::IP);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseSearch("",            // no filter expression
                                       "normalized",  // vector field name
                                       5,             // topk
                                       "IP",          // metric_type
                                       "{\"nprobe\": 10}",  // search_params
                                       3                    // round_decimal
    );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
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

    Timestamp ts = N * 2;
    auto sr = segment->Search(plan.get(), ph_group.get(), ts);
    assert_order(*sr, "ip");
}

TEST(Query, DISABLED_FillSegment) {
    namespace milvus_pb = milvus::proto;
    milvus_pb::schema::CollectionSchema proto;
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
        field->set_data_type(milvus_pb::schema::DataType::FloatVector);
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
        field->set_data_type(milvus_pb::schema::DataType::Int64);
    }

    {
        auto field = proto.add_fields();
        field->set_name("the_value");
        field->set_nullable(true);
        field->set_fieldid(102);
        field->set_is_primary_key(false);
        field->set_description("asdgfsagf");
        field->set_data_type(milvus_pb::schema::DataType::Int32);
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
    segments.emplace_back(CreateSealedWithFieldDataLoaded(schema, dataset));

    // add field
    {
        auto field = proto.add_fields();
        field->set_name("lack_null_binlog");
        field->set_nullable(true);
        field->set_fieldid(103);
        field->set_is_primary_key(false);
        field->set_description("lack null binlog");
        field->set_data_type(milvus_pb::schema::DataType::Float);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_bool");
        field->set_nullable(true);
        field->set_fieldid(104);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(milvus_pb::schema::DataType::Bool);
        field->mutable_default_value()->set_bool_data(bool_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_int");
        field->set_nullable(true);
        field->set_fieldid(105);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(milvus_pb::schema::DataType::Int32);
        field->mutable_default_value()->set_int_data(int_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_int64");
        field->set_nullable(true);
        field->set_fieldid(106);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(milvus_pb::schema::DataType::Int64);
        field->mutable_default_value()->set_int_data(long_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_float");
        field->set_nullable(true);
        field->set_fieldid(107);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(milvus_pb::schema::DataType::Float);
        field->mutable_default_value()->set_float_data(float_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_double");
        field->set_nullable(true);
        field->set_fieldid(108);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(milvus_pb::schema::DataType::Double);
        field->mutable_default_value()->set_double_data(double_default_value);
    }

    {
        auto field = proto.add_fields();
        field->set_name("lack_default_value_binlog_varchar");
        field->set_nullable(true);
        field->set_fieldid(109);
        field->set_is_primary_key(false);
        field->set_description("lack default value binlog");
        field->set_data_type(milvus_pb::schema::DataType::VarChar);
        auto str_type_params = field->add_type_params();
        str_type_params->set_key(MAX_LENGTH);
        str_type_params->set_value(std::to_string(64));
        field->mutable_default_value()->set_string_data(varchar_dafualt_vlaue);
    }

    schema = Schema::ParseFrom(proto);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseSearch("",         // no filter expression
                                       "fakevec",  // vector field name
                                       5,          // topk
                                       "L2",       // metric_type
                                       "{\"nprobe\": 10}",  // search_params
                                       3                    // round_decimal
    );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph_proto = CreatePlaceholderGroup(10, 16, 443);
    auto ph = ParsePlaceholderGroup(plan.get(), ph_proto.SerializeAsString());
    Timestamp ts = N * 2UL;

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

                // check int32 field only if valid
                if (output_i32_valid_data[i]) {
                    int i32;
                    memcpy(&i32, &output_i32_field_data[i], sizeof(int32_t));
                    ASSERT_EQ(i32, std_i32);
                }
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
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

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

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseSearch("age >= -1 AND age < 1",  // filter expression
                           "fakevec",                // vector field name
                           5,                        // topk
                           "JACCARD",                // metric_type
                           "{\"nprobe\": 10}",       // search_params
                           3                         // round_decimal
        );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroupFromBlob<milvus::BinaryVector>(
        num_queries, 512, vec_ptr.data() + 1024 * 512 / 8);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    Timestamp timestamp = 1000000;
    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
    // ASSERT_EQ(json.dump(2), ref.dump(2));
}

TEST(Query, VectorArrayElementLevelInference) {
    auto dim = 32;

    // Helper to create schema + plan for a VECTOR_ARRAY field with given metric
    auto make_plan = [&](const std::string& metric) {
        auto schema = std::make_shared<Schema>();
        auto int64_field = schema->AddDebugField("int64", DataType::INT64);
        schema->AddDebugVectorArrayField(
            "array_vec", DataType::VECTOR_FLOAT, dim, metric);
        schema->set_primary_field_id(int64_field);

        ScopedSchemaHandle handle(*schema);
        auto plan_str =
            handle.ParseSearch("", "array_vec", 5, metric, R"({"nprobe": 10})");
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        return plan;
    };

    int num_queries = 2;
    std::vector<float> query_vec = generate_float_vector(num_queries, dim);

    // Case 1: MAX_SIM + EmbList → element_level=false (embedding list search)
    {
        auto plan = make_plan("MAX_SIM");
        std::vector<size_t> offsets = {0, 1, 2};
        auto ph_raw = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
            num_queries, dim, query_vec.data(), offsets);
        auto ph = ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString());
        EXPECT_FALSE(ph->at(0).element_level_);
    }

    // Case 2: COSINE + plain vector → element_level=true (element-level search)
    {
        auto plan = make_plan("COSINE");
        auto ph_raw =
            CreatePlaceholderGroupFromBlob(num_queries, dim, query_vec.data());
        auto ph = ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString());
        EXPECT_TRUE(ph->at(0).element_level_);
    }

    // Case 3: MAX_SIM + plain vector → error (mismatch)
    {
        auto plan = make_plan("MAX_SIM");
        auto ph_raw =
            CreatePlaceholderGroupFromBlob(num_queries, dim, query_vec.data());
        EXPECT_THROW(
            ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString()),
            std::exception);
    }

    // Case 4: COSINE + EmbList → error (mismatch)
    {
        auto plan = make_plan("COSINE");
        std::vector<size_t> offsets = {0, 1, 2};
        auto ph_raw = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
            num_queries, dim, query_vec.data(), offsets);
        EXPECT_THROW(
            ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString()),
            std::exception);
    }

    // Case 5: omitted metric + EmbList → infer embedding-list search. The
    // segment validates the mode after resolving its own metric.
    {
        auto plan = make_plan("");
        std::vector<size_t> offsets = {0, 1, 2};
        auto ph_raw = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
            num_queries, dim, query_vec.data(), offsets);
        auto ph = ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString());
        EXPECT_FALSE(ph->at(0).element_level_);
    }

    // Case 6: omitted metric + plain vector → infer element-level search.
    {
        auto plan = make_plan("");
        auto ph_raw =
            CreatePlaceholderGroupFromBlob(num_queries, dim, query_vec.data());
        auto ph = ParsePlaceholderGroup(plan.get(), ph_raw.SerializeAsString());
        EXPECT_TRUE(ph->at(0).element_level_);
    }
}

TEST(Query, VectorArrayOmittedMetricUsesSegmentMetric) {
    constexpr int64_t dim = 8;
    constexpr int64_t row_count = 32;
    constexpr int64_t topk = 3;
    constexpr int64_t query_count = 2;

    auto run_case = [&](const MetricType& segment_metric,
                        bool embedding_list_placeholder,
                        bool expect_success,
                        bool zero_hit) {
        auto schema = std::make_shared<Schema>();
        auto primary_key = schema->AddDebugField("pk", DataType::INT64);
        auto array_vec = schema->AddDebugVectorArrayField(
            "structA[array_vec]", DataType::VECTOR_FLOAT, dim, segment_metric);
        schema->set_primary_field_id(primary_key);

        std::map<std::string, std::string> index_params = {
            {knowhere::meta::INDEX_TYPE,
             knowhere::IndexEnum::INDEX_FAISS_IDMAP},
            {knowhere::meta::METRIC_TYPE, segment_metric}};
        std::map<std::string, std::string> type_params = {
            {knowhere::meta::DIM, std::to_string(dim)}};
        FieldIndexMeta field_index_meta(
            array_vec, std::move(index_params), std::move(type_params));
        std::map<FieldId, FieldIndexMeta> field_indexes = {
            {array_vec, std::move(field_index_meta)}};
        auto index_meta = std::make_shared<CollectionIndexMeta>(
            row_count, std::move(field_indexes));

        auto dataset = DataGen(schema, row_count, 42, 0, 1, 2);
        auto sealed = CreateSealedSegment(schema, index_meta);
        LoadGeneratedDataIntoSegment(dataset, sealed.get());
        auto growing = CreateGrowingWithFieldDataLoaded(
            schema, index_meta, SegcoreConfig::default_config(), dataset);
        auto empty_growing = CreateGrowingSegment(schema, index_meta);

        ScopedSchemaHandle handle(*schema);
        auto plan_blob = handle.ParseSearch(
            zero_hit ? "pk < 0" : "", "structA[array_vec]", topk, "", R"({})");
        auto plan =
            CreateSearchPlanByExpr(schema, plan_blob.data(), plan_blob.size());

        auto query_vectors = generate_float_vector(
            embedding_list_placeholder ? query_count * 2 : query_count, dim);
        milvus::proto::common::PlaceholderGroup raw_group;
        if (embedding_list_placeholder) {
            std::vector<size_t> offsets = {0, 2, 4};
            raw_group = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
                query_count * 2, dim, query_vectors.data(), offsets);
        } else {
            raw_group = CreatePlaceholderGroupFromBlob(
                query_count, dim, query_vectors.data());
        }
        auto placeholder =
            ParsePlaceholderGroup(plan.get(), raw_group.SerializeAsString());
        ASSERT_EQ(placeholder->at(0).element_level_,
                  !embedding_list_placeholder);

        auto verify_segment = [&](SegmentInterface* segment,
                                  bool empty_segment) {
            if (expect_success) {
                auto result = segment->Search(plan.get(),
                                              placeholder.get(),
                                              dataset.timestamps_.back() + 1);
                ASSERT_NE(result, nullptr);
                EXPECT_EQ(result->metric_type_, segment_metric);
                EXPECT_EQ(result->element_level_, !embedding_list_placeholder);
                EXPECT_EQ(result->distances_.empty(),
                          zero_hit || empty_segment);
                return;
            }

            try {
                static_cast<void>(
                    segment->Search(plan.get(),
                                    placeholder.get(),
                                    dataset.timestamps_.back() + 1));
                FAIL() << "expected VECTOR_ARRAY search mode mismatch";
            } catch (const SegcoreError& error) {
                EXPECT_EQ(error.get_error_code(), ErrorCode::DataTypeInvalid);
            }
        };

        verify_segment(sealed.get(), false);
        verify_segment(growing.get(), false);
        verify_segment(empty_growing.get(), true);

        // AsyncSearch has a separate inaccessible-field empty-result shortcut
        // before Segment::Search. Exercise that production entry point so it
        // cannot bypass either metric resolution or placeholder-mode checks.
        auto c_future = AsyncSearch({},
                                    empty_growing.get(),
                                    plan.get(),
                                    placeholder.get(),
                                    dataset.timestamps_.back() + 1,
                                    0,
                                    0,
                                    0,
                                    false,
                                    false);
        auto future = static_cast<milvus::futures::IFuture*>(
            static_cast<void*>(static_cast<CFuture*>(c_future)));
        std::mutex mu;
        mu.lock();
        future->registerReadyCallback(
            [](CLockedGoMutex* mutex) {
                reinterpret_cast<std::mutex*>(mutex)->unlock();
            },
            reinterpret_cast<CLockedGoMutex*>(&mu));
        mu.lock();
        mu.unlock();
        auto [raw_result, c_status] = future->leakyGet();
        future_destroy(c_future);
        auto c_result = static_cast<CSearchResult>(raw_result);
        if (expect_success) {
            ASSERT_EQ(c_status.error_code, Success);
            ASSERT_NE(c_result, nullptr);
            EXPECT_EQ(static_cast<SearchResult*>(c_result)->metric_type_,
                      segment_metric);
            EXPECT_EQ(static_cast<SearchResult*>(c_result)->element_level_,
                      !embedding_list_placeholder);
            DeleteSearchResult(c_result);
        } else {
            EXPECT_EQ(c_status.error_code, DataTypeInvalid);
            EXPECT_EQ(c_result, nullptr);
            free(const_cast<char*>(c_status.error_msg));
        }
    };

    // Both valid omitted-metric modes execute on sealed and growing segments.
    run_case(knowhere::metric::MAX_SIM, true, true, false);
    run_case(knowhere::metric::COSINE, false, true, false);

    // Zero-candidate fast paths still carry the segment-resolved metric. Proxy
    // search iterators need it to choose the correct +/-MaxFloat32 bound when
    // the first page is empty and the request omitted metric_type.
    run_case(knowhere::metric::MAX_SIM, true, true, true);
    run_case(knowhere::metric::COSINE, false, true, true);

    // The segment-resolved metric remains authoritative and rejects both
    // placeholder/metric mode mismatch directions before vector search.
    run_case(knowhere::metric::COSINE, true, false, false);
    run_case(knowhere::metric::MAX_SIM, false, false, false);

    // The same invalid request must fail even when the scalar filter removes
    // every candidate before vector_search() would otherwise run.
    run_case(knowhere::metric::COSINE, true, false, true);
    run_case(knowhere::metric::MAX_SIM, false, false, true);
}
