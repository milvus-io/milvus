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
#include "query/Expr.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/generated/ExecPlanNodeVisitor.h"
#include "query/generated/ExprVisitor.h"
#include "query/generated/ShowPlanNodeVisitor.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {
const int64_t ROW_COUNT = 100 * 1000;
}

TEST(Query, ShowExecutor) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    using namespace milvus;
    auto metric_type = knowhere::metric::L2;
    auto node = std::make_unique<FloatVectorANNS>();
    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, metric_type);
    int64_t num_queries = 100L;
    auto raw_data = DataGen(schema, num_queries);
    auto& info = node->search_info_;
    info.metric_type_ = metric_type;
    info.topk_ = 20;
    info.field_id_ = field_id;
    node->predicate_ = std::nullopt;
    ShowPlanNodeVisitor show_visitor;
    PlanNodePtr base(node.release());
    auto res = show_visitor.call_child(*base);
    auto dup = res;
    std::cout << dup.dump(4);
}

TEST(Query, DSL) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    ShowPlanNodeVisitor shower;

    std::string dsl_string = R"(
{
    "bool": {
        "must": [
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10,
                        "round_decimal": 3
                    }
                }
            }
        ]
    }
})";

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto plan = CreatePlan(*schema, dsl_string);
    auto res = shower.call_child(*plan->plan_node_);
    std::cout << res.dump(4) << std::endl;

    std::string dsl_string2 = R"(
{
    "bool": {
        "vector": {
            "fakevec": {
                "metric_type": "L2",
                "params": {
                    "nprobe": 10
                },
                "query": "$0",
                "topk": 10,
                "round_decimal": 3
            }
        }
    }
})";
    auto plan2 = CreatePlan(*schema, dsl_string2);
    auto res2 = shower.call_child(*plan2->plan_node_);
    std::cout << res2.dump(4) << std::endl;
    ASSERT_EQ(res, res2);
}

TEST(Query, ParsePlaceholderGroup) {
    std::string dsl_string = R"(
{
    "bool": {
        "vector": {
            "fakevec": {
                "metric_type": "L2",
                "params": {
                    "nprobe": 10
                },
                "query": "$0",
                "topk": 10,
                "round_decimal":3
            }
        }
    }
})";

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto plan = CreatePlan(*schema, dsl_string);
    int64_t num_queries = 100000;
    int dim = 16;
    auto raw_group = CreatePlaceholderGroup(num_queries, dim);
    auto blob = raw_group.SerializeAsString();
    auto placeholder = ParsePlaceholderGroup(plan.get(), blob);
}

TEST(Query, ExecWithPredicateLoader) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto counter_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(counter_fid);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "range": {
                    "age": {
                        "GE": -1,
                        "LT": 1
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp time = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    int topk = 5;

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
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 7, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "range": {
                    "age": {
                        "GE": -1,
                        "LT": 1
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";
    int64_t N = 177;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 7, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp time = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    int topk = 5;

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}

TEST(Query, ExecWithPredicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "range": {
                    "age": {
                        "GE": -1,
                        "LT": 1
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp time = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    int topk = 5;

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
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "term": {
                    "age": {
                        "values": [],
                        "is_in_field": false
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";
    int64_t N = ROW_COUNT;
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 3;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp time = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    std::vector<std::vector<std::string>> results;
    int topk = 5;
    auto json = SearchResultToJson(*sr);
    ASSERT_EQ(sr->total_nq_, num_queries);
    ASSERT_EQ(sr->unity_topK_, topk);
    // for(auto x: )
}

TEST(Query, ExecEmpty) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("age", DataType::FLOAT);
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";
    int64_t N = ROW_COUNT;
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp time = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    std::cout << SearchResultToJson(*sr);

    for (auto i : sr->seg_offsets_) {
        ASSERT_EQ(i, -1);
    }

    for (auto v : sr->distances_) {
        ASSERT_EQ(v, std::numeric_limits<float>::max());
    }
}

TEST(Query, ExecWithoutPredicateFlat) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, std::nullopt);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";
    auto plan = CreatePlan(*schema, dsl);
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
    Timestamp time = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    std::vector<std::vector<std::string>> results;
    int topk = 5;
    auto json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
}

TEST(Query, ExecWithoutPredicate) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "l2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal":3
                    }
                }
            }
            ]
        }
    })";
    auto plan = CreatePlan(*schema, dsl);
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
    Timestamp time = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    assert_order(*sr, "l2");
    std::vector<std::vector<std::string>> results;
    int topk = 5;
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
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "vector": {
                    "normalized": {
                        "metric_type": "ip",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal":3
                    }
                }
            }
            ]
        }
    })";
    auto vec_fid = schema->AddDebugField(
        "normalized", DataType::VECTOR_FLOAT, dim, knowhere::metric::IP);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto plan = CreatePlan(*schema, dsl);
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
    std::cout << SearchResultToJson(*sr).dump(2);
}

TEST(Query, FillSegment) {
    namespace pb = milvus::proto;
    pb::schema::CollectionSchema proto;
    proto.set_name("col");
    proto.set_description("asdfhsalkgfhsadg");
    proto.set_autoid(false);
    auto dim = 16;

    {
        auto field = proto.add_fields();
        field->set_name("fakevec");
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
        field->set_fieldid(101);
        field->set_is_primary_key(true);
        field->set_description("asdgfsagf");
        field->set_data_type(pb::schema::DataType::Int64);
    }

    {
        auto field = proto.add_fields();
        field->set_name("the_value");
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
        dataset.get_col<float>(FieldId(100));    // vector field
    const auto std_i32_vec =
        dataset.get_col<int32_t>(FieldId(102));  // scalar field

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
        // auto indexing = GenVecIndexing(N, dim, std_vfloat_vec.data());

        // LoadIndexInfo info;
        // auto field_offset = schema->get_offset(FieldName("fakevec"));
        // auto& meta = schema->operator[](field_offset);

        // info.field_id = meta.get_id().get();
        // info.field_name = meta.get_name().get();
        // info.index_params["metric_type"] = "L2";
        // info.index = indexing;

        // segment->LoadIndex(info);
        return segment;
    }());

    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";
    auto plan = CreatePlan(*schema, dsl);
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
        auto result = segment->Search(plan.get(), ph.get(), ts);
        // std::cout << SearchResultToJson(result).dump(2);
        result->result_offsets_.resize(topk * num_queries);
        segment->FillTargetEntry(plan.get(), *result);
        segment->FillPrimaryKeys(plan.get(), *result);

        auto& fields_data = result->output_fields_data_;
        ASSERT_EQ(fields_data.size(), 2);
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

        for (int i = 0; i < topk * num_queries; i++) {
            int64_t val = std::get<int64_t>(result->primary_keys_[i]);

            auto internal_offset = result->seg_offsets_[i];
            auto std_val = std_vec[internal_offset];
            auto std_i32 = std_i32_vec[internal_offset];
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
            }
        }
    }
}

TEST(Query, ExecWithPredicateBinary) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_BINARY, 512, knowhere::metric::JACCARD);
    auto float_fid = schema->AddDebugField("age", DataType::FLOAT);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "range": {
                    "age": {
                        "GE": -1,
                        "LT": 1
                    }
                }
            },
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "JACCARD",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5,
                        "round_decimal": 3
                    }
                }
            }
            ]
        }
    })";
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

    auto plan = CreatePlan(*schema, dsl);
    auto num_queries = 5;
    auto ph_group_raw = CreateBinaryPlaceholderGroupFromBlob(
        num_queries, 512, vec_ptr.data() + 1024 * 512 / 8);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    Timestamp time = 1000000;

    auto sr = segment->Search(plan.get(), ph_group.get(), time);
    int topk = 5;

    query::Json json = SearchResultToJson(*sr);
    std::cout << json.dump(2);
    // ASSERT_EQ(json.dump(2), ref.dump(2));
}
