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

#include <boost/format.hpp>
#include <gtest/gtest.h>
#include <regex>

#include "query/Expr.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "query/generated/ShowPlanNodeVisitor.h"
#include "query/generated/ExecExprVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "index/IndexFactory.h"

using namespace milvus;

TEST(Expr, Naive) {
    SUCCEED();
    std::string dsl_string = R"(
{
    "bool": {
        "must": [
            {
                "term": {
                    "A": [
                        1,
                        2,
                        5
                    ]
                }
            },
            {
                "range": {
                    "B": {
                        "GT": 1,
                        "LT": 100
                    }
                }
            },
            {
                "vector": {
                    "Vec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10
                    }
                }
            }
        ]
    }
})";
}

TEST(Expr, Range) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::string dsl_string = R"({
        "bool": {
            "must": [
                {
                    "range": {
                        "age": {
                            "GT": 1,
                            "LT": 100
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
                            "topk": 10,
                            "round_decimal": 3
                        }
                    }
                }
            ]
        }
    })";
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT32);
    auto plan = CreatePlan(*schema, dsl_string);
    ShowPlanNodeVisitor shower;
    Assert(plan->tag2field_.at("$0") == schema->get_field_id(FieldName("fakevec")));
    auto out = shower.call_child(*plan->plan_node_);
    std::cout << out.dump(4);
}

TEST(Expr, RangeBinary) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::string dsl_string = R"({
        "bool": {
            "must": [
                {
                    "range": {
                        "age": {
                            "GT": 1,
                            "LT": 100
                        }
                    }
                },
                {
                    "vector": {
                        "fakevec": {
                            "metric_type": "Jaccard",
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
    schema->AddDebugField("fakevec", DataType::VECTOR_BINARY, 512, knowhere::metric::JACCARD);
    schema->AddDebugField("age", DataType::INT32);
    auto plan = CreatePlan(*schema, dsl_string);
    ShowPlanNodeVisitor shower;
    Assert(plan->tag2field_.at("$0") == schema->get_field_id(FieldName("fakevec")));
    auto out = shower.call_child(*plan->plan_node_);
    std::cout << out.dump(4);
}

TEST(Expr, InvalidRange) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::string dsl_string = R"(
{
    "bool": {
        "must": [
            {
                "range": {
                    "age": {
                        "GT": 1,
                        "LT": "100"
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
                        "topk": 10
                    }
                }
            }
        ]
    }
})";
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT32);
    ASSERT_ANY_THROW(CreatePlan(*schema, dsl_string));
}

TEST(Expr, InvalidDSL) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::string dsl_string = R"({
        "float": {
            "must": [
                {
                    "range": {
                        "age": {
                            "GT": 1,
                            "LT": 100
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
                            "topk": 10
                        }
                    }
                }
            ]
        }
    })";

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT32);
    ASSERT_ANY_THROW(CreatePlan(*schema, dsl_string));
}

TEST(Expr, ShowExecutor) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto node = std::make_unique<FloatVectorANNS>();
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
    auto field_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, metric_type);
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
    dup["data"] = "...collased...";
    std::cout << dup.dump(4);
}

TEST(Expr, TestRange) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {R"("GT": 2000, "LT": 3000)", [](int v) { return 2000 < v && v < 3000; }},
        {R"("GE": 2000, "LT": 3000)", [](int v) { return 2000 <= v && v < 3000; }},
        {R"("GT": 2000, "LE": 3000)", [](int v) { return 2000 < v && v <= 3000; }},
        {R"("GE": 2000, "LE": 3000)", [](int v) { return 2000 <= v && v <= 3000; }},
        {R"("GE": 2000)", [](int v) { return v >= 2000; }},
        {R"("GT": 2000)", [](int v) { return v > 2000; }},
        {R"("LE": 2000)", [](int v) { return v <= 2000; }},
        {R"("LT": 2000)", [](int v) { return v < 2000; }},
        {R"("EQ": 2000)", [](int v) { return v == 2000; }},
        {R"("NE": 2000)", [](int v) { return v != 2000; }},
    };

    std::string dsl_string_tmp = R"({
        "bool": {
            "must": [
                {
                    "range": {
                        "age": {
                            @@@@
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
                            "topk": 10,
                            "round_decimal": 3
                        }
                    }
                }
            ]
        }
    })";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema);
    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int>(i64_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N, N, raw_data.row_ids_.data(), raw_data.timestamps_.data(), raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(*seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = dsl_string_tmp.find("@@@@");
        auto dsl_string = dsl_string_tmp;
        dsl_string.replace(loc, 4, clause);
        auto plan = CreatePlan(*schema, dsl_string);
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
        }
    }
}

TEST(Expr, TestTerm) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto vec_2k_3k = [] {
        std::string buf = "[";
        for (int i = 2000; i < 3000 - 1; ++i) {
            buf += std::to_string(i) + ", ";
        }
        buf += std::to_string(2999) + "]";
        return buf;
    }();

    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {R"([2000, 3000])", [](int v) { return v == 2000 || v == 3000; }},
        {R"([2000])", [](int v) { return v == 2000; }},
        {R"([3000])", [](int v) { return v == 3000; }},
        {R"([])", [](int v) { return false; }},
        {vec_2k_3k, [](int v) { return 2000 <= v && v < 3000; }},
    };

    std::string dsl_string_tmp = R"({
        "bool": {
            "must": [
                {
                    "term": {
                        "age": {
                            "values": @@@@
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
                            "topk": 10,
                            "round_decimal": 3
                        }
                    }
                }
            ]
        }
    })";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema);
    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int>(i64_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N, N, raw_data.row_ids_.data(), raw_data.timestamps_.data(), raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(*seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = dsl_string_tmp.find("@@@@");
        auto dsl_string = dsl_string_tmp;
        dsl_string.replace(loc, 4, clause);
        auto plan = CreatePlan(*schema, dsl_string);
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
        }
    }
}

TEST(Expr, TestSimpleDsl) {
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto vec_dsl = Json::parse(R"({
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
            })");

    int N = 32;
    auto get_item = [&](int base, int bit = 1) {
        std::vector<int> terms;
        // note: random gen range is [0, 2N)
        for (int i = 0; i < N * 2; ++i) {
            if (((i >> base) & 0x1) == bit) {
                terms.push_back(i);
            }
        }
        Json s;
        s["term"]["age"]["values"] = terms;
        return s;
    };
    // std::cout << get_item(0).dump(-2);
    // std::cout << vec_dsl.dump(-2);
    std::vector<std::tuple<Json, std::function<bool(int)>>> testcases;
    {
        Json dsl;
        dsl["must"] = Json::array({vec_dsl, get_item(0), get_item(1), get_item(2, 0), get_item(3)});
        testcases.emplace_back(dsl, [](int64_t x) { return (x & 0b1111) == 0b1011; });
    }

    {
        Json dsl;
        Json sub_dsl;
        sub_dsl["must"] = Json::array({get_item(0), get_item(1), get_item(2, 0), get_item(3)});
        dsl["must"] = Json::array({sub_dsl, vec_dsl});
        testcases.emplace_back(dsl, [](int64_t x) { return (x & 0b1111) == 0b1011; });
    }

    {
        Json dsl;
        Json sub_dsl;
        sub_dsl["should"] = Json::array({get_item(0), get_item(1), get_item(2, 0), get_item(3)});
        dsl["must"] = Json::array({sub_dsl, vec_dsl});
        testcases.emplace_back(dsl, [](int64_t x) { return !!((x & 0b1111) ^ 0b0100); });
    }

    {
        Json dsl;
        Json sub_dsl;
        sub_dsl["must_not"] = Json::array({get_item(0), get_item(1), get_item(2, 0), get_item(3)});
        dsl["must"] = Json::array({sub_dsl, vec_dsl});
        testcases.emplace_back(dsl, [](int64_t x) { return (x & 0b1111) != 0b1011; });
    }

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema);
    std::vector<int64_t> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int64_t>(i64_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N, N, raw_data.row_ids_.data(), raw_data.timestamps_.data(), raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(*seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        Json dsl;
        dsl["bool"] = clause;
        // std::cout << dsl.dump(2);
        auto plan = CreatePlan(*schema, dsl.dump());
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            bool ans = final[i];
            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
        }
    }
}

TEST(Expr, TestCompare) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int, int64_t)>>> testcases = {
        {R"("LT")", [](int a, int64_t b) { return a < b; }},  {R"("LE")", [](int a, int64_t b) { return a <= b; }},
        {R"("GT")", [](int a, int64_t b) { return a > b; }},  {R"("GE")", [](int a, int64_t b) { return a >= b; }},
        {R"("EQ")", [](int a, int64_t b) { return a == b; }}, {R"("NE")", [](int a, int64_t b) { return a != b; }},
    };

    std::string dsl_string_tpl = R"({
        "bool": {
            "must": [
                {
                    "compare": {
                        %1%: [
                            "age1",
                            "age2"
                        ]
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
                            "topk": 10,
                            "round_decimal": 3
                        }
                    }
                }
            ]
        }
    })";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i32_fid = schema->AddDebugField("age1", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age2", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema);
    int N = 1000;
    std::vector<int> age1_col;
    std::vector<int64_t> age2_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age1_col = raw_data.get_col<int>(i32_fid);
        auto new_age2_col = raw_data.get_col<int64_t>(i64_fid);
        age1_col.insert(age1_col.end(), new_age1_col.begin(), new_age1_col.end());
        age2_col.insert(age2_col.end(), new_age2_col.begin(), new_age2_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N, N, raw_data.row_ids_.data(), raw_data.timestamps_.data(), raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(*seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string = boost::str(boost::format(dsl_string_tpl) % clause);
        auto plan = CreatePlan(*schema, dsl_string);
        // std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val1 = age1_col[i];
            auto val2 = age2_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << boost::format("[%1%, %2%]") % val1 % val2;
        }
    }
}

TEST(Expr, TestCompareWithScalarIndex) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int, int64_t)>>> testcases = {
        {R"(LessThan)", [](int a, int64_t b) { return a < b; }},
        {R"(LessEqual)", [](int a, int64_t b) { return a <= b; }},
        {R"(GreaterThan)", [](int a, int64_t b) { return a > b; }},
        {R"(GreaterEqual)", [](int a, int64_t b) { return a >= b; }},
        {R"(Equal)", [](int a, int64_t b) { return a == b; }},
        {R"(NotEqual)", [](int a, int64_t b) { return a != b; }},
    };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                compare_expr: <
                                                    left_column_info: <
                                                        field_id: %3%
                                                        data_type: %4%
                                                    >
                                                    right_column_info: <
                                                        field_id: %5%
                                                        data_type: %6%
                                                    >
                                                    op: %2%
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    index::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_fid);
    age32_col[0] = 1000;
    GenScalarIndexing(N, age32_col.data());
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data());
    load_index_info.field_id = i32_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index = std::move(age32_index);
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    GenScalarIndexing(N, age64_col.data());
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index = std::move(age64_index);
    seg->LoadIndex(load_index_info);

    ExecExprVisitor visitor(*seg, seg->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string = boost::format(serialized_expr_plan) % vec_fid.get() % clause % i32_fid.get() %
                          proto::schema::DataType_Name(int(DataType::INT32)) % i64_fid.get() %
                          proto::schema::DataType_Name(int(DataType::INT64));
        auto binary_plan = translate_text_plan_to_binary_plan(dsl_string.str().data());
        auto plan = CreateSearchPlanByExpr(*schema, binary_plan.data(), binary_plan.size());
        // std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = age32_col[i];
            auto val2 = age64_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << boost::format("[%1%, %2%]") % val1 % val2;
        }
    }
}

TEST(Expr, TestCompareWithScalarIndexMaris) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(std::string, std::string)>>> testcases = {
        {R"(LessThan)", [](std::string a, std::string b) { return a.compare(b) < 0; }},
        {R"(LessEqual)", [](std::string a, std::string b) { return a.compare(b) <= 0; }},
        {R"(GreaterThan)", [](std::string a, std::string b) { return a.compare(b) > 0; }},
        {R"(GreaterEqual)", [](std::string a, std::string b) { return a.compare(b) >= 0; }},
        {R"(Equal)", [](std::string a, std::string b) { return a.compare(b) == 0; }},
        {R"(NotEqual)", [](std::string a, std::string b) { return a.compare(b) != 0; }},
    };

    const char* serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                compare_expr: <
                                                    left_column_info: <
                                                        field_id: %3%
                                                        data_type: VarChar
                                                    >
                                                    right_column_info: <
                                                        field_id: %4%
                                                        data_type: VarChar
                                                    >
                                                    op: %2%
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    index::LoadIndexInfo load_index_info;

    // load index for int32 field
    auto str1_col = raw_data.get_col<std::string>(str1_fid);
    GenScalarIndexing(N, str1_col.data());
    auto str1_index = milvus::index::CreateScalarIndexSort<std::string>();
    str1_index->Build(N, str1_col.data());
    load_index_info.field_id = str1_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index = std::move(str1_index);
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto str2_col = raw_data.get_col<std::string>(str2_fid);
    GenScalarIndexing(N, str2_col.data());
    auto str2_index = milvus::index::CreateScalarIndexSort<std::string>();
    str2_index->Build(N, str2_col.data());
    load_index_info.field_id = str2_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index = std::move(str2_index);
    seg->LoadIndex(load_index_info);

    ExecExprVisitor visitor(*seg, seg->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto dsl_string =
            boost::format(serialized_expr_plan) % vec_fid.get() % clause % str1_fid.get() % str2_fid.get();
        auto binary_plan = translate_text_plan_to_binary_plan(dsl_string.str().data());
        auto plan = CreateSearchPlanByExpr(*schema, binary_plan.data(), binary_plan.size());
        //         std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = str1_col[i];
            auto val2 = str2_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << boost::format("[%1%, %2%]") % val1 % val2;
        }
    }
}

TEST(Expr, TestBinaryArithOpEvalRange) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int)>, DataType>> testcases = {
        // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
        {R"("EQ": {
            "ADD": {
                "right_operand": 4,
                "value": 8
            }
        })",
         [](int8_t v) { return (v + 4) == 8; }, DataType::INT8},
        {R"("EQ": {
            "SUB": {
                "right_operand": 500,
                "value": 1500
            }
        })",
         [](int16_t v) { return (v - 500) == 1500; }, DataType::INT16},
        {R"("EQ": {
            "MUL": {
                "right_operand": 2,
                "value": 4000
            }
        })",
         [](int32_t v) { return (v * 2) == 4000; }, DataType::INT32},
        {R"("EQ": {
            "DIV": {
                "right_operand": 2,
                "value": 1000
            }
        })",
         [](int64_t v) { return (v / 2) == 1000; }, DataType::INT64},
        {R"("EQ": {
            "MOD": {
                "right_operand": 100,
                "value": 0
            }
        })",
         [](int32_t v) { return (v % 100) == 0; }, DataType::INT32},
        {R"("EQ": {
            "ADD": {
                "right_operand": 500,
                "value": 2500
            }
        })",
         [](float v) { return (v + 500) == 2500; }, DataType::FLOAT},
        {R"("EQ": {
            "ADD": {
                "right_operand": 500,
                "value": 2500
            }
        })",
         [](double v) { return (v + 500) == 2500; }, DataType::DOUBLE},
        // Add test cases for BinaryArithOpEvalRangeExpr NE of various data types
        {R"("NE": {
            "ADD": {
                "right_operand": 500,
                "value": 2500
            }
        })",
         [](float v) { return (v + 500) != 2500; }, DataType::FLOAT},
        {R"("NE": {
            "SUB": {
                "right_operand": 500,
                "value": 2500
            }
        })",
         [](double v) { return (v - 500) != 2500; }, DataType::DOUBLE},
        {R"("NE": {
            "MUL": {
                "right_operand": 2,
                "value": 2
            }
        })",
         [](int8_t v) { return (v * 2) != 2; }, DataType::INT8},
        {R"("NE": {
            "DIV": {
                "right_operand": 2,
                "value": 1000
            }
        })",
         [](int16_t v) { return (v / 2) != 1000; }, DataType::INT16},
        {R"("NE": {
            "MOD": {
                "right_operand": 100,
                "value": 0
            }
        })",
         [](int32_t v) { return (v % 100) != 0; }, DataType::INT32},
        {R"("NE": {
            "ADD": {
                "right_operand": 500,
                "value": 2500
            }
        })",
         [](int64_t v) { return (v + 500) != 2500; }, DataType::INT64},
    };

    std::string dsl_string_tmp = R"({
        "bool": {
            "must": [
                {
                    "range": {
                        @@@@@
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
                            "topk": 10,
                            "round_decimal": 3
                        }
                    }
                }
            ]
        }
    })";

    std::string dsl_string_int8 = R"(
        "age8": {
            @@@@
        })";

    std::string dsl_string_int16 = R"(
        "age16": {
            @@@@
        })";

    std::string dsl_string_int32 = R"(
        "age32": {
            @@@@
        })";

    std::string dsl_string_int64 = R"(
        "age64": {
            @@@@
        })";

    std::string dsl_string_float = R"(
        "age_float": {
            @@@@
        })";

    std::string dsl_string_double = R"(
        "age_double": {
            @@@@
        })";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i8_fid = schema->AddDebugField("age8", DataType::INT8);
    auto i16_fid = schema->AddDebugField("age16", DataType::INT16);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto float_fid = schema->AddDebugField("age_float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("age_double", DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema);
    int N = 1000;
    std::vector<int8_t> age8_col;
    std::vector<int16_t> age16_col;
    std::vector<int32_t> age32_col;
    std::vector<int64_t> age64_col;
    std::vector<float> age_float_col;
    std::vector<double> age_double_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        auto new_age8_col = raw_data.get_col<int8_t>(i8_fid);
        auto new_age16_col = raw_data.get_col<int16_t>(i16_fid);
        auto new_age32_col = raw_data.get_col<int32_t>(i32_fid);
        auto new_age64_col = raw_data.get_col<int64_t>(i64_fid);
        auto new_age_float_col = raw_data.get_col<float>(float_fid);
        auto new_age_double_col = raw_data.get_col<double>(double_fid);

        age8_col.insert(age8_col.end(), new_age8_col.begin(), new_age8_col.end());
        age16_col.insert(age16_col.end(), new_age16_col.begin(), new_age16_col.end());
        age32_col.insert(age32_col.end(), new_age32_col.begin(), new_age32_col.end());
        age64_col.insert(age64_col.end(), new_age64_col.begin(), new_age64_col.end());
        age_float_col.insert(age_float_col.end(), new_age_float_col.begin(), new_age_float_col.end());
        age_double_col.insert(age_double_col.end(), new_age_double_col.begin(), new_age_double_col.end());

        seg->PreInsert(N);
        seg->Insert(iter * N, N, raw_data.row_ids_.data(), raw_data.timestamps_.data(), raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(*seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = dsl_string_tmp.find("@@@@@");
        auto dsl_string = dsl_string_tmp;
        if (dtype == DataType::INT8) {
            dsl_string.replace(loc, 5, dsl_string_int8);
        } else if (dtype == DataType::INT16) {
            dsl_string.replace(loc, 5, dsl_string_int16);
        } else if (dtype == DataType::INT32) {
            dsl_string.replace(loc, 5, dsl_string_int32);
        } else if (dtype == DataType::INT64) {
            dsl_string.replace(loc, 5, dsl_string_int64);
        } else if (dtype == DataType::FLOAT) {
            dsl_string.replace(loc, 5, dsl_string_float);
        } else if (dtype == DataType::DOUBLE) {
            dsl_string.replace(loc, 5, dsl_string_double);
        } else {
            ASSERT_TRUE(false) << "No test case defined for this data type";
        }
        loc = dsl_string.find("@@@@");
        dsl_string.replace(loc, 4, clause);
        auto plan = CreatePlan(*schema, dsl_string);
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::INT8) {
                auto val = age8_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT16) {
                auto val = age16_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT32) {
                auto val = age32_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT64) {
                auto val = age64_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::FLOAT) {
                auto val = age_float_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::DOUBLE) {
                auto val = age_double_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST(Expr, TestBinaryArithOpEvalRangeExceptions) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::string, DataType>> testcases = {
        // Add test for data type mismatch
        {R"("EQ": {
            "ADD": {
                "right_operand": 500,
                "value": 2500.00
            }
        })",
         "Assert \"(value.is_number_integer())\"", DataType::INT32},
        {R"("EQ": {
            "ADD": {
                "right_operand": 500.0,
                "value": 2500
            }
        })",
         "Assert \"(right_operand.is_number_integer())\"", DataType::INT32},
        {R"("EQ": {
            "ADD": {
                "right_operand": 500.0,
                "value": true
            }
        })",
         "Assert \"(value.is_number())\"", DataType::FLOAT},
        {R"("EQ": {
            "ADD": {
                "right_operand": "500",
                "value": 2500.0
            }
        })",
         "Assert \"(right_operand.is_number())\"", DataType::FLOAT},
        // Check unsupported arithmetic operator type
        {R"("EQ": {
            "EXP": {
                "right_operand": 500,
                "value": 2500
            }
        })",
         "arith op(exp) not found", DataType::INT32},
        // Check unsupported data type
        {R"("EQ": {
            "ADD": {
                "right_operand": true,
                "value": false
            }
        })",
         "bool type is not supported", DataType::BOOL},
    };

    std::string dsl_string_tmp = R"({
        "bool": {
            "must": [
                {
                    "range": {
                        @@@@@
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
                            "topk": 10,
                            "round_decimal": 3
                        }
                    }
                }
            ]
        }
    })";

    std::string dsl_string_int = R"(
        "age": {
            @@@@
        })";

    std::string dsl_string_num = R"(
        "FloatN": {
            @@@@
        })";

    std::string dsl_string_bool = R"(
        "BoolField": {
            @@@@
        })";

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT32);
    schema->AddDebugField("FloatN", DataType::FLOAT);
    schema->AddDebugField("BoolField", DataType::BOOL);

    for (auto [clause, assert_info, dtype] : testcases) {
        auto loc = dsl_string_tmp.find("@@@@@");
        auto dsl_string = dsl_string_tmp;
        if (dtype == DataType::INT32) {
            dsl_string.replace(loc, 5, dsl_string_int);
        } else if (dtype == DataType::FLOAT) {
            dsl_string.replace(loc, 5, dsl_string_num);
        } else if (dtype == DataType::BOOL) {
            dsl_string.replace(loc, 5, dsl_string_bool);
        } else {
            ASSERT_TRUE(false) << "No test case defined for this data type";
        }

        loc = dsl_string.find("@@@@");
        dsl_string.replace(loc, 4, clause);

        try {
            auto plan = CreatePlan(*schema, dsl_string);
            FAIL() << "Expected AssertionError: " << assert_info << " not thrown";
        } catch (const std::exception& err) {
            std::string err_msg = err.what();
            ASSERT_TRUE(err_msg.find(assert_info) != std::string::npos);
        } catch (...) {
            FAIL() << "Expected AssertionError: " << assert_info << " not thrown";
        }
    }
}

TEST(Expr, TestBinaryArithOpEvalRangeWithScalarSortIndex) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int)>, DataType>> testcases = {
        // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
        {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: Equal
            value: <
                int64_val: 8
            >)",
         [](int8_t v) { return (v + 4) == 8; }, DataType::INT8},
        {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: Equal
            value: <
                int64_val: 1500
            >)",
         [](int16_t v) { return (v - 500) == 1500; }, DataType::INT16},
        {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: Equal
            value: <
                int64_val: 4000
            >)",
         [](int32_t v) { return (v * 2) == 4000; }, DataType::INT32},
        {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: Equal
            value: <
                int64_val: 1000
            >)",
         [](int64_t v) { return (v / 2) == 1000; }, DataType::INT64},
        {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: Equal
            value: <
                int64_val: 0
            >)",
         [](int32_t v) { return (v % 100) == 0; }, DataType::INT32},
        {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: Equal
            value: <
                float_val: 2500
            >)",
         [](float v) { return (v + 500) == 2500; }, DataType::FLOAT},
        {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: Equal
            value: <
                float_val: 2500
            >)",
         [](double v) { return (v + 500) == 2500; }, DataType::DOUBLE},
        {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: NotEqual
            value: <
                float_val: 2000
            >)",
         [](float v) { return (v + 500) != 2000; }, DataType::FLOAT},
        {R"(arith_op: Sub
            right_operand: <
                float_val: 500
            >
            op: NotEqual
            value: <
                float_val: 2500
            >)",
         [](double v) { return (v - 500) != 2000; }, DataType::DOUBLE},
        {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: NotEqual
            value: <
                int64_val: 2
            >)",
         [](int8_t v) { return (v * 2) != 2; }, DataType::INT8},
        {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: NotEqual
            value: <
                int64_val: 2000
            >)",
         [](int16_t v) { return (v / 2) != 2000; }, DataType::INT16},
        {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: NotEqual
            value: <
                int64_val: 1
            >)",
         [](int32_t v) { return (v % 100) != 1; }, DataType::INT32},
        {R"(arith_op: Add
            right_operand: <
                int64_val: 500
            >
            op: NotEqual
            value: <
                int64_val: 2000
            >)",
         [](int64_t v) { return (v + 500) != 2000; }, DataType::INT64},
    };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                binary_arith_op_eval_range_expr: <
                                                    @@@@@
                                                >
                                            >
                                            query_info: <
                                                topk: 10
                                                round_decimal: 3
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0"
     >)";

    std::string arith_expr = R"(
    column_info: <
        field_id: %2%
        data_type: %3%
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i8_fid = schema->AddDebugField("age8", DataType::INT8);
    auto i16_fid = schema->AddDebugField("age16", DataType::INT16);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto float_fid = schema->AddDebugField("age_float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("age_double", DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    index::LoadIndexInfo load_index_info;

    // load index for int8 field
    auto age8_col = raw_data.get_col<int8_t>(i8_fid);
    age8_col[0] = 4;
    GenScalarIndexing(N, age8_col.data());
    auto age8_index = milvus::index::CreateScalarIndexSort<int8_t>();
    age8_index->Build(N, age8_col.data());
    load_index_info.field_id = i8_fid.get();
    load_index_info.field_type = DataType::INT8;
    load_index_info.index = std::move(age8_index);
    seg->LoadIndex(load_index_info);

    // load index for 16 field
    auto age16_col = raw_data.get_col<int16_t>(i16_fid);
    age16_col[0] = 2000;
    GenScalarIndexing(N, age16_col.data());
    auto age16_index = milvus::index::CreateScalarIndexSort<int16_t>();
    age16_index->Build(N, age16_col.data());
    load_index_info.field_id = i16_fid.get();
    load_index_info.field_type = DataType::INT16;
    load_index_info.index = std::move(age16_index);
    seg->LoadIndex(load_index_info);

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_fid);
    age32_col[0] = 2000;
    GenScalarIndexing(N, age32_col.data());
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data());
    load_index_info.field_id = i32_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index = std::move(age32_index);
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    GenScalarIndexing(N, age64_col.data());
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data());
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index = std::move(age64_index);
    seg->LoadIndex(load_index_info);

    // load index for float field
    auto age_float_col = raw_data.get_col<float>(float_fid);
    age_float_col[0] = 2000;
    GenScalarIndexing(N, age_float_col.data());
    auto age_float_index = milvus::index::CreateScalarIndexSort<float>();
    age_float_index->Build(N, age_float_col.data());
    load_index_info.field_id = float_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index = std::move(age_float_index);
    seg->LoadIndex(load_index_info);

    // load index for double field
    auto age_double_col = raw_data.get_col<double>(double_fid);
    age_double_col[0] = 2000;
    GenScalarIndexing(N, age_double_col.data());
    auto age_double_index = milvus::index::CreateScalarIndexSort<double>();
    age_double_index->Build(N, age_double_col.data());
    load_index_info.field_id = double_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index = std::move(age_double_index);
    seg->LoadIndex(load_index_info);

    auto seg_promote = dynamic_cast<SegmentSealedImpl*>(seg.get());
    ExecExprVisitor visitor(*seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        if (dtype == DataType::INT8) {
            expr = boost::format(expr_plan) % vec_fid.get() % i8_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT8));
        } else if (dtype == DataType::INT16) {
            expr = boost::format(expr_plan) % vec_fid.get() % i16_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT16));
        } else if (dtype == DataType::INT32) {
            expr = boost::format(expr_plan) % vec_fid.get() % i32_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT32));
        } else if (dtype == DataType::INT64) {
            expr = boost::format(expr_plan) % vec_fid.get() % i64_fid.get() %
                   proto::schema::DataType_Name(int(DataType::INT64));
        } else if (dtype == DataType::FLOAT) {
            expr = boost::format(expr_plan) % vec_fid.get() % float_fid.get() %
                   proto::schema::DataType_Name(int(DataType::FLOAT));
        } else if (dtype == DataType::DOUBLE) {
            expr = boost::format(expr_plan) % vec_fid.get() % double_fid.get() %
                   proto::schema::DataType_Name(int(DataType::DOUBLE));
        } else {
            ASSERT_TRUE(false) << "No test case defined for this data type";
        }

        auto binary_plan = translate_text_plan_to_binary_plan(expr.str().data());
        auto plan = CreateSearchPlanByExpr(*schema, binary_plan.data(), binary_plan.size());

        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            if (dtype == DataType::INT8) {
                auto val = age8_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT16) {
                auto val = age16_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT32) {
                auto val = age32_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT64) {
                auto val = age64_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::FLOAT) {
                auto val = age_float_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::DOUBLE) {
                auto val = age_double_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}
