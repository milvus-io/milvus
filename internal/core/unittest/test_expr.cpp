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
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
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
    schema->AddDebugField("fakevec", DataType::VECTOR_BINARY, 512, MetricType::METRIC_Jaccard);
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
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
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
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
    schema->AddDebugField("age", DataType::INT32);
    ASSERT_ANY_THROW(CreatePlan(*schema, dsl_string));
}

TEST(Expr, ShowExecutor) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto node = std::make_unique<FloatVectorANNS>();
    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
    int64_t num_queries = 100L;
    auto raw_data = DataGen(schema, num_queries);
    auto& info = node->search_info_;

    info.metric_type_ = MetricType::METRIC_L2;
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
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
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
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
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
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
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
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
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
    auto vec_fid = schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
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
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
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
