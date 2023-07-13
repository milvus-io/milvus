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
#include <cstdint>
#include <memory>
#include <regex>
#include <vector>
#include <chrono>

#include "common/Json.h"
#include "common/Types.h"
#include "pb/plan.pb.h"
#include "query/Expr.h"
#include "query/ExprImpl.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "query/generated/ShowPlanNodeVisitor.h"
#include "query/generated/ExecExprVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/padded_string.h"
#include "segcore/segment_c.h"
#include "test_utils/DataGen.h"
#include "index/IndexFactory.h"

TEST(Expr, Range) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    // std::string dsl_string = R"({
    //     "bool": {
    //         "must": [
    //             {
    //                 "range": {
    //                     "age": {
    //                         "GT": 1,
    //                         "LT": 100
    //                     }
    //                 }
    //             },
    //             {
    //                 "vector": {
    //                     "fakevec": {
    //                         "metric_type": "L2",
    //                         "params": {
    //                             "nprobe": 10
    //                         },
    //                         "query": "$0",
    //                         "topk": 10,
    //                         "round_decimal": 3
    //                     }
    //                 }
    //             }
    //         ]
    //     }
    // })";

    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_expr: <
                                    op: LogicalAnd
                                    left: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        op: GreaterThan
                                        value: <
                                          int64_val: 1
                                        >
                                      >
                                    >
                                    right: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        op: LessThan
                                        value: <
                                          int64_val: 100
                                        >
                                      >
                                    >
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
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT32);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    ShowPlanNodeVisitor shower;
    Assert(plan->tag2field_.at("$0") ==
           schema->get_field_id(FieldName("fakevec")));
}

TEST(Expr, RangeBinary) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    // std::string dsl_string = R"({
    //     "bool": {
    //         "must": [
    //             {
    //                 "range": {
    //                     "age": {
    //                         "GT": 1,
    //                         "LT": 100
    //                     }
    //                 }
    //             },
    //             {
    //                 "vector": {
    //                     "fakevec": {
    //                         "metric_type": "Jaccard",
    //                         "params": {
    //                             "nprobe": 10
    //                         },
    //                         "query": "$0",
    //                         "topk": 10,
    //                         "round_decimal": 3
    //                     }
    //                 }
    //             }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_expr: <
                                    op: LogicalAnd
                                    left: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        op: GreaterThan
                                        value: <
                                          int64_val: 1
                                        >
                                      >
                                    >
                                    right: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        op: LessThan
                                        value: <
                                          int64_val: 100
                                        >
                                      >
                                    >
                                  >
                                >
                                query_info: <
                                  topk: 10
                                  round_decimal: 3
                                  metric_type: "JACCARD"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
     >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_BINARY, 512, knowhere::metric::JACCARD);
    schema->AddDebugField("age", DataType::INT32);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    ShowPlanNodeVisitor shower;
    Assert(plan->tag2field_.at("$0") ==
           schema->get_field_id(FieldName("fakevec")));
}

TEST(Expr, InvalidRange) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    //     std::string dsl_string = R"(
    // {
    //     "bool": {
    //         "must": [
    //             {
    //                 "range": {
    //                     "age": {
    //                         "GT": 1,
    //                         "LT": "100"
    //                     }
    //                 }
    //             },
    //             {
    //                 "vector": {
    //                     "fakevec": {
    //                         "metric_type": "L2",
    //                         "params": {
    //                             "nprobe": 10
    //                         },
    //                         "query": "$0",
    //                         "topk": 10
    //                     }
    //                 }
    //             }
    //         ]
    //     }
    // })";
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                  binary_expr: <
                                    op: LogicalAnd
                                    left: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Int64
                                        >
                                        op: GreaterThan
                                        value: <
                                          int64_val: 1
                                        >
                                      >
                                    >
                                    right: <
                                      unary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int64
                                        >
                                        op: LessThan
                                        value: <
                                          int64_val: 100
                                        >
                                      >
                                    >
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
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age", DataType::INT32);
    ASSERT_ANY_THROW(
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size()));
}

TEST(Expr, ShowExecutor) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto node = std::make_unique<FloatVectorANNS>();
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
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
    dup["data"] = "...collased...";
    std::cout << dup.dump(4);
}

TEST(Expr, TestRange) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {R"(binary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              lower_inclusive: false,
              upper_inclusive: false,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
         [](int v) { return 2000 < v && v < 3000; }},
        {R"(binary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              lower_inclusive: true,
              upper_inclusive: false,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
         [](int v) { return 2000 <= v && v < 3000; }},
        {R"(binary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              lower_inclusive: false,
              upper_inclusive: true,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
         [](int v) { return 2000 < v && v <= 3000; }},
        {R"(binary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              lower_inclusive: true,
              upper_inclusive: true,
              lower_value: <
                int64_val: 2000
              >
              upper_value: <
                int64_val: 3000
              >
        >)",
         [](int v) { return 2000 <= v && v <= 3000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: GreaterEqual,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v >= 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: GreaterThan,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v > 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: LessEqual,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v <= 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: LessThan,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v < 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: Equal,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v == 2000; }},
        {R"(unary_range_expr: <
              column_info: <
                field_id: 101
                data_type: Int64
              >
              op: NotEqual,
              value: <
                int64_val: 2000
              >
        >)",
         [](int v) { return v != 2000; }},
    };

    // std::string dsl_string_tmp = R"({
    //     "bool": {
    //         "must": [
    //             {
    //                 "range": {
    //                     "age": {
    //                         @@@@
    //                     }
    //                 }
    //             },
    //             {
    //                 "vector": {
    //                     "fakevec": {
    //                         "metric_type": "L2",
    //                         "params": {
    //                             "nprobe": 10
    //                         },
    //                         "query": "$0",
    //                         "topk": 10,
    //                         "round_decimal": 3
    //                     }
    //                 }
    //             }
    //         ]
    //     }
    // })";
    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int>(i64_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
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

TEST(Expr, TestBinaryRangeJSON) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    struct Testcase {
        bool lower_inclusive;
        bool upper_inclusive;
        int64_t lower;
        int64_t upper;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {true, false, 10, 20, {"int"}},
        {true, true, 20, 30, {"int"}},
        {false, true, 30, 40, {"int"}},
        {false, false, 40, 50, {"int"}},
        {true, false, 10, 20, {"double"}},
        {true, true, 20, 30, {"double"}},
        {false, true, 30, 40, {"double"}},
        {false, false, 40, 50, {"double"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](int64_t value) {
            int64_t lower = testcase.lower, upper = testcase.upper;
            if (!testcase.lower_inclusive) {
                lower++;
            }
            if (!testcase.upper_inclusive) {
                upper--;
            }
            return lower <= value && value <= upper;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<BinaryRangeExprImpl<int64_t>>(
            ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::GenericValue::ValCase::kInt64Val,
            testcase.lower_inclusive,
            testcase.upper_inclusive,
            testcase.lower,
            testcase.upper);
        auto final = visitor.call_child(*plan.predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            if (testcase.nested_path[0] == "int") {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(pointer)
                               .value();
                auto ref = check(val);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
            } else {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(pointer)
                               .value();
                auto ref = check(val);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
            }
        }
    }
}

TEST(Expr, TestExistsJson) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    struct Testcase {
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {{"A"}},
        {{"int"}},
        {{"double"}},
        {{"B"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](bool value) { return value; };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        plan.predicate_ = std::make_unique<ExistsExprImpl>(
            ColumnInfo(json_fid, DataType::JSON, testcase.nested_path));
        auto final = visitor.call_child(*plan.predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .exist(pointer);
            auto ref = check(val);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST(Expr, TestUnaryRangeJson) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    struct Testcase {
        int64_t val;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {10, {"int"}},
        {20, {"int"}},
        {30, {"int"}},
        {40, {"int"}},
        {10, {"double"}},
        {20, {"double"}},
        {30, {"double"}},
        {40, {"double"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    std::vector<OpType> ops{
        OpType::Equal,
        OpType::NotEqual,
        OpType::GreaterThan,
        OpType::GreaterEqual,
        OpType::LessThan,
        OpType::LessEqual,
    };
    for (const auto& testcase : testcases) {
        auto check = [&](int64_t value) { return value == testcase.val; };
        std::function<bool(int64_t)> f = check;
        for (auto& op : ops) {
            switch (op) {
                case OpType::Equal: {
                    f = [&](int64_t value) { return value == testcase.val; };
                    break;
                }
                case OpType::NotEqual: {
                    f = [&](int64_t value) { return value != testcase.val; };
                    break;
                }
                case OpType::GreaterEqual: {
                    f = [&](int64_t value) { return value >= testcase.val; };
                    break;
                }
                case OpType::GreaterThan: {
                    f = [&](int64_t value) { return value > testcase.val; };
                    break;
                }
                case OpType::LessEqual: {
                    f = [&](int64_t value) { return value <= testcase.val; };
                    break;
                }
                case OpType::LessThan: {
                    f = [&](int64_t value) { return value < testcase.val; };
                    break;
                }
                default: {
                    PanicInfo("unsupported range node");
                }
            }

            RetrievePlanNode plan;
            auto pointer = milvus::Json::pointer(testcase.nested_path);
            plan.predicate_ = std::make_unique<UnaryRangeExprImpl<int64_t>>(
                ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
                op,
                testcase.val,
                proto::plan::GenericValue::ValCase::kInt64Val);
            auto final = visitor.call_child(*plan.predicate_.value());
            EXPECT_EQ(final.size(), N * num_iters);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                if (testcase.nested_path[0] == "int") {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<int64_t>(pointer)
                            .value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                } else {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<double>(pointer)
                            .value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                }
            }
        }
    }
}

TEST(Expr, TestTermJson) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    struct Testcase {
        std::vector<int64_t> term;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {{1, 2, 3, 4}, {"int"}},
        {{10, 100, 1000, 10000}, {"int"}},
        {{100, 10000, 9999, 444}, {"int"}},
        {{23, 42, 66, 17, 25}, {"int"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](int64_t value) {
            std::unordered_set<int64_t> term_set(testcase.term.begin(),
                                                 testcase.term.end());
            return term_set.find(value) != term_set.end();
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        plan.predicate_ = std::make_unique<TermExprImpl<int64_t>>(
            ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
            testcase.term,
            proto::plan::GenericValue::ValCase::kInt64Val);
        auto final = visitor.call_child(*plan.predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<int64_t>(pointer)
                           .value();
            auto ref = check(val);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST(Expr, TestTerm) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto vec_2k_3k = [] {
        std::string buf;
        for (int i = 2000; i < 3000; ++i) {
            buf += "values: < int64_val: " + std::to_string(i) + " >\n";
        }
        return buf;
    }();

    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {R"(values: <
                int64_val: 2000
            >
            values: <
                int64_val: 3000
            >
        )",
         [](int v) { return v == 2000 || v == 3000; }},
        {R"(values: <
                int64_val: 2000
            >)",
         [](int v) { return v == 2000; }},
        {R"(values: <
                int64_val: 3000
            >)",
         [](int v) { return v == 3000; }},
        {R"()", [](int v) { return false; }},
        {vec_2k_3k, [](int v) { return 2000 <= v && v < 3000; }},
    };

    // std::string dsl_string_tmp = R"({
    //     "bool": {
    //         "must": [
    //             {
    //                 "term": {
    //                     "age": {
    //                         "values": @@@@,
    //                         "is_in_field" : false
    //                     }
    //                 }
    //             },
    //             {
    //                 "vector": {
    //                     "fakevec": {
    //                         "metric_type": "L2",
    //                         "params": {
    //                             "nprobe": 10
    //                         },
    //                         "query": "$0",
    //                         "topk": 10,
    //                         "round_decimal": 3
    //                     }
    //                 }
    //             }
    //         ]
    //     }
    // })";
    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      term_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int64
                                        >
                                        @@@@
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int>(i64_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
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

TEST(Expr, TestCompare) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int, int64_t)>>>
        testcases = {
            {R"(LessThan)", [](int a, int64_t b) { return a < b; }},
            {R"(LessEqual)", [](int a, int64_t b) { return a <= b; }},
            {R"(GreaterThan)", [](int a, int64_t b) { return a > b; }},
            {R"(GreaterEqual)", [](int a, int64_t b) { return a >= b; }},
            {R"(Equal)", [](int a, int64_t b) { return a == b; }},
            {R"(NotEqual)", [](int a, int64_t b) { return a != b; }},
        };

    // std::string dsl_string_tpl = R"({
    //     "bool": {
    //         "must": [
    //             {
    //                 "compare": {
    //                     %1%: [
    //                         "age1",
    //                         "age2"
    //                     ]
    //                 }
    //             },
    //             {
    //                 "vector": {
    //                     "fakevec": {
    //                         "metric_type": "L2",
    //                         "params": {
    //                             "nprobe": 10
    //                         },
    //                         "query": "$0",
    //                         "topk": 10,
    //                         "round_decimal": 3
    //                     }
    //                 }
    //             }
    //         ]
    //     }
    // })";
    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      compare_expr: <
                                        left_column_info: <
                                          field_id: 101
                                          data_type: Int32
                                        >
                                        right_column_info: <
                                          field_id: 102
                                          data_type: Int64
                                        >
                                        op: @@@@
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i32_fid = schema->AddDebugField("age1", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age2", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age1_col;
    std::vector<int64_t> age2_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age1_col = raw_data.get_col<int>(i32_fid);
        auto new_age2_col = raw_data.get_col<int64_t>(i64_fid);
        age1_col.insert(
            age1_col.end(), new_age1_col.begin(), new_age1_col.end());
        age2_col.insert(
            age2_col.end(), new_age2_col.begin(), new_age2_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val1 = age1_col[i];
            auto val2 = age2_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
        }
    }
}

TEST(Expr, TestCompareWithScalarIndex) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int, int64_t)>>>
        testcases = {
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

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
        auto dsl_string =
            boost::format(serialized_expr_plan) % vec_fid.get() % clause %
            i32_fid.get() % proto::schema::DataType_Name(int(DataType::INT32)) %
            i64_fid.get() % proto::schema::DataType_Name(int(DataType::INT64));
        auto binary_plan =
            translate_text_plan_to_binary_plan(dsl_string.str().data());
        auto plan = CreateSearchPlanByExpr(
            *schema, binary_plan.data(), binary_plan.size());
        // std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = age32_col[i];
            auto val2 = age64_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
        }
    }
}

TEST(Expr, TestCompareExpr) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto str3_fid = schema->AddDebugField("string3", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    auto fields = schema->get_fields();
    for (auto field_data : raw_data.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();

        auto info = FieldDataInfo(field_data.field_id(), N, "/tmp/a");
        auto field_meta = fields.at(FieldId(field_id));
        info.channel->push(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.channel->close();

        seg->LoadFieldData(FieldId(field_id), info);
    }

    ExecExprVisitor visitor(*seg, seg->get_row_count(), MAX_TIMESTAMP);
    auto build_expr = [&](enum DataType type) -> std::shared_ptr<query::Expr> {
        switch (type) {
            case DataType::BOOL: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::BOOL;
                compare_expr->left_field_id_ = bool_fid;

                compare_expr->right_data_type_ = DataType::BOOL;
                compare_expr->right_field_id_ = bool_1_fid;
                return compare_expr;
            }
            case DataType::INT8: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::INT8;
                compare_expr->left_field_id_ = int8_fid;

                compare_expr->right_data_type_ = DataType::INT8;
                compare_expr->right_field_id_ = int8_1_fid;
                return compare_expr;
            }
            case DataType::INT16: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::INT16;
                compare_expr->left_field_id_ = int16_fid;

                compare_expr->right_data_type_ = DataType::INT16;
                compare_expr->right_field_id_ = int16_1_fid;
                return compare_expr;
            }
            case DataType::INT32: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::INT32;
                compare_expr->left_field_id_ = int32_fid;

                compare_expr->right_data_type_ = DataType::INT32;
                compare_expr->right_field_id_ = int32_1_fid;
                return compare_expr;
            }
            case DataType::INT64: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::INT64;
                compare_expr->left_field_id_ = int64_fid;

                compare_expr->right_data_type_ = DataType::INT64;
                compare_expr->right_field_id_ = int64_1_fid;
                return compare_expr;
            }
            case DataType::FLOAT: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::FLOAT;
                compare_expr->left_field_id_ = float_fid;

                compare_expr->right_data_type_ = DataType::FLOAT;
                compare_expr->right_field_id_ = float_1_fid;
                return compare_expr;
            }
            case DataType::DOUBLE: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::DOUBLE;
                compare_expr->left_field_id_ = double_fid;

                compare_expr->right_data_type_ = DataType::DOUBLE;
                compare_expr->right_field_id_ = double_1_fid;
                return compare_expr;
            }
            case DataType::VARCHAR: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::VARCHAR;
                compare_expr->left_field_id_ = str2_fid;

                compare_expr->right_data_type_ = DataType::VARCHAR;
                compare_expr->right_field_id_ = str3_fid;
                return compare_expr;
            }
            default:
                return std::make_shared<query::CompareExpr>();
        }
    };
    std::cout << "start compare test" << std::endl;
    auto expr = build_expr(DataType::BOOL);
    auto final = visitor.call_child(*expr);
    expr = build_expr(DataType::INT8);
    final = visitor.call_child(*expr);
    expr = build_expr(DataType::INT16);
    final = visitor.call_child(*expr);
    expr = build_expr(DataType::INT32);
    final = visitor.call_child(*expr);
    expr = build_expr(DataType::INT64);
    final = visitor.call_child(*expr);
    expr = build_expr(DataType::FLOAT);
    final = visitor.call_child(*expr);
    expr = build_expr(DataType::DOUBLE);
    final = visitor.call_child(*expr);
    std::cout << "end compare test" << std::endl;
}

TEST(Expr, TestExprs) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000000;
    auto raw_data = DataGen(schema, N);

    // load field data
    auto fields = schema->get_fields();
    for (auto field_data : raw_data.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();

        auto info = FieldDataInfo(field_data.field_id(), N, "/tmp/a");
        auto field_meta = fields.at(FieldId(field_id));
        info.channel->push(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.channel->close();

        seg->LoadFieldData(FieldId(field_id), info);
    }

    ExecExprVisitor visitor(*seg, seg->get_row_count(), MAX_TIMESTAMP);

    enum ExprType {
        UnaryRangeExpr = 0,
        TermExprImpl = 1,
        CompareExpr = 2,
        LogicalUnaryExpr = 3,
        BinaryRangeExpr = 4,
        LogicalBinaryExpr = 5,
        BinaryArithOpEvalRangeExpr = 6,
    };

    auto build_expr = [&](enum ExprType test_type,
                          int n) -> std::shared_ptr<query::Expr> {
        switch (test_type) {
            case UnaryRangeExpr:
                return std::make_shared<query::UnaryRangeExprImpl<int8_t>>(
                    ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::GreaterThan,
                    10,
                    proto::plan::GenericValue::ValCase::kInt64Val);
                break;
            case TermExprImpl: {
                std::vector<std::string> retrieve_ints;
                for (int i = 0; i < n; ++i) {
                    retrieve_ints.push_back("xxxxxx" + std::to_string(i % 10));
                }
                return std::make_shared<query::TermExprImpl<std::string>>(
                    ColumnInfo(str1_fid, DataType::VARCHAR),
                    retrieve_ints,
                    proto::plan::GenericValue::ValCase::kStringVal);
                // std::vector<double> retrieve_ints;
                // for (int i = 0; i < n; ++i) {
                //     retrieve_ints.push_back(i);
                // }
                // return std::make_shared<query::TermExprImpl<double>>(
                //     ColumnInfo(double_fid, DataType::DOUBLE),
                //     retrieve_ints,
                //     proto::plan::GenericValue::ValCase::kFloatVal);
                break;
            }
            case CompareExpr: {
                auto compare_expr = std::make_shared<query::CompareExpr>();
                compare_expr->op_type_ = OpType::LessThan;

                compare_expr->left_data_type_ = DataType::INT8;
                compare_expr->left_field_id_ = int8_fid;

                compare_expr->right_data_type_ = DataType::INT8;
                compare_expr->right_field_id_ = int8_1_fid;
                return compare_expr;
                break;
            }
            case BinaryRangeExpr: {
                return std::make_shared<query::BinaryRangeExprImpl<int64_t>>(
                    ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::GenericValue::ValCase::kInt64Val,
                    true,
                    true,
                    10,
                    45);
                break;
            }
            case LogicalUnaryExpr: {
                ExprPtr child_expr =
                    std::make_unique<query::UnaryRangeExprImpl<int32_t>>(
                        ColumnInfo(int32_fid, DataType::INT32),
                        proto::plan::OpType::GreaterThan,
                        10,
                        proto::plan::GenericValue::ValCase::kInt64Val);
                return std::make_shared<query::LogicalUnaryExpr>(
                    LogicalUnaryExpr::OpType::LogicalNot, child_expr);
                break;
            }
            case LogicalBinaryExpr: {
                ExprPtr child1_expr =
                    std::make_unique<query::UnaryRangeExprImpl<int8_t>>(
                        ColumnInfo(int8_fid, DataType::INT8),
                        proto::plan::OpType::GreaterThan,
                        10,
                        proto::plan::GenericValue::ValCase::kInt64Val);
                ExprPtr child2_expr =
                    std::make_unique<query::UnaryRangeExprImpl<int8_t>>(
                        ColumnInfo(int8_fid, DataType::INT8),
                        proto::plan::OpType::NotEqual,
                        10,
                        proto::plan::GenericValue::ValCase::kInt64Val);
                return std::make_shared<query::LogicalBinaryExpr>(
                    LogicalBinaryExpr::OpType::LogicalXor,
                    child1_expr,
                    child2_expr);
                break;
            }
            case BinaryArithOpEvalRangeExpr: {
                return std::make_shared<
                    query::BinaryArithOpEvalRangeExprImpl<int8_t>>(
                    ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::GenericValue::ValCase::kInt64Val,
                    proto::plan::ArithOpType::Add,
                    10,
                    proto::plan::OpType::Equal,
                    100);
                break;
            }
            default:
                return std::make_shared<query::BinaryRangeExprImpl<int64_t>>(
                    ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::GenericValue::ValCase::kInt64Val,
                    true,
                    true,
                    10,
                    45);
                break;
        }
    };
    auto test_case = [&](int n) {
        auto expr = build_expr(TermExprImpl, n);
        std::cout << "start test" << std::endl;
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*expr);
        std::cout << n << "cost: "
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << "us" << std::endl;
    };
    test_case(3);
    test_case(10);
    test_case(20);
    test_case(30);
    test_case(50);
    test_case(100);
    test_case(200);
    // test_case(500);
}

TEST(Expr, TestCompareWithScalarIndexMaris) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<
        std::tuple<std::string, std::function<bool(std::string, std::string)>>>
        testcases = {
            {R"(LessThan)",
             [](std::string a, std::string b) { return a.compare(b) < 0; }},
            {R"(LessEqual)",
             [](std::string a, std::string b) { return a.compare(b) <= 0; }},
            {R"(GreaterThan)",
             [](std::string a, std::string b) { return a.compare(b) > 0; }},
            {R"(GreaterEqual)",
             [](std::string a, std::string b) { return a.compare(b) >= 0; }},
            {R"(Equal)",
             [](std::string a, std::string b) { return a.compare(b) == 0; }},
            {R"(NotEqual)",
             [](std::string a, std::string b) { return a.compare(b) != 0; }},
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

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
        auto dsl_string = boost::format(serialized_expr_plan) % vec_fid.get() %
                          clause % str1_fid.get() % str2_fid.get();
        auto binary_plan =
            translate_text_plan_to_binary_plan(dsl_string.str().data());
        auto plan = CreateSearchPlanByExpr(
            *schema, binary_plan.data(), binary_plan.size());
        //         std::cout << ShowPlanNodeVisitor().call_child(*plan->plan_node_) << std::endl;
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = str1_col[i];
            auto val2 = str2_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
        }
    }
}

TEST(Expr, TestBinaryArithOpEvalRange) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int)>, DataType>>
        testcases = {
            // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Add
                  right_operand: <
                    int64_val: 4
                  >
                  op: Equal
                  value: <
                    int64_val: 8
                  >
             >)",
             [](int8_t v) { return (v + 4) == 8; },
             DataType::INT8},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Sub
                  right_operand: <
                    int64_val: 500
                  >
                  op: Equal
                  value: <
                    int64_val: 1500
                  >
             >)",
             [](int16_t v) { return (v - 500) == 1500; },
             DataType::INT16},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 4000
                  >
             >)",
             [](int32_t v) { return (v * 2) == 4000; },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: Equal
                  value: <
                    int64_val: 1000
                  >
             >)",
             [](int64_t v) { return (v / 2) == 1000; },
             DataType::INT64},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: Equal
                  value: <
                    int64_val: 0
                  >
             >)",
             [](int32_t v) { return (v % 100) == 0; },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: Equal
                  value: <
                    float_val: 2500
                  >
             >)",
             [](float v) { return (v + 500) == 2500; },
             DataType::FLOAT},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: Equal
                  value: <
                    float_val: 2500
                  >
             >)",
             [](double v) { return (v + 500) == 2500; },
             DataType::DOUBLE},
            // Add test cases for BinaryArithOpEvalRangeExpr NE of various data types
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 105
                    data_type: Float
                  >
                  arith_op: Add
                  right_operand: <
                    float_val: 500
                  >
                  op: NotEqual
                  value: <
                    float_val: 2500
                  >
             >)",
             [](float v) { return (v + 500) != 2500; },
             DataType::FLOAT},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 106
                    data_type: Double
                  >
                  arith_op: Sub
                  right_operand: <
                    float_val: 500
                  >
                  op: NotEqual
                  value: <
                    float_val: 2500
                  >
             >)",
             [](double v) { return (v - 500) != 2500; },
             DataType::DOUBLE},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 101
                    data_type: Int8
                  >
                  arith_op: Mul
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2
                  >
             >)",
             [](int8_t v) { return (v * 2) != 2; },
             DataType::INT8},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 102
                    data_type: Int16
                  >
                  arith_op: Div
                  right_operand: <
                    int64_val: 2
                  >
                  op: NotEqual
                  value: <
                    int64_val: 1000
                  >
             >)",
             [](int16_t v) { return (v / 2) != 1000; },
             DataType::INT16},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 103
                    data_type: Int32
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 100
                  >
                  op: NotEqual
                  value: <
                    int64_val: 0
                  >
             >)",
             [](int32_t v) { return (v % 100) != 0; },
             DataType::INT32},
            {R"(binary_arith_op_eval_range_expr: <
                  column_info: <
                    field_id: 104
                    data_type: Int64
                  >
                  arith_op: Mod
                  right_operand: <
                    int64_val: 500
                  >
                  op: NotEqual
                  value: <
                    int64_val: 2500
                  >
             >)",
             [](int64_t v) { return (v + 500) != 2500; },
             DataType::INT64},
        };

    // std::string dsl_string_tmp = R"({
    //     "bool": {
    //         "must": [
    //             {
    //                 "range": {
    //                     @@@@@
    //                 }
    //             },
    //             {
    //                 "vector": {
    //                     "fakevec": {
    //                         "metric_type": "L2",
    //                         "params": {
    //                             "nprobe": 10
    //                         },
    //                         "query": "$0",
    //                         "topk": 10,
    //                         "round_decimal": 3
    //                     }
    //                 }
    //             }
    //         ]
    //     }
    // })";

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@@
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i8_fid = schema->AddDebugField("age8", DataType::INT8);
    auto i16_fid = schema->AddDebugField("age16", DataType::INT16);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto float_fid = schema->AddDebugField("age_float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("age_double", DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
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

        age8_col.insert(
            age8_col.end(), new_age8_col.begin(), new_age8_col.end());
        age16_col.insert(
            age16_col.end(), new_age16_col.begin(), new_age16_col.end());
        age32_col.insert(
            age32_col.end(), new_age32_col.begin(), new_age32_col.end());
        age64_col.insert(
            age64_col.end(), new_age64_col.begin(), new_age64_col.end());
        age_float_col.insert(age_float_col.end(),
                             new_age_float_col.begin(),
                             new_age_float_col.end());
        age_double_col.insert(age_double_col.end(),
                              new_age_double_col.begin(),
                              new_age_double_col.end());

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 5, clause);
        // if (dtype == DataType::INT8) {
        //     dsl_string.replace(loc, 5, dsl_string_int8);
        // } else if (dtype == DataType::INT16) {
        //     dsl_string.replace(loc, 5, dsl_string_int16);
        // } else if (dtype == DataType::INT32) {
        //     dsl_string.replace(loc, 5, dsl_string_int32);
        // } else if (dtype == DataType::INT64) {
        //     dsl_string.replace(loc, 5, dsl_string_int64);
        // } else if (dtype == DataType::FLOAT) {
        //     dsl_string.replace(loc, 5, dsl_string_float);
        // } else if (dtype == DataType::DOUBLE) {
        //     dsl_string.replace(loc, 5, dsl_string_double);
        // } else {
        //     ASSERT_TRUE(false) << "No test case defined for this data type";
        // }
        // loc = dsl_string.find("@@@@");
        // dsl_string.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
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

TEST(Expr, TestBinaryArithOpEvalRangeJSON) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    struct Testcase {
        int64_t right_operand;
        int64_t value;
        OpType op;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {10, 20, OpType::Equal, {"int"}},
        {20, 30, OpType::Equal, {"int"}},
        {30, 40, OpType::NotEqual, {"int"}},
        {40, 50, OpType::NotEqual, {"int"}},
        {10, 20, OpType::Equal, {"double"}},
        {20, 30, OpType::Equal, {"double"}},
        {30, 40, OpType::NotEqual, {"double"}},
        {40, 50, OpType::NotEqual, {"double"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](int64_t value) {
            if (testcase.op == OpType::Equal) {
                return value + testcase.right_operand == testcase.value;
            }
            return value + testcase.right_operand != testcase.value;
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        plan.predicate_ =
            std::make_unique<BinaryArithOpEvalRangeExprImpl<int64_t>>(
                ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
                proto::plan::GenericValue::ValCase::kInt64Val,
                ArithOpType::Add,
                testcase.right_operand,
                testcase.op,
                testcase.value);
        auto final = visitor.call_child(*plan.predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            if (testcase.nested_path[0] == "int") {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(pointer)
                               .value();
                auto ref = check(val);
                ASSERT_EQ(ans, ref) << testcase.value << " " << val;
            } else {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(pointer)
                               .value();
                auto ref = check(val);
                ASSERT_EQ(ans, ref) << testcase.value << " " << val;
            }
        }
    }
}

TEST(Expr, TestBinaryArithOpEvalRangeJSONFloat) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    struct Testcase {
        double right_operand;
        double value;
        OpType op;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {10, 20, OpType::Equal, {"double"}},
        {20, 30, OpType::Equal, {"double"}},
        {30, 40, OpType::NotEqual, {"double"}},
        {40, 50, OpType::NotEqual, {"double"}},
        {10, 20, OpType::Equal, {"int"}},
        {20, 30, OpType::Equal, {"int"}},
        {30, 40, OpType::NotEqual, {"int"}},
        {40, 50, OpType::NotEqual, {"int"}},
    };

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](double value) {
            if (testcase.op == OpType::Equal) {
                return value + testcase.right_operand == testcase.value;
            }
            return value + testcase.right_operand != testcase.value;
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        plan.predicate_ =
            std::make_unique<BinaryArithOpEvalRangeExprImpl<double>>(
                ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
                proto::plan::GenericValue::ValCase::kFloatVal,
                ArithOpType::Add,
                testcase.right_operand,
                testcase.op,
                testcase.value);
        auto final = visitor.call_child(*plan.predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<double>(pointer)
                           .value();
            auto ref = check(val);
            ASSERT_EQ(ans, ref) << testcase.value << " " << val;
        }
    }
}

TEST(Expr, TestBinaryArithOpEvalRangeWithScalarSortIndex) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int)>, DataType>>
        testcases = {
            // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(arith_op: Add
            right_operand: <
                int64_val: 4
            >
            op: Equal
            value: <
                int64_val: 8
            >)",
             [](int8_t v) { return (v + 4) == 8; },
             DataType::INT8},
            {R"(arith_op: Sub
            right_operand: <
                int64_val: 500
            >
            op: Equal
            value: <
                int64_val: 1500
            >)",
             [](int16_t v) { return (v - 500) == 1500; },
             DataType::INT16},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: Equal
            value: <
                int64_val: 4000
            >)",
             [](int32_t v) { return (v * 2) == 4000; },
             DataType::INT32},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: Equal
            value: <
                int64_val: 1000
            >)",
             [](int64_t v) { return (v / 2) == 1000; },
             DataType::INT64},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: Equal
            value: <
                int64_val: 0
            >)",
             [](int32_t v) { return (v % 100) == 0; },
             DataType::INT32},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: Equal
            value: <
                float_val: 2500
            >)",
             [](float v) { return (v + 500) == 2500; },
             DataType::FLOAT},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: Equal
            value: <
                float_val: 2500
            >)",
             [](double v) { return (v + 500) == 2500; },
             DataType::DOUBLE},
            {R"(arith_op: Add
            right_operand: <
                float_val: 500
            >
            op: NotEqual
            value: <
                float_val: 2000
            >)",
             [](float v) { return (v + 500) != 2000; },
             DataType::FLOAT},
            {R"(arith_op: Sub
            right_operand: <
                float_val: 500
            >
            op: NotEqual
            value: <
                float_val: 2500
            >)",
             [](double v) { return (v - 500) != 2000; },
             DataType::DOUBLE},
            {R"(arith_op: Mul
            right_operand: <
                int64_val: 2
            >
            op: NotEqual
            value: <
                int64_val: 2
            >)",
             [](int8_t v) { return (v * 2) != 2; },
             DataType::INT8},
            {R"(arith_op: Div
            right_operand: <
                int64_val: 2
            >
            op: NotEqual
            value: <
                int64_val: 2000
            >)",
             [](int16_t v) { return (v / 2) != 2000; },
             DataType::INT16},
            {R"(arith_op: Mod
            right_operand: <
                int64_val: 100
            >
            op: NotEqual
            value: <
                int64_val: 1
            >)",
             [](int32_t v) { return (v % 100) != 1; },
             DataType::INT32},
            {R"(arith_op: Add
            right_operand: <
                int64_val: 500
            >
            op: NotEqual
            value: <
                int64_val: 2000
            >)",
             [](int64_t v) { return (v + 500) != 2000; },
             DataType::INT64},
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
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
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
    segcore::LoadIndexInfo load_index_info;

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
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
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

        auto binary_plan =
            translate_text_plan_to_binary_plan(expr.str().data());
        auto plan = CreateSearchPlanByExpr(
            *schema, binary_plan.data(), binary_plan.size());

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

TEST(Expr, TestUnaryRangeWithJSON) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<
        std::tuple<std::string,
                   std::function<bool(
                       std::variant<int64_t, bool, double, std::string_view>)>,
                   DataType>>
        testcases = {
            {R"(op: Equal
                        value: <
                            bool_val: true
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<bool>(v);
             },
             DataType::BOOL},
            {R"(op: LessEqual
                        value: <
                            int64_val: 1500
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<int64_t>(v) < 1500;
             },
             DataType::INT64},
            {R"(op: LessEqual
                        value: <
                            float_val: 4000
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<double>(v) <= 4000;
             },
             DataType::DOUBLE},
            {R"(op: GreaterThan
                        value: <
                            float_val: 1000
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<double>(v) > 1000;
             },
             DataType::DOUBLE},
            {R"(op: GreaterEqual
                        value: <
                            int64_val: 0
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<int64_t>(v) >= 0;
             },
             DataType::INT64},
            {R"(op: NotEqual
                        value: <
                            bool_val: true
                        >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return !std::get<bool>(v);
             },
             DataType::BOOL},
            {R"(op: Equal
            value: <
                string_val: "test"
            >)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<std::string_view>(v) == "test";
             },
             DataType::STRING},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                unary_range_expr: <
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
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_to_binary_plan(expr.str().data());
        auto plan = CreateSearchPlanByExpr(
            *schema, unary_plan.data(), unary_plan.size());

        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>("/bool")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>("/int")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>("/double")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>("/string")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST(Expr, TestTermWithJSON) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<
        std::tuple<std::string,
                   std::function<bool(
                       std::variant<int64_t, bool, double, std::string_view>)>,
                   DataType>>
        testcases = {
            {R"(values: <bool_val: true>)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<bool> term_set;
                 term_set = {true, false};
                 return term_set.find(std::get<bool>(v)) != term_set.end();
             },
             DataType::BOOL},
            {R"(values: <int64_val: 1500>, values: <int64_val: 2048>, values: <int64_val: 3216>)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<int64_t> term_set;
                 term_set = {1500, 2048, 3216};
                 return term_set.find(std::get<int64_t>(v)) != term_set.end();
             },
             DataType::INT64},
            {R"(values: <float_val: 1500.0>, values: <float_val: 4000>, values: <float_val: 235.14>)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<double> term_set;
                 term_set = {1500.0, 4000, 235.14};
                 return term_set.find(std::get<double>(v)) != term_set.end();
             },
             DataType::DOUBLE},
            {R"(values: <string_val: "aaa">, values: <string_val: "abc">, values: <string_val: "235.14">)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<std::string_view> term_set;
                 term_set = {"aaa", "abc", "235.14"};
                 return term_set.find(std::get<std::string_view>(v)) !=
                        term_set.end();
             },
             DataType::STRING},
            {R"()",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return false;
             },
             DataType::INT64},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                term_expr: <
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
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_to_binary_plan(expr.str().data());
        auto plan = CreateSearchPlanByExpr(
            *schema, unary_plan.data(), unary_plan.size());

        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>("/bool")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>("/int")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>("/double")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>("/string")
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST(Expr, TestExistsWithJSON) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(bool)>, DataType>>
        testcases = {
            {R"()", [](bool v) { return v; }, DataType::BOOL},
            {R"()", [](bool v) { return v; }, DataType::INT64},
            {R"()", [](bool v) { return v; }, DataType::STRING},
            {R"()", [](bool v) { return v; }, DataType::VARCHAR},
            {R"()", [](bool v) { return v; }, DataType::DOUBLE},
        };

    std::string serialized_expr_plan = R"(vector_anns: <
                                            field_id: %1%
                                            predicates: <
                                                exists_expr: <
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
    info: <
        field_id: %2%
        data_type: %3%
        nested_path:"%4%"
    >
    @@@@)";

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    int offset = 0;
    for (auto [clause, ref_func, dtype] : testcases) {
        auto loc = serialized_expr_plan.find("@@@@@");
        auto expr_plan = serialized_expr_plan;
        expr_plan.replace(loc, 5, arith_expr);
        loc = expr_plan.find("@@@@");
        expr_plan.replace(loc, 4, clause);
        boost::format expr;
        switch (dtype) {
            case DataType::BOOL: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "bool";
                break;
            }
            case DataType::INT64: {
                expr =
                    boost::format(expr_plan) % vec_fid.get() % json_fid.get() %
                    proto::schema::DataType_Name(int(DataType::JSON)) % "int";
                break;
            }
            case DataType::DOUBLE: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "double";
                break;
            }
            case DataType::STRING: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "string";
                break;
            }
            case DataType::VARCHAR: {
                expr = boost::format(expr_plan) % vec_fid.get() %
                       json_fid.get() %
                       proto::schema::DataType_Name(int(DataType::JSON)) %
                       "varchar";
                break;
            }
            default: {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }

        auto unary_plan = translate_text_plan_to_binary_plan(expr.str().data());
        auto plan = CreateSearchPlanByExpr(
            *schema, unary_plan.data(), unary_plan.size());

        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/bool");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/int");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/double");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/string");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else if (dtype == DataType::VARCHAR) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .exist("/varchar");
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

template <typename T>
struct Testcase {
    std::vector<T> term;
    std::vector<std::string> nested_path;
};

TEST(Expr, TestTermInFieldJson) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 10000;
    std::vector<std::string> json_col;
    int num_iters = 2;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);

    std::vector<Testcase<bool>> bool_testcases{{{true}, {"bool"}},
                                               {{false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        plan.predicate_ = std::make_unique<TermExprImpl<bool>>(
            ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
            testcase.term,
            proto::plan::GenericValue::ValCase::kBoolVal,
            true);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        // std::cout << "cost"
        //           << std::chrono::duration_cast<std::chrono::microseconds>(
        //                  std::chrono::steady_clock::now() - start)
        //                  .count()
        //           << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res));
        }
    }

    std::vector<Testcase<double>> double_testcases{
        {{1.123}, {"double"}},
        {{10.34}, {"double"}},
        {{100.234}, {"double"}},
        {{1000.4546}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        plan.predicate_ = std::make_unique<TermExprImpl<double>>(
            ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
            testcase.term,
            proto::plan::GenericValue::ValCase::kFloatVal,
            true);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res));
        }
    }

    std::vector<Testcase<int64_t>> testcases{
        {{1}, {"int"}},
        {{10}, {"int"}},
        {{100}, {"int"}},
        {{1000}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        plan.predicate_ = std::make_unique<TermExprImpl<int64_t>>(
            ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
            testcase.term,
            proto::plan::GenericValue::ValCase::kInt64Val,
            true);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res));
        }
    }

    std::vector<Testcase<std::string>> testcases_string = {
        {{"1sads"}, {"string"}},
        {{"10dsf"}, {"string"}},
        {{"100"}, {"string"}},
        {{"100ddfdsssdfdsfsd0"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        plan.predicate_ = std::make_unique<TermExprImpl<std::string>>(
            ColumnInfo(json_fid, DataType::JSON, testcase.nested_path),
            testcase.term,
            proto::plan::GenericValue::ValCase::kStringVal,
            true);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res));
        }
    }
}
