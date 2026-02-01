// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <folly/FBVector.h>
#include <simdjson.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <ratio>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <variant>
#include <vector>

#include "ExprTestBase.h"
#include "bitset/bitset.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/Json.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/expression/EvalCtx.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/dom/array.h"
#include "simdjson/dom/element.h"
#include "simdjson/error.h"
#include "simdjson/padded_string.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

EXPR_TEST_INSTANTIATE();

TEST_P(ExprTest, TestUnaryRangeWithJSON) {
    // Test cases: {expression string, reference function, data type, json key}
    std::vector<
        std::tuple<std::string,
                   std::function<bool(
                       std::variant<int64_t, bool, double, std::string_view>)>,
                   DataType,
                   std::string>>
        testcases = {
            {R"(json["bool"] == true)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<bool>(v);
             },
             DataType::BOOL,
             "bool"},
            {R"(json["int"] <= 1500)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<int64_t>(v) < 1500;
             },
             DataType::INT64,
             "int"},
            {R"(json["double"] <= 4000)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<double>(v) <= 4000;
             },
             DataType::DOUBLE,
             "double"},
            {R"(json["double"] > 1000)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<double>(v) > 1000;
             },
             DataType::DOUBLE,
             "double"},
            {R"(json["int"] >= 0)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<int64_t>(v) >= 0;
             },
             DataType::INT64,
             "int"},
            {R"(json["bool"] != true)",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return !std::get<bool>(v);
             },
             DataType::BOOL,
             "bool"},
            {R"(json["string"] == "test")",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return std::get<std::string_view>(v) == "test";
             },
             DataType::STRING,
             "string"},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);

    for (auto [expr_str, ref_func, dtype, json_key] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::string json_path = "/" + json_key;
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>(json_path)
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(json_path)
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(json_path)
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>(json_path)
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestUnaryRangeWithJSONNullable) {
    // Test cases: {expression string, reference function, data type, json key}
    std::vector<std::tuple<
        std::string,
        std::function<bool(
            std::variant<int64_t, bool, double, std::string_view>, bool)>,
        DataType,
        std::string>>
        testcases = {
            {R"(json["bool"] == true)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<bool>(v);
             },
             DataType::BOOL,
             "bool"},
            {R"(json["int"] <= 1500)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<int64_t>(v) < 1500;
             },
             DataType::INT64,
             "int"},
            {R"(json["double"] <= 4000)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<double>(v) <= 4000;
             },
             DataType::DOUBLE,
             "double"},
            {R"(json["double"] > 1000)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<double>(v) > 1000;
             },
             DataType::DOUBLE,
             "double"},
            {R"(json["int"] >= 0)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<int64_t>(v) >= 0;
             },
             DataType::INT64,
             "int"},
            {R"(json["bool"] != true)",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return true;
                 }
                 return !std::get<bool>(v);
             },
             DataType::BOOL,
             "bool"},
            {R"(json["string"] == "test")",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return std::get<std::string_view>(v) == "test";
             },
             DataType::STRING,
             "string"},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);

    for (auto [expr_str, ref_func, dtype, json_key] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::string json_path = "/" + json_key;
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>(json_path)
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(json_path)
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(json_path)
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>(json_path)
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestNullExprWithJSON) {
    // Test cases: {expression, expected result function}
    std::vector<std::tuple<std::string, std::function<bool(bool)>>> testcases =
        {
            {R"(json is null)", [](bool v) { return !v; }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);
    SetSchema(schema);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    int num_iters = 1;
    FixedVector<bool> valid_data;
    std::vector<std::string> json_col;

    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        auto new_valid_col = raw_data.get_col_valid(json_fid);
        valid_data.insert(
            valid_data.end(), new_valid_col.begin(), new_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto [expr, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);
        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto valid = valid_data[i];
            auto ref = ref_func(valid);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST_P(ExprTest, TestTermWithJSON) {
    // Test cases: {expression string, reference function, data type, json key}
    std::vector<
        std::tuple<std::string,
                   std::function<bool(
                       std::variant<int64_t, bool, double, std::string_view>)>,
                   DataType,
                   std::string>>
        testcases = {
            {R"(json["bool"] in [true])",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<bool> term_set;
                 term_set = {true, false};
                 return term_set.find(std::get<bool>(v)) != term_set.end();
             },
             DataType::BOOL,
             "bool"},
            {R"(json["int"] in [1500, 2048, 3216])",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<int64_t> term_set;
                 term_set = {1500, 2048, 3216};
                 return term_set.find(std::get<int64_t>(v)) != term_set.end();
             },
             DataType::INT64,
             "int"},
            {R"(json["double"] in [1500.0, 4000, 235.14])",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<double> term_set;
                 term_set = {1500.0, 4000, 235.14};
                 return term_set.find(std::get<double>(v)) != term_set.end();
             },
             DataType::DOUBLE,
             "double"},
            {R"(json["string"] in ["aaa", "abc", "235.14"])",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 std::unordered_set<std::string_view> term_set;
                 term_set = {"aaa", "abc", "235.14"};
                 return term_set.find(std::get<std::string_view>(v)) !=
                        term_set.end();
             },
             DataType::STRING,
             "string"},
            {R"(json["int"] in [])",
             [](std::variant<int64_t, bool, double, std::string_view> v) {
                 return false;
             },
             DataType::INT64,
             "int"},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);

    for (auto [expr_str, ref_func, dtype, json_key] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::string json_path = "/" + json_key;
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>(json_path)
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(json_path)
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(json_path)
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>(json_path)
                               .value();
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestTermWithJSONNullable) {
    // Test cases: {expression string, reference function, data type, json key}
    std::vector<std::tuple<
        std::string,
        std::function<bool(
            std::variant<int64_t, bool, double, std::string_view>, bool)>,
        DataType,
        std::string>>
        testcases = {
            {R"(json["bool"] in [true])",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 std::unordered_set<bool> term_set;
                 term_set = {true, false};
                 return term_set.find(std::get<bool>(v)) != term_set.end();
             },
             DataType::BOOL,
             "bool"},
            {R"(json["int"] in [1500, 2048, 3216])",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 std::unordered_set<int64_t> term_set;
                 term_set = {1500, 2048, 3216};
                 return term_set.find(std::get<int64_t>(v)) != term_set.end();
             },
             DataType::INT64,
             "int"},
            {R"(json["double"] in [1500.0, 4000, 235.14])",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 std::unordered_set<double> term_set;
                 term_set = {1500.0, 4000, 235.14};
                 return term_set.find(std::get<double>(v)) != term_set.end();
             },
             DataType::DOUBLE,
             "double"},
            {R"(json["string"] in ["aaa", "abc", "235.14"])",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 std::unordered_set<std::string_view> term_set;
                 term_set = {"aaa", "abc", "235.14"};
                 return term_set.find(std::get<std::string_view>(v)) !=
                        term_set.end();
             },
             DataType::STRING,
             "string"},
            {R"(json["int"] in [])",
             [](std::variant<int64_t, bool, double, std::string_view> v,
                bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return false;
             },
             DataType::INT64,
             "int"},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);

    for (auto [expr_str, ref_func, dtype, json_key] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::string json_path = "/" + json_key;
            if (dtype == DataType::BOOL) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<bool>(json_path)
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(json_path)
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(json_path)
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::STRING) {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<std::string_view>(json_path)
                               .value();
                auto ref = ref_func(val, valid_data[i]);
                ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << expr_str << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestExistsWithJSON) {
    // Test cases: {expression string, reference function, json key}
    std::vector<std::tuple<std::string, std::function<bool(bool)>, std::string>>
        testcases = {
            {R"(exists json["bool"])", [](bool v) { return v; }, "bool"},
            {R"(exists json["int"])", [](bool v) { return v; }, "int"},
            {R"(exists json["string"])", [](bool v) { return v; }, "string"},
            {R"(exists json["varchar"])", [](bool v) { return v; }, "varchar"},
            {R"(exists json["double"])", [](bool v) { return v; }, "double"},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);

    for (auto [expr_str, ref_func, json_key] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::string json_path = "/" + json_key;
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .exist(json_path);
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!" << val;
            }
        }
    }
}

TEST_P(ExprTest, TestExistsWithJSONNullable) {
    // Test cases: {expression string, reference function, json key}
    std::vector<
        std::tuple<std::string, std::function<bool(bool, bool)>, std::string>>
        testcases = {
            {R"(exists json["bool"])",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             "bool"},
            {R"(exists json["int"])",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             "int"},
            {R"(exists json["string"])",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             "string"},
            {R"(exists json["varchar"])",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             "varchar"},
            {R"(exists json["double"])",
             [](bool v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v;
             },
             "double"},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);

    for (auto [expr_str, ref_func, json_key] : testcases) {
        auto plan_str = create_search_plan_from_expr(expr_str);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            std::string json_path = "/" + json_key;
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .exist(json_path);
            auto ref = ref_func(val, valid_data[i]);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!" << val;
            }
        }
    }
}

template <typename T>
struct Testcase {
    std::vector<T> term;
    std::vector<std::string> nested_path;
    bool res;
};

TEST_P(ExprTest, TestTermInFieldJson) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    int num_iters = 1;
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
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    std::vector<Testcase<bool>> bool_testcases{{{true}, {"bool"}},
                                               {{false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
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
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
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
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
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
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }
}

TEST_P(ExprTest, TestTermInFieldJsonNullable) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGenForJsonArray(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data = raw_data.get_col_valid(json_fid);

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
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    std::vector<Testcase<bool>> bool_testcases{{{true}, {"bool"}},
                                               {{false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<bool> res;
            for (const auto& element : array) {
                res.push_back(element.template get<bool>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<double>> double_testcases{
        {{1.123}, {"double"}},
        {{10.34}, {"double"}},
        {{100.234}, {"double"}},
        {{1000.4546}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<double> res;
            for (const auto& element : array) {
                res.push_back(element.template get<double>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<int64_t>> testcases{
        {{1}, {"int"}},
        {{10}, {"int"}},
        {{100}, {"int"}},
        {{1000}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values, bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<int64_t> res;
            for (const auto& element : array) {
                res.push_back(element.template get<int64_t>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }

    std::vector<Testcase<std::string>> testcases_string = {
        {{"1sads"}, {"string"}},
        {{"10dsf"}, {"string"}},
        {{"100"}, {"string"}},
        {{"100ddfdsssdfdsfsd0"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values,
                         bool valid) {
            if (!valid) {
                return false;
            }
            return std::find(values.begin(), values.end(), testcase.term[0]) !=
                   values.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Json(simdjson::padded_string(json_col[i]))
                             .array_at(pointer);
            std::vector<std::string_view> res;
            for (const auto& element : array) {
                res.push_back(element.template get<std::string_view>());
            }
            ASSERT_EQ(ans, check(res, valid_data[i]));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, valid_data[i]));
            }
        }
    }
}
