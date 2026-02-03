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
#include <thread>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "ExprTestBase.h"
#include "bitset/bitset.h"
#include "common/IndexMeta.h"
#include "common/Json.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/expression/EvalCtx.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/error.h"
#include "simdjson/padded_string.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

EXPR_TEST_INSTANTIATE();

TEST_P(ExprTest, TestTermJson) {
    struct Testcase {
        std::string expr;
        std::vector<int64_t> term;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {R"(json["int"] in [1, 2, 3, 4])", {1, 2, 3, 4}, {"int"}},
        {R"(json["int"] in [10, 100, 1000, 10000])",
         {10, 100, 1000, 10000},
         {"int"}},
        {R"(json["int"] in [100, 10000, 9999, 444])",
         {100, 10000, 9999, 444},
         {"int"}},
        {R"(json["int"] in [23, 42, 66, 17, 25])",
         {23, 42, 66, 17, 25},
         {"int"}},
    };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    SetSchema(schema);
    for (auto testcase : testcases) {
        auto check = [&](int64_t value) {
            std::unordered_set<int64_t> term_set(testcase.term.begin(),
                                                 testcase.term.end());
            return term_set.find(value) != term_set.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto plan_str = create_search_plan_from_expr(testcase.expr);
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
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<int64_t>(pointer)
                           .value();
            auto ref = check(val);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST_P(ExprTest, TestTermJsonNullable) {
    struct Testcase {
        std::string expr;
        std::vector<int64_t> term;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{
        {R"(json["int"] in [1, 2, 3, 4])", {1, 2, 3, 4}, {"int"}},
        {R"(json["int"] in [10, 100, 1000, 10000])",
         {10, 100, 1000, 10000},
         {"int"}},
        {R"(json["int"] in [100, 10000, 9999, 444])",
         {100, 10000, 9999, 444},
         {"int"}},
        {R"(json["int"] in [23, 42, 66, 17, 25])",
         {23, 42, 66, 17, 25},
         {"int"}},
    };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        auto new_valid_data_col = raw_data.get_col_valid(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        valid_data_col.insert(valid_data_col.end(),
                              new_valid_data_col.begin(),
                              new_valid_data_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    SetSchema(schema);
    for (auto testcase : testcases) {
        auto check = [&](int64_t value, bool valid) {
            if (!valid) {
                return false;
            }
            std::unordered_set<int64_t> term_set(testcase.term.begin(),
                                                 testcase.term.end());
            return term_set.find(value) != term_set.end();
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto plan_str = create_search_plan_from_expr(testcase.expr);
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
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<int64_t>(pointer)
                           .value();
            auto ref = check(val, valid_data_col[i]);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST_P(ExprTest, TestTerm) {
    // Build a term expression for values 2000..2999
    auto vec_2k_3k_expr = [] {
        std::string buf = "age in [";
        for (int i = 2000; i < 3000; ++i) {
            if (i > 2000)
                buf += ", ";
            buf += std::to_string(i);
        }
        buf += "]";
        return buf;
    }();

    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {"age in [2000, 3000]", [](int v) { return v == 2000 || v == 3000; }},
        {"age in [2000]", [](int v) { return v == 2000; }},
        {"age in [3000]", [](int v) { return v == 3000; }},
        {"age in []", [](int v) { return false; }},
        {vec_2k_3k_expr, [](int v) { return 2000 <= v && v < 3000; }},
    };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
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
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
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

            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref) << expr_str << "@" << i << "!!" << val;
            }
        }
    }
}

TEST_P(ExprTest, TestTermNullable) {
    // Build a term expression for values 2000..2999
    auto vec_2k_3k_expr = [] {
        std::string buf = "nullable in [";
        for (int i = 2000; i < 3000; ++i) {
            if (i > 2000)
                buf += ", ";
            buf += std::to_string(i);
        }
        buf += "]";
        return buf;
    }();

    std::vector<std::tuple<std::string, std::function<bool(int, bool)>>>
        testcases = {
            {"nullable in [2000, 3000]",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v == 2000 || v == 3000;
             }},
            {"nullable in [2000]",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v == 2000;
             }},
            {"nullable in [3000]",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v == 3000;
             }},
            {"nullable in []",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return false;
             }},
            {vec_2k_3k_expr,
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 <= v && v < 3000;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> nullable_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_nullable_col = raw_data.get_col<int>(nullable_fid);
        auto new_valid_data_col = raw_data.get_col_valid(nullable_fid);
        valid_data_col.insert(valid_data_col.end(),
                              new_valid_data_col.begin(),
                              new_valid_data_col.end());
        nullable_col.insert(nullable_col.end(),
                            new_nullable_col.begin(),
                            new_nullable_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
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

            auto val = nullable_col[i];
            auto ref = ref_func(val, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!" << val;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref) << expr_str << "@" << i << "!!" << val;
            }
        }
    }
}
