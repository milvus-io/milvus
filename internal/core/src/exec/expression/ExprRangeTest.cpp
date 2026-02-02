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

#include "ExprTestBase.h"

EXPR_TEST_INSTANTIATE();

TEST_P(ExprTest, Range) {
    SUCCEED();
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->AddDebugField("age", DataType::INT32);
    SetSchema(schema);
    auto plan_str = create_search_plan_from_expr("age > 1 && age < 100");
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    Assert(plan->tag2field_.at("$0") ==
           schema->get_field_id(FieldName("fakevec")));
}

TEST_P(ExprTest, InvalidRange) {
    SUCCEED();
    // This test validates that CreateSearchPlanByExpr fails when referencing
    // a field that doesn't exist in the schema.
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->AddDebugField("age", DataType::INT32);
    SetSchema(schema);
    // "nonexistent_field" doesn't exist in schema, should throw
    ASSERT_ANY_THROW(
        create_search_plan_from_expr("nonexistent_field > 1 && age < 100"));
}

TEST_P(ExprTest, ShowExecutor) {
    auto node = std::make_unique<VectorPlanNode>();
    auto schema = std::make_shared<Schema>();
    auto field_id =
        schema->AddDebugField("fakevec", data_type, 16, metric_type);
    int64_t num_queries = 100L;
    auto raw_data = DataGen(schema, num_queries);
    auto& info = node->search_info_;

    info.metric_type_ = metric_type;
    info.topk_ = 20;
    info.field_id_ = field_id;
}

TEST_P(ExprTest, TestRange) {
    std::vector<std::tuple<std::string, std::function<bool(int)>>> testcases = {
        {"age > 2000 && age < 3000",
         [](int v) { return 2000 < v && v < 3000; }},
        {"age >= 2000 && age < 3000",
         [](int v) { return 2000 <= v && v < 3000; }},
        {"age > 2000 && age <= 3000",
         [](int v) { return 2000 < v && v <= 3000; }},
        {"age >= 2000 && age <= 3000",
         [](int v) { return 2000 <= v && v <= 3000; }},
        {"age >= 2000", [](int v) { return v >= 2000; }},
        {"age > 2000", [](int v) { return v > 2000; }},
        {"age <= 2000", [](int v) { return v <= 2000; }},
        {"age < 2000", [](int v) { return v < 2000; }},
        {"age == 2000", [](int v) { return v == 2000; }},
        {"age != 2000", [](int v) { return v != 2000; }},
    };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 1;
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
    for (auto [clause, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(clause);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < num_iters; ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());

        EXPECT_EQ(final.size(), N * num_iters);
        EXPECT_EQ(view.size(), num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;

            if (i < num_iters) {
                ASSERT_EQ(view[i], ref) << clause << "@" << i << "!!" << val;
            }
        }
    }
}

TEST_P(ExprTest, TestRangeNullable) {
    // Test cases for nullable field using string expressions
    std::vector<std::tuple<std::string, std::function<bool(int, bool)>>>
        testcases = {
            {"nullable > 2000 && nullable < 3000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 < v && v < 3000;
             }},
            {"nullable >= 2000 && nullable < 3000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 <= v && v < 3000;
             }},
            {"nullable > 2000 && nullable <= 3000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 < v && v <= 3000;
             }},
            {"nullable >= 2000 && nullable <= 3000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return 2000 <= v && v <= 3000;
             }},
            {"nullable >= 2000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v >= 2000;
             }},
            {"nullable > 2000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v > 2000;
             }},
            {"nullable <= 2000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v <= 2000;
             }},
            {"nullable < 2000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v < 2000;
             }},
            {"nullable == 2000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v == 2000;
             }},
            {"nullable != 2000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return v != 2000;
             }},
        };

    // Test cases for pre_check field (Int8) that test overflow behavior
    // These test values (1000000, -1000000) that overflow Int8 range
    std::vector<std::tuple<std::string, std::function<bool(int, bool)>>>
        precheck_testcases = {
            // 1000000 < pre_check < 1000001: always false (overflow)
            {"pre_check > 1000000 && pre_check < 1000001",
             [](int v, bool valid) { return false; }},
            // -1000001 < pre_check < -1000000: always false (overflow)
            {"pre_check > -1000001 && pre_check < -1000000",
             [](int v, bool valid) { return false; }},
            // pre_check >= 1000000: always false (no Int8 can be >= 1000000)
            {"pre_check >= 1000000", [](int v, bool valid) { return false; }},
            // pre_check >= -1000000: always true for valid (all Int8 >= -1000000)
            {"pre_check >= -1000000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return true;
             }},
            // pre_check <= 1000000: always true for valid (all Int8 <= 1000000)
            {"pre_check <= 1000000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return true;
             }},
            // pre_check < -1000000: always false (no Int8 can be < -1000000)
            {"pre_check < -1000000", [](int v, bool valid) { return false; }},
            // pre_check == 1000000: always false (Int8 cannot be 1000000)
            {"pre_check == 1000000", [](int v, bool valid) { return false; }},
            // pre_check != 1000000: always true for valid (all Int8 != 1000000)
            {"pre_check != 1000000",
             [](int v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return true;
             }},
        };
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);

    auto nullable_fid_pre_check =
        schema->AddDebugField("pre_check", DataType::INT8, true);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> data_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_data_col = raw_data.get_col<int>(i64_fid);
        valid_data_col = raw_data.get_col_valid(nullable_fid);
        data_col.insert(
            data_col.end(), new_data_col.begin(), new_data_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);

    // Test nullable field with string expressions
    for (auto [clause, ref_func] : testcases) {
        auto plan_str = create_search_plan_from_expr(clause);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        query::ExecPlanNodeVisitor inner_visitor(*seg_promote, MAX_TIMESTAMP);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
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

        EXPECT_EQ(final.size(), N * num_iters);
        EXPECT_EQ(view.size(), int(N * num_iters / 2));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = data_col[i];
            auto valid_data = valid_data_col[i];
            auto ref = ref_func(val, valid_data);
            ASSERT_EQ(ans, ref)
                << clause << "@" << i << "!!" << val << "!!" << valid_data;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!" << val << "!!" << valid_data;
            }
        }
    }

    // Test pre_check field (Int8) with overflow values
    for (auto [clause, ref_func] : precheck_testcases) {
        auto plan_str = create_search_plan_from_expr(clause);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        query::ExecPlanNodeVisitor inner_visitor(*seg_promote, MAX_TIMESTAMP);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
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

        EXPECT_EQ(final.size(), N * num_iters);
        EXPECT_EQ(view.size(), int(N * num_iters / 2));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = data_col[i];
            auto valid_data = valid_data_col[i];
            auto ref = ref_func(val, valid_data);
            ASSERT_EQ(ans, ref)
                << clause << "@" << i << "!!" << val << "!!" << valid_data;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << clause << "@" << i << "!!" << val << "!!" << valid_data;
            }
        }
    }
}
