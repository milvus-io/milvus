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

TEST_P(ExprTest, TestBinaryArithOpEvalRange) {
    std::vector<std::tuple<std::string, std::function<bool(int)>, DataType>> testcases = {
        // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
        {R"(age8 + 4 == 8)",
         [](int8_t v) { return (v + 4) == 8; },
         DataType::INT8},
        {R"(age16 - 500 == 1500)",
         [](int16_t v) { return (v - 500) == 1500; },
         DataType::INT16},
        {R"(age32 * 2 == 4000)",
         [](int32_t v) { return (v * 2) == 4000; },
         DataType::INT32},
        {R"(age64 / 2 == 1000)",
         [](int64_t v) { return (v / 2) == 1000; },
         DataType::INT64},
        {R"(age32 % 100 == 0)",
         [](int32_t v) { return (v % 100) == 0; },
         DataType::INT32},
        {R"(age_float + 500 == 2500)",
         [](float v) { return (v + 500) == 2500; },
         DataType::FLOAT},
        {R"(age_double + 500 == 2500)",
         [](double v) { return (v + 500) == 2500; },
         DataType::DOUBLE},
        // Add test cases for BinaryArithOpEvalRangeExpr NE of various data types
        {R"(age_float + 500 != 2500)",
         [](float v) { return (v + 500) != 2500; },
         DataType::FLOAT},
        {R"(age_double - 500 != 2500)",
         [](double v) { return (v - 500) != 2500; },
         DataType::DOUBLE},
        {R"(age8 * 2 != 2)",
         [](int8_t v) { return (v * 2) != 2; },
         DataType::INT8},
        {R"(age16 / 2 != 1000)",
         [](int16_t v) { return (v / 2) != 1000; },
         DataType::INT16},
        {R"(age32 % 100 != 0)",
         [](int32_t v) { return (v % 100) != 0; },
         DataType::INT32},
        {R"(age64 + 500 != 2500)",
         [](int64_t v) { return (v + 500) != 2500; },
         DataType::INT64},
        // Add test cases for BinaryArithOpEvalRangeExpr GT of various data types
        {R"(age_float + 500 > 2500)",
         [](float v) { return (v + 500) > 2500; },
         DataType::FLOAT},
        {R"(age_double - 500 > 2500)",
         [](double v) { return (v - 500) > 2500; },
         DataType::DOUBLE},
        {R"(age8 * 2 > 2)",
         [](int8_t v) { return (v * 2) > 2; },
         DataType::INT8},
        {R"(age16 / 2 > 1000)",
         [](int16_t v) { return (v / 2) > 1000; },
         DataType::INT16},
        {R"(age32 % 100 > 0)",
         [](int32_t v) { return (v % 100) > 0; },
         DataType::INT32},
        {R"(age64 + 500 > 2500)",
         [](int64_t v) { return (v + 500) > 2500; },
         DataType::INT64},
        // Add test cases for BinaryArithOpEvalRangeExpr GE of various data types
        {R"(age_float + 500 >= 2500)",
         [](float v) { return (v + 500) >= 2500; },
         DataType::FLOAT},
        {R"(age_double - 500 >= 2500)",
         [](double v) { return (v - 500) >= 2500; },
         DataType::DOUBLE},
        {R"(age8 * 2 >= 2)",
         [](int8_t v) { return (v * 2) >= 2; },
         DataType::INT8},
        {R"(age16 / 2 >= 1000)",
         [](int16_t v) { return (v / 2) >= 1000; },
         DataType::INT16},
        {R"(age32 % 100 >= 0)",
         [](int32_t v) { return (v % 100) >= 0; },
         DataType::INT32},
        {R"(age64 + 500 >= 2500)",
         [](int64_t v) { return (v + 500) >= 2500; },
         DataType::INT64},
        // Add test cases for BinaryArithOpEvalRangeExpr LT of various data types
        {R"(age_float + 500 < 2500)",
         [](float v) { return (v + 500) < 2500; },
         DataType::FLOAT},
        {R"(age_double - 500 < 2500)",
         [](double v) { return (v - 500) < 2500; },
         DataType::DOUBLE},
        {R"(age8 * 2 < 2)",
         [](int8_t v) { return (v * 2) < 2; },
         DataType::INT8},
        {R"(age16 / 2 < 1000)",
         [](int16_t v) { return (v / 2) < 1000; },
         DataType::INT16},
        {R"(age32 % 100 < 0)",
         [](int32_t v) { return (v % 100) < 0; },
         DataType::INT32},
        {R"(age64 + 500 < 2500)",
         [](int64_t v) { return (v + 500) < 2500; },
         DataType::INT64},
        // Add test cases for BinaryArithOpEvalRangeExpr LE of various data types
        {R"(age_float + 500 <= 2500)",
         [](float v) { return (v + 500) <= 2500; },
         DataType::FLOAT},
        {R"(age_double - 500 <= 2500)",
         [](double v) { return (v - 500) <= 2500; },
         DataType::DOUBLE},
        {R"(age8 * 2 <= 2)",
         [](int8_t v) { return (v * 2) <= 2; },
         DataType::INT8},
        {R"(age16 / 2 <= 1000)",
         [](int16_t v) { return (v / 2) <= 1000; },
         DataType::INT16},
        {R"(age32 % 100 <= 0)",
         [](int32_t v) { return (v % 100) <= 0; },
         DataType::INT32},
        {R"(age64 + 500 <= 2500)",
         [](int64_t v) { return (v + 500) <= 2500; },
         DataType::INT64},
    };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
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
    int num_iters = 1;
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
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [clause, ref_func, dtype] : testcases) {
        auto plan_str = create_search_plan_from_expr(clause);
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
            if (dtype == DataType::INT8) {
                auto val = age8_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref)
                    << clause << "@" << i << "!!" << val << std::endl;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val << std::endl;
                }
            } else if (dtype == DataType::INT16) {
                auto val = age16_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT32) {
                auto val = age32_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = age64_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::FLOAT) {
                auto val = age_float_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = age_double_col[i];
                auto ref = ref_func(val);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeNullable) {
    std::vector<
        std::tuple<std::string, std::function<bool(int, bool)>, DataType>>
        testcases = {
            // Add test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(age8 + 4 == 8)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 4) == 8;
             },
             DataType::INT8},
            {R"(age16 - 500 == 1500)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) == 1500;
             },
             DataType::INT16},
            {R"(age32 * 2 == 4000)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) == 4000;
             },
             DataType::INT32},
            {R"(age64_nullable / 2 == 1000)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) == 1000;
             },
             DataType::INT64},
            {R"(age32 % 100 == 0)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) == 0;
             },
             DataType::INT32},
            {R"(age_float + 500 == 2500)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) == 2500;
             },
             DataType::FLOAT},
            {R"(age_double + 500 == 2500)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) == 2500;
             },
             DataType::DOUBLE},
            // Add test cases for BinaryArithOpEvalRangeExpr NE of various data types
            {R"(age_float + 500 != 2500)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) != 2500;
             },
             DataType::FLOAT},
            {R"(age_double - 500 != 2500)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) != 2500;
             },
             DataType::DOUBLE},
            {R"(age8 * 2 != 2)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) != 2;
             },
             DataType::INT8},
            {R"(age16 / 2 != 1000)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) != 1000;
             },
             DataType::INT16},
            {R"(age32 % 100 != 0)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) != 0;
             },
             DataType::INT32},
            {R"(age64_nullable + 500 != 2500)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) != 2500;
             },
             DataType::INT64},
            // Add test cases for BinaryArithOpEvalRangeExpr GT of various data types
            {R"(age_float + 500 > 2500)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) > 2500;
             },
             DataType::FLOAT},
            {R"(age_double - 500 > 2500)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) > 2500;
             },
             DataType::DOUBLE},
            {R"(age8 * 2 > 2)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) > 2;
             },
             DataType::INT8},
            {R"(age16 / 2 > 1000)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) > 1000;
             },
             DataType::INT16},
            {R"(age32 % 100 > 0)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) > 0;
             },
             DataType::INT32},
            {R"(age64_nullable + 500 > 2500)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) > 2500;
             },
             DataType::INT64},
            // Add test cases for BinaryArithOpEvalRangeExpr GE of various data types
            {R"(age_float + 500 >= 2500)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) >= 2500;
             },
             DataType::FLOAT},
            {R"(age_double - 500 >= 2500)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) >= 2500;
             },
             DataType::DOUBLE},
            {R"(age8 * 2 >= 2)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) >= 2;
             },
             DataType::INT8},
            {R"(age16 / 2 >= 1000)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) >= 1000;
             },
             DataType::INT16},
            {R"(age32 % 100 >= 0)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) >= 0;
             },
             DataType::INT32},
            {R"(age64_nullable + 500 >= 2500)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) >= 2500;
             },
             DataType::INT64},
            // Add test cases for BinaryArithOpEvalRangeExpr LT of various data types
            {R"(age_float + 500 < 2500)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) < 2500;
             },
             DataType::FLOAT},
            {R"(age_double - 500 < 2500)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) < 2500;
             },
             DataType::DOUBLE},
            {R"(age8 * 2 < 2)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) < 2;
             },
             DataType::INT8},
            {R"(age16 / 2 < 1000)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) < 1000;
             },
             DataType::INT16},
            {R"(age32 % 100 < 0)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) < 0;
             },
             DataType::INT32},
            {R"(age64_nullable + 500 < 2500)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) < 2500;
             },
             DataType::INT64},
            // Add test cases for BinaryArithOpEvalRangeExpr LE of various data types
            {R"(age_float + 500 <= 2500)",
             [](float v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) <= 2500;
             },
             DataType::FLOAT},
            {R"(age_double - 500 <= 2500)",
             [](double v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v - 500) <= 2500;
             },
             DataType::DOUBLE},
            {R"(age8 * 2 <= 2)",
             [](int8_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v * 2) <= 2;
             },
             DataType::INT8},
            {R"(age16 / 2 <= 1000)",
             [](int16_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v / 2) <= 1000;
             },
             DataType::INT16},
            {R"(age32 % 100 <= 0)",
             [](int32_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v % 100) <= 0;
             },
             DataType::INT32},
            {R"(age64_nullable + 500 <= 2500)",
             [](int64_t v, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return (v + 500) <= 2500;
             },
             DataType::INT64},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i8_nullable_fid = schema->AddDebugField("age8", DataType::INT8, true);
    auto i16_nullable_fid =
        schema->AddDebugField("age16", DataType::INT16, true);
    auto i32_nullable_fid =
        schema->AddDebugField("age32", DataType::INT32, true);
    auto i64_nullable_fid =
        schema->AddDebugField("age64_nullable", DataType::INT64, true);
    auto float_nullable_fid =
        schema->AddDebugField("age_float", DataType::FLOAT, true);
    auto double_nullable_fid =
        schema->AddDebugField("age_double", DataType::DOUBLE, true);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int8_t> age8_col;
    std::vector<int16_t> age16_col;
    std::vector<int32_t> age32_col;
    std::vector<int64_t> age64_col;
    std::vector<float> age_float_col;
    std::vector<double> age_double_col;
    FixedVector<bool> age8_valid_col;
    FixedVector<bool> age16_valid_col;
    FixedVector<bool> age32_valid_col;
    FixedVector<bool> age64_valid_col;
    FixedVector<bool> age_float_valid_col;
    FixedVector<bool> age_double_valid_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        auto new_age8_col = raw_data.get_col<int8_t>(i8_nullable_fid);
        auto new_age16_col = raw_data.get_col<int16_t>(i16_nullable_fid);
        auto new_age32_col = raw_data.get_col<int32_t>(i32_nullable_fid);
        auto new_age64_col = raw_data.get_col<int64_t>(i64_nullable_fid);
        auto new_age_float_col = raw_data.get_col<float>(float_nullable_fid);
        auto new_age_double_col = raw_data.get_col<double>(double_nullable_fid);
        age8_valid_col = raw_data.get_col_valid(i8_nullable_fid);
        age16_valid_col = raw_data.get_col_valid(i16_nullable_fid);
        age32_valid_col = raw_data.get_col_valid(i32_nullable_fid);
        age64_valid_col = raw_data.get_col_valid(i64_nullable_fid);
        age_float_valid_col = raw_data.get_col_valid(float_nullable_fid);
        age_double_valid_col = raw_data.get_col_valid(double_nullable_fid);

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
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [clause, ref_func, dtype] : testcases) {
        auto plan_str = create_search_plan_from_expr(clause);
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
            if (dtype == DataType::INT8) {
                auto val = age8_col[i];
                auto ref = ref_func(val, age8_valid_col[i]);
                ASSERT_EQ(ans, ref)
                    << clause << "@" << i << "!!" << val << std::endl;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val << std::endl;
                }
            } else if (dtype == DataType::INT16) {
                auto val = age16_col[i];
                auto ref = ref_func(val, age16_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT32) {
                auto val = age32_col[i];
                auto ref = ref_func(val, age32_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::INT64) {
                auto val = age64_col[i];
                auto ref = ref_func(val, age64_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::FLOAT) {
                auto val = age_float_col[i];
                auto ref = ref_func(val, age_float_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else if (dtype == DataType::DOUBLE) {
                auto val = age_double_col[i];
                auto ref = ref_func(val, age_double_valid_col[i]);
                ASSERT_EQ(ans, ref) << clause << "@" << i << "!!" << val;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << clause << "@" << i << "!!" << val;
                }
            } else {
                ASSERT_TRUE(false) << "No test case defined for this data type";
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeJSON) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // Test cases: {expression, expected result function}
    std::vector<
        std::tuple<std::string, std::function<bool(const milvus::Json& json)>>>
        testcases = {
            // Test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(json["int"] + 1 == 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) == 2;
             }},
            {R"(json["int"] - 1 == 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) == 2;
             }},
            {R"(json["int"] * 2 == 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) == 4;
             }},
            {R"(json["int"] / 2 == 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) == 4;
             }},
            {R"(json["int"] % 2 == 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) == 4;
             }},
            {R"(array_length(json["array"]) == 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length == 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr NQ of various data types
            {R"(json["int"] + 1 != 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) != 2;
             }},
            {R"(json["int"] - 1 != 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) != 2;
             }},
            {R"(json["int"] * 2 != 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) != 4;
             }},
            {R"(json["int"] / 2 != 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) != 4;
             }},
            {R"(json["int"] % 2 != 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) != 4;
             }},
            {R"(array_length(json["array"]) != 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length != 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr GT of various data types
            {R"(json["int"] + 1 > 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) > 2;
             }},
            {R"(json["int"] - 1 > 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) > 2;
             }},
            {R"(json["int"] * 2 > 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) > 4;
             }},
            {R"(json["int"] / 2 > 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) > 4;
             }},
            {R"(json["int"] % 2 > 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) > 4;
             }},
            {R"(array_length(json["array"]) > 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length > 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr GE of various data types
            {R"(json["int"] + 1 >= 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) >= 2;
             }},
            {R"(json["int"] - 1 >= 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) >= 2;
             }},
            {R"(json["int"] * 2 >= 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) >= 4;
             }},
            {R"(json["int"] / 2 >= 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) >= 4;
             }},
            {R"(json["int"] % 2 >= 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) >= 4;
             }},
            {R"(array_length(json["array"]) >= 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length >= 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr LT of various data types
            {R"(json["int"] + 1 < 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) < 2;
             }},
            {R"(json["int"] - 1 < 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) < 2;
             }},
            {R"(json["int"] * 2 < 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) < 4;
             }},
            {R"(json["int"] / 2 < 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) < 4;
             }},
            {R"(json["int"] % 2 < 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) < 4;
             }},
            {R"(array_length(json["array"]) < 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length < 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr LE of various data types
            {R"(json["int"] + 1 <= 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) <= 2;
             }},
            {R"(json["int"] - 1 <= 2)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) <= 2;
             }},
            {R"(json["int"] * 2 <= 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) <= 4;
             }},
            {R"(json["int"] / 2 <= 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) <= 4;
             }},
            {R"(json["int"] % 2 <= 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) <= 4;
             }},
            {R"(array_length(json["array"]) <= 4)",
             [](const milvus::Json& json) {
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length <= 4;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);
    SetSchema(schema);

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

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    ScopedSchemaHandle schema_handle(*schema);
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
            auto ref =
                ref_func(milvus::Json(simdjson::padded_string(json_col[i])));
            ASSERT_EQ(ans, ref) << expr << "@" << i << "!!" << json_col[i];
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr << "@" << i << "!!" << json_col[i];
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeJSONNullable) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // Test cases: {expression, expected result function with validity check}
    std::vector<
        std::tuple<std::string,
                   std::function<bool(const milvus::Json& json, bool valid)>>>
        testcases = {
            // Test cases for BinaryArithOpEvalRangeExpr EQ of various data types
            {R"(json["int"] + 1 == 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) == 2;
             }},
            {R"(json["int"] - 1 == 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) == 2;
             }},
            {R"(json["int"] * 2 == 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) == 4;
             }},
            {R"(json["int"] / 2 == 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) == 4;
             }},
            {R"(json["int"] % 2 == 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) == 4;
             }},
            {R"(array_length(json["array"]) == 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length == 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr NQ of various data types
            {R"(json["int"] + 1 != 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) != 2;
             }},
            {R"(json["int"] - 1 != 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) != 2;
             }},
            {R"(json["int"] * 2 != 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) != 4;
             }},
            {R"(json["int"] / 2 != 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) != 4;
             }},
            {R"(json["int"] % 2 != 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) != 4;
             }},
            {R"(array_length(json["array"]) != 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length != 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr GT of various data types
            {R"(json["int"] + 1 > 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) > 2;
             }},
            {R"(json["int"] - 1 > 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) > 2;
             }},
            {R"(json["int"] * 2 > 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) > 4;
             }},
            {R"(json["int"] / 2 > 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) > 4;
             }},
            {R"(json["int"] % 2 > 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) > 4;
             }},
            {R"(array_length(json["array"]) > 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length > 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr GE of various data types
            {R"(json["int"] + 1 >= 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) >= 2;
             }},
            {R"(json["int"] - 1 >= 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) >= 2;
             }},
            {R"(json["int"] * 2 >= 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) >= 4;
             }},
            {R"(json["int"] / 2 >= 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) >= 4;
             }},
            {R"(json["int"] % 2 >= 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) >= 4;
             }},
            {R"(array_length(json["array"]) >= 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length >= 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr LT of various data types
            {R"(json["int"] + 1 < 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) < 2;
             }},
            {R"(json["int"] - 1 < 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) < 2;
             }},
            {R"(json["int"] * 2 < 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) < 4;
             }},
            {R"(json["int"] / 2 < 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) < 4;
             }},
            {R"(json["int"] % 2 < 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) < 4;
             }},
            {R"(array_length(json["array"]) < 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length < 4;
             }},
            // Test cases for BinaryArithOpEvalRangeExpr LE of various data types
            {R"(json["int"] + 1 <= 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val + 1) <= 2;
             }},
            {R"(json["int"] - 1 <= 2)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val - 1) <= 2;
             }},
            {R"(json["int"] * 2 <= 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val * 2) <= 4;
             }},
            {R"(json["int"] / 2 <= 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val / 2) <= 4;
             }},
            {R"(json["int"] % 2 <= 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"int"});
                 auto val = json.template at<int64_t>(pointer).value();
                 return (val % 2) <= 4;
             }},
            {R"(array_length(json["array"]) <= 4)",
             [](const milvus::Json& json, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 auto pointer = milvus::Json::pointer({"array"});
                 int array_length = 0;
                 auto doc = json.doc();
                 auto array = doc.at_pointer(pointer).get_array();
                 if (!array.error()) {
                     array_length = array.count_elements();
                 }
                 return array_length <= 4;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto nullable_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);
    SetSchema(schema);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(nullable_fid);
        valid_data = raw_data.get_col_valid(nullable_fid);

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
            auto ref =
                ref_func(milvus::Json(simdjson::padded_string(json_col[i])),
                         valid_data[i]);
            ASSERT_EQ(ans, ref) << expr << "@" << i << "!!" << json_col[i];
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr << "@" << i << "!!" << json_col[i];
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeJSONFloat) {
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

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](double value) {
            if (testcase.op == OpType::Equal) {
                return value + testcase.right_operand == testcase.value;
            }
            return value + testcase.right_operand != testcase.value;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        proto::plan::GenericValue value;
        value.set_float_val(testcase.value);
        proto::plan::GenericValue right_operand;
        right_operand.set_float_val(testcase.right_operand);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            testcase.op,
            ArithOpType::Add,
            value,
            right_operand);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<double>(pointer)
                           .value();
            auto ref = check(val);
            ASSERT_EQ(ans, ref)
                << testcase.value << " " << val << " " << testcase.op;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref)
                    << testcase.value << " " << val << " " << testcase.op;
            }
        }
    }

    std::vector<Testcase> array_testcases{
        {0, 3, OpType::Equal, {"array"}},
        {0, 5, OpType::NotEqual, {"array"}},
    };

    for (auto testcase : array_testcases) {
        auto check = [&](int64_t value) {
            if (testcase.op == OpType::Equal) {
                return value == testcase.value;
            }
            return value != testcase.value;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        proto::plan::GenericValue value;
        value.set_int64_val(testcase.value);
        proto::plan::GenericValue right_operand;
        right_operand.set_int64_val(testcase.right_operand);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            testcase.op,
            ArithOpType::ArrayLength,
            value,
            right_operand);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto json = milvus::Json(simdjson::padded_string(json_col[i]));
            int64_t array_length = 0;
            auto doc = json.doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (!array.error()) {
                array_length = array.count_elements();
            }
            auto ref = check(array_length);
            ASSERT_EQ(ans, ref)
                << testcase.value << " " << array_length << " " << testcase.op;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref) << testcase.value << " " << array_length
                                        << " " << testcase.op;
            }
        }
    }
}
TEST_P(ExprTest, TestBinaryArithOpEvalRangeJSONFloatNullable) {
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

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    for (auto testcase : testcases) {
        auto check = [&](double value, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.op == OpType::Equal) {
                return value + testcase.right_operand == testcase.value;
            }
            return value + testcase.right_operand != testcase.value;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        proto::plan::GenericValue value;
        value.set_float_val(testcase.value);
        proto::plan::GenericValue right_operand;
        right_operand.set_float_val(testcase.right_operand);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            testcase.op,
            ArithOpType::Add,
            value,
            right_operand);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .template at<double>(pointer)
                           .value();
            auto ref = check(val, valid_data[i]);
            ASSERT_EQ(ans, ref)
                << testcase.value << " " << val << " " << testcase.op;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref)
                    << testcase.value << " " << val << " " << testcase.op;
            }
        }
    }

    std::vector<Testcase> array_testcases{
        {0, 3, OpType::Equal, {"array"}},
        {0, 5, OpType::NotEqual, {"array"}},
    };

    for (auto testcase : array_testcases) {
        auto check = [&](int64_t value, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.op == OpType::Equal) {
                return value == testcase.value;
            }
            return value != testcase.value;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        proto::plan::GenericValue value;
        value.set_int64_val(testcase.value);
        proto::plan::GenericValue right_operand;
        right_operand.set_int64_val(testcase.right_operand);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            testcase.op,
            ArithOpType::ArrayLength,
            value,
            right_operand);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto json = milvus::Json(simdjson::padded_string(json_col[i]));
            int64_t array_length = 0;
            auto doc = json.doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (!array.error()) {
                array_length = array.count_elements();
            }
            auto ref = check(array_length, valid_data[i]);
            ASSERT_EQ(ans, ref) << testcase.value << " " << array_length;
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref)
                    << testcase.value << " " << array_length;
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeWithScalarSortIndex) {
    // Test cases: {expression, validation_func, data_type}
    // Fields: age8(INT8), age16(INT16), age32(INT32), age64(INT64), age_float(FLOAT), age_double(DOUBLE)
    std::vector<std::tuple<std::string, std::function<bool(int)>, DataType>>
        testcases = {
            // EQ tests
            {"age8 + 4 == 8",
             [](int8_t v) { return (v + 4) == 8; },
             DataType::INT8},
            {"age16 - 500 == 1500",
             [](int16_t v) { return (v - 500) == 1500; },
             DataType::INT16},
            {"age32 * 2 == 4000",
             [](int32_t v) { return (v * 2) == 4000; },
             DataType::INT32},
            {"age64 / 2 == 1000",
             [](int64_t v) { return (v / 2) == 1000; },
             DataType::INT64},
            {"age32 % 100 == 0",
             [](int32_t v) { return (v % 100) == 0; },
             DataType::INT32},
            {"age_float + 500 == 2500",
             [](float v) { return (v + 500) == 2500; },
             DataType::FLOAT},
            {"age_double + 500 == 2500",
             [](double v) { return (v + 500) == 2500; },
             DataType::DOUBLE},
            // NE tests
            {"age_float + 500 != 2000",
             [](float v) { return (v + 500) != 2000; },
             DataType::FLOAT},
            {"age_double - 500 != 2500",
             [](double v) { return (v - 500) != 2000; },
             DataType::DOUBLE},
            {"age8 * 2 != 2",
             [](int8_t v) { return (v * 2) != 2; },
             DataType::INT8},
            {"age16 / 2 != 2000",
             [](int16_t v) { return (v / 2) != 2000; },
             DataType::INT16},
            {"age32 % 100 != 1",
             [](int32_t v) { return (v % 100) != 1; },
             DataType::INT32},
            {"age64 + 500 != 2000",
             [](int64_t v) { return (v + 500) != 2000; },
             DataType::INT64},
            // GT tests
            {"age8 + 4 > 8",
             [](int8_t v) { return (v + 4) > 8; },
             DataType::INT8},
            {"age16 - 500 > 1500",
             [](int16_t v) { return (v - 500) > 1500; },
             DataType::INT16},
            {"age32 * 2 > 4000",
             [](int32_t v) { return (v * 2) > 4000; },
             DataType::INT32},
            {"age64 / 2 > 1000",
             [](int64_t v) { return (v / 2) > 1000; },
             DataType::INT64},
            {"age32 % 100 > 0",
             [](int32_t v) { return (v % 100) > 0; },
             DataType::INT32},
            // GE tests
            {"age8 + 4 >= 8",
             [](int8_t v) { return (v + 4) >= 8; },
             DataType::INT8},
            {"age16 - 500 >= 1500",
             [](int16_t v) { return (v - 500) >= 1500; },
             DataType::INT16},
            {"age32 * 2 >= 4000",
             [](int32_t v) { return (v * 2) >= 4000; },
             DataType::INT32},
            {"age64 / 2 >= 1000",
             [](int64_t v) { return (v / 2) >= 1000; },
             DataType::INT64},
            {"age32 % 100 >= 0",
             [](int32_t v) { return (v % 100) >= 0; },
             DataType::INT32},
            // LT tests
            {"age8 + 4 < 8",
             [](int8_t v) { return (v + 4) < 8; },
             DataType::INT8},
            {"age16 - 500 < 1500",
             [](int16_t v) { return (v - 500) < 1500; },
             DataType::INT16},
            {"age32 * 2 < 4000",
             [](int32_t v) { return (v * 2) < 4000; },
             DataType::INT32},
            {"age64 / 2 < 1000",
             [](int64_t v) { return (v / 2) < 1000; },
             DataType::INT64},
            {"age32 % 100 < 0",
             [](int32_t v) { return (v % 100) < 0; },
             DataType::INT32},
            // LE tests
            {"age8 + 4 <= 8",
             [](int8_t v) { return (v + 4) <= 8; },
             DataType::INT8},
            {"age16 - 500 <= 1500",
             [](int16_t v) { return (v - 500) <= 1500; },
             DataType::INT16},
            {"age32 * 2 <= 4000",
             [](int32_t v) { return (v * 2) <= 4000; },
             DataType::INT32},
            {"age64 / 2 <= 1000",
             [](int64_t v) { return (v / 2) <= 1000; },
             DataType::INT64},
            {"age32 % 100 <= 0",
             [](int32_t v) { return (v % 100) <= 0; },
             DataType::INT32},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
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
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);
    segcore::LoadIndexInfo load_index_info;

    // load index for int8 field
    auto age8_col = raw_data.get_col<int8_t>(i8_fid);
    age8_col[0] = 4;
    auto age8_index = milvus::index::CreateScalarIndexSort<int8_t>();
    age8_index->Build(N, age8_col.data(), nullptr);
    load_index_info.field_id = i8_fid.get();
    load_index_info.field_type = DataType::INT8;
    load_index_info.index_params = GenIndexParams(age8_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age8_index));
    seg->LoadIndex(load_index_info);

    // load index for int16 field
    auto age16_col = raw_data.get_col<int16_t>(i16_fid);
    age16_col[0] = 2000;
    auto age16_index = milvus::index::CreateScalarIndexSort<int16_t>();
    age16_index->Build(N, age16_col.data(), nullptr);
    load_index_info.field_id = i16_fid.get();
    load_index_info.field_type = DataType::INT16;
    load_index_info.index_params = GenIndexParams(age16_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age16_index));
    seg->LoadIndex(load_index_info);

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_fid);
    age32_col[0] = 2000;
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data(), nullptr);
    load_index_info.field_id = i32_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(age32_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age32_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data(), nullptr);
    load_index_info.field_id = i64_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    // load index for float field
    auto age_float_col = raw_data.get_col<float>(float_fid);
    age_float_col[0] = 2000;
    auto age_float_index = milvus::index::CreateScalarIndexSort<float>();
    age_float_index->Build(N, age_float_col.data(), nullptr);
    load_index_info.field_id = float_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index_params = GenIndexParams(age_float_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_float_index));
    seg->LoadIndex(load_index_info);

    // load index for double field
    auto age_double_col = raw_data.get_col<double>(double_fid);
    age_double_col[0] = 2000;
    auto age_double_index = milvus::index::CreateScalarIndexSort<double>();
    age_double_index->Build(N, age_double_col.data(), nullptr);
    load_index_info.field_id = double_fid.get();
    load_index_info.field_type = DataType::DOUBLE;
    load_index_info.index_params = GenIndexParams(age_double_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_double_index));
    seg->LoadIndex(load_index_info);

    auto seg_promote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto& [clause, ref_func, dtype] : testcases) {
        auto plan_str = create_search_plan_from_expr(clause);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            bool ref = false;
            if (dtype == DataType::INT8) {
                ref = ref_func(age8_col[i]);
            } else if (dtype == DataType::INT16) {
                ref = ref_func(age16_col[i]);
            } else if (dtype == DataType::INT32) {
                ref = ref_func(age32_col[i]);
            } else if (dtype == DataType::INT64) {
                ref = ref_func(age64_col[i]);
            } else if (dtype == DataType::FLOAT) {
                ref = ref_func(age_float_col[i]);
            } else if (dtype == DataType::DOUBLE) {
                ref = ref_func(age_double_col[i]);
            }
            ASSERT_EQ(ans, ref) << clause << "@" << i;
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeWithScalarSortIndexNullable) {
    // Test cases: {expression, validation_func, data_type}
    // Nullable fields: age8, age16, age32, age641, age_float, age_double
    std::vector<
        std::tuple<std::string, std::function<bool(int, bool)>, DataType>>
        testcases = {
            // EQ tests
            {"age8 + 4 == 8",
             [](int8_t v, bool valid) { return valid && (v + 4) == 8; },
             DataType::INT8},
            {"age16 - 500 == 1500",
             [](int16_t v, bool valid) { return valid && (v - 500) == 1500; },
             DataType::INT16},
            {"age32 * 2 == 4000",
             [](int32_t v, bool valid) { return valid && (v * 2) == 4000; },
             DataType::INT32},
            {"age641 / 2 == 1000",
             [](int64_t v, bool valid) { return valid && (v / 2) == 1000; },
             DataType::INT64},
            {"age32 % 100 == 0",
             [](int32_t v, bool valid) { return valid && (v % 100) == 0; },
             DataType::INT32},
            {"age_float + 500 == 2500",
             [](float v, bool valid) { return valid && (v + 500) == 2500; },
             DataType::FLOAT},
            {"age_double + 500 == 2500",
             [](double v, bool valid) { return valid && (v + 500) == 2500; },
             DataType::DOUBLE},
            // NE tests
            {"age_float + 500 != 2000",
             [](float v, bool valid) { return valid && (v + 500) != 2000; },
             DataType::FLOAT},
            {"age_double - 500 != 2500",
             [](double v, bool valid) { return valid && (v - 500) != 2000; },
             DataType::DOUBLE},
            {"age8 * 2 != 2",
             [](int8_t v, bool valid) { return valid && (v * 2) != 2; },
             DataType::INT8},
            {"age16 / 2 != 2000",
             [](int16_t v, bool valid) { return valid && (v / 2) != 2000; },
             DataType::INT16},
            {"age32 % 100 != 1",
             [](int32_t v, bool valid) { return valid && (v % 100) != 1; },
             DataType::INT32},
            {"age641 + 500 != 2000",
             [](int64_t v, bool valid) { return valid && (v + 500) != 2000; },
             DataType::INT64},
            // GT tests
            {"age8 + 4 > 8",
             [](int8_t v, bool valid) { return valid && (v + 4) > 8; },
             DataType::INT8},
            {"age16 - 500 > 1500",
             [](int16_t v, bool valid) { return valid && (v - 500) > 1500; },
             DataType::INT16},
            {"age32 * 2 > 4000",
             [](int32_t v, bool valid) { return valid && (v * 2) > 4000; },
             DataType::INT32},
            {"age641 / 2 > 1000",
             [](int64_t v, bool valid) { return valid && (v / 2) > 1000; },
             DataType::INT64},
            {"age32 % 100 > 0",
             [](int32_t v, bool valid) { return valid && (v % 100) > 0; },
             DataType::INT32},
            // GE tests
            {"age8 + 4 >= 8",
             [](int8_t v, bool valid) { return valid && (v + 4) >= 8; },
             DataType::INT8},
            {"age16 - 500 >= 1500",
             [](int16_t v, bool valid) { return valid && (v - 500) >= 1500; },
             DataType::INT16},
            {"age32 * 2 >= 4000",
             [](int32_t v, bool valid) { return valid && (v * 2) >= 4000; },
             DataType::INT32},
            {"age641 / 2 >= 1000",
             [](int64_t v, bool valid) { return valid && (v / 2) >= 1000; },
             DataType::INT64},
            {"age32 % 100 >= 0",
             [](int32_t v, bool valid) { return valid && (v % 100) >= 0; },
             DataType::INT32},
            // LT tests
            {"age8 + 4 < 8",
             [](int8_t v, bool valid) { return valid && (v + 4) < 8; },
             DataType::INT8},
            {"age16 - 500 < 1500",
             [](int16_t v, bool valid) { return valid && (v - 500) < 1500; },
             DataType::INT16},
            {"age32 * 2 < 4000",
             [](int32_t v, bool valid) { return valid && (v * 2) < 4000; },
             DataType::INT32},
            {"age641 / 2 < 1000",
             [](int64_t v, bool valid) { return valid && (v / 2) < 1000; },
             DataType::INT64},
            {"age32 % 100 < 0",
             [](int32_t v, bool valid) { return valid && (v % 100) < 0; },
             DataType::INT32},
            // LE tests
            {"age8 + 4 <= 8",
             [](int8_t v, bool valid) { return valid && (v + 4) <= 8; },
             DataType::INT8},
            {"age16 - 500 <= 1500",
             [](int16_t v, bool valid) { return valid && (v - 500) <= 1500; },
             DataType::INT16},
            {"age32 * 2 <= 4000",
             [](int32_t v, bool valid) { return valid && (v * 2) <= 4000; },
             DataType::INT32},
            {"age641 / 2 <= 1000",
             [](int64_t v, bool valid) { return valid && (v / 2) <= 1000; },
             DataType::INT64},
            {"age32 % 100 <= 0",
             [](int32_t v, bool valid) { return valid && (v % 100) <= 0; },
             DataType::INT32},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i8_nullable_fid = schema->AddDebugField("age8", DataType::INT8, true);
    auto i16_nullable_fid =
        schema->AddDebugField("age16", DataType::INT16, true);
    auto i32_nullable_fid =
        schema->AddDebugField("age32", DataType::INT32, true);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto i64_nullable_fid =
        schema->AddDebugField("age641", DataType::INT64, true);
    auto float_nullable_fid =
        schema->AddDebugField("age_float", DataType::FLOAT, true);
    auto double_nullable_fid =
        schema->AddDebugField("age_double", DataType::DOUBLE, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);
    segcore::LoadIndexInfo load_index_info;

    auto i8_valid_data = raw_data.get_col_valid(i8_nullable_fid);
    auto i16_valid_data = raw_data.get_col_valid(i16_nullable_fid);
    auto i32_valid_data = raw_data.get_col_valid(i32_nullable_fid);
    auto i64_valid_data = raw_data.get_col_valid(i64_nullable_fid);
    auto float_valid_data = raw_data.get_col_valid(float_nullable_fid);
    auto double_valid_data = raw_data.get_col_valid(double_nullable_fid);

    // load index for int8 field
    auto age8_col = raw_data.get_col<int8_t>(i8_nullable_fid);
    age8_col[0] = 4;
    auto age8_index = milvus::index::CreateScalarIndexSort<int8_t>();
    age8_index->Build(N, age8_col.data(), i8_valid_data.data());
    load_index_info.field_id = i8_nullable_fid.get();
    load_index_info.field_type = DataType::INT8;
    load_index_info.index_params = GenIndexParams(age8_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age8_index));
    seg->LoadIndex(load_index_info);

    // load index for int16 field
    auto age16_col = raw_data.get_col<int16_t>(i16_nullable_fid);
    age16_col[0] = 2000;
    auto age16_index = milvus::index::CreateScalarIndexSort<int16_t>();
    age16_index->Build(N, age16_col.data(), i16_valid_data.data());
    load_index_info.field_id = i16_nullable_fid.get();
    load_index_info.field_type = DataType::INT16;
    load_index_info.index_params = GenIndexParams(age16_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age16_index));
    seg->LoadIndex(load_index_info);

    // load index for int32 field
    auto age32_col = raw_data.get_col<int32_t>(i32_nullable_fid);
    age32_col[0] = 2000;
    auto age32_index = milvus::index::CreateScalarIndexSort<int32_t>();
    age32_index->Build(N, age32_col.data(), i32_valid_data.data());
    load_index_info.field_id = i32_nullable_fid.get();
    load_index_info.field_type = DataType::INT32;
    load_index_info.index_params = GenIndexParams(age32_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age32_index));
    seg->LoadIndex(load_index_info);

    // load index for int64 field
    auto age64_col = raw_data.get_col<int64_t>(i64_nullable_fid);
    age64_col[0] = 2000;
    auto age64_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age64_index->Build(N, age64_col.data(), i64_valid_data.data());
    load_index_info.field_id = i64_nullable_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age64_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age64_index));
    seg->LoadIndex(load_index_info);

    // load index for float field
    auto age_float_col = raw_data.get_col<float>(float_nullable_fid);
    age_float_col[0] = 2000;
    auto age_float_index = milvus::index::CreateScalarIndexSort<float>();
    age_float_index->Build(N, age_float_col.data(), float_valid_data.data());
    load_index_info.field_id = float_nullable_fid.get();
    load_index_info.field_type = DataType::FLOAT;
    load_index_info.index_params = GenIndexParams(age_float_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_float_index));
    seg->LoadIndex(load_index_info);

    // load index for double field
    auto age_double_col = raw_data.get_col<double>(double_nullable_fid);
    age_double_col[0] = 2000;
    auto age_double_index = milvus::index::CreateScalarIndexSort<double>();
    age_double_index->Build(N, age_double_col.data(), double_valid_data.data());
    load_index_info.field_id = double_nullable_fid.get();
    load_index_info.field_type = DataType::DOUBLE;
    load_index_info.index_params = GenIndexParams(age_double_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(age_double_index));
    seg->LoadIndex(load_index_info);

    auto seg_promote = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto& [clause, ref_func, dtype] : testcases) {
        auto plan_str = create_search_plan_from_expr(clause);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N, 10));

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            bool ref = false;
            if (dtype == DataType::INT8) {
                ref = ref_func(age8_col[i], i8_valid_data[i]);
            } else if (dtype == DataType::INT16) {
                ref = ref_func(age16_col[i], i16_valid_data[i]);
            } else if (dtype == DataType::INT32) {
                ref = ref_func(age32_col[i], i32_valid_data[i]);
            } else if (dtype == DataType::INT64) {
                ref = ref_func(age64_col[i], i64_valid_data[i]);
            } else if (dtype == DataType::FLOAT) {
                ref = ref_func(age_float_col[i], float_valid_data[i]);
            } else if (dtype == DataType::DOUBLE) {
                ref = ref_func(age_double_col[i], double_valid_data[i]);
            }
            ASSERT_EQ(ans, ref) << clause << "@" << i;
            if (i < std::min(N, 10)) {
                ASSERT_EQ(view[i], ref) << clause << "@" << i;
            }
        }
    }
}
