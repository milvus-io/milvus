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

TEST_P(ExprTest, TestBinaryRangeJSON) {
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
        milvus::proto::plan::GenericValue lower_val;
        lower_val.set_int64_val(testcase.lower);
        milvus::proto::plan::GenericValue upper_val;
        upper_val.set_int64_val(testcase.upper);
        auto expr = std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            lower_val,
            upper_val,
            testcase.lower_inclusive,
            testcase.upper_inclusive);
        auto plannode =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final = ExecuteQueryExpr(
            plannode, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(plannode.get(),
                                                    seg_promote,
                                                    N * num_iters,
                                                    MAX_TIMESTAMP,
                                                    &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

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
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << val << testcase.lower_inclusive << testcase.lower
                        << testcase.upper_inclusive << testcase.upper;
                }
            } else {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(pointer)
                               .value();
                auto ref = check(val);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << val << testcase.lower_inclusive << testcase.lower
                        << testcase.upper_inclusive << testcase.upper;
                }
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryRangeJSONNullable) {
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
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        valid_data_col = raw_data.get_col_valid(json_fid);
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (auto testcase : testcases) {
        auto check = [&](int64_t value, bool valid) {
            if (!valid) {
                return false;
            }
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
        milvus::proto::plan::GenericValue lower_val;
        lower_val.set_int64_val(testcase.lower);
        milvus::proto::plan::GenericValue upper_val;
        upper_val.set_int64_val(testcase.upper);
        auto expr = std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            lower_val,
            upper_val,
            testcase.lower_inclusive,
            testcase.upper_inclusive);
        BitsetType final;
        auto plannode =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final = ExecuteQueryExpr(
            plannode, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(plannode.get(),
                                                    seg_promote,
                                                    N * num_iters,
                                                    MAX_TIMESTAMP,
                                                    &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            if (testcase.nested_path[0] == "int") {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<int64_t>(pointer)
                               .value();
                auto ref = check(val, valid_data_col[i]);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << val << testcase.lower_inclusive << testcase.lower
                        << testcase.upper_inclusive << testcase.upper;
                }
            } else {
                auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                               .template at<double>(pointer)
                               .value();
                auto ref = check(val, valid_data_col[i]);
                ASSERT_EQ(ans, ref)
                    << val << testcase.lower_inclusive << testcase.lower
                    << testcase.upper_inclusive << testcase.upper;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref)
                        << val << testcase.lower_inclusive << testcase.lower
                        << testcase.upper_inclusive << testcase.upper;
                }
            }
        }
    }
}

TEST_P(ExprTest, TestExistsJson) {
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
    for (auto testcase : testcases) {
        auto check = [&](bool value) { return value; };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr =
            std::make_shared<milvus::expr::ExistsExpr>(milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path));
        auto plannode =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final = ExecuteQueryExpr(
            plannode, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(plannode.get(),
                                                    seg_promote,
                                                    N * num_iters,
                                                    MAX_TIMESTAMP,
                                                    &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .exist(pointer);
            auto ref = check(val);
            ASSERT_EQ(ans, ref);
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref);
            }
        }
    }
}

TEST_P(ExprTest, TestExistsJsonNullable) {
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
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);

        json_col.insert(
            json_col.end(), new_json_col.begin(), new_json_col.end());
        valid_data_col = raw_data.get_col_valid(json_fid);
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
    for (auto testcase : testcases) {
        auto check = [&](bool value, bool valid) {
            if (!valid) {
                return false;
            }
            return value;
        };
        RetrievePlanNode plan;
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr =
            std::make_shared<milvus::expr::ExistsExpr>(milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path));
        auto plannode =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final = ExecuteQueryExpr(
            plannode, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        for (auto i = 0; i < std::min(N * num_iters, 10); ++i) {
            offsets.emplace_back(i);
        }
        auto col_vec = milvus::test::gen_filter_res(plannode.get(),
                                                    seg_promote,
                                                    N * num_iters,
                                                    MAX_TIMESTAMP,
                                                    &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), std::min(N * num_iters, 10));

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto val = milvus::Json(simdjson::padded_string(json_col[i]))
                           .exist(pointer);
            auto ref = check(val, valid_data_col[i]);
            ASSERT_EQ(ans, ref);
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], ref);
            }
        }
    }
}

template <typename T>
T
GetValueFromProto(const milvus::proto::plan::GenericValue& value_proto) {
    if constexpr (std::is_same_v<T, bool>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kBoolVal);
        return static_cast<T>(value_proto.bool_val());
    } else if constexpr (std::is_integral_v<T>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kInt64Val);
        return static_cast<T>(value_proto.int64_val());
    } else if constexpr (std::is_floating_point_v<T>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kFloatVal);
        return static_cast<T>(value_proto.float_val());
    } else if constexpr (std::is_same_v<T, std::string>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kStringVal);
        return static_cast<T>(value_proto.string_val());
    } else if constexpr (std::is_same_v<T, milvus::proto::plan::Array>) {
        Assert(value_proto.val_case() ==
               milvus::proto::plan::GenericValue::kArrayVal);
        return static_cast<T>(value_proto.array_val());
    } else if constexpr (std::is_same_v<T, milvus::proto::plan::GenericValue>) {
        return static_cast<T>(value_proto);
    } else {
        ThrowInfo(milvus::ErrorCode::UnexpectedError,
                  "unsupported generic value type");
    }
};

TEST_P(ExprTest, TestUnaryRangeJson) {
    struct Testcase {
        int64_t val;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{{10, {"int"}},
                                    {20, {"int"}},
                                    {30, {"int"}},
                                    {40, {"int"}},
                                    {1, {"array", "0"}},
                                    {2, {"array", "1"}},
                                    {3, {"array", "2"}}};

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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

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
                    ThrowInfo(Unsupported, "unsupported range node");
                }
            }

            auto pointer = milvus::Json::pointer(testcase.nested_path);
            proto::plan::GenericValue value;
            value.set_int64_val(testcase.val);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, testcase.nested_path),
                op,
                value,
                std::vector<proto::plan::GenericValue>{});
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            auto final = ExecuteQueryExpr(
                plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N * num_iters);

            // specify some offsets and do scalar filtering on these offsets
            milvus::exec::OffsetVector offsets;
            offsets.reserve(N * num_iters / 2);
            for (auto i = 0; i < N * num_iters; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                        seg_promote,
                                                        N * num_iters,
                                                        MAX_TIMESTAMP,
                                                        &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            EXPECT_EQ(view.size(), N * num_iters / 2);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                if (testcase.nested_path[0] == "int" ||
                    testcase.nested_path[0] == "array") {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<int64_t>(pointer)
                            .value();

                    auto ref = f(val);
                    ASSERT_EQ(ans, ref) << "@" << i << "op" << op;

                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                } else {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<double>(pointer)
                            .value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                }
            }
        }
    }

    {
        struct Testcase {
            double val;
            std::vector<std::string> nested_path;
        };
        std::vector<Testcase> testcases{{1.1, {"double"}},
                                        {2.2, {"double"}},
                                        {3.3, {"double"}},
                                        {4.4, {"double"}},
                                        {1e40, {"double"}}};

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
        std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
        auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

        std::vector<OpType> ops{
            OpType::Equal,
            OpType::NotEqual,
            OpType::GreaterThan,
            OpType::GreaterEqual,
            OpType::LessThan,
            OpType::LessEqual,
        };
        for (const auto& testcase : testcases) {
            auto check = [&](double value) { return value == testcase.val; };
            std::function<bool(double)> f = check;
            for (auto& op : ops) {
                switch (op) {
                    case OpType::Equal: {
                        f = [&](double value) { return value == testcase.val; };
                        break;
                    }
                    case OpType::NotEqual: {
                        f = [&](double value) { return value != testcase.val; };
                        break;
                    }
                    case OpType::GreaterEqual: {
                        f = [&](double value) { return value >= testcase.val; };
                        break;
                    }
                    case OpType::GreaterThan: {
                        f = [&](double value) { return value > testcase.val; };
                        break;
                    }
                    case OpType::LessEqual: {
                        f = [&](double value) { return value <= testcase.val; };
                        break;
                    }
                    case OpType::LessThan: {
                        f = [&](double value) { return value < testcase.val; };
                        break;
                    }
                    default: {
                        ThrowInfo(Unsupported, "unsupported range node");
                    }
                }

                auto pointer = milvus::Json::pointer(testcase.nested_path);
                proto::plan::GenericValue value;
                value.set_float_val(testcase.val);
                auto expr =
                    std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                        milvus::expr::ColumnInfo(
                            json_fid, DataType::JSON, testcase.nested_path),
                        op,
                        value,
                        std::vector<proto::plan::GenericValue>{});
                auto plan = std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, expr);
                auto final = ExecuteQueryExpr(
                    plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
                EXPECT_EQ(final.size(), N * num_iters);

                // specify some offsets and do scalar filtering on these offsets
                milvus::exec::OffsetVector offsets;
                offsets.reserve(N * num_iters / 2);
                for (auto i = 0; i < N * num_iters; ++i) {
                    if (i % 2 == 0) {
                        offsets.emplace_back(i);
                    }
                }
                auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                            seg_promote,
                                                            N * num_iters,
                                                            MAX_TIMESTAMP,
                                                            &offsets);
                BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
                EXPECT_EQ(view.size(), N * num_iters / 2);

                for (int i = 0; i < N * num_iters; ++i) {
                    auto ans = final[i];

                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<double>(pointer)
                            .value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                }
            }
        }
    }

    {
        struct Testcase {
            std::string val;
            std::vector<std::string> nested_path;
        };
        std::vector<Testcase> testcases{
            {"abc", {"string"}},
            {"This is a line break\\nThis is a new line!", {"string"}}};

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
        std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
        auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

        std::vector<OpType> ops{
            OpType::Equal,
            OpType::NotEqual,
            OpType::GreaterThan,
            OpType::GreaterEqual,
            OpType::LessThan,
            OpType::LessEqual,
        };
        for (const auto& testcase : testcases) {
            auto check = [&](std::string_view value) {
                return value == testcase.val;
            };
            std::function<bool(std::string_view)> f = check;
            for (auto& op : ops) {
                switch (op) {
                    case OpType::Equal: {
                        f = [&](std::string_view value) {
                            return value == testcase.val;
                        };
                        break;
                    }
                    case OpType::NotEqual: {
                        f = [&](std::string_view value) {
                            return value != testcase.val;
                        };
                        break;
                    }
                    case OpType::GreaterEqual: {
                        f = [&](std::string_view value) {
                            return value >= testcase.val;
                        };
                        break;
                    }
                    case OpType::GreaterThan: {
                        f = [&](std::string_view value) {
                            return value > testcase.val;
                        };
                        break;
                    }
                    case OpType::LessEqual: {
                        f = [&](std::string_view value) {
                            return value <= testcase.val;
                        };
                        break;
                    }
                    case OpType::LessThan: {
                        f = [&](std::string_view value) {
                            return value < testcase.val;
                        };
                        break;
                    }
                    default: {
                        ThrowInfo(Unsupported, "unsupported range node");
                    }
                }

                auto pointer = milvus::Json::pointer(testcase.nested_path);
                proto::plan::GenericValue value;
                value.set_string_val(testcase.val);
                auto expr =
                    std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                        milvus::expr::ColumnInfo(
                            json_fid, DataType::JSON, testcase.nested_path),
                        op,
                        value,
                        std::vector<proto::plan::GenericValue>{});
                auto plan = std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, expr);
                auto final = ExecuteQueryExpr(
                    plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
                EXPECT_EQ(final.size(), N * num_iters);

                // specify some offsets and do scalar filtering on these offsets
                milvus::exec::OffsetVector offsets;
                offsets.reserve(N * num_iters / 2);
                for (auto i = 0; i < N * num_iters; ++i) {
                    if (i % 2 == 0) {
                        offsets.emplace_back(i);
                    }
                }
                auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                            seg_promote,
                                                            N * num_iters,
                                                            MAX_TIMESTAMP,
                                                            &offsets);
                BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
                EXPECT_EQ(view.size(), N * num_iters / 2);

                for (int i = 0; i < N * num_iters; ++i) {
                    auto ans = final[i];

                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<std::string_view>(pointer)
                            .value();
                    auto ref = f(val);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                }
            }
        }
    }

    struct TestArrayCase {
        proto::plan::GenericValue val;
        std::vector<std::string> nested_path;
    };

    proto::plan::GenericValue value;
    auto* arr = value.mutable_array_val();
    arr->set_same_type(true);
    proto::plan::GenericValue int_val1;
    int_val1.set_int64_val(int64_t(1));
    arr->add_array()->CopyFrom(int_val1);

    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(2));
    arr->add_array()->CopyFrom(int_val2);

    proto::plan::GenericValue int_val3;
    int_val3.set_int64_val(int64_t(3));
    arr->add_array()->CopyFrom(int_val3);

    std::vector<TestArrayCase> array_cases = {{value, {"array"}}};
    for (const auto& testcase : array_cases) {
        auto check = [&](OpType op) {
            if (testcase.nested_path[0] == "array" && op == OpType::Equal) {
                return true;
            }
            return false;
        };
        for (auto& op : ops) {
            auto pointer = milvus::Json::pointer(testcase.nested_path);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, testcase.nested_path),
                op,
                testcase.val,
                std::vector<proto::plan::GenericValue>{});
            BitsetType final;
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            final = ExecuteQueryExpr(
                plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N * num_iters);

            // specify some offsets and do scalar filtering on these offsets
            milvus::exec::OffsetVector offsets;
            offsets.reserve(N * num_iters / 2);
            for (auto i = 0; i < N * num_iters; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                        seg_promote,
                                                        N * num_iters,
                                                        MAX_TIMESTAMP,
                                                        &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            EXPECT_EQ(view.size(), N * num_iters / 2);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                auto ref = check(op);
                ASSERT_EQ(ans, ref) << "@" << i << "op" << op;
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], ref) << "@" << i << "op" << op;
                }
            }
        }
    }
}

TEST_P(ExprTest, TestUnaryRangeJsonNullable) {
    struct Testcase {
        int64_t val;
        std::vector<std::string> nested_path;
    };
    std::vector<Testcase> testcases{{10, {"int"}},
                                    {20, {"int"}},
                                    {30, {"int"}},
                                    {40, {"int"}},
                                    {1, {"array", "0"}},
                                    {2, {"array", "1"}},
                                    {3, {"array", "2"}}};

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> json_col;
    FixedVector<bool> valid_data_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_json_col = raw_data.get_col<std::string>(json_fid);
        valid_data_col = raw_data.get_col_valid(json_fid);

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
    std::vector<OpType> ops{
        OpType::Equal,
        OpType::NotEqual,
        OpType::GreaterThan,
        OpType::GreaterEqual,
        OpType::LessThan,
        OpType::LessEqual,
    };
    for (const auto& testcase : testcases) {
        auto check = [&](int64_t value, bool valid) {
            return value == testcase.val;
        };
        std::function<bool(int64_t, bool)> f = check;
        for (auto& op : ops) {
            switch (op) {
                case OpType::Equal: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value == testcase.val;
                    };
                    break;
                }
                case OpType::NotEqual: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return true;
                        }
                        return value != testcase.val;
                    };
                    break;
                }
                case OpType::GreaterEqual: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value >= testcase.val;
                    };
                    break;
                }
                case OpType::GreaterThan: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value > testcase.val;
                    };
                    break;
                }
                case OpType::LessEqual: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value <= testcase.val;
                    };
                    break;
                }
                case OpType::LessThan: {
                    f = [&](int64_t value, bool valid) {
                        if (!valid) {
                            return false;
                        }
                        return value < testcase.val;
                    };
                    break;
                }
                default: {
                    ThrowInfo(Unsupported, "unsupported range node");
                }
            }

            auto pointer = milvus::Json::pointer(testcase.nested_path);
            proto::plan::GenericValue value;
            value.set_int64_val(testcase.val);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, testcase.nested_path),
                op,
                value,
                std::vector<proto::plan::GenericValue>{});
            BitsetType final;
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            final = ExecuteQueryExpr(
                plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N * num_iters);

            // specify some offsets and do scalar filtering on these offsets
            milvus::exec::OffsetVector offsets;
            offsets.reserve(N * num_iters / 2);
            for (auto i = 0; i < N * num_iters; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(plan.get(),
                                                        seg_promote,
                                                        N * num_iters,
                                                        MAX_TIMESTAMP,
                                                        &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            EXPECT_EQ(view.size(), N * num_iters / 2);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                if (testcase.nested_path[0] == "int") {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<int64_t>(pointer)
                            .value();
                    auto ref = f(val, valid_data_col[i]);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                } else {
                    auto val =
                        milvus::Json(simdjson::padded_string(json_col[i]))
                            .template at<double>(pointer)
                            .value();
                    auto ref = f(val, valid_data_col[i]);
                    ASSERT_EQ(ans, ref);
                    if (i % 2 == 0) {
                        ASSERT_EQ(view[int(i / 2)], ref);
                    }
                }
            }
        }
    }

    struct TestArrayCase {
        proto::plan::GenericValue val;
        std::vector<std::string> nested_path;
    };

    proto::plan::GenericValue value;
    auto* arr = value.mutable_array_val();
    arr->set_same_type(true);
    proto::plan::GenericValue int_val1;
    int_val1.set_int64_val(int64_t(1));
    arr->add_array()->CopyFrom(int_val1);

    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(2));
    arr->add_array()->CopyFrom(int_val2);

    proto::plan::GenericValue int_val3;
    int_val3.set_int64_val(int64_t(3));
    arr->add_array()->CopyFrom(int_val3);

    std::vector<TestArrayCase> array_cases = {{value, {"array"}}};
    for (const auto& testcase : array_cases) {
        auto check = [&](OpType op, bool valid) {
            if (!valid) {
                return op == OpType::NotEqual ? true : false;
            }
            if (testcase.nested_path[0] == "array" && op == OpType::Equal) {
                return true;
            }
            return false;
        };
        for (auto& op : ops) {
            auto pointer = milvus::Json::pointer(testcase.nested_path);
            auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, testcase.nested_path),
                op,
                testcase.val,
                std::vector<proto::plan::GenericValue>{});
            BitsetType final;
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            final = ExecuteQueryExpr(
                plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N * num_iters);

            for (int i = 0; i < N * num_iters; ++i) {
                auto ans = final[i];
                auto ref = check(op, valid_data_col[i]);
                ASSERT_EQ(ans, ref) << "@" << i << "op" << op;
            }
        }
    }
}
