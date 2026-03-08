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
#include <memory>
#include <ratio>
#include <string>
#include <string_view>
#include <thread>
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

TEST_P(ExprTest, PraseJsonContainsExpr) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->AddDebugField("json", DataType::JSON);

    // Test cases using string expressions instead of text proto format
    std::vector<std::string> expressions{
        // ContainsAny with int64 values
        R"(json_contains_any(json["A"], [1, 2, 3]))",
        // ContainsAll with int64 values
        R"(json_contains_all(json["A"], [1, 2, 3]))",
        // ContainsAll with bool values
        R"(json_contains_all(json["A"], [true, false, true]))",
        // ContainsAll with float values
        R"(json_contains_all(json["A"], [1.1, 2.2, 3.3]))",
        // ContainsAll with string values
        R"(json_contains_all(json["A"], ["1", "2", "3"]))",
        // ContainsAll with mixed types
        R"(json_contains_all(json["A"], ["1", 2, 3.3, true]))",
    };

    SetSchema(schema);
    for (auto& expr : expressions) {
        auto plan_str = create_search_plan_from_expr(expr);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    }
}

template <typename T>
struct Testcase {
    std::vector<T> term;
    std::vector<std::string> nested_path;
    bool res;
};

TEST_P(ExprTest, TestJsonContainsAny) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
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
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
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
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
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
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
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
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
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

TEST_P(ExprTest, TestJsonContainsAnyNullable) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
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
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
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
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
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
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
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
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            values);
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

TEST_P(ExprTest, TestJsonContainsAll) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<Testcase<bool>> bool_testcases{{{true, true}, {"bool"}},
                                               {{false, false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
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
        {{1.123, 10.34}, {"double"}},
        {{10.34, 100.234}, {"double"}},
        {{100.234, 1000.4546}, {"double"}},
        {{1000.4546, 1.123}, {"double"}},
        {{1000.4546, 10.34}, {"double"}},
        {{1.123, 100.234}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
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
        {{1, 10}, {"int"}},
        {{10, 100}, {"int"}},
        {{100, 1000}, {"int"}},
        {{1000, 10}, {"int"}},
        {{2, 4, 6, 8, 10}, {"int"}},
        {{1, 2, 3, 4, 5}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
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
        {{"1sads", "10dsf"}, {"string"}},
        {{"10dsf", "100"}, {"string"}},
        {{"100", "10dsf", "1sads"}, {"string"}},
        {{"100ddfdsssdfdsfsd0", "100"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
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

TEST_P(ExprTest, TestJsonContainsAllNullable) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<Testcase<bool>> bool_testcases{{{true, true}, {"bool"}},
                                               {{false, false}, {"bool"}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values, bool valid) {
            if (!valid) {
                return false;
            }
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_bool_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
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
        {{1.123, 10.34}, {"double"}},
        {{10.34, 100.234}, {"double"}},
        {{100.234, 1000.4546}, {"double"}},
        {{1000.4546, 1.123}, {"double"}},
        {{1000.4546, 10.34}, {"double"}},
        {{1.123, 100.234}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values, bool valid) {
            if (!valid) {
                return false;
            }
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_float_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
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
        {{1, 10}, {"int"}},
        {{10, 100}, {"int"}},
        {{100, 1000}, {"int"}},
        {{1000, 10}, {"int"}},
        {{2, 4, 6, 8, 10}, {"int"}},
        {{1, 2, 3, 4, 5}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values, bool valid) {
            if (!valid) {
                return false;
            }
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
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
        {{"1sads", "10dsf"}, {"string"}},
        {{"10dsf", "100"}, {"string"}},
        {{"100", "10dsf", "1sads"}, {"string"}},
        {{"100ddfdsssdfdsfsd0", "100"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values,
                         bool valid) {
            if (!valid) {
                return false;
            }
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.term) {
            proto::plan::GenericValue val;
            val.set_string_val(v);
            values.push_back(val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
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

TEST_P(ExprTest, TestJsonContainsArray) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    proto::plan::GenericValue generic_a;
    auto* a = generic_a.mutable_array_val();
    a->set_same_type(false);
    for (int i = 0; i < 4; ++i) {
        if (i % 4 == 0) {
            proto::plan::GenericValue int_val;
            int_val.set_int64_val(int64_t(i));
            a->add_array()->CopyFrom(int_val);
        } else if ((i - 1) % 4 == 0) {
            proto::plan::GenericValue bool_val;
            bool_val.set_bool_val(bool(i));
            a->add_array()->CopyFrom(bool_val);
        } else if ((i - 2) % 4 == 0) {
            proto::plan::GenericValue float_val;
            float_val.set_float_val(double(i));
            a->add_array()->CopyFrom(float_val);
        } else if ((i - 3) % 4 == 0) {
            proto::plan::GenericValue string_val;
            string_val.set_string_val(std::to_string(i));
            a->add_array()->CopyFrom(string_val);
        }
    }
    proto::plan::GenericValue generic_b;
    auto* b = generic_b.mutable_array_val();
    b->set_same_type(true);
    proto::plan::GenericValue int_val1;
    int_val1.set_int64_val(int64_t(1));
    b->add_array()->CopyFrom(int_val1);

    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(2));
    b->add_array()->CopyFrom(int_val2);

    proto::plan::GenericValue int_val3;
    int_val3.set_int64_val(int64_t(3));
    b->add_array()->CopyFrom(int_val3);

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{generic_a}, {"string"}}, {{generic_b}, {"array"}}};

    for (auto& testcase : diff_testcases) {
        auto check = [&](const std::vector<bool>& values, int i) {
            if (testcase.nested_path[0] == "array" && (i == 1 || i == N + 1)) {
                return true;
            }
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res, i));
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto check = [&](const std::vector<bool>& values, int i) {
            if (testcase.nested_path[0] == "array" && (i == 1 || i == N + 1)) {
                return true;
            }
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }

    proto::plan::GenericValue g_sub_arr1;
    auto* sub_arr1 = g_sub_arr1.mutable_array_val();
    sub_arr1->set_same_type(true);
    proto::plan::GenericValue int_val11;
    int_val11.set_int64_val(int64_t(1));
    sub_arr1->add_array()->CopyFrom(int_val11);

    proto::plan::GenericValue int_val12;
    int_val12.set_int64_val(int64_t(2));
    sub_arr1->add_array()->CopyFrom(int_val12);

    proto::plan::GenericValue g_sub_arr2;
    auto* sub_arr2 = g_sub_arr2.mutable_array_val();
    sub_arr2->set_same_type(true);
    proto::plan::GenericValue int_val21;
    int_val21.set_int64_val(int64_t(3));
    sub_arr2->add_array()->CopyFrom(int_val21);

    proto::plan::GenericValue int_val22;
    int_val22.set_int64_val(int64_t(4));
    sub_arr2->add_array()->CopyFrom(int_val22);
    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases2{
        {{g_sub_arr1, g_sub_arr2}, {"array2"}}};

    for (auto& testcase : diff_testcases2) {
        auto check = [&]() { return true; };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            ASSERT_EQ(ans, check());
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check());
            }
        }
    }

    for (auto& testcase : diff_testcases2) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }

    proto::plan::GenericValue g_sub_arr3;
    auto* sub_arr3 = g_sub_arr3.mutable_array_val();
    sub_arr3->set_same_type(true);
    proto::plan::GenericValue int_val31;
    int_val31.set_int64_val(int64_t(5));
    sub_arr3->add_array()->CopyFrom(int_val31);

    proto::plan::GenericValue int_val32;
    int_val32.set_int64_val(int64_t(6));
    sub_arr3->add_array()->CopyFrom(int_val32);

    proto::plan::GenericValue g_sub_arr4;
    auto* sub_arr4 = g_sub_arr4.mutable_array_val();
    sub_arr4->set_same_type(true);
    proto::plan::GenericValue int_val41;
    int_val41.set_int64_val(int64_t(7));
    sub_arr4->add_array()->CopyFrom(int_val41);

    proto::plan::GenericValue int_val42;
    int_val42.set_int64_val(int64_t(8));
    sub_arr4->add_array()->CopyFrom(int_val42);
    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases3{
        {{g_sub_arr3, g_sub_arr4}, {"array2"}}};

    for (auto& testcase : diff_testcases3) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }

    for (auto& testcase : diff_testcases3) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsArrayNullable) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    proto::plan::GenericValue generic_a;
    auto* a = generic_a.mutable_array_val();
    a->set_same_type(false);
    for (int i = 0; i < 4; ++i) {
        if (i % 4 == 0) {
            proto::plan::GenericValue int_val;
            int_val.set_int64_val(int64_t(i));
            a->add_array()->CopyFrom(int_val);
        } else if ((i - 1) % 4 == 0) {
            proto::plan::GenericValue bool_val;
            bool_val.set_bool_val(bool(i));
            a->add_array()->CopyFrom(bool_val);
        } else if ((i - 2) % 4 == 0) {
            proto::plan::GenericValue float_val;
            float_val.set_float_val(double(i));
            a->add_array()->CopyFrom(float_val);
        } else if ((i - 3) % 4 == 0) {
            proto::plan::GenericValue string_val;
            string_val.set_string_val(std::to_string(i));
            a->add_array()->CopyFrom(string_val);
        }
    }
    proto::plan::GenericValue generic_b;
    auto* b = generic_b.mutable_array_val();
    b->set_same_type(true);
    proto::plan::GenericValue int_val1;
    int_val1.set_int64_val(int64_t(1));
    b->add_array()->CopyFrom(int_val1);

    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(2));
    b->add_array()->CopyFrom(int_val2);

    proto::plan::GenericValue int_val3;
    int_val3.set_int64_val(int64_t(3));
    b->add_array()->CopyFrom(int_val3);

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{generic_a}, {"string"}}, {{generic_b}, {"array"}}};

    for (auto& testcase : diff_testcases) {
        auto check = [&](const std::vector<bool>& values, int i, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.nested_path[0] == "array" && (i == 1 || i == N + 1)) {
                return true;
            }
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i, valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i, valid_data[i]));
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto check = [&](const std::vector<bool>& values, int i, bool valid) {
            if (!valid) {
                return false;
            }
            if (testcase.nested_path[0] == "array" && (i == 1 || i == N + 1)) {
                return true;
            }
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i, valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i, valid_data[i]));
            }
        }
    }

    proto::plan::GenericValue g_sub_arr1;
    auto* sub_arr1 = g_sub_arr1.mutable_array_val();
    sub_arr1->set_same_type(true);
    proto::plan::GenericValue int_val11;
    int_val11.set_int64_val(int64_t(1));
    sub_arr1->add_array()->CopyFrom(int_val11);

    proto::plan::GenericValue int_val12;
    int_val12.set_int64_val(int64_t(2));
    sub_arr1->add_array()->CopyFrom(int_val12);

    proto::plan::GenericValue g_sub_arr2;
    auto* sub_arr2 = g_sub_arr2.mutable_array_val();
    sub_arr2->set_same_type(true);
    proto::plan::GenericValue int_val21;
    int_val21.set_int64_val(int64_t(3));
    sub_arr2->add_array()->CopyFrom(int_val21);

    proto::plan::GenericValue int_val22;
    int_val22.set_int64_val(int64_t(4));
    sub_arr2->add_array()->CopyFrom(int_val22);
    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases2{
        {{g_sub_arr1, g_sub_arr2}, {"array2"}}};

    for (auto& testcase : diff_testcases2) {
        auto check = [&](bool valid) {
            if (!valid) {
                return false;
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            ASSERT_EQ(ans, check(valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(valid_data[i]));
            }
        }
    }

    for (auto& testcase : diff_testcases2) {
        auto check = [&](const std::vector<bool>& values, int i, bool valid) {
            if (!valid) {
                return false;
            }
            return true;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i, valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i, valid_data[i]));
            }
        }
    }

    proto::plan::GenericValue g_sub_arr3;
    auto* sub_arr3 = g_sub_arr3.mutable_array_val();
    sub_arr3->set_same_type(true);
    proto::plan::GenericValue int_val31;
    int_val31.set_int64_val(int64_t(5));
    sub_arr3->add_array()->CopyFrom(int_val31);

    proto::plan::GenericValue int_val32;
    int_val32.set_int64_val(int64_t(6));
    sub_arr3->add_array()->CopyFrom(int_val32);

    proto::plan::GenericValue g_sub_arr4;
    auto* sub_arr4 = g_sub_arr4.mutable_array_val();
    sub_arr4->set_same_type(true);
    proto::plan::GenericValue int_val41;
    int_val41.set_int64_val(int64_t(7));
    sub_arr4->add_array()->CopyFrom(int_val41);

    proto::plan::GenericValue int_val42;
    int_val42.set_int64_val(int64_t(8));
    sub_arr4->add_array()->CopyFrom(int_val42);
    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases3{
        {{g_sub_arr3, g_sub_arr4}, {"array2"}}};

    for (auto& testcase : diff_testcases3) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }

    for (auto& testcase : diff_testcases3) {
        auto check = [&](const std::vector<bool>& values, int i) {
            return false;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            testcase.term);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
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
            std::vector<bool> res;
            ASSERT_EQ(ans, check(res, i));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(res, i));
            }
        }
    }
}

milvus::proto::plan::GenericValue
generatedArrayWithFourDiffType(int64_t int_val,
                               double float_val,
                               bool bool_val,
                               std::string string_val) {
    proto::plan::GenericValue value;
    proto::plan::Array diff_type_array;
    diff_type_array.set_same_type(false);
    proto::plan::GenericValue int_value;
    int_value.set_int64_val(int_val);
    diff_type_array.add_array()->CopyFrom(int_value);

    proto::plan::GenericValue float_value;
    float_value.set_float_val(float_val);
    diff_type_array.add_array()->CopyFrom(float_value);

    proto::plan::GenericValue bool_value;
    bool_value.set_bool_val(bool_val);
    diff_type_array.add_array()->CopyFrom(bool_value);

    proto::plan::GenericValue string_value;
    string_value.set_string_val(string_val);
    diff_type_array.add_array()->CopyFrom(string_value);

    value.mutable_array_val()->CopyFrom(diff_type_array);
    return value;
}

TEST_P(ExprTest, TestJsonContainsDiffTypeArray) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);
    auto diff_type_array1 =
        generatedArrayWithFourDiffType(1, 2.2, false, "abc");
    auto diff_type_array2 =
        generatedArrayWithFourDiffType(1, 2.2, false, "def");
    auto diff_type_array3 = generatedArrayWithFourDiffType(1, 2.2, true, "abc");
    auto diff_type_array4 =
        generatedArrayWithFourDiffType(1, 3.3, false, "abc");
    auto diff_type_array5 =
        generatedArrayWithFourDiffType(2, 2.2, false, "abc");

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{diff_type_array1, int_value}, {"array3"}, true},
        {{diff_type_array2, int_value}, {"array3"}, false},
        {{diff_type_array3, int_value}, {"array3"}, false},
        {{diff_type_array4, int_value}, {"array3"}, false},
        {{diff_type_array5, int_value}, {"array3"}, false},
    };

    for (auto& testcase : diff_testcases) {
        auto check = [&]() { return testcase.res; };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            false,
            testcase.term);
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
            ASSERT_EQ(ans, check());
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check());
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto check = [&]() { return false; };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            false,
            testcase.term);
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
            ASSERT_EQ(ans, check());
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check());
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsDiffTypeArrayNullable) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);
    auto diff_type_array1 =
        generatedArrayWithFourDiffType(1, 2.2, false, "abc");
    auto diff_type_array2 =
        generatedArrayWithFourDiffType(1, 2.2, false, "def");
    auto diff_type_array3 = generatedArrayWithFourDiffType(1, 2.2, true, "abc");
    auto diff_type_array4 =
        generatedArrayWithFourDiffType(1, 3.3, false, "abc");
    auto diff_type_array5 =
        generatedArrayWithFourDiffType(2, 2.2, false, "abc");

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{diff_type_array1, int_value}, {"array3"}, true},
        {{diff_type_array2, int_value}, {"array3"}, false},
        {{diff_type_array3, int_value}, {"array3"}, false},
        {{diff_type_array4, int_value}, {"array3"}, false},
        {{diff_type_array5, int_value}, {"array3"}, false},
    };

    for (auto& testcase : diff_testcases) {
        auto check = [&](bool valid) {
            if (!valid) {
                return false;
            }
            return testcase.res;
        };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            false,
            testcase.term);
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
            ASSERT_EQ(ans, check(valid_data[i]));
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check(valid_data[i]));
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto check = [&]() { return false; };
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            false,
            testcase.term);
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
            ASSERT_EQ(ans, check());
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], check());
            }
        }
    }
}

TEST_P(ExprTest, TestJsonContainsDiffType) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    proto::plan::GenericValue int_val;
    int_val.set_int64_val(int64_t(3));
    proto::plan::GenericValue bool_val;
    bool_val.set_bool_val(bool(false));
    proto::plan::GenericValue float_val;
    float_val.set_float_val(double(100.34));
    proto::plan::GenericValue string_val;
    string_val.set_string_val("10dsf");

    proto::plan::GenericValue string_val2;
    string_val2.set_string_val("abc");
    proto::plan::GenericValue bool_val2;
    bool_val2.set_bool_val(bool(true));
    proto::plan::GenericValue float_val2;
    float_val2.set_float_val(double(2.2));
    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(1));

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{int_val, bool_val, float_val, string_val},
         {"diff_type_array"},
         false},
        {{string_val2, bool_val2, float_val2, int_val2},
         {"diff_type_array"},
         true},
    };

    for (auto& testcase : diff_testcases) {
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            false,
            testcase.term);
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
            ASSERT_EQ(ans, testcase.res);
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], testcase.res);
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            false,
            testcase.term);
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
            ASSERT_EQ(ans, testcase.res);
            if (i < std::min(N * num_iters, 10)) {
                ASSERT_EQ(view[i], testcase.res);
            }
        }
    }
}
TEST_P(ExprTest, TestJsonContainsDiffTypeNullable) {
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
    std::this_thread::sleep_for(std::chrono::milliseconds(200) * 2);
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);

    proto::plan::GenericValue int_val;
    int_val.set_int64_val(int64_t(3));
    proto::plan::GenericValue bool_val;
    bool_val.set_bool_val(bool(false));
    proto::plan::GenericValue float_val;
    float_val.set_float_val(double(100.34));
    proto::plan::GenericValue string_val;
    string_val.set_string_val("10dsf");

    proto::plan::GenericValue string_val2;
    string_val2.set_string_val("abc");
    proto::plan::GenericValue bool_val2;
    bool_val2.set_bool_val(bool(true));
    proto::plan::GenericValue float_val2;
    float_val2.set_float_val(double(2.2));
    proto::plan::GenericValue int_val2;
    int_val2.set_int64_val(int64_t(1));

    std::vector<Testcase<proto::plan::GenericValue>> diff_testcases{
        {{int_val, bool_val, float_val, string_val},
         {"diff_type_array"},
         false},
        {{string_val2, bool_val2, float_val2, int_val2},
         {"diff_type_array"},
         true},
    };

    for (auto& testcase : diff_testcases) {
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            false,
            testcase.term);
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
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], false);
                }
            } else {
                ASSERT_EQ(ans, testcase.res);
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], testcase.res);
                }
            }
        }
    }

    for (auto& testcase : diff_testcases) {
        auto pointer = milvus::Json::pointer(testcase.nested_path);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            milvus::expr::ColumnInfo(
                json_fid, DataType::JSON, testcase.nested_path),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            false,
            testcase.term);
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
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], false);
                }
            } else {
                ASSERT_EQ(ans, testcase.res);
                if (i < std::min(N * num_iters, 10)) {
                    ASSERT_EQ(view[i], testcase.res);
                }
            }
        }
    }
}
