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

#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "common/Schema.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"

using namespace milvus;

TEST(JsonProjectedExpressionTest, ContainsUsesIndexAndExactRawFallback) {
    milvus::test::ExprBatchSizeGuard batch_size_guard(8);
    std::vector<std::string> json_raw_data = {
        R"(1)",
        R"("a simple string")",
        R"(null)",
        R"(true)",
        R"([])",
        R"({})",
        R"([1, 2, 3])",
        R"([1.0, 2.0, 3.0])",
        R"([true, false, null])",
        R"(["hello", "world"])",
        R"([{"nested": true}, {"nested": false}])",
        R"({"a": true})",
        R"({"a": 1.0})",
        R"({"a": 1})",
        R"({"a": null})",
        R"({"a": "hello"})",
        R"({"a": {"nested": true}})",
        R"({"a": [1, 2, 3]})",
        R"({"a": [1.0, 2, 3]})",
        R"({"a": [true, false]})",
        R"({"a": ["x", "y"]})",
        R"({"a": [{"nested": true}, {"nested": false}]})",
        R"({"a": []})",
        R"({"a": [0, 2, 3]})",
        R"({"a": [{"b": 1}, 2.0, 3.0, "4", true, [1, 3.0], null]})",
        R"({"a": [9007199254740992]})",
        R"({"a": [9007199254740993]})",
        R"({"a": [9007199254740994]})",
    };

    auto json_path = "/a";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    test::expr_index::RawFieldFiles raw_files;
    auto json_field = test::expr_index::JsonField(json_raw_data);
    auto segment = segcore::CreateSealedSegment(schema);
    auto raw_info = raw_files.Prepare(json_fid, {json_field});
    segment->LoadFieldData(raw_info);
    auto opened = test::expr_index::BuildIndex(
        json_fid,
        DataType::JSON,
        index::INVERTED_INDEX_TYPE,
        {json_field},
        {{JSON_PATH, json_path}, {JSON_CAST_TYPE, "ARRAY_DOUBLE"}});
    test::expr_index::InstallIndex(
        *segment, json_fid, DataType::JSON, std::move(opened));

    std::vector<std::tuple<proto::plan::GenericValue, std::vector<int64_t>>>
        test_cases;

    proto::plan::GenericValue value;
    value.set_int64_val(1);
    test_cases.push_back(std::make_tuple(value, std::vector<int64_t>{17, 18}));

    proto::plan::GenericValue value2;
    value2.set_int64_val(2);
    test_cases.push_back(
        std::make_tuple(value2, std::vector<int64_t>{17, 18, 23, 24}));

    for (auto& test_case : test_cases) {
        auto query_value = std::get<0>(test_case);
        auto expr = std::make_shared<expr::JsonContainsExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
            proto::plan::JSONContainsExpr_JSONOp::
                JSONContainsExpr_JSONOp_Contains,
            true,
            std::vector<proto::plan::GenericValue>{query_value});
        EXPECT_TRUE(test::CanExprExecuteAllAtOnce(
            expr, segment.get(), json_raw_data.size()));

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        auto result = query::ExecuteQueryExpr(
            plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

        auto expect_result = std::get<1>(test_case);
        EXPECT_EQ(result.count(), expect_result.size());
        for (auto& id : expect_result) {
            EXPECT_TRUE(result[id]);
        }
    }

    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);
    proto::plan::GenericValue string_value;
    string_value.set_string_val("4");
    auto mixed_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
        proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        false,
        std::vector<proto::plan::GenericValue>{int_value, string_value});
    EXPECT_FALSE(test::CanExprExecuteAllAtOnce(
        mixed_expr, segment.get(), json_raw_data.size()));
    auto mixed_plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, mixed_expr);
    auto mixed_result = query::ExecuteQueryExpr(
        mixed_plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
    EXPECT_EQ(mixed_result.count(), 3);
    EXPECT_TRUE(mixed_result[17]);
    EXPECT_TRUE(mixed_result[18]);
    EXPECT_TRUE(mixed_result[24]);

    proto::plan::GenericValue large_int_value;
    large_int_value.set_int64_val(9007199254740993LL);
    auto large_int_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{large_int_value});
    EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
        large_int_expr, segment.get(), json_raw_data.size()));
    EXPECT_EQ(milvus::test::EvalExprBatchSizes(
                  large_int_expr, segment.get(), json_raw_data.size()),
              (std::vector<int64_t>{8, 8, 8, 4}));
    auto large_int_plan = std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, large_int_expr);
    auto large_int_result = query::ExecuteQueryExpr(
        large_int_plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
    EXPECT_EQ(large_int_result.count(), 1);
    EXPECT_TRUE(large_int_result[26]);
}

TEST(JsonProjectedExpressionTest, StringToDoubleCastReachesExpression) {
    std::vector<std::string> json_raw_data = {
        R"(1)",
        R"({"a": 1.0})",
        R"({"a": 1})",
        R"({"a": "1.0"})",
        R"({"a": true})",
        R"({"a": [1, 2, 3]})",
        R"({"a": {"b": 1}})",
    };

    auto json_path = "/a";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    test::expr_index::RawFieldFiles raw_files;
    auto json_field = test::expr_index::JsonField(json_raw_data);
    auto segment = segcore::CreateSealedSegment(schema);
    auto raw_info = raw_files.Prepare(json_fid, {json_field});
    segment->LoadFieldData(raw_info);
    auto opened = test::expr_index::BuildIndex(
        json_fid,
        DataType::JSON,
        index::INVERTED_INDEX_TYPE,
        {json_field},
        {{JSON_PATH, json_path},
         {JSON_CAST_TYPE, "DOUBLE"},
         {JSON_CAST_FUNCTION, "STRING_TO_DOUBLE"}});
    test::expr_index::InstallIndex(
        *segment, json_fid, DataType::JSON, std::move(opened));

    std::vector<std::tuple<proto::plan::GenericValue, std::vector<int64_t>>>
        test_cases;

    proto::plan::GenericValue value;
    value.set_int64_val(1);
    test_cases.push_back(std::make_tuple(value, std::vector<int64_t>{1, 2, 3}));
    for (auto& test_case : test_cases) {
        auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
            proto::plan::OpType::Equal,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        auto result = query::ExecuteQueryExpr(
            plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

        auto expect_result = std::get<1>(test_case);
        EXPECT_EQ(result.count(), expect_result.size());
        for (auto& id : expect_result) {
            EXPECT_TRUE(result[id]);
        }
    }
}
