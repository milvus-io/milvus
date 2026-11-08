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
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/Schema.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"

using namespace milvus;

static void
CheckContainsUsesIndexAndExactRawFallback(const std::string& index_type,
                                          Config params = Config::object()) {
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
    params[JSON_PATH] = json_path;
    params[JSON_CAST_TYPE] = "ARRAY_DOUBLE";
    auto opened = test::expr_index::BuildIndex(
        json_fid, DataType::JSON, index_type, {json_field}, params);
    if (index_type == index::HYBRID_INDEX_TYPE) {
        ASSERT_EQ(opened.family, index::families::kSort);
    }
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

TEST(JsonProjectedExpressionTest, ContainsUsesIndexAndExactRawFallback) {
    CheckContainsUsesIndexAndExactRawFallback(index::INVERTED_INDEX_TYPE);
}

TEST(JsonProjectedExpressionTest, HybridArrayContainsPreservesExactFallback) {
    CheckContainsUsesIndexAndExactRawFallback(
        index::HYBRID_INDEX_TYPE,
        {{index::SCALAR_INDEX_ENGINE_VERSION, kArrayHybridStlSortMinVersion},
         {index::ENABLE_ARRAY_SORT_INDEX, true},
         {index::BITMAP_INDEX_CARDINALITY_LIMIT, 1}});
}

TEST(JsonProjectedExpressionTest, TypedArraysMatchRawContainsAnyAndAll) {
    milvus::test::ExprBatchSizeGuard batch_size_guard(2);
    for (const auto& cast : {std::string("ARRAY_BOOL"),
                             std::string("ARRAY_DOUBLE"),
                             std::string("ARRAY_VARCHAR")}) {
        proto::plan::GenericValue first;
        proto::plan::GenericValue second;
        std::vector<std::string> rows;
        if (cast == "ARRAY_BOOL") {
            first.set_bool_val(false);
            second.set_bool_val(true);
            rows = {R"({"a":[false,true,true]})",
                    R"({"a":[true]})",
                    R"({"a":[]})",
                    R"({})",
                    R"({"a":null})",
                    R"({"a":3})",
                    R"({"a":[false,3,"ignored",null,{}]})",
                    R"({"a":[false]})",
                    R"({"a":[false,true]})"};
        } else if (cast == "ARRAY_DOUBLE") {
            first.set_float_val(1.0);
            second.set_float_val(100.0);
            rows = {R"({"a":[1,100,100]})",
                    R"({"a":[100.0]})",
                    R"({"a":[]})",
                    R"({})",
                    R"({"a":null})",
                    R"({"a":true})",
                    R"({"a":[1.0,false,"ignored",null,{}]})",
                    R"({"a":[1]})",
                    R"({"a":[1,100]})"};
        } else {
            first.set_string_val("a");
            second.set_string_val("z");
            rows = {R"({"a":["a","z","z"]})",
                    R"({"a":["z"]})",
                    R"({"a":[]})",
                    R"({})",
                    R"({"a":null})",
                    R"({"a":true})",
                    R"({"a":["a",false,3,null,{}]})",
                    R"({"a":["a"]})",
                    R"({"a":["a","z"]})"};
        }
        auto schema = std::make_shared<Schema>();
        const auto field = schema->AddDebugField("json", DataType::JSON, true);
        rows.push_back(R"({"a":[null,{},[]]})");
        const std::vector<uint8_t> validity{0x7f, 0x03};
        auto data = test::expr_index::JsonField(rows, true, validity.data());
        test::expr_index::RawFieldFiles raw_files;
        auto raw = segcore::CreateSealedSegment(schema);
        raw->LoadFieldData(raw_files.Prepare(field, {data}));
        for (const auto& [index_type, threshold] :
             {std::pair{index::HYBRID_INDEX_TYPE, 1},
              std::pair{index::HYBRID_INDEX_TYPE, 16},
              std::pair{index::ASCENDING_SORT, 1}}) {
            const bool hybrid =
                std::string_view(index_type) == index::HYBRID_INDEX_TYPE;
            for (const int engine_version :
                 hybrid ? std::vector<int>{3, 4, 5} : std::vector<int>{5}) {
                for (const bool mmap : {false, true}) {
                    SCOPED_TRACE(cast + "/index=" + index_type +
                                 "/version=" + std::to_string(engine_version) +
                                 "/threshold=" + std::to_string(threshold) +
                                 "/mmap=" + std::to_string(mmap));
                    auto indexed = segcore::CreateSealedSegment(schema);
                    indexed->LoadFieldData(raw_files.Prepare(field, {data}));
                    Config index_params{
                        {JSON_PATH, "/a"},
                        {JSON_CAST_TYPE, cast},
                        {index::SCALAR_INDEX_ENGINE_VERSION, engine_version},
                        {index::ENABLE_ARRAY_SORT_INDEX, true}};
                    if (std::string_view(index_type) ==
                        index::HYBRID_INDEX_TYPE) {
                        index_params[index::BITMAP_INDEX_CARDINALITY_LIMIT] =
                            threshold;
                    }
                    auto opened = test::expr_index::BuildIndex(field,
                                                               DataType::JSON,
                                                               index_type,
                                                               {data},
                                                               index_params,
                                                               DataType::NONE,
                                                               false,
                                                               mmap);
                    ASSERT_EQ(opened.family,
                              threshold != 1
                                  ? index::families::kBitmap
                                  : (hybrid && engine_version <
                                                   kArrayHybridStlSortMinVersion
                                         ? index::families::kInverted
                                         : index::families::kSort));
                    test::expr_index::InstallIndex(
                        *indexed, field, DataType::JSON, std::move(opened));
                    const auto expect_equal = [&](const auto& expected,
                                                  const auto& actual,
                                                  const auto&
                                                      expected_validity) {
                        const BitsetTypeView expected_bits(
                            expected.result->GetRawData(), rows.size());
                        const BitsetTypeView actual_bits(
                            actual.result->GetRawData(), rows.size());
                        const BitsetTypeView expected_valid(
                            expected_validity.result->GetValidRawData(),
                            rows.size());
                        const BitsetTypeView actual_valid(
                            actual.result->GetValidRawData(), rows.size());
                        for (size_t row = 0; row < rows.size(); ++row) {
                            EXPECT_EQ(actual_bits[row], expected_bits[row])
                                << row;
                            EXPECT_EQ(actual_valid[row], expected_valid[row])
                                << row;
                        }
                    };
                    for (const auto op :
                         {proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
                          proto::plan::JSONContainsExpr_JSONOp_ContainsAll}) {
                        for (const auto& terms : std::vector<
                                 std::vector<proto::plan::GenericValue>>{
                                 {first, second, first}, {}}) {
                            SCOPED_TRACE("query terms=" +
                                         std::to_string(terms.size()));
                            const auto expression =
                                std::make_shared<expr::JsonContainsExpr>(
                                    expr::ColumnInfo(
                                        field, DataType::JSON, {"a"}, true),
                                    op,
                                    true,
                                    terms);
                            if (!terms.empty()) {
                                EXPECT_TRUE(test::CanExprExecuteAllAtOnce(
                                    expression, indexed.get(), rows.size()));
                            }
                            const auto reference = test::EvalExprInBatches(
                                expression, raw.get(), rows.size());
                            const auto actual = test::EvalExprInBatches(
                                expression, indexed.get(), rows.size());
                            expect_equal(reference, actual, reference);
                            const BitsetTypeView actual_bits(
                                actual.result->GetRawData(), rows.size());
                            const BitsetTypeView actual_valid(
                                actual.result->GetValidRawData(), rows.size());
                            // Empty query lists use the shared expression short-circuit;
                            // compare its existing result and validity without changing
                            // the JSON NULL semantics in this index-selection patch.
                            if (!terms.empty()) {
                                const auto negated = std::make_shared<
                                    expr::LogicalUnaryExpr>(
                                    expr::LogicalUnaryExpr::OpType::LogicalNot,
                                    expression);
                                const auto raw_not = test::EvalExprInBatches(
                                    negated, raw.get(), rows.size());
                                const auto indexed_not =
                                    test::EvalExprInBatches(
                                        negated, indexed.get(), rows.size());
                                // NOT preserves the original expression validity.
                                expect_equal(raw_not, indexed_not, reference);
                                const BitsetTypeView indexed_not_bits(
                                    indexed_not.result->GetRawData(),
                                    rows.size());
                                EXPECT_TRUE(actual_valid[2]);
                                EXPECT_TRUE(actual_valid[9]);
                                EXPECT_TRUE(indexed_not_bits[2]);
                                EXPECT_TRUE(indexed_not_bits[9]);
                                EXPECT_FALSE(actual_valid[7]);
                                EXPECT_TRUE(actual_bits[0]);
                                EXPECT_TRUE(actual_bits[8]);
                                EXPECT_EQ(
                                    actual_bits[6],
                                    op ==
                                        proto::plan::
                                            JSONContainsExpr_JSONOp_ContainsAny);
                            }
                        }
                    }
                }
            }
        }
    }
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
