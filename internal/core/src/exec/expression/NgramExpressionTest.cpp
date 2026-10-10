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
#include <vector>

#include "common/Schema.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

// Regression for #44020: installing a candidate-only NGRAM reader must not
// route equality, membership, ranges, or logical composition into that reader.
TEST(NgramExpressionTest, NonPatternOperatorsUseTheirOrdinaryExpressionPaths) {
    const std::vector<std::string> data = {
        "apple", "banana", "cherry", "date", "elderberry", "fig", "grape",
        "honeydew", "kiwi", "lemon"};
    auto schema = std::make_shared<Schema>();
    const auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);
    test::expr_index::RawFieldFiles raw_files;
    auto field = test::expr_index::StringField(data);
    auto segment = CreateSealedSegment(schema);
    auto raw_info = raw_files.Prepare(field_id, {field});
    segment->LoadFieldData(raw_info);
    auto opened = test::expr_index::BuildIndex(
        field_id, DataType::VARCHAR, index::NGRAM_INDEX_TYPE, {field},
        {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}},
        DataType::NONE, false, true);
    test::expr_index::InstallIndex(*segment, field_id, DataType::VARCHAR,
                                   std::move(opened));
    const auto nb = data.size();
    // Test: TermFilterExpr (IN operator)
    {
        std::vector<proto::plan::GenericValue> values;
        proto::plan::GenericValue val1;
        val1.set_string_val("apple");
        values.push_back(val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("banana");
        values.push_back(val2);
        proto::plan::GenericValue val3;
        val3.set_string_val("cherry");
        values.push_back(val3);

        auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(field_id, DataType::VARCHAR), values);
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, term_expr);

        BitsetType final =
            ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        // Only apple, banana, cherry should match
        for (size_t i = 0; i < nb; i++) {
            if (i < 3) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            } else {
                ASSERT_FALSE(final[i]) << "Expected false at index " << i;
            }
        }
    }

    // Test: UnaryRangeExpr with Equal operator
    {
        auto unary_range_expr =
            test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
        auto column_info = test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false);
        unary_range_expr->set_allocated_column_info(column_info);
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary_range_expr);
        auto parser = ProtoParser(schema);
        auto typed_expr = parser.ParseExprs(*expr);
        auto parsed = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        BitsetType final =
            ExecuteQueryExpr(parsed, segment.get(), nb, MAX_TIMESTAMP);
        // Only apple should match (exact match)
        for (size_t i = 0; i < nb; i++) {
            if (i == 0) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            } else {
                ASSERT_FALSE(final[i]) << "Expected false at index " << i;
            }
        }
    }

    // Test: BinaryRangeFilterExpr
    {
        proto::plan::GenericValue lower_val;
        lower_val.set_string_val("cherry");
        proto::plan::GenericValue upper_val;
        upper_val.set_string_val("grape");

        auto binary_range_expr =
            std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
                lower_val,
                upper_val,
                true,
                true);
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, binary_range_expr);

        BitsetType final =
            ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        // Strings between "cherry" and "grape" inclusive: cherry, date, elderberry, fig, grape
        for (size_t i = 0; i < nb; i++) {
            if (i >= 2 && i <= 6) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            } else {
                ASSERT_FALSE(final[i]) << "Expected false at index " << i;
            }
        }
    }

    // Test: LogicalBinaryExpr with AND
    {
        // Create Equal expression
        auto unary_range_expr1 =
            test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
        auto column_info1 = test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false);
        unary_range_expr1->set_allocated_column_info(column_info1);
        auto expr1 = test::GenExpr();
        expr1->set_allocated_unary_range_expr(unary_range_expr1);
        auto parser1 = ProtoParser(schema);
        auto typed_expr1 = parser1.ParseExprs(*expr1);

        // Create NotEqual expression
        auto unary_range_expr2 = test::GenUnaryRangeExpr(
            proto::plan::OpType::NotEqual, "banana");
        auto column_info2 = test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false);
        unary_range_expr2->set_allocated_column_info(column_info2);
        auto expr2 = test::GenExpr();
        expr2->set_allocated_unary_range_expr(unary_range_expr2);
        auto parser2 = ProtoParser(schema);
        auto typed_expr2 = parser2.ParseExprs(*expr2);

        // Create LogicalBinaryExpr with AND
        auto logical_and_expr =
            std::make_shared<milvus::expr::LogicalBinaryExpr>(
                milvus::expr::LogicalBinaryExpr::OpType::And,
                typed_expr1,
                typed_expr2);
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, logical_and_expr);

        BitsetType final =
            ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        // Only apple should match (apple == "apple" AND apple != "banana")
        for (size_t i = 0; i < nb; i++) {
            if (i == 0) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            } else {
                ASSERT_FALSE(final[i]) << "Expected false at index " << i;
            }
        }
    }

    // Test: LogicalUnaryExpr with NOT
    {
        // Create Equal expression
        auto unary_range_expr =
            test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
        auto column_info = test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false);
        unary_range_expr->set_allocated_column_info(column_info);
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary_range_expr);
        auto parser = ProtoParser(schema);
        auto typed_expr = parser.ParseExprs(*expr);

        // Create LogicalUnaryExpr with NOT
        auto logical_not_expr =
            std::make_shared<milvus::expr::LogicalUnaryExpr>(
                milvus::expr::LogicalUnaryExpr::OpType::LogicalNot,
                typed_expr);
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, logical_not_expr);

        BitsetType final =
            ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        // All except apple should match (NOT (field == "apple"))
        for (size_t i = 0; i < nb; i++) {
            if (i != 0) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            } else {
                ASSERT_FALSE(final[i]) << "Expected false at index " << i;
            }
        }
    }

    // Test: LogicalBinaryExpr with OR
    {
        // Create Equal expression
        auto unary_range_expr1 =
            test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "apple");
        auto column_info1 = test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false);
        unary_range_expr1->set_allocated_column_info(column_info1);
        auto expr1 = test::GenExpr();
        expr1->set_allocated_unary_range_expr(unary_range_expr1);
        auto parser1 = ProtoParser(schema);
        auto typed_expr1 = parser1.ParseExprs(*expr1);

        // Create Equal expression for "banana"
        auto unary_range_expr2 =
            test::GenUnaryRangeExpr(proto::plan::OpType::Equal, "banana");
        auto column_info2 = test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false);
        unary_range_expr2->set_allocated_column_info(column_info2);
        auto expr2 = test::GenExpr();
        expr2->set_allocated_unary_range_expr(unary_range_expr2);
        auto parser2 = ProtoParser(schema);
        auto typed_expr2 = parser2.ParseExprs(*expr2);

        // Create LogicalBinaryExpr with OR
        auto logical_or_expr =
            std::make_shared<milvus::expr::LogicalBinaryExpr>(
                milvus::expr::LogicalBinaryExpr::OpType::Or,
                typed_expr1,
                typed_expr2);
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, logical_or_expr);

        BitsetType final =
            ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        // Apple and banana should match (apple == "apple" OR field == "banana")
        for (size_t i = 0; i < nb; i++) {
            if (i == 0 || i == 1) {
                ASSERT_TRUE(final[i]) << "Expected true at index " << i;
            } else {
                ASSERT_FALSE(final[i]) << "Expected false at index " << i;
            }
        }
    }

    // Test: NullExpr with IS_NULL
    {
        auto null_expr = std::make_shared<milvus::expr::NullExpr>(
            milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
            proto::plan::NullExpr_NullOp_IsNull);
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, null_expr);

        BitsetType final =
            ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        // None should match since we have no null values
        for (size_t i = 0; i < nb; i++) {
            ASSERT_FALSE(final[i]) << "Expected false at index " << i;
        }
    }

    // Test: NullExpr with IS_NOT_NULL
    {
        auto null_expr = std::make_shared<milvus::expr::NullExpr>(
            milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
            proto::plan::NullExpr_NullOp_IsNotNull);
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, null_expr);

        BitsetType final =
            ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        // All should match since we have no null values
        for (size_t i = 0; i < nb; i++) {
            ASSERT_TRUE(final[i]) << "Expected true at index " << i;
        }
    }

    // Test: AlwaysTrueExpr
    {
        auto always_true_expr =
            std::make_shared<milvus::expr::AlwaysTrueExpr>();
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, always_true_expr);

        BitsetType final =
            ExecuteQueryExpr(plan, segment.get(), nb, MAX_TIMESTAMP);
        // All should match
        for (size_t i = 0; i < nb; i++) {
            ASSERT_TRUE(final[i]) << "Expected true at index " << i;
        }
    }
}

TEST(NgramExpressionTest, JsonProjectionPreservesNonPatternFallbacks) {
    std::vector<std::string> json_raw_data = {R"({"name": "apple"})",
                                              R"({"name": "banana"})",
                                              R"({"name": "cherry"})",
                                              R"({"name": "date"})",
                                              R"({"name": "elderberry"})",
                                              R"({"name": "fig"})",
                                              R"({"name": "grape"})",
                                              R"({"name": "honeydew"})",
                                              R"({"name": "kiwi"})",
                                              R"({"name": "lemon"})"};
    const std::string json_path = "/name";
    auto schema = std::make_shared<Schema>();
    const auto json_fid = schema->AddDebugField("json", DataType::JSON);
    test::expr_index::RawFieldFiles raw_files;
    auto field = test::expr_index::JsonField(json_raw_data);
    auto segment = CreateSealedSegment(schema);
    auto raw_info = raw_files.Prepare(json_fid, {field});
    segment->LoadFieldData(raw_info);
    auto opened = test::expr_index::BuildIndex(
        json_fid, DataType::JSON, index::NGRAM_INDEX_TYPE, {field},
        {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4},
         {JSON_PATH, json_path}, {JSON_CAST_TYPE, "VARCHAR"}},
        DataType::NONE, false, true);
    test::expr_index::InstallIndex(*segment, json_fid, DataType::JSON,
                                   std::move(opened));
    const auto nb = json_raw_data.size();

    // Test: JSON Equal operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only first record should match (exact match for "apple")
        EXPECT_EQ(result.count(), 1);
        EXPECT_TRUE(result[0]);
    }

    // Test: JSON NotEqual operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::NotEqual,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All except first record should match
        EXPECT_EQ(result.count(), 9);
        EXPECT_FALSE(result[0]);
        for (size_t i = 1; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON GreaterThan operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("fig");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::GreaterThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Records with names > "fig": grape, honeydew, kiwi, lemon
        EXPECT_EQ(result.count(), 4);
        for (size_t i = 6; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON LessThan operation
    {
        proto::plan::GenericValue value;
        value.set_string_val("date");
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::LessThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Records with names < "date": apple, banana, cherry
        EXPECT_EQ(result.count(), 3);
        for (size_t i = 0; i < 3; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON TermFilterExpr (IN operation)
    {
        std::vector<proto::plan::GenericValue> values;
        proto::plan::GenericValue val1, val2, val3;
        val1.set_string_val("apple");
        val2.set_string_val("cherry");
        val3.set_string_val("grape");
        values.push_back(val1);
        values.push_back(val2);
        values.push_back(val3);

        auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            values);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           term_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only apple, cherry, grape should match
        EXPECT_EQ(result.count(), 3);
        EXPECT_TRUE(result[0]);  // apple
        EXPECT_TRUE(result[2]);  // cherry
        EXPECT_TRUE(result[6]);  // grape
    }

    // Test: JSON BinaryRangeFilterExpr
    {
        proto::plan::GenericValue lower_val;
        lower_val.set_string_val("cherry");
        proto::plan::GenericValue upper_val;
        upper_val.set_string_val("grape");

        auto binary_range_expr =
            std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
                milvus::expr::ColumnInfo(
                    json_fid, DataType::JSON, {"name"}, true),
                lower_val,
                upper_val,
                true,
                true);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           binary_range_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Strings between "cherry" and "grape" inclusive: cherry, date, elderberry, fig, grape
        EXPECT_EQ(result.count(), 5);
        for (size_t i = 2; i <= 6; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON NullExpr IS_NULL
    {
        auto null_expr = std::make_shared<milvus::expr::NullExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::NullExpr_NullOp_IsNull);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // None should match since all have non-null names
        EXPECT_EQ(result.count(), 0);
    }

    // Test: JSON NullExpr IS_NOT_NULL
    {
        auto null_expr = std::make_shared<milvus::expr::NullExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::NullExpr_NullOp_IsNotNull);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All should match since all have non-null names
        EXPECT_EQ(result.count(), 10);
        for (size_t i = 0; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON ExistsExpr
    {
        auto exists_expr = std::make_shared<milvus::expr::ExistsExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true));
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           exists_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All should match since all have the "name" field
        EXPECT_EQ(result.count(), 10);
        for (size_t i = 0; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }

    // Test: JSON LogicalBinaryExpr with AND
    {
        // Create Equal expression for "apple"
        proto::plan::GenericValue val1;
        val1.set_string_val("apple");
        auto expr1 = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            val1,
            std::vector<proto::plan::GenericValue>{});

        // Create NotEqual expression for "banana"
        proto::plan::GenericValue val2;
        val2.set_string_val("banana");
        auto expr2 = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::NotEqual,
            val2,
            std::vector<proto::plan::GenericValue>{});

        // Create LogicalBinaryExpr with AND
        auto logical_and_expr =
            std::make_shared<milvus::expr::LogicalBinaryExpr>(
                milvus::expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_and_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // Only apple should match (name == "apple" AND name != "banana")
        EXPECT_EQ(result.count(), 1);
        EXPECT_TRUE(result[0]);
    }

    // Test: JSON LogicalUnaryExpr with NOT
    {
        proto::plan::GenericValue value;
        value.set_string_val("apple");
        auto equal_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(json_fid, DataType::JSON, {"name"}, true),
            proto::plan::OpType::Equal,
            value,
            std::vector<proto::plan::GenericValue>{});

        // Create LogicalUnaryExpr with NOT
        auto logical_not_expr =
            std::make_shared<milvus::expr::LogicalUnaryExpr>(
                milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, equal_expr);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_not_expr);

        auto result = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), nb, MAX_TIMESTAMP);

        // All except apple should match (NOT (name == "apple"))
        EXPECT_EQ(result.count(), 9);
        EXPECT_FALSE(result[0]);
        for (size_t i = 1; i < nb; i++) {
            EXPECT_TRUE(result[i]);
        }
    }
}
