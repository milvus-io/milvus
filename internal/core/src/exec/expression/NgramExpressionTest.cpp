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
#include <boost/container/vector.hpp>
#include "common/RegexQuery.h"
#include "index/contracts/query/INgramReader.h"

#include <memory>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/LikeConjunctExpr.h"
#include "exec/expression/UnaryExpr.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "test_utils/GenExprProto.h"
#include "exec/QueryContext.h"
#include <algorithm>

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

// Regression for #44020: installing a candidate-only NGRAM reader must not
// route equality, membership, ranges, or logical composition into that reader.
TEST(NgramExpressionTest, NonPatternOperatorsUseTheirOrdinaryExpressionPaths) {
    const std::vector<std::string> data = {"apple",
                                           "banana",
                                           "cherry",
                                           "date",
                                           "elderberry",
                                           "fig",
                                           "grape",
                                           "honeydew",
                                           "kiwi",
                                           "lemon"};
    auto schema = std::make_shared<Schema>();
    const auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);
    test::expr_index::RawFieldFiles raw_files;
    auto field = test::expr_index::StringField(data);
    auto segment = CreateSealedSegment(schema);
    auto raw_info = raw_files.Prepare(field_id, {field});
    segment->LoadFieldData(raw_info);
    auto opened = test::expr_index::BuildIndex(
        field_id,
        DataType::VARCHAR,
        index::NGRAM_INDEX_TYPE,
        {field},
        {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}},
        DataType::NONE,
        false,
        true);
    test::expr_index::InstallIndex(
        *segment, field_id, DataType::VARCHAR, std::move(opened));
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
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           term_expr);

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
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           binary_range_expr);

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
        auto unary_range_expr2 =
            test::GenUnaryRangeExpr(proto::plan::OpType::NotEqual, "banana");
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
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_and_expr);

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
                milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, typed_expr);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_not_expr);

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
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_or_expr);

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
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);

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
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);

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
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           always_true_expr);

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
    auto opened = test::expr_index::BuildIndex(json_fid,
                                               DataType::JSON,
                                               index::NGRAM_INDEX_TYPE,
                                               {field},
                                               {{index::MIN_GRAM, 2},
                                                {index::MAX_GRAM, 4},
                                                {JSON_PATH, json_path},
                                                {JSON_CAST_TYPE, "VARCHAR"}},
                                               DataType::NONE,
                                               false,
                                               true);
    test::expr_index::InstallIndex(
        *segment, json_fid, DataType::JSON, std::move(opened));
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

namespace {
void
test_ngram_with_data(const boost::container::vector<std::string>& input,
                     const std::string& literal,
                     proto::plan::OpType op,
                     const std::vector<bool>& expected,
                     bool forward_to_br) {
    std::vector<std::string> data(input.begin(), input.end());
    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("ngram", DataType::VARCHAR);
    auto field = test::expr_index::StringField(data);
    test::expr_index::RawFieldFiles raw_files;
    auto segment = CreateSealedSegment(schema);
    segment->LoadFieldData(raw_files.Prepare(field_id, {field}));
    auto opened = test::expr_index::BuildIndex(
        field_id,
        DataType::VARCHAR,
        index::NGRAM_INDEX_TYPE,
        {field},
        {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}});
    const auto* ngram =
        dynamic_cast<const index::INgramReader*>(opened.reader.get());
    ASSERT_NE(ngram, nullptr);
    index::PatternOp pattern_op = index::PatternOp::Match;
    switch (op) {
        case proto::plan::InnerMatch:
            pattern_op = index::PatternOp::InnerMatch;
            break;
        case proto::plan::PrefixMatch:
            pattern_op = index::PatternOp::PrefixMatch;
            break;
        case proto::plan::PostfixMatch:
            pattern_op = index::PatternOp::PostfixMatch;
            break;
        case proto::plan::RegexMatch:
            pattern_op = index::PatternOp::RegexMatch;
            break;
        default:
            break;
    }
    EXPECT_EQ(ngram->CanHandle(literal, pattern_op), !forward_to_br);
    test::expr_index::InstallIndex(
        *segment, field_id, DataType::VARCHAR, std::move(opened));
    proto::plan::GenericValue value;
    value.set_string_val(literal);
    auto expression = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_id, DataType::VARCHAR),
        op,
        value,
        std::vector<proto::plan::GenericValue>{});
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expression);
    auto result =
        ExecuteQueryExpr(plan, segment.get(), data.size(), MAX_TIMESTAMP);
    ASSERT_EQ(result.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(result[i], expected[i]) << i;
    }
}
}  // namespace

TEST(NgramExpressionTest, SingleMultiByteCharLiteralFallsBack) {
    boost::container::vector<std::string> test_data = {
        "订单（已取消）",
        "中文测试",
        "hello world",
        "abc）",
    };

    struct TestCase {
        std::string literal;
        proto::plan::OpType op_type;
        std::string like_pattern;
    };
    std::vector<TestCase> test_cases = {
        {"）", proto::plan::OpType::InnerMatch, "%）%"},
        {"订", proto::plan::OpType::PrefixMatch, "订%"},
        {"）", proto::plan::OpType::PostfixMatch, "%）"},
        {"%abc%）%", proto::plan::OpType::Match, "%abc%）%"},
        // 2-byte and 4-byte characters take the same path
        {"é", proto::plan::OpType::InnerMatch, "%é%"},
        {"😀", proto::plan::OpType::InnerMatch, "%😀%"},
    };

    for (const auto& test_case : test_cases) {
        ASSERT_GE(test_case.literal.size(), 2)
            << "literal must be >= min_gram bytes for this test to matter";
        PatternMatchTranslator translator;
        RegexMatcher re2_matcher(translator(test_case.like_pattern));
        std::vector<bool> expected_results;
        for (const auto& data : test_data) {
            expected_results.push_back(re2_matcher(data));
        }
        test_ngram_with_data(test_data,
                             test_case.literal,
                             test_case.op_type,
                             expected_results,
                             /*forward_to_br=*/true);
    }
}

// A literal with an interior NUL must cross the ngram FFI intact: the entry
// points take (pointer, length), so rust checks and queries the same literal
// the C++ gate saw. Both the candidate lookup and the raw post-filter must
// evaluate the full literal, including its interior NUL.
TEST(NgramExpressionTest, EmbeddedNulLiteralIsHandledByNgram) {
    const std::string nul_row("xab\0cy", 6);
    const std::string nul_short("ab\0c", 4);
    boost::container::vector<std::string> test_data = {
        nul_row, "xabzz", "hello", nul_short};

    struct TestCase {
        std::string literal;
        proto::plan::OpType op_type;
        std::vector<bool> expected;
    };
    std::vector<TestCase> test_cases = {
        {nul_short,
         proto::plan::OpType::InnerMatch,
         {true, false, false, true}},
        {nul_short,
         proto::plan::OpType::PrefixMatch,
         {false, false, false, true}},
        {std::string("b\0c", 3),
         proto::plan::OpType::PostfixMatch,
         {false, false, false, true}},
        {"%xa%" + std::string("\0cy", 3) + "%",
         proto::plan::OpType::Match,
         {true, false, false, false}},
    };
    for (const auto& test_case : test_cases) {
        test_ngram_with_data(test_data,
                             test_case.literal,
                             test_case.op_type,
                             test_case.expected,
                             /*forward_to_br=*/false);
    }
}

// Differential check of NGRAM regex candidate planning against RE2: AND the
// literals within a branch, union the OR branches, and scan (CanHandle false)
// when a branch has no usable literal, the pattern matches empty or the bounded
// condition overflows. Phase 2 always rechecks with RE2, so results must equal
// a plain RE2 PartialMatch scan either way.
TEST(NgramExpressionTest, RegexAlternationMatchesRe2) {
    std::string long_han;
    for (int i = 0; i < 30; ++i) {
        long_han += "\xe4\xb8\xad";  // 中, cut inside a character at 64 bytes
    }
    boost::container::vector<std::string> test_data = {
        "the cat sat",      "hotdog stand",
        "catalog",          "dog",
        "a bird",           "ERROR: read timeout",
        "WARN timeout",     "INFO timeout",
        "foo|bar",          "foo bar",
        "foo0123456789bar", "foo01bar",
        "abcfghi",          "abcdehi",
        "Cat Dog",          "",
        "你好世界",         "x" + long_han + "y",
        "hq--zhw",          "iq ziw",
        "Zq--zZw",
    };

    // Distinct leading bytes keep RE2 from factoring the branches, so 60
    // branches of two literals each exceed the 127-node condition budget.
    auto branches = [](size_t count) {
        const std::string heads =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::string pattern;
        for (size_t i = 0; i < count; ++i) {
            const auto head = heads.substr(i, 1);
            pattern += (i == 0 ? "" : "|") + head + "q.*z" + head + "w";
        }
        return pattern;
    };

    struct TestCase {
        std::string pattern;
        bool ngram;  // whether NGRAM planning prunes (CanHandle)
    };
    const std::vector<TestCase> test_cases = {
        {"cat|dog", true},
        {"^(cat|dog)$", true},
        {"(cat|dog)", true},
        {"(ERROR|WARN).*timeout", true},
        {"(?:cat|dog)(?:alog| stand)", true},
        {"abc(de|fg)hi", true},
        {"foo\\|bar", true},
        {"\\Qfoo|bar\\E", true},
        {"[|]bar|bird", true},
        {"hello|你好", true},
        {long_han + "|bird", true},
        {branches(8), true},
        // The removed hand-written extractor required the literal "10,20"
        // here and dropped the first matching row.
        {"foo.{10,20}bar", true},
        {"cat|a", false},
        {"cat|", false},
        {"(?:cat|dog)?", false},
        {"(?i)cat|dog", false},
        {"cat|(?i:dog)", false},
        {"x(?:abc|d)y", false},
        {"\\ba[ab]b(\\d)", false},
        {branches(60), false},
    };
    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(test_case.pattern);
        PartialRegexMatcher matcher(test_case.pattern);
        std::vector<bool> expected;
        for (const auto& row : test_data) {
            expected.push_back(matcher(row));
        }
        test_ngram_with_data(test_data,
                             test_case.pattern,
                             proto::plan::OpType::RegexMatch,
                             expected,
                             /*forward_to_br=*/!test_case.ngram);
    }
}

TEST(NgramExpressionTest, NativeOffsetsDoNotFetchNgram) {
    const std::vector<std::string> rows{"abcdef", "xyz", "ab123"};
    auto schema = std::make_shared<Schema>();
    const auto field_id = schema->AddDebugField("text", DataType::VARCHAR);
    test::expr_index::RawFieldFiles raw_files;
    auto field = test::expr_index::StringField(rows);
    auto segment = CreateSealedSegment(schema);
    segment->LoadFieldData(raw_files.Prepare(field_id, {field}));
    auto opened = test::expr_index::BuildIndex(
        field_id,
        DataType::VARCHAR,
        index::NGRAM_INDEX_TYPE,
        {field},
        {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}});
    OpContext untouched;
    OpContext* observed_pin = &untouched;
    test::expr_index::InstallIndex(*segment,
                                   field_id,
                                   DataType::VARCHAR,
                                   std::move(opened),
                                   DataType::NONE,
                                   &observed_pin);
    proto::plan::GenericValue literal;
    literal.set_string_val("ab%");
    auto filter = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_id, DataType::VARCHAR),
        proto::plan::OpType::Match,
        literal);
    exec::PhyUnaryRangeFilterExpr physical({},
                                           filter,
                                           "ngram-offset",
                                           nullptr,
                                           segment.get(),
                                           rows.size(),
                                           rows.size(),
                                           0);
    exec::QueryContext query_context(
        "ngram-offset", segment.get(), rows.size(), MAX_TIMESTAMP);
    exec::ExecContext exec_context(&query_context);
    exec::OffsetVector offsets{2, 1, 0, 2};
    exec::EvalCtx context(&exec_context, &offsets);
    VectorPtr result;
    physical.Eval(context, result);
    EXPECT_EQ(physical.GetExecPath(), exec::ExprExecPath::RawData);
    EXPECT_EQ(observed_pin, &untouched);
    auto column = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(column, nullptr);
    ASSERT_EQ(column->size(), offsets.size());
    TargetBitmapView bits(column->GetRawData(), column->size());
    TargetBitmapView valid(column->GetValidRawData(), column->size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_TRUE(valid[i]);
        EXPECT_EQ(bits[i], offsets[i] != 1);
    }
}

namespace {

class NgramRefinementSegment final : public ChunkedSegmentSealedImpl {
 public:
    explicit NgramRefinementSegment(const SchemaPtr& schema)
        : ChunkedSegmentSealedImpl(
              schema, empty_index_meta, SegcoreConfig::default_config(), 0) {
    }

    mutable size_t raw_view_reads{0};

 protected:
    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    chunk_string_view_impl(
        OpContext* context,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        ++raw_view_reads;
        return ChunkedSegmentSealedImpl::chunk_string_view_impl(
            context, field_id, chunk_id, offset_len);
    }
};

struct NgramPhase2Fixture {
    NgramPhase2Fixture() {
        auto schema = std::make_shared<Schema>();
        field_id = schema->AddDebugField("text", DataType::VARCHAR, true);
        const uint8_t validity[] = {0x7F};
        auto field = test::expr_index::StringField(rows, true, validity);
        segment = std::make_unique<NgramRefinementSegment>(schema);
        segment->LoadFieldData(raw_files.Prepare(field_id, {field}));
        auto opened = test::expr_index::BuildIndex(
            field_id,
            DataType::VARCHAR,
            index::NGRAM_INDEX_TYPE,
            {field},
            {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}});
        test::expr_index::InstallIndex(
            *segment, field_id, DataType::VARCHAR, std::move(opened));
    }

    exec::ExprPtr
    Unary(const std::string& literal,
          proto::plan::OpType op = proto::plan::InnerMatch) const {
        proto::plan::GenericValue value;
        value.set_string_val(literal);
        auto typed = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_id, DataType::VARCHAR, {}, true), op, value);
        return std::make_shared<exec::PhyUnaryRangeFilterExpr>(
            std::vector<exec::ExprPtr>{},
            typed,
            "ngram-phase2",
            nullptr,
            segment.get(),
            rows.size(),
            rows.size(),
            0);
    }

    void
    Expect(const exec::ExprPtr& expression,
           const std::vector<size_t>& hits,
           bool fused = false) {
        exec::QueryContext query_context(
            "ngram-phase2", segment.get(), rows.size(), MAX_TIMESTAMP);
        exec::ExecContext exec_context(&query_context);
        exec::EvalCtx context(&exec_context);
        VectorPtr result;
        expression->Eval(context, result);
        if (auto unary =
                std::dynamic_pointer_cast<exec::PhyUnaryRangeFilterExpr>(
                    expression)) {
            EXPECT_EQ(unary->GetExecPath(), exec::ExprExecPath::ScalarIndex);
        }
        auto column = std::dynamic_pointer_cast<ColumnVector>(result);
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), rows.size());
        TargetBitmapView bits(column->GetRawData(), column->size());
        TargetBitmapView valid(column->GetValidRawData(), column->size());
        for (size_t row = 0; row < rows.size(); ++row) {
            EXPECT_EQ(valid[row], fused || row != 7) << row;
            EXPECT_EQ(bits[row] && valid[row],
                      std::find(hits.begin(), hits.end(), row) != hits.end())
                << row;
        }
    }

    const std::vector<std::string> rows{"ab",
                                        "xabx",
                                        "中文",
                                        "x中文y",
                                        "abcde",
                                        "abcd--bcde",
                                        "ab中文",
                                        "unused"};
    FieldId field_id{0};
    test::expr_index::RawFieldFiles raw_files;
    std::unique_ptr<NgramRefinementSegment> segment;
};

}  // namespace

TEST(NgramExpressionTest, InnerMatchSkipsOnlyExactCandidateRefinement) {
    NgramPhase2Fixture fixture;
    fixture.Expect(fixture.Unary("ab"), {0, 1, 4, 5, 6});
    EXPECT_EQ(fixture.segment->raw_view_reads, 0);
    fixture.Expect(fixture.Unary("中文"), {2, 3, 6});
    EXPECT_EQ(fixture.segment->raw_view_reads, 0);
    fixture.Expect(fixture.Unary("abcde"), {4});
    EXPECT_GT(fixture.segment->raw_view_reads, 0);
    fixture.segment->raw_view_reads = 0;
    fixture.Expect(fixture.Unary("ab", proto::plan::PrefixMatch), {0, 4, 5, 6});
    EXPECT_GT(fixture.segment->raw_view_reads, 0);
}

TEST(NgramExpressionTest, FusedInnerMatchesSkipExactCandidateRefinement) {
    NgramPhase2Fixture fixture;
    auto fused = std::make_shared<exec::PhyConjunctFilterExpr>(
        std::vector<exec::ExprPtr>{fixture.Unary("ab"), fixture.Unary("中文")},
        true,
        nullptr);
    fused->SetLikeIndices({0, 1});
    fused->Reorder({0, 1, 2});
    fixture.Expect(fused, {6}, true);
    ASSERT_EQ(fused->GetInputsRef().size(), 3);
    EXPECT_NE(std::dynamic_pointer_cast<exec::PhyLikeConjunctExpr>(
                  fused->GetInputsRef().back()),
              nullptr);
    EXPECT_EQ(fixture.segment->raw_view_reads, 0);
}
