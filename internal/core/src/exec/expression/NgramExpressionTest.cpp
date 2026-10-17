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
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <boost/container/vector.hpp>
#include "common/RegexQuery.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/INgramReader.h"

#include <memory>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/LikeConjunctExpr.h"
#include "exec/expression/LogicalUnaryExpr.h"
#include "exec/expression/UnaryExpr.h"
#include "index/contracts/query/INullReader.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "test_utils/GenExprProto.h"
#include "folly/ScopeGuard.h"
#include "exec/QueryContext.h"
#include "index/Families.h"
#include "query/Plan.h"
#include "rescores/BoostScoreRunner.h"
#include "rescores/Scorer.h"
#include "test_utils/DataGen.h"
#include "test_utils/index_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

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

TEST(NgramExpressionTest, IterativeAndBoostOffsetsDoNotFetchNgram) {
    constexpr int dim = 8;
    constexpr int64_t rows = 64;
    auto schema = std::make_shared<Schema>();
    const auto vector_id = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    const auto pk_id = schema->AddDebugField("pk", DataType::INT64);
    const auto text_id = schema->AddDebugField("text", DataType::VARCHAR);
    schema->set_primary_field_id(pk_id);
    auto raw = DataGen(schema, rows, 42);
    std::vector<std::string> texts;
    for (int64_t row = 0; row < rows; ++row) {
        texts.push_back(row % 2 == 0 ? "abcdef" : "xyz");
    }
    for (auto& field : *raw.raw_->mutable_fields_data()) {
        if (field.field_id() == text_id.get()) {
            auto* values = field.mutable_scalars()->mutable_string_data();
            values->clear_data();
            for (const auto& text : texts) {
                values->add_data(text);
            }
        }
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw);
    auto vectors = raw.get_col<float>(vector_id);
    auto vector_reader = GenVecIndexing(
        rows, dim, vectors.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo vector_info{};
    vector_info.field_id = vector_id.get();
    SetTestIndexMetadata(
        vector_info, *vector_reader, index::families::kVectorMem);
    vector_info.index_params = GenIndexParams(vector_reader.get());
    vector_info.index_params["metric_type"] = knowhere::metric::L2;
    vector_info.cache_index =
        CreateTestCacheIndex("ngram-offset-vector", std::move(vector_reader));
    segment->LoadIndex(vector_info);

    auto field = test::expr_index::StringField(texts);
    auto opened = test::expr_index::BuildIndex(
        text_id,
        DataType::VARCHAR,
        index::NGRAM_INDEX_TYPE,
        {field},
        {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}});
    OpContext untouched;
    OpContext* observed_pin = &untouched;
    test::expr_index::InstallIndex(*segment,
                                   text_id,
                                   DataType::VARCHAR,
                                   std::move(opened),
                                   DataType::NONE,
                                   &observed_pin);
    const bool saved_prefetch = ENABLE_DRIVER_PREFETCH.load();
    auto restore_prefetch = folly::makeGuard(
        [saved_prefetch] { ENABLE_DRIVER_PREFETCH = saved_prefetch; });
    ScopedSchemaHandle handle(*schema);
    for (const bool prefetch : {false, true}) {
        SCOPED_TRACE(prefetch);
        ENABLE_DRIVER_PREFETCH = prefetch;
        for (const std::string filter :
             {"text like \"ab%\"",
              "text like \"ab%\" AND text like \"%bc%\""}) {
            auto bytes = handle.ParseSearch(
                filter, "vec", 4, "L2", "{\"ef\": 64}", -1, "iterative_filter");
            auto plan =
                CreateSearchPlanByExpr(schema, bytes.data(), bytes.size());
            auto placeholder = CreatePlaceholderGroup(1, dim, 1024);
            auto group = ParsePlaceholderGroup(plan.get(),
                                               placeholder.SerializeAsString());
            auto result =
                segment->Search(plan.get(), group.get(), MAX_TIMESTAMP);
            ASSERT_EQ(result->seg_offsets_.size(), 4);
            for (const auto offset : result->seg_offsets_) {
                ASSERT_GE(offset, 0);
                EXPECT_EQ(offset % 2, 0);
            }
            EXPECT_EQ(observed_pin, &untouched);
        }
    }

    // Boost's prepared native ExprSet takes the same offset-input path.
    proto::plan::GenericValue literal;
    literal.set_string_val("ab%");
    auto filter = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(text_id, DataType::VARCHAR),
        proto::plan::OpType::Match,
        literal);
    auto scorer = std::make_shared<rescores::WeightScorer>(filter, 2.0F);
    exec::QueryContext query_context(
        "ngram-offset-boost", segment.get(), rows, MAX_TIMESTAMP);
    OpContext op_context;
    query_context.set_op_context(&op_context);
    exec::ExecContext exec_context(&query_context);
    std::unique_ptr<exec::ExprSet> prepared;
    EXPECT_FALSE(
        rescores::ComputeNonNativeFilterBitset(&exec_context, scorer, &prepared)
            .has_value());
    ASSERT_NE(prepared, nullptr);
    FixedVector<int32_t> offsets{0, 1, 2};
    std::vector<std::optional<float>> scores(offsets.size());
    rescores::ComputeScorerScores(&exec_context,
                                  &op_context,
                                  segment.get(),
                                  scorer,
                                  offsets,
                                  scores,
                                  nullptr,
                                  prepared.get());
    ASSERT_TRUE(scores[0].has_value());
    EXPECT_EQ(*scores[0], 2.0F);
    EXPECT_FALSE(scores[1].has_value());
    ASSERT_TRUE(scores[2].has_value());
    EXPECT_EQ(*scores[2], 2.0F);
    EXPECT_EQ(observed_pin, &untouched);
}

TEST(NgramExpressionTest, PrefetchedUnaryJoinsBeforeBindingOffsetMode) {
    class ObservedPrefetchUnary final : public exec::PhyUnaryRangeFilterExpr {
     public:
        using PhyUnaryRangeFilterExpr::PhyUnaryRangeFilterExpr;

        void
        WaitPrefetch() override {
            offset_mode_before_wait = has_offset_input_;
            exec::SegmentExpr::WaitPrefetch();
        }

        bool offset_mode_before_wait = true;
    };

    const std::vector<std::string> rows{"abcdef", "xyz", "ab123"};
    auto schema = std::make_shared<Schema>();
    const auto field_id = schema->AddDebugField("text", DataType::VARCHAR);
    test::expr_index::RawFieldFiles raw_files;
    auto field = test::expr_index::StringField(rows);
    auto segment = CreateSealedSegment(schema);
    auto raw_info = raw_files.Prepare(field_id, {field});
    segment->LoadFieldData(raw_info);
    auto opened = test::expr_index::BuildIndex(
        field_id,
        DataType::VARCHAR,
        index::NGRAM_INDEX_TYPE,
        {field},
        {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}});
    test::expr_index::InstallIndex(
        *segment, field_id, DataType::VARCHAR, std::move(opened));
    proto::plan::GenericValue literal;
    literal.set_string_val("ab%");
    auto filter = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_id, DataType::VARCHAR),
        proto::plan::OpType::Match,
        literal);
    exec::QueryContext query_context(
        "ngram-prefetched-unary", segment.get(), rows.size(), MAX_TIMESTAMP);
    exec::ExecContext exec_context(&query_context);
    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(1);

    for (const bool with_offsets : {false, true}) {
        SCOPED_TRACE(with_offsets);
        auto physical = std::make_shared<ObservedPrefetchUnary>(
            std::vector<exec::ExprPtr>{},
            filter,
            "ObservedPrefetchUnary",
            nullptr,
            segment.get(),
            rows.size(),
            rows.size(),
            0);
        physical->PrefetchAsync(pool);
        exec::OffsetVector offsets{2, 1, 0, 2};
        exec::EvalCtx eval_context(&exec_context,
                                   with_offsets ? &offsets : nullptr);
        VectorPtr result;
        physical->Eval(eval_context, result);
        EXPECT_FALSE(physical->offset_mode_before_wait);
        EXPECT_EQ(physical->GetExecPath(), exec::ExprExecPath::ScalarIndex);
        auto column = std::dynamic_pointer_cast<ColumnVector>(result);
        ASSERT_NE(column, nullptr);
        const auto count = with_offsets ? offsets.size() : rows.size();
        ASSERT_EQ(column->size(), count);
        TargetBitmapView bits(column->GetRawData(), count);
        TargetBitmapView valid(column->GetValidRawData(), count);
        for (size_t i = 0; i < count; ++i) {
            const auto row = with_offsets ? offsets[i] : i;
            EXPECT_EQ(bits[i], row != 1);
            EXPECT_TRUE(valid[i]);
        }
    }
}

namespace {

// Count the raw views used by the real phase-2 implementation. These tests
// construct physical expressions directly, without a frozen column snapshot,
// so ProcessDataChunkForRange reaches this normal virtual segment boundary.
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

struct NgramQueryCounts {
    size_t candidates{0};
    size_t exactness_checks{0};
};

// Observe the production reader without substituting candidate or NULL results.
class ObservedNgramReader final : public index::IIndexReaderBase,
                                  public index::INgramReader,
                                  public index::INullReader {
 public:
    ObservedNgramReader(index::IIndexReaderBasePtr reader,
                        std::shared_ptr<NgramQueryCounts> counts)
        : ObservedNgramReader(
              index::JsonResolvedReader::Owned(std::move(reader)),
              std::move(counts)) {
    }

    ObservedNgramReader(index::JsonResolvedReader reader,
                        std::shared_ptr<NgramQueryCounts> counts)
        : reader_(std::move(reader)), counts_(std::move(counts)) {
    }

    index::ReaderCaps
    Caps() const override {
        return reader_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return reader_->CoordDomain();
    }
    int64_t
    Count() const override {
        return reader_->Count();
    }
    DataType
    ValueType() const override {
        return reader_->ValueType();
    }
    int64_t
    MemoryUsage() const override {
        return reader_->MemoryUsage();
    }
    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return reader_->CellByteSize();
    }
    bool
    CanHandle(std::string_view literal, index::PatternOp op) const override {
        return Ngram().CanHandle(literal, op);
    }
    bool
    CandidatesAreExact(std::string_view literal,
                       index::PatternOp op) const override {
        ++counts_->exactness_checks;
        return Ngram().CandidatesAreExact(literal, op);
    }
    void
    Candidates(std::string_view literal,
               index::PatternOp op,
               TargetBitmap& candidates) const override {
        ++counts_->candidates;
        Ngram().Candidates(literal, op, candidates);
    }
    TargetBitmap
    IsNull() const override {
        return dynamic_cast<const index::INullReader&>(*reader_.get()).IsNull();
    }
    TargetBitmap
    IsNotNull() const override {
        return dynamic_cast<const index::INullReader&>(*reader_.get())
            .IsNotNull();
    }

 private:
    const index::INgramReader&
    Ngram() const {
        return dynamic_cast<const index::INgramReader&>(*reader_.get());
    }

    // Retain an owned or borrowed resolved view exactly as the router returned
    // it. A borrowed view still relies on the enclosing JSON router cache pin.
    index::JsonResolvedReader reader_;
    std::shared_ptr<NgramQueryCounts> counts_;
};

class ObservedJsonNgramReader final : public index::IIndexReaderBase,
                                      public index::IJsonIndexReader {
 public:
    ObservedJsonNgramReader(index::IIndexReaderBasePtr reader,
                            std::shared_ptr<NgramQueryCounts> counts)
        : reader_(std::move(reader)), counts_(std::move(counts)) {
    }

    index::ReaderCaps
    Caps() const override {
        return reader_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return reader_->CoordDomain();
    }
    int64_t
    Count() const override {
        return reader_->Count();
    }
    DataType
    ValueType() const override {
        return reader_->ValueType();
    }
    int64_t
    MemoryUsage() const override {
        return reader_->MemoryUsage();
    }
    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return reader_->CellByteSize();
    }
    index::JsonResolvedReader
    Resolve(std::string_view path, JsonCastType cast_type) const override {
        auto resolved = Json().Resolve(path, cast_type);
        if (!resolved) {
            return {};
        }
        return index::JsonResolvedReader::Owned(
            std::make_unique<ObservedNgramReader>(std::move(resolved),
                                                  counts_));
    }
    TargetBitmap
    Exists(std::string_view path) const override {
        return Json().Exists(path);
    }
    std::vector<JsonCastType>
    CastTypesOf(std::string_view path) const override {
        return Json().CastTypesOf(path);
    }

 private:
    const index::IJsonIndexReader&
    Json() const {
        return dynamic_cast<const index::IJsonIndexReader&>(*reader_);
    }

    index::IIndexReaderBasePtr reader_;
    std::shared_ptr<NgramQueryCounts> counts_;
};

struct NgramPhase2Fixture {
    explicit NgramPhase2Fixture(bool nullable,
                                DataType field_type = DataType::VARCHAR)
        : nullable(nullable), field_type(field_type) {
        schema = std::make_shared<Schema>();
        field_id = schema->AddDebugField("text", field_type, nullable);
        std::vector<uint8_t> validity((rows.size() + 7) / 8, 0xFF);
        if (nullable) {
            validity[(rows.size() - 1) / 8] &=
                static_cast<uint8_t>(~(1U << ((rows.size() - 1) % 8)));
        }
        FieldDataPtr field;
        Config params{{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}};
        if (field_type == DataType::JSON) {
            std::vector<std::string> json;
            json.reserve(rows.size());
            for (const auto& row : rows) {
                json.push_back(R"({"name":")" + row + R"("})");
            }
            field =
                test::expr_index::JsonField(json, nullable, validity.data());
            params[JSON_PATH] = "/name";
            params[JSON_CAST_TYPE] = "VARCHAR";
        } else {
            field =
                test::expr_index::StringField(rows, nullable, validity.data());
        }
        segment = std::make_unique<NgramRefinementSegment>(schema);
        raw_segment = CreateSealedSegment(schema);
        const auto raw_info = raw_files.Prepare(field_id, {field});
        segment->LoadFieldData(raw_info);
        raw_segment->LoadFieldData(raw_info);
        auto opened = test::expr_index::BuildIndex(
            field_id, field_type, index::NGRAM_INDEX_TYPE, {field}, params);
        if (opened.reader->Caps().json_paths) {
            opened.reader = std::make_unique<ObservedJsonNgramReader>(
                std::move(opened.reader), counts);
        } else {
            opened.reader = std::make_unique<ObservedNgramReader>(
                std::move(opened.reader), counts);
        }
        test::expr_index::InstallIndex(
            *segment, field_id, field_type, std::move(opened));
    }

    exec::ExprPtr
    Unary(SegmentInternalInterface* source,
          const std::string& literal,
          proto::plan::OpType op = proto::plan::InnerMatch,
          bool negate = false) const {
        proto::plan::GenericValue value;
        value.set_string_val(literal);
        auto typed = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_id,
                             field_type,
                             field_type == DataType::JSON
                                 ? std::vector<std::string>{"name"}
                                 : std::vector<std::string>{},
                             nullable),
            op,
            value);
        auto physical = std::make_shared<exec::PhyUnaryRangeFilterExpr>(
            std::vector<exec::ExprPtr>{},
            typed,
            "PhyUnaryRangeFilterExpr",
            nullptr,
            source,
            rows.size(),
            rows.size(),
            0);
        if (!negate) {
            return physical;
        }
        auto logical = std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, typed);
        return std::make_shared<exec::PhyLogicalUnaryExpr>(
            std::vector<exec::ExprPtr>{physical},
            logical,
            "PhyLogicalUnaryExpr",
            nullptr);
    }

    std::shared_ptr<exec::PhyConjunctFilterExpr>
    And(SegmentInternalInterface* source,
        const std::string& first,
        const std::string& second,
        bool fuse) const {
        auto result = std::make_shared<exec::PhyConjunctFilterExpr>(
            std::vector<exec::ExprPtr>{Unary(source, first),
                                       Unary(source, second)},
            true,
            nullptr);
        if (fuse) {
            result->SetLikeIndices({0, 1});
            result->Reorder({0, 1, 2});
        }
        return result;
    }

    ColumnVectorPtr
    Eval(const exec::ExprPtr& expression,
         SegmentInternalInterface* source,
         exec::OffsetVector* offsets = nullptr) const {
        exec::QueryContext query_context(
            "ngram-phase2", source, rows.size(), MAX_TIMESTAMP);
        exec::ExecContext exec_context(&query_context);
        exec::EvalCtx eval_context(&exec_context, offsets);
        VectorPtr result;
        expression->Eval(eval_context, result);
        return std::dynamic_pointer_cast<ColumnVector>(result);
    }

    static void
    ExpectSame(const ColumnVectorPtr& actual, const ColumnVectorPtr& raw) {
        ASSERT_NE(actual, nullptr);
        ASSERT_NE(raw, nullptr);
        ASSERT_EQ(actual->size(), raw->size());
        TargetBitmapView actual_bits(actual->GetRawData(), actual->size());
        TargetBitmapView actual_valid(actual->GetValidRawData(),
                                      actual->size());
        TargetBitmapView raw_bits(raw->GetRawData(), raw->size());
        TargetBitmapView raw_valid(raw->GetValidRawData(), raw->size());
        for (size_t row = 0; row < actual->size(); ++row) {
            EXPECT_EQ(actual_valid[row], raw_valid[row]) << row;
            if (raw_valid[row]) {
                EXPECT_EQ(actual_bits[row], raw_bits[row]) << row;
            }
            EXPECT_EQ(actual_bits[row] && actual_valid[row],
                      raw_bits[row] && raw_valid[row])
                << row;
        }
    }

    const bool nullable;
    const DataType field_type;
    const std::vector<std::string> rows{"a",
                                        "ab",
                                        "abcd",
                                        "abcde",
                                        "xabcdey",
                                        "abcd--bcde",
                                        "ABCD",
                                        "xyz",
                                        "中",
                                        "中文",
                                        "中文测试",
                                        "中文测试数",
                                        "x中文测试数y",
                                        "中文测试-文测试数",
                                        "ab中文",
                                        "abcd中文测试",
                                        "",
                                        "abcde中文测试数"};
    SchemaPtr schema;
    FieldId field_id{0};
    test::expr_index::RawFieldFiles raw_files;
    std::unique_ptr<NgramRefinementSegment> segment;
    SegmentSealedUPtr raw_segment;
    std::shared_ptr<NgramQueryCounts> counts =
        std::make_shared<NgramQueryCounts>();
};

}  // namespace

TEST(NgramExpressionTest, InnerMatchSkipsRefinementOnlyForCompleteGrams) {
    for (const auto field_type : {DataType::VARCHAR, DataType::JSON}) {
        SCOPED_TRACE(static_cast<int>(field_type));
        NgramPhase2Fixture fixture(true, field_type);
        for (const std::string literal : {"a",
                                          "ab",
                                          "abcd",
                                          "abcde",
                                          "中",
                                          "中文",
                                          "中文测试",
                                          "中文测试数"}) {
            SCOPED_TRACE(literal);
            const auto length = Utf8CharCount(literal.data(), literal.size());
            for (const bool negate : {false, true}) {
                SCOPED_TRACE(negate);
                *fixture.counts = {};
                fixture.segment->raw_view_reads = 0;
                const auto actual =
                    fixture.Eval(fixture.Unary(fixture.segment.get(),
                                               literal,
                                               proto::plan::InnerMatch,
                                               negate),
                                 fixture.segment.get());
                const auto raw =
                    fixture.Eval(fixture.Unary(fixture.raw_segment.get(),
                                               literal,
                                               proto::plan::InnerMatch,
                                               negate),
                                 fixture.raw_segment.get());
                NgramPhase2Fixture::ExpectSame(actual, raw);
                ASSERT_NE(actual, nullptr);
                TargetBitmapView valid(actual->GetValidRawData(),
                                       actual->size());
                EXPECT_FALSE(valid[fixture.rows.size() - 1]);
                EXPECT_EQ(fixture.counts->candidates, length >= 2 ? 1 : 0);
                if (length >= 2 && length <= 4) {
                    EXPECT_GT(fixture.counts->exactness_checks, 0);
                    EXPECT_EQ(fixture.segment->raw_view_reads, 0);
                } else if (length > 4) {
                    EXPECT_GT(fixture.counts->exactness_checks, 0);
                    EXPECT_GT(fixture.segment->raw_view_reads, 0);
                } else {
                    EXPECT_EQ(fixture.counts->exactness_checks, 0);
                }
            }
        }
    }
}

TEST(NgramExpressionTest, AnchoredAndWildcardMatchesStillRefineCompleteGrams) {
    NgramPhase2Fixture fixture(true);
    for (const auto op : {proto::plan::PrefixMatch,
                          proto::plan::PostfixMatch,
                          proto::plan::Match,
                          proto::plan::RegexMatch}) {
        SCOPED_TRACE(static_cast<int>(op));
        const std::string literal = op == proto::plan::Match ? "%ab%" : "ab";
        *fixture.counts = {};
        fixture.segment->raw_view_reads = 0;
        const auto actual =
            fixture.Eval(fixture.Unary(fixture.segment.get(), literal, op),
                         fixture.segment.get());
        const auto raw =
            fixture.Eval(fixture.Unary(fixture.raw_segment.get(), literal, op),
                         fixture.raw_segment.get());
        NgramPhase2Fixture::ExpectSame(actual, raw);
        EXPECT_EQ(fixture.counts->candidates, 1);
        EXPECT_GT(fixture.segment->raw_view_reads, 0);
    }
}

TEST(NgramExpressionTest, FusedInnerMatchesShareRefinementEligibility) {
    NgramPhase2Fixture fixture(false);
    for (const auto& literals :
         {std::pair<std::string, std::string>{"ab", "中文"},
          {"abcde", "中文"},
          {"abcd", "中文测试数"}}) {
        SCOPED_TRACE(literals.first + " AND " + literals.second);
        *fixture.counts = {};
        fixture.segment->raw_view_reads = 0;
        auto fused = fixture.And(
            fixture.segment.get(), literals.first, literals.second, true);
        const auto actual = fixture.Eval(fused, fixture.segment.get());
        const auto raw = fixture.Eval(fixture.And(fixture.raw_segment.get(),
                                                  literals.first,
                                                  literals.second,
                                                  false),
                                      fixture.raw_segment.get());
        NgramPhase2Fixture::ExpectSame(actual, raw);
        ASSERT_EQ(fused->GetInputsRef().size(), 3);
        EXPECT_NE(std::dynamic_pointer_cast<exec::PhyLikeConjunctExpr>(
                      fused->GetInputsRef().back()),
                  nullptr);
        EXPECT_EQ(fixture.counts->candidates, 2);
        EXPECT_EQ(fixture.counts->exactness_checks, 2);
        EXPECT_EQ(fixture.segment->raw_view_reads,
                  literals.first == "ab"
                      ? 0
                      : fixture.segment->num_chunk_data(fixture.field_id));
    }
}

TEST(NgramExpressionTest, OffsetInnerMatchesKeepRawEvaluationAndNullValidity) {
    NgramPhase2Fixture fixture(true);
    exec::OffsetVector offsets{17, 3, 14, 0, 17};
    for (const bool fused : {false, true}) {
        *fixture.counts = {};
        auto actual_expr =
            fused ? fixture.And(fixture.segment.get(), "ab", "中文", true)
                  : fixture.Unary(fixture.segment.get(), "ab");
        auto raw_expr =
            fused ? fixture.And(fixture.raw_segment.get(), "ab", "中文", false)
                  : fixture.Unary(fixture.raw_segment.get(), "ab");
        const auto actual =
            fixture.Eval(actual_expr, fixture.segment.get(), &offsets);
        const auto raw =
            fixture.Eval(raw_expr, fixture.raw_segment.get(), &offsets);
        NgramPhase2Fixture::ExpectSame(actual, raw);
        ASSERT_NE(actual, nullptr);
        TargetBitmapView valid(actual->GetValidRawData(), actual->size());
        EXPECT_FALSE(valid[0]);
        EXPECT_FALSE(valid[4]);
        EXPECT_EQ(fixture.counts->candidates, 0);
        EXPECT_EQ(fixture.counts->exactness_checks, 0);
        if (fused) {
            const auto conjunction =
                std::dynamic_pointer_cast<exec::PhyConjunctFilterExpr>(
                    actual_expr);
            ASSERT_NE(conjunction, nullptr);
            EXPECT_EQ(conjunction->GetInputsRef().size(), 2);
        }
    }
}
