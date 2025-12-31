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

#include <boost/format.hpp>
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <vector>
#include <chrono>

#include "segcore/SegmentSealed.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/DataGen.h"
#include "plan/PlanNode.h"
#include "exec/Task.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "exec/expression/Expr.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/LogicalUnaryExpr.h"
#include "exec/expression/function/FunctionFactory.h"

using namespace milvus;
using namespace milvus::exec;
using namespace milvus::query;
using namespace milvus::segcore;

class TaskTest : public testing::TestWithParam<DataType> {
 protected:
    void
    SetUp() override {
        using namespace milvus;
        using namespace milvus::query;
        using namespace milvus::segcore;
        milvus::exec::expression::FunctionFactory& factory =
            milvus::exec::expression::FunctionFactory::Instance();
        factory.Initialize();

        auto schema = std::make_shared<Schema>();
        auto vec_fid = schema->AddDebugField(
            "fakevec", GetParam(), 16, knowhere::metric::L2);
        auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
        field_map_.insert({"bool", bool_fid});
        auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
        field_map_.insert({"bool1", bool_1_fid});
        auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
        field_map_.insert({"int8", int8_fid});
        auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
        field_map_.insert({"int81", int8_1_fid});
        auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
        field_map_.insert({"int16", int16_fid});
        auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
        field_map_.insert({"int161", int16_1_fid});
        auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
        field_map_.insert({"int32", int32_fid});
        auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
        field_map_.insert({"int321", int32_1_fid});
        auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
        field_map_.insert({"int64", int64_fid});
        auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
        field_map_.insert({"int641", int64_1_fid});
        auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
        field_map_.insert({"float", float_fid});
        auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
        field_map_.insert({"float1", float_1_fid});
        auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
        field_map_.insert({"double", double_fid});
        auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
        field_map_.insert({"double1", double_1_fid});
        auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
        field_map_.insert({"string1", str1_fid});
        auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
        field_map_.insert({"string2", str2_fid});
        auto str3_fid = schema->AddDebugField("string3", DataType::VARCHAR);
        field_map_.insert({"string3", str3_fid});
        auto json_fid = schema->AddDebugField("json", DataType::JSON);
        field_map_.insert({"json", json_fid});
        schema->set_primary_field_id(str1_fid);

        size_t N = 100000;
        num_rows_ = N;
        auto raw_data = DataGen(schema, N);
        auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
        segment_ = SegmentSealedSPtr(segment.release());
    }

    void
    TearDown() override {
    }

 public:
    SegmentSealedSPtr segment_;
    std::map<std::string, FieldId> field_map_;
    int64_t num_rows_{0};
};

INSTANTIATE_TEST_SUITE_P(TaskTestSuite,
                         TaskTest,
                         ::testing::Values(DataType::VECTOR_FLOAT,
                                           DataType::VECTOR_SPARSE_U32_F32));

TEST_P(TaskTest, RegisterFunction) {
    milvus::exec::expression::FunctionFactory& factory =
        milvus::exec::expression::FunctionFactory::Instance();
    ASSERT_EQ(factory.GetFilterFunctionNum(), 2);
    auto all_functions = factory.ListAllFilterFunctions();
    // for (auto& f : all_functions) {
    //     std::cout << f.toString() << std::endl;
    // }

    auto func_ptr = factory.GetFilterFunction(
        milvus::exec::expression::FilterFunctionRegisterKey{
            "empty", {DataType::VARCHAR}});
    ASSERT_TRUE(func_ptr != nullptr);
}

TEST_P(TaskTest, CallExprEmpty) {
    expr::ColumnInfo col(field_map_["string1"], DataType::VARCHAR);
    std::vector<milvus::expr::TypedExprPtr> parameters;
    parameters.push_back(std::make_shared<milvus::expr::ColumnExpr>(col));
    milvus::exec::expression::FunctionFactory& factory =
        milvus::exec::expression::FunctionFactory::Instance();
    auto empty_function_ptr = factory.GetFilterFunction(
        milvus::exec::expression::FilterFunctionRegisterKey{
            "empty", {DataType::VARCHAR}});
    auto call_expr = std::make_shared<milvus::expr::CallExpr>(
        "empty", parameters, empty_function_ptr);
    ASSERT_EQ(call_expr->inputs().size(), 1);
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        "plannode id 1", call_expr, sources);
    auto plan = plan::PlanFragment(filter_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        100000,
        MAX_TIMESTAMP,
        0,
        0,
        query::PlanOptions{false},
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto start = std::chrono::steady_clock::now();
    auto task = Task::Create("task_call_expr_empty", plan, 0, query_context);
    int64_t num_rows = 0;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            break;
        }
        num_rows += result->size();
    }
    auto cost = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count();
    std::cout << "cost: " << cost << "us" << std::endl;
    EXPECT_EQ(num_rows, num_rows_);
}

TEST_P(TaskTest, UnaryExpr) {
    ::milvus::proto::plan::GenericValue value;
    value.set_int64_val(-1);
    auto logical_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_map_["int64"], DataType::INT64),
        proto::plan::OpType::LessThan,
        value,
        std::vector<proto::plan::GenericValue>{});
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        "plannode id 1", logical_expr, sources);
    auto plan = plan::PlanFragment(filter_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        100000,
        MAX_TIMESTAMP,
        0,
        0,
        query::PlanOptions{false},
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto start = std::chrono::steady_clock::now();
    auto task = Task::Create("task_unary_expr", plan, 0, query_context);
    int64_t num_rows = 0;
    int i = 0;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            break;
        }
        num_rows += result->size();
    }
    auto cost = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count();
    std::cout << "cost: " << cost << "us" << std::endl;
    EXPECT_EQ(num_rows, num_rows_);
}

TEST_P(TaskTest, LogicalExpr) {
    ::milvus::proto::plan::GenericValue value;
    value.set_int64_val(-1);
    auto left = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_map_["int64"], DataType::INT64),
        proto::plan::OpType::LessThan,
        value,
        std::vector<proto::plan::GenericValue>{});
    auto right = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_map_["int64"], DataType::INT64),
        proto::plan::OpType::LessThan,
        value,
        std::vector<proto::plan::GenericValue>{});

    auto top = std::make_shared<milvus::expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, left, right);
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        "plannode id 1", top, sources);
    auto plan = plan::PlanFragment(filter_node);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test1",
        segment_.get(),
        100000,
        MAX_TIMESTAMP,
        0,
        0,
        query::PlanOptions{false},
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto start = std::chrono::steady_clock::now();
    auto task =
        Task::Create("task_logical_binary_expr", plan, 0, query_context);
    int64_t num_rows = 0;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            break;
        }
        num_rows += result->size();
    }
    auto cost = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count();
    std::cout << "cost: " << cost << "us" << std::endl;
    EXPECT_EQ(num_rows, num_rows_);
}

TEST_P(TaskTest, Test_reorder) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    using namespace milvus::exec;

    {
        // expr:  string2 like '%xx' and string2 == 'xxx'
        // reorder: string2 == "xxx" and string2 like '%xxx'
        proto::plan::GenericValue val1;
        val1.set_string_val("%xxx");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Match,
            val1,
            std::vector<proto::plan::GenericValue>{});
        proto::plan::GenericValue val2;
        val2.set_string_val("xxx");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val2,
            std::vector<proto::plan::GenericValue>{});
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr3}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        std::cout << phy_expr->ToString() << std::endl;
        auto reorder = phy_expr->GetReorder();
        EXPECT_EQ(reorder.size(), 2);
        EXPECT_EQ(reorder[0], 1);
        EXPECT_EQ(reorder[1], 0);
    }

    {
        // expr:  string2 == 'xxx' and int1 < 100
        // reorder: int1 < 100 and string2 == 'xxx'
        proto::plan::GenericValue val1;
        val1.set_string_val("xxx");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val1,
            std::vector<proto::plan::GenericValue>{});
        proto::plan::GenericValue val2;
        val2.set_int64_val(100);
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::LessThan,
            val2,
            std::vector<proto::plan::GenericValue>{});
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr3}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        std::cout << phy_expr->ToString() << std::endl;
        auto reorder = phy_expr->GetReorder();
        EXPECT_EQ(reorder.size(), 2);
        EXPECT_EQ(reorder[0], 1);
        EXPECT_EQ(reorder[1], 0);
    }

    {
        // expr: json['b'] like '%xx' and json['a'] == 'xxx'
        // reorder: json['a'] == 'xxx' and json['b'] like '%xx'
        proto::plan::GenericValue val1;
        val1.set_string_val("%xxx");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"], DataType::JSON),
            proto::plan::OpType::Match,
            val1,
            std::vector<proto::plan::GenericValue>{});
        proto::plan::GenericValue val2;
        val2.set_string_val("xxx");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"], DataType::JSON),
            proto::plan::OpType::Equal,
            val2,
            std::vector<proto::plan::GenericValue>{});
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr3}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        std::cout << phy_expr->ToString() << std::endl;
        auto reorder = phy_expr->GetReorder();
        EXPECT_EQ(reorder.size(), 2);
        EXPECT_EQ(reorder[0], 1);
        EXPECT_EQ(reorder[1], 0);
    }

    {
        // expr: json['a'] == 'xxx' and int1 ==  100
        // reorder: int1 == 100 and json['a'] == 'xxx'
        proto::plan::GenericValue val1;
        val1.set_string_val("xxx");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"], DataType::JSON),
            proto::plan::OpType::Equal,
            val1,
            std::vector<proto::plan::GenericValue>{});
        proto::plan::GenericValue val2;
        val2.set_int64_val(100);
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::Equal,
            val2,
            std::vector<proto::plan::GenericValue>{});
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr3}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        std::cout << phy_expr->ToString() << std::endl;
        auto reorder = phy_expr->GetReorder();
        EXPECT_EQ(reorder.size(), 2);
        EXPECT_EQ(reorder[0], 1);
        EXPECT_EQ(reorder[1], 0);
    }

    {
        // expr: json['a'] == 'xxx' and 0 < int1 < 100
        // reorder:  0 < int1 < 100 and json['a'] == 'xxx'
        proto::plan::GenericValue val1;
        val1.set_string_val("xxx");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"], DataType::JSON),
            proto::plan::OpType::Equal,
            val1,
            std::vector<proto::plan::GenericValue>{});
        proto::plan::GenericValue low;
        low.set_int64_val(0);
        proto::plan::GenericValue upper;
        upper.set_int64_val(100);
        auto expr2 = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            low,
            upper,
            false,
            false);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr3}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        std::cout << phy_expr->ToString() << std::endl;
        auto reorder = phy_expr->GetReorder();
        EXPECT_EQ(reorder.size(), 2);
        EXPECT_EQ(reorder[0], 1);
        EXPECT_EQ(reorder[1], 0);
    }

    {
        // expr: string1 != string2 and 0 < int1 < 100
        // reorder:  0 < int1 < 100 and string1 != string2
        proto::plan::GenericValue val1;
        val1.set_string_val("xxx");
        auto expr1 = std::make_shared<expr::CompareExpr>(field_map_["string1"],
                                                         field_map_["string2"],
                                                         DataType::VARCHAR,
                                                         DataType::VARCHAR,
                                                         OpType::LessThan);
        proto::plan::GenericValue low;
        low.set_int64_val(0);
        proto::plan::GenericValue upper;
        upper.set_int64_val(100);
        auto expr2 = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            low,
            upper,
            false,
            false);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr3}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        std::cout << phy_expr->ToString() << std::endl;
        auto reorder = phy_expr->GetReorder();
        EXPECT_EQ(reorder.size(), 2);
        EXPECT_EQ(reorder[0], 1);
        EXPECT_EQ(reorder[1], 0);
    }

    {
        // expr:  string2 like '%xx' and string2 == 'xxx'
        // disable optimize expr, still remain sequence
        proto::plan::GenericValue val1;
        val1.set_string_val("%xxx");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Match,
            val1,
            std::vector<proto::plan::GenericValue>{});
        proto::plan::GenericValue val2;
        val2.set_string_val("xxx");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val2,
            std::vector<proto::plan::GenericValue>{});
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        OPTIMIZE_EXPR_ENABLED.store(false);
        auto exprs =
            milvus::exec::CompileExpressions({expr3}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        std::cout << phy_expr->ToString() << std::endl;
        auto reorder = phy_expr->GetReorder();
        EXPECT_EQ(reorder.size(), 0);
        OPTIMIZE_EXPR_ENABLED.store(true, std::memory_order_release);
    }
}

// This test verifies the fix for https://github.com/milvus-io/milvus/issues/46053.
//
// Bug scenario:
// - Expression: string_field == "target" AND int64_field == X AND float_field > Y
// - Data is stored in multiple chunks
// - SkipIndex skips some chunks for the float range condition
// - Expression reordering: numeric expressions execute before string expressions
// - When a chunk is skipped, processed_cursor in execute_sub_batch wasn't updated
// - This caused bitmap_input indices to be misaligned for subsequent expressions
//
// The fix ensures that when a chunk is skipped by SkipIndex, we still call
// func(nullptr, ...) so that execute_sub_batch can update its internal cursors.
TEST(TaskTest, SkipIndexWithBitmapInputAlignment) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    using namespace milvus::exec;

    auto schema = std::make_shared<Schema>();
    auto dim = 4;
    auto metrics_type = "L2";
    auto fake_vec_fid = schema->AddDebugField(
        "fakeVec", DataType::VECTOR_FLOAT, dim, metrics_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto string_fid = schema->AddDebugField("string_field", DataType::VARCHAR);
    auto int64_fid = schema->AddDebugField("int64_field", DataType::INT64);
    auto float_fid = schema->AddDebugField("float_field", DataType::FLOAT);

    auto segment = CreateSealedSegment(schema);
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();

    // Create two chunks with different data distributions:
    // Chunk 0: float values [10, 20, 30, 40, 50] - will be SKIPPED by float > 60
    // Chunk 1: float values [65, 70, 75, 80, 85] - will NOT be skipped
    //
    // We place the target row (string="target_value", int64=999) in chunk 1 at index 2
    // with float=75 which satisfies float > 60

    const size_t chunk_size = 5;

    // Chunk 0: floats that will cause this chunk to be skipped (max=50 < 60)
    std::vector<float> floats_chunk0 = {10.0f, 20.0f, 30.0f, 40.0f, 50.0f};
    auto float_field_data_0 = storage::CreateFieldData(
        DataType::FLOAT, DataType::NONE, false, 1, chunk_size);
    float_field_data_0->FillFieldData(floats_chunk0.data(), chunk_size);

    // Chunk 1: floats that will NOT be skipped (min=65 > 60)
    std::vector<float> floats_chunk1 = {65.0f, 70.0f, 75.0f, 80.0f, 85.0f};
    auto float_field_data_1 = storage::CreateFieldData(
        DataType::FLOAT, DataType::NONE, false, 1, chunk_size);
    float_field_data_1->FillFieldData(floats_chunk1.data(), chunk_size);

    auto float_load_info =
        PrepareSingleFieldInsertBinlog(kCollectionID,
                                       kPartitionID,
                                       kSegmentID,
                                       float_fid.get(),
                                       {float_field_data_0, float_field_data_1},
                                       cm);
    segment->LoadFieldData(float_load_info);

    // Int64 field - target value 999 at chunk 1 index 2
    std::vector<int64_t> int64s_chunk0 = {1, 2, 3, 4, 5};
    auto int64_field_data_0 = storage::CreateFieldData(
        DataType::INT64, DataType::NONE, false, 1, chunk_size);
    int64_field_data_0->FillFieldData(int64s_chunk0.data(), chunk_size);

    std::vector<int64_t> int64s_chunk1 = {6, 7, 999, 9, 10};  // 999 at index 2
    auto int64_field_data_1 = storage::CreateFieldData(
        DataType::INT64, DataType::NONE, false, 1, chunk_size);
    int64_field_data_1->FillFieldData(int64s_chunk1.data(), chunk_size);

    auto int64_load_info =
        PrepareSingleFieldInsertBinlog(kCollectionID,
                                       kPartitionID,
                                       kSegmentID,
                                       int64_fid.get(),
                                       {int64_field_data_0, int64_field_data_1},
                                       cm);
    segment->LoadFieldData(int64_load_info);

    // String field - target value "target_value" at chunk 1 index 2
    std::vector<std::string> strings_chunk0 = {"a", "b", "c", "d", "e"};
    auto string_field_data_0 = storage::CreateFieldData(
        DataType::VARCHAR, DataType::NONE, false, 1, chunk_size);
    string_field_data_0->FillFieldData(strings_chunk0.data(), chunk_size);

    std::vector<std::string> strings_chunk1 = {
        "f", "g", "target_value", "i", "j"};
    auto string_field_data_1 = storage::CreateFieldData(
        DataType::VARCHAR, DataType::NONE, false, 1, chunk_size);
    string_field_data_1->FillFieldData(strings_chunk1.data(), chunk_size);

    auto string_load_info = PrepareSingleFieldInsertBinlog(
        kCollectionID,
        kPartitionID,
        kSegmentID,
        string_fid.get(),
        {string_field_data_0, string_field_data_1},
        cm);
    segment->LoadFieldData(string_load_info);

    // PK field
    std::vector<int64_t> pks_chunk0 = {100, 101, 102, 103, 104};
    auto pk_field_data_0 = storage::CreateFieldData(
        DataType::INT64, DataType::NONE, false, 1, chunk_size);
    pk_field_data_0->FillFieldData(pks_chunk0.data(), chunk_size);

    std::vector<int64_t> pks_chunk1 = {105, 106, 107, 108, 109};
    auto pk_field_data_1 = storage::CreateFieldData(
        DataType::INT64, DataType::NONE, false, 1, chunk_size);
    pk_field_data_1->FillFieldData(pks_chunk1.data(), chunk_size);

    auto pk_load_info =
        PrepareSingleFieldInsertBinlog(kCollectionID,
                                       kPartitionID,
                                       kSegmentID,
                                       pk_fid.get(),
                                       {pk_field_data_0, pk_field_data_1},
                                       cm);
    segment->LoadFieldData(pk_load_info);

    // Vector field (required but not used in filter)
    std::vector<float> vec_chunk0(chunk_size * dim, 1.0f);
    auto vec_field_data_0 = storage::CreateFieldData(
        DataType::VECTOR_FLOAT, DataType::NONE, false, dim, chunk_size);
    vec_field_data_0->FillFieldData(vec_chunk0.data(), chunk_size);

    std::vector<float> vec_chunk1(chunk_size * dim, 2.0f);
    auto vec_field_data_1 = storage::CreateFieldData(
        DataType::VECTOR_FLOAT, DataType::NONE, false, dim, chunk_size);
    vec_field_data_1->FillFieldData(vec_chunk1.data(), chunk_size);

    auto vec_load_info =
        PrepareSingleFieldInsertBinlog(kCollectionID,
                                       kPartitionID,
                                       kSegmentID,
                                       fake_vec_fid.get(),
                                       {vec_field_data_0, vec_field_data_1},
                                       cm);
    segment->LoadFieldData(vec_load_info);

    // Row IDs
    std::vector<int64_t> row_ids_chunk0 = {0, 1, 2, 3, 4};
    auto row_ids_data_0 = storage::CreateFieldData(
        DataType::INT64, DataType::NONE, false, 1, chunk_size);
    row_ids_data_0->FillFieldData(row_ids_chunk0.data(), chunk_size);

    std::vector<int64_t> row_ids_chunk1 = {5, 6, 7, 8, 9};
    auto row_ids_data_1 = storage::CreateFieldData(
        DataType::INT64, DataType::NONE, false, 1, chunk_size);
    row_ids_data_1->FillFieldData(row_ids_chunk1.data(), chunk_size);

    auto row_id_load_info =
        PrepareSingleFieldInsertBinlog(kCollectionID,
                                       kPartitionID,
                                       kSegmentID,
                                       RowFieldID.get(),
                                       {row_ids_data_0, row_ids_data_1},
                                       cm);
    segment->LoadFieldData(row_id_load_info);

    // Timestamps
    std::vector<int64_t> timestamps_chunk0 = {1, 1, 1, 1, 1};
    auto ts_data_0 = storage::CreateFieldData(
        DataType::INT64, DataType::NONE, false, 1, chunk_size);
    ts_data_0->FillFieldData(timestamps_chunk0.data(), chunk_size);

    std::vector<int64_t> timestamps_chunk1 = {1, 1, 1, 1, 1};
    auto ts_data_1 = storage::CreateFieldData(
        DataType::INT64, DataType::NONE, false, 1, chunk_size);
    ts_data_1->FillFieldData(timestamps_chunk1.data(), chunk_size);

    auto ts_load_info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                       kPartitionID,
                                                       kSegmentID,
                                                       TimestampFieldID.get(),
                                                       {ts_data_0, ts_data_1},
                                                       cm);
    segment->LoadFieldData(ts_load_info);

    // Build the expression:
    // string_field == "target_value" AND int64_field == 999 AND float_field > 60
    //
    // Due to expression reordering, this will execute as:
    // 1. float_field > 60 (numeric, runs first) - SkipIndex skips chunk 0
    // 2. int64_field == 999 (numeric, runs second)
    // 3. string_field == "target_value" (string, runs last)

    // string_field == "target_value"
    proto::plan::GenericValue string_val;
    string_val.set_string_val("target_value");
    auto string_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(string_fid, DataType::VARCHAR),
        proto::plan::OpType::Equal,
        string_val,
        std::vector<proto::plan::GenericValue>{});

    // int64_field == 999
    proto::plan::GenericValue int64_val;
    int64_val.set_int64_val(999);
    auto int64_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::Equal,
        int64_val,
        std::vector<proto::plan::GenericValue>{});

    // float_field > 60
    proto::plan::GenericValue float_val;
    float_val.set_float_val(60.0f);
    auto float_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(float_fid, DataType::FLOAT),
        proto::plan::OpType::GreaterThan,
        float_val,
        std::vector<proto::plan::GenericValue>{});

    // Build AND expression: string_expr AND int64_expr AND float_expr
    auto and_expr1 = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, string_expr, int64_expr);
    auto and_expr2 = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, and_expr1, float_expr);

    // Verify SkipIndex is working before running the expression:
    // Check if chunk 0 can be skipped for float > 60
    auto& skip_index = segment->GetSkipIndex();
    bool chunk0_can_skip = skip_index.CanSkipUnaryRange<float>(
        float_fid, 0, proto::plan::OpType::GreaterThan, 60.0f);
    bool chunk1_can_skip = skip_index.CanSkipUnaryRange<float>(
        float_fid, 1, proto::plan::OpType::GreaterThan, 60.0f);

    // Chunk 0 should be skippable (max=50 < 60), chunk 1 should not (min=65 > 60)
    EXPECT_TRUE(chunk0_can_skip)
        << "Chunk 0 should be skippable for float > 60 (max=50)";
    EXPECT_FALSE(chunk1_can_skip)
        << "Chunk 1 should NOT be skippable for float > 60 (min=65)";

    std::vector<milvus::plan::PlanNodePtr> sources;
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        "plannode id 1", and_expr2, sources);
    auto plan = plan::PlanFragment(filter_node);

    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test_skip_index_bitmap_alignment",
        segment.get(),
        chunk_size * 2,  // total rows
        MAX_TIMESTAMP,
        0,
        0,
        query::PlanOptions{false},
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_skip_index_bitmap", plan, 0, query_context);

    int64_t total_rows = 0;
    int64_t filtered_rows = 0;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            break;
        }
        auto col_vec =
            std::dynamic_pointer_cast<ColumnVector>(result->child(0));
        if (col_vec && col_vec->IsBitmap()) {
            TargetBitmapView view(col_vec->GetRawData(), col_vec->size());
            total_rows += col_vec->size();
            filtered_rows +=
                view.count();  // These are filtered OUT (don't match)
        }
    }

    int64_t num_matched = total_rows - filtered_rows;

    // Expected result: exactly 1 row should match
    // - Row at chunk 1, index 2 (global index 7) has:
    //   - string_field = "target_value" ✓
    //   - int64_field = 999 ✓
    //   - float_field = 75 > 60 ✓
    //
    // With the bug (before fix): 0 rows would match because bitmap_input
    // indices were misaligned after chunk 0 was skipped.
    //
    // With the fix: 1 row should match correctly.
    EXPECT_EQ(num_matched, 1);
}
