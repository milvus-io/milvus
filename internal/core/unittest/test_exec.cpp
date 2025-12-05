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
#include <regex>
#include <vector>
#include <chrono>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "query/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/DataGen.h"
#include "plan/PlanNode.h"
#include "exec/Task.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "exec/expression/Expr.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/function/FunctionFactory.h"
#include "mmap/Types.h"

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

        auto segment = CreateSealedSegment(schema);
        size_t N = 100000;
        num_rows_ = N;
        auto raw_data = DataGen(schema, N);
        auto fields = schema->get_fields();
        for (auto field_data : raw_data.raw_->fields_data()) {
            int64_t field_id = field_data.field_id();

            auto info = FieldDataInfo(field_data.field_id(), N, "/tmp/a");
            auto field_meta = fields.at(FieldId(field_id));
            info.channel->push(
                CreateFieldDataFromDataArray(N, &field_data, field_meta));
            info.channel->close();

            segment->LoadFieldData(FieldId(field_id), info);
        }
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
                                           DataType::VECTOR_SPARSE_FLOAT));

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
        value);
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
        value);
    auto right = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_map_["int64"], DataType::INT64),
        proto::plan::OpType::LessThan,
        value);

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

TEST_P(TaskTest, CompileInputs_and) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto vec_fid =
        schema->AddDebugField("fakevec", GetParam(), 16, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    proto::plan::GenericValue val;
    val.set_int64_val(10);
    // expr: (int64_fid < 10 and int64_fid < 10) and (int64_fid < 10 and int64_fid < 10)
    auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        val);
    auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        val);
    auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
    auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        val);
    auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        val);
    auto expr6 = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
    auto expr7 = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, expr3, expr6);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
    auto exprs = milvus::exec::CompileInputs(expr7, query_context.get(), {});
    EXPECT_EQ(exprs.size(), 4);
    for (int i = 0; i < exprs.size(); ++i) {
        std::cout << exprs[i]->name() << std::endl;
        EXPECT_STREQ(exprs[i]->name().c_str(), "PhyUnaryRangeFilterExpr");
    }
}

TEST_P(TaskTest, CompileInputs_or_with_and) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto vec_fid =
        schema->AddDebugField("fakevec", GetParam(), 16, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    proto::plan::GenericValue val;
    val.set_int64_val(10);
    {
        // expr: (int64_fid > 10 and int64_fid > 10) or (int64_fid > 10 and int64_fid > 10)
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr6 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        auto expr7 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr3, expr6);
        auto exprs =
            milvus::exec::CompileInputs(expr7, query_context.get(), {});
        EXPECT_EQ(exprs.size(), 2);
        for (int i = 0; i < exprs.size(); ++i) {
            std::cout << exprs[i]->name() << std::endl;
            EXPECT_STREQ(exprs[i]->name().c_str(), "PhyConjunctFilterExpr");
        }
    }
    {
        // expr: (int64_fid < 10 or int64_fid < 10) or (int64_fid > 10 and int64_fid > 10)
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr6 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        auto expr7 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr3, expr6);
        auto exprs =
            milvus::exec::CompileInputs(expr7, query_context.get(), {});
        std::cout << exprs.size() << std::endl;
        EXPECT_EQ(exprs.size(), 3);
        for (int i = 0; i < exprs.size() - 1; ++i) {
            std::cout << exprs[i]->name() << std::endl;
            EXPECT_STREQ(exprs[i]->name().c_str(), "PhyUnaryRangeFilterExpr");
        }
        EXPECT_STREQ(exprs[2]->name().c_str(), "PhyConjunctFilterExpr");
    }
    {
        // expr: (int64_fid > 10 or int64_fid > 10) and (int64_fid > 10 and int64_fid > 10)
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val);
        auto expr6 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        auto expr7 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr3, expr6);
        auto exprs =
            milvus::exec::CompileInputs(expr7, query_context.get(), {});
        std::cout << exprs.size() << std::endl;
        EXPECT_EQ(exprs.size(), 3);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        for (int i = 1; i < exprs.size(); ++i) {
            std::cout << exprs[i]->name() << std::endl;
            EXPECT_STREQ(exprs[i]->name().c_str(), "PhyUnaryRangeFilterExpr");
        }
    }
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
            val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("xxx");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val2);
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
            val1);
        proto::plan::GenericValue val2;
        val2.set_int64_val(100);
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::LessThan,
            val2);
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
            val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("xxx");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"], DataType::JSON),
            proto::plan::OpType::Equal,
            val2);
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
            val1);
        proto::plan::GenericValue val2;
        val2.set_int64_val(100);
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::Equal,
            val2);
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
            val1);
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
            val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("xxx");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val2);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        OPTIMIZE_EXPR_ENABLED = false;
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
        OPTIMIZE_EXPR_ENABLED = true;
    }
}

TEST_P(TaskTest, Test_MultiInConvert) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    using namespace milvus::exec;

    {
        // expr:  string2 == '111' or string2 == '222' or string2 == "333"
        proto::plan::GenericValue val1;
        val1.set_string_val("111");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("222");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val2);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        proto::plan::GenericValue val3;
        val3.set_string_val("333");
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val3);
        auto expr5 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr3, expr4);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr5}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        auto inputs = phy_expr->GetInputsRef();
        EXPECT_EQ(inputs.size(), 1);
        EXPECT_STREQ(inputs[0]->name().c_str(), "PhyTermFilterExpr");
    }

    {
        // expr:  string2 == '111' or string2 == '222' or (int64 > 10 && int64 < 100) or string2 == "333"
        proto::plan::GenericValue val1;
        val1.set_string_val("111");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("222");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val2);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);

        proto::plan::GenericValue val3;
        val3.set_int64_val(10);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val3);
        proto::plan::GenericValue val4;
        val4.set_int64_val(100);
        auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::LessThan,
            val4);
        auto expr6 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr4, expr5);

        auto expr7 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr6, expr3);

        proto::plan::GenericValue val5;
        val5.set_string_val("333");
        auto expr8 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["string2"], DataType::VARCHAR),
            proto::plan::OpType::Equal,
            val5);
        auto expr9 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr7, expr8);

        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr9}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        auto inputs = phy_expr->GetInputsRef();
        EXPECT_EQ(inputs.size(), 2);
        EXPECT_STREQ(inputs[0]->name().c_str(), "PhyConjunctFilterExpr");
        EXPECT_STREQ(inputs[1]->name().c_str(), "PhyTermFilterExpr");
    }
    {
        // expr: json['a'] == "111" or json['a'] == "222" or json['3'] = "333"
        proto::plan::GenericValue val1;
        val1.set_string_val("111");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'a'}),
            proto::plan::OpType::Equal,
            val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("222");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'a'}),
            proto::plan::OpType::Equal,
            val2);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        proto::plan::GenericValue val3;
        val3.set_string_val("333");
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'a'}),
            proto::plan::OpType::Equal,
            val3);
        auto expr5 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr3, expr4);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr5}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        auto inputs = phy_expr->GetInputsRef();
        EXPECT_EQ(inputs.size(), 1);
        EXPECT_STREQ(inputs[0]->name().c_str(), "PhyTermFilterExpr");
    }

    {
        // expr: json['a'] == "111" or json['b'] == "222" or json['a'] == "333"
        proto::plan::GenericValue val1;
        val1.set_string_val("111");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'a'}),
            proto::plan::OpType::Equal,
            val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("222");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'b'}),
            proto::plan::OpType::Equal,
            val2);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        proto::plan::GenericValue val3;
        val3.set_string_val("333");
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'a'}),
            proto::plan::OpType::Equal,
            val3);
        auto expr5 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr3, expr4);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr5}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        auto inputs = phy_expr->GetInputsRef();
        EXPECT_EQ(inputs.size(), 2);
        EXPECT_STREQ(inputs[0]->name().c_str(), "PhyTermFilterExpr");
        EXPECT_STREQ(inputs[1]->name().c_str(), "PhyUnaryRangeFilterExpr");
    }

    {
        // expr: json['a'] == "111" or json['b'] == "222" or json['a'] == 1
        proto::plan::GenericValue val1;
        val1.set_string_val("111");
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'a'}),
            proto::plan::OpType::Equal,
            val1);
        proto::plan::GenericValue val2;
        val2.set_string_val("222");
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'b'}),
            proto::plan::OpType::Equal,
            val2);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        proto::plan::GenericValue val3;
        val3.set_int64_val(1);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["json"],
                             DataType::JSON,
                             std::vector<std::string>{'a'}),
            proto::plan::OpType::Equal,
            val3);
        auto expr5 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr3, expr4);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr5}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        auto inputs = phy_expr->GetInputsRef();
        EXPECT_EQ(inputs.size(), 3);
    }

    {
        // expr: int1 == 11 or int1 == 22 or int3 == 33
        proto::plan::GenericValue val1;
        val1.set_int64_val(11);
        auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::Equal,
            val1);
        proto::plan::GenericValue val2;
        val2.set_int64_val(222);
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::Equal,
            val2);
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        proto::plan::GenericValue val3;
        val3.set_int64_val(1);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_map_["int64"], DataType::INT64),
            proto::plan::OpType::Equal,
            val3);
        auto expr5 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr3, expr4);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            DEAFULT_QUERY_ID, segment_.get(), 100000, MAX_TIMESTAMP);
        ExecContext context(query_context.get());
        auto exprs =
            milvus::exec::CompileExpressions({expr5}, &context, {}, false);
        EXPECT_EQ(exprs.size(), 1);
        EXPECT_STREQ(exprs[0]->name().c_str(), "PhyConjunctFilterExpr");
        auto phy_expr =
            std::static_pointer_cast<milvus::exec::PhyConjunctFilterExpr>(
                exprs[0]);
        auto inputs = phy_expr->GetInputsRef();
        EXPECT_EQ(inputs.size(), 3);
    }
}

// This test verifies the fix for https://github.com/milvus-io/milvus/issues/46053.
//
// Bug scenario:
// - Expression: int32_field == X AND int64_field == Y AND float_field > Z
// - Data is stored in multiple chunks
// - SkipIndex skips some chunks for the float range condition
// - When a chunk is skipped, processed_cursor in execute_sub_batch wasn't updated
// - This caused bitmap_input indices to be misaligned for subsequent expressions
//
// The fix ensures that when a chunk is skipped by SkipIndex, we still call
// func(nullptr, ...) so that execute_sub_batch can update its internal cursors.
//
// Note: This test uses ChunkedSegmentSealedImpl which requires Arrow API for data loading.
TEST(TaskTest, SkipIndexWithBitmapInputAlignment) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    using namespace milvus::exec;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto int32_fid = schema->AddDebugField("int32_field", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64_field", DataType::INT64);
    auto float_fid = schema->AddDebugField("float_field", DataType::FLOAT);
    schema->AddField(FieldName("ts"), TimestampFieldID, DataType::INT64, false);

    // Use ChunkedSegmentSealedImpl to support multiple chunks (is_multi_chunk=true)
    auto segment = CreateSealedSegment(schema,
                                       nullptr,
                                       0,
                                       SegcoreConfig::default_config(),
                                       false,
                                       true /* is_multi_chunk */);

    // Create two chunks with different data distributions:
    // Chunk 0: float values [10, 20, 30, 40, 50] - will be SKIPPED by float > 60
    // Chunk 1: float values [65, 70, 75, 80, 85] - will NOT be skipped
    //
    // We place the target row (int32=888, int64=999) in chunk 1 at index 2
    // with float=75 which satisfies float > 60

    const int64_t chunk_size = 5;
    const int64_t num_chunks = 2;
    const int64_t total_rows = chunk_size * num_chunks;

    // Data for each chunk
    // Chunk 0: pk=[100..104], int32=[11..15], int64=[1..5], float=[10,20,30,40,50]
    // Chunk 1: pk=[105..109], int32=[16,17,888,19,20], int64=[6,7,999,9,10], float=[65,70,75,80,85]
    std::vector<std::vector<int64_t>> pk_data = {{100, 101, 102, 103, 104},
                                                 {105, 106, 107, 108, 109}};
    std::vector<std::vector<int32_t>> int32_data = {{11, 12, 13, 14, 15},
                                                    {16, 17, 888, 19, 20}};
    std::vector<std::vector<int64_t>> int64_data = {{1, 2, 3, 4, 5},
                                                    {6, 7, 999, 9, 10}};
    std::vector<std::vector<float>> float_data = {
        {10.0f, 20.0f, 30.0f, 40.0f, 50.0f},
        {65.0f, 70.0f, 75.0f, 80.0f, 85.0f}};
    std::vector<std::vector<int64_t>> ts_data = {{1, 1, 1, 1, 1},
                                                 {1, 1, 1, 1, 1}};

    // Helper lambda to create Arrow RecordBatchReader from data
    auto create_int64_reader = [&](const std::vector<int64_t>& data,
                                   const std::string& field_name)
        -> std::shared_ptr<ArrowDataWrapper> {
        auto builder = std::make_shared<arrow::Int64Builder>();
        std::vector<bool> validity(data.size(), true);
        EXPECT_TRUE(
            builder->AppendValues(data.begin(), data.end(), validity.begin())
                .ok());
        std::shared_ptr<arrow::Array> arrow_array;
        EXPECT_TRUE(builder->Finish(&arrow_array).ok());

        auto arrow_field = arrow::field(field_name, arrow::int64());
        auto arrow_schema =
            std::make_shared<arrow::Schema>(arrow::FieldVector(1, arrow_field));
        auto record_batch =
            arrow::RecordBatch::Make(arrow_schema, data.size(), {arrow_array});
        auto res = arrow::RecordBatchReader::Make({record_batch});
        EXPECT_TRUE(res.ok());
        return std::make_shared<ArrowDataWrapper>(
            res.ValueOrDie(), nullptr, nullptr);
    };

    auto create_int32_reader = [&](const std::vector<int32_t>& data,
                                   const std::string& field_name)
        -> std::shared_ptr<ArrowDataWrapper> {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::vector<bool> validity(data.size(), true);
        EXPECT_TRUE(
            builder->AppendValues(data.begin(), data.end(), validity.begin())
                .ok());
        std::shared_ptr<arrow::Array> arrow_array;
        EXPECT_TRUE(builder->Finish(&arrow_array).ok());

        auto arrow_field = arrow::field(field_name, arrow::int32());
        auto arrow_schema =
            std::make_shared<arrow::Schema>(arrow::FieldVector(1, arrow_field));
        auto record_batch =
            arrow::RecordBatch::Make(arrow_schema, data.size(), {arrow_array});
        auto res = arrow::RecordBatchReader::Make({record_batch});
        EXPECT_TRUE(res.ok());
        return std::make_shared<ArrowDataWrapper>(
            res.ValueOrDie(), nullptr, nullptr);
    };

    auto create_float_reader = [&](const std::vector<float>& data,
                                   const std::string& field_name)
        -> std::shared_ptr<ArrowDataWrapper> {
        auto builder = std::make_shared<arrow::FloatBuilder>();
        std::vector<bool> validity(data.size(), true);
        EXPECT_TRUE(
            builder->AppendValues(data.begin(), data.end(), validity.begin())
                .ok());
        std::shared_ptr<arrow::Array> arrow_array;
        EXPECT_TRUE(builder->Finish(&arrow_array).ok());

        auto arrow_field = arrow::field(field_name, arrow::float32());
        auto arrow_schema =
            std::make_shared<arrow::Schema>(arrow::FieldVector(1, arrow_field));
        auto record_batch =
            arrow::RecordBatch::Make(arrow_schema, data.size(), {arrow_array});
        auto res = arrow::RecordBatchReader::Make({record_batch});
        EXPECT_TRUE(res.ok());
        return std::make_shared<ArrowDataWrapper>(
            res.ValueOrDie(), nullptr, nullptr);
    };

    // Load PK field
    FieldDataInfo pk_info;
    pk_info.field_id = pk_fid.get();
    pk_info.row_count = total_rows;
    for (int i = 0; i < num_chunks; i++) {
        pk_info.arrow_reader_channel->push(
            create_int64_reader(pk_data[i], "pk"));
    }
    pk_info.arrow_reader_channel->close();
    segment->LoadFieldData(pk_fid, pk_info);

    // Load int32 field
    FieldDataInfo int32_info;
    int32_info.field_id = int32_fid.get();
    int32_info.row_count = total_rows;
    for (int i = 0; i < num_chunks; i++) {
        int32_info.arrow_reader_channel->push(
            create_int32_reader(int32_data[i], "int32_field"));
    }
    int32_info.arrow_reader_channel->close();
    segment->LoadFieldData(int32_fid, int32_info);

    // Load int64 field
    FieldDataInfo int64_info;
    int64_info.field_id = int64_fid.get();
    int64_info.row_count = total_rows;
    for (int i = 0; i < num_chunks; i++) {
        int64_info.arrow_reader_channel->push(
            create_int64_reader(int64_data[i], "int64_field"));
    }
    int64_info.arrow_reader_channel->close();
    segment->LoadFieldData(int64_fid, int64_info);

    // Load float field
    FieldDataInfo float_info;
    float_info.field_id = float_fid.get();
    float_info.row_count = total_rows;
    for (int i = 0; i < num_chunks; i++) {
        float_info.arrow_reader_channel->push(
            create_float_reader(float_data[i], "float_field"));
    }
    float_info.arrow_reader_channel->close();
    segment->LoadFieldData(float_fid, float_info);

    // Load timestamp field
    FieldDataInfo ts_info;
    ts_info.field_id = TimestampFieldID.get();
    ts_info.row_count = total_rows;
    for (int i = 0; i < num_chunks; i++) {
        ts_info.arrow_reader_channel->push(
            create_int64_reader(ts_data[i], "ts"));
    }
    ts_info.arrow_reader_channel->close();
    segment->LoadFieldData(TimestampFieldID, ts_info);

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

    // Build the expression:
    // int32_field == 888 AND int64_field == 999 AND float_field > 60

    // int32_field == 888
    proto::plan::GenericValue int32_val;
    int32_val.set_int64_val(888);
    auto int32_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int32_fid, DataType::INT32),
        proto::plan::OpType::Equal,
        int32_val);

    // int64_field == 999
    proto::plan::GenericValue int64_val;
    int64_val.set_int64_val(999);
    auto int64_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::Equal,
        int64_val);

    // float_field > 60
    proto::plan::GenericValue float_val;
    float_val.set_float_val(60.0f);
    auto float_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(float_fid, DataType::FLOAT),
        proto::plan::OpType::GreaterThan,
        float_val);

    // Build AND expression: int32_expr AND int64_expr AND float_expr
    auto and_expr1 = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, int32_expr, int64_expr);
    auto and_expr2 = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, and_expr1, float_expr);

    std::vector<milvus::plan::PlanNodePtr> sources;
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        "plannode id 1", and_expr2, sources);
    auto plan = plan::PlanFragment(filter_node);

    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "test_skip_index_bitmap_alignment",
        segment.get(),
        total_rows,
        MAX_TIMESTAMP,
        0,
        0,
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_skip_index_bitmap", plan, 0, query_context);

    int64_t num_total_rows = 0;
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
            num_total_rows += col_vec->size();
            filtered_rows +=
                view.count();  // These are filtered OUT (don't match)
        }
    }

    int64_t num_matched = num_total_rows - filtered_rows;

    // Expected result: exactly 1 row should match
    // - Row at chunk 1, index 2 (global index 7) has:
    //   - int32_field = 888 ✓
    //   - int64_field = 999 ✓
    //   - float_field = 75 > 60 ✓
    //
    // With the bug (before fix): 0 rows would match because bitmap_input
    // indices were misaligned after chunk 0 was skipped.
    //
    // With the fix: 1 row should match correctly.
    EXPECT_EQ(num_matched, 1);
}
