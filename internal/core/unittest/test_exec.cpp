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
#include <cstdint>
#include <memory>
#include <regex>
#include <vector>
#include <chrono>

#include "query/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "plan/PlanNode.h"
#include "exec/Task.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "exec/expression/Expr.h"
#include "exec/expression/ConjunctExpr.h"
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
        val,
        std::vector<proto::plan::GenericValue>{});
    auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        val,
        std::vector<proto::plan::GenericValue>{});
    auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
    auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        val,
        std::vector<proto::plan::GenericValue>{});
    auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        val,
        std::vector<proto::plan::GenericValue>{});
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
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, expr1, expr2);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
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
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
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
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
        auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr5 = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
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
