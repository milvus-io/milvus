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

#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <unordered_map>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "bitset/common.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/QueryContext.h"
#include "exec/Task.h"
#include "exec/operator/RescoresNode.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/Expr.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "index/NgramInvertedIndex.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "rescores/Scorer.h"
#include "query/PlanNode.h"
#include "query/Utils.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

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
        schema->AddDebugField("fakevec", GetParam(), 16, knowhere::metric::L2);
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

namespace {

bool
PlanTreeContainsRescoresNode(
    const std::shared_ptr<milvus::plan::PlanNode>& root) {
    std::queue<std::shared_ptr<milvus::plan::PlanNode>> queue;
    if (root != nullptr) {
        queue.push(root);
    }

    while (!queue.empty()) {
        auto node = queue.front();
        queue.pop();
        if (std::dynamic_pointer_cast<milvus::plan::RescoresNode>(node) !=
            nullptr) {
            return true;
        }
        for (const auto& source : node->sources()) {
            queue.push(source);
        }
    }
    return false;
}

}  // namespace

TEST(PlanProtoTest, ScorersDoNotInsertRescoresNode) {
    using namespace milvus;
    using namespace milvus::query;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    proto::plan::PlanNode plan_node;
    auto anns = plan_node.mutable_vector_anns();
    anns->set_vector_type(proto::plan::VectorType::FloatVector);
    anns->set_field_id(vec_fid.get());
    anns->set_placeholder_tag("$0");
    auto query_info = anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_metric_type(knowhere::metric::L2);
    query_info->set_search_params(R"({"nprobe": 10})");

    auto scorer = plan_node.add_scorers();
    scorer->set_weight(2.0F);
    scorer->set_type(proto::plan::FunctionType::FunctionTypeWeight);
    plan_node.mutable_score_option()->set_boost_mode(
        proto::plan::BoostMode::BoostModeMultiply);
    plan_node.mutable_score_option()->set_function_mode(
        proto::plan::FunctionMode::FunctionModeSum);

    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_NE(plan, nullptr);
    ASSERT_NE(plan->plan_node_, nullptr);
    ASSERT_NE(plan->plan_node_->plannodes_, nullptr);
    EXPECT_FALSE(PlanTreeContainsRescoresNode(plan->plan_node_->plannodes_));
}

TEST(RescoresNodeTest, ReturnsNullBeforeNoMoreInputAndPassesThroughNullInput) {
    proto::plan::ScoreOption option;
    option.set_boost_mode(proto::plan::BoostModeMultiply);
    option.set_function_mode(proto::plan::FunctionModeSum);
    std::vector<std::shared_ptr<rescores::Scorer>> scorers;
    auto logical_node = std::make_shared<plan::RescoresNode>(
        "rescore", scorers, option, std::vector<plan::PlanNodePtr>{});
    auto query_context =
        std::make_shared<QueryContext>("rescore-test",
                                       nullptr,
                                       0,
                                       MAX_TIMESTAMP,
                                       0,
                                       0,
                                       query::PlanOptions{false},
                                       std::make_shared<QueryConfig>());
    auto task = Task::Create("rescore-test-task",
                             plan::PlanFragment(logical_node),
                             0,
                             query_context);
    DriverContext driver_context(task, 0, 0, 0, 0);
    PhyRescoresNode node(0, &driver_context, logical_node);

    EXPECT_TRUE(node.NeedInput());
    EXPECT_EQ(node.GetOutput(), nullptr);

    node.NoMoreInput();
    EXPECT_EQ(node.GetOutput(), nullptr);
    EXPECT_TRUE(node.IsFinished());
    EXPECT_FALSE(node.NeedInput());
}

TEST(RescoresNodeTest, AppliesBoostAndSortsSearchResult) {
    proto::plan::ScoreOption option;
    option.set_boost_mode(proto::plan::BoostModeMultiply);
    option.set_function_mode(proto::plan::FunctionModeSum);
    std::vector<std::shared_ptr<rescores::Scorer>> scorers{
        std::make_shared<rescores::WeightScorer>(nullptr, 10.0F),
    };
    auto logical_node = std::make_shared<plan::RescoresNode>(
        "rescore", scorers, option, std::vector<plan::PlanNodePtr>{});

    SearchResult search_result;
    search_result.total_nq_ = 1;
    search_result.unity_topK_ = 4;
    search_result.total_data_cnt_ = 4;
    search_result.distances_ = {0.4F, 0.1F, 0.3F, 0.2F};
    search_result.seg_offsets_ = {4, -1, 3, 2};

    SearchInfo search_info;
    search_info.topk_ = 4;
    search_info.metric_type_ = knowhere::metric::IP;

    auto query_context =
        std::make_shared<QueryContext>("rescore-test",
                                       nullptr,
                                       4,
                                       MAX_TIMESTAMP,
                                       0,
                                       0,
                                       query::PlanOptions{false},
                                       std::make_shared<QueryConfig>());
    OpContext op_context;
    query_context->set_op_context(&op_context);
    query_context->set_search_info(search_info);
    query_context->set_search_result(std::move(search_result));

    auto task = Task::Create("rescore-test-task",
                             plan::PlanFragment(logical_node),
                             0,
                             query_context);
    DriverContext driver_context(task, 0, 0, 0, 0);
    PhyRescoresNode node(0, &driver_context, logical_node);
    auto input = std::make_shared<RowVector>(std::vector<VectorPtr>{});
    auto expected_input = input;
    node.AddInput(input);
    node.NoMoreInput();

    auto output = node.GetOutput();
    EXPECT_EQ(output, expected_input);
    EXPECT_TRUE(node.IsFinished());

    auto rescored = query_context->get_search_result();
    EXPECT_EQ(rescored.seg_offsets_, (std::vector<int64_t>{4, 3, 2, -1}));
    ASSERT_EQ(rescored.distances_.size(), 4);
    EXPECT_FLOAT_EQ(rescored.distances_[0], 4.0F);
    EXPECT_FLOAT_EQ(rescored.distances_[1], 3.0F);
    EXPECT_FLOAT_EQ(rescored.distances_[2], 2.0F);
    EXPECT_FLOAT_EQ(rescored.distances_[3], 0.1F);
}

TEST_P(TaskTest, RegisterFunction) {
    milvus::exec::expression::FunctionFactory& factory =
        milvus::exec::expression::FunctionFactory::Instance();
    ASSERT_EQ(factory.GetFilterFunctionNum(), 2);

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

TEST_P(TaskTest, DetermineExecPathFailureReleasesTaskDriverCycle) {
    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);
    proto::plan::GenericValue string_value;
    string_value.set_string_val("1");
    auto logical_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(field_map_["json"], DataType::JSON, {"v"}),
        std::vector<proto::plan::GenericValue>{int_value, string_value},
        false);
    auto filter_node = std::make_shared<plan::FilterBitsNode>(
        "mixed-json-term", logical_expr, std::vector<plan::PlanNodePtr>{});
    auto query_context =
        std::make_shared<QueryContext>("mixed-json-term",
                                       segment_.get(),
                                       num_rows_,
                                       MAX_TIMESTAMP,
                                       0,
                                       0,
                                       query::PlanOptions{false},
                                       std::make_shared<QueryConfig>());

    auto task = Task::Create(
        "mixed-json-term", plan::PlanFragment(filter_node), 0, query_context);
    std::weak_ptr<Task> weak_task = task;
    EXPECT_ANY_THROW(task->Next());
    task.reset();
    EXPECT_TRUE(weak_task.expired());
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

// Test CSearchFilterOnly for two-stage search
// This tests the filter-only search path where we only execute the filter
// and return valid_count without performing actual vector search
TEST(FilterOnlySearchTest, CSearchFilterOnlyBasic) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    int dim = 16;
    int N = 1000;
    // DataGen produces age values 0..N-1, so age >= 100 matches rows
    // 100..999 = exactly 900 rows.  An inverted-bitset bug would yield 100.
    int64_t filter_threshold = 100;
    int expected_valid_count = N - filter_threshold;  // 900

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto raw_data = DataGen(schema, N);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Create a search plan with filter: age >= 100
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                    unary_range_expr: <
                                        column_info: <
                                            field_id: 101
                                            data_type: Int64
                                        >
                                        op: GreaterEqual
                                        value: <
                                            int64_val: 100
                                        >
                                    >
                                >
                                query_info: <
                                    topk: 10
                                    metric_type: "L2"
                                    search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
            >)";

    proto::plan::PlanNode plan_node;
    auto ok =
        google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
    ASSERT_TRUE(ok);

    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_NE(plan, nullptr);

    // Execute filter-only search
    auto search_result = segment->Search(plan.get(),
                                         nullptr,
                                         MAX_TIMESTAMP,
                                         folly::CancellationToken(),
                                         0,
                                         0,
                                         0,
                                         true);

    // Verify filter-only results
    ASSERT_NE(search_result, nullptr);
    EXPECT_EQ(search_result->valid_count_, expected_valid_count);

    // In filter-only mode, distances and seg_offsets should be empty
    EXPECT_TRUE(search_result->distances_.empty());
    EXPECT_TRUE(search_result->seg_offsets_.empty());
}

// Test CSearchFilterOnly with no filter (all rows should be valid)
TEST(FilterOnlySearchTest, CSearchFilterOnlyNoFilter) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    int dim = 16;
    int N = 500;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto raw_data = DataGen(schema, N);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Create a search plan without filter
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                query_info: <
                                    topk: 10
                                    metric_type: "L2"
                                    search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
            >)";

    proto::plan::PlanNode plan_node;
    auto ok =
        google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
    ASSERT_TRUE(ok);

    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_NE(plan, nullptr);

    // Execute filter-only search
    auto search_result = segment->Search(plan.get(),
                                         nullptr,
                                         MAX_TIMESTAMP,
                                         folly::CancellationToken(),
                                         0,
                                         0,
                                         0,
                                         true);

    // Without filter, all rows should be valid
    ASSERT_NE(search_result, nullptr);
    EXPECT_EQ(search_result->valid_count_, N);
}

// Test CSearchFilterOnly with filter that matches no rows
TEST(FilterOnlySearchTest, CSearchFilterOnlyNoMatch) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    int dim = 16;
    int N = 500;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto raw_data = DataGen(schema, N);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Create a search plan with filter that matches nothing: age > 10000
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                    unary_range_expr: <
                                        column_info: <
                                            field_id: 101
                                            data_type: Int64
                                        >
                                        op: GreaterThan
                                        value: <
                                            int64_val: 10000
                                        >
                                    >
                                >
                                query_info: <
                                    topk: 10
                                    metric_type: "L2"
                                    search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
            >)";

    proto::plan::PlanNode plan_node;
    auto ok =
        google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
    ASSERT_TRUE(ok);

    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_NE(plan, nullptr);

    // Execute filter-only search
    auto search_result = segment->Search(plan.get(),
                                         nullptr,
                                         MAX_TIMESTAMP,
                                         folly::CancellationToken(),
                                         0,
                                         0,
                                         0,
                                         true);

    // Filter matches nothing, valid_count should be 0
    ASSERT_NE(search_result, nullptr);
    EXPECT_EQ(search_result->valid_count_, 0);
}

// Test ExtractFilterOnlyPlan function with various inputs
TEST(ExtractFilterOnlyPlanTest, NullInput) {
    using namespace milvus::query;

    // Test with nullptr input
    auto result = ProtoParser::ExtractFilterOnlyPlan(nullptr);
    EXPECT_EQ(result, nullptr);
}

TEST(ExtractFilterOnlyPlanTest, VectorSearchNodeWithNoSources) {
    using namespace milvus::query;

    // Create a VectorSearchNode without any sources
    auto vector_search_node =
        std::make_shared<milvus::plan::VectorSearchNode>("test_vector_search");

    // ExtractFilterOnlyPlan should return nullptr because there are no sources
    auto result = ProtoParser::ExtractFilterOnlyPlan(vector_search_node);
    EXPECT_EQ(result, nullptr);
}

TEST(ExtractFilterOnlyPlanTest, VectorSearchNodeWithSources) {
    using namespace milvus::query;

    // Create a filter node (MvccNode) as the source
    auto mvcc_node = std::make_shared<milvus::plan::MvccNode>("mvcc_node");

    // Create a VectorSearchNode with the filter node as source
    std::vector<milvus::plan::PlanNodePtr> sources;
    sources.push_back(mvcc_node);
    auto vector_search_node = std::make_shared<milvus::plan::VectorSearchNode>(
        "test_vector_search", std::move(sources));

    // ExtractFilterOnlyPlan should return the source subtree (mvcc_node)
    auto result = ProtoParser::ExtractFilterOnlyPlan(vector_search_node);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->name(), "MvccNode");
}

TEST(ExtractFilterOnlyPlanTest, NonVectorSearchNode) {
    using namespace milvus::query;

    // Create a node that is not a VectorSearchNode (e.g., MvccNode)
    auto mvcc_node = std::make_shared<milvus::plan::MvccNode>("test_mvcc");

    // ExtractFilterOnlyPlan should return nullptr because there's no VectorSearchNode
    auto result = ProtoParser::ExtractFilterOnlyPlan(mvcc_node);
    EXPECT_EQ(result, nullptr);
}

TEST(ExtractFilterOnlyPlanTest, NestedVectorSearchNode) {
    using namespace milvus::query;

    // Create a filter node chain: MvccNode -> FilterBitsNode
    auto mvcc_node = std::make_shared<milvus::plan::MvccNode>("mvcc_node");

    // Create a VectorSearchNode with the filter node as source
    std::vector<milvus::plan::PlanNodePtr> sources;
    sources.push_back(mvcc_node);
    auto vector_search_node = std::make_shared<milvus::plan::VectorSearchNode>(
        "vector_search", std::move(sources));

    // Wrap VectorSearchNode in another node (e.g., SearchGroupByNode)
    std::vector<milvus::plan::PlanNodePtr> group_sources;
    group_sources.push_back(vector_search_node);
    auto group_by_node = std::make_shared<milvus::plan::SearchGroupByNode>(
        "group_by", std::move(group_sources));

    // ExtractFilterOnlyPlan should find the VectorSearchNode and return its source
    auto result = ProtoParser::ExtractFilterOnlyPlan(group_by_node);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->name(), "MvccNode");
}

// Test GetSearchResultValidCount C API function
TEST(GetSearchResultValidCountTest, NullInput) {
    // Test with nullptr input
    int64_t result = GetSearchResultValidCount(nullptr);
    EXPECT_EQ(result, -1);
}

TEST(GetSearchResultValidCountTest, ValidSearchResult) {
    // Create a SearchResult with valid_count set
    auto search_result = new milvus::SearchResult();
    search_result->valid_count_ = 42;

    int64_t result = GetSearchResultValidCount(search_result);
    EXPECT_EQ(result, 42);

    delete search_result;
}

TEST(GetSearchResultValidCountTest, DefaultValidCount) {
    // Create a SearchResult with default valid_count (-1)
    auto search_result = new milvus::SearchResult();

    int64_t result = GetSearchResultValidCount(search_result);
    EXPECT_EQ(result, -1);

    delete search_result;
}

// Test filter-only search on an empty segment (active_count == 0)
TEST(FilterOnlySearchTest, CSearchFilterOnlyEmptySegment) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    int dim = 16;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    // Create an empty growing segment (no data inserted)
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    // Create a search plan with filter
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                predicates: <
                                    unary_range_expr: <
                                        column_info: <
                                            field_id: 101
                                            data_type: Int64
                                        >
                                        op: GreaterThan
                                        value: <
                                            int64_val: 500
                                        >
                                    >
                                >
                                query_info: <
                                    topk: 10
                                    metric_type: "L2"
                                    search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
            >)";

    proto::plan::PlanNode plan_node;
    auto ok =
        google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
    ASSERT_TRUE(ok);

    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_NE(plan, nullptr);

    // Execute filter-only search on empty segment
    auto search_result = segment->Search(plan.get(),
                                         nullptr,
                                         MAX_TIMESTAMP,
                                         folly::CancellationToken(),
                                         0,
                                         0,
                                         0,
                                         true);

    // Empty segment should return valid_count = 0
    ASSERT_NE(search_result, nullptr);
    EXPECT_EQ(search_result->valid_count_, 0);
}
