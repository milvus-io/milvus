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
#include <set>
#include "test_utils/DataGen.h"
#include "segcore/SegmentSealed.h"
#include "plan/PlanNode.h"
#include "plan/PlanNodeIdGenerator.h"
#include "test_utils/storage_test_utils.h"
#include "exec/expression/function/FunctionFactory.h"
#include "exec/operator/query-agg/CountAggregateBase.h"
#include "exec/HashTable.h"
#include "exec/VectorHasher.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::plan;

class QueryAggTest : public testing::TestWithParam<bool> {
 public:
    constexpr static const char bool_field[] = "bool";
    constexpr static const char int8_field[] = "int8";
    constexpr static const char int16_field[] = "int16";
    constexpr static const char int32_field[] = "int32";
    constexpr static const char int64_field[] = "int64";
    constexpr static const char float_field[] = "float";
    constexpr static const char double_field[] = "double";
    constexpr static const char string_field[] = "string";
    constexpr static const char vector_field[] = "vector";

 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto nullable = GetParam();
        auto bool_fid =
            schema_->AddDebugField(bool_field, DataType::BOOL, nullable);
        auto int8_fid =
            schema_->AddDebugField(int8_field, DataType::INT8, nullable);
        auto int16_fid =
            schema_->AddDebugField(int16_field, DataType::INT16, nullable);
        auto int32_fid =
            schema_->AddDebugField(int32_field, DataType::INT32, nullable);
        auto int64_fid =
            schema_->AddDebugField(int64_field, DataType::INT64, nullable);
        auto float_fid =
            schema_->AddDebugField(float_field, DataType::FLOAT, nullable);
        auto double_fid =
            schema_->AddDebugField(double_field, DataType::DOUBLE, nullable);
        auto str_fid = schema_->AddDebugField(string_field, DataType::VARCHAR);
        auto vector_fid = schema_->AddDebugField(
            vector_field, DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        field_map_[bool_field] = bool_fid;
        field_map_[int8_field] = int8_fid;
        field_map_[int16_field] = int16_fid;
        field_map_[int32_field] = int32_fid;
        field_map_[int64_field] = int64_fid;
        field_map_[float_field] = float_fid;
        field_map_[double_field] = double_fid;
        field_map_[string_field] = str_fid;
        field_map_[vector_field] = vector_fid;
        schema_->set_primary_field_id(str_fid);

        num_rows_ = 10;
        auto raw_data =
            DataGen(schema_, num_rows_, 42, 0, 2, 10, false, false, false);

        auto segment = CreateSealedWithFieldDataLoaded(schema_, raw_data);
        segment_ = SegmentSealedSPtr(segment.release());

        milvus::exec::expression::FunctionFactory& factory =
            milvus::exec::expression::FunctionFactory::Instance();
        factory.Initialize();
    }

    void
    TearDown() override {
    }

 public:
    int64_t num_rows_{0};
    SegmentSealedSPtr segment_;
    std::shared_ptr<Schema> schema_;
    std::map<std::string, FieldId> field_map_;
};

INSTANTIATE_TEST_SUITE_P(TaskTestSuite,
                         QueryAggTest,
                         ::testing::Values(true, false));

// Helper function to create RetrievePlan from aggregation plan node
std::unique_ptr<query::RetrievePlan>
createRetrievePlan(SchemaPtr schema,
                   milvus::plan::PlanNodePtr agg_node,
                   int64_t limit) {
    auto retrieve_plan = std::make_unique<query::RetrievePlan>(schema);
    retrieve_plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    retrieve_plan->plan_node_->plannodes_ = agg_node;
    retrieve_plan->plan_node_->limit_ = limit;
    return retrieve_plan;
}

TEST_P(QueryAggTest, GroupFixedLengthType) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto nullable = GetParam();
    //set up mvcc_node + project_node + agg_node
    // group by int16_field
    // mvcc node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int16_id = field_map_[int16_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int16_id},
        std::vector<std::string>{int16_field},
        std::vector<DataType>{DataType::INT16},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // agg node
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT16, int16_field, int16_id));
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(groupingKeys),
        std::vector<std::string>{},
        std::vector<plan::AggregationNode::Aggregate>{},
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    ASSERT_EQ(retrieve_results->fields_data_size(), 1);
    auto& field_data = retrieve_results->fields_data(0);
    auto result_size = field_data.scalars().int_data().data_size();

    if (nullable) {
        // as there are 10 values repeating 2 times, after groupby, at most 6 valid unique values will be returned
        EXPECT_EQ(result_size, 6);
    } else {
        EXPECT_EQ(result_size, 5);
    }

    if (!nullable) {
        std::set<int32_t> set;
        for (int i = 0; i < result_size; i++) {
            int32_t val = field_data.scalars().int_data().data(i);
            EXPECT_EQ(set.count(val), 0)
                << "there should not be any duplicated vals in the returned "
                   "column";
            set.insert(val);
        }
        EXPECT_EQ(set.size(), result_size);
    }
}

TEST_P(QueryAggTest, GroupFixedLengthMultipleColumn) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto nullable = GetParam();
    //set up mvcc_node + project_node + agg_node
    // group by int16_field and int32_field
    // mvcc node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int16_id = field_map_[int16_field];
    auto int32_id = field_map_[int32_field];
    auto int64_id = field_map_[int64_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int16_id, int32_id, int64_id},
        std::vector<std::string>{int16_field, int32_field, int64_field},
        std::vector<DataType>{
            DataType::INT16, DataType::INT32, DataType::INT64},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // agg node, group by int16, int32, sum(int64)
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT16, int16_field, int16_id));
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT32, int32_field, int32_id));
    std::string agg_name = "sum";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
        DataType::INT64, int64_field, int64_id);
    auto call = std::make_shared<const expr::CallExpr>(
        agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
    aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
    aggregates.back().rawInputTypes_.emplace_back(DataType::INT64);
    aggregates.back().resultType_ = GetAggResultType(agg_name, DataType::INT64);
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(groupingKeys),
        std::vector<std::string>{"sum"},
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    ASSERT_EQ(retrieve_results->fields_data_size(), 3);

    // Check all columns have the same size
    int size =
        retrieve_results->fields_data(0).scalars().int_data().data_size();
    EXPECT_EQ(retrieve_results->fields_data(1).scalars().int_data().data_size(),
              size);
    EXPECT_EQ(
        retrieve_results->fields_data(2).scalars().long_data().data_size(),
        size);

    if (nullable) {
        EXPECT_EQ(size, 6);
    } else {
        EXPECT_EQ(size, 5);
    }

    // Print values for debugging
    for (int j = 0; j < size; j++) {
        auto int16_val =
            retrieve_results->fields_data(0).scalars().int_data().data(j);
        auto int32_val =
            retrieve_results->fields_data(1).scalars().int_data().data(j);
        auto int64_val =
            retrieve_results->fields_data(2).scalars().long_data().data(j);
        std::cout << "int16_val:" << int16_val << " int32_val:" << int32_val
                  << " sum(int64):" << int64_val << std::endl;
    }
}

TEST_P(QueryAggTest, GroupVariableLengthMultipleColumn) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto nullable = GetParam();
    //set up mvcc_node + project_node + agg_node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int8_id = field_map_[int8_field];
    auto str_id = field_map_[string_field];
    auto float_id = field_map_[float_field];
    auto double_id = field_map_[double_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int8_id, str_id, float_id, double_id},
        std::vector<std::string>{
            int8_field, string_field, float_field, double_field},
        std::vector<DataType>{DataType::INT8,
                              DataType::VARCHAR,
                              DataType::FLOAT,
                              DataType::DOUBLE},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // group by int8_field, str_field, sum(float), sum(double)
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT8, int8_field, int8_id));
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::VARCHAR, string_field, str_id));
    std::string agg_name = "sum";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    // agg1: sum(float)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::FLOAT, float_field, float_id);
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::FLOAT);
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::FLOAT);
    }
    // agg2: sum(double)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::DOUBLE, double_field, double_id);
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::DOUBLE);
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::DOUBLE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(groupingKeys),
        std::vector<std::string>{"sum", "sum"},
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    ASSERT_EQ(retrieve_results->fields_data_size(), 4);

    // Check all columns have the same size
    int size =
        retrieve_results->fields_data(0).scalars().int_data().data_size();
    EXPECT_EQ(
        retrieve_results->fields_data(1).scalars().string_data().data_size(),
        size);
    EXPECT_EQ(
        retrieve_results->fields_data(2).scalars().double_data().data_size(),
        size);
    EXPECT_EQ(
        retrieve_results->fields_data(3).scalars().double_data().data_size(),
        size);

    if (nullable) {
        EXPECT_EQ(size, 10);
    } else {
        EXPECT_EQ(size, 5);
    }

    // Print values for debugging
    for (int j = 0; j < size; j++) {
        auto int8_val =
            retrieve_results->fields_data(0).scalars().int_data().data(j);
        auto str_val =
            retrieve_results->fields_data(1).scalars().string_data().data(j);
        auto float_sum =
            retrieve_results->fields_data(2).scalars().double_data().data(j);
        auto double_sum =
            retrieve_results->fields_data(3).scalars().double_data().data(j);
        std::cout << "int8_val:" << int8_val << " str_val:" << str_val
                  << " sum(float):" << float_sum
                  << " sum(double):" << double_sum << std::endl;
    }
}

TEST_P(QueryAggTest, CountAggTest) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    auto nullable = GetParam();
    //set up mvcc_node + project_node + agg_node
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
    // project node
    auto int8_id = field_map_[int8_field];
    auto str_id = field_map_[string_field];
    auto double_id = field_map_[double_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int8_id, str_id, double_id},
        std::vector<std::string>{int8_field, string_field, double_field},
        std::vector<DataType>{
            DataType::INT8, DataType::VARCHAR, DataType::DOUBLE},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};
    // group by int8_field, str_field, count(*), count(double)
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT8, int8_field, int8_id));
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::VARCHAR, string_field, str_id));
    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    //  count(*)
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    // count(double)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::DOUBLE, double_field, double_id);
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::DOUBLE);
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::DOUBLE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(groupingKeys),
        std::vector<std::string>{agg_name, agg_name},
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    ASSERT_EQ(retrieve_results->fields_data_size(), 4);

    // Check all columns have the same size
    int size =
        retrieve_results->fields_data(0).scalars().int_data().data_size();
    EXPECT_EQ(
        retrieve_results->fields_data(1).scalars().string_data().data_size(),
        size);
    EXPECT_EQ(
        retrieve_results->fields_data(2).scalars().long_data().data_size(),
        size);
    EXPECT_EQ(
        retrieve_results->fields_data(3).scalars().long_data().data_size(),
        size);

    if (nullable) {
        EXPECT_EQ(size, 10);
    } else {
        EXPECT_EQ(size, 5);
    }

    // Check the count values in column 2 (count(*))
    for (int j = 0; j < size; j++) {
        auto count_val =
            retrieve_results->fields_data(2).scalars().long_data().data(j);
        if (nullable) {
            // For nullable case, each count should be 1
            EXPECT_EQ(count_val, 1);
        } else {
            // For non-nullable case, each count should be 2
            EXPECT_EQ(count_val, 2);
        }
    }

    // Print values for debugging
    for (int j = 0; j < size; j++) {
        auto int8_val =
            retrieve_results->fields_data(0).scalars().int_data().data(j);
        auto str_val =
            retrieve_results->fields_data(1).scalars().string_data().data(j);
        auto count_star =
            retrieve_results->fields_data(2).scalars().long_data().data(j);
        auto count_double =
            retrieve_results->fields_data(3).scalars().long_data().data(j);
        std::cout << "int8_val:" << int8_val << " str_val:" << str_val
                  << " count(*):" << count_star
                  << " count(double):" << count_double << std::endl;
    }
}

TEST_P(QueryAggTest, GlobalCountAggTest) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    // MvccNode -> ProjectNode (empty fields) -> AggNode
    // ProjectNode is always created in production (PlanProto.cpp),
    // so tests should match that code path.
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};

    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{},
        std::vector<std::string>{},
        std::vector<DataType>{},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};

    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    //  count(*)
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<expr::FieldAccessTypeExprPtr>{},
        std::vector<std::string>{agg_name},
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    ASSERT_EQ(retrieve_results->fields_data_size(), 1);
    auto& field_data = retrieve_results->fields_data(0);
    ASSERT_EQ(field_data.scalars().long_data().data_size(), 1);
    auto actual_count = field_data.scalars().long_data().data(0);
    std::cout << "count:" << actual_count << std::endl;
    EXPECT_EQ(num_rows_, actual_count);
    // count(*) will always get all results' count no matter nullable or not
}

// Test count(*) when activeCount is zero
TEST_P(QueryAggTest, GlobalCountEmptyTest) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};

    // Add a FilterBitsNode with always-false expression (field IN empty set)
    // to filter out all rows
    auto str_id = field_map_[string_field];
    expr::ColumnInfo column_info(str_id, DataType::VARCHAR);
    auto always_false_expr = std::make_shared<expr::TermFilterExpr>(
        column_info, std::vector<proto::plan::GenericValue>{});
    PlanNodePtr filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        milvus::plan::GetNextPlanNodeId(), always_false_expr, sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{filter_node};

    // ProjectNode with empty field list to consume the filter bitmap,
    // matching the production code path (PlanProto.cpp always creates
    // ProjectNode when aggregation is present).
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{},
        std::vector<std::string>{},
        std::vector<DataType>{},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};

    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    //  count(*)
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<expr::FieldAccessTypeExprPtr>{},
        std::vector<std::string>{agg_name},
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    ASSERT_EQ(retrieve_results->fields_data_size(), 1);
    auto& field_data = retrieve_results->fields_data(0);
    ASSERT_EQ(field_data.scalars().long_data().data_size(), 1);
    auto actual_count = field_data.scalars().long_data().data(0);
    EXPECT_EQ(0, actual_count);
    // count(*) will get zero if no valid input into agg node
}

// Regression test for #47509: count(*) returns wrong result when queried
// together with count(nullable_field) in global aggregation (no GROUP BY).
// Before fix, populateTempVectors had a buggy special case that passed the
// nullable field column to count(*), making count(*) == count(field).
TEST_P(QueryAggTest, GlobalCountStarWithCountField) {
    auto nullable = GetParam();
    std::vector<milvus::plan::PlanNodePtr> sources;

    // MvccNode
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};

    // ProjectNode with double field (for count(double))
    auto double_id = field_map_[double_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{double_id},
        std::vector<std::string>{double_field},
        std::vector<DataType>{DataType::DOUBLE},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};

    // Global aggregation: count(*), count(double)
    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    // count(*)
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    // count(double)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::DOUBLE, double_field, double_id);
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::DOUBLE);
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::DOUBLE);
    }

    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<expr::FieldAccessTypeExprPtr>{},
        std::vector<std::string>{agg_name, agg_name},
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    ASSERT_EQ(retrieve_results->fields_data_size(), 2);
    auto count_star =
        retrieve_results->fields_data(0).scalars().long_data().data(0);
    auto count_double =
        retrieve_results->fields_data(1).scalars().long_data().data(0);

    std::cout << "GlobalCountStarWithCountField: count(*)=" << count_star
              << " count(double)=" << count_double << " nullable=" << nullable
              << std::endl;

    // count(*) must equal total rows regardless of nullable
    EXPECT_EQ(count_star, num_rows_);

    if (nullable) {
        // count(nullable_field) should be less than count(*)
        EXPECT_LT(count_double, count_star);
        EXPECT_GT(count_double, 0);
    } else {
        EXPECT_EQ(count_double, num_rows_);
    }
}

// Test ProjectNode with empty field list: when only count(*) is requested,
// ProjectNode has no fields to project but must still correctly report the
// row count via resize(selected_count).
TEST_P(QueryAggTest, CountStarOnlyGlobalWithProjectNode) {
    std::vector<milvus::plan::PlanNodePtr> sources;

    // MvccNode
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};

    // Empty ProjectNode (no fields projected)
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{},
        std::vector<std::string>{},
        std::vector<DataType>{},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};

    // Global count(*)
    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }

    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<expr::FieldAccessTypeExprPtr>{},
        std::vector<std::string>{agg_name},
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    ASSERT_EQ(retrieve_results->fields_data_size(), 1);
    auto& field_data = retrieve_results->fields_data(0);
    ASSERT_EQ(field_data.scalars().long_data().data_size(), 1);
    auto actual_count = field_data.scalars().long_data().data(0);
    std::cout << "CountStarOnlyGlobalWithProjectNode: count=" << actual_count
              << std::endl;
    EXPECT_EQ(num_rows_, actual_count);
}

// Test aggregation through segment->Retrieve() API to cover
// fillDataArrayFromColumnVector and bitmap unpacking logic
TEST_P(QueryAggTest, RetrieveAggregationWithValidityBitmap) {
    auto nullable = GetParam();

    // Build aggregation plan: MvccNode -> ProjectNode -> AggNode
    // Group by int64 field using existing segment_
    std::vector<milvus::plan::PlanNodePtr> sources;
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};

    // Project node with int64 field
    auto int64_id = field_map_[int64_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int64_id},
        std::vector<std::string>{int64_field},
        std::vector<DataType>{DataType::INT64},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};

    // Aggregation node: group by int64
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT64, int64_field, int64_id));
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(groupingKeys),
        std::vector<std::string>{},
        std::vector<plan::AggregationNode::Aggregate>{},
        sources);

    // Call segment_->Retrieve() which goes through ExecPlanNodeVisitor
    // and fillDataArrayFromColumnVector with bitmap unpacking
    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    // Verify results
    ASSERT_EQ(retrieve_results->fields_data_size(), 1);
    auto& field_data = retrieve_results->fields_data(0);

    // Check that valid_data is properly populated for nullable fields
    auto valid_data_size = field_data.valid_data_size();
    auto data_size = field_data.scalars().long_data().data_size();

    if (nullable) {
        ASSERT_GT(valid_data_size, 0)
            << "valid_data should be populated for nullable field";

        // Count valid and invalid entries
        int valid_count = 0;
        for (int i = 0; i < valid_data_size; i++) {
            if (field_data.valid_data(i)) {
                valid_count++;
            }
        }
        // With nullable fields, we expect some null values
        EXPECT_GT(valid_count, 0) << "Should have some valid values";
        // Note: The exact count depends on DataGen's null pattern
    } else {
        // Without nullable fields, valid_data may be empty (all valid)
        // or all entries should be true if populated
        if (valid_data_size > 0) {
            for (int i = 0; i < valid_data_size; i++) {
                EXPECT_TRUE(field_data.valid_data(i))
                    << "All values should be valid for non-nullable field";
            }
        }
    }

    // Verify the data values are present
    ASSERT_TRUE(field_data.has_scalars());
    EXPECT_GT(data_size, 0) << "Should have result data";
}

// ============================================================
// Direct unit tests for CountAggregate optimized paths
// ============================================================

// Helper: allocate a group row buffer with int64_t accumulator at given offset.
// Layout: [nullByte(1)] [padding(7)] [int64_t accumulator]
// Total 16 bytes, accumulator at offset 8 (8-byte aligned).
struct CountAggTestHelper {
    static constexpr int32_t kAccOffset = 8;
    static constexpr int32_t kNullByte = 0;
    static constexpr uint8_t kNullMask = 1;
    static constexpr int32_t kRowSizeOffset = 0;
    static constexpr size_t kRowSize = 16;

    static void
    setupAggregate(milvus::exec::CountAggregate& agg) {
        agg.setOffsets(kAccOffset, kNullByte, kNullMask, kRowSizeOffset);
    }

    static std::vector<char>
    makeGroupRow() {
        return std::vector<char>(kRowSize, 0);
    }

    static int64_t
    getCount(const std::vector<char>& row) {
        return *reinterpret_cast<const int64_t*>(row.data() + kAccOffset);
    }
};

// addSingleGroupRawInput: all valid (nullCount == 0)
TEST(CountAggregateTest, SingleGroupAllValid) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    auto row = CountAggTestHelper::makeGroupRow();
    char* group = row.data();
    const std::vector<milvus::vector_size_t> indices = {0};
    agg.initializeNewGroups(&group, indices);

    // 100 rows, all valid
    auto col =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64, 100);
    std::vector<milvus::VectorPtr> input = {col};
    agg.addSingleGroupRawInput(group, 100, input);

    EXPECT_EQ(CountAggTestHelper::getCount(row), 100);
}

// addSingleGroupRawInput: all null
TEST(CountAggregateTest, SingleGroupAllNull) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    auto row = CountAggTestHelper::makeGroupRow();
    char* group = row.data();
    const std::vector<milvus::vector_size_t> indices = {0};
    agg.initializeNewGroups(&group, indices);

    auto col =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64, 100);
    for (size_t i = 0; i < 100; i++) {
        col->nullAt(i);
    }
    std::vector<milvus::VectorPtr> input = {col};
    agg.addSingleGroupRawInput(group, 100, input);

    EXPECT_EQ(CountAggTestHelper::getCount(row), 0);
}

// addSingleGroupRawInput: mixed nulls
TEST(CountAggregateTest, SingleGroupMixedNulls) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    auto row = CountAggTestHelper::makeGroupRow();
    char* group = row.data();
    const std::vector<milvus::vector_size_t> indices = {0};
    agg.initializeNewGroups(&group, indices);

    // 100 rows, even indices are null
    auto col =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64, 100);
    for (size_t i = 0; i < 100; i += 2) {
        col->nullAt(i);
    }
    std::vector<milvus::VectorPtr> input = {col};
    agg.addSingleGroupRawInput(group, 100, input);

    EXPECT_EQ(CountAggTestHelper::getCount(row), 50);
}

// addSingleGroupRawInput: single row valid
TEST(CountAggregateTest, SingleGroupSingleRow) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    auto row = CountAggTestHelper::makeGroupRow();
    char* group = row.data();
    const std::vector<milvus::vector_size_t> indices = {0};
    agg.initializeNewGroups(&group, indices);

    auto col =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64, 1);
    std::vector<milvus::VectorPtr> input = {col};
    agg.addSingleGroupRawInput(group, 1, input);

    EXPECT_EQ(CountAggTestHelper::getCount(row), 1);
}

// addSingleGroupRawInput: non-64-multiple size (e.g., 65 rows)
TEST(CountAggregateTest, SingleGroupNon64Multiple) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    auto row = CountAggTestHelper::makeGroupRow();
    char* group = row.data();
    const std::vector<milvus::vector_size_t> indices = {0};
    agg.initializeNewGroups(&group, indices);

    // 65 rows, last row is null
    auto col =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64, 65);
    col->nullAt(64);
    std::vector<milvus::VectorPtr> input = {col};
    agg.addSingleGroupRawInput(group, 65, input);

    EXPECT_EQ(CountAggTestHelper::getCount(row), 64);
}

// addRawInput (GROUP BY): all valid, multiple groups
TEST(CountAggregateTest, GroupByAllValid) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    // 3 groups
    auto g0 = CountAggTestHelper::makeGroupRow();
    auto g1 = CountAggTestHelper::makeGroupRow();
    auto g2 = CountAggTestHelper::makeGroupRow();
    std::vector<char*> groups_vec = {
        g0.data(), g1.data(), g2.data(), g0.data(), g1.data(), g2.data()};
    char** groups = groups_vec.data();

    const std::vector<milvus::vector_size_t> indices = {0, 1, 2};
    char* init_groups[] = {g0.data(), g1.data(), g2.data()};
    agg.initializeNewGroups(init_groups, indices);

    // 6 rows, all valid, round-robin across 3 groups
    auto col =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64, 6);
    std::vector<milvus::VectorPtr> input = {col};
    agg.addRawInput(groups, 6, input);

    EXPECT_EQ(CountAggTestHelper::getCount(g0), 2);
    EXPECT_EQ(CountAggTestHelper::getCount(g1), 2);
    EXPECT_EQ(CountAggTestHelper::getCount(g2), 2);
}

// addRawInput (GROUP BY): mixed nulls with multiple groups
TEST(CountAggregateTest, GroupByMixedNulls) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    auto g0 = CountAggTestHelper::makeGroupRow();
    auto g1 = CountAggTestHelper::makeGroupRow();
    // rows: [g0, g1, g0, g1, g0, g1]
    // nulls: row 1 and 4 are null
    std::vector<char*> groups_vec = {
        g0.data(), g1.data(), g0.data(), g1.data(), g0.data(), g1.data()};
    char** groups = groups_vec.data();

    const std::vector<milvus::vector_size_t> indices = {0, 1};
    char* init_groups[] = {g0.data(), g1.data()};
    agg.initializeNewGroups(init_groups, indices);

    auto col =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64, 6);
    col->nullAt(1);  // g1 row null
    col->nullAt(4);  // g0 row null
    std::vector<milvus::VectorPtr> input = {col};
    agg.addRawInput(groups, 6, input);

    // g0: rows 0,2,4 -> row 4 null -> count 2
    // g1: rows 1,3,5 -> row 1 null -> count 2
    EXPECT_EQ(CountAggTestHelper::getCount(g0), 2);
    EXPECT_EQ(CountAggTestHelper::getCount(g1), 2);
}

// addRawInput (GROUP BY): all null
TEST(CountAggregateTest, GroupByAllNull) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    auto g0 = CountAggTestHelper::makeGroupRow();
    std::vector<char*> groups_vec = {g0.data(), g0.data(), g0.data()};
    char** groups = groups_vec.data();

    const std::vector<milvus::vector_size_t> indices = {0};
    char* init_groups[] = {g0.data()};
    agg.initializeNewGroups(init_groups, indices);

    auto col =
        std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64, 3);
    col->nullAt(0);
    col->nullAt(1);
    col->nullAt(2);
    std::vector<milvus::VectorPtr> input = {col};
    agg.addRawInput(groups, 3, input);

    EXPECT_EQ(CountAggTestHelper::getCount(g0), 0);
}

// addRawInput (GROUP BY): non-64-multiple size with mixed nulls
TEST(CountAggregateTest, GroupByNon64Multiple) {
    milvus::exec::CountAggregate agg;
    CountAggTestHelper::setupAggregate(agg);

    auto g0 = CountAggTestHelper::makeGroupRow();
    const std::vector<milvus::vector_size_t> indices = {0};
    char* init_groups[] = {g0.data()};
    agg.initializeNewGroups(init_groups, indices);

    // 65 rows all in one group, row 63 and 64 are null
    const int numRows = 65;
    std::vector<char*> groups_vec(numRows, g0.data());
    char** groups = groups_vec.data();

    auto col = std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64,
                                                      numRows);
    col->nullAt(63);
    col->nullAt(64);
    std::vector<milvus::VectorPtr> input = {col};
    agg.addRawInput(groups, numRows, input);

    EXPECT_EQ(CountAggTestHelper::getCount(g0), 63);
}

// Contract test: nullCount()-based path matches ValidAt() semantics
TEST(CountAggregateTest, NullCountMatchesValidAt) {
    // Verify that size() - nullCount() equals the number of ValidAt()==true
    for (int size : {1, 7, 63, 64, 65, 127, 128, 129, 200}) {
        auto col = std::make_shared<milvus::ColumnVector>(
            milvus::DataType::INT64, size);
        // null every 3rd element
        for (int i = 0; i < size; i += 3) {
            col->nullAt(i);
        }
        int64_t validAtCount = 0;
        for (int i = 0; i < size; i++) {
            if (col->ValidAt(i))
                validAtCount++;
        }
        EXPECT_EQ(col->size() - static_cast<int64_t>(col->nullCount()),
                  validAtCount)
            << "Mismatch at size=" << size;
    }
}

// Regression test for #47316: GROUP BY aggregation with empty result set
// When filter matches zero rows, outputRowCount() must handle null lookup_
// and setupRetrieveResult must return empty field_data arrays with correct schema.
TEST_P(QueryAggTest, GroupByEmptyResultSet) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};

    // FilterBitsNode with always-false expression to filter out all rows
    auto str_id = field_map_[string_field];
    expr::ColumnInfo column_info(str_id, DataType::VARCHAR);
    auto always_false_expr = std::make_shared<expr::TermFilterExpr>(
        column_info, std::vector<proto::plan::GenericValue>{});
    PlanNodePtr filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        milvus::plan::GetNextPlanNodeId(), always_false_expr, sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{filter_node};

    // ProjectNode with the grouping key field
    auto int16_id = field_map_[int16_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{int16_id},
        std::vector<std::string>{int16_field},
        std::vector<DataType>{DataType::INT16},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};

    // AggregationNode: GROUP BY int16, count(*)
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::INT16, int16_field, int16_id));
    std::string agg_name = "count";
    std::vector<plan::AggregationNode::Aggregate> aggregates;
    {
        auto call = std::make_shared<const expr::CallExpr>(
            agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType(agg_name, DataType::NONE);
    }
    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(groupingKeys),
        std::vector<std::string>{agg_name},
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    // Should return 2 empty field_data arrays (grouping key + count),
    // not crash or return 0 field_data arrays.
    ASSERT_EQ(retrieve_results->fields_data_size(), 2);
    EXPECT_EQ(retrieve_results->fields_data(0).scalars().int_data().data_size(),
              0);
    EXPECT_EQ(
        retrieve_results->fields_data(1).scalars().long_data().data_size(), 0);
}

// Regression test for #47316: GROUP BY with multiple aggregates and empty result
TEST_P(QueryAggTest, GroupByEmptyResultMultipleAggs) {
    std::vector<milvus::plan::PlanNodePtr> sources;
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};

    // Always-false filter
    auto str_id = field_map_[string_field];
    expr::ColumnInfo column_info(str_id, DataType::VARCHAR);
    auto always_false_expr = std::make_shared<expr::TermFilterExpr>(
        column_info, std::vector<proto::plan::GenericValue>{});
    PlanNodePtr filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        milvus::plan::GetNextPlanNodeId(), always_false_expr, sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{filter_node};

    // ProjectNode: group key (string) + agg input (int64, double)
    auto int64_id = field_map_[int64_field];
    auto double_id = field_map_[double_field];
    PlanNodePtr project_node = std::make_shared<milvus::plan::ProjectNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::vector<FieldId>{str_id, int64_id, double_id},
        std::vector<std::string>{string_field, int64_field, double_field},
        std::vector<DataType>{
            DataType::VARCHAR, DataType::INT64, DataType::DOUBLE},
        sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{project_node};

    // AggregationNode: GROUP BY string, count(*), sum(int64), max(double)
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
    groupingKeys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        DataType::VARCHAR, string_field, str_id));

    std::vector<plan::AggregationNode::Aggregate> aggregates;
    std::vector<std::string> agg_names;

    // count(*)
    {
        auto call = std::make_shared<const expr::CallExpr>(
            "count", std::vector<expr::TypedExprPtr>{}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().resultType_ =
            GetAggResultType("count", DataType::NONE);
        agg_names.push_back("count");
    }
    // sum(int64)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::INT64, int64_field, int64_id);
        auto call = std::make_shared<const expr::CallExpr>(
            "sum", std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::INT64);
        aggregates.back().resultType_ =
            GetAggResultType("sum", DataType::INT64);
        agg_names.push_back("sum");
    }
    // max(double)
    {
        auto agg_input = std::make_shared<expr::FieldAccessTypeExpr>(
            DataType::DOUBLE, double_field, double_id);
        auto call = std::make_shared<const expr::CallExpr>(
            "max", std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
        aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
        aggregates.back().rawInputTypes_.emplace_back(DataType::DOUBLE);
        aggregates.back().resultType_ =
            GetAggResultType("max", DataType::DOUBLE);
        agg_names.push_back("max");
    }

    PlanNodePtr agg_node = std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(groupingKeys),
        std::move(agg_names),
        std::move(aggregates),
        sources);

    auto retrieve_plan = createRetrievePlan(schema_, agg_node, num_rows_);
    auto retrieve_results = segment_->Retrieve(nullptr,
                                               retrieve_plan.get(),
                                               MAX_TIMESTAMP,
                                               DEFAULT_MAX_OUTPUT_SIZE,
                                               false);

    // 4 columns: string (group key) + count + sum + max, all empty
    ASSERT_EQ(retrieve_results->fields_data_size(), 4);
    EXPECT_EQ(
        retrieve_results->fields_data(0).scalars().string_data().data_size(),
        0);
    EXPECT_EQ(
        retrieve_results->fields_data(1).scalars().long_data().data_size(), 0);
    EXPECT_EQ(
        retrieve_results->fields_data(2).scalars().long_data().data_size(), 0);
    EXPECT_EQ(
        retrieve_results->fields_data(3).scalars().double_data().data_size(),
        0);
}

// ============================================================
// HashTable rehash and group limit tests
// ============================================================

namespace {
// Helper to create a HashTable with a single INT64 key column, no accumulators.
// Inserts 'numGroups' distinct INT64 values via groupProbe in batches.
// Returns the HashTable.
std::unique_ptr<milvus::exec::HashTable>
createAndInsertGroups(int64_t numGroups,
                      int64_t maxNumGroups,
                      int batchSize = 1024) {
    std::vector<milvus::exec::Accumulator> accumulators;
    std::vector<std::unique_ptr<milvus::exec::VectorHasher>> hashers;
    hashers.push_back(
        milvus::exec::VectorHasher::create(milvus::DataType::INT64, 0));
    auto table = std::make_unique<milvus::exec::HashTable>(
        std::move(hashers), accumulators, maxNumGroups);

    int64_t inserted = 0;
    while (inserted < numGroups) {
        int64_t thisCount = std::min((int64_t)batchSize, numGroups - inserted);
        auto col = std::make_shared<milvus::ColumnVector>(
            milvus::DataType::INT64, thisCount);
        auto* data = reinterpret_cast<int64_t*>(col->GetRawData());
        for (int64_t i = 0; i < thisCount; i++) {
            data[i] = inserted + i;
        }

        std::vector<milvus::DataType> dtypes = {milvus::DataType::INT64};
        std::vector<milvus::VectorPtr> children = {col};
        auto input = std::make_shared<milvus::RowVector>(children);

        milvus::exec::HashLookup lookup(table->hashers());
        table->prepareForGroupProbe(lookup, input);
        table->groupProbe(lookup);

        inserted += thisCount;
    }
    return table;
}
}  // namespace

TEST(HashTableRehashTest, TestHashTableRehashBasic) {
    // Insert 5000 distinct groups (exceeds initial 2048 capacity),
    // verify no crash and numDistinct == 5000
    const int64_t numGroups = 5000;
    auto table = createAndInsertGroups(numGroups, 1000000);
    ASSERT_NE(table->rows(), nullptr);
    EXPECT_EQ(table->rows()->allRows().size(), numGroups);
}

TEST(HashTableRehashTest, TestHashTableRehashCorrectness) {
    // Insert N groups, record row pointers.
    // Then re-probe the same keys and verify we get back the same row pointers.
    const int64_t numGroups = 3000;
    std::vector<milvus::exec::Accumulator> accumulators;
    std::vector<std::unique_ptr<milvus::exec::VectorHasher>> hashers;
    hashers.push_back(
        milvus::exec::VectorHasher::create(milvus::DataType::INT64, 0));
    auto table = std::make_unique<milvus::exec::HashTable>(
        std::move(hashers), accumulators, 1000000);

    // Insert numGroups distinct values
    auto col = std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64,
                                                      numGroups);
    auto* data = reinterpret_cast<int64_t*>(col->GetRawData());
    for (int64_t i = 0; i < numGroups; i++) {
        data[i] = i;
    }
    std::vector<milvus::VectorPtr> children = {col};
    auto input = std::make_shared<milvus::RowVector>(children);

    milvus::exec::HashLookup lookup1(table->hashers());
    table->prepareForGroupProbe(lookup1, input);
    table->groupProbe(lookup1);

    // Record row pointers
    std::vector<char*> firstHits(lookup1.hits_.begin(), lookup1.hits_.end());

    // Re-probe with the same keys — should get the same row pointers
    auto col2 = std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64,
                                                       numGroups);
    auto* data2 = reinterpret_cast<int64_t*>(col2->GetRawData());
    for (int64_t i = 0; i < numGroups; i++) {
        data2[i] = i;
    }
    std::vector<milvus::VectorPtr> children2 = {col2};
    auto input2 = std::make_shared<milvus::RowVector>(children2);

    milvus::exec::HashLookup lookup2(table->hashers());
    table->prepareForGroupProbe(lookup2, input2);
    table->groupProbe(lookup2);

    ASSERT_EQ(lookup2.hits_.size(), numGroups);
    for (int64_t i = 0; i < numGroups; i++) {
        EXPECT_EQ(lookup2.hits_[i], firstHits[i])
            << "Row pointer mismatch for group " << i;
    }
    // No new groups should have been created on re-probe
    EXPECT_TRUE(lookup2.newGroups_.empty());
}

TEST(HashTableRehashTest, TestHashTableMaxGroupsLimit) {
    // Set maxNumGroups=100, insert >100 groups, verify exception
    const int64_t maxGroups = 100;
    EXPECT_THROW(
        {
            try {
                createAndInsertGroups(200, maxGroups);
            } catch (const std::exception& e) {
                // Verify the error message mentions the limit
                std::string msg = e.what();
                EXPECT_NE(msg.find("too many groups"), std::string::npos)
                    << "Expected 'too many groups' in: " << msg;
                EXPECT_NE(msg.find("maxGroupByGroups"), std::string::npos)
                    << "Expected 'maxGroupByGroups' in: " << msg;
                throw;
            }
        },
        std::exception);
}

TEST(HashTableRehashTest, TestHashTableRehashMultipleRounds) {
    // Insert 50K groups forcing ~5 rehash rounds (2048->4096->...->65536)
    // Verify all groups found correctly
    const int64_t numGroups = 50000;
    auto table = createAndInsertGroups(numGroups, 1000000, 2048);
    ASSERT_NE(table->rows(), nullptr);
    EXPECT_EQ(table->rows()->allRows().size(), numGroups);

    // Verify we can look up all groups
    auto col = std::make_shared<milvus::ColumnVector>(milvus::DataType::INT64,
                                                      numGroups);
    auto* data = reinterpret_cast<int64_t*>(col->GetRawData());
    for (int64_t i = 0; i < numGroups; i++) {
        data[i] = i;
    }
    std::vector<milvus::VectorPtr> children = {col};
    auto input = std::make_shared<milvus::RowVector>(children);

    milvus::exec::HashLookup lookup(table->hashers());
    table->prepareForGroupProbe(lookup, input);
    table->groupProbe(lookup);

    // All should be existing groups, no new groups created
    EXPECT_TRUE(lookup.newGroups_.empty());
    // All hits should be non-null
    for (int64_t i = 0; i < numGroups; i++) {
        EXPECT_NE(lookup.hits_[i], nullptr)
            << "Group " << i << " not found after multiple rehashes";
    }
}