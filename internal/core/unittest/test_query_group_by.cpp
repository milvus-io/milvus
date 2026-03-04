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
        auto vec_fid = schema_->AddDebugField(
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
    //set up mvcc_node + agg_node: global aggregation no need project column
    PlanNodePtr mvcc_node = std::make_shared<milvus::plan::MvccNode>(
        milvus::plan::GetNextPlanNodeId(), sources);
    sources = std::vector<milvus::plan::PlanNodePtr>{mvcc_node};
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
        int invalid_count = 0;
        for (int i = 0; i < valid_data_size; i++) {
            if (field_data.valid_data(i)) {
                valid_count++;
            } else {
                invalid_count++;
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