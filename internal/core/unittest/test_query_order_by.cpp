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
#include "common/Consts.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::plan;

class QueryOrderByTest : public testing::TestWithParam<bool> {
 public:
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
        field_map_[int8_field] = int8_fid;
        field_map_[int16_field] = int16_fid;
        field_map_[int32_field] = int32_fid;
        field_map_[int64_field] = int64_fid;
        field_map_[float_field] = float_fid;
        field_map_[double_field] = double_fid;
        field_map_[string_field] = str_fid;
        field_map_[vector_field] = vector_fid;
        schema_->set_primary_field_id(str_fid);

        num_rows_ = 20;
        auto raw_data =
            DataGen(schema_, num_rows_, 42, 0, 1, 10, false, false, false);

        auto segment = CreateSealedWithFieldDataLoaded(schema_, raw_data);
        segment_ = SegmentSealedSPtr(segment.release());

        milvus::exec::expression::FunctionFactory& factory =
            milvus::exec::expression::FunctionFactory::Instance();
        factory.Initialize();
    }

    void
    TearDown() override {
    }

    // Helper: create RetrievePlan with ORDER BY settings
    std::unique_ptr<query::RetrievePlan>
    createOrderByPlan(PlanNodePtr top_node,
                      int64_t limit,
                      const std::vector<FieldId>& pipeline_field_ids,
                      const std::vector<FieldId>& deferred_field_ids = {}) {
        auto retrieve_plan = std::make_unique<query::RetrievePlan>(schema_);
        retrieve_plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
        retrieve_plan->plan_node_->plannodes_ = top_node;
        retrieve_plan->plan_node_->limit_ = limit;
        retrieve_plan->plan_node_->has_order_by_ = true;
        retrieve_plan->plan_node_->pipeline_field_ids_ = pipeline_field_ids;
        retrieve_plan->plan_node_->deferred_field_ids_ = deferred_field_ids;
        return retrieve_plan;
    }

    // Helper: build MvccNode -> ProjectNode -> OrderByNode chain
    // Projects specified fields + SegmentOffsetFieldID, sorts by sort_field
    PlanNodePtr
    buildOrderByPlan(const std::string& sort_field_name,
                     const std::vector<std::string>& project_field_names,
                     bool ascending,
                     bool nulls_first,
                     int64_t limit,
                     std::vector<FieldId>& out_pipeline_ids) {
        // MvccNode
        std::vector<PlanNodePtr> sources;
        PlanNodePtr mvcc_node =
            std::make_shared<MvccNode>(GetNextPlanNodeId(), sources);
        sources = {mvcc_node};

        // Collect projected field IDs and types
        std::vector<FieldId> field_ids;
        std::vector<std::string> field_names;
        std::vector<DataType> field_types;
        for (auto& name : project_field_names) {
            auto fid = field_map_[name];
            field_ids.push_back(fid);
            field_names.push_back(name);
            field_types.push_back(schema_->operator[](fid).get_data_type());
        }
        // Append SegmentOffsetFieldID as the last pipeline column
        field_ids.push_back(SegmentOffsetFieldID);
        field_names.push_back("__segment_offset__");
        field_types.push_back(DataType::INT64);

        out_pipeline_ids = field_ids;

        // ProjectNode
        PlanNodePtr project_node =
            std::make_shared<ProjectNode>(GetNextPlanNodeId(),
                                          std::vector<FieldId>(field_ids),
                                          std::vector<std::string>(field_names),
                                          std::vector<DataType>(field_types),
                                          sources);
        sources = {project_node};

        // OrderByNode
        std::vector<expr::FieldAccessTypeExprPtr> sorting_keys;
        auto sort_fid = field_map_[sort_field_name];
        auto sort_type = schema_->operator[](sort_fid).get_data_type();
        sorting_keys.emplace_back(
            std::make_shared<const expr::FieldAccessTypeExpr>(
                sort_type, sort_field_name, sort_fid));

        std::vector<SortOrder> sorting_orders;
        sorting_orders.emplace_back(ascending, nulls_first);

        PlanNodePtr order_by_node =
            std::make_shared<OrderByNode>(GetNextPlanNodeId(),
                                          std::move(sorting_keys),
                                          std::move(sorting_orders),
                                          limit,
                                          sources);

        return order_by_node;
    }

 public:
    int64_t num_rows_{0};
    SegmentSealedSPtr segment_;
    std::shared_ptr<Schema> schema_;
    std::map<std::string, FieldId> field_map_;
};

INSTANTIATE_TEST_SUITE_P(QueryOrderBySuite,
                         QueryOrderByTest,
                         ::testing::Values(true, false));

TEST_P(QueryOrderByTest, OrderBySingleInt64Asc) {
    auto nullable = GetParam();
    std::vector<FieldId> pipeline_ids;
    auto top_node = buildOrderByPlan(
        int64_field,
        {string_field, int64_field},  // project PK + sort field
        true,                         // ASC
        false,                        // NULLS LAST
        10,
        pipeline_ids);

    auto plan = createOrderByPlan(top_node, 10, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    // Should have at least 2 fields (string PK + int64)
    ASSERT_GE(results->fields_data_size(), 2);

    // Verify int64 column is sorted ASC
    auto& int64_data = results->fields_data(1);
    auto count = int64_data.scalars().long_data().data_size();
    ASSERT_GT(count, 0);
    ASSERT_LE(count, 10);

    if (!nullable) {
        for (int i = 1; i < count; i++) {
            EXPECT_LE(int64_data.scalars().long_data().data(i - 1),
                      int64_data.scalars().long_data().data(i))
                << "int64 values should be in ascending order at index " << i;
        }
    }
}

TEST_P(QueryOrderByTest, OrderBySingleInt64Desc) {
    auto nullable = GetParam();
    std::vector<FieldId> pipeline_ids;
    auto top_node = buildOrderByPlan(int64_field,
                                     {string_field, int64_field},
                                     false,  // DESC
                                     true,   // NULLS FIRST
                                     10,
                                     pipeline_ids);

    auto plan = createOrderByPlan(top_node, 10, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_GE(results->fields_data_size(), 2);

    auto& int64_data = results->fields_data(1);
    auto count = int64_data.scalars().long_data().data_size();
    ASSERT_GT(count, 0);

    if (!nullable) {
        for (int i = 1; i < count; i++) {
            EXPECT_GE(int64_data.scalars().long_data().data(i - 1),
                      int64_data.scalars().long_data().data(i))
                << "int64 values should be in descending order at index " << i;
        }
    }
}

TEST_P(QueryOrderByTest, OrderByDoubleAsc) {
    auto nullable = GetParam();
    std::vector<FieldId> pipeline_ids;
    auto top_node = buildOrderByPlan(double_field,
                                     {string_field, double_field},
                                     true,   // ASC
                                     false,  // NULLS LAST
                                     num_rows_,
                                     pipeline_ids);

    auto plan = createOrderByPlan(top_node, num_rows_, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_GE(results->fields_data_size(), 2);

    auto& double_data = results->fields_data(1);
    auto count = double_data.scalars().double_data().data_size();
    ASSERT_GT(count, 0);

    if (!nullable) {
        for (int i = 1; i < count; i++) {
            EXPECT_LE(double_data.scalars().double_data().data(i - 1),
                      double_data.scalars().double_data().data(i))
                << "double values should be in ascending order at index " << i;
        }
    }
}

TEST_P(QueryOrderByTest, OrderByStringAsc) {
    std::vector<FieldId> pipeline_ids;
    // string_field is PK and always non-nullable
    auto top_node = buildOrderByPlan(string_field,
                                     {string_field},
                                     true,   // ASC
                                     false,  // NULLS LAST
                                     num_rows_,
                                     pipeline_ids);

    auto plan = createOrderByPlan(top_node, num_rows_, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_GE(results->fields_data_size(), 1);

    auto& str_data = results->fields_data(0);
    auto count = str_data.scalars().string_data().data_size();
    ASSERT_GT(count, 0);

    for (int i = 1; i < count; i++) {
        EXPECT_LE(str_data.scalars().string_data().data(i - 1),
                  str_data.scalars().string_data().data(i))
            << "string values should be in ascending order at index " << i;
    }
}

TEST_P(QueryOrderByTest, OrderByWithLimit) {
    std::vector<FieldId> pipeline_ids;
    int64_t limit = 5;
    auto top_node = buildOrderByPlan(int64_field,
                                     {string_field, int64_field},
                                     true,   // ASC
                                     false,  // NULLS LAST
                                     limit,
                                     pipeline_ids);

    auto plan = createOrderByPlan(top_node, limit, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_GE(results->fields_data_size(), 2);

    auto& int64_data = results->fields_data(1);
    auto count = int64_data.scalars().long_data().data_size();
    // Should return at most `limit` rows
    EXPECT_LE(count, limit);
    EXPECT_GT(count, 0);
}

TEST_P(QueryOrderByTest, OrderByWithDeferredFields) {
    auto nullable = GetParam();
    // Two-project mode: project PK + sort field in pipeline,
    // defer double_field for late materialization
    std::vector<FieldId> pipeline_ids;
    auto top_node =
        buildOrderByPlan(int64_field,
                         {string_field, int64_field},  // pipeline fields
                         true,                         // ASC
                         false,                        // NULLS LAST
                         10,
                         pipeline_ids);

    auto double_fid = field_map_[double_field];
    auto plan = createOrderByPlan(top_node, 10, pipeline_ids, {double_fid});

    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    // Should have 3 fields: string PK + int64 + deferred double
    ASSERT_GE(results->fields_data_size(), 3);

    // Verify the deferred double field is present
    auto& deferred_data = results->fields_data(2);
    auto count = deferred_data.scalars().double_data().data_size();
    EXPECT_GT(count, 0);
}

TEST_P(QueryOrderByTest, OrderByInt16Asc) {
    auto nullable = GetParam();
    std::vector<FieldId> pipeline_ids;
    auto top_node = buildOrderByPlan(int16_field,
                                     {string_field, int16_field},
                                     true,   // ASC
                                     false,  // NULLS LAST
                                     num_rows_,
                                     pipeline_ids);

    auto plan = createOrderByPlan(top_node, num_rows_, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_GE(results->fields_data_size(), 2);

    auto& int_data = results->fields_data(1);
    // INT16 is stored as INT32 in protobuf
    auto count = int_data.scalars().int_data().data_size();
    ASSERT_GT(count, 0);

    if (!nullable) {
        for (int i = 1; i < count; i++) {
            EXPECT_LE(int_data.scalars().int_data().data(i - 1),
                      int_data.scalars().int_data().data(i))
                << "int16 values should be in ascending order at index " << i;
        }
    }
}

TEST_P(QueryOrderByTest, OrderByFloatDesc) {
    auto nullable = GetParam();
    std::vector<FieldId> pipeline_ids;
    auto top_node = buildOrderByPlan(float_field,
                                     {string_field, float_field},
                                     false,  // DESC
                                     true,   // NULLS FIRST
                                     num_rows_,
                                     pipeline_ids);

    auto plan = createOrderByPlan(top_node, num_rows_, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_GE(results->fields_data_size(), 2);

    auto& float_data = results->fields_data(1);
    auto count = float_data.scalars().float_data().data_size();
    ASSERT_GT(count, 0);

    if (!nullable) {
        for (int i = 1; i < count; i++) {
            EXPECT_GE(float_data.scalars().float_data().data(i - 1),
                      float_data.scalars().float_data().data(i))
                << "float values should be in descending order at index " << i;
        }
    }
}

TEST_P(QueryOrderByTest, OrderByUnlimited) {
    // limit = -1 means return all rows
    std::vector<FieldId> pipeline_ids;
    auto top_node = buildOrderByPlan(int64_field,
                                     {string_field, int64_field},
                                     true,   // ASC
                                     false,  // NULLS LAST
                                     -1,     // unlimited
                                     pipeline_ids);

    auto plan = createOrderByPlan(top_node, -1, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_GE(results->fields_data_size(), 2);

    auto& int64_data = results->fields_data(1);
    auto count = int64_data.scalars().long_data().data_size();
    // Should return all non-null rows
    EXPECT_GT(count, 0);
}

TEST_P(QueryOrderByTest, OrderByPopulatesIDs) {
    // Verify that FillOrderByResult populates the IDs field from PK
    std::vector<FieldId> pipeline_ids;
    auto top_node = buildOrderByPlan(int64_field,
                                     {string_field, int64_field},
                                     true,
                                     false,
                                     10,
                                     pipeline_ids);

    auto plan = createOrderByPlan(top_node, 10, pipeline_ids);
    auto results = segment_->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    // PK is VARCHAR, so IDs should be populated as str_id
    ASSERT_TRUE(results->has_ids());
    auto& ids = results->ids();
    ASSERT_TRUE(ids.has_str_id());
    auto pk_count = ids.str_id().data_size();
    EXPECT_GT(pk_count, 0);

    // IDs count should match fields_data row count
    auto& first_field = results->fields_data(0);
    auto field_count = first_field.scalars().string_data().data_size();
    EXPECT_EQ(pk_count, field_count);
}

TEST_P(QueryOrderByTest, OrderByMultiKey) {
    // Compound ORDER BY: int8 ASC, int16 DESC
    // Use integer types for both sort keys because DataGen's DOUBLE/FLOAT
    // always uses random values (ignoring random_val), making deterministic
    // secondary sort verification impossible.
    //
    // With random_val=true, repeat_count=1, N=100:
    // - VARCHAR PK: unique random strings (repeat_count=1 guarantees uniqueness)
    // - INT8: random() % 200 cast to int8_t → small range, many collisions
    // - INT16: random() % 200 → different values within INT8 groups
    auto nullable = GetParam();
    int64_t N = 100;
    auto raw_data = DataGen(schema_,
                            N,
                            /*seed=*/99,
                            /*ts_offset=*/0,
                            /*repeat_count=*/1,
                            /*array_len=*/10,
                            /*group_count=*/false,
                            /*random_pk=*/false,
                            /*random_val=*/true);
    auto local_segment = SegmentSealedSPtr(
        CreateSealedWithFieldDataLoaded(schema_, raw_data).release());

    // MvccNode
    std::vector<PlanNodePtr> sources;
    PlanNodePtr mvcc_node =
        std::make_shared<MvccNode>(GetNextPlanNodeId(), sources);
    sources = {mvcc_node};

    // Project: PK (string) + int8 + int16 + SegmentOffset
    auto str_fid = field_map_[string_field];
    auto i8_fid = field_map_[int8_field];
    auto i16_fid = field_map_[int16_field];

    std::vector<FieldId> field_ids = {
        str_fid, i8_fid, i16_fid, SegmentOffsetFieldID};
    std::vector<std::string> field_names = {
        string_field, int8_field, int16_field, "__segment_offset__"};
    std::vector<DataType> field_types = {
        schema_->operator[](str_fid).get_data_type(),
        schema_->operator[](i8_fid).get_data_type(),
        schema_->operator[](i16_fid).get_data_type(),
        DataType::INT64};
    auto pipeline_ids = field_ids;

    PlanNodePtr project_node =
        std::make_shared<ProjectNode>(GetNextPlanNodeId(),
                                      std::vector<FieldId>(field_ids),
                                      std::vector<std::string>(field_names),
                                      std::vector<DataType>(field_types),
                                      sources);
    sources = {project_node};

    // OrderByNode: int8 ASC NULLS LAST, int16 DESC NULLS FIRST
    std::vector<expr::FieldAccessTypeExprPtr> sorting_keys;
    sorting_keys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        schema_->operator[](i8_fid).get_data_type(), int8_field, i8_fid));
    sorting_keys.emplace_back(std::make_shared<const expr::FieldAccessTypeExpr>(
        schema_->operator[](i16_fid).get_data_type(), int16_field, i16_fid));

    std::vector<SortOrder> sorting_orders;
    sorting_orders.emplace_back(true, false);  // int8 ASC, NULLS LAST
    sorting_orders.emplace_back(false, true);  // int16 DESC, NULLS FIRST

    PlanNodePtr order_by_node =
        std::make_shared<OrderByNode>(GetNextPlanNodeId(),
                                      std::move(sorting_keys),
                                      std::move(sorting_orders),
                                      N,
                                      sources);

    auto plan = createOrderByPlan(order_by_node, N, pipeline_ids);
    auto results = local_segment->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    // Should have 3 user fields: string PK + int8 + int16
    // INT8 and INT16 are both stored as int_data in protobuf
    ASSERT_GE(results->fields_data_size(), 3);

    auto& i8_data = results->fields_data(1);
    auto& i16_data = results->fields_data(2);
    auto count = i8_data.scalars().int_data().data_size();
    ASSERT_GT(count, 0);
    ASSERT_EQ(count, i16_data.scalars().int_data().data_size());

    if (!nullable) {
        // INT8 has small value range → guaranteed collisions in 100 rows.
        // Within each INT8 group, verify INT16 is sorted DESC.
        bool secondary_sort_tested = false;
        for (int i = 1; i < count; i++) {
            auto prev_i8 = i8_data.scalars().int_data().data(i - 1);
            auto curr_i8 = i8_data.scalars().int_data().data(i);
            EXPECT_LE(prev_i8, curr_i8)
                << "int8 should be ascending at index " << i;

            if (prev_i8 == curr_i8) {
                secondary_sort_tested = true;
                EXPECT_GE(i16_data.scalars().int_data().data(i - 1),
                          i16_data.scalars().int_data().data(i))
                    << "int16 should be descending within equal int8 group "
                       "at index "
                    << i;
            }
        }
        EXPECT_TRUE(secondary_sort_tested)
            << "No duplicate int8 values found — secondary sort was not tested";
    }
}
