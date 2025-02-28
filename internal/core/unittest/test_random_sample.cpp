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
#include <iostream>

#include "common/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "plan/PlanNode.h"

using namespace milvus;
using namespace milvus::segcore;

class RandomSampleTest : public ::testing::TestWithParam<double> {
 protected:
    void
    SetUp() override {
    }

    void
    TearDown() override {
    }
};

std::unique_ptr<proto::segcore::RetrieveResults>
RetrieveWithDefaultOutputSizeAndLargeTimestamp(
    SegmentInterface* segment, const query::RetrievePlan* plan) {
    return segment->Retrieve(
        nullptr, plan, 100000000, DEFAULT_MAX_OUTPUT_SIZE, false);
}

INSTANTIATE_TEST_SUITE_P(RandomSampleTest,
                         RandomSampleTest,
                         ::testing::Values(0.01, 0.1, 0.5, 0.9));

TEST_P(RandomSampleTest, SampleOnly) {
    double sample_factor = GetParam();

    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    schema->set_primary_field_id(fid_64);

    const int64_t N = 3000;
    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);

    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanForRandomSample(sample_factor);
    std::vector<FieldId> target_offsets{fid_64};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = RetrieveWithDefaultOutputSizeAndLargeTimestamp(
        segment.get(), plan.get());

    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field = retrieve_results->fields_data(0);
    int data_size = field.scalars().long_data().data_size();
    int expected_size = static_cast<int>(N * sample_factor);
    // We can accept size one difference due to the float point calculation in sampling.
    assert(expected_size - 1 <= data_size && data_size <= expected_size + 1);
}

TEST_P(RandomSampleTest, SampleWithUnaryFiler) {
    double sample_factor = GetParam();

    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    schema->set_primary_field_id(fid_64);
    fid_64 = schema->AddDebugField("integer", DataType::INT64);

    const int64_t N = 3000;
    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);

    auto size = dataset.raw_->mutable_fields_data()->size();
    auto i64_col = dataset.raw_->mutable_fields_data()
                       ->at(size - 1)
                       .mutable_scalars()
                       ->mutable_long_data()
                       ->mutable_data();
    for (int i = 0; i < N; ++i) {
        if (i % 3 == 0) {
            i64_col->at(i) = 1;
        } else if (i % 3 == 1) {
            i64_col->at(i) = 2;
        } else {
            i64_col->at(i) = 3;
        }
    }

    SealedLoadFieldData(dataset, *segment);

    milvus::proto::plan::GenericValue val;
    val.set_int64_val(1);
    auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        OpType::Equal,
        val,
        std::vector<proto::plan::GenericValue>{});
    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanForRandomSample(sample_factor, expr);
    std::vector<FieldId> target_offsets{fid_64};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = RetrieveWithDefaultOutputSizeAndLargeTimestamp(
        segment.get(), plan.get());
    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field = retrieve_results->fields_data(0);
    auto field_data = field.scalars().long_data();
    int data_size = field.scalars().long_data().data_size();

    for (int i = 0; i < data_size; i++) {
        ASSERT_EQ(field_data.data(i), 1);
    }

    int expected_size = static_cast<int>(N * sample_factor) / 3;
    // We can accept size one difference due to the float point calculation in sampling.
    assert(expected_size - 1 <= data_size && data_size <= expected_size + 1);
}

TEST(RandomSampleTest, SampleWithEmptyInput) {
    double sample_factor = 0.1;

    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    schema->set_primary_field_id(fid_64);
    fid_64 = schema->AddDebugField("integer", DataType::INT64);

    const int64_t N = 3000;
    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);

    SealedLoadFieldData(dataset, *segment);

    milvus::proto::plan::GenericValue val;
    val.set_int64_val(0);
    // Less than 0 will not match any data
    auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        OpType::LessThan,
        val,
        std::vector<proto::plan::GenericValue>{});
    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanForRandomSample(sample_factor, expr);
    std::vector<FieldId> target_offsets{fid_64};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = RetrieveWithDefaultOutputSizeAndLargeTimestamp(
        segment.get(), plan.get());
    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field = retrieve_results->fields_data(0);
    auto field_data = field.scalars().long_data();
    int data_size = field.scalars().long_data().data_size();

    assert(data_size == 0);
}