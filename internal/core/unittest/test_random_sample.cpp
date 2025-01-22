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
    Assert(field.scalars().long_data().data_size() ==
           static_cast<int>(N * sample_factor));
}

TEST_P(RandomSampleTest, SampleWithTermAllhit) {
    double sample_factor = GetParam();

    auto schema = std::make_shared<Schema>();
    auto fid_64 = schema->AddDebugField("i64", DataType::INT64);
    schema->set_primary_field_id(fid_64);

    const int64_t N = 3000;
    auto dataset = DataGen(schema, N);
    auto segment = CreateSealedSegment(schema);
    SealedLoadFieldData(dataset, *segment);

    auto i64_col = dataset.get_col<int64_t>(fid_64);
    std::vector<proto::plan::GenericValue> values;
    for (int i = 0; i < N; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i64_col[i]);
        values.push_back(val);
    }

    auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
        milvus::expr::ColumnInfo(
            fid_64, DataType::INT64, std::vector<std::string>()),
        values);
    auto plan = std::make_unique<query::RetrievePlan>(*schema);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ =
        milvus::test::CreateRetrievePlanForRandomSample(sample_factor,
                                                        term_expr);
    std::vector<FieldId> target_offsets{fid_64};
    plan->field_ids_ = target_offsets;

    auto retrieve_results = RetrieveWithDefaultOutputSizeAndLargeTimestamp(
        segment.get(), plan.get());
    Assert(retrieve_results->fields_data_size() == target_offsets.size());
    auto field = retrieve_results->fields_data(0);
    Assert(field.scalars().long_data().data_size() ==
           static_cast<int>(N * sample_factor));
}