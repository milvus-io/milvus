// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/ArrowSealedSegment.h"

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/Consts.h"
#include "common/Schema.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanImpl.h"
#include "test_utils/GenExprProto.h"

namespace milvus::segcore {
namespace {

struct ArrowSegmentFixture {
    SchemaPtr schema;
    FieldId pk_fid;
    FieldId age_fid;
    FieldId name_fid;
    std::shared_ptr<ArrowSealedSegment> segment;
};

std::shared_ptr<arrow::Array>
BuildInt64Array(const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
    auto status = builder.AppendValues(values);
    EXPECT_TRUE(status.ok()) << status.ToString();

    std::shared_ptr<arrow::Array> array;
    status = builder.Finish(&array);
    EXPECT_TRUE(status.ok()) << status.ToString();
    return array;
}

std::shared_ptr<arrow::Array>
BuildStringArray(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    auto status = builder.AppendValues(values);
    EXPECT_TRUE(status.ok()) << status.ToString();

    std::shared_ptr<arrow::Array> array;
    status = builder.Finish(&array);
    EXPECT_TRUE(status.ok()) << status.ToString();
    return array;
}

std::shared_ptr<arrow::RecordBatch>
BuildBatch(const std::vector<int64_t>& pk,
           const std::vector<int64_t>& age,
           const std::vector<std::string>& name) {
    EXPECT_EQ(pk.size(), age.size());
    EXPECT_EQ(pk.size(), name.size());

    auto schema = arrow::schema({arrow::field("pk", arrow::int64()),
                                 arrow::field("age", arrow::int64()),
                                 arrow::field("name", arrow::utf8())});
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        BuildInt64Array(pk), BuildInt64Array(age), BuildStringArray(name)};
    return arrow::RecordBatch::Make(
        schema, static_cast<int64_t>(pk.size()), arrays);
}

ArrowSegmentFixture
BuildSegment() {
    ArrowSegmentFixture fixture;
    fixture.schema = std::make_shared<Schema>();
    fixture.pk_fid = fixture.schema->AddDebugField("pk", DataType::INT64);
    fixture.age_fid = fixture.schema->AddDebugField("age", DataType::INT64);
    fixture.name_fid = fixture.schema->AddDebugField("name", DataType::VARCHAR);
    fixture.schema->set_primary_field_id(fixture.pk_fid);

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {
        BuildBatch({100, 101}, {20, 35}, {"alice", "bob"}),
        BuildBatch({102, 103, 104}, {31, 29, 42}, {"cara", "dina", "eric"})};
    fixture.segment =
        ArrowSealedSegment::FromRecordBatches(fixture.schema, batches, 1001);
    return fixture;
}

std::shared_ptr<expr::UnaryRangeFilterExpr>
BuildAgeGreaterThanExpr(FieldId age_fid, int64_t threshold) {
    proto::plan::GenericValue value;
    value.set_int64_val(threshold);
    return std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(age_fid, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>{});
}

std::unique_ptr<query::RetrievePlan>
BuildRetrievePlan(const ArrowSegmentFixture& fixture) {
    auto plan = std::make_unique<query::RetrievePlan>(fixture.schema);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = milvus::test::CreateRetrievePlanByExpr(
        BuildAgeGreaterThanExpr(fixture.age_fid, 30));
    plan->field_ids_ = {fixture.pk_fid, fixture.name_fid};
    return plan;
}

}  // namespace

TEST(ArrowSealedSegmentTest, SegmentRowsAreAddressableAcrossArrowBatches) {
    auto fixture = BuildSegment();
    auto segment = fixture.segment;

    EXPECT_EQ(segment->get_row_count(), 5);
    EXPECT_TRUE(segment->is_chunked());
    EXPECT_EQ(segment->ArrowColumnGroupCountForTest(), 3);
    EXPECT_EQ(segment->ArrowFieldColumnGroupForTest(fixture.pk_fid), 0);
    EXPECT_EQ(segment->ArrowFieldColumnGroupForTest(fixture.age_fid), 1);
    EXPECT_EQ(segment->ArrowFieldColumnGroupForTest(fixture.name_fid), 2);
    EXPECT_EQ(segment->num_chunk_data(fixture.age_fid), 2);
    EXPECT_EQ(segment->num_rows_until_chunk(fixture.age_fid, 1), 2);
    EXPECT_EQ(segment->chunk_size(fixture.age_fid, 0), 2);
    EXPECT_EQ(segment->chunk_size(fixture.age_fid, 1), 3);

    EXPECT_EQ(segment->get_chunk_by_offset(fixture.age_fid, 1),
              (std::pair<int64_t, int64_t>{0, 1}));
    EXPECT_EQ(segment->get_chunk_by_offset(fixture.age_fid, 2),
              (std::pair<int64_t, int64_t>{1, 0}));
    EXPECT_EQ(segment->get_chunk_by_offset(fixture.age_fid, 4),
              (std::pair<int64_t, int64_t>{1, 2}));

    auto first_chunk =
        segment->chunk_data<int64_t>(nullptr, fixture.age_fid, 0);
    auto second_chunk =
        segment->chunk_data<int64_t>(nullptr, fixture.age_fid, 1);
    EXPECT_EQ(first_chunk.get()[1], 35);
    EXPECT_EQ(second_chunk.get()[0], 31);
    EXPECT_EQ(second_chunk.get()[2], 42);
}

TEST(ArrowSealedSegmentTest, SegcoreExprFiltersArrowBackedSegment) {
    auto fixture = BuildSegment();
    auto filter_node = std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, BuildAgeGreaterThanExpr(fixture.age_fid, 30));

    EXPECT_EQ(fixture.segment->ArrowNativeExprExecutionCountForTest(), 0);
    EXPECT_EQ(fixture.segment->ArrowRecordBatchReaderCreatedCountForTest(), 0);
    auto bitset = query::ExecuteQueryExpr(
        filter_node,
        fixture.segment.get(),
        fixture.segment->get_active_count(MAX_TIMESTAMP),
        MAX_TIMESTAMP);
    EXPECT_EQ(fixture.segment->ArrowNativeExprExecutionCountForTest(), 0);
    EXPECT_EQ(fixture.segment->ArrowRecordBatchReaderCreatedCountForTest(), 1);
    BitsetTypeView view(bitset);

    EXPECT_FALSE(view[0]);
    EXPECT_TRUE(view[1]);
    EXPECT_TRUE(view[2]);
    EXPECT_FALSE(view[3]);
    EXPECT_TRUE(view[4]);
}

TEST(ArrowSealedSegmentTest, SegcoreRetrieveUsesExprAndArrowFieldData) {
    auto fixture = BuildSegment();
    auto plan = BuildRetrievePlan(fixture);

    auto results = fixture.segment->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_EQ(results->offset().size(), 3);
    EXPECT_EQ(results->offset(0), 1);
    EXPECT_EQ(results->offset(1), 2);
    EXPECT_EQ(results->offset(2), 4);

    ASSERT_EQ(results->fields_data_size(), 2);
    const auto& pk_data = results->fields_data(0).scalars().long_data().data();
    ASSERT_EQ(pk_data.size(), 3);
    EXPECT_EQ(pk_data[0], 101);
    EXPECT_EQ(pk_data[1], 102);
    EXPECT_EQ(pk_data[2], 104);

    const auto& name_data =
        results->fields_data(1).scalars().string_data().data();
    ASSERT_EQ(name_data.size(), 3);
    EXPECT_EQ(name_data[0], "bob");
    EXPECT_EQ(name_data[1], "cara");
    EXPECT_EQ(name_data[2], "eric");
}

TEST(ArrowSealedSegmentTest, CacheLayerSimulatesRecordBatchLoadEvictAndPin) {
    auto fixture = BuildSegment();
    auto segment = fixture.segment;
    auto age_group = segment->ArrowFieldColumnGroupForTest(fixture.age_fid);
    segment->SetSimulatedLoadLatency(std::chrono::milliseconds(5));

    EXPECT_EQ(segment->ArrowRecordBatchLoadCount(age_group, 1), 0);

    const auto start = std::chrono::steady_clock::now();
    {
        auto pinned =
            segment->PinArrowRecordBatchForTest(nullptr, age_group, 1);
        ASSERT_NE(pinned.get(), nullptr);
        EXPECT_EQ(pinned.get()->num_columns(), 1);
        EXPECT_EQ(pinned.get()->schema()->field(0)->name(), "age");
        EXPECT_EQ(segment->ArrowRecordBatchLoadCount(age_group, 1), 1);
        EXPECT_FALSE(segment->EvictArrowRecordBatch(age_group, 1));
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    EXPECT_GE(elapsed.count(), 1);

    EXPECT_TRUE(segment->EvictArrowRecordBatch(age_group, 1));

    auto load_count = segment->ArrowRecordBatchLoadCount(age_group, 1);
    auto chunk = segment->chunk_data<int64_t>(nullptr, fixture.age_fid, 1);
    EXPECT_EQ(chunk.get()[2], 42);
    EXPECT_EQ(segment->ArrowRecordBatchLoadCount(age_group, 1), load_count + 1);
}

TEST(ArrowSealedSegmentTest,
     SegcoreFilterAndRetrieveReloadEvictedRecordBatchCells) {
    auto fixture = BuildSegment();
    auto pk_group =
        fixture.segment->ArrowFieldColumnGroupForTest(fixture.pk_fid);
    auto age_group =
        fixture.segment->ArrowFieldColumnGroupForTest(fixture.age_fid);
    auto name_group =
        fixture.segment->ArrowFieldColumnGroupForTest(fixture.name_fid);

    auto filter_node = std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, BuildAgeGreaterThanExpr(fixture.age_fid, 30));
    auto bitset = query::ExecuteQueryExpr(
        filter_node,
        fixture.segment.get(),
        fixture.segment->get_active_count(MAX_TIMESTAMP),
        MAX_TIMESTAMP);
    BitsetTypeView view(bitset);
    EXPECT_TRUE(view[1]);
    EXPECT_TRUE(view[2]);
    EXPECT_TRUE(view[4]);

    EXPECT_EQ(fixture.segment->ArrowRecordBatchLoadCount(age_group, 0), 1);
    EXPECT_EQ(fixture.segment->ArrowRecordBatchLoadCount(age_group, 1), 1);
    EXPECT_EQ(fixture.segment->ArrowRecordBatchLoadCount(pk_group, 0), 0);
    EXPECT_EQ(fixture.segment->ArrowRecordBatchLoadCount(name_group, 0), 0);
    ASSERT_TRUE(fixture.segment->EvictArrowRecordBatch(age_group, 1));

    auto stripe_1_loads =
        fixture.segment->ArrowRecordBatchLoadCount(age_group, 1);
    auto plan = BuildRetrievePlan(fixture);
    auto results = fixture.segment->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

    ASSERT_EQ(results->offset().size(), 3);
    EXPECT_EQ(results->offset(0), 1);
    EXPECT_EQ(results->offset(1), 2);
    EXPECT_EQ(results->offset(2), 4);
    EXPECT_EQ(fixture.segment->ArrowRecordBatchLoadCount(age_group, 1),
              stripe_1_loads + 1);

    EXPECT_GT(fixture.segment->ArrowRecordBatchLoadCount(pk_group, 0), 0);
    EXPECT_GT(fixture.segment->ArrowRecordBatchLoadCount(name_group, 0), 0);

    auto stripe_0_loads =
        fixture.segment->ArrowRecordBatchLoadCount(pk_group, 0);
    auto pk_chunk_0 =
        fixture.segment->chunk_data<int64_t>(nullptr, fixture.pk_fid, 0);
    EXPECT_EQ(pk_chunk_0.get()[0], 100);
    EXPECT_EQ(fixture.segment->ArrowRecordBatchLoadCount(pk_group, 0),
              stripe_0_loads);
}

TEST(ArrowSealedSegmentTest,
     RecordBatchIteratorProjectsFieldsAndPinsRowStripes) {
    auto fixture = BuildSegment();
    auto segment = fixture.segment;
    auto age_group = segment->ArrowFieldColumnGroupForTest(fixture.age_fid);
    auto name_group = segment->ArrowFieldColumnGroupForTest(fixture.name_fid);

    auto iterator = segment->IterateRecordBatches(
        nullptr, {fixture.age_fid, fixture.name_fid});
    ASSERT_TRUE(iterator.HasNext());

    {
        auto view = iterator.Next();
        EXPECT_EQ(view.row_begin, 0);
        EXPECT_EQ(view.row_count, 2);
        ASSERT_NE(view.batch.get(), nullptr);
        EXPECT_EQ(view.batch.get()->num_columns(), 2);
        EXPECT_EQ(view.batch.get()->schema()->field(0)->name(), "age");
        EXPECT_EQ(view.batch.get()->schema()->field(1)->name(), "name");

        auto ages = std::static_pointer_cast<arrow::Int64Array>(
            view.batch.get()->column(0));
        auto names = std::static_pointer_cast<arrow::StringArray>(
            view.batch.get()->column(1));
        EXPECT_EQ(ages->Value(1), 35);
        EXPECT_EQ(names->GetString(1), "bob");
        EXPECT_EQ(segment->ArrowRecordBatchLoadCount(age_group, 0), 1);
        EXPECT_EQ(segment->ArrowRecordBatchLoadCount(name_group, 0), 1);
        EXPECT_FALSE(segment->EvictArrowRecordBatch(age_group, 0));
        EXPECT_FALSE(segment->EvictArrowRecordBatch(name_group, 0));
    }

    EXPECT_TRUE(segment->EvictArrowRecordBatch(age_group, 0));
    EXPECT_TRUE(segment->EvictArrowRecordBatch(name_group, 0));
    ASSERT_TRUE(iterator.HasNext());

    auto view = iterator.Next();
    EXPECT_EQ(view.row_begin, 2);
    EXPECT_EQ(view.row_count, 3);
    EXPECT_EQ(segment->ArrowRecordBatchLoadCount(age_group, 1), 1);
    EXPECT_EQ(segment->ArrowRecordBatchLoadCount(name_group, 1), 1);
    EXPECT_FALSE(iterator.HasNext());
    EXPECT_THROW(iterator.Next(), std::out_of_range);
}

TEST(ArrowSealedSegmentTest, ArrowNativeExprFiltersViaRecordBatchIterator) {
    auto fixture = BuildSegment();
    auto segment = fixture.segment;
    auto pk_group = segment->ArrowFieldColumnGroupForTest(fixture.pk_fid);
    auto age_group = segment->ArrowFieldColumnGroupForTest(fixture.age_fid);
    auto name_group = segment->ArrowFieldColumnGroupForTest(fixture.name_fid);

    auto expr = BuildAgeGreaterThanExpr(fixture.age_fid, 30);
    auto bitset = segment->ExecuteArrowNativeExprForTest(nullptr, expr);
    BitsetTypeView view(bitset);

    EXPECT_FALSE(view[0]);
    EXPECT_TRUE(view[1]);
    EXPECT_TRUE(view[2]);
    EXPECT_FALSE(view[3]);
    EXPECT_TRUE(view[4]);

    EXPECT_EQ(segment->ArrowRecordBatchLoadCount(age_group, 0), 1);
    EXPECT_EQ(segment->ArrowRecordBatchLoadCount(age_group, 1), 1);
    EXPECT_EQ(segment->ArrowRecordBatchLoadCount(pk_group, 0), 0);
    EXPECT_EQ(segment->ArrowRecordBatchLoadCount(name_group, 0), 0);
}

TEST(ArrowSealedSegmentTest, RejectsInvalidFieldAndOffset) {
    auto fixture = BuildSegment();
    auto segment = fixture.segment;

    EXPECT_THROW(segment->chunk_data<int64_t>(nullptr, fixture.name_fid, 0),
                 std::runtime_error);
    EXPECT_THROW(segment->get_chunk_by_offset(FieldId(999), 0),
                 std::invalid_argument);
    EXPECT_THROW(segment->get_chunk_by_offset(fixture.age_fid, 5),
                 std::out_of_range);
}

}  // namespace milvus::segcore
