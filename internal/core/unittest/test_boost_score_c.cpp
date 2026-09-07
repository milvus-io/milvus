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

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/Schema.h"
#include "futures/future_c.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/boost_score_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

namespace {

class BoostScoreCTest : public testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<milvus::Schema>();
        vec_fid_ = schema_->AddDebugField("fakevec",
                                          milvus::DataType::VECTOR_FLOAT,
                                          16,
                                          knowhere::metric::L2);
        auto pk_fid = schema_->AddDebugField("pk", milvus::DataType::INT64);
        schema_->set_primary_field_id(pk_fid);

        auto raw_data = milvus::segcore::DataGen(schema_, 100);
        segment_ = CreateSealedWithFieldDataLoaded(schema_, raw_data);

        milvus::proto::plan::PlanNode plan_node;
        auto anns = plan_node.mutable_vector_anns();
        anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
        anns->set_field_id(vec_fid_.get());
        anns->set_placeholder_tag("$0");
        auto query_info = anns->mutable_query_info();
        query_info->set_topk(10);
        query_info->set_metric_type(knowhere::metric::L2);
        query_info->set_search_params(R"({"nprobe": 10})");
        plan_ = milvus::query::CreateSearchPlanFromPlanNode(schema_, plan_node);
    }

    milvus::proto::plan::ScoreFunction
    MakeWeightScorer(float weight) {
        milvus::proto::plan::ScoreFunction scorer;
        scorer.set_weight(weight);
        scorer.set_type(milvus::proto::plan::FunctionType::FunctionTypeWeight);
        return scorer;
    }

    std::shared_ptr<arrow::Array>
    MakeOffsets(const std::vector<int64_t>& offsets) {
        arrow::Int64Builder builder;
        for (auto offset : offsets) {
            auto status = builder.Append(offset);
            EXPECT_TRUE(status.ok()) << status.ToString();
        }
        std::shared_ptr<arrow::Array> result;
        auto status = builder.Finish(&result);
        EXPECT_TRUE(status.ok()) << status.ToString();
        return result;
    }

    void
    ExportOffsets(const std::shared_ptr<arrow::Array>& offsets,
                  ArrowArray* offset_array,
                  ArrowSchema* offset_schema) {
        auto status = arrow::ExportArray(*offsets, offset_array, offset_schema);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    void
    ReleaseArrow(ArrowArray* offset_array, ArrowSchema* offset_schema) {
        if (offset_array->release != nullptr) {
            offset_array->release(offset_array);
        }
        if (offset_schema->release != nullptr) {
            offset_schema->release(offset_schema);
        }
    }

    void
    FreeStatus(CStatus* status) {
        if (status->error_msg != nullptr && status->error_msg[0] != '\0') {
            free(const_cast<char*>(status->error_msg));
            status->error_msg = nullptr;
        }
    }

    milvus::SchemaPtr schema_;
    milvus::FieldId vec_fid_;
    std::unique_ptr<milvus::segcore::SegmentSealed> segment_;
    std::unique_ptr<milvus::query::Plan> plan_;
};

TEST_F(BoostScoreCTest, ComputeScorerScoresSync) {
    auto scorer = MakeWeightScorer(2.0F);
    std::string scorer_blob;
    ASSERT_TRUE(scorer.SerializeToString(&scorer_blob));

    auto offsets = MakeOffsets({0, 1, 2});
    ArrowArray offset_array{};
    ArrowSchema offset_schema{};
    ExportOffsets(offsets, &offset_array, &offset_schema);

    std::vector<float> scores(3);
    auto has_scores = std::make_unique<bool[]>(3);
    float* score_chunks[] = {scores.data()};
    bool* has_score_chunks[] = {has_scores.get()};

    auto status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        static_cast<CSearchPlan>(plan_.get()),
        scorer_blob.data(),
        scorer_blob.size(),
        &offset_array,
        &offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_chunks,
        has_score_chunks);
    ASSERT_EQ(status.error_code, milvus::Success) << status.error_msg;

    for (auto i = 0; i < 3; ++i) {
        EXPECT_TRUE(has_scores[i]);
        EXPECT_FLOAT_EQ(scores[i], 2.0F);
    }

    ReleaseArrow(&offset_array, &offset_schema);
}

TEST_F(BoostScoreCTest, ComputeScorerScoresAsync) {
    auto scorer = MakeWeightScorer(3.0F);
    std::string scorer_blob;
    ASSERT_TRUE(scorer.SerializeToString(&scorer_blob));

    auto offsets = MakeOffsets({0, 1, 2});
    ArrowArray offset_array{};
    ArrowSchema offset_schema{};
    ExportOffsets(offsets, &offset_array, &offset_schema);

    std::vector<float> scores(3);
    auto has_scores = std::make_unique<bool[]>(3);
    float* score_chunks[] = {scores.data()};
    bool* has_score_chunks[] = {has_scores.get()};

    auto future = AsyncComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        static_cast<CSearchPlan>(plan_.get()),
        scorer_blob.data(),
        scorer_blob.size(),
        &offset_array,
        &offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_chunks,
        has_score_chunks);
    ASSERT_NE(future, nullptr);

    void* result = nullptr;
    while (!future_is_ready(future)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto status = future_leak_and_get(future, &result);
    EXPECT_EQ(result, nullptr);
    ASSERT_EQ(status.error_code, milvus::Success) << status.error_msg;
    future_destroy(future);

    for (auto i = 0; i < 3; ++i) {
        EXPECT_TRUE(has_scores[i]);
        EXPECT_FLOAT_EQ(scores[i], 3.0F);
    }

    ReleaseArrow(&offset_array, &offset_schema);
}

TEST_F(BoostScoreCTest, AsyncPreflightFailureReturnsDestroyableFuture) {
    auto offsets = MakeOffsets({0});
    ArrowArray offset_array{};
    ArrowSchema offset_schema{};
    ExportOffsets(offsets, &offset_array, &offset_schema);

    std::vector<float> scores(1);
    auto has_scores = std::make_unique<bool[]>(1);
    float* score_chunks[] = {scores.data()};
    bool* has_score_chunks[] = {has_scores.get()};
    const char invalid_scorer_blob[] = {static_cast<char>(0xff), 0x01};

    auto future = AsyncComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        static_cast<CSearchPlan>(plan_.get()),
        invalid_scorer_blob,
        sizeof(invalid_scorer_blob),
        &offset_array,
        &offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_chunks,
        has_score_chunks);
    ASSERT_NE(future, nullptr);

    void* result = nullptr;
    while (!future_is_ready(future)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto status = future_leak_and_get(future, &result);
    EXPECT_EQ(result, nullptr);
    EXPECT_NE(status.error_code, milvus::Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(
        std::string(status.error_msg)
            .find("AsyncComputeScorerScoresOnOffsetChunks preflight failed"),
        std::string::npos);
    FreeStatus(&status);
    future_destroy(future);

    ReleaseArrow(&offset_array, &offset_schema);
}

TEST_F(BoostScoreCTest, ValidateRequiredInputs) {
    auto scorer = MakeWeightScorer(2.0F);
    std::string scorer_blob;
    ASSERT_TRUE(scorer.SerializeToString(&scorer_blob));

    auto offsets = MakeOffsets({0});
    ArrowArray offset_array{};
    ArrowSchema offset_schema{};
    ExportOffsets(offsets, &offset_array, &offset_schema);

    std::vector<float> scores(1);
    auto has_scores = std::make_unique<bool[]>(1);
    float* score_chunks[] = {scores.data()};
    bool* has_score_chunks[] = {has_scores.get()};

    auto status =
        ComputeScorerScoresOnOffsetChunks(nullptr,
                                          static_cast<CSearchPlan>(plan_.get()),
                                          scorer_blob.data(),
                                          scorer_blob.size(),
                                          &offset_array,
                                          &offset_schema,
                                          1,
                                          milvus::MAX_TIMESTAMP,
                                          0,
                                          0,
                                          0,
                                          score_chunks,
                                          has_score_chunks);
    EXPECT_NE(status.error_code, milvus::Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("segment is null"),
              std::string::npos);
    FreeStatus(&status);

    status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        nullptr,
        scorer_blob.data(),
        scorer_blob.size(),
        &offset_array,
        &offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_chunks,
        has_score_chunks);
    EXPECT_NE(status.error_code, milvus::Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("search plan is null"),
              std::string::npos);
    FreeStatus(&status);

    status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        static_cast<CSearchPlan>(plan_.get()),
        nullptr,
        scorer_blob.size(),
        &offset_array,
        &offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_chunks,
        has_score_chunks);
    EXPECT_NE(status.error_code, milvus::Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("score function is null"),
              std::string::npos);
    FreeStatus(&status);

    status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        static_cast<CSearchPlan>(plan_.get()),
        scorer_blob.data(),
        0,
        &offset_array,
        &offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_chunks,
        has_score_chunks);
    EXPECT_NE(status.error_code, milvus::Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("score function is empty"),
              std::string::npos);
    FreeStatus(&status);

    ReleaseArrow(&offset_array, &offset_schema);
}

TEST_F(BoostScoreCTest, ZeroChunksSucceedsWithoutOutputBuffers) {
    auto scorer = MakeWeightScorer(2.0F);
    std::string scorer_blob;
    ASSERT_TRUE(scorer.SerializeToString(&scorer_blob));

    auto status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        static_cast<CSearchPlan>(plan_.get()),
        scorer_blob.data(),
        scorer_blob.size(),
        nullptr,
        nullptr,
        0,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        nullptr,
        nullptr);
    ASSERT_EQ(status.error_code, milvus::Success) << status.error_msg;
}

TEST_F(BoostScoreCTest, RejectInvalidOffsetChunks) {
    auto scorer = MakeWeightScorer(2.0F);
    std::string scorer_blob;
    ASSERT_TRUE(scorer.SerializeToString(&scorer_blob));

    arrow::Int32Builder int32_builder;
    ASSERT_TRUE(int32_builder.Append(1).ok());
    std::shared_ptr<arrow::Array> int32_offsets;
    ASSERT_TRUE(int32_builder.Finish(&int32_offsets).ok());
    ArrowArray int32_offset_array{};
    ArrowSchema int32_offset_schema{};
    ExportOffsets(int32_offsets, &int32_offset_array, &int32_offset_schema);

    std::vector<float> scores(1);
    auto has_scores = std::make_unique<bool[]>(1);
    float* score_chunks[] = {scores.data()};
    bool* has_score_chunks[] = {has_scores.get()};

    auto status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        static_cast<CSearchPlan>(plan_.get()),
        scorer_blob.data(),
        scorer_blob.size(),
        &int32_offset_array,
        &int32_offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_chunks,
        has_score_chunks);
    EXPECT_NE(status.error_code, milvus::Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("offset array must be Int64"),
              std::string::npos);
    FreeStatus(&status);

    ReleaseArrow(&int32_offset_array, &int32_offset_schema);

    arrow::Int64Builder null_builder;
    ASSERT_TRUE(null_builder.Append(1).ok());
    ASSERT_TRUE(null_builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> null_offsets;
    ASSERT_TRUE(null_builder.Finish(&null_offsets).ok());
    ArrowArray null_offset_array{};
    ArrowSchema null_offset_schema{};
    ExportOffsets(null_offsets, &null_offset_array, &null_offset_schema);

    std::vector<float> null_scores(2);
    auto null_has_scores = std::make_unique<bool[]>(2);
    float* null_score_chunks[] = {null_scores.data()};
    bool* null_has_score_chunks[] = {null_has_scores.get()};

    status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment_.get()),
        static_cast<CSearchPlan>(plan_.get()),
        scorer_blob.data(),
        scorer_blob.size(),
        &null_offset_array,
        &null_offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        null_score_chunks,
        null_has_score_chunks);
    EXPECT_NE(status.error_code, milvus::Success);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("offset array contains null"),
              std::string::npos);
    FreeStatus(&status);

    ReleaseArrow(&null_offset_array, &null_offset_schema);
}

// Text match cannot consume offset input, so the C wrapper evaluates the
// filter once over the whole segment and reuses the bitset for every offset
// chunk. Every chunk -- including offsets beyond the first expression batch
// (8192 rows) -- must be scored against the full-segment filter result.
TEST_F(BoostScoreCTest, NonNativeFilterScoresAllChunks) {
    const int64_t N = 10000;  // more than one expression batch (8192)
    auto schema = std::make_shared<milvus::Schema>();
    std::map<std::string, std::string> match_params;
    {
        milvus::FieldMeta f(milvus::FieldName("pk"),
                            milvus::FieldId(100),
                            milvus::DataType::INT64,
                            false,
                            std::nullopt);
        schema->AddField(std::move(f));
        schema->set_primary_field_id(milvus::FieldId(100));
    }
    {
        milvus::FieldMeta f(milvus::FieldName("str"),
                            milvus::FieldId(101),
                            milvus::DataType::VARCHAR,
                            65536,
                            false,
                            true,
                            true,
                            match_params,
                            std::nullopt);
        schema->AddField(std::move(f));
    }
    {
        milvus::FieldMeta f(milvus::FieldName("fvec"),
                            milvus::FieldId(102),
                            milvus::DataType::VECTOR_FLOAT,
                            16,
                            knowhere::metric::L2,
                            false,
                            std::nullopt);
        schema->AddField(std::move(f));
    }

    auto raw_data = milvus::segcore::DataGen(schema, N);
    auto* str_col = raw_data.raw_->mutable_fields_data()
                        ->at(1)
                        .mutable_scalars()
                        ->mutable_string_data()
                        ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = (i % 2 == 0) ? "football match" : "swimming pool";
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    segment->CreateTextIndex(milvus::FieldId(101));

    milvus::proto::plan::PlanNode plan_node;
    auto anns = plan_node.mutable_vector_anns();
    anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    anns->set_field_id(102);
    anns->set_placeholder_tag("$0");
    auto query_info = anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_metric_type(knowhere::metric::L2);
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan = milvus::query::CreateSearchPlanFromPlanNode(schema, plan_node);

    auto scorer = MakeWeightScorer(2.0F);
    auto column_info = milvus::test::GenColumnInfo(
        101, milvus::proto::schema::DataType::VarChar, false, false);
    std::string query = "football";
    auto unary_range_expr = milvus::test::GenUnaryRangeExpr(
        milvus::proto::plan::OpType::TextMatch, query);
    unary_range_expr->set_allocated_column_info(column_info);
    auto slop = milvus::test::GenGenericValue(static_cast<int64_t>(0));
    unary_range_expr->add_extra_values()->CopyFrom(*slop);
    delete slop;
    scorer.mutable_filter()->set_allocated_unary_range_expr(unary_range_expr);
    std::string scorer_blob;
    ASSERT_TRUE(scorer.SerializeToString(&scorer_blob));

    // Two offset chunks, both mixing matching and non-matching rows; the
    // second one reaches past the first expression batch.
    std::vector<std::vector<int64_t>> chunk_offsets = {
        {0, 1},
        {9000, 9001, N - 2},
    };
    const int64_t num_chunks = static_cast<int64_t>(chunk_offsets.size());
    std::vector<ArrowArray> offset_arrays(num_chunks);
    std::vector<ArrowSchema> offset_schemas(num_chunks);
    std::vector<std::vector<float>> scores(num_chunks);
    std::vector<std::unique_ptr<bool[]>> has_scores(num_chunks);
    std::vector<float*> score_ptrs(num_chunks);
    std::vector<bool*> has_score_ptrs(num_chunks);
    for (int64_t i = 0; i < num_chunks; ++i) {
        ExportOffsets(MakeOffsets(chunk_offsets[i]),
                      &offset_arrays[i],
                      &offset_schemas[i]);
        scores[i].resize(chunk_offsets[i].size(), -1.0F);
        has_scores[i] = std::make_unique<bool[]>(chunk_offsets[i].size());
        score_ptrs[i] = scores[i].data();
        has_score_ptrs[i] = has_scores[i].get();
    }

    auto status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment.get()),
        static_cast<CSearchPlan>(plan.get()),
        scorer_blob.data(),
        scorer_blob.size(),
        offset_arrays.data(),
        offset_schemas.data(),
        num_chunks,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_ptrs.data(),
        has_score_ptrs.data());
    ASSERT_EQ(status.error_code, milvus::Success) << status.error_msg;
    FreeStatus(&status);

    // Chunk 0: row 0 matches, row 1 does not.
    EXPECT_TRUE(has_scores[0][0]);
    EXPECT_FLOAT_EQ(scores[0][0], 2.0F);
    EXPECT_FALSE(has_scores[0][1]);

    // Chunk 1: offsets beyond the first expression batch keep their boost.
    EXPECT_TRUE(has_scores[1][0]);
    EXPECT_FLOAT_EQ(scores[1][0], 2.0F);
    EXPECT_FALSE(has_scores[1][1]);
    EXPECT_TRUE(has_scores[1][2]);
    EXPECT_FLOAT_EQ(scores[1][2], 2.0F);

    for (int64_t i = 0; i < num_chunks; ++i) {
        ReleaseArrow(&offset_arrays[i], &offset_schemas[i]);
    }
}

// The Go caller exports every offset chunk, empty ones included, and only
// short-circuits when the chunk count itself is zero. So a request with
// nothing to score still reaches the C wrapper as N empty chunks. Hoisting
// the non-native filter out of the per-chunk loop must not make that request
// scan the whole segment: for GIS that would be millions of wasted GEOS
// calls, and a filter that cannot be evaluated at all (here: text match with
// no text index) would fail a request that needed no scoring.
TEST_F(BoostScoreCTest, AllEmptyChunksSkipNonNativeFilter) {
    const int64_t N = 100;
    auto schema = std::make_shared<milvus::Schema>();
    std::map<std::string, std::string> match_params;
    {
        milvus::FieldMeta f(milvus::FieldName("pk"),
                            milvus::FieldId(100),
                            milvus::DataType::INT64,
                            false,
                            std::nullopt);
        schema->AddField(std::move(f));
        schema->set_primary_field_id(milvus::FieldId(100));
    }
    {
        milvus::FieldMeta f(milvus::FieldName("str"),
                            milvus::FieldId(101),
                            milvus::DataType::VARCHAR,
                            65536,
                            false,
                            true,
                            true,
                            match_params,
                            std::nullopt);
        schema->AddField(std::move(f));
    }
    {
        milvus::FieldMeta f(milvus::FieldName("fvec"),
                            milvus::FieldId(102),
                            milvus::DataType::VECTOR_FLOAT,
                            16,
                            knowhere::metric::L2,
                            false,
                            std::nullopt);
        schema->AddField(std::move(f));
    }

    auto raw_data = milvus::segcore::DataGen(schema, N);
    // Deliberately no CreateTextIndex: evaluating the filter would throw.
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    milvus::proto::plan::PlanNode plan_node;
    auto anns = plan_node.mutable_vector_anns();
    anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    anns->set_field_id(102);
    anns->set_placeholder_tag("$0");
    auto query_info = anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_metric_type(knowhere::metric::L2);
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan = milvus::query::CreateSearchPlanFromPlanNode(schema, plan_node);

    auto scorer = MakeWeightScorer(2.0F);
    auto column_info = milvus::test::GenColumnInfo(
        101, milvus::proto::schema::DataType::VarChar, false, false);
    std::string query = "football";
    auto unary_range_expr = milvus::test::GenUnaryRangeExpr(
        milvus::proto::plan::OpType::TextMatch, query);
    unary_range_expr->set_allocated_column_info(column_info);
    auto slop = milvus::test::GenGenericValue(static_cast<int64_t>(0));
    unary_range_expr->add_extra_values()->CopyFrom(*slop);
    delete slop;
    scorer.mutable_filter()->set_allocated_unary_range_expr(unary_range_expr);
    std::string scorer_blob;
    ASSERT_TRUE(scorer.SerializeToString(&scorer_blob));

    // Two chunks, both empty -- exactly what the Go path hands over when no
    // row of this segment needs a boost score.
    const int64_t num_chunks = 2;
    std::vector<ArrowArray> offset_arrays(num_chunks);
    std::vector<ArrowSchema> offset_schemas(num_chunks);
    for (int64_t i = 0; i < num_chunks; ++i) {
        ExportOffsets(MakeOffsets({}), &offset_arrays[i], &offset_schemas[i]);
    }
    // Empty chunks get no output buffer from the Go caller either.
    std::vector<float*> score_ptrs(num_chunks, nullptr);
    std::vector<bool*> has_score_ptrs(num_chunks, nullptr);

    auto status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment.get()),
        static_cast<CSearchPlan>(plan.get()),
        scorer_blob.data(),
        scorer_blob.size(),
        offset_arrays.data(),
        offset_schemas.data(),
        num_chunks,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_ptrs.data(),
        has_score_ptrs.data());
    EXPECT_EQ(status.error_code, milvus::Success) << status.error_msg;
    FreeStatus(&status);

    for (int64_t i = 0; i < num_chunks; ++i) {
        ReleaseArrow(&offset_arrays[i], &offset_schemas[i]);
    }

    // Control: the same setup with one non-empty chunk does evaluate the
    // filter, so the assertion above is about the skip and not about the
    // filter being harmless here.
    ArrowArray offset_array{};
    ArrowSchema offset_schema{};
    ExportOffsets(MakeOffsets({0}), &offset_array, &offset_schema);
    std::vector<float> scores(1, -1.0F);
    auto has_scores = std::make_unique<bool[]>(1);
    float* score_chunks[] = {scores.data()};
    bool* has_score_chunks[] = {has_scores.get()};

    status = ComputeScorerScoresOnOffsetChunks(
        static_cast<CSegmentInterface>(segment.get()),
        static_cast<CSearchPlan>(plan.get()),
        scorer_blob.data(),
        scorer_blob.size(),
        &offset_array,
        &offset_schema,
        1,
        milvus::MAX_TIMESTAMP,
        0,
        0,
        0,
        score_chunks,
        has_score_chunks);
    EXPECT_NE(status.error_code, milvus::Success);
    FreeStatus(&status);

    ReleaseArrow(&offset_array, &offset_schema);
}

}  // namespace
