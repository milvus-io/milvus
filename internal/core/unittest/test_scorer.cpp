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

#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/Geometry.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/QueryContext.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "geos_c.h"
#include "gtest/gtest.h"
#include "index/ScalarIndexSort.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "query/PlanProto.h"
#include "rescores/BoostScoreRunner.h"
#include "rescores/Scorer.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::rescores;

namespace {

class StaticScorer : public Scorer {
 public:
    explicit StaticScorer(std::vector<std::optional<float>> scores)
        : scores_(std::move(scores)) {
    }

    expr::TypedExprPtr
    filter() override {
        return nullptr;
    }

    void
    batch_score(milvus::OpContext* op_ctx,
                const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                const TargetBitmapView& bitmap,
                std::vector<std::optional<float>>& boost_scores) override {
        for (auto i = 0; i < offsets.size(); ++i) {
            if (bitmap[i] && scores_[i].has_value()) {
                boost_scores[i] = scores_[i];
            }
        }
    }

    void
    batch_score(milvus::OpContext* op_ctx,
                const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                const TargetBitmap& bitmap,
                std::vector<std::optional<float>>& boost_scores) override {
        for (auto i = 0; i < offsets.size(); ++i) {
            auto offset = offsets[i];
            if (offset >= 0 && static_cast<size_t>(offset) < bitmap.size() &&
                bitmap[offset] && scores_[i].has_value()) {
                boost_scores[i] = scores_[i];
            }
        }
    }

    void
    batch_score(milvus::OpContext* op_ctx,
                const segcore::SegmentInternalInterface* segment,
                const proto::plan::FunctionMode& mode,
                const FixedVector<int32_t>& offsets,
                std::vector<std::optional<float>>& boost_scores) override {
        for (auto i = 0; i < offsets.size(); ++i) {
            if (scores_[i].has_value()) {
                boost_scores[i] = scores_[i];
            }
        }
    }

    float
    weight() override {
        return 0.0F;
    }

 private:
    std::vector<std::optional<float>> scores_;
};

}  // namespace

class WeightScorerTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create a WeightScorer with no filter and weight of 2.0
        scorer_ = std::make_unique<WeightScorer>(nullptr, 2.0f);
    }

    std::unique_ptr<WeightScorer> scorer_;
};

// Test: TargetBitmap batch_score with valid offsets (all within bitmap bounds)
TEST_F(WeightScorerTest, BatchScoreTargetBitmapValidOffsets) {
    TargetBitmap bitmap(100);
    bitmap.set(10);
    bitmap.set(50);
    bitmap.set(90);

    // Offsets that are all within bitmap bounds
    FixedVector<int32_t> offsets = {10, 20, 50, 90};
    std::vector<std::optional<float>> boost_scores(offsets.size(),
                                                   std::nullopt);

    proto::plan::FunctionMode mode = proto::plan::FunctionMode::FunctionModeSum;

    scorer_->batch_score(nullptr, nullptr, mode, offsets, bitmap, boost_scores);

    // Positions 10, 50, 90 should have scores (they are set in bitmap)
    EXPECT_TRUE(boost_scores[0].has_value());
    EXPECT_FALSE(boost_scores[1].has_value());
    EXPECT_TRUE(boost_scores[2].has_value());
    EXPECT_TRUE(boost_scores[3].has_value());
}

// Test: TargetBitmap batch_score with out-of-bounds offsets (should NOT crash)
TEST_F(WeightScorerTest, BatchScoreTargetBitmapOutOfBoundsOffsets) {
    // Create a small bitmap of size 50
    TargetBitmap bitmap(50);
    bitmap.set(10);  // Set bit at position 10
    bitmap.set(40);  // Set bit at position 40

    // Offsets where some are OUT OF BOUNDS (>= 50)
    // This simulates the race condition where text index lags behind vector index
    FixedVector<int32_t> offsets = {10, 40, 60, 100, 200};
    std::vector<std::optional<float>> boost_scores(offsets.size(),
                                                   std::nullopt);

    proto::plan::FunctionMode mode = proto::plan::FunctionMode::FunctionModeSum;

    // Should NOT crash! Out-of-bounds offsets should be safely skipped
    ASSERT_NO_THROW(scorer_->batch_score(
        nullptr, nullptr, mode, offsets, bitmap, boost_scores));

    // In-bounds offsets should be scored correctly
    EXPECT_TRUE(boost_scores[0].has_value());
    EXPECT_TRUE(boost_scores[1].has_value());

    // Out-of-bounds offsets should NOT have scores (safely skipped)
    EXPECT_FALSE(boost_scores[2].has_value());
    EXPECT_FALSE(boost_scores[3].has_value());
    EXPECT_FALSE(boost_scores[4].has_value());
}

TEST(BoostScoreRunnerTest, ComputeScorerScoresNoFilterCopiesToBuffers) {
    auto scorer = std::make_shared<WeightScorer>(nullptr, 2.5F);
    FixedVector<int32_t> offsets = {3, 1, 4};
    std::vector<float> scores(offsets.size(), -1.0F);
    auto has_scores = std::make_unique<bool[]>(offsets.size());

    ComputeScorerScores(nullptr,
                        nullptr,
                        nullptr,
                        scorer,
                        offsets,
                        scores.data(),
                        has_scores.get());

    for (auto i = 0; i < offsets.size(); ++i) {
        EXPECT_TRUE(has_scores[i]);
        EXPECT_FLOAT_EQ(scores[i], 2.5F);
    }
}

TEST(BoostScoreRunnerTest, ComputeFunctionScoresMergesAndSkipsNulls) {
    std::vector<std::shared_ptr<Scorer>> scorers{
        std::make_shared<StaticScorer>(
            std::vector<std::optional<float>>{2.0F, std::nullopt, 4.0F}),
        std::make_shared<StaticScorer>(
            std::vector<std::optional<float>>{3.0F, 5.0F, std::nullopt}),
    };
    FixedVector<int32_t> offsets = {0, 1, 2};
    std::vector<float> scores(offsets.size(), -1.0F);
    auto has_scores = std::make_unique<bool[]>(offsets.size());

    ComputeFunctionScores(nullptr,
                          nullptr,
                          nullptr,
                          scorers,
                          proto::plan::FunctionModeSum,
                          offsets,
                          scores.data(),
                          has_scores.get());

    EXPECT_TRUE(has_scores[0]);
    EXPECT_FLOAT_EQ(scores[0], 5.0F);
    EXPECT_TRUE(has_scores[1]);
    EXPECT_FLOAT_EQ(scores[1], 5.0F);
    EXPECT_TRUE(has_scores[2]);
    EXPECT_FLOAT_EQ(scores[2], 4.0F);

    std::vector<std::optional<float>> optional_scores(offsets.size(),
                                                      std::nullopt);
    ComputeFunctionScores(nullptr,
                          nullptr,
                          nullptr,
                          scorers,
                          proto::plan::FunctionModeMultiply,
                          offsets,
                          optional_scores);

    ASSERT_TRUE(optional_scores[0].has_value());
    EXPECT_FLOAT_EQ(optional_scores[0].value(), 6.0F);
    ASSERT_TRUE(optional_scores[1].has_value());
    EXPECT_FLOAT_EQ(optional_scores[1].value(), 5.0F);
    ASSERT_TRUE(optional_scores[2].has_value());
    EXPECT_FLOAT_EQ(optional_scores[2].value(), 4.0F);
}

TEST(BoostScoreRunnerTest, ComputeFunctionScoresRejectsMismatchedOutputSize) {
    std::vector<std::shared_ptr<Scorer>> scorers{
        std::make_shared<WeightScorer>(nullptr, 2.0F),
    };
    FixedVector<int32_t> offsets = {0, 1};
    std::vector<std::optional<float>> scores(1, std::nullopt);

    EXPECT_THROW(ComputeFunctionScores(nullptr,
                                       nullptr,
                                       nullptr,
                                       scorers,
                                       proto::plan::FunctionModeSum,
                                       offsets,
                                       scores),
                 milvus::SegcoreError);
}

// Test: TargetBitmap batch_score with out-of-bounds offsets (should NOT crash).
// Unlike WeightScorer, RandomScorer had no bounds check on bitmap[offset].
TEST(RandomScorerTest, BatchScoreTargetBitmapOutOfBoundsOffsets) {
    // The segment is only consulted for get_segment_id() on the
    // no-seed-field path of random_score.
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto raw_data = segcore::DataGen(schema, 8);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    expr::TypedExprPtr filter = nullptr;
    ProtoParams params;
    auto* seed = params.Add();
    seed->set_key("seed");
    seed->set_value("42");
    RandomScorer scorer(filter, 1.0F, params);

    TargetBitmap bitmap(50);
    bitmap.set(10);
    bitmap.set(40);

    // Offsets where some are OUT OF BOUNDS (>= 50), e.g. when the filter
    // bitmap does not cover the whole segment.
    FixedVector<int32_t> offsets = {10, 40, 60, 100, 200};
    std::vector<std::optional<float>> boost_scores(offsets.size(),
                                                   std::nullopt);

    ASSERT_NO_THROW(scorer.batch_score(nullptr,
                                       segment.get(),
                                       proto::plan::FunctionModeSum,
                                       offsets,
                                       bitmap,
                                       boost_scores));

    // In-bounds matched offsets should be scored.
    EXPECT_TRUE(boost_scores[0].has_value());
    EXPECT_TRUE(boost_scores[1].has_value());

    // Out-of-bounds offsets should NOT have scores (safely skipped).
    EXPECT_FALSE(boost_scores[2].has_value());
    EXPECT_FALSE(boost_scores[3].has_value());
    EXPECT_FALSE(boost_scores[4].has_value());
}

namespace {

SchemaPtr
GenTextMatchSchema() {
    auto schema = std::make_shared<Schema>();
    std::map<std::string, std::string> match_params;
    {
        FieldMeta f(FieldName("pk"),
                    FieldId(100),
                    DataType::INT64,
                    false,
                    std::nullopt);
        schema->AddField(std::move(f));
        schema->set_primary_field_id(FieldId(100));
    }
    {
        FieldMeta f(FieldName("str"),
                    FieldId(101),
                    DataType::VARCHAR,
                    65536,
                    false,
                    true,
                    true,
                    match_params,
                    std::nullopt);
        schema->AddField(std::move(f));
    }
    {
        FieldMeta f(FieldName("fvec"),
                    FieldId(102),
                    DataType::VECTOR_FLOAT,
                    16,
                    knowhere::metric::L2,
                    false,
                    std::nullopt);
        schema->AddField(std::move(f));
    }
    return schema;
}

expr::TypedExprPtr
GenTextMatchTypedExpr(const SchemaPtr& schema, const std::string& query) {
    const auto& str_meta = schema->operator[](FieldName("str"));
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto unary_range_expr =
        test::GenUnaryRangeExpr(proto::plan::OpType::TextMatch, query);
    unary_range_expr->set_allocated_column_info(column_info);
    auto slop = test::GenGenericValue(static_cast<int64_t>(0));
    unary_range_expr->add_extra_values()->CopyFrom(*slop);
    delete slop;
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr);
    auto parser = query::ProtoParser(schema);
    return parser.ParseExprs(*expr);
}

}  // namespace

// Test: a filter whose expression does not support offset input (text match,
// GIS) is evaluated batch by batch over the whole segment. The resulting
// bitset must cover every active row, not just the first expression batch,
// otherwise offsets beyond DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE silently lose
// their boost.
TEST(BoostScoreRunnerTest, ComputeScorerScoresNonNativeFilterCoversAllBatches) {
    const int64_t N = 10000;  // more than one expression batch (8192)
    auto schema = GenTextMatchSchema();
    auto raw_data = segcore::DataGen(schema, N);
    auto* str_col = raw_data.raw_->mutable_fields_data()
                        ->at(1)
                        .mutable_scalars()
                        ->mutable_string_data()
                        ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = (i % 2 == 0) ? "football match" : "swimming pool";
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    segment->CreateTextIndex(FieldId(101));

    auto filter = GenTextMatchTypedExpr(schema, "football");
    auto scorer = std::make_shared<WeightScorer>(filter, 2.0F);

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_scorer_multi_batch", segment.get(), N, MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    FixedVector<int32_t> offsets = {
        0, 1, 9000, 9001, static_cast<int32_t>(N - 2)};
    std::vector<std::optional<float>> scores(offsets.size(), std::nullopt);
    ComputeScorerScores(
        &exec_context, &op_context, segment.get(), scorer, offsets, scores);

    // First batch behaves as before.
    ASSERT_TRUE(scores[0].has_value());  // 0: "football match"
    EXPECT_FLOAT_EQ(scores[0].value(), 2.0F);
    EXPECT_FALSE(scores[1].has_value());  // 1: "swimming pool"

    // Offsets beyond the first expression batch must still be scored.
    ASSERT_TRUE(scores[2].has_value());  // 9000: "football match"
    EXPECT_FLOAT_EQ(scores[2].value(), 2.0F);
    EXPECT_FALSE(scores[3].has_value());  // 9001: "swimming pool"
    ASSERT_TRUE(scores[4].has_value());   // 9998: "football match"
    EXPECT_FLOAT_EQ(scores[4].value(), 2.0F);
}

// Same regression through a GIS filter. GIS gained SupportOffsetInput() ==
// false in the offset-input contract fix, which routes it into the same
// non-native fallback as text match; a boosted offset past the first
// expression batch must still be scored.
TEST(BoostScoreRunnerTest, ComputeScorerScoresGISFilterCoversAllBatches) {
    const int64_t N = 10000;  // more than one expression batch (8192)
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto geo_fid = schema->AddDebugField("geo", DataType::GEOMETRY);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);

    auto raw_data = segcore::DataGen(schema, N);
    proto::schema::FieldData* geo_field_data = nullptr;
    for (auto& fd : *raw_data.raw_->mutable_fields_data()) {
        if (fd.field_id() == geo_fid.get()) {
            geo_field_data = &fd;
            break;
        }
    }
    ASSERT_NE(geo_field_data, nullptr);
    // Even rows sit inside the query polygon, odd rows far outside.
    auto* geo_col = geo_field_data->mutable_scalars()->mutable_geometry_data();
    geo_col->clear_data();
    auto ctx = GEOS_init_r();
    for (int64_t i = 0; i < N; i++) {
        const char* wkt =
            (i % 2 == 0) ? "POINT (0.5 0.5)" : "POINT (100.0 100.0)";
        Geometry geom(ctx, wkt);
        geo_col->add_data(geom.to_wkb_string());
    }
    GEOS_finish_r(ctx);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    auto filter = std::make_shared<expr::GISFunctionFilterExpr>(
        expr::ColumnInfo(geo_fid, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_Within,
        "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))");
    auto scorer = std::make_shared<WeightScorer>(filter, 3.0F);

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_scorer_gis_multi_batch", segment.get(), N, MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    FixedVector<int32_t> offsets = {
        0, 1, 9000, 9001, static_cast<int32_t>(N - 2)};
    std::vector<std::optional<float>> scores(offsets.size(), std::nullopt);
    ComputeScorerScores(
        &exec_context, &op_context, segment.get(), scorer, offsets, scores);

    // First batch behaves as before.
    ASSERT_TRUE(scores[0].has_value());  // 0: inside the polygon
    EXPECT_FLOAT_EQ(scores[0].value(), 3.0F);
    EXPECT_FALSE(scores[1].has_value());  // 1: outside the polygon

    // Offsets beyond the first expression batch must still be scored.
    ASSERT_TRUE(scores[2].has_value());  // 9000: inside the polygon
    EXPECT_FLOAT_EQ(scores[2].value(), 3.0F);
    EXPECT_FALSE(scores[3].has_value());  // 9001: outside the polygon
    ASSERT_TRUE(scores[4].has_value());   // 9998: inside the polygon
    EXPECT_FLOAT_EQ(scores[4].value(), 3.0F);
}

// NULL policy must be identical on the native and non-native branches of
// ComputeScorerScores: an UNKNOWN (NULL) filter verdict never grants a
// boost. The non-native branch folds the valid bitmap explicitly; this
// pins the same contract for a native (offset-input) filter evaluated on
// a nullable field.
TEST(BoostScoreRunnerTest, NativeFilterGivesNullRowsNoBoost) {
    const int64_t N = 1000;
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto age_fid =
        schema->AddDebugField("age", DataType::INT64, /*nullable=*/true);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);

    auto raw_data = segcore::DataGen(schema, N);
    proto::schema::FieldData* age_field_data = nullptr;
    for (auto& fd : *raw_data.raw_->mutable_fields_data()) {
        if (fd.field_id() == age_fid.get()) {
            age_field_data = &fd;
            break;
        }
    }
    ASSERT_NE(age_field_data, nullptr);
    // Every row satisfies the filter on its data bits; odd rows are NULL.
    auto* age_col =
        age_field_data->mutable_scalars()->mutable_long_data()->mutable_data();
    auto* valid_col = age_field_data->mutable_valid_data();
    ASSERT_EQ(valid_col->size(), N);
    for (int64_t i = 0; i < N; i++) {
        age_col->at(i) = i;
        valid_col->at(i) = (i % 2 == 0);
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    proto::plan::GenericValue val;
    val.set_int64_val(0);
    auto filter = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(age_fid, DataType::INT64, {}, /*nullable=*/true),
        proto::plan::OpType::GreaterEqual,
        val);
    auto scorer = std::make_shared<WeightScorer>(filter, 2.0F);

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_scorer_native_null_fold", segment.get(), N, MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    // Guard: this filter must resolve to the native branch, otherwise the
    // assertions below silently degrade into another non-native case.
    EXPECT_FALSE(
        ComputeNonNativeFilterBitset(&exec_context, scorer).has_value());

    FixedVector<int32_t> offsets = {0, 1, 2, 3, 500, 501};
    std::vector<std::optional<float>> scores(offsets.size(), std::nullopt);
    ComputeScorerScores(
        &exec_context, &op_context, segment.get(), scorer, offsets, scores);

    for (size_t i = 0; i < offsets.size(); ++i) {
        if (offsets[i] % 2 == 0) {
            ASSERT_TRUE(scores[i].has_value())
                << "valid row " << offsets[i] << " must be boosted";
            EXPECT_FLOAT_EQ(scores[i].value(), 2.0F);
        } else {
            EXPECT_FALSE(scores[i].has_value())
                << "null row " << offsets[i] << " must not be boosted";
        }
    }
}

// The per-chunk scoring loop in boost_score.cpp must not re-evaluate a
// non-native filter once per offset chunk; ComputeNonNativeFilterBitset is
// its hoisting hook. Pin the contract: no filter and native filters yield
// std::nullopt (nothing to hoist), non-native filters yield the
// whole-segment bitset.
TEST(BoostScoreRunnerTest, ComputeNonNativeFilterBitsetNulloptWithoutFilter) {
    auto scorer = std::make_shared<WeightScorer>(nullptr, 2.0F);
    EXPECT_FALSE(ComputeNonNativeFilterBitset(nullptr, scorer).has_value());
}

TEST(BoostScoreRunnerTest, ComputeNonNativeFilterBitsetNulloptForNativeFilter) {
    const int64_t N = 100;
    auto schema = GenTextMatchSchema();
    auto raw_data = segcore::DataGen(schema, N);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // An int64 unary range expression consumes offset input natively, so
    // there is no whole-segment bitset to hoist.
    proto::plan::GenericValue val;
    val.set_int64_val(0);
    auto filter = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(FieldId(100), DataType::INT64),
        proto::plan::OpType::GreaterEqual,
        val);
    auto scorer = std::make_shared<WeightScorer>(filter, 2.0F);

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_native_filter_bitset", segment.get(), N, MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    EXPECT_FALSE(
        ComputeNonNativeFilterBitset(&exec_context, scorer).has_value());
}

// A non-native filter evaluated once via ComputeNonNativeFilterBitset must
// cover the whole segment, and passing that bitset into per-chunk
// ComputeScorerScores calls must score every chunk as if the filter had been
// evaluated inside the call.
TEST(BoostScoreRunnerTest, PrecomputedFilterBitsetScoresChunksConsistently) {
    const int64_t N = 10000;  // more than one expression batch (8192)
    auto schema = GenTextMatchSchema();
    auto raw_data = segcore::DataGen(schema, N);
    auto* str_col = raw_data.raw_->mutable_fields_data()
                        ->at(1)
                        .mutable_scalars()
                        ->mutable_string_data()
                        ->mutable_data();
    for (int64_t i = 0; i < N; i++) {
        str_col->at(i) = (i % 2 == 0) ? "football match" : "swimming pool";
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    segment->CreateTextIndex(FieldId(101));

    auto filter = GenTextMatchTypedExpr(schema, "football");
    auto scorer = std::make_shared<WeightScorer>(filter, 2.0F);

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_precomputed_filter_bitset", segment.get(), N, MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    auto filter_bitset = ComputeNonNativeFilterBitset(&exec_context, scorer);
    ASSERT_TRUE(filter_bitset.has_value());
    ASSERT_EQ(filter_bitset->size(), N);
    EXPECT_TRUE((*filter_bitset)[0]);
    EXPECT_FALSE((*filter_bitset)[1]);
    EXPECT_TRUE((*filter_bitset)[9000]);
    EXPECT_FALSE((*filter_bitset)[9001]);

    // Chunk 1 through the optional<float> overload.
    FixedVector<int32_t> chunk1 = {0, 1};
    std::vector<std::optional<float>> scores1(chunk1.size(), std::nullopt);
    ComputeScorerScores(&exec_context,
                        &op_context,
                        segment.get(),
                        scorer,
                        chunk1,
                        scores1,
                        &filter_bitset.value());
    ASSERT_TRUE(scores1[0].has_value());
    EXPECT_FLOAT_EQ(scores1[0].value(), 2.0F);
    EXPECT_FALSE(scores1[1].has_value());

    // Chunk 2 through the raw-buffer overload, with offsets beyond the
    // first expression batch.
    FixedVector<int32_t> chunk2 = {9000, 9001, static_cast<int32_t>(N - 2)};
    std::vector<float> scores2(chunk2.size(), -1.0F);
    auto has_scores2 = std::make_unique<bool[]>(chunk2.size());
    ComputeScorerScores(&exec_context,
                        &op_context,
                        segment.get(),
                        scorer,
                        chunk2,
                        scores2.data(),
                        has_scores2.get(),
                        &filter_bitset.value());
    EXPECT_TRUE(has_scores2[0]);
    EXPECT_FLOAT_EQ(scores2[0], 2.0F);
    EXPECT_FALSE(has_scores2[1]);
    EXPECT_TRUE(has_scores2[2]);
    EXPECT_FLOAT_EQ(scores2[2], 2.0F);
}

// Deciding native-vs-non-native already compiles the filter (and pins its
// scalar indexes). A native filter yields no hoisted bitset, but the compiled
// expressions must come back through out_expr_set so per-chunk scoring reuses
// them instead of recompiling once per chunk. Reusing one ExprSet across
// chunks must produce exactly what a freshly compiled one produces.
TEST(BoostScoreRunnerTest, NativeFilterHandsBackReusableExprSet) {
    const int64_t N = 10000;  // more than one expression batch (8192)
    auto schema = GenTextMatchSchema();
    auto raw_data = segcore::DataGen(schema, N);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // An int64 unary range expression consumes offset input natively.
    proto::plan::GenericValue val;
    val.set_int64_val(0);
    auto filter = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(FieldId(100), DataType::INT64),
        proto::plan::OpType::GreaterEqual,
        val);
    auto scorer = std::make_shared<WeightScorer>(filter, 2.0F);

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_native_expr_set_reuse", segment.get(), N, MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    std::unique_ptr<exec::ExprSet> expr_set;
    auto filter_bitset =
        ComputeNonNativeFilterBitset(&exec_context, scorer, &expr_set);
    EXPECT_FALSE(filter_bitset.has_value());
    ASSERT_NE(expr_set, nullptr);

    // Two chunks, the second past the first expression batch, scored against
    // the single reused ExprSet.
    FixedVector<int32_t> chunk1 = {0, 1, 2};
    FixedVector<int32_t> chunk2 = {9000, 9001, static_cast<int32_t>(N - 1)};
    std::vector<std::optional<float>> reused1(chunk1.size(), std::nullopt);
    std::vector<std::optional<float>> reused2(chunk2.size(), std::nullopt);
    ComputeScorerScores(&exec_context,
                        &op_context,
                        segment.get(),
                        scorer,
                        chunk1,
                        reused1,
                        nullptr,
                        expr_set.get());
    ComputeScorerScores(&exec_context,
                        &op_context,
                        segment.get(),
                        scorer,
                        chunk2,
                        reused2,
                        nullptr,
                        expr_set.get());

    // The same chunks, each compiling its own ExprSet (the old behaviour).
    std::vector<std::optional<float>> fresh1(chunk1.size(), std::nullopt);
    std::vector<std::optional<float>> fresh2(chunk2.size(), std::nullopt);
    ComputeScorerScores(
        &exec_context, &op_context, segment.get(), scorer, chunk1, fresh1);
    ComputeScorerScores(
        &exec_context, &op_context, segment.get(), scorer, chunk2, fresh2);

    EXPECT_EQ(reused1, fresh1);
    EXPECT_EQ(reused2, fresh2);
}

// Cross-chunk ExprSet reuse must also hold on the ScalarIndex exec path --
// the only path with a stateful index cursor that could in principle desync
// across chunks. It cannot: the index branch is gated on !has_offset_input_
// and MoveCursor() is a no-op while offset input is set, so offset-input
// evaluation never touches the cursor. The sibling test above filters the
// primary key, which resolves to PkIndex and skips that machinery entirely;
// this variant loads a real STL_SORT index on a non-pk field and pins the
// resolved path via UseIndexCursor() so the invariant is actually exercised.
TEST(BoostScoreRunnerTest, NativeFilterExprSetReuseOnScalarIndexPath) {
    const int64_t N = 10000;  // more than one expression batch (8192)
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto age_fid = schema->AddDebugField("age", DataType::INT64);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(pk_fid);

    auto raw_data = segcore::DataGen(schema, N);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // DataGen fills the non-pk int64 column with the row index, so
    // `age >= 5000` matches exactly the rows past the midpoint.
    auto age_col = raw_data.get_col<int64_t>(age_fid);
    auto age_index = milvus::index::CreateScalarIndexSort<int64_t>();
    age_index->Build(N, age_col.data());
    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = age_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_params = GenIndexParams(age_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test_age_index", std::move(age_index));
    segment->LoadIndex(load_index_info);

    proto::plan::GenericValue val;
    val.set_int64_val(5000);
    auto filter = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(age_fid, DataType::INT64),
        proto::plan::OpType::GreaterEqual,
        val);
    auto scorer = std::make_shared<WeightScorer>(filter, 2.0F);

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_native_expr_set_reuse_scalar_index",
        segment.get(),
        N,
        MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    std::unique_ptr<exec::ExprSet> expr_set;
    auto filter_bitset =
        ComputeNonNativeFilterBitset(&exec_context, scorer, &expr_set);
    EXPECT_FALSE(filter_bitset.has_value());
    ASSERT_NE(expr_set, nullptr);

    // Pin the exec path this variant exists for: with the index loaded the
    // compiled expression must resolve to ScalarIndex, not RawData/PkIndex,
    // or the reuse-under-index-cursor invariant goes untested.
    ASSERT_EQ(expr_set->exprs().size(), 1u);
    auto segment_expr =
        std::dynamic_pointer_cast<exec::SegmentExpr>(expr_set->exprs()[0]);
    ASSERT_NE(segment_expr, nullptr);
    ASSERT_TRUE(segment_expr->UseIndexCursor())
        << "filter did not resolve to the ScalarIndex path; the reuse "
           "invariant is not being exercised";

    // Two chunks straddling the expression batch boundary, scored against
    // the single reused ExprSet.
    FixedVector<int32_t> chunk1 = {0, 4999, 5000};
    FixedVector<int32_t> chunk2 = {9000, 9001, static_cast<int32_t>(N - 1)};
    std::vector<std::optional<float>> reused1(chunk1.size(), std::nullopt);
    std::vector<std::optional<float>> reused2(chunk2.size(), std::nullopt);
    ComputeScorerScores(&exec_context,
                        &op_context,
                        segment.get(),
                        scorer,
                        chunk1,
                        reused1,
                        nullptr,
                        expr_set.get());
    ComputeScorerScores(&exec_context,
                        &op_context,
                        segment.get(),
                        scorer,
                        chunk2,
                        reused2,
                        nullptr,
                        expr_set.get());

    // Semantic expectations, not just reuse==fresh: rows below 5000 get no
    // boost, rows at or above it do.
    EXPECT_FALSE(reused1[0].has_value());  // 0
    EXPECT_FALSE(reused1[1].has_value());  // 4999
    ASSERT_TRUE(reused1[2].has_value());   // 5000
    EXPECT_FLOAT_EQ(reused1[2].value(), 2.0F);
    for (size_t i = 0; i < reused2.size(); ++i) {
        ASSERT_TRUE(reused2[i].has_value()) << "offset idx " << i;
        EXPECT_FLOAT_EQ(reused2[i].value(), 2.0F);
    }

    // The same chunks, each compiling its own ExprSet, must agree.
    std::vector<std::optional<float>> fresh1(chunk1.size(), std::nullopt);
    std::vector<std::optional<float>> fresh2(chunk2.size(), std::nullopt);
    ComputeScorerScores(
        &exec_context, &op_context, segment.get(), scorer, chunk1, fresh1);
    ComputeScorerScores(
        &exec_context, &op_context, segment.get(), scorer, chunk2, fresh2);
    EXPECT_EQ(reused1, fresh1);
    EXPECT_EQ(reused2, fresh2);
}

// The non-native branch advances its ExprSet to the end of the segment while
// building the bitset, so a spent ExprSet must never be handed back for reuse.
TEST(BoostScoreRunnerTest, NonNativeFilterDoesNotHandBackSpentExprSet) {
    const int64_t N = 100;
    auto schema = GenTextMatchSchema();
    auto raw_data = segcore::DataGen(schema, N);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    segment->CreateTextIndex(FieldId(101));

    auto filter = GenTextMatchTypedExpr(schema, "football");
    auto scorer = std::make_shared<WeightScorer>(filter, 2.0F);

    auto query_context = std::make_shared<exec::QueryContext>(
        "test_non_native_expr_set", segment.get(), N, MAX_TIMESTAMP);
    OpContext op_context;
    query_context->set_op_context(&op_context);
    auto exec_context = exec::ExecContext(query_context.get());

    std::unique_ptr<exec::ExprSet> expr_set;
    auto filter_bitset =
        ComputeNonNativeFilterBitset(&exec_context, scorer, &expr_set);
    ASSERT_TRUE(filter_bitset.has_value());
    EXPECT_EQ(expr_set, nullptr);
}

// Passing no sink must keep the original two-argument behaviour intact.
TEST(BoostScoreRunnerTest, ExprSetSinkIsOptional) {
    auto scorer = std::make_shared<WeightScorer>(nullptr, 2.0F);
    EXPECT_FALSE(
        ComputeNonNativeFilterBitset(nullptr, scorer, nullptr).has_value());
}
