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
#include <vector>

#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "pb/plan.pb.h"
#include "rescores/Scorer.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::rescores;
using namespace milvus::segcore;

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

namespace {

// Build a ScoreFunction with the given key/value params for FieldScorer tests.
proto::plan::ScoreFunction
MakeFieldScoreFunction(
    const std::vector<std::pair<std::string, std::string>>& kvs) {
    proto::plan::ScoreFunction func;
    func.set_type(proto::plan::FunctionTypeField);
    for (const auto& [k, v] : kvs) {
        auto* param = func.add_params();
        param->set_key(k);
        param->set_value(v);
    }
    return func;
}

expr::TypedExprPtr kNoFilter;  // null filter for simple scorer tests

}  // namespace

TEST(FieldScorerTest, ConstructorMissingFieldIdThrows) {
    auto func = MakeFieldScoreFunction({});
    EXPECT_THROW(FieldScorer(kNoFilter, 1.0f, func.params()), std::exception);
}

TEST(FieldScorerTest, ConstructorInvalidFieldIdThrows) {
    auto func = MakeFieldScoreFunction({{"field_id", "not-a-number"}});
    EXPECT_THROW(FieldScorer(kNoFilter, 1.0f, func.params()), std::exception);
}

TEST(FieldScorerTest, ConstructorInvalidMissingValueThrows) {
    auto func = MakeFieldScoreFunction(
        {{"field_id", "100"}, {"missing_value", "not-a-number"}});
    EXPECT_THROW(FieldScorer(kNoFilter, 1.0f, func.params()), std::exception);
}

TEST(FieldScorerTest, ConstructorValidParamsKeepsWeight) {
    auto func = MakeFieldScoreFunction(
        {{"field_id", "123"}, {"missing_value", "42.5"}});
    FieldScorer scorer(kNoFilter, 3.5f, func.params());
    EXPECT_FLOAT_EQ(scorer.weight(), 3.5f);
}

// Build a tiny sealed segment with one int64 "score" field and a float vector.
class FieldScorerSegmentTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
        score_fid_ = schema_->AddDebugField("score", DataType::INT64);
        float_fid_ = schema_->AddDebugField(
            "opt_score", DataType::FLOAT, /*nullable=*/true);
        auto pk_fid = schema_->AddDebugField("pk", DataType::INT64);
        schema_->set_primary_field_id(pk_fid);

        constexpr int64_t kN = 16;
        num_rows_ = kN;
        auto dataset = DataGen(schema_, kN);
        score_col_ = dataset.get_col<int64_t>(score_fid_);
        float_col_ = dataset.get_col<float>(float_fid_);
        auto segment = CreateSealedWithFieldDataLoaded(schema_, dataset);
        segment_ = segcore::SegmentSealedSPtr(segment.release());
    }

    proto::plan::ScoreFunction
    MakeFuncForField(FieldId fid,
                     std::optional<float> missing = std::nullopt) {
        std::vector<std::pair<std::string, std::string>> kvs = {
            {"field_id", std::to_string(fid.get())},
        };
        if (missing.has_value()) {
            kvs.emplace_back("missing_value", std::to_string(*missing));
        }
        return MakeFieldScoreFunction(kvs);
    }

    SchemaPtr schema_;
    FieldId vec_fid_;
    FieldId score_fid_;
    FieldId float_fid_;
    int64_t num_rows_{0};
    FixedVector<int64_t> score_col_;
    FixedVector<float> float_col_;
    segcore::SegmentSealedSPtr segment_;
};

TEST_F(FieldScorerSegmentTest, BatchScoreNoFilterMultipliesByWeight) {
    auto func = MakeFuncForField(score_fid_);
    FieldScorer scorer(kNoFilter, 2.5f, func.params());

    FixedVector<int32_t> offsets;
    offsets.reserve(num_rows_);
    for (int64_t i = 0; i < num_rows_; ++i) {
        offsets.push_back(static_cast<int32_t>(i));
    }
    std::vector<std::optional<float>> boost(num_rows_, std::nullopt);
    auto mode = proto::plan::FunctionMode::FunctionModeSum;

    scorer.batch_score(nullptr, segment_.get(), mode, offsets, boost);

    for (int64_t i = 0; i < num_rows_; ++i) {
        ASSERT_TRUE(boost[i].has_value()) << i;
        EXPECT_FLOAT_EQ(*boost[i], 2.5f * static_cast<float>(score_col_[i]))
            << "row " << i;
    }
}

TEST_F(FieldScorerSegmentTest, BatchScoreWithBitmapFiltersSkipped) {
    auto func = MakeFuncForField(score_fid_);
    FieldScorer scorer(kNoFilter, 1.0f, func.params());

    FixedVector<int32_t> offsets = {0, 2, 4, 6};
    TargetBitmap bitmap(offsets.size());
    bitmap.set(0);  // keep offsets[0]
    bitmap.set(2);  // keep offsets[2]
    std::vector<std::optional<float>> boost(offsets.size(), std::nullopt);
    auto mode = proto::plan::FunctionMode::FunctionModeSum;

    scorer.batch_score(
        nullptr, segment_.get(), mode, offsets, TargetBitmapView(bitmap), boost);

    EXPECT_TRUE(boost[0].has_value());
    EXPECT_FALSE(boost[1].has_value());
    EXPECT_TRUE(boost[2].has_value());
    EXPECT_FALSE(boost[3].has_value());
    EXPECT_FLOAT_EQ(*boost[0], static_cast<float>(score_col_[0]));
    EXPECT_FLOAT_EQ(*boost[2], static_cast<float>(score_col_[4]));
}

TEST_F(FieldScorerSegmentTest, BatchScoreNullRowUsesMissingValue) {
    auto func = MakeFuncForField(float_fid_, /*missing=*/100.0f);
    FieldScorer scorer(kNoFilter, 1.0f, func.params());

    FixedVector<int32_t> offsets;
    offsets.reserve(num_rows_);
    for (int64_t i = 0; i < num_rows_; ++i) {
        offsets.push_back(static_cast<int32_t>(i));
    }
    std::vector<std::optional<float>> boost(num_rows_, std::nullopt);
    auto mode = proto::plan::FunctionMode::FunctionModeSum;

    // DataGen marks even-index rows valid and odd-index null when nullable.
    scorer.batch_score(nullptr, segment_.get(), mode, offsets, boost);

    for (int64_t i = 0; i < num_rows_; ++i) {
        ASSERT_TRUE(boost[i].has_value()) << i;
        if (i % 2 == 0) {
            EXPECT_FLOAT_EQ(*boost[i], float_col_[i]) << i;
        } else {
            EXPECT_FLOAT_EQ(*boost[i], 100.0f) << i;
        }
    }
}

TEST_F(FieldScorerSegmentTest, BatchScoreNullRowWithoutMissingValueThrows) {
    auto func = MakeFuncForField(float_fid_);  // no missing_value
    FieldScorer scorer(kNoFilter, 1.0f, func.params());

    FixedVector<int32_t> offsets;
    offsets.reserve(num_rows_);
    for (int64_t i = 0; i < num_rows_; ++i) {
        offsets.push_back(static_cast<int32_t>(i));
    }
    std::vector<std::optional<float>> boost(num_rows_, std::nullopt);
    auto mode = proto::plan::FunctionMode::FunctionModeSum;

    EXPECT_THROW(
        scorer.batch_score(nullptr, segment_.get(), mode, offsets, boost),
        std::exception);
}
