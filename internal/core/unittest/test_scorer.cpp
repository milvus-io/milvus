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

#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "pb/plan.pb.h"
#include "rescores/BoostScoreRunner.h"
#include "rescores/Scorer.h"

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
