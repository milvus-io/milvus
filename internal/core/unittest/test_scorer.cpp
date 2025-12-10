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
#include <optional>
#include <vector>

#include "common/Types.h"
#include "rescores/Scorer.h"
#include "pb/plan.pb.h"

using namespace milvus;
using namespace milvus::rescores;

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
