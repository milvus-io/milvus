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

#include "common/ArrayOffsets.h"

TEST(ArrayOffsetsSealedTest,
     BuildAllZerosKeepsEmptyRangesAcrossRepeatedConstruction) {
    constexpr int64_t kRowCount = 1000;
    auto offsets = milvus::ArrayOffsetsSealed::BuildAllZeros(kRowCount);
    ASSERT_NE(offsets, nullptr);

    EXPECT_EQ(offsets->GetRowCount(), kRowCount);
    // Every row is an empty array -> zero total elements.
    EXPECT_EQ(offsets->GetTotalElementCount(), 0);

    // Each old row maps to an empty element range [x, x).
    for (int32_t row : {0, 1, 499, 999}) {
        auto range = offsets->ElementIDRangeOfRow(row);
        EXPECT_EQ(range.first, range.second)
            << "row " << row << " should be an empty array";
        EXPECT_EQ(range.first, 0);
    }

    // Each newly constructed table independently describes empty arrays.
    for (int i = 0; i < 256; ++i) {
        auto tmp = milvus::ArrayOffsetsSealed::BuildAllZeros(500);
        ASSERT_EQ(tmp->GetRowCount(), 500);
        ASSERT_EQ(tmp->GetTotalElementCount(), 0);
        EXPECT_EQ(tmp->ElementIDRangeOfRow(0), std::make_pair(0, 0));
        EXPECT_EQ(tmp->ElementIDRangeOfRow(499), std::make_pair(0, 0));
    }
}
