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

#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/common.h"
#include "common/ArrayOffsets.h"
#include "common/Types.h"
#include "gtest/gtest.h"

using namespace milvus;

class ArrayOffsetsTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }
};

TEST_F(ArrayOffsetsTest, SealedBasic) {
    // Create a simple ArrayOffsetsSealed manually
    // row 0: 2 elements (elem 0, 1)
    // row 1: 3 elements (elem 2, 3, 4)
    // row 2: 1 element  (elem 5)
    ArrayOffsetsSealed offsets(
        {0, 0, 1, 1, 1, 2},  // element_row_ids
        {0, 2, 5, 6}         // row_to_element_start (size = row_count + 1)
    );

    // Test GetRowCount
    EXPECT_EQ(offsets.GetRowCount(), 3);

    // Test GetTotalElementCount
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    // Test ElementIDToRowID
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(0);
        EXPECT_EQ(row_id, 0);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(1);
        EXPECT_EQ(row_id, 0);
        EXPECT_EQ(elem_idx, 1);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(2);
        EXPECT_EQ(row_id, 1);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(4);
        EXPECT_EQ(row_id, 1);
        EXPECT_EQ(elem_idx, 2);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(5);
        EXPECT_EQ(row_id, 2);
        EXPECT_EQ(elem_idx, 0);
    }

    // Test ElementIDRangeOfRow
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(0);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 2);
    }
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 5);
    }
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(2);
        EXPECT_EQ(start, 5);
        EXPECT_EQ(end, 6);
    }
    // When row_id == row_count, return (total_elements, total_elements)
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(3);
        EXPECT_EQ(start, 6);
        EXPECT_EQ(end, 6);
    }
}

TEST_F(ArrayOffsetsTest, SealedRowBitsetToElementBitset) {
    ArrayOffsetsSealed offsets({0, 0, 1, 1, 1, 2},  // element_row_ids
                               {0, 2, 5, 6}         // row_to_element_start
    );

    // row_bitset: row 0 = true, row 1 = false, row 2 = true
    TargetBitmap row_bitset(3);
    row_bitset[0] = true;
    row_bitset[1] = false;
    row_bitset[2] = true;

    TargetBitmap valid_row_bitset(3, true);

    TargetBitmapView row_view(row_bitset.data(), row_bitset.size());
    TargetBitmapView valid_view(valid_row_bitset.data(),
                                valid_row_bitset.size());

    auto [elem_bitset, valid_elem_bitset] =
        offsets.RowBitsetToElementBitset(row_view, valid_view, 0);

    EXPECT_EQ(elem_bitset.size(), 6);
    // Elements of row 0 (elem 0, 1) should be true
    EXPECT_TRUE(elem_bitset[0]);
    EXPECT_TRUE(elem_bitset[1]);
    // Elements of row 1 (elem 2, 3, 4) should be false
    EXPECT_FALSE(elem_bitset[2]);
    EXPECT_FALSE(elem_bitset[3]);
    EXPECT_FALSE(elem_bitset[4]);
    // Elements of row 2 (elem 5) should be true
    EXPECT_TRUE(elem_bitset[5]);
}

TEST_F(ArrayOffsetsTest, SealedEmptyArrays) {
    // Test with some rows having empty arrays
    // row 1 and row 3 are empty
    ArrayOffsetsSealed offsets({0, 0, 2, 2, 2},  // element_row_ids
                               {0, 2, 2, 5, 5}   // row_to_element_start
    );

    EXPECT_EQ(offsets.GetRowCount(), 4);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);

    // Row 1 has no elements
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 2);  // empty range
    }
    // Row 3 has no elements
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(3);
        EXPECT_EQ(start, 5);
        EXPECT_EQ(end, 5);  // empty range
    }
}

TEST_F(ArrayOffsetsTest, GrowingBasicInsert) {
    ArrayOffsetsGrowing offsets;

    // Insert rows in order
    std::vector<int32_t> lens1 = {2};  // row 0: 2 elements
    offsets.Insert(0, lens1.data(), 1);

    std::vector<int32_t> lens2 = {3};  // row 1: 3 elements
    offsets.Insert(1, lens2.data(), 1);

    std::vector<int32_t> lens3 = {1};  // row 2: 1 element
    offsets.Insert(2, lens3.data(), 1);

    EXPECT_EQ(offsets.GetRowCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    // Test ElementIDToRowID
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(0);
        EXPECT_EQ(row_id, 0);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(2);
        EXPECT_EQ(row_id, 1);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(5);
        EXPECT_EQ(row_id, 2);
        EXPECT_EQ(elem_idx, 0);
    }

    // Test ElementIDRangeOfRow
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(0);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 2);
    }
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 5);
    }
    // When row_id == row_count, return (total_elements, total_elements)
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(3);
        EXPECT_EQ(start, 6);
        EXPECT_EQ(end, 6);
    }
}

TEST_F(ArrayOffsetsTest, GrowingBatchInsert) {
    ArrayOffsetsGrowing offsets;

    // Insert multiple rows at once
    std::vector<int32_t> lens = {2, 3, 1};  // row 0, 1, 2
    offsets.Insert(0, lens.data(), 3);

    EXPECT_EQ(offsets.GetRowCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    {
        auto [start, end] = offsets.ElementIDRangeOfRow(0);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 2);
    }
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 5);
    }
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(2);
        EXPECT_EQ(start, 5);
        EXPECT_EQ(end, 6);
    }
}

TEST_F(ArrayOffsetsTest, GrowingOutOfOrderInsert) {
    ArrayOffsetsGrowing offsets;

    // Insert out of order - row 2 arrives before row 1
    std::vector<int32_t> lens0 = {2};
    offsets.Insert(0, lens0.data(), 1);  // row 0

    std::vector<int32_t> lens2 = {1};
    offsets.Insert(2, lens2.data(), 1);  // row 2 (pending)

    // row 1 not inserted yet, so only row 0 should be committed
    EXPECT_EQ(offsets.GetRowCount(), 1);
    EXPECT_EQ(offsets.GetTotalElementCount(), 2);

    // Now insert row 1, which should drain pending row 2
    std::vector<int32_t> lens1 = {3};
    offsets.Insert(1, lens1.data(), 1);  // row 1

    // Now all 3 rows should be committed
    EXPECT_EQ(offsets.GetRowCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    // Verify order is correct
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(0);
        EXPECT_EQ(row_id, 0);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(2);
        EXPECT_EQ(row_id, 1);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(5);
        EXPECT_EQ(row_id, 2);
    }
}

TEST_F(ArrayOffsetsTest, GrowingEmptyArrays) {
    ArrayOffsetsGrowing offsets;

    // Insert rows with some empty arrays
    std::vector<int32_t> lens = {2, 0, 3, 0};  // row 1 and row 3 are empty
    offsets.Insert(0, lens.data(), 4);

    EXPECT_EQ(offsets.GetRowCount(), 4);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);

    // Row 1 has no elements
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(1);
        EXPECT_EQ(start, 2);
        EXPECT_EQ(end, 2);
    }
    // Row 3 has no elements
    {
        auto [start, end] = offsets.ElementIDRangeOfRow(3);
        EXPECT_EQ(start, 5);
        EXPECT_EQ(end, 5);
    }
}

TEST_F(ArrayOffsetsTest, GrowingRowBitsetToElementBitset) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

    TargetBitmap row_bitset(3);
    row_bitset[0] = true;
    row_bitset[1] = false;
    row_bitset[2] = true;

    TargetBitmap valid_row_bitset(3, true);

    TargetBitmapView row_view(row_bitset.data(), row_bitset.size());
    TargetBitmapView valid_view(valid_row_bitset.data(),
                                valid_row_bitset.size());

    auto [elem_bitset, valid_elem_bitset] =
        offsets.RowBitsetToElementBitset(row_view, valid_view, 0);

    EXPECT_EQ(elem_bitset.size(), 6);
    EXPECT_TRUE(elem_bitset[0]);
    EXPECT_TRUE(elem_bitset[1]);
    EXPECT_FALSE(elem_bitset[2]);
    EXPECT_FALSE(elem_bitset[3]);
    EXPECT_FALSE(elem_bitset[4]);
    EXPECT_TRUE(elem_bitset[5]);
}

TEST_F(ArrayOffsetsTest, GrowingConcurrentRead) {
    ArrayOffsetsGrowing offsets;

    // Insert initial data
    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

    // Concurrent reads should be safe
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&offsets]() {
            for (int i = 0; i < 1000; ++i) {
                auto row_count = offsets.GetRowCount();
                auto elem_count = offsets.GetTotalElementCount();
                EXPECT_GE(row_count, 0);
                EXPECT_GE(elem_count, 0);

                if (row_count > 0) {
                    auto [start, end] = offsets.ElementIDRangeOfRow(0);
                    EXPECT_GE(start, 0);
                    EXPECT_GE(end, start);
                }

                if (elem_count > 0) {
                    auto [row_id, elem_idx] = offsets.ElementIDToRowID(0);
                    EXPECT_GE(row_id, 0);
                    EXPECT_GE(elem_idx, 0);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(ArrayOffsetsTest, SingleRow) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {5};
    offsets.Insert(0, lens.data(), 1);

    EXPECT_EQ(offsets.GetRowCount(), 1);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);

    for (int i = 0; i < 5; ++i) {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(i);
        EXPECT_EQ(row_id, 0);
        EXPECT_EQ(elem_idx, i);
    }

    auto [start, end] = offsets.ElementIDRangeOfRow(0);
    EXPECT_EQ(start, 0);
    EXPECT_EQ(end, 5);
}

TEST_F(ArrayOffsetsTest, SingleElementPerRow) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {1, 1, 1, 1, 1};
    offsets.Insert(0, lens.data(), 5);

    EXPECT_EQ(offsets.GetRowCount(), 5);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);

    for (int i = 0; i < 5; ++i) {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(i);
        EXPECT_EQ(row_id, i);
        EXPECT_EQ(elem_idx, 0);

        auto [start, end] = offsets.ElementIDRangeOfRow(i);
        EXPECT_EQ(start, i);
        EXPECT_EQ(end, i + 1);
    }
}

TEST_F(ArrayOffsetsTest, LargeArrayLength) {
    ArrayOffsetsGrowing offsets;

    // Single row with many elements
    std::vector<int32_t> lens = {10000};
    offsets.Insert(0, lens.data(), 1);

    EXPECT_EQ(offsets.GetRowCount(), 1);
    EXPECT_EQ(offsets.GetTotalElementCount(), 10000);

    // Test first, middle, and last elements
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(0);
        EXPECT_EQ(row_id, 0);
        EXPECT_EQ(elem_idx, 0);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(5000);
        EXPECT_EQ(row_id, 0);
        EXPECT_EQ(elem_idx, 5000);
    }
    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(9999);
        EXPECT_EQ(row_id, 0);
        EXPECT_EQ(elem_idx, 9999);
    }
}
