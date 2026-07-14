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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <random>
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
    ArrayOffsetsSealed offsets({0, 2, 5, 6}
                               // row_to_element_start (size = row_count + 1)
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
    ArrayOffsetsSealed offsets({0, 2, 5, 6}  // row_to_element_start
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
    // row 0: 2 elements, row 1: 0 elements, row 2: 3 elements, row 3: 0 elements
    ArrayOffsetsSealed offsets({0, 2, 2, 5, 5}  // row_to_element_start
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

TEST_F(ArrayOffsetsTest, SealedRowBitsetToElementOffsets) {
    // row 0: 2 elements (elem 0, 1)
    // row 1: 3 elements (elem 2, 3, 4)
    // row 2: 1 element  (elem 5)
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

    // Select row 0 and row 2
    TargetBitmap row_bitset(3);
    row_bitset[0] = true;
    row_bitset[1] = false;
    row_bitset[2] = true;
    TargetBitmapView view(row_bitset.data(), row_bitset.size());

    auto elem_offsets = offsets.RowBitsetToElementOffsets(view, 0);

    // Should return elements from row 0 (0, 1) and row 2 (5)
    EXPECT_EQ(elem_offsets.size(), 3);
    EXPECT_EQ(elem_offsets[0], 0);
    EXPECT_EQ(elem_offsets[1], 1);
    EXPECT_EQ(elem_offsets[2], 5);
}

TEST_F(ArrayOffsetsTest, SealedRowBitsetToElementOffsetsWithRowStart) {
    // row 0: 2 elements (elem 0, 1)
    // row 1: 3 elements (elem 2, 3, 4)
    // row 2: 1 element  (elem 5)
    // row 3: 2 elements (elem 6, 7)
    ArrayOffsetsSealed offsets({0, 2, 5, 6, 8});

    // Select from row 1 onwards, select row 1 and row 3 (relative indices 0 and 2)
    TargetBitmap row_bitset(3);
    row_bitset[0] = true;   // row 1
    row_bitset[1] = false;  // row 2
    row_bitset[2] = true;   // row 3
    TargetBitmapView view(row_bitset.data(), row_bitset.size());

    auto elem_offsets = offsets.RowBitsetToElementOffsets(view, 1);

    // Should return elements from row 1 (2, 3, 4) and row 3 (6, 7)
    EXPECT_EQ(elem_offsets.size(), 5);
    EXPECT_EQ(elem_offsets[0], 2);
    EXPECT_EQ(elem_offsets[1], 3);
    EXPECT_EQ(elem_offsets[2], 4);
    EXPECT_EQ(elem_offsets[3], 6);
    EXPECT_EQ(elem_offsets[4], 7);
}

TEST_F(ArrayOffsetsTest, SealedRowBitsetToElementOffsetsEmpty) {
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

    // No rows selected
    TargetBitmap row_bitset(3, false);
    TargetBitmapView view(row_bitset.data(), row_bitset.size());

    auto elem_offsets = offsets.RowBitsetToElementOffsets(view, 0);
    EXPECT_EQ(elem_offsets.size(), 0);
}

TEST_F(ArrayOffsetsTest, SealedRowOffsetsToElementOffsets) {
    // row 0: 2 elements (elem 0, 1)
    // row 1: 3 elements (elem 2, 3, 4)
    // row 2: 1 element  (elem 5)
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

    // Select row 0 and row 2
    FixedVector<int32_t> row_offsets = {0, 2};

    auto elem_offsets = offsets.RowOffsetsToElementOffsets(row_offsets);

    EXPECT_EQ(elem_offsets.size(), 3);
    EXPECT_EQ(elem_offsets[0], 0);
    EXPECT_EQ(elem_offsets[1], 1);
    EXPECT_EQ(elem_offsets[2], 5);
}

TEST_F(ArrayOffsetsTest, SealedRowOffsetsToElementOffsetsEmpty) {
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

    FixedVector<int32_t> row_offsets;
    auto elem_offsets = offsets.RowOffsetsToElementOffsets(row_offsets);
    EXPECT_EQ(elem_offsets.size(), 0);
}

TEST_F(ArrayOffsetsTest, SealedForEachRowElementRange) {
    // row 0: 2 elements (elem 0, 1)
    // row 1: 3 elements (elem 2, 3, 4)
    // row 2: 1 element  (elem 5)
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

    // Predicate: return true if row has more than 1 element
    auto predicate = [](int32_t elem_start, int32_t elem_end) {
        return (elem_end - elem_start) > 1;
    };

    auto result = offsets.ForEachRowElementRange(predicate, 0, 3);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(result[0]);   // row 0: 2 elements > 1
    EXPECT_TRUE(result[1]);   // row 1: 3 elements > 1
    EXPECT_FALSE(result[2]);  // row 2: 1 element <= 1
}

TEST_F(ArrayOffsetsTest, SealedForEachRowElementRangeWithRowStart) {
    // row 0: 2 elements, row 1: 3 elements, row 2: 1 element, row 3: 4 elements
    ArrayOffsetsSealed offsets({0, 2, 5, 6, 10});

    auto predicate = [](int32_t elem_start, int32_t elem_end) {
        return (elem_end - elem_start) >= 3;
    };

    // Start from row 1, check 3 rows
    auto result = offsets.ForEachRowElementRange(predicate, 1, 3);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(result[0]);   // row 1: 3 elements >= 3
    EXPECT_FALSE(result[1]);  // row 2: 1 element < 3
    EXPECT_TRUE(result[2]);   // row 3: 4 elements >= 3
}

TEST_F(ArrayOffsetsTest, SealedRowBitsetToElementBitsetWithRowStart) {
    // row 0: 2 elements, row 1: 3 elements, row 2: 1 element, row 3: 2 elements
    ArrayOffsetsSealed offsets({0, 2, 5, 6, 8});

    // Start from row 1, select row 1 and row 3
    TargetBitmap row_bitset(3);
    row_bitset[0] = true;   // row 1
    row_bitset[1] = false;  // row 2
    row_bitset[2] = true;   // row 3

    TargetBitmap valid_row_bitset(3, true);

    TargetBitmapView row_view(row_bitset.data(), row_bitset.size());
    TargetBitmapView valid_view(valid_row_bitset.data(),
                                valid_row_bitset.size());

    auto [elem_bitset, valid_elem_bitset] =
        offsets.RowBitsetToElementBitset(row_view, valid_view, 1);

    // Elements from row 1-3: elem 2,3,4,5,6,7 (6 elements)
    EXPECT_EQ(elem_bitset.size(), 6);
    // Row 1 (elem 2,3,4 -> indices 0,1,2) = true
    EXPECT_TRUE(elem_bitset[0]);
    EXPECT_TRUE(elem_bitset[1]);
    EXPECT_TRUE(elem_bitset[2]);
    // Row 2 (elem 5 -> index 3) = false
    EXPECT_FALSE(elem_bitset[3]);
    // Row 3 (elem 6,7 -> indices 4,5) = true
    EXPECT_TRUE(elem_bitset[4]);
    EXPECT_TRUE(elem_bitset[5]);
}

TEST_F(ArrayOffsetsTest, SealedRowBitsetToElementBitsetWithInvalidRows) {
    // row 0: 2 elements, row 1: 3 elements, row 2: 1 element
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

    TargetBitmap row_bitset(3);
    row_bitset[0] = true;
    row_bitset[1] = true;
    row_bitset[2] = true;

    // Row 1 is invalid (e.g., NULL)
    TargetBitmap valid_row_bitset(3);
    valid_row_bitset[0] = true;
    valid_row_bitset[1] = false;  // invalid
    valid_row_bitset[2] = true;

    TargetBitmapView row_view(row_bitset.data(), row_bitset.size());
    TargetBitmapView valid_view(valid_row_bitset.data(),
                                valid_row_bitset.size());

    auto [elem_bitset, valid_elem_bitset] =
        offsets.RowBitsetToElementBitset(row_view, valid_view, 0);

    EXPECT_EQ(elem_bitset.size(), 6);
    EXPECT_EQ(valid_elem_bitset.size(), 6);

    // Row 0 elements: valid
    EXPECT_TRUE(valid_elem_bitset[0]);
    EXPECT_TRUE(valid_elem_bitset[1]);
    // Row 1 elements: invalid
    EXPECT_FALSE(valid_elem_bitset[2]);
    EXPECT_FALSE(valid_elem_bitset[3]);
    EXPECT_FALSE(valid_elem_bitset[4]);
    // Row 2 elements: valid
    EXPECT_TRUE(valid_elem_bitset[5]);
}

TEST_F(ArrayOffsetsTest, GrowingRowBitsetToElementOffsets) {
    ArrayOffsetsGrowing offsets;
    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

    TargetBitmap row_bitset(3);
    row_bitset[0] = true;
    row_bitset[1] = false;
    row_bitset[2] = true;
    TargetBitmapView view(row_bitset.data(), row_bitset.size());

    auto elem_offsets = offsets.RowBitsetToElementOffsets(view, 0);

    EXPECT_EQ(elem_offsets.size(), 3);
    EXPECT_EQ(elem_offsets[0], 0);
    EXPECT_EQ(elem_offsets[1], 1);
    EXPECT_EQ(elem_offsets[2], 5);
}

TEST_F(ArrayOffsetsTest, GrowingRowOffsetsToElementOffsets) {
    ArrayOffsetsGrowing offsets;
    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

    FixedVector<int32_t> row_offsets = {0, 2};
    auto elem_offsets = offsets.RowOffsetsToElementOffsets(row_offsets);

    EXPECT_EQ(elem_offsets.size(), 3);
    EXPECT_EQ(elem_offsets[0], 0);
    EXPECT_EQ(elem_offsets[1], 1);
    EXPECT_EQ(elem_offsets[2], 5);
}

TEST_F(ArrayOffsetsTest, GrowingForEachRowElementRange) {
    ArrayOffsetsGrowing offsets;
    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

    auto predicate = [](int32_t elem_start, int32_t elem_end) {
        return (elem_end - elem_start) > 1;
    };

    auto result = offsets.ForEachRowElementRange(predicate, 0, 3);

    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(result[0]);   // 2 > 1
    EXPECT_TRUE(result[1]);   // 3 > 1
    EXPECT_FALSE(result[2]);  // 1 <= 1
}

TEST_F(ArrayOffsetsTest, GrowingRowBitsetToElementBitsetWithRowStart) {
    ArrayOffsetsGrowing offsets;
    std::vector<int32_t> lens = {2, 3, 1, 2};
    offsets.Insert(0, lens.data(), 4);

    TargetBitmap row_bitset(3);
    row_bitset[0] = true;   // row 1
    row_bitset[1] = false;  // row 2
    row_bitset[2] = true;   // row 3

    TargetBitmap valid_row_bitset(3, true);

    TargetBitmapView row_view(row_bitset.data(), row_bitset.size());
    TargetBitmapView valid_view(valid_row_bitset.data(),
                                valid_row_bitset.size());

    auto [elem_bitset, valid_elem_bitset] =
        offsets.RowBitsetToElementBitset(row_view, valid_view, 1);

    EXPECT_EQ(elem_bitset.size(), 6);
    EXPECT_TRUE(elem_bitset[0]);
    EXPECT_TRUE(elem_bitset[1]);
    EXPECT_TRUE(elem_bitset[2]);
    EXPECT_FALSE(elem_bitset[3]);
    EXPECT_TRUE(elem_bitset[4]);
    EXPECT_TRUE(elem_bitset[5]);
}

// ============ ElementBitsetToRowBitsetAny (word-wise ANY reduction) ============

namespace {

// Per-bit reference implementation of ANY-semantics element->row reduction.
TargetBitmap
ReferenceAnyReduce(const IArrayOffsets& offsets,
                   const TargetBitmap& elem_bitset,
                   int64_t elem_offset,
                   int64_t row_start,
                   int64_t row_count) {
    TargetBitmap expected(row_count);
    for (int64_t i = 0; i < row_count; ++i) {
        auto [start, end] = offsets.ElementIDRangeOfRow(row_start + i);
        for (int64_t e = start; e < end; ++e) {
            if (elem_bitset[e - elem_offset]) {
                expected[i] = true;
                break;
            }
        }
    }
    return expected;
}

void
CheckAnyReduce(const IArrayOffsets& offsets,
               const TargetBitmap& elem_bitset,
               int64_t elem_offset,
               int64_t row_start,
               int64_t row_count) {
    TargetBitmap actual(row_count);
    offsets.ElementBitsetToRowBitsetAny(
        elem_bitset.view(), elem_offset, row_start, actual.view());
    TargetBitmap expected = ReferenceAnyReduce(
        offsets, elem_bitset, elem_offset, row_start, row_count);
    ASSERT_EQ(actual.size(), expected.size());
    for (int64_t i = 0; i < row_count; ++i) {
        ASSERT_EQ(actual[i], expected[i])
            << "row " << i << " (row_start=" << row_start
            << ", elem_offset=" << elem_offset << ")";
    }
}

}  // namespace

TEST_F(ArrayOffsetsTest, SealedElementBitsetToRowBitsetAnyBasic) {
    // row 0: elems [0,2), row 1: elems [2,5), row 2: empty, row 3: elems [5,6)
    ArrayOffsetsSealed offsets({0, 2, 5, 5, 6});

    TargetBitmap elem_bitset(6);
    elem_bitset[3] = true;  // row 1
    elem_bitset[5] = true;  // row 3

    TargetBitmap row_bitset(4);
    offsets.ElementBitsetToRowBitsetAny(
        elem_bitset.view(), 0, 0, row_bitset.view());
    EXPECT_FALSE(row_bitset[0]);
    EXPECT_TRUE(row_bitset[1]);
    EXPECT_FALSE(row_bitset[2]);  // empty row never matches
    EXPECT_TRUE(row_bitset[3]);

    // Never clears pre-set bits.
    TargetBitmap preset(4);
    preset[0] = true;
    offsets.ElementBitsetToRowBitsetAny(
        elem_bitset.view(), 0, 0, preset.view());
    EXPECT_TRUE(preset[0]);
    EXPECT_TRUE(preset[1]);
    EXPECT_FALSE(preset[2]);
    EXPECT_TRUE(preset[3]);
}

TEST_F(ArrayOffsetsTest, SealedElementBitsetToRowBitsetAnyEdgeCases) {
    ArrayOffsetsSealed offsets({0, 2, 5, 5, 6});

    // All-zero element bitmap -> no rows.
    {
        TargetBitmap elem_bitset(6);
        TargetBitmap row_bitset(4);
        offsets.ElementBitsetToRowBitsetAny(
            elem_bitset.view(), 0, 0, row_bitset.view());
        EXPECT_EQ(row_bitset.count(), 0);
    }
    // All-one element bitmap -> all non-empty rows.
    {
        TargetBitmap elem_bitset(6, true);
        TargetBitmap row_bitset(4);
        offsets.ElementBitsetToRowBitsetAny(
            elem_bitset.view(), 0, 0, row_bitset.view());
        EXPECT_TRUE(row_bitset[0]);
        EXPECT_TRUE(row_bitset[1]);
        EXPECT_FALSE(row_bitset[2]);
        EXPECT_TRUE(row_bitset[3]);
    }
    // Empty row range.
    {
        TargetBitmap elem_bitset(6, true);
        TargetBitmap row_bitset(0);
        offsets.ElementBitsetToRowBitsetAny(
            elem_bitset.view(), 0, 0, row_bitset.view());
    }
    // Sub-range of rows with a batch-local bitmap (elem_offset != 0):
    // rows [1, 4) cover elements [2, 6).
    {
        TargetBitmap elem_bitset(4);
        elem_bitset[0] = true;  // global elem 2 -> row 1
        elem_bitset[3] = true;  // global elem 5 -> row 3
        TargetBitmap row_bitset(3);
        offsets.ElementBitsetToRowBitsetAny(elem_bitset.view(),
                                            /*elem_offset=*/2,
                                            /*row_start=*/1,
                                            row_bitset.view());
        EXPECT_TRUE(row_bitset[0]);
        EXPECT_FALSE(row_bitset[1]);
        EXPECT_TRUE(row_bitset[2]);
    }
    // Global bitmap but only a sub-range of rows: hits outside the row range
    // must be ignored.
    {
        TargetBitmap elem_bitset(6);
        elem_bitset[0] = true;  // row 0, outside range
        elem_bitset[3] = true;  // row 1, inside
        TargetBitmap row_bitset(2);
        offsets.ElementBitsetToRowBitsetAny(
            elem_bitset.view(), 0, /*row_start=*/1, row_bitset.view());
        EXPECT_TRUE(row_bitset[0]);   // row 1
        EXPECT_FALSE(row_bitset[1]);  // row 2 (empty)
    }
}

TEST_F(ArrayOffsetsTest, ElementBitsetToRowBitsetAnyRandomized) {
    std::mt19937 rng(12345);
    for (int iter = 0; iter < 20; ++iter) {
        // Random layout: mixes empty rows, short and long (multi-word) rows.
        int64_t num_rows = 1 + rng() % 300;
        std::vector<int32_t> starts(num_rows + 1, 0);
        std::vector<int32_t> lengths(num_rows);
        for (int64_t i = 0; i < num_rows; ++i) {
            int pick = rng() % 4;
            lengths[i] = pick == 0 ? 0 : (pick == 1 ? rng() % 3 : rng() % 200);
            starts[i + 1] = starts[i] + lengths[i];
        }
        int64_t total = starts[num_rows];

        ArrayOffsetsSealed sealed(starts);
        ArrayOffsetsGrowing growing;
        growing.Insert(0, lengths.data(), num_rows);

        for (double density : {0.0, 0.005, 0.1, 0.9, 1.0}) {
            TargetBitmap elem_bitset(total);
            for (int64_t e = 0; e < total; ++e) {
                if (rng() % 1000 < density * 1000) {
                    elem_bitset[e] = true;
                }
            }
            CheckAnyReduce(sealed, elem_bitset, 0, 0, num_rows);
            CheckAnyReduce(growing, elem_bitset, 0, 0, num_rows);

            // Random row sub-range with a batch-local element bitmap.
            int64_t row_start = rng() % num_rows;
            int64_t row_count = rng() % (num_rows - row_start + 1);
            int64_t elem_lo = starts[row_start];
            int64_t elem_hi = starts[row_start + row_count];
            TargetBitmap local(elem_hi - elem_lo);
            for (int64_t e = elem_lo; e < elem_hi; ++e) {
                if (elem_bitset[e]) {
                    local[e - elem_lo] = true;
                }
            }
            CheckAnyReduce(sealed, local, elem_lo, row_start, row_count);
            CheckAnyReduce(growing, local, elem_lo, row_start, row_count);
        }
    }
}

// ==== CopyRowElementStarts / CopyRowElementRanges (batched row lookups) ====

TEST_F(ArrayOffsetsTest, SealedCopyRowElementStarts) {
    // row 0: [0,2), row 1: [2,5), row 2: empty, row 3: [5,6)
    ArrayOffsetsSealed offsets({0, 2, 5, 5, 6});

    // Full range: row_count + 1 entries.
    {
        std::vector<int32_t> out(5, -1);
        offsets.CopyRowElementStarts(0, 4, out.data());
        EXPECT_EQ(out, (std::vector<int32_t>{0, 2, 5, 5, 6}));
    }
    // Sub-range starting mid-way (covers the empty row 2).
    {
        std::vector<int32_t> out(3, -1);
        offsets.CopyRowElementStarts(1, 2, out.data());
        EXPECT_EQ(out, (std::vector<int32_t>{2, 5, 5}));
    }
    // row_count == 0: single entry, the start of row_start (here the total,
    // since row_start == row count).
    {
        int32_t out = -1;
        offsets.CopyRowElementStarts(4, 0, &out);
        EXPECT_EQ(out, 6);
    }
    // Consistency with the per-row method.
    {
        std::vector<int32_t> out(5, -1);
        offsets.CopyRowElementStarts(0, 4, out.data());
        for (int32_t row = 0; row < 4; ++row) {
            auto [start, end] = offsets.ElementIDRangeOfRow(row);
            EXPECT_EQ(out[row], start);
            EXPECT_EQ(out[row + 1], end);
        }
    }
}

TEST_F(ArrayOffsetsTest, GrowingCopyRowElementStartsClamp) {
    ArrayOffsetsGrowing offsets;

    // No committed rows yet: every entry clamps to 0.
    {
        std::vector<int32_t> out(4, -1);
        offsets.CopyRowElementStarts(0, 3, out.data());
        EXPECT_EQ(out, (std::vector<int32_t>{0, 0, 0, 0}));
    }

    std::vector<int32_t> lens = {2, 3, 0, 1};
    offsets.Insert(0, lens.data(), 4);

    // Fully committed range, including the empty row 2.
    {
        std::vector<int32_t> out(5, -1);
        offsets.CopyRowElementStarts(0, 4, out.data());
        EXPECT_EQ(out, (std::vector<int32_t>{0, 2, 5, 5, 6}));
    }
    // Row 6 arrives out of order and stays PENDING (row 4/5 missing): rows
    // at or beyond the committed count clamp to the committed total, so
    // pending rows read as empty instead of exposing garbage.
    std::vector<int32_t> lens6 = {7};
    offsets.Insert(6, lens6.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 4);
    {
        std::vector<int32_t> out(8, -1);
        offsets.CopyRowElementStarts(0, 7, out.data());
        EXPECT_EQ(out, (std::vector<int32_t>{0, 2, 5, 5, 6, 6, 6, 6}));
    }
    // Range entirely beyond the committed rows.
    {
        std::vector<int32_t> out(3, -1);
        offsets.CopyRowElementStarts(5, 2, out.data());
        EXPECT_EQ(out, (std::vector<int32_t>{6, 6, 6}));
    }
    // Committing the gap drains the pending row and un-clamps it.
    std::vector<int32_t> lens45 = {1, 0};
    offsets.Insert(4, lens45.data(), 2);
    EXPECT_EQ(offsets.GetRowCount(), 7);
    {
        std::vector<int32_t> out(8, -1);
        offsets.CopyRowElementStarts(0, 7, out.data());
        EXPECT_EQ(out, (std::vector<int32_t>{0, 2, 5, 5, 6, 7, 7, 14}));
    }
}

TEST_F(ArrayOffsetsTest, SealedCopyRowElementRanges) {
    // row 0: [0,2), row 1: [2,5), row 2: empty, row 3: [5,6)
    ArrayOffsetsSealed offsets({0, 2, 5, 5, 6});

    // Arbitrary order + duplicates + the end sentinel row (== row_count).
    std::vector<int32_t> rows = {3, 0, 2, 0, 4};
    std::vector<std::pair<int32_t, int32_t>> out(rows.size());
    offsets.CopyRowElementRanges(
        rows.data(), static_cast<int64_t>(rows.size()), out.data());
    EXPECT_EQ(out[0], (std::pair<int32_t, int32_t>{5, 6}));
    EXPECT_EQ(out[1], (std::pair<int32_t, int32_t>{0, 2}));
    EXPECT_EQ(out[2], (std::pair<int32_t, int32_t>{5, 5}));  // empty row
    EXPECT_EQ(out[3], (std::pair<int32_t, int32_t>{0, 2}));
    EXPECT_EQ(out[4], (std::pair<int32_t, int32_t>{6, 6}));  // == row_count
    // Consistency with the per-row method.
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(out[i], offsets.ElementIDRangeOfRow(rows[i]));
    }
}

TEST_F(ArrayOffsetsTest, GrowingCopyRowElementRanges) {
    ArrayOffsetsGrowing offsets;

    // No committed rows: {0, 0} out-of-range insurance.
    {
        std::vector<int32_t> rows = {0};
        std::pair<int32_t, int32_t> out{-1, -1};
        offsets.CopyRowElementRanges(rows.data(), 1, &out);
        EXPECT_EQ(out, (std::pair<int32_t, int32_t>{0, 0}));
    }

    std::vector<int32_t> lens = {2, 0, 3};
    offsets.Insert(0, lens.data(), 3);

    std::vector<int32_t> rows = {2, 1, 0, 3};
    std::vector<std::pair<int32_t, int32_t>> out(rows.size());
    offsets.CopyRowElementRanges(
        rows.data(), static_cast<int64_t>(rows.size()), out.data());
    EXPECT_EQ(out[0], (std::pair<int32_t, int32_t>{2, 5}));
    EXPECT_EQ(out[1], (std::pair<int32_t, int32_t>{2, 2}));  // empty row
    EXPECT_EQ(out[2], (std::pair<int32_t, int32_t>{0, 2}));
    // committed_row_count_ itself reads as {total, total}.
    EXPECT_EQ(out[3], (std::pair<int32_t, int32_t>{5, 5}));
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(out[i], offsets.ElementIDRangeOfRow(rows[i]));
    }

    // Row 5 arrives out of order and stays pending: it is beyond the
    // committed count and reads as {0, 0} (out-of-range insurance) rather
    // than exposing a half-written range.
    std::vector<int32_t> lens5 = {4};
    offsets.Insert(5, lens5.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 3);
    {
        std::vector<int32_t> pending_rows = {5, 4};
        std::vector<std::pair<int32_t, int32_t>> pending_out(2);
        offsets.CopyRowElementRanges(pending_rows.data(), 2, pending_out.data());
        EXPECT_EQ(pending_out[0], (std::pair<int32_t, int32_t>{0, 0}));
        EXPECT_EQ(pending_out[1], (std::pair<int32_t, int32_t>{0, 0}));
    }
}

TEST_F(ArrayOffsetsTest, GrowingEmptyTableRange) {
    ArrayOffsetsGrowing offsets;

    EXPECT_EQ(offsets.GetRowCount(), 0);
    EXPECT_EQ(offsets.GetTotalElementCount(), 0);
    EXPECT_EQ(offsets.ElementIDRangeOfRow(0),
              (std::pair<int32_t, int32_t>{0, 0}));

    TargetBitmap rows(0);
    TargetBitmap valid_rows(0);
    auto [elements, valid_elements] =
        offsets.RowBitsetToElementBitset(rows.view(), valid_rows.view(), 0);
    EXPECT_TRUE(elements.empty());
    EXPECT_TRUE(valid_elements.empty());
}

TEST_F(ArrayOffsetsTest, GrowingChunkBoundary) {
    constexpr int64_t kChunk = ArrayOffsetsGrowing::kEntriesPerChunk;
    constexpr int64_t kTotalRows = 2 * kChunk + 137;

    std::vector<int32_t> lens(kTotalRows);
    std::vector<int32_t> expected(kTotalRows + 1, 0);
    for (int64_t i = 0; i < kTotalRows; ++i) {
        lens[i] = static_cast<int32_t>(i % 4);
        expected[i + 1] = expected[i] + lens[i];
    }

    ArrayOffsetsGrowing offsets;

    // Leave a gap before each chunk boundary, insert the following rows as
    // pending, then fill the gap and drain across the boundary.
    offsets.Insert(0, lens.data(), kChunk - 3);
    offsets.Insert(kChunk + 3, lens.data() + kChunk + 3, 37);
    EXPECT_EQ(offsets.GetRowCount(), kChunk - 3);
    offsets.Insert(kChunk - 3, lens.data() + kChunk - 3, 6);
    EXPECT_EQ(offsets.GetRowCount(), kChunk + 40);

    offsets.Insert(2 * kChunk + 1,
                   lens.data() + 2 * kChunk + 1,
                   kTotalRows - (2 * kChunk + 1));
    EXPECT_EQ(offsets.GetRowCount(), kChunk + 40);
    offsets.Insert(
        kChunk + 40, lens.data() + kChunk + 40, 2 * kChunk + 1 - (kChunk + 40));

    EXPECT_EQ(offsets.GetRowCount(), kTotalRows);
    EXPECT_EQ(offsets.GetTotalElementCount(), expected[kTotalRows]);

    for (int64_t row : {int64_t{0},
                        kChunk - 2,
                        kChunk - 1,
                        kChunk,
                        kChunk + 1,
                        2 * kChunk - 1,
                        2 * kChunk,
                        2 * kChunk + 1,
                        kTotalRows - 1}) {
        auto [start, end] =
            offsets.ElementIDRangeOfRow(static_cast<int32_t>(row));
        EXPECT_EQ(start, expected[row]) << "row " << row;
        EXPECT_EQ(end, expected[row + 1]) << "row " << row;
    }
    EXPECT_EQ(offsets.ElementIDRangeOfRow(kTotalRows),
              (std::pair<int32_t, int32_t>{expected[kTotalRows],
                                           expected[kTotalRows]}));

    auto check_element = [&](int32_t element) {
        auto it = std::upper_bound(expected.begin(), expected.end(), element);
        const auto expected_row =
            static_cast<int32_t>(it - expected.begin()) - 1;
        auto [row, index] = offsets.ElementIDToRowID(element);
        EXPECT_EQ(row, expected_row) << "element " << element;
        EXPECT_EQ(index, element - expected[expected_row])
            << "element " << element;
    };
    for (int32_t element : {0,
                            expected[kChunk] - 1,
                            expected[kChunk],
                            expected[kChunk] + 1,
                            expected[2 * kChunk] - 1,
                            expected[2 * kChunk],
                            expected[kTotalRows] - 1}) {
        check_element(element);
    }

    const int64_t row_start = kChunk - 8;
    const int64_t row_count = 16;
    TargetBitmap row_bits(row_count);
    TargetBitmap valid_bits(row_count, true);
    for (int64_t i = 0; i < row_count; i += 2) {
        row_bits[i] = true;
    }

    auto [element_bits, valid_element_bits] = offsets.RowBitsetToElementBitset(
        row_bits.view(), valid_bits.view(), row_start);
    ASSERT_EQ(static_cast<int64_t>(element_bits.size()),
              expected[row_start + row_count] - expected[row_start]);
    for (int64_t i = 0; i < row_count; ++i) {
        for (int32_t element = expected[row_start + i];
             element < expected[row_start + i + 1];
             ++element) {
            const auto local = element - expected[row_start];
            EXPECT_EQ(bool(element_bits[local]), bool(row_bits[i]));
            EXPECT_TRUE(valid_element_bits[local]);
        }
    }

    std::vector<int32_t> copied_starts(row_count + 1, -1);
    offsets.CopyRowElementStarts(row_start, row_count, copied_starts.data());
    for (int64_t i = 0; i <= row_count; ++i) {
        EXPECT_EQ(copied_starts[i], expected[row_start + i]);
    }

    TargetBitmap any_rows(row_count);
    offsets.ElementBitsetToRowBitsetAny(
        element_bits.view(), expected[row_start], row_start, any_rows.view());
    for (int64_t i = 0; i < row_count; ++i) {
        EXPECT_EQ(bool(any_rows[i]),
                  bool(row_bits[i]) && lens[row_start + i] > 0);
    }

    auto element_offsets =
        offsets.RowBitsetToElementOffsets(row_bits.view(), row_start);
    std::vector<int32_t> expected_offsets;
    for (int64_t i = 0; i < row_count; i += 2) {
        for (int32_t element = expected[row_start + i];
             element < expected[row_start + i + 1];
             ++element) {
            expected_offsets.push_back(element);
        }
    }
    EXPECT_TRUE(std::equal(element_offsets.begin(),
                           element_offsets.end(),
                           expected_offsets.begin(),
                           expected_offsets.end()));

    FixedVector<int32_t> row_offsets = {static_cast<int32_t>(kChunk - 1),
                                        static_cast<int32_t>(kChunk),
                                        static_cast<int32_t>(kChunk + 1),
                                        static_cast<int32_t>(2 * kChunk)};
    std::vector<std::pair<int32_t, int32_t>> copied_ranges(row_offsets.size());
    offsets.CopyRowElementRanges(row_offsets.data(),
                                 static_cast<int64_t>(row_offsets.size()),
                                 copied_ranges.data());
    for (size_t i = 0; i < row_offsets.size(); ++i) {
        const int32_t row = row_offsets[i];
        EXPECT_EQ(
            copied_ranges[i],
            (std::pair<int32_t, int32_t>{expected[row], expected[row + 1]}));
    }

    auto selected = offsets.RowOffsetsToElementOffsets(row_offsets);
    expected_offsets.clear();
    for (auto row : row_offsets) {
        for (int32_t element = expected[row]; element < expected[row + 1];
             ++element) {
            expected_offsets.push_back(element);
        }
    }
    EXPECT_TRUE(std::equal(selected.begin(),
                           selected.end(),
                           expected_offsets.begin(),
                           expected_offsets.end()));

    auto ranges = offsets.ForEachRowElementRange(
        [](int32_t start, int32_t end) { return end - start >= 2; },
        row_start,
        row_count);
    for (int64_t i = 0; i < row_count; ++i) {
        EXPECT_EQ(bool(ranges[i]), lens[row_start + i] >= 2);
    }
}

TEST_F(ArrayOffsetsTest, GrowingConcurrentWriterReaderStress) {
    constexpr int64_t kTotalRows = 100000;
    constexpr int64_t kInsertBatch = 32;
    static_assert(kTotalRows % kInsertBatch == 0);

    std::vector<int32_t> lens(kTotalRows);
    std::vector<int32_t> expected(kTotalRows + 1, 0);
    std::mt19937 gen(4242);
    for (int64_t i = 0; i < kTotalRows; ++i) {
        lens[i] = static_cast<int32_t>(gen() % 5);
        expected[i + 1] = expected[i] + lens[i];
    }

    // Precompute the exact row-count endpoints that the deterministic writer
    // plan may publish. For an out-of-order pair, the high batch leaves the
    // watermark unchanged and the following low batch publishes both batches
    // together; the intermediate prefix is deliberately not an endpoint.
    std::vector<bool> allowed_counts(kTotalRows + 1, false);
    allowed_counts[0] = true;
    std::mt19937 plan_gen(999);
    int64_t planned_next = 0;
    while (planned_next < kTotalRows) {
        if (plan_gen() % 4 == 0 &&
            planned_next + 2 * kInsertBatch <= kTotalRows) {
            planned_next += 2 * kInsertBatch;
        } else {
            planned_next += kInsertBatch;
        }
        allowed_counts[planned_next] = true;
    }

    ArrayOffsetsGrowing offsets;
    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::atomic<int32_t> ready{0};
    std::atomic<int64_t> live_reads{0};
    std::atomic<int64_t> partial_reads{0};

    std::thread writer([&]() {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (live_reads.load(std::memory_order_acquire) == 0) {
            std::this_thread::yield();
        }

        std::mt19937 writer_gen(999);
        int64_t next = 0;
        while (next < kTotalRows) {
            if (writer_gen() % 4 == 0 &&
                next + 2 * kInsertBatch <= kTotalRows) {
                offsets.Insert(next + kInsertBatch,
                               lens.data() + next + kInsertBatch,
                               kInsertBatch);
                offsets.Insert(next, lens.data() + next, kInsertBatch);
                next += 2 * kInsertBatch;
            } else {
                offsets.Insert(next, lens.data() + next, kInsertBatch);
                next += kInsertBatch;
            }

            // Keep the first published prefix visible until a reader has
            // observed it, proving the stress actually overlaps ingest.
            if (partial_reads.load(std::memory_order_acquire) == 0) {
                while (partial_reads.load(std::memory_order_acquire) == 0) {
                    std::this_thread::yield();
                }
            }
            std::this_thread::yield();
        }
        done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    for (int thread_id = 0; thread_id < 4; ++thread_id) {
        readers.emplace_back([&, thread_id]() {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            std::mt19937 reader_gen(1000 + thread_id);
            int64_t last_count = 0;
            int64_t iterations = 0;
            while (!done.load(std::memory_order_acquire) || iterations < 200) {
                ++iterations;
                if (!done.load(std::memory_order_acquire)) {
                    live_reads.fetch_add(1, std::memory_order_release);
                }
                const int64_t count = offsets.GetRowCount();
                EXPECT_GE(count, last_count);
                if (count < 0 || count > kTotalRows) {
                    ADD_FAILURE() << "invalid committed row count " << count;
                    continue;
                }
                EXPECT_TRUE(allowed_counts[count])
                    << "reader observed a count that was not an Insert-call "
                       "endpoint: "
                    << count;
                last_count = count;
                if (count > 0 && count < kTotalRows) {
                    partial_reads.fetch_add(1, std::memory_order_release);
                }

                const int64_t total = offsets.GetTotalElementCount();
                if (total < expected[count] || total > expected[kTotalRows]) {
                    ADD_FAILURE() << "invalid total element count " << total
                                  << " for committed rows " << count;
                    continue;
                }
                if (count == 0) {
                    continue;
                }

                const int64_t row_start = reader_gen() % count;
                const int64_t row_count =
                    std::min<int64_t>(128, count - row_start);

                auto [first_start, first_end] =
                    offsets.ElementIDRangeOfRow(row_start);
                EXPECT_EQ(first_start, expected[row_start]);
                EXPECT_EQ(first_end, expected[row_start + 1]);
                auto [last_start, last_end] =
                    offsets.ElementIDRangeOfRow(row_start + row_count - 1);
                EXPECT_EQ(last_start, expected[row_start + row_count - 1]);
                EXPECT_EQ(last_end, expected[row_start + row_count]);

                int32_t copied_starts[129];
                offsets.CopyRowElementStarts(
                    row_start, row_count, copied_starts);
                for (int64_t i = 0; i <= row_count; ++i) {
                    EXPECT_EQ(copied_starts[i], expected[row_start + i]);
                }

                int32_t copied_rows[2] = {
                    static_cast<int32_t>(row_start),
                    static_cast<int32_t>(row_start + row_count - 1)};
                std::pair<int32_t, int32_t> copied_ranges[2];
                offsets.CopyRowElementRanges(copied_rows, 2, copied_ranges);
                EXPECT_EQ(copied_ranges[0],
                          (std::pair<int32_t, int32_t>{
                              expected[row_start], expected[row_start + 1]}));
                EXPECT_EQ(copied_ranges[1],
                          (std::pair<int32_t, int32_t>{
                              expected[row_start + row_count - 1],
                              expected[row_start + row_count]}));

                if (total > 0) {
                    const auto element =
                        static_cast<int32_t>(reader_gen() % total);
                    auto it = std::upper_bound(
                        expected.begin(), expected.end(), element);
                    const auto expected_row =
                        static_cast<int32_t>(it - expected.begin()) - 1;
                    auto [row, index] = offsets.ElementIDToRowID(element);
                    EXPECT_EQ(row, expected_row);
                    EXPECT_EQ(index, element - expected[expected_row]);
                }

                TargetBitmap row_bits(row_count);
                TargetBitmap valid_bits(row_count, true);
                for (int64_t i = 0; i < row_count; i += 3) {
                    row_bits[i] = true;
                }
                auto [element_bits, valid_element_bits] =
                    offsets.RowBitsetToElementBitset(
                        row_bits.view(), valid_bits.view(), row_start);
                EXPECT_EQ(
                    static_cast<int64_t>(element_bits.size()),
                    expected[row_start + row_count] - expected[row_start]);
                EXPECT_EQ(element_bits.size(), valid_element_bits.count());
                for (int64_t i = 0; i < row_count; ++i) {
                    for (int32_t element = expected[row_start + i];
                         element < expected[row_start + i + 1];
                         ++element) {
                        EXPECT_EQ(
                            bool(element_bits[element - expected[row_start]]),
                            bool(row_bits[i]));
                    }
                }

                TargetBitmap any_rows(row_count);
                offsets.ElementBitsetToRowBitsetAny(element_bits.view(),
                                                    expected[row_start],
                                                    row_start,
                                                    any_rows.view());
                for (int64_t i = 0; i < row_count; ++i) {
                    EXPECT_EQ(bool(any_rows[i]),
                              bool(row_bits[i]) && lens[row_start + i] > 0);
                }

                auto selected = offsets.RowBitsetToElementOffsets(
                    row_bits.view(), row_start);
                int64_t expected_selected = 0;
                std::vector<int32_t> expected_selected_offsets;
                for (int64_t i = 0; i < row_count; i += 3) {
                    expected_selected += lens[row_start + i];
                    for (int32_t element = expected[row_start + i];
                         element < expected[row_start + i + 1];
                         ++element) {
                        expected_selected_offsets.push_back(element);
                    }
                }
                EXPECT_EQ(static_cast<int64_t>(selected.size()),
                          expected_selected);
                EXPECT_TRUE(std::equal(selected.begin(),
                                       selected.end(),
                                       expected_selected_offsets.begin(),
                                       expected_selected_offsets.end()));

                FixedVector<int32_t> rows = {static_cast<int32_t>(row_start)};
                if (row_count > 1) {
                    rows.push_back(
                        static_cast<int32_t>(row_start + row_count - 1));
                }
                auto row_elements = offsets.RowOffsetsToElementOffsets(rows);
                std::vector<int32_t> expected_row_elements;
                for (auto row : rows) {
                    for (int32_t element = expected[row];
                         element < expected[row + 1];
                         ++element) {
                        expected_row_elements.push_back(element);
                    }
                }
                EXPECT_TRUE(std::equal(row_elements.begin(),
                                       row_elements.end(),
                                       expected_row_elements.begin(),
                                       expected_row_elements.end()));

                auto ranges = offsets.ForEachRowElementRange(
                    [](int32_t begin, int32_t end) {
                        return (end - begin) % 2 == 0;
                    },
                    row_start,
                    row_count);
                for (int64_t i = 0; i < row_count; ++i) {
                    EXPECT_EQ(bool(ranges[i]), lens[row_start + i] % 2 == 0);
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != 5) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }

    EXPECT_EQ(offsets.GetRowCount(), kTotalRows);
    EXPECT_EQ(offsets.GetTotalElementCount(), expected[kTotalRows]);
    EXPECT_GT(live_reads.load(std::memory_order_relaxed), 0);
    EXPECT_GT(partial_reads.load(std::memory_order_relaxed), 0);
    for (int64_t row = 0; row < kTotalRows; ++row) {
        auto [begin, end] = offsets.ElementIDRangeOfRow(row);
        ASSERT_EQ(begin, expected[row]) << "row " << row;
        ASSERT_EQ(end, expected[row + 1]) << "row " << row;
    }
}

TEST_F(ArrayOffsetsTest, GrowingConcurrentWriters) {
    constexpr int64_t kTotalRows = 20000;
    constexpr int64_t kBatchSize = 25;
    constexpr int64_t kBatchCount = kTotalRows / kBatchSize;
    constexpr int32_t kWriterCount = 4;

    std::vector<int32_t> lens(kTotalRows);
    std::vector<int32_t> expected(kTotalRows + 1, 0);
    for (int64_t row = 0; row < kTotalRows; ++row) {
        lens[row] = static_cast<int32_t>(row % 5);
        expected[row + 1] = expected[row] + lens[row];
    }

    std::vector<int64_t> batches(kBatchCount);
    for (int64_t batch = 0; batch < kBatchCount; ++batch) {
        batches[batch] = batch;
    }
    std::mt19937 gen(2026);
    std::shuffle(batches.begin(), batches.end(), gen);

    ArrayOffsetsGrowing offsets;
    std::atomic<int64_t> next_batch{0};
    std::atomic<int32_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> writers;
    for (int32_t writer_id = 0; writer_id < kWriterCount; ++writer_id) {
        writers.emplace_back([&]() {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            while (true) {
                const int64_t index =
                    next_batch.fetch_add(1, std::memory_order_relaxed);
                if (index >= kBatchCount) {
                    break;
                }
                const int64_t batch = batches[index];
                const int64_t row_start = batch * kBatchSize;
                offsets.Insert(row_start, lens.data() + row_start, kBatchSize);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != kWriterCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& writer : writers) {
        writer.join();
    }

    EXPECT_EQ(offsets.GetRowCount(), kTotalRows);
    EXPECT_EQ(offsets.GetTotalElementCount(), expected[kTotalRows]);
    for (int64_t row = 0; row < kTotalRows; ++row) {
        auto [begin, end] = offsets.ElementIDRangeOfRow(row);
        ASSERT_EQ(begin, expected[row]) << "row " << row;
        ASSERT_EQ(end, expected[row + 1]) << "row " << row;
    }
}
