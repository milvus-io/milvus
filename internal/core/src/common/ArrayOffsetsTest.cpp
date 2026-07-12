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
