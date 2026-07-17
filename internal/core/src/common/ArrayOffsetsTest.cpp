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

TEST_F(ArrayOffsetsTest, SealedAllNullArrays) {
    constexpr int64_t row_count = 4;
    auto offsets = ArrayOffsetsSealed::BuildAllNulls(row_count);

    ASSERT_NE(offsets, nullptr);
    EXPECT_EQ(offsets->GetRowCount(), row_count);
    EXPECT_EQ(offsets->GetTotalElementCount(), 0);
    for (int64_t row = 0; row <= row_count; ++row) {
        auto [start, end] = offsets->ElementIDRangeOfRow(row);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 0);
    }

    milvus::TargetBitmap probe(row_count, true);
    offsets->AndRowValidBitmap(probe.view(), 0, row_count);
    EXPECT_TRUE(probe.none());
}

TEST_F(ArrayOffsetsTest, GrowingRowValidity) {
    ArrayOffsetsGrowing offsets;

    // -1 marks a NULL row: zero elements + row-invalid; 0 is a real empty
    // array and stays valid.
    std::vector<int32_t> lens = {2, -1, 0, 3};
    offsets.Insert(0, lens.data(), 4);

    EXPECT_EQ(offsets.GetRowCount(), 4);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);
    // NULL row and empty row both have zero-length element ranges...
    auto [null_start, null_end] = offsets.ElementIDRangeOfRow(1);
    EXPECT_EQ(null_start, null_end);
    auto [empty_start, empty_end] = offsets.ElementIDRangeOfRow(2);
    EXPECT_EQ(empty_start, empty_end);

    // ...but only the NULL row is reported invalid.
    milvus::TargetBitmap probe(4, true);
    offsets.AndRowValidBitmap(probe.view(), 0, 4);
    EXPECT_TRUE(probe[0]);
    EXPECT_FALSE(probe[1]);
    EXPECT_TRUE(probe[2]);
    EXPECT_TRUE(probe[3]);

    // Out-of-order insert keeps validity lockstep: row 5 arrives (pending),
    // then row 4 (NULL) commits both.
    std::vector<int32_t> lens5 = {1};
    offsets.Insert(5, lens5.data(), 1);
    std::vector<int32_t> lens4 = {-1};
    offsets.Insert(4, lens4.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 6);
    milvus::TargetBitmap probe2(6, true);
    offsets.AndRowValidBitmap(probe2.view(), 0, 6);
    EXPECT_FALSE(probe2[4]);
    EXPECT_TRUE(probe2[5]);

    // row_start slicing.
    milvus::TargetBitmap probe3(2, true);
    offsets.AndRowValidBitmap(probe3.view(), 4, 2);
    EXPECT_FALSE(probe3[0]);
    EXPECT_TRUE(probe3[1]);
}

TEST_F(ArrayOffsetsTest, GrowingInsertNullsBackfill) {
    ArrayOffsetsGrowing offsets;

    // Schema-evolution backfill: historical rows are NULL, not empty arrays
    // (mirrors ArrayOffsetsSealed::BuildAllNulls).
    offsets.InsertNulls(0, 3);
    EXPECT_EQ(offsets.GetRowCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 0);

    milvus::TargetBitmap probe(3, true);
    offsets.AndRowValidBitmap(probe.view(), 0, 3);
    EXPECT_TRUE(probe.none());

    // Rows inserted after the evolution behave normally.
    std::vector<int32_t> lens = {2};
    offsets.Insert(3, lens.data(), 1);
    milvus::TargetBitmap probe2(4, true);
    offsets.AndRowValidBitmap(probe2.view(), 0, 4);
    EXPECT_FALSE(probe2[0]);
    EXPECT_FALSE(probe2[1]);
    EXPECT_FALSE(probe2[2]);
    EXPECT_TRUE(probe2[3]);
}

TEST_F(ArrayOffsetsTest, GrowingAllValidIsNoop) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {1, 0, 2};
    offsets.Insert(0, lens.data(), 3);

    // No NULL row was ever inserted: validity is not materialized and the
    // apply is a no-op.
    milvus::TargetBitmap probe(3, true);
    offsets.AndRowValidBitmap(probe.view(), 0, 3);
    EXPECT_TRUE(probe.all());
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

    // Fully committed range (fast copy path), includes the empty row 2.
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

    // No committed rows: {0, 0} insurance (same as ElementIDRangeOfRow).
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
        offsets.CopyRowElementRanges(
            pending_rows.data(), 2, pending_out.data());
        EXPECT_EQ(pending_out[0], (std::pair<int32_t, int32_t>{0, 0}));
        EXPECT_EQ(pending_out[1], (std::pair<int32_t, int32_t>{0, 0}));
    }
}

// ==== Chunked growing storage: boundary + concurrency coverage ====

// Rows, batches and validity materialization straddling the fixed chunk
// size K (and 2K): the growing implementation stores starts/validity in
// K-entry chunks, so every logical index computation, memcpy run, pending
// drain and validity backfill has an edge exactly at multiples of K.
TEST_F(ArrayOffsetsTest, GrowingChunkBoundary) {
    constexpr int64_t K = ArrayOffsetsGrowing::kEntriesPerChunk;
    constexpr int64_t kTotalRows = 2 * K + 137;

    // Lengths mix empty rows and small rows. The first NULL row is K + 123
    // (mid-chunk 1), so validity materialization backfills all of chunk 0
    // plus a partial chunk 1 (mid-chunk trigger + cross-chunk backfill).
    std::vector<int32_t> lens(kTotalRows);
    for (int64_t i = 0; i < kTotalRows; ++i) {
        lens[i] = static_cast<int32_t>(i % 4);  // 0..3, includes empties
    }
    for (int64_t i = K + 123; i < kTotalRows; ++i) {
        if (i % 7 == 0) {
            lens[i] = -1;
        }
    }
    lens[K + 123] = -1;

    // Reference starts table.
    std::vector<int32_t> exp(kTotalRows + 1, 0);
    for (int64_t i = 0; i < kTotalRows; ++i) {
        exp[i + 1] = exp[i] + std::max(lens[i], 0);
    }

    ArrayOffsetsGrowing offsets;
    // Out-of-order commit around the first chunk boundary: rows
    // [K+3, K+40) arrive while [K-3, K+3) is still missing, then the gap
    // batch drains straight across K.
    offsets.Insert(0, lens.data(), K - 3);
    EXPECT_EQ(offsets.GetRowCount(), K - 3);
    offsets.Insert(K + 3, lens.data() + K + 3, 37);
    EXPECT_EQ(offsets.GetRowCount(), K - 3);  // pending, gap at K-3
    offsets.Insert(K - 3, lens.data() + K - 3, 6);
    EXPECT_EQ(offsets.GetRowCount(), K + 40);

    // Same around the 2K boundary: the tail goes pending first, then one
    // batch commits across 2K and drains it.
    offsets.Insert(
        2 * K + 1, lens.data() + 2 * K + 1, kTotalRows - (2 * K + 1));
    EXPECT_EQ(offsets.GetRowCount(), K + 40);
    offsets.Insert(K + 40, lens.data() + K + 40, 2 * K + 1 - (K + 40));
    EXPECT_EQ(offsets.GetRowCount(), kTotalRows);
    EXPECT_EQ(offsets.GetTotalElementCount(), exp[kTotalRows]);

    // Per-row ranges at and around both chunk boundaries.
    for (int64_t row : {int64_t{0},
                        K - 2,
                        K - 1,
                        K,
                        K + 1,
                        2 * K - 1,
                        2 * K,
                        2 * K + 1,
                        kTotalRows - 1}) {
        auto [start, end] =
            offsets.ElementIDRangeOfRow(static_cast<int32_t>(row));
        EXPECT_EQ(start, exp[row]) << "row " << row;
        EXPECT_EQ(end, exp[row + 1]) << "row " << row;
    }
    {
        auto [start, end] =
            offsets.ElementIDRangeOfRow(static_cast<int32_t>(kTotalRows));
        EXPECT_EQ(start, exp[kTotalRows]);
        EXPECT_EQ(end, exp[kTotalRows]);
    }

    // ElementIDToRowID (binary search over chunked entries) against an
    // upper_bound on the reference table, probing element ids that sit at
    // the chunk-boundary rows' starts.
    auto check_elem_to_row = [&](int32_t e) {
        auto it = std::upper_bound(exp.begin(), exp.end(), e);
        const auto want_row = static_cast<int32_t>(it - exp.begin()) - 1;
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(e);
        EXPECT_EQ(row_id, want_row) << "elem " << e;
        EXPECT_EQ(elem_idx, e - exp[want_row]) << "elem " << e;
    };
    for (int32_t e : {0,
                      exp[K] - 1,
                      exp[K],
                      exp[K] + 1,
                      exp[2 * K] - 1,
                      exp[2 * K],
                      exp[kTotalRows] - 1}) {
        check_elem_to_row(e);
    }

    // Chunk-run memcpy path across the K boundary.
    {
        std::vector<int32_t> out(201, -1);
        offsets.CopyRowElementStarts(K - 100, 200, out.data());
        for (int64_t i = 0; i <= 200; ++i) {
            ASSERT_EQ(out[i], exp[K - 100 + i]) << "entry " << i;
        }
    }
    // Full-table copy (spans all three chunks).
    {
        std::vector<int32_t> out(kTotalRows + 1, -1);
        offsets.CopyRowElementStarts(0, kTotalRows, out.data());
        EXPECT_TRUE(std::equal(out.begin(), out.end(), exp.begin()));
    }

    // Batched ranges spanning both boundaries + out-of-range insurance.
    {
        std::vector<int32_t> rows = {static_cast<int32_t>(K - 1),
                                     static_cast<int32_t>(K),
                                     static_cast<int32_t>(2 * K),
                                     static_cast<int32_t>(kTotalRows),
                                     static_cast<int32_t>(kTotalRows + 5),
                                     -1};
        std::vector<std::pair<int32_t, int32_t>> out(rows.size());
        offsets.CopyRowElementRanges(
            rows.data(), static_cast<int64_t>(rows.size()), out.data());
        EXPECT_EQ(out[0], (std::pair<int32_t, int32_t>{exp[K - 1], exp[K]}));
        EXPECT_EQ(out[1], (std::pair<int32_t, int32_t>{exp[K], exp[K + 1]}));
        EXPECT_EQ(out[2],
                  (std::pair<int32_t, int32_t>{exp[2 * K], exp[2 * K + 1]}));
        EXPECT_EQ(
            out[3],
            (std::pair<int32_t, int32_t>{exp[kTotalRows], exp[kTotalRows]}));
        EXPECT_EQ(out[4], (std::pair<int32_t, int32_t>{0, 0}));
        EXPECT_EQ(out[5], (std::pair<int32_t, int32_t>{0, 0}));
    }

    // Validity was materialized mid-chunk-1; the backfill covered chunk 0
    // entirely and part of chunk 1.
    {
        milvus::TargetBitmap probe(kTotalRows, true);
        offsets.AndRowValidBitmap(probe.view(), 0, kTotalRows);
        for (int64_t i = 0; i < kTotalRows; ++i) {
            ASSERT_EQ(bool(probe[i]), lens[i] >= 0) << "row " << i;
        }
    }
    // Windowed validity across the 2K boundary.
    {
        milvus::TargetBitmap probe(200, true);
        offsets.AndRowValidBitmap(probe.view(), 2 * K - 100, 200);
        for (int64_t i = 0; i < 200; ++i) {
            ASSERT_EQ(bool(probe[i]), lens[2 * K - 100 + i] >= 0)
                << "row " << (2 * K - 100 + i);
        }
    }

    // Word-wise ANY reduction across the K boundary (per-bit reference).
    {
        const int64_t row_start = K - 50;
        const int64_t row_count = 150;
        const int64_t elem_lo = exp[row_start];
        const int64_t elem_hi = exp[row_start + row_count];
        TargetBitmap local(elem_hi - elem_lo);
        for (int64_t e = elem_lo; e < elem_hi; ++e) {
            if (e % 3 == 0) {
                local[e - elem_lo] = true;
            }
        }
        CheckAnyReduce(offsets, local, elem_lo, row_start, row_count);
    }

    // Row->element bitset expansion across the K boundary.
    {
        const int64_t row_start = K - 8;
        const int64_t row_count = 16;
        TargetBitmap row_bits(row_count);
        for (int64_t i = 0; i < row_count; i += 2) {
            row_bits[i] = true;
        }
        TargetBitmap valid_bits(row_count, true);
        auto [elem_bits, valid_elem_bits] = offsets.RowBitsetToElementBitset(
            row_bits.view(), valid_bits.view(), row_start);
        ASSERT_EQ(static_cast<int64_t>(elem_bits.size()),
                  exp[row_start + row_count] - exp[row_start]);
        for (int64_t i = 0; i < row_count; ++i) {
            for (int64_t e = exp[row_start + i]; e < exp[row_start + i + 1];
                 ++e) {
                ASSERT_EQ(bool(elem_bits[e - exp[row_start]]),
                          bool(row_bits[i]))
                    << "row " << (row_start + i) << " elem " << e;
            }
        }
    }
}

// One writer inserting many small batches (in-order, out-of-order and
// -1 NULL rows) while four readers continuously hit the committed prefix.
// The committed prefix is deterministic (a single writer commits rows in
// order), so every read is checked EXACTLY against a reference table --
// stronger than just monotonicity -- and the final state is verified
// entry for entry after the join.
TEST_F(ArrayOffsetsTest, GrowingConcurrentWriterReaderStress) {
    constexpr int64_t kTotalRows = 100000;
    // No NULLs in the prefix, so readers exercise both the
    // pre-materialization phase and the live has_row_valid_ flip (which
    // lands mid-chunk and backfills across multiple chunks).
    constexpr int64_t kNullFreeRows = 20000;

    std::vector<int32_t> lens(kTotalRows);
    std::mt19937 gen(4242);
    for (int64_t i = 0; i < kTotalRows; ++i) {
        const auto p = gen() % 8;
        if (p == 7 && i >= kNullFreeRows) {
            lens[i] = -1;  // NULL row
        } else {
            lens[i] = static_cast<int32_t>(p % 5);  // 0..4 elements
        }
    }
    lens[kNullFreeRows] = -1;  // deterministic materialization point

    std::vector<int32_t> exp(kTotalRows + 1, 0);
    for (int64_t i = 0; i < kTotalRows; ++i) {
        exp[i + 1] = exp[i] + std::max(lens[i], 0);
    }

    ArrayOffsetsGrowing offsets;
    std::atomic<bool> done{false};

    std::thread writer([&]() {
        std::mt19937 wgen(999);
        int64_t next = 0;
        while (next < kTotalRows) {
            int64_t b1 = 1 + wgen() % 40;
            b1 = std::min(b1, kTotalRows - next);
            if (wgen() % 4 == 0 && next + b1 < kTotalRows) {
                int64_t b2 = 1 + wgen() % 40;
                b2 = std::min(b2, kTotalRows - next - b1);
                // Out of order: the lookahead batch goes pending...
                offsets.Insert(next + b1, lens.data() + next + b1, b2);
                // ...until the gap batch commits and drains it.
                offsets.Insert(next, lens.data() + next, b1);
                next += b1 + b2;
            } else {
                offsets.Insert(next, lens.data() + next, b1);
                next += b1;
            }
        }
        done.store(true);
    });

    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&, t]() {
            std::mt19937 rgen(1000 + t);
            int64_t last_count = 0;
            int64_t iters = 0;
            // Keep reading until the writer finishes, with a minimum
            // iteration count so the full committed table is also swept.
            while (!done.load() || iters < 200) {
                ++iters;
                const int64_t count = offsets.GetRowCount();
                EXPECT_GE(count, last_count);  // watermark is monotone
                EXPECT_LE(count, kTotalRows);
                last_count = count;

                const int64_t total = offsets.GetTotalElementCount();
                // total == exp[W'] for some W' >= count (monotone
                // watermark, loaded after count).
                EXPECT_GE(total, exp[count]);
                EXPECT_LE(total, exp[kTotalRows]);

                if (count == 0) {
                    continue;
                }

                const int64_t row_start = rgen() % count;
                const int64_t row_cnt =
                    std::min<int64_t>(512, count - row_start);

                // Starts of committed rows match the reference exactly
                // (also proves monotone non-decreasing within the read).
                std::vector<int32_t> buf(row_cnt + 1, -1);
                offsets.CopyRowElementStarts(row_start, row_cnt, buf.data());
                for (int64_t i = 0; i <= row_cnt; ++i) {
                    EXPECT_EQ(buf[i], exp[row_start + i]);
                }

                // ElementIDToRowID against the reference.
                if (total > 0) {
                    const auto e = static_cast<int32_t>(rgen() % total);
                    auto it = std::upper_bound(exp.begin(), exp.end(), e);
                    const auto want_row =
                        static_cast<int32_t>(it - exp.begin()) - 1;
                    auto [row_id, elem_idx] = offsets.ElementIDToRowID(e);
                    EXPECT_EQ(row_id, want_row);
                    EXPECT_EQ(elem_idx, e - exp[want_row]);
                }

                // Row validity of the committed window is exact: having
                // acquired `count`, a NULL row < count implies the
                // materialization flag is visible.
                {
                    milvus::TargetBitmap probe(row_cnt, true);
                    offsets.AndRowValidBitmap(probe.view(), row_start, row_cnt);
                    for (int64_t i = 0; i < row_cnt; ++i) {
                        EXPECT_EQ(bool(probe[i]), lens[row_start + i] >= 0);
                    }
                }

                // ANY-reduce over the window matches a per-row recompute.
                {
                    const int64_t elem_lo = exp[row_start];
                    const int64_t elem_hi = exp[row_start + row_cnt];
                    TargetBitmap local(elem_hi - elem_lo);
                    for (int64_t e = elem_lo; e < elem_hi; ++e) {
                        if (e % 3 == 0) {
                            local[e - elem_lo] = true;
                        }
                    }
                    TargetBitmap row_result(row_cnt);
                    offsets.ElementBitsetToRowBitsetAny(
                        local.view(), elem_lo, row_start, row_result.view());
                    for (int64_t i = 0; i < row_cnt; ++i) {
                        bool want = false;
                        for (int64_t e = exp[row_start + i];
                             e < exp[row_start + i + 1] && !want;
                             ++e) {
                            want = (e % 3 == 0);
                        }
                        EXPECT_EQ(bool(row_result[i]), want);
                    }
                }
            }
        });
    }

    writer.join();
    for (auto& r : readers) {
        r.join();
    }

    // Final state must be exact.
    EXPECT_EQ(offsets.GetRowCount(), kTotalRows);
    EXPECT_EQ(offsets.GetTotalElementCount(), exp[kTotalRows]);
    std::vector<int32_t> all(kTotalRows + 1, -1);
    offsets.CopyRowElementStarts(0, kTotalRows, all.data());
    EXPECT_TRUE(std::equal(all.begin(), all.end(), exp.begin()));
    milvus::TargetBitmap probe(kTotalRows, true);
    offsets.AndRowValidBitmap(probe.view(), 0, kTotalRows);
    for (int64_t i = 0; i < kTotalRows; ++i) {
        ASSERT_EQ(bool(probe[i]), lens[i] >= 0) << "row " << i;
    }
    TargetBitmap elem_bitset(exp[kTotalRows]);
    std::mt19937 fgen(777);
    for (int64_t e = 0; e < exp[kTotalRows]; ++e) {
        if (fgen() % 100 == 0) {
            elem_bitset[e] = true;
        }
    }
    CheckAnyReduce(offsets, elem_bitset, 0, 0, kTotalRows);
}
