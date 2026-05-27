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

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/ArrayOffsets.h"

using namespace milvus;

TEST(ArrayOffsetsSealed, Basic) {
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

    EXPECT_EQ(offsets.GetRowCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

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

    EXPECT_EQ(offsets.ElementIDRangeOfRow(0), std::make_pair(0, 2));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(1), std::make_pair(2, 5));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(2), std::make_pair(5, 6));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(3), std::make_pair(6, 6));
}

TEST(ArrayOffsetsSealed, EmptyArrays) {
    ArrayOffsetsSealed offsets({0, 2, 2, 5, 5});

    EXPECT_EQ(offsets.GetRowCount(), 4);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);
    EXPECT_EQ(offsets.ElementIDRangeOfRow(1), std::make_pair(2, 2));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(3), std::make_pair(5, 5));

    auto [row_id, elem_idx] = offsets.ElementIDToRowID(2);
    EXPECT_EQ(row_id, 2);
    EXPECT_EQ(elem_idx, 0);
}

TEST(ArrayOffsetsSealed, RowBitsetToElementBitset) {
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

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

    EXPECT_EQ(valid_elem_bitset.size(), 6);
    EXPECT_TRUE(valid_elem_bitset.all());
}

TEST(ArrayOffsetsGrowing, BasicInsert) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens1 = {2};
    offsets.Insert(0, lens1.data(), 1);

    std::vector<int32_t> lens2 = {3};
    offsets.Insert(1, lens2.data(), 1);

    std::vector<int32_t> lens3 = {1};
    offsets.Insert(2, lens3.data(), 1);

    EXPECT_EQ(offsets.GetRowCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);

    EXPECT_EQ(offsets.ElementIDRangeOfRow(0), std::make_pair(0, 2));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(1), std::make_pair(2, 5));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(2), std::make_pair(5, 6));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(3), std::make_pair(6, 6));

    {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(5);
        EXPECT_EQ(row_id, 2);
        EXPECT_EQ(elem_idx, 0);
    }
}

TEST(ArrayOffsetsGrowing, BatchInsertAndEmptyArrays) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {2, 0, 3, 0};
    offsets.Insert(0, lens.data(), 4);

    EXPECT_EQ(offsets.GetRowCount(), 4);
    EXPECT_EQ(offsets.GetTotalElementCount(), 5);
    EXPECT_EQ(offsets.ElementIDRangeOfRow(0), std::make_pair(0, 2));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(1), std::make_pair(2, 2));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(2), std::make_pair(2, 5));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(3), std::make_pair(5, 5));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(4), std::make_pair(5, 5));

    auto [row_id, elem_idx] = offsets.ElementIDToRowID(4);
    EXPECT_EQ(row_id, 2);
    EXPECT_EQ(elem_idx, 2);
}

TEST(ArrayOffsetsGrowing, OutOfOrderInsert) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens0 = {2};
    offsets.Insert(0, lens0.data(), 1);

    std::vector<int32_t> lens2 = {1};
    offsets.Insert(2, lens2.data(), 1);

    EXPECT_EQ(offsets.GetRowCount(), 1);
    EXPECT_EQ(offsets.GetTotalElementCount(), 2);

    std::vector<int32_t> lens1 = {3};
    offsets.Insert(1, lens1.data(), 1);

    EXPECT_EQ(offsets.GetRowCount(), 3);
    EXPECT_EQ(offsets.GetTotalElementCount(), 6);
    EXPECT_EQ(offsets.ElementIDRangeOfRow(0), std::make_pair(0, 2));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(1), std::make_pair(2, 5));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(2), std::make_pair(5, 6));
}

TEST(ArrayOffsetsGrowing, PurePendingThenDrain) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens1 = {3, 2, 4};
    offsets.Insert(2, lens1.data(), 3);

    EXPECT_EQ(offsets.GetRowCount(), 0);
    EXPECT_EQ(offsets.GetTotalElementCount(), 0);

    std::vector<int32_t> lens2 = {2, 3};
    offsets.Insert(0, lens2.data(), 2);

    EXPECT_EQ(offsets.GetRowCount(), 5);
    EXPECT_EQ(offsets.GetTotalElementCount(), 14);

    std::vector<std::pair<int32_t, int32_t>> expected = {
        {0, 0},
        {0, 1},
        {1, 0},
        {1, 1},
        {1, 2},
        {2, 0},
        {2, 1},
        {2, 2},
        {3, 0},
        {3, 1},
        {4, 0},
        {4, 1},
        {4, 2},
        {4, 3},
    };

    for (int32_t elem_id = 0; elem_id < 14; ++elem_id) {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(elem_id);
        EXPECT_EQ(row_id, expected[elem_id].first);
        EXPECT_EQ(elem_idx, expected[elem_id].second);
    }
}

TEST(ArrayOffsetsGrowing, MultiplePendingBatches) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens5 = {2};
    offsets.Insert(5, lens5.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 0);

    std::vector<int32_t> lens3 = {3};
    offsets.Insert(3, lens3.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 0);

    std::vector<int32_t> lens1 = {1};
    offsets.Insert(1, lens1.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 0);

    std::vector<int32_t> lens0 = {2};
    offsets.Insert(0, lens0.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 2);
    EXPECT_EQ(offsets.GetTotalElementCount(), 3);

    std::vector<int32_t> lens2 = {1};
    offsets.Insert(2, lens2.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 4);
    EXPECT_EQ(offsets.GetTotalElementCount(), 7);

    std::vector<int32_t> lens4 = {2};
    offsets.Insert(4, lens4.data(), 1);
    EXPECT_EQ(offsets.GetRowCount(), 6);
    EXPECT_EQ(offsets.GetTotalElementCount(), 11);

    EXPECT_EQ(offsets.ElementIDToRowID(0), std::make_pair(0, 0));
    EXPECT_EQ(offsets.ElementIDToRowID(2), std::make_pair(1, 0));
    EXPECT_EQ(offsets.ElementIDToRowID(4), std::make_pair(3, 0));
    EXPECT_EQ(offsets.ElementIDToRowID(7), std::make_pair(4, 0));
    EXPECT_EQ(offsets.ElementIDToRowID(10), std::make_pair(5, 1));
}

TEST(ArrayOffsetsGrowing, RowBitsetToElementBitset) {
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
    EXPECT_TRUE(valid_elem_bitset.all());
}

TEST(ArrayOffsetsGrowing, ConcurrentRead) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

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

TEST(ArrayOffsetsGrowing, LargeArrayLength) {
    ArrayOffsetsGrowing offsets;

    std::vector<int32_t> lens = {10000};
    offsets.Insert(0, lens.data(), 1);

    EXPECT_EQ(offsets.GetRowCount(), 1);
    EXPECT_EQ(offsets.GetTotalElementCount(), 10000);
    EXPECT_EQ(offsets.ElementIDToRowID(0), std::make_pair(0, 0));
    EXPECT_EQ(offsets.ElementIDToRowID(5000), std::make_pair(0, 5000));
    EXPECT_EQ(offsets.ElementIDToRowID(9999), std::make_pair(0, 9999));
    EXPECT_EQ(offsets.ElementIDRangeOfRow(0), std::make_pair(0, 10000));
}

TEST(ArrayOffsetsSealed, RowOffsetsToElementOffsets) {
    ArrayOffsetsSealed offsets({0, 2, 5, 6});

    FixedVector<int32_t> row_offsets = {0, 2};
    auto elem_offsets = offsets.RowOffsetsToElementOffsets(row_offsets);

    ASSERT_EQ(elem_offsets.size(), 3);
    EXPECT_EQ(elem_offsets[0], 0);
    EXPECT_EQ(elem_offsets[1], 1);
    EXPECT_EQ(elem_offsets[2], 5);

    FixedVector<int32_t> empty_rows;
    EXPECT_TRUE(offsets.RowOffsetsToElementOffsets(empty_rows).empty());
}

TEST(ArrayOffsetsSealed, RowBitsetToElementOffsetsWithRowStart) {
    ArrayOffsetsSealed offsets({0, 2, 5, 6, 8});

    TargetBitmap row_bitset(3);
    row_bitset[0] = true;   // row 1
    row_bitset[1] = false;  // row 2
    row_bitset[2] = true;   // row 3

    TargetBitmapView view(row_bitset.data(), row_bitset.size());
    auto elem_offsets = offsets.RowBitsetToElementOffsets(view, 1);

    ASSERT_EQ(elem_offsets.size(), 5);
    EXPECT_EQ(elem_offsets[0], 2);
    EXPECT_EQ(elem_offsets[1], 3);
    EXPECT_EQ(elem_offsets[2], 4);
    EXPECT_EQ(elem_offsets[3], 6);
    EXPECT_EQ(elem_offsets[4], 7);
}

TEST(ArrayOffsetsSealed, RowBitsetToElementBitsetWithRowStartAndInvalidRows) {
    ArrayOffsetsSealed offsets({0, 2, 5, 6, 8});

    TargetBitmap row_bitset(3);
    row_bitset[0] = true;  // row 1
    row_bitset[1] = true;  // row 2
    row_bitset[2] = true;  // row 3

    TargetBitmap valid_row_bitset(3);
    valid_row_bitset[0] = true;
    valid_row_bitset[1] = false;
    valid_row_bitset[2] = true;

    TargetBitmapView row_view(row_bitset.data(), row_bitset.size());
    TargetBitmapView valid_view(valid_row_bitset.data(),
                                valid_row_bitset.size());
    auto [elem_bitset, valid_elem_bitset] =
        offsets.RowBitsetToElementBitset(row_view, valid_view, 1);

    ASSERT_EQ(elem_bitset.size(), 6);
    EXPECT_TRUE(elem_bitset.all());
    EXPECT_TRUE(valid_elem_bitset[0]);
    EXPECT_TRUE(valid_elem_bitset[1]);
    EXPECT_TRUE(valid_elem_bitset[2]);
    EXPECT_FALSE(valid_elem_bitset[3]);
    EXPECT_TRUE(valid_elem_bitset[4]);
    EXPECT_TRUE(valid_elem_bitset[5]);
}

TEST(ArrayOffsetsGrowing, RowOffsetsAndBitsetToElementOffsets) {
    ArrayOffsetsGrowing offsets;
    std::vector<int32_t> lens = {2, 3, 1};
    offsets.Insert(0, lens.data(), 3);

    FixedVector<int32_t> row_offsets = {0, 2};
    auto elem_offsets = offsets.RowOffsetsToElementOffsets(row_offsets);
    ASSERT_EQ(elem_offsets.size(), 3);
    EXPECT_EQ(elem_offsets[0], 0);
    EXPECT_EQ(elem_offsets[1], 1);
    EXPECT_EQ(elem_offsets[2], 5);

    TargetBitmap row_bitset(3);
    row_bitset[0] = true;
    row_bitset[1] = false;
    row_bitset[2] = true;

    TargetBitmapView view(row_bitset.data(), row_bitset.size());
    auto elem_offsets_from_bitset = offsets.RowBitsetToElementOffsets(view, 0);
    EXPECT_EQ(elem_offsets_from_bitset, elem_offsets);
}

TEST(ArrayOffsetsGrowing, RowBitsetToElementBitsetWithRowStart) {
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

    ASSERT_EQ(elem_bitset.size(), 6);
    EXPECT_TRUE(elem_bitset[0]);
    EXPECT_TRUE(elem_bitset[1]);
    EXPECT_TRUE(elem_bitset[2]);
    EXPECT_FALSE(elem_bitset[3]);
    EXPECT_TRUE(elem_bitset[4]);
    EXPECT_TRUE(elem_bitset[5]);
    EXPECT_TRUE(valid_elem_bitset.all());
}
