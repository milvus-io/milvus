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
#include "common/Vector.h"

using namespace milvus;

// Helper function to create a bitmap ColumnVector with given data and valid bitmaps
static ColumnVectorPtr
CreateBitmapColumnVector(const std::vector<bool>& data,
                         const std::vector<bool>& valid) {
    size_t size = data.size();
    TargetBitmap data_bitmap(size);
    TargetBitmap valid_bitmap(size);
    for (size_t i = 0; i < size; ++i) {
        if (data[i]) {
            data_bitmap.set(i);
        }
        if (valid[i]) {
            valid_bitmap.set(i);
        }
    }
    return std::make_shared<ColumnVector>(std::move(data_bitmap),
                                          std::move(valid_bitmap));
}

// ============================================================================
// ColumnVector::AllTrue and AllFalse Tests
// ============================================================================

TEST(ColumnVectorTest, AllTrueBasic) {
    // Test with: [TRUE, FALSE, NULL] - not all true
    auto vec =
        CreateBitmapColumnVector({true, false, false}, {true, true, false});
    EXPECT_FALSE(vec->AllTrue());
}

TEST(ColumnVectorTest, AllFalseBasic) {
    // Test with: [TRUE, FALSE, NULL] - not all false
    auto vec =
        CreateBitmapColumnVector({true, false, false}, {true, true, false});
    EXPECT_FALSE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllTrueWhenAllTrue) {
    auto vec = CreateBitmapColumnVector({true, true, true}, {true, true, true});
    EXPECT_TRUE(vec->AllTrue());
    EXPECT_FALSE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllFalseWhenAllFalse) {
    auto vec =
        CreateBitmapColumnVector({false, false, false}, {true, true, true});
    EXPECT_FALSE(vec->AllTrue());
    EXPECT_TRUE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllTrueWithNull) {
    // If any row is NULL, AllTrue should return false
    auto vec =
        CreateBitmapColumnVector({true, true, true}, {true, true, false});
    EXPECT_FALSE(vec->AllTrue());
}

TEST(ColumnVectorTest, AllFalseWithNull) {
    // If any row is NULL, AllFalse should return false
    auto vec =
        CreateBitmapColumnVector({false, false, false}, {true, true, false});
    EXPECT_FALSE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllTrueAllNull) {
    // All NULL values - neither AllTrue nor AllFalse
    auto vec =
        CreateBitmapColumnVector({false, true, false}, {false, false, false});
    EXPECT_FALSE(vec->AllTrue());
    EXPECT_FALSE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllTrueEmpty) {
    auto vec = CreateBitmapColumnVector({}, {});
    // Empty vector is vacuously all true and all false
    EXPECT_TRUE(vec->AllTrue());
    EXPECT_TRUE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllTrueMixedValues) {
    // Test with: [TRUE, FALSE, NULL, TRUE, FALSE, NULL, NULL, TRUE]
    auto vec = CreateBitmapColumnVector(
        {true, false, false, true, false, true, false, true},
        {true, true, false, true, true, false, false, true});

    EXPECT_FALSE(vec->AllTrue());
    EXPECT_FALSE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllTrueLargeScale) {
    // Test with a larger dataset - all true
    const size_t size = 1000;
    std::vector<bool> data(size, true), valid(size, true);

    auto vec = CreateBitmapColumnVector(data, valid);
    EXPECT_TRUE(vec->AllTrue());
    EXPECT_FALSE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllFalseLargeScale) {
    // Test with a larger dataset - all false
    const size_t size = 1000;
    std::vector<bool> data(size, false), valid(size, true);

    auto vec = CreateBitmapColumnVector(data, valid);
    EXPECT_FALSE(vec->AllTrue());
    EXPECT_TRUE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllTrueEarlyTermination) {
    // Test early termination - first element is false
    const size_t size = 10000;
    std::vector<bool> data(size, true), valid(size, true);
    data[0] = false;  // First element breaks the condition

    auto vec = CreateBitmapColumnVector(data, valid);
    EXPECT_FALSE(vec->AllTrue());
}

TEST(ColumnVectorTest, AllFalseEarlyTermination) {
    // Test early termination - first element is true
    const size_t size = 10000;
    std::vector<bool> data(size, false), valid(size, true);
    data[0] = true;  // First element breaks the condition

    auto vec = CreateBitmapColumnVector(data, valid);
    EXPECT_FALSE(vec->AllFalse());
}

TEST(ColumnVectorTest, AllTrueBoundaryConditions) {
    // Test with sizes that are not multiples of 64 (word size)
    for (size_t size : {1, 7, 31, 63, 64, 65, 127, 128, 129, 255, 256, 257}) {
        // All true case
        std::vector<bool> data_true(size, true), valid_true(size, true);
        auto vec_true = CreateBitmapColumnVector(data_true, valid_true);
        EXPECT_TRUE(vec_true->AllTrue()) << "AllTrue failed for size " << size;
        EXPECT_FALSE(vec_true->AllFalse())
            << "AllFalse should be false for size " << size;

        // All false case
        std::vector<bool> data_false(size, false), valid_false(size, true);
        auto vec_false = CreateBitmapColumnVector(data_false, valid_false);
        EXPECT_FALSE(vec_false->AllTrue())
            << "AllTrue should be false for size " << size;
        EXPECT_TRUE(vec_false->AllFalse())
            << "AllFalse failed for size " << size;

        // Last element breaks condition
        std::vector<bool> data_last(size, true), valid_last(size, true);
        data_last[size - 1] = false;
        auto vec_last = CreateBitmapColumnVector(data_last, valid_last);
        EXPECT_FALSE(vec_last->AllTrue())
            << "AllTrue should fail when last element is false for size "
            << size;
    }
}

TEST(ColumnVectorTest, AllMethodsOnNonBitmapThrows) {
    // AllTrue and AllFalse should only work on bitmap ColumnVectors
    // Create a non-bitmap ColumnVector (regular scalar vector)
    auto non_bitmap_vec =
        std::make_shared<ColumnVector>(DataType::INT32, 10, std::nullopt);

    // Verify it's not a bitmap
    EXPECT_FALSE(non_bitmap_vec->IsBitmap());

    // Calling AllTrue on non-bitmap should throw
    EXPECT_THROW(non_bitmap_vec->AllTrue(), std::runtime_error);

    // Calling AllFalse on non-bitmap should throw
    EXPECT_THROW(non_bitmap_vec->AllFalse(), std::runtime_error);
}
