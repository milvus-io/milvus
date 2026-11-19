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
#include <cstddef>
#include <initializer_list>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "gtest/gtest.h"

using namespace milvus;

TEST(TypedSimpleVectorTest, ReadsFlatAndConstantThroughTheSameInterface) {
    auto flat = std::make_shared<ColumnVector<int64_t>>(DataType::INT64, 3);
    flat->SetValueAt(0, 11);
    flat->SetValueAt(1, 22);
    flat->SetValueAt(2, 33);
    flat->nullAt(1);
    auto constant =
        std::make_shared<ConstantVector<int64_t>>(DataType::INT64, 3, 7);
    std::vector<SimpleVectorPtr<int64_t>> columns{flat, constant};
    std::vector<int64_t> sums;
    for (const auto& column : columns) {
        const SimpleVector<int64_t>& input = *column;
        int64_t sum = 0;
        for (size_t i = 0; i < input.size(); ++i) {
            if (input.ValidAt(i)) {
                sum += input.ValueAt(i);
            }
        }
        sums.push_back(sum);
    }
    EXPECT_EQ(sums, (std::vector<int64_t>{44, 21}));
}

TEST(TypedSimpleVectorTest, UntypedNullLiteralPreservesItsLogicalType) {
    ConstantVector<bool> literal(DataType::NONE, 4, false, 1);
    const SimpleVector<bool>& input = literal;
    EXPECT_EQ(input.type(), DataType::NONE);
    EXPECT_EQ(input.size(), 4);
    for (size_t i = 0; i < input.size(); ++i) {
        EXPECT_FALSE(input.ValidAt(i));
    }
}

TEST(TypedSimpleVectorTest, StringsBorrowOwnedPayloadWithoutCopyingIt) {
    auto flat =
        std::make_shared<ColumnVector<std::string>>(DataType::VARCHAR, 2);
    const std::string long_value(1024, 'x');
    flat->SetValueAt(0, long_value);
    flat->SetValueAt(1, "");
    const SimpleVector<std::string>& input = *flat;
    static_assert(std::is_same_v<decltype(input.ValueAt(0)), std::string_view>);
    auto view = input.ValueAt(0);
    EXPECT_EQ(view, long_value);
    EXPECT_EQ(view.data(), flat->RawAsValues<std::string>()[0].data());
    EXPECT_TRUE(input.ValueAt(1).empty());

    auto constant = std::make_shared<ConstantVector<std::string>>(
        DataType::VARCHAR, 2, long_value);
    const SimpleVector<std::string>& constant_input = *constant;
    EXPECT_EQ(constant_input.ValueAt(0), long_value);
    EXPECT_EQ(constant_input.ValueAt(1).data(), constant->GetValue().data());
}

TEST(TypedSimpleVectorTest,
     FactoryPreservesLogicalAliasesAndRejectsWrongTypes) {
    for (auto type : {DataType::INT64, DataType::TIMESTAMPTZ}) {
        auto vector = MakeColumnVector(type, 1);
        EXPECT_EQ(vector->type(), type);
        auto column = AsColumnVector<int64_t>(vector);
        column->SetValueAt(0, 123);
        EXPECT_EQ(AsSimpleVector<int64_t>(vector)->ValueAt(0), 123);
        EXPECT_THROW(AsColumnVector<int32_t>(vector), SegcoreError);
        EXPECT_THROW(AsSimpleVector<int32_t>(vector), SegcoreError);
    }
    for (auto type : {DataType::STRING,
                      DataType::VARCHAR,
                      DataType::TEXT,
                      DataType::GEOMETRY}) {
        auto vector = MakeColumnVector(type, 1);
        EXPECT_EQ(vector->type(), type);
        AsColumnVector<std::string>(vector)->SetValueAt(0, "value");
        EXPECT_EQ(AsSimpleVector<std::string>(vector)->ValueAt(0), "value");
    }
    EXPECT_THROW((ColumnVector<int64_t>(DataType::INT32, 1)), SegcoreError);
    auto values = InitScalarFieldDataWithLength(DataType::INT32, 1);
    EXPECT_THROW(
        (ColumnVector<int64_t>(std::move(values), TargetBitmap(1, true), 0)),
        SegcoreError);
}

TEST(TypedSimpleVectorTest, BitmapAndInt8HaveDifferentPhysicalAccess) {
    auto flat = std::make_shared<ColumnVector<int8_t>>(DataType::INT8, 65);
    flat->SetValueAt(0, -7);
    flat->SetValueAt(64, 42);
    TargetBitmap bits(65, false);
    bits.set(0, true);
    bits.set(64, true);
    TargetBitmap valid(65, true);
    valid.set(1, false);
    auto bitmap = std::make_shared<ColumnVector<int8_t>>(std::move(bits),
                                                         std::move(valid));
    const SimpleVector<int8_t>& input = *bitmap;
    EXPECT_EQ(flat->ValueAt(0), -7);
    EXPECT_EQ(flat->ValueAt(64), 42);
    EXPECT_EQ(input.ValueAt(0), 1);
    EXPECT_EQ(input.ValueAt(63), 0);
    EXPECT_EQ(input.ValueAt(64), 1);
    EXPECT_FALSE(input.ValidAt(1));
    bitmap->SetValueAt(64, 0);
    EXPECT_FALSE(TargetBitmapView(bitmap->GetRawData(), bitmap->size())[64]);
    EXPECT_THROW(bitmap->resize(66), SegcoreError);
}

// Helper function to create a bitmap ColumnVector with given data and valid bitmaps
static ColumnVectorPtr<int8_t>
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
    return std::make_shared<ColumnVector<int8_t>>(std::move(data_bitmap),
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

TEST(FieldDataTest, InitScalarFieldDataWithLengthSetsLength) {
    auto field_data = InitScalarFieldDataWithLength(DataType::INT64, 3);
    EXPECT_EQ(field_data->Length(), 3);
    EXPECT_EQ(field_data->get_num_rows(), 3);
    EXPECT_FALSE(field_data->IsNullable());
}

TEST(ColumnVectorTest, FieldDataBackedConstructorRejectsInvalidNullCount) {
    EXPECT_THROW(
        {
            FixedVector<int64_t> values(3);
            auto field_data = std::make_shared<FieldData<int64_t>>(
                DataType::INT64, false, std::move(values));
            TargetBitmap valid(3, true);
            auto vec = std::make_shared<ColumnVector<int64_t>>(
                std::move(field_data), std::move(valid), 4);
        },
        std::runtime_error);
}

TEST(ColumnVectorTest, AllMethodsOnNonBitmapThrows) {
    // AllTrue and AllFalse should only work on bitmap ColumnVectors
    // Create a non-bitmap ColumnVector (regular scalar vector)
    auto non_bitmap_vec = std::make_shared<ColumnVector<int32_t>>(
        DataType::INT32, 10, std::nullopt);

    // Verify it's not a bitmap
    EXPECT_FALSE(non_bitmap_vec->IsBitmap());

    // Calling AllTrue on non-bitmap should throw
    EXPECT_THROW(non_bitmap_vec->AllTrue(), std::runtime_error);

    // Calling AllFalse on non-bitmap should throw
    EXPECT_THROW(non_bitmap_vec->AllFalse(), std::runtime_error);
}
