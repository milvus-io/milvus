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

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <gtest/gtest.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_nested.h"
#include "common/Chunk.h"
#include "common/ChunkTarget.h"
#include "common/ChunkWriter.h"
#include "common/Types.h"
#include "gtest/gtest.h"

using milvus::DataType;
using milvus::MemChunkTarget;
using milvus::MMAP_ARRAY_PADDING;
using milvus::VectorArrayChunk;
using milvus::VectorArrayChunkWriter;

namespace {

// Calculate byte width for a single vector based on data type and dimension
int
GetByteWidth(DataType data_type, int dim) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT:
            return dim * sizeof(float);
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
            return dim * 2;
        case DataType::VECTOR_INT8:
            return dim;
        case DataType::VECTOR_BINARY:
            return (dim + 7) / 8;
        default:
            return dim * sizeof(float);
    }
}

// Helper function to build a ListArray of FixedSizeBinary (vector array)
// Each row contains a variable number of vectors
// vectors_per_row: specifies how many vectors each row contains
// dim: dimension of each vector
// data_type: the vector data type
std::shared_ptr<arrow::ListArray>
BuildVectorArrayListArray(const std::vector<int>& vectors_per_row,
                          int dim,
                          DataType data_type = DataType::VECTOR_FLOAT) {
    int byte_width = GetByteWidth(data_type, dim);
    auto value_type = arrow::fixed_size_binary(byte_width);

    arrow::FixedSizeBinaryBuilder value_builder(value_type);
    arrow::ListBuilder list_builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::FixedSizeBinaryBuilder>(value_type));

    auto& fsb_builder = dynamic_cast<arrow::FixedSizeBinaryBuilder&>(
        *list_builder.value_builder());

    std::default_random_engine gen(42);
    std::uniform_int_distribution<int> dist(0, 255);

    for (size_t row = 0; row < vectors_per_row.size(); ++row) {
        EXPECT_TRUE(list_builder.Append().ok());
        for (int vec = 0; vec < vectors_per_row[row]; ++vec) {
            std::vector<uint8_t> vector_data(byte_width);
            for (int d = 0; d < byte_width; ++d) {
                vector_data[d] = static_cast<uint8_t>(dist(gen));
            }
            EXPECT_TRUE(fsb_builder.Append(vector_data.data()).ok());
        }
    }

    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(list_builder.Finish(&result).ok());
    return std::static_pointer_cast<arrow::ListArray>(result);
}

// Test parameter structure for parameterized tests
struct VectorArrayWriterTestParam {
    DataType data_type;
    int dim;
    std::string test_name;
};

}  // namespace

// Parameterized test class for VectorArrayChunkWriter
class VectorArrayChunkWriterParameterizedTest
    : public ::testing::TestWithParam<VectorArrayWriterTestParam> {
 protected:
    DataType
    data_type() const {
        return GetParam().data_type;
    }
    int
    dim() const {
        return GetParam().dim;
    }
    int
    byte_width() const {
        return GetByteWidth(data_type(), dim());
    }
};

// Test basic functionality without slicing - parameterized version
TEST_P(VectorArrayChunkWriterParameterizedTest, BasicNoSlice) {
    // 5 rows with varying number of vectors per row
    std::vector<int> vectors_per_row = {2, 3, 1, 4, 2};  // Total: 12 vectors

    auto list_array =
        BuildVectorArrayListArray(vectors_per_row, dim(), data_type());
    ASSERT_EQ(list_array->length(), 5);

    arrow::ArrayVector vec{list_array};

    VectorArrayChunkWriter writer(dim(), data_type());
    auto [calculated_size, row_count] = writer.calculate_size(vec);

    // Expected size:
    // - 12 vectors * byte_width bytes for data
    // - (5 * 2 + 1) * 4 bytes = 44 bytes for offsets and lengths
    // - MMAP_ARRAY_PADDING (1) byte for padding
    int expected_data_size = 12 * byte_width();
    int expected_overhead =
        sizeof(uint32_t) * (5 * 2 + 1) + MMAP_ARRAY_PADDING;  // 44 + 1 = 45
    EXPECT_EQ(calculated_size, expected_data_size + expected_overhead);
    EXPECT_EQ(row_count, 5);

    // Verify write_to_target works correctly
    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(vec, target);

    // Create chunk from target data
    auto* data = target->release();
    auto chunk = std::make_unique<VectorArrayChunk>(
        dim(), row_count, data, calculated_size, data_type(), nullptr);
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->RowNums(), 5);
}

// Test with sliced ListArray - THIS IS THE KEY TEST FOR THE BUG
TEST_P(VectorArrayChunkWriterParameterizedTest, SlicedListArray) {
    // Original: 10 rows with 2 vectors each = 20 vectors total
    std::vector<int> vectors_per_row(10, 2);

    auto original_array =
        BuildVectorArrayListArray(vectors_per_row, dim(), data_type());
    ASSERT_EQ(original_array->length(), 10);

    // Slice: take rows 3-6 (4 rows, should have 8 vectors)
    auto sliced_array =
        std::static_pointer_cast<arrow::ListArray>(original_array->Slice(3, 4));
    ASSERT_EQ(sliced_array->length(), 4);

    // Verify the slice behavior that caused the bug:
    // values() returns the ENTIRE underlying array
    auto values = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
        sliced_array->values());
    EXPECT_EQ(values->length(), 20);  // Still 20, not 8!

    // But the actual data we need is only 8 vectors
    const int32_t* offsets = sliced_array->raw_value_offsets();
    int64_t actual_values = offsets[sliced_array->length()] - offsets[0];
    EXPECT_EQ(actual_values, 8);  // This is what we actually need

    arrow::ArrayVector vec{sliced_array};

    VectorArrayChunkWriter writer(dim(), data_type());
    auto [calculated_size, row_count] = writer.calculate_size(vec);

    // Expected size with the fix:
    // - 8 vectors * byte_width bytes for data
    // - (4 * 2 + 1) * 4 bytes = 36 bytes for offsets and lengths
    // - MMAP_ARRAY_PADDING (1) byte for padding
    int expected_data_size = 8 * byte_width();
    int expected_overhead =
        sizeof(uint32_t) * (4 * 2 + 1) + MMAP_ARRAY_PADDING;  // 36 + 1 = 37
    EXPECT_EQ(calculated_size, expected_data_size + expected_overhead);
    EXPECT_EQ(row_count, 4);

    // Verify write_to_target completes successfully and matches calculated size
    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(vec, target);

    // Create chunk from target data
    auto* data = target->release();
    auto chunk = std::make_unique<VectorArrayChunk>(
        dim(), row_count, data, calculated_size, data_type(), nullptr);
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->RowNums(), 4);
}

// Test with multiple sliced arrays in array_vec
TEST_P(VectorArrayChunkWriterParameterizedTest, MultipleSlicedArrays) {
    // First array: 8 rows with varying vectors
    std::vector<int> vectors_per_row1 = {1, 2, 3, 2, 1, 2, 3, 2};  // 16 total
    auto array1 =
        BuildVectorArrayListArray(vectors_per_row1, dim(), data_type());

    // Second array: 6 rows with 2 vectors each
    std::vector<int> vectors_per_row2(6, 2);  // 12 total
    auto array2 =
        BuildVectorArrayListArray(vectors_per_row2, dim(), data_type());

    // Slice both: first array rows 2-5 (4 rows), second array rows 1-4 (4 rows)
    auto sliced1 =
        std::static_pointer_cast<arrow::ListArray>(array1->Slice(2, 4));
    auto sliced2 =
        std::static_pointer_cast<arrow::ListArray>(array2->Slice(1, 4));

    ASSERT_EQ(sliced1->length(), 4);
    ASSERT_EQ(sliced2->length(), 4);

    // Calculate expected vectors from sliced arrays
    // sliced1: rows 2,3,4,5 from original = vectors_per_row1[2:6] = {3,2,1,2} = 8 vectors
    // sliced2: rows 1,2,3,4 from original = 4*2 = 8 vectors
    int expected_vectors = 8 + 8;  // 16 vectors total
    int expected_rows = 4 + 4;     // 8 rows total

    arrow::ArrayVector vec{sliced1, sliced2};

    VectorArrayChunkWriter writer(dim(), data_type());
    auto [calculated_size, row_count] = writer.calculate_size(vec);

    int expected_data_size = expected_vectors * byte_width();
    int expected_overhead = sizeof(uint32_t) * (expected_rows * 2 + 1) +
                            MMAP_ARRAY_PADDING;  // 17 * 4 + 1 = 69
    EXPECT_EQ(calculated_size, expected_data_size + expected_overhead);
    EXPECT_EQ(row_count, expected_rows);

    // Verify write_to_target completes successfully
    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(vec, target);

    // Create chunk from target data
    auto* data = target->release();
    auto chunk = std::make_unique<VectorArrayChunk>(
        dim(), row_count, data, calculated_size, data_type(), nullptr);
    ASSERT_NE(chunk, nullptr);
    EXPECT_EQ(chunk->RowNums(), expected_rows);
}

// Test edge case: slice from the beginning
TEST_P(VectorArrayChunkWriterParameterizedTest, SliceFromBeginning) {
    std::vector<int> vectors_per_row = {3, 2, 1, 4, 2};  // 12 vectors total

    auto original_array =
        BuildVectorArrayListArray(vectors_per_row, dim(), data_type());

    // Slice first 2 rows (should have 3+2=5 vectors)
    auto sliced =
        std::static_pointer_cast<arrow::ListArray>(original_array->Slice(0, 2));
    ASSERT_EQ(sliced->length(), 2);

    arrow::ArrayVector vec{sliced};

    VectorArrayChunkWriter writer(dim(), data_type());
    auto [calculated_size, row_count] = writer.calculate_size(vec);

    int expected_data_size = 5 * byte_width();
    int expected_overhead =
        sizeof(uint32_t) * (2 * 2 + 1) + MMAP_ARRAY_PADDING;  // 20 + 1 = 21
    EXPECT_EQ(calculated_size, expected_data_size + expected_overhead);
    EXPECT_EQ(row_count, 2);
}

// Test edge case: slice to the end
TEST_P(VectorArrayChunkWriterParameterizedTest, SliceToEnd) {
    std::vector<int> vectors_per_row = {3, 2, 1, 4, 2};  // 12 vectors total

    auto original_array =
        BuildVectorArrayListArray(vectors_per_row, dim(), data_type());

    // Slice last 2 rows (should have 4+2=6 vectors)
    auto sliced =
        std::static_pointer_cast<arrow::ListArray>(original_array->Slice(3, 2));
    ASSERT_EQ(sliced->length(), 2);

    arrow::ArrayVector vec{sliced};

    VectorArrayChunkWriter writer(dim(), data_type());
    auto [calculated_size, row_count] = writer.calculate_size(vec);

    int expected_data_size = 6 * byte_width();
    int expected_overhead =
        sizeof(uint32_t) * (2 * 2 + 1) + MMAP_ARRAY_PADDING;  // 20 + 1 = 21
    EXPECT_EQ(calculated_size, expected_data_size + expected_overhead);
    EXPECT_EQ(row_count, 2);
}

// Test that calculate_size and write_to_target produce consistent results
TEST_P(VectorArrayChunkWriterParameterizedTest, SizeConsistencyWithSlice) {
    // Use dim() from parameter, but for consistency test use a fixed dim=8
    // to match the original test's vectors_per_row expectations
    const int test_dim = 8;
    std::vector<int> vectors_per_row = {
        1, 3, 2, 4, 1, 2, 3, 1};  // 17 vectors total

    auto original_array =
        BuildVectorArrayListArray(vectors_per_row, test_dim, data_type());

    // Try various slices and verify size consistency
    std::vector<std::pair<int64_t, int64_t>> slices = {
        {0, 8},  // full array
        {0, 4},  // first half
        {4, 4},  // second half
        {2, 3},  // middle
        {1, 6},  // most of it
        {0, 1},  // single row
        {7, 1},  // last row
    };

    int test_byte_width = GetByteWidth(data_type(), test_dim);

    for (const auto& [offset, length] : slices) {
        auto sliced = std::static_pointer_cast<arrow::ListArray>(
            original_array->Slice(offset, length));
        ASSERT_EQ(sliced->length(), length);

        arrow::ArrayVector vec{sliced};

        VectorArrayChunkWriter writer(test_dim, data_type());
        auto [calculated_size, row_count] = writer.calculate_size(vec);
        EXPECT_EQ(row_count, length);

        // Write and verify the chunk was created successfully
        // This implicitly tests that calculate_size matches the actual write
        auto target = std::make_shared<MemChunkTarget>(calculated_size);
        writer.write_to_target(vec, target);

        // Create chunk from target data
        auto* data = target->release();
        auto chunk = std::make_unique<VectorArrayChunk>(
            test_dim, row_count, data, calculated_size, data_type(), nullptr);
        ASSERT_NE(chunk, nullptr)
            << "Failed for slice(" << offset << ", " << length << ")";
        EXPECT_EQ(chunk->RowNums(), length);
    }
}

// Instantiate parameterized tests for all vector types
INSTANTIATE_TEST_SUITE_P(
    VectorTypes,
    VectorArrayChunkWriterParameterizedTest,
    ::testing::Values(
        VectorArrayWriterTestParam{DataType::VECTOR_FLOAT, 4, "FloatVector"},
        VectorArrayWriterTestParam{
            DataType::VECTOR_FLOAT16, 4, "Float16Vector"},
        VectorArrayWriterTestParam{
            DataType::VECTOR_BFLOAT16, 4, "BFloat16Vector"},
        VectorArrayWriterTestParam{DataType::VECTOR_INT8, 4, "Int8Vector"},
        VectorArrayWriterTestParam{
            DataType::VECTOR_BINARY, 32, "BinaryVector"}),
    [](const ::testing::TestParamInfo<VectorArrayWriterTestParam>& info) {
        return info.param.test_name;
    });
