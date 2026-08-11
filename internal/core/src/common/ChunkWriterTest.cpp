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
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
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

using milvus::ArrayChunk;
using milvus::ArrayChunkWriter;
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

std::shared_ptr<arrow::ListArray>
BuildNullableFloatVectorArrayListArray(
    const std::vector<std::optional<int>>& vectors_per_row, int dim) {
    auto value_type = arrow::fixed_size_binary(dim * sizeof(float));
    arrow::ListBuilder list_builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::FixedSizeBinaryBuilder>(value_type));
    auto& fsb_builder = dynamic_cast<arrow::FixedSizeBinaryBuilder&>(
        *list_builder.value_builder());

    float value = 1.0F;
    for (const auto& vector_count : vectors_per_row) {
        if (!vector_count.has_value()) {
            EXPECT_TRUE(list_builder.AppendNull().ok());
            continue;
        }
        EXPECT_TRUE(list_builder.Append().ok());
        for (int vec = 0; vec < vector_count.value(); ++vec) {
            std::vector<float> vector_data(dim);
            for (int d = 0; d < dim; ++d) {
                vector_data[d] = value++;
            }
            EXPECT_TRUE(fsb_builder
                            .Append(reinterpret_cast<const uint8_t*>(
                                vector_data.data()))
                            .ok());
        }
    }

    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(list_builder.Finish(&result).ok());
    return std::static_pointer_cast<arrow::ListArray>(result);
}

std::shared_ptr<arrow::ListArray>
BuildElementNullableFloatVectorArrayListArray(int dim) {
    auto value_type = arrow::fixed_size_binary(dim * sizeof(float));
    arrow::ListBuilder list_builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::FixedSizeBinaryBuilder>(value_type));
    auto& fsb_builder = dynamic_cast<arrow::FixedSizeBinaryBuilder&>(
        *list_builder.value_builder());

    auto append_vector = [&](float first) {
        std::vector<float> vector_data(dim);
        for (int i = 0; i < dim; ++i) {
            vector_data[i] = first + i;
        }
        EXPECT_TRUE(
            fsb_builder
                .Append(reinterpret_cast<const uint8_t*>(vector_data.data()))
                .ok());
    };

    EXPECT_TRUE(list_builder.Append().ok());
    append_vector(1.0F);
    EXPECT_TRUE(fsb_builder.AppendNull().ok());

    EXPECT_TRUE(list_builder.Append().ok());

    EXPECT_TRUE(list_builder.Append().ok());
    EXPECT_TRUE(fsb_builder.AppendNull().ok());
    append_vector(3.0F);

    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(list_builder.Finish(&result).ok());
    return std::static_pointer_cast<arrow::ListArray>(result);
}

std::vector<uint8_t>
BuildVectorBytes(DataType data_type, int dim, int seed) {
    const auto byte_width = GetByteWidth(data_type, dim);
    std::vector<uint8_t> bytes(byte_width);
    if (data_type == DataType::VECTOR_FLOAT) {
        std::vector<float> values(dim);
        for (int i = 0; i < dim; ++i) {
            values[i] = static_cast<float>(seed + i);
        }
        std::memcpy(bytes.data(), values.data(), byte_width);
        return bytes;
    }
    for (int i = 0; i < byte_width; ++i) {
        bytes[i] = static_cast<uint8_t>(seed + i);
    }
    return bytes;
}

std::shared_ptr<arrow::ListArray>
BuildElementNullableVectorArrayListArray(int dim,
                                         DataType data_type,
                                         const std::vector<uint8_t>& first,
                                         const std::vector<uint8_t>& second) {
    const auto byte_width = GetByteWidth(data_type, dim);
    EXPECT_EQ(first.size(), byte_width);
    EXPECT_EQ(second.size(), byte_width);
    auto value_type = arrow::fixed_size_binary(byte_width);
    arrow::ListBuilder list_builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::FixedSizeBinaryBuilder>(value_type));
    auto& value_builder = dynamic_cast<arrow::FixedSizeBinaryBuilder&>(
        *list_builder.value_builder());

    EXPECT_TRUE(list_builder.Append().ok());
    EXPECT_TRUE(value_builder.Append(first.data()).ok());
    EXPECT_TRUE(value_builder.AppendNull().ok());

    EXPECT_TRUE(list_builder.Append().ok());

    EXPECT_TRUE(list_builder.Append().ok());
    EXPECT_TRUE(value_builder.AppendNull().ok());
    EXPECT_TRUE(value_builder.Append(second.data()).ok());

    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(list_builder.Finish(&result).ok());
    return std::static_pointer_cast<arrow::ListArray>(result);
}

std::string
GetVectorPayloadBytes(const milvus::VectorFieldProto& field,
                      DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT: {
            const auto& data = field.float_vector().data();
            return {reinterpret_cast<const char*>(data.data()),
                    data.size() * sizeof(float)};
        }
        case DataType::VECTOR_BINARY:
            return field.binary_vector();
        case DataType::VECTOR_FLOAT16:
            return field.float16_vector();
        case DataType::VECTOR_BFLOAT16:
            return field.bfloat16_vector();
        case DataType::VECTOR_INT8:
            return field.int8_vector();
        default:
            ADD_FAILURE() << "unsupported vector type "
                          << static_cast<int>(data_type);
            return {};
    }
}

std::string
BuildElementNullableIntArrayRow(const std::vector<int32_t>& values,
                                const std::vector<bool>& valid_data) {
    milvus::ScalarFieldProto row;
    for (auto value : values) {
        row.mutable_int_data()->add_data(value);
    }
    for (auto valid : valid_data) {
        row.add_valid_data(valid);
    }
    std::string serialized;
    EXPECT_TRUE(row.SerializeToString(&serialized));
    return serialized;
}

std::shared_ptr<arrow::BinaryArray>
BuildBinaryArray(const std::vector<std::optional<std::string>>& values) {
    arrow::BinaryBuilder builder;
    for (const auto& value : values) {
        if (value.has_value()) {
            EXPECT_TRUE(builder.Append(value.value()).ok());
        } else {
            EXPECT_TRUE(builder.AppendNull().ok());
        }
    }
    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(builder.Finish(&result).ok());
    return std::static_pointer_cast<arrow::BinaryArray>(result);
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

    VectorArrayChunkWriter writer(dim(), data_type(), false, false);
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
    auto chunk = std::make_unique<VectorArrayChunk>(dim(),
                                                    row_count,
                                                    data,
                                                    calculated_size,
                                                    data_type(),
                                                    nullptr,
                                                    false,
                                                    false);
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

    VectorArrayChunkWriter writer(dim(), data_type(), false, false);
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
    auto chunk = std::make_unique<VectorArrayChunk>(dim(),
                                                    row_count,
                                                    data,
                                                    calculated_size,
                                                    data_type(),
                                                    nullptr,
                                                    false,
                                                    false);
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

    VectorArrayChunkWriter writer(dim(), data_type(), false, false);
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
    auto chunk = std::make_unique<VectorArrayChunk>(dim(),
                                                    row_count,
                                                    data,
                                                    calculated_size,
                                                    data_type(),
                                                    nullptr,
                                                    false,
                                                    false);
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

    VectorArrayChunkWriter writer(dim(), data_type(), false, false);
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

    VectorArrayChunkWriter writer(dim(), data_type(), false, false);
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

    for (const auto& [offset, length] : slices) {
        auto sliced = std::static_pointer_cast<arrow::ListArray>(
            original_array->Slice(offset, length));
        ASSERT_EQ(sliced->length(), length);

        arrow::ArrayVector vec{sliced};

        VectorArrayChunkWriter writer(test_dim, data_type(), false, false);
        auto [calculated_size, row_count] = writer.calculate_size(vec);
        EXPECT_EQ(row_count, length);

        // Write and verify the chunk was created successfully
        // This implicitly tests that calculate_size matches the actual write
        auto target = std::make_shared<MemChunkTarget>(calculated_size);
        writer.write_to_target(vec, target);

        // Create chunk from target data
        auto* data = target->release();
        auto chunk = std::make_unique<VectorArrayChunk>(test_dim,
                                                        row_count,
                                                        data,
                                                        calculated_size,
                                                        data_type(),
                                                        nullptr,
                                                        false,
                                                        false);
        ASSERT_NE(chunk, nullptr)
            << "Failed for slice(" << offset << ", " << length << ")";
        EXPECT_EQ(chunk->RowNums(), length);
    }
}

TEST_P(VectorArrayChunkWriterParameterizedTest,
       ElementNullableRoundTripThroughChunkViews) {
    auto first = BuildVectorBytes(data_type(), dim(), 1);
    auto second = BuildVectorBytes(data_type(), dim(), 33);
    auto list_array = BuildElementNullableVectorArrayListArray(
        dim(), data_type(), first, second);
    ASSERT_EQ(list_array->length(), 3);

    arrow::ArrayVector arrays{list_array};
    VectorArrayChunkWriter writer(dim(), data_type(), false, true);
    auto [calculated_size, row_count] = writer.calculate_size(arrays);

    const int expected_vectors = 4;
    const int expected_element_bitmap_bytes =
        2 * milvus::TargetBitmap::policy_type::get_required_size_in_bytes(2);
    const int expected_size =
        sizeof(uint32_t) * (row_count * 2 + 1) + expected_element_bitmap_bytes +
        expected_vectors * byte_width() + MMAP_ARRAY_PADDING;
    EXPECT_EQ(calculated_size, expected_size);

    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(arrays, target);
    VectorArrayChunk chunk(dim(),
                           row_count,
                           target->release(),
                           calculated_size,
                           data_type(),
                           nullptr,
                           false,
                           true);

    const auto* offsets = chunk.Offsets();
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 2);
    EXPECT_EQ(offsets[3], 4);

    const auto* dense = reinterpret_cast<const uint8_t*>(chunk.Data());
    EXPECT_EQ(std::vector<uint8_t>(dense, dense + byte_width()), first);
    EXPECT_EQ(
        std::vector<uint8_t>(dense + byte_width(), dense + byte_width() * 2),
        std::vector<uint8_t>(byte_width(), 0));
    EXPECT_EQ(std::vector<uint8_t>(dense + byte_width() * 2,
                                   dense + byte_width() * 3),
              std::vector<uint8_t>(byte_width(), 0));
    EXPECT_EQ(std::vector<uint8_t>(dense + byte_width() * 3,
                                   dense + byte_width() * 4),
              second);

    auto [views, row_valid] = chunk.Views();
    ASSERT_TRUE(row_valid.empty());
    ASSERT_EQ(views.size(), 3);
    ASSERT_EQ(views[0].length(), 2);
    EXPECT_TRUE(views[0].is_element_valid(0));
    EXPECT_FALSE(views[0].is_element_valid(1));
    EXPECT_EQ(
        GetVectorPayloadBytes(views[0].output_data(), data_type()),
        std::string(reinterpret_cast<const char*>(first.data()), first.size()));
    ASSERT_EQ(views[1].length(), 0);
    ASSERT_EQ(views[2].length(), 2);
    EXPECT_FALSE(views[2].is_element_valid(0));
    EXPECT_TRUE(views[2].is_element_valid(1));
    EXPECT_EQ(GetVectorPayloadBytes(views[2].output_data(), data_type()),
              std::string(reinterpret_cast<const char*>(second.data()),
                          second.size()));
}

TEST_P(VectorArrayChunkWriterParameterizedTest,
       ElementNullableSlicedListArraysRoundTrip) {
    auto first = BuildVectorBytes(data_type(), dim(), 1);
    auto second = BuildVectorBytes(data_type(), dim(), 33);
    auto third = BuildVectorBytes(data_type(), dim(), 65);
    auto fourth = BuildVectorBytes(data_type(), dim(), 97);
    auto original1 = BuildElementNullableVectorArrayListArray(
        dim(), data_type(), first, second);
    auto original2 = BuildElementNullableVectorArrayListArray(
        dim(), data_type(), third, fourth);

    // Both inputs retain their original child arrays. The writer must use the
    // sliced list offsets rather than treating values() as slice-local.
    auto sliced1 =
        std::static_pointer_cast<arrow::ListArray>(original1->Slice(1, 2));
    auto sliced2 =
        std::static_pointer_cast<arrow::ListArray>(original2->Slice(2, 1));
    arrow::ArrayVector arrays{sliced1, sliced2};

    VectorArrayChunkWriter writer(dim(), data_type(), false, true);
    auto [calculated_size, row_count] = writer.calculate_size(arrays);
    ASSERT_EQ(row_count, 3);
    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(arrays, target);

    VectorArrayChunk chunk(dim(),
                           row_count,
                           target->release(),
                           calculated_size,
                           data_type(),
                           nullptr,
                           false,
                           true);
    const auto* offsets = chunk.Offsets();
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
    EXPECT_EQ(offsets[2], 2);
    EXPECT_EQ(offsets[3], 4);

    auto [views, row_valid] = chunk.Views();
    ASSERT_TRUE(row_valid.empty());
    ASSERT_EQ(views.size(), 3);
    EXPECT_EQ(views[0].length(), 0);
    ASSERT_EQ(views[1].length(), 2);
    EXPECT_FALSE(views[1].is_element_valid(0));
    EXPECT_TRUE(views[1].is_element_valid(1));
    EXPECT_EQ(GetVectorPayloadBytes(views[1].output_data(), data_type()),
              std::string(reinterpret_cast<const char*>(second.data()),
                          second.size()));
    ASSERT_EQ(views[2].length(), 2);
    EXPECT_FALSE(views[2].is_element_valid(0));
    EXPECT_TRUE(views[2].is_element_valid(1));
    EXPECT_EQ(GetVectorPayloadBytes(views[2].output_data(), data_type()),
              std::string(reinterpret_cast<const char*>(fourth.data()),
                          fourth.size()));
}

TEST(VectorArrayChunkWriterTest, NullableRowsRoundTripThroughChunkViews) {
    constexpr int dim = 2;
    auto list_array =
        BuildNullableFloatVectorArrayListArray({1, std::nullopt, 0, 2}, dim);
    ASSERT_EQ(list_array->length(), 4);
    ASSERT_EQ(list_array->null_count(), 1);

    arrow::ArrayVector vec{list_array};
    VectorArrayChunkWriter writer(dim, DataType::VECTOR_FLOAT, true, false);
    auto [calculated_size, row_count] = writer.calculate_size(vec);

    const int expected_valid_rows = 3;
    const int expected_vectors = 3;
    const int expected_size =
        (row_count + 7) / 8 + sizeof(uint32_t) * (expected_valid_rows * 2 + 1) +
        expected_vectors * dim * sizeof(float) + MMAP_ARRAY_PADDING;
    EXPECT_EQ(calculated_size, expected_size);
    EXPECT_EQ(row_count, 4);

    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(vec, target);

    auto* data = target->release();
    VectorArrayChunk chunk(dim,
                           row_count,
                           data,
                           calculated_size,
                           DataType::VECTOR_FLOAT,
                           nullptr,
                           true,
                           false);
    auto [views, valid] = chunk.Views();
    ASSERT_EQ(views.size(), 4);
    ASSERT_EQ(valid.size(), 4);
    EXPECT_TRUE(valid[0]);
    EXPECT_FALSE(valid[1]);
    EXPECT_TRUE(valid[2]);
    EXPECT_TRUE(valid[3]);
    EXPECT_EQ(views[0].length(), 1);
    EXPECT_EQ(views[1].length(), 0);
    EXPECT_EQ(views[2].length(), 0);
    EXPECT_EQ(views[3].length(), 2);

    const auto* offsets = chunk.Offsets();
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);
    EXPECT_EQ(offsets[2], 1);
    EXPECT_EQ(offsets[3], 1);
    EXPECT_EQ(offsets[4], 3);

    EXPECT_ANY_THROW(chunk.PhysicalOffsetOf(1));
    EXPECT_EQ(chunk.View(chunk.PhysicalOffsetOf(2)).length(), 0);
}

TEST(VectorArrayChunkWriterTest, ElementNullableRoundTripThroughChunkViews) {
    constexpr int dim = 2;
    auto list_array = BuildElementNullableFloatVectorArrayListArray(dim);
    ASSERT_EQ(list_array->length(), 3);

    arrow::ArrayVector vec{list_array};
    VectorArrayChunkWriter writer(dim, DataType::VECTOR_FLOAT, false, true);
    auto [calculated_size, row_count] = writer.calculate_size(vec);

    const int expected_vectors = 4;
    const int expected_element_bitmap_bytes =
        2 * milvus::TargetBitmap::policy_type::get_required_size_in_bytes(2);
    const int expected_size =
        sizeof(uint32_t) * (row_count * 2 + 1) + expected_element_bitmap_bytes +
        expected_vectors * dim * sizeof(float) + MMAP_ARRAY_PADDING;
    EXPECT_EQ(calculated_size, expected_size);
    EXPECT_EQ(row_count, 3);

    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(vec, target);

    auto* data = target->release();
    VectorArrayChunk chunk(dim,
                           row_count,
                           data,
                           calculated_size,
                           DataType::VECTOR_FLOAT,
                           nullptr,
                           false,
                           true);
    const auto* offsets = chunk.Offsets();
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 2);
    EXPECT_EQ(offsets[2], 2);
    EXPECT_EQ(offsets[3], 4);

    auto dense_data = reinterpret_cast<const float*>(chunk.Data());
    EXPECT_FLOAT_EQ(dense_data[0], 1.0F);
    EXPECT_FLOAT_EQ(dense_data[1], 2.0F);
    EXPECT_FLOAT_EQ(dense_data[2], 0.0F);
    EXPECT_FLOAT_EQ(dense_data[3], 0.0F);
    EXPECT_FLOAT_EQ(dense_data[4], 0.0F);
    EXPECT_FLOAT_EQ(dense_data[5], 0.0F);
    EXPECT_FLOAT_EQ(dense_data[6], 3.0F);
    EXPECT_FLOAT_EQ(dense_data[7], 4.0F);

    auto [views, valid] = chunk.Views();
    ASSERT_EQ(views.size(), 3);
    EXPECT_TRUE(valid.empty());

    ASSERT_TRUE(views[0].is_element_nullable());
    EXPECT_EQ(views[0].length(), 2);
    EXPECT_TRUE(views[0].is_element_valid(0));
    EXPECT_FALSE(views[0].is_element_valid(1));
    auto row0 = views[0].output_data();
    EXPECT_EQ(row0.valid_data_size(), 2);
    EXPECT_EQ(row0.float_vector().data_size(), dim);

    ASSERT_TRUE(views[1].is_element_nullable());
    EXPECT_EQ(views[1].length(), 0);
    auto row1 = views[1].output_data();
    EXPECT_EQ(row1.valid_data_size(), 0);
    EXPECT_EQ(row1.float_vector().data_size(), 0);

    ASSERT_TRUE(views[2].is_element_nullable());
    EXPECT_EQ(views[2].length(), 2);
    EXPECT_FALSE(views[2].is_element_valid(0));
    EXPECT_TRUE(views[2].is_element_valid(1));
    auto row2 = views[2].output_data();
    EXPECT_EQ(row2.valid_data_size(), 2);
    EXPECT_EQ(row2.float_vector().data_size(), dim);
}

TEST(VectorArrayChunkWriterTest,
     RowAndElementNullableRoundTripPreservesLogicalOffsets) {
    constexpr int dim = 2;
    auto value_type = arrow::fixed_size_binary(dim * sizeof(float));
    arrow::ListBuilder list_builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::FixedSizeBinaryBuilder>(value_type));
    auto& value_builder = dynamic_cast<arrow::FixedSizeBinaryBuilder&>(
        *list_builder.value_builder());

    auto append_vector = [&](float first) {
        const std::array<float, dim> vector{first, first + 1.0F};
        ASSERT_TRUE(value_builder
                        .Append(reinterpret_cast<const uint8_t*>(vector.data()))
                        .ok());
    };

    ASSERT_TRUE(list_builder.Append().ok());
    append_vector(1.0F);
    ASSERT_TRUE(value_builder.AppendNull().ok());
    append_vector(3.0F);

    ASSERT_TRUE(list_builder.AppendNull().ok());
    ASSERT_TRUE(list_builder.Append().ok());

    ASSERT_TRUE(list_builder.Append().ok());
    ASSERT_TRUE(value_builder.AppendNull().ok());
    append_vector(5.0F);

    ASSERT_TRUE(list_builder.Append().ok());
    append_vector(7.0F);
    append_vector(9.0F);

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(list_builder.Finish(&array).ok());
    auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
    ASSERT_EQ(list_array->length(), 5);
    ASSERT_EQ(list_array->null_count(), 1);

    arrow::ArrayVector arrays{list_array};
    VectorArrayChunkWriter writer(dim, DataType::VECTOR_FLOAT, true, true);
    auto [calculated_size, row_count] = writer.calculate_size(arrays);

    const size_t element_bitmap_bytes =
        milvus::TargetBitmap::policy_type::get_required_size_in_bytes(3) +
        milvus::TargetBitmap::policy_type::get_required_size_in_bytes(0) +
        milvus::TargetBitmap::policy_type::get_required_size_in_bytes(2) +
        milvus::TargetBitmap::policy_type::get_required_size_in_bytes(2);
    const size_t expected_size =
        (row_count + 7) / 8 + sizeof(uint32_t) * (4 * 2 + 1) +
        element_bitmap_bytes + 7 * dim * sizeof(float) + MMAP_ARRAY_PADDING;
    EXPECT_EQ(calculated_size, expected_size);

    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(arrays, target);

    VectorArrayChunk chunk(dim,
                           row_count,
                           target->release(),
                           calculated_size,
                           DataType::VECTOR_FLOAT,
                           nullptr,
                           true,
                           true);
    auto [views, row_valid] = chunk.Views();
    ASSERT_EQ(views.size(), 5);
    ASSERT_EQ(row_valid.size(), 5);
    EXPECT_TRUE(row_valid[0]);
    EXPECT_FALSE(row_valid[1]);
    EXPECT_TRUE(row_valid[2]);
    EXPECT_TRUE(row_valid[3]);
    EXPECT_TRUE(row_valid[4]);

    const auto* offsets = chunk.Offsets();
    const std::array<size_t, 6> expected_offsets{0, 3, 3, 3, 5, 7};
    for (size_t i = 0; i < expected_offsets.size(); ++i) {
        EXPECT_EQ(offsets[i], expected_offsets[i]);
    }

    ASSERT_EQ(views[0].length(), 3);
    EXPECT_TRUE(views[0].is_element_valid(0));
    EXPECT_FALSE(views[0].is_element_valid(1));
    EXPECT_TRUE(views[0].is_element_valid(2));
    EXPECT_EQ(views[1].length(), 0);
    EXPECT_EQ(views[2].length(), 0);
    ASSERT_EQ(views[3].length(), 2);
    EXPECT_FALSE(views[3].is_element_valid(0));
    EXPECT_TRUE(views[3].is_element_valid(1));
    ASSERT_EQ(views[4].length(), 2);
    EXPECT_TRUE(views[4].is_element_valid(0));
    EXPECT_TRUE(views[4].is_element_valid(1));

    const auto* dense_data = reinterpret_cast<const float*>(chunk.Data());
    const std::array<float, 14> expected_data{1.0F,
                                              2.0F,
                                              0.0F,
                                              0.0F,
                                              3.0F,
                                              4.0F,
                                              0.0F,
                                              0.0F,
                                              5.0F,
                                              6.0F,
                                              7.0F,
                                              8.0F,
                                              9.0F,
                                              10.0F};
    for (size_t i = 0; i < expected_data.size(); ++i) {
        EXPECT_FLOAT_EQ(dense_data[i], expected_data[i]);
    }
}

TEST(ArrayChunkWriterTest, ElementNullableRoundTripThroughChunkViews) {
    arrow::BinaryBuilder builder;
    auto row0 =
        BuildElementNullableIntArrayRow({10, 0, 30}, {true, false, true});
    auto row2 = BuildElementNullableIntArrayRow({}, {});
    auto row3 = BuildElementNullableIntArrayRow({40, 50}, {true, true});
    ASSERT_TRUE(builder.Append(row0).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append(row2).ok());
    ASSERT_TRUE(builder.Append(row3).ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    arrow::ArrayVector vec{array};

    ArrayChunkWriter writer(DataType::INT32, true, true);
    auto [calculated_size, row_count] = writer.calculate_size(vec);
    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(vec, target);

    auto* data = target->release();
    ArrayChunk chunk(
        row_count, data, calculated_size, DataType::INT32, true, true, nullptr);
    auto [views, valid] = chunk.Views();
    ASSERT_EQ(views.size(), 4);
    ASSERT_EQ(valid.size(), 4);
    EXPECT_TRUE(valid[0]);
    EXPECT_FALSE(valid[1]);
    EXPECT_TRUE(valid[2]);
    EXPECT_TRUE(valid[3]);

    ASSERT_TRUE(views[0].is_element_nullable());
    ASSERT_EQ(views[0].length(), 3);
    EXPECT_TRUE(views[0].is_element_valid(0));
    EXPECT_FALSE(views[0].is_element_valid(1));
    EXPECT_TRUE(views[0].is_element_valid(2));
    EXPECT_EQ(views[0].get_data_unchecked<int32_t>(0), 10);
    EXPECT_EQ(views[0].get_data_unchecked<int32_t>(2), 30);

    EXPECT_EQ(views[1].length(), 0);
    ASSERT_TRUE(views[2].is_element_nullable());
    EXPECT_EQ(views[2].length(), 0);
    ASSERT_TRUE(views[3].is_element_nullable());
    ASSERT_EQ(views[3].length(), 2);
    EXPECT_TRUE(views[3].is_element_valid(0));
    EXPECT_TRUE(views[3].is_element_valid(1));
    EXPECT_EQ(views[3].get_data_unchecked<int32_t>(0), 40);
    EXPECT_EQ(views[3].get_data_unchecked<int32_t>(1), 50);
}

TEST(ArrayChunkWriterTest,
     ElementNullableSlicedBinaryArraysPreserveRowAndElementNulls) {
    auto skip0 = BuildElementNullableIntArrayRow({1}, {true});
    auto row0 = BuildElementNullableIntArrayRow({10, 0}, {true, false});
    auto empty = BuildElementNullableIntArrayRow({}, {});
    auto skip1 = BuildElementNullableIntArrayRow({2}, {true});
    auto batch1 = BuildBinaryArray(
        {skip0, row0, std::nullopt, empty, skip1});

    auto skip2 = BuildElementNullableIntArrayRow({3}, {true});
    auto row3 = BuildElementNullableIntArrayRow({0, 30}, {false, true});
    auto row4 = BuildElementNullableIntArrayRow({40}, {true});
    auto skip3 = BuildElementNullableIntArrayRow({4}, {true});
    auto batch2 = BuildBinaryArray({skip2, row3, row4, skip3});

    auto sliced1 =
        std::static_pointer_cast<arrow::BinaryArray>(batch1->Slice(1, 3));
    auto sliced2 =
        std::static_pointer_cast<arrow::BinaryArray>(batch2->Slice(1, 2));
    arrow::ArrayVector arrays{sliced1, sliced2};

    ArrayChunkWriter writer(DataType::INT32, true, true);
    auto [calculated_size, row_count] = writer.calculate_size(arrays);
    ASSERT_EQ(row_count, 5);
    auto target = std::make_shared<MemChunkTarget>(calculated_size);
    writer.write_to_target(arrays, target);

    ArrayChunk chunk(row_count,
                     target->release(),
                     calculated_size,
                     DataType::INT32,
                     true,
                     true,
                     nullptr);
    auto [views, row_valid] = chunk.Views();
    ASSERT_EQ(views.size(), 5);
    ASSERT_EQ(row_valid.size(), 5);
    EXPECT_TRUE(row_valid[0]);
    EXPECT_FALSE(row_valid[1]);
    EXPECT_TRUE(row_valid[2]);
    EXPECT_TRUE(row_valid[3]);
    EXPECT_TRUE(row_valid[4]);

    ASSERT_EQ(views[0].length(), 2);
    EXPECT_TRUE(views[0].is_element_valid(0));
    EXPECT_FALSE(views[0].is_element_valid(1));
    EXPECT_EQ(views[0].get_data_unchecked<int32_t>(0), 10);
    EXPECT_EQ(views[1].length(), 0);
    EXPECT_EQ(views[2].length(), 0);
    ASSERT_EQ(views[3].length(), 2);
    EXPECT_FALSE(views[3].is_element_valid(0));
    EXPECT_TRUE(views[3].is_element_valid(1));
    EXPECT_EQ(views[3].get_data_unchecked<int32_t>(1), 30);
    ASSERT_EQ(views[4].length(), 1);
    EXPECT_TRUE(views[4].is_element_valid(0));
    EXPECT_EQ(views[4].get_data_unchecked<int32_t>(0), 40);
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
