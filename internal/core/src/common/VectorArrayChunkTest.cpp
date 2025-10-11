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
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <random>
#include <vector>
#include <chrono>

#include "common/ChunkWriter.h"
#include "common/Chunk.h"
#include "common/VectorArray.h"
#include "common/Types.h"
#include "pb/schema.pb.h"
#include "test_utils/DataGen.h"

using namespace milvus;

class VectorArrayChunkTest : public ::testing::Test {
 protected:
    std::vector<float>
    generateFloatVector(int64_t seed, int64_t N, int64_t dim) {
        std::vector<float> result(dim * N);
        std::default_random_engine gen(seed);
        std::normal_distribution<float> dist(0.0f, 1.0f);

        for (int64_t i = 0; i < dim * N; ++i) {
            result[i] = dist(gen);
        }
        return result;
    }

    std::vector<uint16_t>
    generateFloat16Vector(int64_t seed, int64_t N, int64_t dim) {
        std::vector<uint16_t> result(dim * N);
        std::default_random_engine gen(seed);
        std::uniform_int_distribution<uint16_t> dist(0, 65535);

        for (int64_t i = 0; i < dim * N; ++i) {
            result[i] = dist(gen);
        }
        return result;
    }

    std::vector<uint16_t>
    generateBFloat16Vector(int64_t seed, int64_t N, int64_t dim) {
        // Same as Float16 for testing purposes
        return generateFloat16Vector(seed, N, dim);
    }

    std::vector<int8_t>
    generateInt8Vector(int64_t seed, int64_t N, int64_t dim) {
        std::vector<int8_t> result(dim * N);
        std::default_random_engine gen(seed);
        std::uniform_int_distribution<int> dist(-128, 127);

        for (int64_t i = 0; i < dim * N; ++i) {
            result[i] = static_cast<int8_t>(dist(gen));
        }
        return result;
    }

    std::vector<uint8_t>
    generateBinaryVector(int64_t seed, int64_t N, int64_t dim) {
        std::vector<uint8_t> result((dim * N + 7) / 8);
        std::default_random_engine gen(seed);
        std::uniform_int_distribution<int> dist(0, 255);

        for (size_t i = 0; i < result.size(); ++i) {
            result[i] = static_cast<uint8_t>(dist(gen));
        }
        return result;
    }

    std::shared_ptr<arrow::ListArray>
    createFloatVectorListArray(const std::vector<float>& data,
                               const std::vector<int32_t>& offsets,
                               int64_t dim) {
        int byte_width = dim * sizeof(float);
        auto value_builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(
            arrow::fixed_size_binary(byte_width));
        auto list_builder = std::make_shared<arrow::ListBuilder>(
            arrow::default_memory_pool(), value_builder);

        arrow::Status ast;
        for (size_t i = 0; i < offsets.size() - 1; ++i) {
            ast = list_builder->Append();
            assert(ast.ok());
            int32_t start = offsets[i];
            int32_t end = offsets[i + 1];

            // Each vector is dim floats
            for (int32_t j = start; j < end; j += dim) {
                // Convert float vector to binary
                const uint8_t* binary_data =
                    reinterpret_cast<const uint8_t*>(&data[j]);
                ast = value_builder->Append(binary_data);
                assert(ast.ok());
            }
        }

        std::shared_ptr<arrow::Array> array;
        ast = list_builder->Finish(&array);
        assert(ast.ok());
        return std::static_pointer_cast<arrow::ListArray>(array);
    }

    std::shared_ptr<arrow::ListArray>
    createFloat16VectorListArray(const std::vector<uint16_t>& data,
                                 const std::vector<int32_t>& offsets,
                                 int64_t dim) {
        int byte_width = dim * 2;  // 2 bytes per float16
        auto value_builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(
            arrow::fixed_size_binary(byte_width));
        auto list_builder = std::make_shared<arrow::ListBuilder>(
            arrow::default_memory_pool(), value_builder);

        arrow::Status ast;
        for (size_t i = 0; i < offsets.size() - 1; ++i) {
            ast = list_builder->Append();
            assert(ast.ok());
            int32_t start = offsets[i];
            int32_t end = offsets[i + 1];

            for (int32_t j = start; j < end; j += dim) {
                const uint8_t* binary_data =
                    reinterpret_cast<const uint8_t*>(&data[j]);
                ast = value_builder->Append(binary_data);
                assert(ast.ok());
            }
        }

        std::shared_ptr<arrow::Array> array;
        ast = list_builder->Finish(&array);
        assert(ast.ok());
        return std::static_pointer_cast<arrow::ListArray>(array);
    }

    std::shared_ptr<arrow::ListArray>
    createBFloat16VectorListArray(const std::vector<uint16_t>& data,
                                  const std::vector<int32_t>& offsets,
                                  int64_t dim) {
        // Same as Float16 but for bfloat16
        return createFloat16VectorListArray(data, offsets, dim);
    }

    std::shared_ptr<arrow::ListArray>
    createInt8VectorListArray(const std::vector<int8_t>& data,
                              const std::vector<int32_t>& offsets,
                              int64_t dim) {
        int byte_width = dim;  // 1 byte per int8
        auto value_builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(
            arrow::fixed_size_binary(byte_width));
        auto list_builder = std::make_shared<arrow::ListBuilder>(
            arrow::default_memory_pool(), value_builder);

        arrow::Status ast;
        for (size_t i = 0; i < offsets.size() - 1; ++i) {
            ast = list_builder->Append();
            assert(ast.ok());
            int32_t start = offsets[i];
            int32_t end = offsets[i + 1];

            for (int32_t j = start; j < end; j += dim) {
                const uint8_t* binary_data =
                    reinterpret_cast<const uint8_t*>(&data[j]);
                ast = value_builder->Append(binary_data);
                assert(ast.ok());
            }
        }

        std::shared_ptr<arrow::Array> array;
        ast = list_builder->Finish(&array);
        assert(ast.ok());
        return std::static_pointer_cast<arrow::ListArray>(array);
    }

    std::shared_ptr<arrow::ListArray>
    createBinaryVectorListArray(const std::vector<uint8_t>& data,
                                const std::vector<int32_t>& offsets,
                                int64_t dim) {
        int byte_width = (dim + 7) / 8;  // bits packed into bytes
        auto value_builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(
            arrow::fixed_size_binary(byte_width));
        auto list_builder = std::make_shared<arrow::ListBuilder>(
            arrow::default_memory_pool(), value_builder);

        arrow::Status ast;
        for (size_t i = 0; i < offsets.size() - 1; ++i) {
            ast = list_builder->Append();
            assert(ast.ok());
            int32_t start = offsets[i];
            int32_t end = offsets[i + 1];

            for (int32_t j = start; j < end; j += byte_width) {
                ast = value_builder->Append(&data[j]);
                assert(ast.ok());
            }
        }

        std::shared_ptr<arrow::Array> array;
        ast = list_builder->Finish(&array);
        assert(ast.ok());
        return std::static_pointer_cast<arrow::ListArray>(array);
    }
};

TEST_F(VectorArrayChunkTest, TestWriteMultipleBatches) {
    const int64_t dim = 64;
    const int batch_size = 50;
    const int num_batches = 3;
    const int vectors_per_row = 3;

    arrow::ArrayVector array_vec;
    std::vector<std::vector<float>> all_batch_data;

    // Create multiple batches
    for (int batch = 0; batch < num_batches; ++batch) {
        std::vector<float> batch_data;
        std::vector<int32_t> batch_offsets = {0};

        for (int row = 0; row < batch_size; ++row) {
            auto row_data =
                generateFloatVector(batch * 1000 + row, vectors_per_row, dim);
            batch_data.insert(
                batch_data.end(), row_data.begin(), row_data.end());
            batch_offsets.push_back(batch_offsets.back() +
                                    vectors_per_row * dim);
        }

        all_batch_data.push_back(batch_data);
        array_vec.push_back(
            createFloatVectorListArray(batch_data, batch_offsets, dim));
    }

    // Write using VectorArrayChunkWriter
    VectorArrayChunkWriter writer(dim, DataType::VECTOR_FLOAT);
    writer.write(array_vec);

    auto chunk = writer.finish();
    auto vector_array_chunk = static_cast<VectorArrayChunk*>(chunk.get());

    // Verify total rows
    EXPECT_EQ(vector_array_chunk->RowNums(), batch_size * num_batches);

    // Verify data from each batch using View method
    for (int batch = 0; batch < num_batches; ++batch) {
        for (int row = 0; row < batch_size; ++row) {
            int global_row = batch * batch_size + row;
            auto view = vector_array_chunk->View(global_row);

            // Verify by converting back to VectorFieldProto and checking
            auto proto = view.output_data();
            EXPECT_EQ(proto.dim(), dim);
            EXPECT_EQ(proto.float_vector().data_size(), vectors_per_row * dim);

            const float* expected =
                all_batch_data[batch].data() + row * vectors_per_row * dim;
            for (int i = 0; i < vectors_per_row * dim; ++i) {
                EXPECT_FLOAT_EQ(proto.float_vector().data(i), expected[i]);
            }
        }
    }
}

TEST_F(VectorArrayChunkTest, TestWriteWithMmap) {
    const int64_t dim = 128;
    const int num_rows = 100;
    const int vectors_per_row = 4;

    // Create temp file path
    std::string temp_file =
        "/tmp/test_vector_array_chunk_" +
        std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());

    // Generate test data
    std::vector<float> all_data;
    std::vector<int32_t> offsets = {0};

    for (int row = 0; row < num_rows; ++row) {
        auto row_data = generateFloatVector(row, vectors_per_row, dim);
        all_data.insert(all_data.end(), row_data.begin(), row_data.end());
        offsets.push_back(offsets.back() + vectors_per_row * dim);
    }

    auto list_array = createFloatVectorListArray(all_data, offsets, dim);
    arrow::ArrayVector array_vec = {list_array};

    // Write with mmap
    VectorArrayChunkWriter writer(dim, DataType::VECTOR_FLOAT, temp_file);
    writer.write(array_vec);

    auto chunk = writer.finish();
    auto vector_array_chunk = static_cast<VectorArrayChunk*>(chunk.get());

    // Verify mmap write
    EXPECT_EQ(vector_array_chunk->RowNums(), num_rows);

    for (int row = 0; row < num_rows; ++row) {
        auto view = vector_array_chunk->View(row);

        // Verify by converting back to VectorFieldProto and checking
        auto proto = view.output_data();
        EXPECT_EQ(proto.dim(), dim);
        EXPECT_EQ(proto.float_vector().data_size(), vectors_per_row * dim);

        const float* expected = all_data.data() + row * vectors_per_row * dim;
        for (int i = 0; i < vectors_per_row * dim; ++i) {
            EXPECT_FLOAT_EQ(proto.float_vector().data(i), expected[i]);
        }
    }

    // Clean up
    std::remove(temp_file.c_str());
}

TEST_F(VectorArrayChunkTest, TestEmptyVectorArray) {
    const int64_t dim = 128;

    arrow::ArrayVector array_vec;

    VectorArrayChunkWriter writer(dim, DataType::VECTOR_FLOAT);
    writer.write(array_vec);

    auto chunk = writer.finish();
    auto vector_array_chunk = static_cast<VectorArrayChunk*>(chunk.get());

    EXPECT_EQ(vector_array_chunk->RowNums(), 0);
}

struct VectorArrayTestParam {
    DataType data_type;
    int64_t dim;
    int num_rows;
    int vectors_per_row;
    std::string test_name;
};

class VectorArrayChunkParameterizedTest
    : public VectorArrayChunkTest,
      public ::testing::WithParamInterface<VectorArrayTestParam> {};

template <typename T>
std::shared_ptr<arrow::ListArray>
createVectorListArray(const std::vector<T>& data,
                      const std::vector<int32_t>& offsets,
                      int64_t dim,
                      DataType dtype) {
    int byte_width;
    switch (dtype) {
        case DataType::VECTOR_FLOAT:
            byte_width = dim * sizeof(float);
            break;
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
            byte_width = dim * 2;
            break;
        case DataType::VECTOR_INT8:
            byte_width = dim;
            break;
        case DataType::VECTOR_BINARY:
            byte_width = (dim + 7) / 8;
            break;
        default:
            throw std::invalid_argument("Unsupported data type");
    }

    auto value_builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(
        arrow::fixed_size_binary(byte_width));
    auto list_builder = std::make_shared<arrow::ListBuilder>(
        arrow::default_memory_pool(), value_builder);

    arrow::Status ast;
    int element_size = (dtype == DataType::VECTOR_BINARY) ? byte_width : dim;

    for (size_t i = 0; i < offsets.size() - 1; ++i) {
        ast = list_builder->Append();
        assert(ast.ok());
        int32_t start = offsets[i];
        int32_t end = offsets[i + 1];

        for (int32_t j = start; j < end; j += element_size) {
            const uint8_t* binary_data =
                reinterpret_cast<const uint8_t*>(&data[j]);
            ast = value_builder->Append(binary_data);
            assert(ast.ok());
        }
    }

    std::shared_ptr<arrow::Array> array;
    ast = list_builder->Finish(&array);
    assert(ast.ok());
    return std::static_pointer_cast<arrow::ListArray>(array);
}

TEST_P(VectorArrayChunkParameterizedTest, TestWriteVectorArray) {
    auto param = GetParam();

    // Generate test data based on type
    std::vector<uint8_t> all_data;
    std::vector<int32_t> offsets = {0};

    for (int row = 0; row < param.num_rows; ++row) {
        std::vector<uint8_t> row_data;

        switch (param.data_type) {
            case DataType::VECTOR_FLOAT: {
                auto float_data =
                    generateFloatVector(row, param.vectors_per_row, param.dim);
                row_data.resize(float_data.size() * sizeof(float));
                memcpy(row_data.data(), float_data.data(), row_data.size());
                break;
            }
            case DataType::VECTOR_FLOAT16:
            case DataType::VECTOR_BFLOAT16: {
                auto uint16_data = generateFloat16Vector(
                    row, param.vectors_per_row, param.dim);
                row_data.resize(uint16_data.size() * sizeof(uint16_t));
                memcpy(row_data.data(), uint16_data.data(), row_data.size());
                break;
            }
            case DataType::VECTOR_INT8: {
                auto int8_data =
                    generateInt8Vector(row, param.vectors_per_row, param.dim);
                row_data.resize(int8_data.size());
                memcpy(row_data.data(), int8_data.data(), row_data.size());
                break;
            }
            case DataType::VECTOR_BINARY: {
                row_data =
                    generateBinaryVector(row, param.vectors_per_row, param.dim);
                break;
            }
            default:
                FAIL() << "Unsupported data type";
        }

        all_data.insert(all_data.end(), row_data.begin(), row_data.end());

        // Calculate offset based on data type
        int offset_increment;
        if (param.data_type == DataType::VECTOR_BINARY) {
            offset_increment = param.vectors_per_row * ((param.dim + 7) / 8);
        } else if (param.data_type == DataType::VECTOR_FLOAT) {
            offset_increment =
                param.vectors_per_row * param.dim * sizeof(float);
        } else if (param.data_type == DataType::VECTOR_FLOAT16 ||
                   param.data_type == DataType::VECTOR_BFLOAT16) {
            offset_increment = param.vectors_per_row * param.dim * 2;
        } else {
            offset_increment = param.vectors_per_row * param.dim;
        }
        offsets.push_back(offsets.back() + offset_increment);
    }

    // Create Arrow ListArray
    auto list_array =
        createVectorListArray(all_data, offsets, param.dim, param.data_type);
    arrow::ArrayVector array_vec = {list_array};

    // Test VectorArrayChunkWriter
    VectorArrayChunkWriter writer(param.dim, param.data_type);
    writer.write(array_vec);

    auto chunk = writer.finish();
    auto vector_array_chunk = static_cast<VectorArrayChunk*>(chunk.get());

    // Verify results
    EXPECT_EQ(vector_array_chunk->RowNums(), param.num_rows);

    // Basic verification - ensure View doesn't crash and returns valid data
    for (int row = 0; row < param.num_rows; ++row) {
        auto view = vector_array_chunk->View(row);
        auto proto = view.output_data();
        EXPECT_EQ(proto.dim(), param.dim);

        // Verify the correct field is populated based on data type
        switch (param.data_type) {
            case DataType::VECTOR_FLOAT:
                EXPECT_GT(proto.float_vector().data_size(), 0);
                break;
            case DataType::VECTOR_FLOAT16:
                EXPECT_GT(proto.float16_vector().size(), 0);
                break;
            case DataType::VECTOR_BFLOAT16:
                EXPECT_GT(proto.bfloat16_vector().size(), 0);
                break;
            case DataType::VECTOR_INT8:
                EXPECT_GT(proto.int8_vector().size(), 0);
                break;
            case DataType::VECTOR_BINARY:
                EXPECT_GT(proto.binary_vector().size(), 0);
                break;
            default:
                break;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    VectorTypes,
    VectorArrayChunkParameterizedTest,
    ::testing::Values(
        VectorArrayTestParam{
            DataType::VECTOR_FLOAT, 128, 100, 5, "FloatVector"},
        VectorArrayTestParam{
            DataType::VECTOR_FLOAT16, 128, 100, 3, "Float16Vector"},
        VectorArrayTestParam{
            DataType::VECTOR_BFLOAT16, 64, 50, 2, "BFloat16Vector"},
        VectorArrayTestParam{DataType::VECTOR_INT8, 256, 80, 4, "Int8Vector"},
        VectorArrayTestParam{
            DataType::VECTOR_BINARY, 512, 60, 3, "BinaryVector"}),
    [](const testing::TestParamInfo<VectorArrayTestParam>& info) {
        return info.param.test_name;
    });
