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

    std::shared_ptr<arrow::ListArray>
    createFloatVectorListArray(const std::vector<float>& data,
                               const std::vector<int32_t>& offsets) {
        auto float_builder = std::make_shared<arrow::FloatBuilder>();
        auto list_builder = std::make_shared<arrow::ListBuilder>(
            arrow::default_memory_pool(), float_builder);

        arrow::Status ast;
        for (size_t i = 0; i < offsets.size() - 1; ++i) {
            ast = list_builder->Append();
            assert(ast.ok());
            int32_t start = offsets[i];
            int32_t end = offsets[i + 1];

            for (int32_t j = start; j < end; ++j) {
                float_builder->Append(data[j]);
            }
        }

        std::shared_ptr<arrow::Array> array;
        ast = list_builder->Finish(&array);
        assert(ast.ok());
        return std::static_pointer_cast<arrow::ListArray>(array);
    }
};

TEST_F(VectorArrayChunkTest, TestWriteFloatVectorArray) {
    // Test parameters
    const int64_t dim = 128;
    const int num_rows = 100;
    const int vectors_per_row = 5;  // Each row contains 5 vectors

    // Generate test data
    std::vector<float> all_data;
    std::vector<int32_t> offsets = {0};

    for (int row = 0; row < num_rows; ++row) {
        auto row_data = generateFloatVector(row, vectors_per_row, dim);
        all_data.insert(all_data.end(), row_data.begin(), row_data.end());
        offsets.push_back(offsets.back() + vectors_per_row * dim);
    }

    // Create Arrow ListArray
    auto list_array = createFloatVectorListArray(all_data, offsets);
    arrow::ArrayVector array_vec = {list_array};

    // Test VectorArrayChunkWriter
    VectorArrayChunkWriter writer(dim, DataType::VECTOR_FLOAT);
    writer.write(array_vec);

    auto chunk = writer.finish();
    auto vector_array_chunk = static_cast<VectorArrayChunk*>(chunk.get());

    // Verify results
    EXPECT_EQ(vector_array_chunk->RowNums(), num_rows);

    // Verify data integrity using View method
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
}

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
            createFloatVectorListArray(batch_data, batch_offsets));
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

    auto list_array = createFloatVectorListArray(all_data, offsets);
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
