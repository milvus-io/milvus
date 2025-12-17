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

#include "common/VectorArray.h"
#include "segcore/ConcurrentVectorArray.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

milvus::VectorArray
CreateFloatVectorArray(int num_vectors, int64_t dim, float start_value = 0.0f) {
    std::vector<float> data(num_vectors * dim);
    for (int i = 0; i < num_vectors * dim; ++i) {
        data[i] = start_value + static_cast<float>(i);
    }
    return milvus::VectorArray(
        data.data(), num_vectors, dim, milvus::DataType::VECTOR_FLOAT);
}

}  // namespace

class ConcurrentVectorArrayTest : public ::testing::Test {
 protected:
    static constexpr int64_t kDim = 4;
    static constexpr DataType kElementType = DataType::VECTOR_FLOAT;
    static constexpr int64_t kSizePerChunk = 3;
};

TEST_F(ConcurrentVectorArrayTest, Construction) {
    ConcurrentVectorArray cva(kDim, kElementType, kSizePerChunk);
    EXPECT_TRUE(cva.empty());
    EXPECT_EQ(cva.num_chunk(), 0);
}

TEST_F(ConcurrentVectorArrayTest, SingleChunkWrite) {
    ConcurrentVectorArray cva(kDim, kElementType, kSizePerChunk);

    std::vector<milvus::VectorArray> data;
    data.push_back(CreateFloatVectorArray(3, kDim, 0.0f));
    data.push_back(CreateFloatVectorArray(5, kDim, 100.0f));

    cva.set_data_raw(0, data.data(), 2);

    EXPECT_FALSE(cva.empty());
    EXPECT_EQ(cva.num_chunk(), 1);

    auto accessor = cva.get_chunk_data(0);
    auto chunk_data = static_cast<const float*>(accessor->data());
    EXPECT_EQ(chunk_data[0], 0.0f);
    EXPECT_EQ(chunk_data[12], 100.0f);
}

TEST_F(ConcurrentVectorArrayTest, MultipleChunksWrite) {
    ConcurrentVectorArray cva(kDim, kElementType, kSizePerChunk);

    // 5 VectorArrays, each with 2 vectors of dim=4
    // kSizePerChunk=3, so chunk0 has rows 0,1,2 (6 vectors), chunk1 has rows 3,4 (4 vectors)
    std::vector<milvus::VectorArray> data;
    for (int i = 0; i < 5; ++i) {
        data.push_back(
            CreateFloatVectorArray(2, kDim, static_cast<float>(i * 100)));
    }

    cva.set_data_raw(0, data.data(), 5);

    EXPECT_EQ(cva.num_chunk(), 2);

    // Verify chunk 0: rows 0,1,2 → 6 vectors × 4 floats = 24 floats
    // row 0: start=0, row 1: start=100, row 2: start=200
    auto accessor0 = cva.get_chunk_data(0);
    auto chunk0 = static_cast<const float*>(accessor0->data());
    for (int row = 0; row < 3; ++row) {
        float start = static_cast<float>(row * 100);
        for (int i = 0; i < 2 * kDim; ++i) {
            EXPECT_FLOAT_EQ(chunk0[row * 2 * kDim + i], start + i)
                << "chunk0 mismatch at row=" << row << ", i=" << i;
        }
    }

    // Verify chunk 1: rows 3,4 → 4 vectors × 4 floats = 16 floats
    // row 3: start=300, row 4: start=400
    auto accessor1 = cva.get_chunk_data(1);
    auto chunk1 = static_cast<const float*>(accessor1->data());
    for (int row = 0; row < 2; ++row) {
        float start = static_cast<float>((row + 3) * 100);
        for (int i = 0; i < 2 * kDim; ++i) {
            EXPECT_FLOAT_EQ(chunk1[row * 2 * kDim + i], start + i)
                << "chunk1 mismatch at row=" << row << ", i=" << i;
        }
    }
}

TEST_F(ConcurrentVectorArrayTest, IncrementalWrite) {
    ConcurrentVectorArray cva(kDim, kElementType, kSizePerChunk);

    std::vector<milvus::VectorArray> batch1;
    batch1.push_back(CreateFloatVectorArray(2, kDim, 0.0f));
    batch1.push_back(CreateFloatVectorArray(3, kDim, 100.0f));
    cva.set_data_raw(0, batch1.data(), 2);

    EXPECT_EQ(cva.num_chunk(), 1);

    std::vector<milvus::VectorArray> batch2;
    batch2.push_back(CreateFloatVectorArray(1, kDim, 200.0f));
    batch2.push_back(CreateFloatVectorArray(4, kDim, 300.0f));
    cva.set_data_raw(2, batch2.data(), 2);

    EXPECT_EQ(cva.num_chunk(), 2);
}

TEST_F(ConcurrentVectorArrayTest, GetElementOffset) {
    ConcurrentVectorArray cva(kDim, kElementType, kSizePerChunk);

    EXPECT_EQ(cva.get_element_offset(0), 0);
    EXPECT_EQ(cva.get_element_offset(1), kSizePerChunk);
    EXPECT_EQ(cva.get_element_offset(2), 2 * kSizePerChunk);
}

TEST_F(ConcurrentVectorArrayTest, Clear) {
    ConcurrentVectorArray cva(kDim, kElementType, kSizePerChunk);

    std::vector<milvus::VectorArray> data;
    data.push_back(CreateFloatVectorArray(2, kDim));
    cva.set_data_raw(0, data.data(), 1);

    EXPECT_FALSE(cva.empty());
    cva.clear();
    EXPECT_TRUE(cva.empty());
    EXPECT_EQ(cva.num_chunk(), 0);

    cva.set_data_raw(0, data.data(), 1);
    EXPECT_EQ(cva.num_chunk(), 1);
}

TEST_F(ConcurrentVectorArrayTest, DataIntegrity) {
    ConcurrentVectorArray cva(kDim, kElementType, kSizePerChunk);

    std::vector<milvus::VectorArray> data;
    data.push_back(CreateFloatVectorArray(2, kDim, 1.0f));

    cva.set_data_raw(0, data.data(), 1);

    auto accessor = cva.get_chunk_data(0);
    auto chunk_data = static_cast<const float*>(accessor->data());
    for (int i = 0; i < 2 * kDim; ++i) {
        EXPECT_FLOAT_EQ(chunk_data[i], 1.0f + static_cast<float>(i));
    }
}

TEST_F(ConcurrentVectorArrayTest, ChunkOffsets) {
    ConcurrentVectorArray cva(kDim, kElementType, kSizePerChunk);

    // 6 VectorArrays with varying vector counts: 1,2,3,4,5,6
    // kSizePerChunk=3, so:
    //   chunk0: rows 0,1,2 with 1,2,3 vectors → offsets = [0,1,3,6]
    //   chunk1: rows 3,4,5 with 4,5,6 vectors → offsets = [0,4,9,15]
    std::vector<milvus::VectorArray> data;
    for (int i = 0; i < 6; ++i) {
        data.push_back(CreateFloatVectorArray(i + 1, kDim));
    }
    cva.set_data_raw(0, data.data(), 6);

    // Verify chunk 0 offsets
    auto offsets0 = cva.get_chunk_offsets(0);
    EXPECT_EQ(offsets0[0], 0);
    EXPECT_EQ(offsets0[1], 1);  // row 0: 1 vector
    EXPECT_EQ(offsets0[2], 3);  // row 1: 2 vectors, cumsum = 1+2
    EXPECT_EQ(offsets0[3], 6);  // row 2: 3 vectors, cumsum = 1+2+3

    // Verify chunk 1 offsets
    auto offsets1 = cva.get_chunk_offsets(1);
    EXPECT_EQ(offsets1[0], 0);
    EXPECT_EQ(offsets1[1], 4);   // row 3: 4 vectors
    EXPECT_EQ(offsets1[2], 9);   // row 4: 5 vectors, cumsum = 4+5
    EXPECT_EQ(offsets1[3], 15);  // row 5: 6 vectors, cumsum = 4+5+6
}
