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
#include <cstddef>
#include "milvus-storage/common/metadata.h"
#include "segcore/memory_planner.h"
#include <gtest/gtest.h>
#include <vector>

using namespace milvus::segcore;

milvus_storage::RowGroupMetadataVector
CreateRowGroupMetadataVector(const std::vector<int64_t>& memory_sizes_mb) {
    std::vector<milvus_storage::RowGroupMetadata> metadatas;
    for (auto size : memory_sizes_mb) {
        metadatas.emplace_back(
            size * 1024 * 1024, 0, 0);  // Convert MB to bytes
    }
    return milvus_storage::RowGroupMetadataVector(metadatas);
}

TEST(MemoryBasedSplitStrategy, EmptyInput) {
    std::vector<int64_t> empty_input;
    auto metadatas = CreateRowGroupMetadataVector({});
    auto strategy = std::make_unique<MemoryBasedSplitStrategy>(metadatas);
    auto result = strategy->split(empty_input);
    EXPECT_TRUE(result.empty());
}

TEST(MemoryBasedSplitStrategy, SingleRowGroup) {
    std::vector<int64_t> input = {0};
    auto metadatas = CreateRowGroupMetadataVector({12});  // 12MB
    auto strategy = std::make_unique<MemoryBasedSplitStrategy>(metadatas);
    auto result = strategy->split(input);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (RowGroupBlock{0, 1}));
}

TEST(MemoryBasedSplitStrategy, ContinuousWithinMemoryLimit) {
    std::vector<int64_t> input = {0, 1, 2, 3};
    auto metadatas = CreateRowGroupMetadataVector({
        2, 2, 2, 2  // Total 8MB, well within the 16MB limit
    });
    auto strategy = std::make_unique<MemoryBasedSplitStrategy>(metadatas);
    auto result = strategy->split(input);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (RowGroupBlock{0, 4}));
}

TEST(MemoryBasedSplitStrategy, ContinuousExceedMemoryLimit) {
    std::vector<int64_t> input = {0, 1, 2, 3, 4};
    auto metadatas = CreateRowGroupMetadataVector({
        8, 6, 4, 5, 3  // Different sizes: 8MB, 6MB, 4MB, 5MB, 3MB
    });
    auto strategy = std::make_unique<MemoryBasedSplitStrategy>(metadatas);
    auto result = strategy->split(input);
    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], (RowGroupBlock{0, 2}));
    EXPECT_EQ(result[1], (RowGroupBlock{2, 3}));
}

TEST(MemoryBasedSplitStrategy, NonContinuous_SmallGap) {
    std::vector<int64_t> input = {0, 2, 4, 6};
    auto metadatas = CreateRowGroupMetadataVector({
        5, 3, 4, 6, 2, 7, 4  // Different sizes for each row group
    });
    auto strategy = std::make_unique<MemoryBasedSplitStrategy>(metadatas);
    auto result = strategy->split(input);
    EXPECT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], (RowGroupBlock{0, 1}));
    EXPECT_EQ(result[1], (RowGroupBlock{2, 1}));
    EXPECT_EQ(result[2], (RowGroupBlock{4, 1}));
    EXPECT_EQ(result[3], (RowGroupBlock{6, 1}));
}

TEST(MemoryBasedSplitStrategy, Mixed_ContinuousAndNonContinuous) {
    std::vector<int64_t> input = {0, 1, 3, 4, 5, 7};
    auto metadatas = CreateRowGroupMetadataVector({
        4, 3, 2, 5, 4, 3, 6, 4  // Different sizes for each row group
    });
    auto strategy = std::make_unique<MemoryBasedSplitStrategy>(metadatas);
    auto result = strategy->split(input);
    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], (RowGroupBlock{0, 2}));
    EXPECT_EQ(result[1], (RowGroupBlock{3, 3}));
    EXPECT_EQ(result[2], (RowGroupBlock{7, 1}));
}

TEST(MemoryBasedSplitStrategy, EdgeCase_SingleBlock) {
    std::vector<int64_t> input = {0, 1, 2, 3, 4};
    auto metadatas = CreateRowGroupMetadataVector({
        2, 3, 4, 3, 2  // Different sizes for each row group
    });
    auto strategy = std::make_unique<MemoryBasedSplitStrategy>(metadatas);
    auto result = strategy->split(input);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (RowGroupBlock{0, 5}));
}

TEST(MemoryBasedSplitStrategy, EdgeCase_EachRowGroupSeparate) {
    std::vector<int64_t> input = {0, 2, 4, 6, 8};
    auto metadatas = CreateRowGroupMetadataVector({
        7, 5, 6, 4, 8, 3, 9, 5, 6  // Different sizes for each row group
    });
    auto strategy = std::make_unique<MemoryBasedSplitStrategy>(metadatas);
    auto result = strategy->split(input);
    EXPECT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_EQ(result[i], (RowGroupBlock{input[i], 1}));
    }
}

TEST(ParallelDegreeSplitStrategy, Basic) {
    std::vector<int64_t> input = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    auto strategy = std::make_unique<ParallelDegreeSplitStrategy>(3);
    auto blocks = strategy->split(input);

    EXPECT_EQ(blocks.size(), 3);
    EXPECT_EQ(blocks[0], (RowGroupBlock{0, 4}));
    EXPECT_EQ(blocks[1], (RowGroupBlock{4, 4}));
    EXPECT_EQ(blocks[2], (RowGroupBlock{8, 2}));
}

TEST(ParallelDegreeSplitStrategy, LargerThanInput) {
    std::vector<int64_t> input = {0, 1, 2, 3, 4};
    auto strategy = std::make_unique<ParallelDegreeSplitStrategy>(10);
    auto blocks = strategy->split(input);

    EXPECT_EQ(blocks.size(), 1);
    EXPECT_EQ(blocks[0], (RowGroupBlock{0, 5}));
}

TEST(ParallelDegreeSplitStrategy, Empty) {
    std::vector<int64_t> input;
    auto strategy = std::make_unique<ParallelDegreeSplitStrategy>(3);
    auto blocks = strategy->split(input);
    EXPECT_TRUE(blocks.empty());
}

TEST(ParallelDegreeSplitStrategy, MixedContinuousAndNonContinuous) {
    std::vector<int64_t> input = {1, 2, 3, 5, 6, 7, 9, 10};
    auto strategy = std::make_unique<ParallelDegreeSplitStrategy>(2);
    auto blocks = strategy->split(input);

    EXPECT_EQ(blocks.size(), 3);  // Three continuous blocks
    EXPECT_EQ(blocks[0], (RowGroupBlock{1, 3}));
    EXPECT_EQ(blocks[1], (RowGroupBlock{5, 3}));
    EXPECT_EQ(blocks[2], (RowGroupBlock{9, 2}));
}

TEST(ParallelDegreeSplitStrategy, ContinuousExceedingAvgSize) {
    std::vector<int64_t> input = {1, 2, 3, 4, 5, 6, 7, 8};
    auto strategy = std::make_unique<ParallelDegreeSplitStrategy>(2);
    auto blocks = strategy->split(input);

    EXPECT_EQ(blocks.size(), 2);
    EXPECT_EQ(blocks[0], (RowGroupBlock{1, 4}));
    EXPECT_EQ(blocks[1], (RowGroupBlock{5, 4}));
}