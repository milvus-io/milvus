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
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include <folly/CancellationToken.h>
#include "common/EasyAssert.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/metadata.h"
#include "segcore/memory_planner.h"

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

// ---- LoadCellBatchAsync tests ----

namespace {

// Helper: create a BatchReaderFactory that returns empty Arrow tables.
// Records (batch_key, rg_offset, total_rg_count) for each call.
BatchReaderFactory
MakeMockReaderFactory() {
    return [](size_t /*batch_key*/,
              int64_t /*rg_offset*/,
              int64_t total_rg_count,
              int64_t /*reader_memory_limit*/)
               -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
        // Return one empty table per row group
        auto schema = arrow::schema({arrow::field("x", arrow::int64())});
        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (int64_t i = 0; i < total_rg_count; ++i) {
            tables.push_back(arrow::Table::MakeEmpty(schema).ValueOrDie());
        }
        return tables;
    };
}

// Helper to run LoadCellBatchAsync and return future count (= batch count).
size_t
RunAndCountBatches(std::vector<CellSpec> cell_specs, int64_t memory_limit) {
    auto channel = std::make_shared<CellReaderChannel>(64);
    auto futures = LoadCellBatchAsync(nullptr,
                                      std::move(cell_specs),
                                      MakeMockReaderFactory(),
                                      channel,
                                      memory_limit);
    for (auto& f : futures) {
        f.get();
    }
    return futures.size();
}

}  // namespace

TEST(LoadCellBatchAsync, MemoryBasedSplit) {
    // 10 cells x 64MB each, limit=128MB -> 5 batches (2 cells each)
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs;
    for (int i = 0; i < 10; ++i) {
        specs.push_back({/*cid=*/i,
                         /*file_idx=*/0,
                         /*local_rg_offset=*/i,
                         /*rg_count=*/1,
                         /*memory_size=*/64 * MB});
    }
    auto batches = RunAndCountBatches(specs, 128 * MB);
    EXPECT_EQ(batches, 5);
}

TEST(LoadCellBatchAsync, ZeroMemorySizeAsserts) {
    // memory_size=0 must trigger assertion failure
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs = {{/*cid=*/0,
                                    /*file_idx=*/0,
                                    /*local_rg_offset=*/0,
                                    /*rg_count=*/1,
                                    /*memory_size=*/0}};
    EXPECT_ANY_THROW(RunAndCountBatches(specs, 128 * MB));
}

TEST(LoadCellBatchAsync, UnevenCellSizes) {
    // Cells with different memory sizes are batched by memory limit
    // 2 small cells (16MB each) fit in one batch, 1 large cell (96MB) alone
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs = {
        {0, 0, 0, 1, 16 * MB},
        {1, 0, 1, 1, 16 * MB},
        {2, 0, 2, 1, 96 * MB},
        {3, 0, 3, 1, 16 * MB},
        {4, 0, 4, 1, 16 * MB},
    };
    // limit=48MB: first batch [0,1]=32MB, second [2]=96MB (single cell > limit),
    // third [3,4]=32MB -> 3 batches
    auto batches = RunAndCountBatches(specs, 48 * MB);
    EXPECT_EQ(batches, 3);
}

TEST(LoadCellBatchAsync, SingleCellExceedsLimit) {
    // 1 cell of 256MB with limit=128MB -> 1 batch (can't split a cell)
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs = {{/*cid=*/0,
                                    /*file_idx=*/0,
                                    /*local_rg_offset=*/0,
                                    /*rg_count=*/1,
                                    /*memory_size=*/256 * MB}};
    auto batches = RunAndCountBatches(specs, 128 * MB);
    EXPECT_EQ(batches, 1);
}

TEST(LoadCellBatchAsync, FileBoundarySplit) {
    // 4 cells x 32MB across 2 files, limit=128MB
    // File boundary forces a split even though memory fits
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs = {
        {0, /*file_idx=*/0, 0, 1, 32 * MB},
        {1, /*file_idx=*/0, 1, 1, 32 * MB},
        {2, /*file_idx=*/1, 0, 1, 32 * MB},
        {3, /*file_idx=*/1, 1, 1, 32 * MB},
    };
    auto batches = RunAndCountBatches(specs, 128 * MB);
    // Should be at least 2 batches (one per file)
    EXPECT_GE(batches, 2);
}

TEST(LoadCellBatchAsync, ContiguityBreak) {
    // Cells with rg_offset gap -> split at gap
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs = {
        {0, 0, /*local_rg_offset=*/0, 1, 32 * MB},
        {1, 0, /*local_rg_offset=*/1, 1, 32 * MB},
        // gap: offset 3 instead of 2
        {2, 0, /*local_rg_offset=*/3, 1, 32 * MB},
        {3, 0, /*local_rg_offset=*/4, 1, 32 * MB},
    };
    auto batches = RunAndCountBatches(specs, 256 * MB);
    // Should split at the contiguity break
    EXPECT_EQ(batches, 2);
}

TEST(LoadCellBatchAsync, EmptyCells) {
    auto channel = std::make_shared<CellReaderChannel>(64);
    auto futures = LoadCellBatchAsync(
        nullptr, {}, MakeMockReaderFactory(), channel, 128 << 20);
    EXPECT_EQ(futures.size(), 0);
}

TEST(LoadCellBatchAsync, CancellationStopsMidBatchPush) {
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs = {
        {0, 0, 0, 1, 8 * MB},
        {1, 0, 1, 1, 8 * MB},
        {2, 0, 2, 1, 8 * MB},
        {3, 0, 3, 1, 8 * MB},
    };

    folly::CancellationSource source;
    milvus::OpContext op_ctx(source.getToken());
    auto channel = std::make_shared<CellReaderChannel>(1);
    auto futures = LoadCellBatchAsync(&op_ctx,
                                      specs,
                                      MakeMockReaderFactory(),
                                      channel,
                                      128 * MB);

    ASSERT_EQ(futures.size(), 1);

    std::shared_ptr<CellLoadResult> cell_data;
    ASSERT_TRUE(channel->pop(cell_data));
    ASSERT_NE(cell_data, nullptr);
    EXPECT_EQ(cell_data->cid, 0);

    source.requestCancellation();

    size_t received = 1;
    while (channel->pop(cell_data)) {
        ASSERT_NE(cell_data, nullptr);
        ++received;
    }

    try {
        futures[0].get();
        FAIL() << "expected cancellation";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
    EXPECT_LT(received, specs.size());
}
