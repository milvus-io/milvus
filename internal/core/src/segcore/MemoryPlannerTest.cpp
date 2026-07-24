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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include <folly/CancellationToken.h>
#include "folly/ScopeGuard.h"
#include "common/EasyAssert.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/metadata.h"
#include "segcore/memory_planner.h"
#include "storage/EntryStreamUtils.h"
#include "storage/ThreadPools.h"

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

TEST(FieldDataLoadBatchSplitTargetBytes, UsesBatchTargetByDefault) {
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    auto cleanup = folly::makeGuard(
        [&budget, old_capacity]() { budget.SetCapacityBytes(old_capacity); });

    budget.SetCapacityBytes(kDefaultFieldDataLoadBatchTargetBytes * 4);

    EXPECT_EQ(FieldDataLoadBatchSplitTargetBytes(),
              kDefaultFieldDataLoadBatchTargetBytes);
}

TEST(FieldDataLoadBatchSplitTargetBytes, CapsTargetByConfiguredBudget) {
    constexpr int64_t MB = 1 << 20;
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    auto cleanup = folly::makeGuard(
        [&budget, old_capacity]() { budget.SetCapacityBytes(old_capacity); });

    budget.SetCapacityBytes(8 * MB);

    EXPECT_EQ(FieldDataLoadBatchSplitTargetBytes(), 8 * MB);
}

TEST(FieldDataLoadingOverheadUpperBound, UsesPoolBoundWhenBudgetDisabled) {
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    auto old_batch_target = FieldDataLoadBatchTargetBytes();
    auto& high_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& low_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    auto old_high_max = high_pool.GetMaxThreadNum();
    auto old_low_max = low_pool.GetMaxThreadNum();
    auto cleanup = folly::makeGuard([&budget,
                                     old_capacity,
                                     old_batch_target,
                                     &high_pool,
                                     &low_pool,
                                     old_high_max,
                                     old_low_max]() {
        budget.SetCapacityBytes(old_capacity);
        SetFieldDataLoadBatchTargetBytes(old_batch_target);
        high_pool.Resize(old_high_max);
        low_pool.Resize(old_low_max);
    });

    budget.SetCapacityBytes(0);
    SetFieldDataLoadBatchTargetBytes(64);
    auto high_load_tasks = std::max<size_t>(2, high_pool.GetThreadNum());
    auto low_load_tasks = std::max<size_t>(3, low_pool.GetThreadNum());
    high_pool.Resize(static_cast<int>(high_load_tasks));
    low_pool.Resize(static_cast<int>(low_load_tasks));
    auto max_load_tasks = high_load_tasks + low_load_tasks;

    auto upper_bound =
        FieldDataLoadingOverheadUpperBound(/*max_memory_overhead=*/128);

    EXPECT_EQ(upper_bound.memory_bytes, max_load_tasks * 128);
    EXPECT_EQ(upper_bound.file_bytes, 0);

    auto mmap_upper_bound = FieldDataLoadingOverheadUpperBound(
        /*max_memory_overhead=*/128,
        /*max_file_overhead=*/std::optional<int64_t>{256});

    EXPECT_EQ(mmap_upper_bound.memory_bytes, max_load_tasks * 128);
    EXPECT_EQ(mmap_upper_bound.file_bytes, max_load_tasks * 256);
}

TEST(FieldDataLoadingOverheadUpperBound, UsesBudgetWithMaxOverheadFloor) {
    constexpr int64_t MB = 1 << 20;
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    auto cleanup = folly::makeGuard(
        [&budget, old_capacity]() { budget.SetCapacityBytes(old_capacity); });

    budget.SetCapacityBytes(8 * MB);

    auto memory_only =
        FieldDataLoadingOverheadUpperBound(/*max_memory_overhead=*/16 * MB);
    EXPECT_EQ(memory_only.memory_bytes, 16 * MB);
    EXPECT_EQ(memory_only.file_bytes, 0);

    auto mmap_smaller_overhead = FieldDataLoadingOverheadUpperBound(
        /*max_memory_overhead=*/4 * MB, std::optional<int64_t>{2 * MB});
    EXPECT_EQ(mmap_smaller_overhead.memory_bytes, 8 * MB);
    EXPECT_EQ(mmap_smaller_overhead.file_bytes, 8 * MB);

    auto mmap_larger_file_overhead = FieldDataLoadingOverheadUpperBound(
        /*max_memory_overhead=*/4 * MB, std::optional<int64_t>{32 * MB});
    EXPECT_EQ(mmap_larger_file_overhead.memory_bytes, 8 * MB);
    EXPECT_EQ(mmap_larger_file_overhead.file_bytes, 32 * MB);
}

TEST(LoadTransientSharedOverheadUpperBound, UsesBudgetAndSingleTaskBounds) {
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    auto cleanup =
        folly::makeGuard([&]() { budget.SetCapacityBytes(old_capacity); });
    constexpr size_t task_overhead = 128;

    budget.SetCapacityBytes(512);
    EXPECT_EQ(LoadTransientSharedOverheadUpperBound(task_overhead), 512);

    budget.SetCapacityBytes(1024);
    EXPECT_EQ(LoadTransientSharedOverheadUpperBound(task_overhead), 1024);

    budget.SetCapacityBytes(64);
    EXPECT_EQ(LoadTransientSharedOverheadUpperBound(task_overhead), 128);
}

TEST(LoadTransientPoolUpperBound, UsesLiveWorkersDuringShrink) {
    auto& high_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& low_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    auto old_high_max = high_pool.GetMaxThreadNum();
    auto old_low_max = low_pool.GetMaxThreadNum();
    auto old_thread_limit = milvus::THREAD_POOL_MAX_THREADS_SIZE.load();
    auto pool_cleanup = folly::makeGuard([&]() {
        milvus::SetThreadPoolMaxThreadsSize(old_thread_limit);
        high_pool.Resize(old_high_max);
        low_pool.Resize(old_low_max);
    });

    milvus::SetThreadPoolMaxThreadsSize(
        std::max(old_thread_limit, static_cast<int>(3)));
    high_pool.Resize(3);
    low_pool.Resize(1);

    std::mutex started_mu;
    std::condition_variable started_cv;
    size_t started = 0;
    std::promise<void> release;
    auto release_future = release.get_future().share();
    std::vector<std::future<void>> blockers;
    auto blocker_cleanup = folly::makeGuard([&]() {
        release.set_value();
        for (auto& blocker : blockers) {
            blocker.get();
        }
    });

    for (size_t i = 0; i < 3; ++i) {
        blockers.push_back(high_pool.Submit([&]() {
            {
                std::lock_guard<std::mutex> lock(started_mu);
                ++started;
            }
            started_cv.notify_one();
            release_future.wait();
        }));
    }

    {
        std::unique_lock<std::mutex> lock(started_mu);
        ASSERT_TRUE(started_cv.wait_for(
            lock, std::chrono::seconds(2), [&]() { return started == 3; }));
    }

    high_pool.Resize(1);
    auto effective_high =
        std::max(high_pool.GetMaxThreadNum(), high_pool.GetThreadNum());
    auto effective_low =
        std::max(low_pool.GetMaxThreadNum(), low_pool.GetThreadNum());
    ASSERT_GT(effective_high, high_pool.GetMaxThreadNum());

    constexpr size_t task_overhead = 128;
    EXPECT_EQ(
        LoadTransientPoolUpperBound(task_overhead),
        static_cast<int64_t>((effective_high + effective_low) * task_overhead));
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

CellFinalizeFunc
MakeMockCellFinalizer() {
    return
        [](const std::vector<std::shared_ptr<arrow::Table>>& /*tables*/,
           int64_t /*cid*/) { return std::make_unique<milvus::GroupChunk>(); };
}

LoadedCellBatch
CollectLoadedCells(std::vector<CellLoadFuture>& futures) {
    LoadedCellBatch loaded_cells;
    for (auto& future : futures) {
        auto batch = future.get();
        for (auto& loaded_cell : batch) {
            loaded_cells.push_back(std::move(loaded_cell));
        }
    }
    return loaded_cells;
}

// Helper to run LoadCellBatchAsync and return future count (= batch count).
size_t
RunAndCountBatches(std::vector<CellSpec> cell_specs, int64_t memory_limit) {
    auto futures = LoadCellBatchAsync(nullptr,
                                      std::move(cell_specs),
                                      MakeMockReaderFactory(),
                                      memory_limit,
                                      milvus::proto::common::LoadPriority::HIGH,
                                      MakeMockCellFinalizer());
    CollectLoadedCells(futures);
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

TEST(LoadCellBatchAsync, LoadingOverheadBasedSplitKeepsReaderMemoryLimit) {
    constexpr int64_t MB = 1 << 20;

    std::atomic<int> read_calls{0};
    std::atomic<int64_t> reader_memory_limit_sum{0};
    BatchReaderFactory factory = [&read_calls, &reader_memory_limit_sum](
                                     size_t /*batch_key*/,
                                     int64_t /*rg_offset*/,
                                     int64_t total_rg_count,
                                     int64_t reader_memory_limit)
        -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
        read_calls.fetch_add(1);
        reader_memory_limit_sum.fetch_add(reader_memory_limit);
        auto schema = arrow::schema({arrow::field("x", arrow::int64())});
        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (int64_t i = 0; i < total_rg_count; ++i) {
            tables.push_back(arrow::Table::MakeEmpty(schema).ValueOrDie());
        }
        return tables;
    };

    std::vector<CellSpec> specs = {
        {/*cid=*/0,
         /*file_idx=*/0,
         /*local_rg_offset=*/0,
         /*rg_count=*/1,
         /*memory_size=*/32 * MB,
         /*loading_overhead_size=*/64 * MB},
        {/*cid=*/1,
         /*file_idx=*/0,
         /*local_rg_offset=*/1,
         /*rg_count=*/1,
         /*memory_size=*/32 * MB,
         /*loading_overhead_size=*/64 * MB},
    };

    auto futures = LoadCellBatchAsync(nullptr,
                                      std::move(specs),
                                      std::move(factory),
                                      96 * MB,
                                      milvus::proto::common::LoadPriority::HIGH,
                                      MakeMockCellFinalizer());
    CollectLoadedCells(futures);

    EXPECT_EQ(futures.size(), 2);
    EXPECT_EQ(read_calls.load(), 2);
    EXPECT_EQ(reader_memory_limit_sum.load(), 64 * MB);
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

TEST(LoadCellBatchAsync, ReaderMemoryLimitUsesReadWindowFloor) {
    std::atomic<int64_t> observed_reader_memory_limit{0};
    BatchReaderFactory factory = [&observed_reader_memory_limit](
                                     size_t /*batch_key*/,
                                     int64_t /*rg_offset*/,
                                     int64_t total_rg_count,
                                     int64_t reader_memory_limit)
        -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
        observed_reader_memory_limit.store(reader_memory_limit);
        auto schema = arrow::schema({arrow::field("x", arrow::int64())});
        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (int64_t i = 0; i < total_rg_count; ++i) {
            tables.push_back(arrow::Table::MakeEmpty(schema).ValueOrDie());
        }
        return tables;
    };

    std::vector<CellSpec> specs = {{/*cid=*/0,
                                    /*file_idx=*/0,
                                    /*local_rg_offset=*/0,
                                    /*rg_count=*/1,
                                    /*memory_size=*/1}};

    auto futures = LoadCellBatchAsync(nullptr,
                                      std::move(specs),
                                      std::move(factory),
                                      1,
                                      milvus::proto::common::LoadPriority::HIGH,
                                      MakeMockCellFinalizer());
    CollectLoadedCells(futures);

    EXPECT_EQ(observed_reader_memory_limit.load(), FieldDataReadWindowBytes());
}

TEST(LoadCellBatchAsync, FinalizesCellsBeforeFutureCompletion) {
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs = {
        {0, 0, 0, 1, 8 * MB},
        {1, 0, 1, 1, 8 * MB},
    };

    std::atomic<int> finalized{0};
    auto futures = LoadCellBatchAsync(
        nullptr,
        std::move(specs),
        MakeMockReaderFactory(),
        32 * MB,
        milvus::proto::common::LoadPriority::HIGH,
        [&finalized](const std::vector<std::shared_ptr<arrow::Table>>& tables,
                     int64_t /*cid*/) {
            EXPECT_EQ(tables.size(), 1);
            finalized.fetch_add(1);
            return std::make_unique<milvus::GroupChunk>();
        });

    auto loaded_cells = CollectLoadedCells(futures);
    for (const auto& loaded_cell : loaded_cells) {
        EXPECT_NE(loaded_cell.chunk, nullptr);
    }

    EXPECT_EQ(loaded_cells.size(), 2);
    EXPECT_EQ(finalized.load(), 2);
}

TEST(LoadCellBatchAsync, ReleasesBatchBudgetBeforeFutureCompletion) {
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    budget.SetCapacityBytes(2);
    auto cleanup = folly::makeGuard(
        [&budget, old_capacity]() { budget.SetCapacityBytes(old_capacity); });

    std::vector<CellSpec> specs = {
        {0, 0, 0, 1, 1},
        {1, 0, 1, 1, 1},
    };

    std::atomic<int> finalized{0};
    auto futures = LoadCellBatchAsync(
        nullptr,
        std::move(specs),
        MakeMockReaderFactory(),
        2,
        milvus::proto::common::LoadPriority::HIGH,
        [&finalized](const std::vector<std::shared_ptr<arrow::Table>>&,
                     int64_t) {
            finalized.fetch_add(1);
            return std::make_unique<milvus::GroupChunk>();
        });

    ASSERT_EQ(futures.size(), 1);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (finalized.load() < 2 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(finalized.load(), 2);

    folly::CancellationSource cancel_source;
    auto acquire_future = std::async(
        std::launch::async, [&budget, token = cancel_source.getToken()] {
            return budget.AcquireUntil(2, token);
        });
    bool acquired = false;
    if (acquire_future.wait_for(std::chrono::seconds(2)) ==
        std::future_status::ready) {
        acquired = acquire_future.get();
    } else {
        cancel_source.requestCancellation();
        ASSERT_EQ(acquire_future.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        acquired = acquire_future.get();
    }
    if (acquired) {
        budget.Release(2);
    }

    auto loaded_cells = futures[0].get();
    EXPECT_EQ(loaded_cells.size(), 2);
    EXPECT_TRUE(acquired) << "budget stayed held after cells were finalized";
}

TEST(LoadCellBatchAsync, CompletedBatchDoesNotRequireResultConsumer) {
    std::vector<CellSpec> specs = {
        {0, 0, 0, 1, 1},
        {1, 0, 1, 1, 1},
    };

    auto futures = LoadCellBatchAsync(nullptr,
                                      std::move(specs),
                                      MakeMockReaderFactory(),
                                      2,
                                      milvus::proto::common::LoadPriority::HIGH,
                                      MakeMockCellFinalizer());
    ASSERT_EQ(futures.size(), 1);

    auto completion_status =
        futures[0].wait_for(std::chrono::milliseconds(100));

    EXPECT_EQ(completion_status, std::future_status::ready);
    auto loaded_cells = futures[0].get();
    EXPECT_EQ(loaded_cells.size(), 2);
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
    auto futures = LoadCellBatchAsync(nullptr,
                                      {},
                                      MakeMockReaderFactory(),
                                      128 << 20,
                                      milvus::proto::common::LoadPriority::HIGH,
                                      MakeMockCellFinalizer());
    EXPECT_EQ(futures.size(), 0);
}

TEST(LoadCellBatchAsync, CancellationStopsMidBatchFinalize) {
    constexpr int64_t MB = 1 << 20;
    std::vector<CellSpec> specs = {
        {0, 0, 0, 1, 8 * MB},
        {1, 0, 1, 1, 8 * MB},
        {2, 0, 2, 1, 8 * MB},
        {3, 0, 3, 1, 8 * MB},
    };

    folly::CancellationSource source;
    milvus::OpContext op_ctx(source.getToken());
    std::atomic<size_t> finalized{0};
    auto futures = LoadCellBatchAsync(
        &op_ctx,
        specs,
        MakeMockReaderFactory(),
        128 * MB,
        milvus::proto::common::LoadPriority::HIGH,
        [&source, &finalized](const std::vector<std::shared_ptr<arrow::Table>>&,
                              int64_t) {
            if (finalized.fetch_add(1) == 0) {
                source.requestCancellation();
            }
            return std::make_unique<milvus::GroupChunk>();
        });

    ASSERT_EQ(futures.size(), 1);

    try {
        futures[0].get();
        FAIL() << "expected cancellation";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
    EXPECT_LT(finalized.load(), specs.size());
}

TEST(LoadCellBatchAsync, CancellationWhileWaitingForBudgetSkipsRead) {
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    budget.SetCapacityBytes(1);
    budget.Acquire(1);
    auto cleanup = folly::makeGuard([&budget, old_capacity]() {
        budget.Release(1);
        budget.SetCapacityBytes(old_capacity);
    });

    std::atomic<int> read_calls{0};
    BatchReaderFactory factory = [&read_calls](size_t /*batch_key*/,
                                               int64_t /*rg_offset*/,
                                               int64_t total_rg_count,
                                               int64_t /*reader_memory_limit*/)
        -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
        read_calls.fetch_add(1);
        auto schema = arrow::schema({arrow::field("x", arrow::int64())});
        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (int64_t i = 0; i < total_rg_count; ++i) {
            tables.push_back(arrow::Table::MakeEmpty(schema).ValueOrDie());
        }
        return tables;
    };

    folly::CancellationSource source;
    milvus::OpContext op_ctx(source.getToken());
    std::vector<CellSpec> specs = {{/*cid=*/0,
                                    /*file_idx=*/0,
                                    /*local_rg_offset=*/0,
                                    /*rg_count=*/1,
                                    /*memory_size=*/1}};
    auto load_future = std::async(
        std::launch::async,
        [&op_ctx,
         specs = std::move(specs),
         factory = std::move(factory)]() mutable {
            return LoadCellBatchAsync(
                &op_ctx,
                std::move(specs),
                std::move(factory),
                1,
                milvus::proto::common::LoadPriority::HIGH,
                [](const std::vector<std::shared_ptr<arrow::Table>>&, int64_t) {
                    return std::make_unique<milvus::GroupChunk>();
                });
        });

    EXPECT_EQ(load_future.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    EXPECT_EQ(read_calls.load(), 0);

    source.requestCancellation();

    ASSERT_EQ(load_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto futures = load_future.get();
    ASSERT_EQ(futures.size(), 1);
    ASSERT_EQ(futures[0].wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    try {
        futures[0].get();
        FAIL() << "expected cancellation";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
    EXPECT_EQ(read_calls.load(), 0);
}

TEST(LoadCellBatchAsync, WaitingForBudgetDoesNotOccupyLoadPoolWorker) {
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    budget.SetCapacityBytes(1);
    budget.Acquire(1);
    auto budget_cleanup = folly::makeGuard([&budget, old_capacity]() {
        budget.Release(1);
        budget.SetCapacityBytes(old_capacity);
    });

    auto priority = milvus::proto::common::LoadPriority::LOW;
    auto pool_priority = milvus::PriorityForLoad(priority);
    auto& pool = milvus::ThreadPools::GetThreadPool(pool_priority);
    auto old_max_threads = pool.GetMaxThreadNum();
    auto cpu_num = std::max(1, milvus::CPU_NUM);
    milvus::ThreadPools::ResizeThreadPool(pool_priority,
                                          1.0F / static_cast<float>(cpu_num));
    auto pool_cleanup =
        folly::makeGuard([pool_priority, old_max_threads, cpu_num]() {
            milvus::ThreadPools::ResizeThreadPool(
                pool_priority,
                static_cast<float>(old_max_threads) /
                    static_cast<float>(cpu_num));
        });
    ASSERT_EQ(pool.GetMaxThreadNum(), 1);
    if (pool.GetThreadNum() > 1) {
        GTEST_SKIP() << "load pool already has more than one worker";
    }

    std::atomic<int> read_calls{0};
    BatchReaderFactory factory = [&read_calls](size_t /*batch_key*/,
                                               int64_t /*rg_offset*/,
                                               int64_t total_rg_count,
                                               int64_t /*reader_memory_limit*/)
        -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
        read_calls.fetch_add(1);
        auto schema = arrow::schema({arrow::field("x", arrow::int64())});
        std::vector<std::shared_ptr<arrow::Table>> tables;
        for (int64_t i = 0; i < total_rg_count; ++i) {
            tables.push_back(arrow::Table::MakeEmpty(schema).ValueOrDie());
        }
        return tables;
    };

    folly::CancellationSource source;
    milvus::OpContext op_ctx(source.getToken());
    std::vector<CellSpec> specs = {{/*cid=*/0,
                                    /*file_idx=*/0,
                                    /*local_rg_offset=*/0,
                                    /*rg_count=*/1,
                                    /*memory_size=*/1}};
    auto load_future = std::async(
        std::launch::async,
        [&op_ctx,
         specs = std::move(specs),
         factory = std::move(factory),
         priority]() mutable {
            return LoadCellBatchAsync(
                &op_ctx,
                std::move(specs),
                std::move(factory),
                1,
                priority,
                [](const std::vector<std::shared_ptr<arrow::Table>>&, int64_t) {
                    return std::make_unique<milvus::GroupChunk>();
                });
        });

    ASSERT_EQ(load_future.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    EXPECT_EQ(read_calls.load(), 0);

    auto marker = pool.Submit([]() { return 42; });
    ASSERT_EQ(marker.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(marker.get(), 42);

    source.requestCancellation();

    ASSERT_EQ(load_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto futures = load_future.get();
    ASSERT_EQ(futures.size(), 1);
    ASSERT_EQ(futures[0].wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    try {
        futures[0].get();
        FAIL() << "expected cancellation";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::FollyCancel);
    }
    EXPECT_EQ(read_calls.load(), 0);
}
