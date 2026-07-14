// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/storagev2translator/AsyncLoadPipeline.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "common/EasyAssert.h"
#include "folly/CancellationToken.h"
#include "folly/Executor.h"
#include "folly/futures/Future.h"
#include "folly/futures/Promise.h"
#include "futures/Executor.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/reader.h"
#include "storage/EntryStreamUtils.h"

namespace milvus::segcore::storagev2translator {
namespace {

class InlineRecordingExecutor : public folly::Executor {
 public:
    void
    add(folly::Func func) override {
        Run(std::move(func), futures::ExecutePriority::NORMAL);
    }

    void
    addWithPriority(folly::Func func, int8_t priority) override {
        Run(std::move(func), priority);
    }

    uint8_t
    getNumPriorities() const override {
        return 3;
    }

    bool
    IsRunning() const {
        return running_;
    }

    std::vector<int8_t>
    Priorities() const {
        std::lock_guard lock(mutex_);
        return priorities_;
    }

 private:
    void
    Run(folly::Func func, int8_t priority) {
        {
            std::lock_guard lock(mutex_);
            priorities_.push_back(priority);
        }
        running_ = true;
        func();
        running_ = false;
    }

    static thread_local bool running_;
    mutable std::mutex mutex_;
    std::vector<int8_t> priorities_;
};

thread_local bool InlineRecordingExecutor::running_ = false;

class FakeChunkReader : public milvus_storage::api::ChunkReader {
 public:
    explicit FakeChunkReader(InlineRecordingExecutor* executor)
        : executor_(executor) {
    }

    size_t
    total_number_of_chunks() const override {
        return 32;
    }

    arrow::Result<std::vector<int64_t>>
    get_chunk_indices(const std::vector<int64_t>& row_indices) override {
        return row_indices;
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    get_chunk(int64_t chunk_index) override {
        ARROW_ASSIGN_OR_RAISE(auto batches,
                              MakeBatches(std::vector<int64_t>{chunk_index}));
        return batches.front();
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
    get_chunks(const std::vector<int64_t>& chunk_indices,
               size_t /*parallelism*/) override {
        return MakeBatches(chunk_indices);
    }

    folly::SemiFuture<
        arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>>
    get_chunks_async(const std::vector<int64_t>& chunk_indices,
                     size_t parallelism) override {
        async_calls_.fetch_add(1);
        called_on_executor_.store(executor_ && executor_->IsRunning());
        parallelism_.store(parallelism);
        requested_indices_.push_back(chunk_indices);
        if (on_async_call_) {
            on_async_call_();
        }
        if (deferred_read_) {
            return deferred_read_->getSemiFuture();
        }
        if (!status_.ok()) {
            return folly::makeSemiFuture(
                arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>(
                    status_));
        }
        return folly::makeSemiFuture(MakeBatches(chunk_indices));
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_estimated_size() override {
        return std::vector<uint64_t>(32, 1);
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_column_estimated_size(
        const std::string& /*field_name*/) override {
        return std::vector<uint64_t>(32, 1);
    }

    arrow::Result<std::vector<std::vector<uint64_t>>>
    get_chunk_column_estimated_size() override {
        return std::vector<std::vector<uint64_t>>{std::vector<uint64_t>(32, 1)};
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_rows() override {
        return std::vector<uint64_t>(32, 1);
    }

    void
    SetStatus(arrow::Status status) {
        status_ = std::move(status);
    }

    void
    SetOnAsyncCall(std::function<void()> on_async_call) {
        on_async_call_ = std::move(on_async_call);
    }

    void
    DeferNextRead() {
        deferred_read_ = std::make_shared<folly::Promise<
            arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>>>();
    }

    void
    CompleteDeferredRead() {
        ASSERT_TRUE(deferred_read_ != nullptr);
        ASSERT_FALSE(requested_indices_.empty());
        deferred_read_->setValue(MakeBatches(requested_indices_.back()));
    }

    size_t
    AsyncCalls() const {
        return async_calls_.load();
    }

    bool
    CalledOnExecutor() const {
        return called_on_executor_.load();
    }

    size_t
    Parallelism() const {
        return parallelism_.load();
    }

 private:
    static arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
    MakeBatches(const std::vector<int64_t>& chunk_indices) {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        batches.reserve(chunk_indices.size());
        auto schema = arrow::schema({arrow::field("value", arrow::int64())});
        for (auto chunk_index : chunk_indices) {
            arrow::Int64Builder builder;
            ARROW_RETURN_NOT_OK(builder.Append(chunk_index));
            ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
            batches.push_back(
                arrow::RecordBatch::Make(schema, 1, {std::move(array)}));
        }
        return batches;
    }

    InlineRecordingExecutor* executor_;
    arrow::Status status_;
    std::atomic<size_t> async_calls_{0};
    std::atomic<bool> called_on_executor_{false};
    std::atomic<size_t> parallelism_{0};
    std::vector<std::vector<int64_t>> requested_indices_;
    std::function<void()> on_async_call_;
    std::shared_ptr<folly::Promise<
        arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>>>
        deferred_read_;
};

class AsyncLoadPipelineTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        budget_.SetCapacityBytes(1024);
    }

    void
    TearDown() override {
        budget_.SetCapacityBytes(0);
    }

    AsyncLoadPipelineOptions
    Options(milvus::proto::common::LoadPriority priority =
                milvus::proto::common::LoadPriority::HIGH) {
        return {.segment_id = 100,
                .read_window_bytes = 8,
                .load_priority = priority,
                .executor = &executor_};
    }

    CellFinalizeFunc
    Finalizer(std::vector<int64_t>* finalized = nullptr) {
        return [this, finalized](
                   const std::vector<std::shared_ptr<arrow::Table>>& tables,
                   int64_t cid) {
            EXPECT_TRUE(executor_.IsRunning());
            EXPECT_FALSE(tables.empty());
            if (finalized) {
                finalized->push_back(cid);
            }
            return std::make_unique<GroupChunk>();
        };
    }

    storage::TransientMemoryBudget& budget_ =
        storage::TransientMemoryBudget::GetLoadTransientBudget();
    InlineRecordingExecutor executor_;
};

TEST_F(AsyncLoadPipelineTest, BuildsContiguousReadWindows) {
    std::vector<CellSpec> cells{
        {.cid = 2,
         .file_idx = 0,
         .local_rg_offset = 4,
         .rg_count = 2,
         .memory_size = 4,
         .loading_overhead_size = 6},
        {.cid = 0,
         .file_idx = 0,
         .local_rg_offset = 0,
         .rg_count = 2,
         .memory_size = 4,
         .loading_overhead_size = 6},
        {.cid = 1,
         .file_idx = 0,
         .local_rg_offset = 2,
         .rg_count = 2,
         .memory_size = 4,
         .loading_overhead_size = 6},
        {.cid = 3,
         .file_idx = 0,
         .local_rg_offset = 8,
         .rg_count = 1,
         .memory_size = 1,
         .loading_overhead_size = 1},
    };

    auto windows = BuildAsyncReadWindows(cells, 8);

    ASSERT_EQ(windows.size(), 3);
    EXPECT_EQ(windows[0].chunk_indices, (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(windows[0].budget_bytes, 12);
    EXPECT_EQ(windows[1].chunk_indices, (std::vector<int64_t>{4, 5}));
    EXPECT_EQ(windows[2].chunk_indices, (std::vector<int64_t>{8}));
}

TEST_F(AsyncLoadPipelineTest, RestoresRequestedCellOrder) {
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{
        {.cid = 2,
         .file_idx = 0,
         .local_rg_offset = 4,
         .rg_count = 2,
         .memory_size = 4},
        {.cid = 0,
         .file_idx = 0,
         .local_rg_offset = 0,
         .rg_count = 2,
         .memory_size = 4},
        {.cid = 1,
         .file_idx = 0,
         .local_rg_offset = 2,
         .rg_count = 2,
         .memory_size = 4},
    };
    std::vector<int64_t> finalized;

    auto results =
        LoadCellsAsync(
            nullptr, std::move(cells), reader, Finalizer(&finalized), Options())
            .get();

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0].first, 2);
    EXPECT_EQ(results[1].first, 0);
    EXPECT_EQ(results[2].first, 1);
    EXPECT_EQ(reader->Parallelism(), 1);
    EXPECT_TRUE(reader->CalledOnExecutor());
}

TEST_F(AsyncLoadPipelineTest, DefaultExecutorRunsOffTheCallingThread) {
    auto caller = std::this_thread::get_id();
    std::atomic<bool> ran_off_caller{false};
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    reader->SetOnAsyncCall(
        [&]() { ran_off_caller.store(std::this_thread::get_id() != caller); });
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options();
    options.executor = nullptr;

    auto results =
        LoadCellsAsync(
            nullptr,
            std::move(cells),
            reader,
            [](const auto&, int64_t) { return std::make_unique<GroupChunk>(); },
            options)
            .get();

    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(ran_off_caller.load());
}

TEST_F(AsyncLoadPipelineTest, WaitsForAsyncBudgetBeforeReading) {
    budget_.SetCapacityBytes(1);
    auto blocker =
        budget_.AcquireAsync(1, storage::TransientBudgetPriority::High).get();
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};

    auto future = LoadCellsAsync(
        nullptr, std::move(cells), reader, Finalizer(), Options());

    EXPECT_FALSE(future.isReady());
    EXPECT_EQ(reader->AsyncCalls(), 0);
    blocker.Release();
    auto results = std::move(future).get();
    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(reader->AsyncCalls(), 1);
}

TEST_F(AsyncLoadPipelineTest, WaitsForStorageFutureBeforeFinalizing) {
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    reader->DeferNextRead();
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    size_t finalized = 0;
    auto finalizer = [&finalized](
                         const std::vector<std::shared_ptr<arrow::Table>>&,
                         int64_t) {
        ++finalized;
        return std::make_unique<GroupChunk>();
    };

    auto future = LoadCellsAsync(
        nullptr, std::move(cells), reader, std::move(finalizer), Options());

    EXPECT_FALSE(future.isReady());
    EXPECT_EQ(finalized, 0);
    reader->CompleteDeferredRead();
    EXPECT_EQ(std::move(future).get().size(), 1);
    EXPECT_EQ(finalized, 1);
}

TEST_F(AsyncLoadPipelineTest, MapsLoadPriorityToBudgetAndExecutor) {
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};

    auto results =
        LoadCellsAsync(nullptr,
                       std::move(cells),
                       reader,
                       Finalizer(),
                       Options(milvus::proto::common::LoadPriority::LOW))
            .get();

    EXPECT_EQ(results.size(), 1);
    auto priorities = executor_.Priorities();
    EXPECT_NE(std::find(priorities.begin(),
                        priorities.end(),
                        futures::ExecutePriority::LOW),
              priorities.end());
}

TEST_F(AsyncLoadPipelineTest, HighPriorityAdmissionPassesQueuedLowLoad) {
    budget_.SetCapacityBytes(1);
    auto blocker =
        budget_.AcquireAsync(1, storage::TransientBudgetPriority::Low).get();
    std::vector<int64_t> read_order;
    auto low_reader = std::make_shared<FakeChunkReader>(&executor_);
    low_reader->SetOnAsyncCall([&read_order]() { read_order.push_back(1); });
    auto high_reader = std::make_shared<FakeChunkReader>(&executor_);
    high_reader->SetOnAsyncCall([&read_order]() { read_order.push_back(2); });
    auto cell = []() {
        return std::vector<CellSpec>{{.cid = 0,
                                      .file_idx = 0,
                                      .local_rg_offset = 0,
                                      .rg_count = 1,
                                      .memory_size = 1}};
    };

    auto low =
        LoadCellsAsync(nullptr,
                       cell(),
                       low_reader,
                       Finalizer(),
                       Options(milvus::proto::common::LoadPriority::LOW));
    auto high =
        LoadCellsAsync(nullptr,
                       cell(),
                       high_reader,
                       Finalizer(),
                       Options(milvus::proto::common::LoadPriority::HIGH));

    blocker.Release();
    EXPECT_EQ(std::move(high).get().size(), 1);
    EXPECT_EQ(std::move(low).get().size(), 1);
    EXPECT_EQ(read_order, (std::vector<int64_t>{2, 1}));
}

TEST_F(AsyncLoadPipelineTest, CancelsWhileWaitingForBudget) {
    budget_.SetCapacityBytes(1);
    auto blocker =
        budget_.AcquireAsync(1, storage::TransientBudgetPriority::High).get();
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto future =
        LoadCellsAsync(&ctx, std::move(cells), reader, Finalizer(), Options());

    source.requestCancellation();

    try {
        std::move(future).get();
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(reader->AsyncCalls(), 0);
    blocker.Release();
}

TEST_F(AsyncLoadPipelineTest, CancelsAfterStorageRead) {
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    reader->SetOnAsyncCall([&source]() { source.requestCancellation(); });
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    size_t finalized = 0;
    auto finalizer = [&finalized](
                         const std::vector<std::shared_ptr<arrow::Table>>&,
                         int64_t) {
        ++finalized;
        return std::make_unique<GroupChunk>();
    };

    try {
        LoadCellsAsync(
            &ctx, std::move(cells), reader, std::move(finalizer), Options())
            .get();
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(finalized, 0);
}

TEST_F(AsyncLoadPipelineTest, CancelsBetweenCellFinalization) {
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{
        {.cid = 0,
         .file_idx = 0,
         .local_rg_offset = 0,
         .rg_count = 1,
         .memory_size = 1},
        {.cid = 1,
         .file_idx = 0,
         .local_rg_offset = 1,
         .rg_count = 1,
         .memory_size = 1},
    };
    std::vector<int64_t> finalized;
    auto finalizer = [&source, &finalized](
                         const std::vector<std::shared_ptr<arrow::Table>>&,
                         int64_t cid) {
        finalized.push_back(cid);
        if (cid == 0) {
            source.requestCancellation();
        }
        return std::make_unique<GroupChunk>();
    };

    try {
        LoadCellsAsync(
            &ctx, std::move(cells), reader, std::move(finalizer), Options())
            .get();
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(finalized, (std::vector<int64_t>{0}));
}

TEST_F(AsyncLoadPipelineTest, PreservesTypedStorageErrors) {
    auto run = [this](arrow::Status status, ErrorCode expected) {
        auto reader = std::make_shared<FakeChunkReader>(&executor_);
        reader->SetStatus(std::move(status));
        std::vector<CellSpec> cells{{.cid = 0,
                                     .file_idx = 0,
                                     .local_rg_offset = 0,
                                     .rg_count = 1,
                                     .memory_size = 1}};
        try {
            LoadCellsAsync(
                nullptr, std::move(cells), reader, Finalizer(), Options())
                .get();
            FAIL() << "expected storage error";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), expected);
        }
    };

    run(arrow::Status::IOError("permanent io"), ErrorCode::StorageError);
    run(milvus_storage::MakeExtendError(
            milvus_storage::ExtendStatusCode::StorageTransientThrottling,
            "throttled"),
        ErrorCode::StorageTransientError);
}

}  // namespace
}  // namespace milvus::segcore::storagev2translator
