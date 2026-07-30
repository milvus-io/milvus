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
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "common/EasyAssert.h"
#include "folly/CancellationToken.h"
#include "folly/Executor.h"
#include "folly/ScopeGuard.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Task.h"
#include "folly/coro/WithCancellation.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/futures/Future.h"
#include "folly/futures/Promise.h"
#include "folly/system/ThreadName.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/reader.h"
#include "storage/EntryStreamUtils.h"
#include "storage/FileWriter.h"
#include "storage/LocalFileIOPool.h"

namespace milvus::segcore::storagev2translator {
namespace {

using ChunkReadResult =
    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>;
using ChunkReadFuture = folly::SemiFuture<ChunkReadResult>;

using LoadCellsAsyncReturn = decltype(LoadCellsAsync(
    std::declval<milvus::OpContext*>(),
    std::declval<std::vector<CellSpec>>(),
    std::declval<std::shared_ptr<milvus_storage::api::ChunkReader>>(),
    std::declval<CellFinalizeFunc>(),
    std::declval<AsyncLoadPipelineOptions>()));

static_assert(std::is_same_v<LoadCellsAsyncReturn,
                             folly::coro::Task<std::vector<AsyncCellResult>>>);

class InlineRecordingExecutor : public folly::Executor {
 public:
    void
    add(folly::Func func) override {
        Run(std::move(func), folly::Executor::MID_PRI);
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
        return running_executor_ == this;
    }

    int8_t
    CurrentPriority() const {
        return running_priority_;
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
        auto* previous_executor = running_executor_;
        auto previous_priority = running_priority_;
        running_executor_ = this;
        running_priority_ = priority;
        try {
            func();
        } catch (...) {
            running_executor_ = previous_executor;
            running_priority_ = previous_priority;
            throw;
        }
        running_executor_ = previous_executor;
        running_priority_ = previous_priority;
    }

    static thread_local InlineRecordingExecutor* running_executor_;
    static thread_local int8_t running_priority_;
    mutable std::mutex mutex_;
    std::vector<int8_t> priorities_;
};

thread_local InlineRecordingExecutor*
    InlineRecordingExecutor::running_executor_ = nullptr;
thread_local int8_t InlineRecordingExecutor::running_priority_ =
    folly::Executor::MID_PRI;

class KeepAliveRecordingExecutor : public folly::Executor {
 public:
    void
    add(folly::Func) override {
        ADD_FAILURE() << "lazy task should not submit executor work";
    }

    size_t
    OutstandingKeepAlives() const {
        return acquires_.load() - releases_.load();
    }

 protected:
    bool
    keepAliveAcquire() noexcept override {
        acquires_.fetch_add(1);
        return true;
    }

    void
    keepAliveRelease() noexcept override {
        releases_.fetch_add(1);
    }

 private:
    std::atomic<size_t> acquires_{0};
    std::atomic<size_t> releases_{0};
};

class AddOnlyInlineExecutor final : public folly::Executor {
 public:
    void
    add(folly::Func func) override {
        calls_.fetch_add(1);
        func();
    }

    size_t
    Calls() const {
        return calls_.load();
    }

 private:
    std::atomic<size_t> calls_{0};
};

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

    ChunkReadFuture
    get_chunks_async(const std::vector<int64_t>& chunk_indices,
                     size_t parallelism) override {
        async_calls_.fetch_add(1);
        called_on_executor_.store(executor_ && executor_->IsRunning());
        parallelism_.store(parallelism);
        requested_indices_.push_back(chunk_indices);
        if (on_async_call_) {
            on_async_call_();
        }
        auto future = [&]() -> ChunkReadFuture {
            if (deferred_read_) {
                return deferred_read_->getSemiFuture();
            }
            if (!status_.ok()) {
                return folly::makeSemiFuture(ChunkReadResult(status_));
            }
            return folly::makeSemiFuture(MakeBatches(chunk_indices));
        }();
        if (!observe_deferred_continuation_) {
            return future;
        }
        return std::move(future).deferValue([this](auto result) {
            deferred_continuation_on_executor_.store(executor_ &&
                                                     executor_->IsRunning());
            deferred_continuation_priority_.store(
                executor_ ? executor_->CurrentPriority()
                          : folly::Executor::MID_PRI);
            return result;
        });
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
        deferred_read_ = std::make_shared<folly::Promise<ChunkReadResult>>();
    }

    void
    ObserveDeferredContinuation() {
        observe_deferred_continuation_ = true;
    }

    void
    CompleteDeferredRead() {
        ASSERT_TRUE(deferred_read_ != nullptr);
        ASSERT_FALSE(requested_indices_.empty());
        deferred_read_->setValue(MakeBatches(requested_indices_.back()));
    }

    void
    FailDeferredRead() {
        ASSERT_TRUE(deferred_read_ != nullptr);
        deferred_read_->setException(std::runtime_error("read failed"));
    }

    size_t
    AsyncCalls() const {
        return async_calls_.load();
    }

    bool
    CalledOnExecutor() const {
        return called_on_executor_.load();
    }

    bool
    DeferredContinuationRanOnExecutor() const {
        return deferred_continuation_on_executor_.load();
    }

    int8_t
    DeferredContinuationPriority() const {
        return deferred_continuation_priority_.load();
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
    std::atomic<bool> deferred_continuation_on_executor_{false};
    std::atomic<int8_t> deferred_continuation_priority_{
        folly::Executor::MID_PRI};
    std::atomic<size_t> parallelism_{0};
    bool observe_deferred_continuation_{false};
    std::vector<std::vector<int64_t>> requested_indices_;
    std::function<void()> on_async_call_;
    std::shared_ptr<folly::Promise<ChunkReadResult>> deferred_read_;
};

class AsyncLoadPipelineTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        budget_.SetCapacityBytes(1024);
    }

    void
    TearDown() override {
        storage::LocalFileIOPool::GetInstance().Configure(0);
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

    folly::Future<std::vector<AsyncCellResult>>
    Start(folly::coro::Task<std::vector<AsyncCellResult>> task) {
        return std::move(task).semi().via(folly::getKeepAliveToken(&executor_));
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

    auto results = folly::coro::blockingWait(LoadCellsAsync(
        nullptr, std::move(cells), reader, Finalizer(&finalized), Options()));

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

    auto results = folly::coro::blockingWait(LoadCellsAsync(
        nullptr,
        std::move(cells),
        reader,
        [](const auto&, int64_t) { return std::make_unique<GroupChunk>(); },
        options));

    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(ran_off_caller.load());
}

TEST_F(AsyncLoadPipelineTest,
       BindsPendingReadContinuationToLoadExecutorAndPriority) {
    InlineRecordingExecutor caller_executor;
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    reader->DeferNextRead();
    reader->ObserveDeferredContinuation();
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options(milvus::proto::common::LoadPriority::LOW);

    auto future =
        std::move(LoadCellsAsync(
                      nullptr, std::move(cells), reader, Finalizer(), options))
            .semi()
            .via(folly::getKeepAliveToken(&caller_executor),
                 folly::Executor::HI_PRI);

    ASSERT_FALSE(future.isReady());
    ASSERT_EQ(reader->AsyncCalls(), 1);
    reader->CompleteDeferredRead();
    auto results = std::move(future).get();

    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(reader->DeferredContinuationRanOnExecutor());
    EXPECT_EQ(reader->DeferredContinuationPriority(), folly::Executor::LO_PRI);
}

TEST_F(AsyncLoadPipelineTest,
       ReleasesBudgetOnLoadExecutorWhenStorageFutureFails) {
    budget_.SetCapacityBytes(1);
    InlineRecordingExecutor caller_executor;
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    reader->DeferNextRead();
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options(milvus::proto::common::LoadPriority::LOW);
    auto load =
        std::move(LoadCellsAsync(
                      nullptr, std::move(cells), reader, Finalizer(), options))
            .semi()
            .via(folly::getKeepAliveToken(&caller_executor),
                 folly::Executor::HI_PRI);

    ASSERT_FALSE(load.isReady());
    ASSERT_EQ(reader->AsyncCalls(), 1);

    bool release_on_load_executor = false;
    bool release_on_caller_executor = false;
    int8_t release_priority = folly::Executor::MID_PRI;
    auto release_observer =
        budget_.AcquireAsync(1, storage::TransientBudgetPriority::High)
            .toUnsafeFuture()
            .thenValueInline([&](storage::TransientBudgetLease lease) {
                release_on_load_executor = executor_.IsRunning();
                release_on_caller_executor = caller_executor.IsRunning();
                if (release_on_load_executor) {
                    release_priority = executor_.CurrentPriority();
                } else if (release_on_caller_executor) {
                    release_priority = caller_executor.CurrentPriority();
                }
                lease.Release();
            });
    ASSERT_FALSE(release_observer.isReady());

    reader->FailDeferredRead();

    try {
        std::move(load).get();
        FAIL() << "expected read failure";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "read failed");
    }
    ASSERT_TRUE(release_observer.isReady());
    std::move(release_observer).get();
    EXPECT_TRUE(release_on_load_executor);
    EXPECT_FALSE(release_on_caller_executor);
    EXPECT_EQ(release_priority, folly::Executor::LO_PRI);
}

TEST_F(AsyncLoadPipelineTest, CapturesExecutorKeepAliveBeforeTaskStarts) {
    KeepAliveRecordingExecutor executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options();
    options.executor = &executor;

    {
        auto task = LoadCellsAsync(
            nullptr, std::move(cells), reader, Finalizer(), options);
        EXPECT_EQ(executor.OutstandingKeepAlives(), 1);
        EXPECT_EQ(reader->AsyncCalls(), 0);
    }

    EXPECT_EQ(executor.OutstandingKeepAlives(), 0);
}

TEST_F(AsyncLoadPipelineTest, CapturesContextCancellationBeforeTaskStarts) {
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto task =
        LoadCellsAsync(&ctx, std::move(cells), reader, Finalizer(), Options());

    ctx.cancellation_token = {};
    source.requestCancellation();

    try {
        folly::coro::blockingWait(std::move(task));
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(reader->AsyncCalls(), 0);
}

TEST_F(AsyncLoadPipelineTest,
       HonorsCallerCoroutineCancellationWhileWaitingForBudget) {
    budget_.SetCapacityBytes(1);
    auto blocker =
        budget_.AcquireAsync(1, storage::TransientBudgetPriority::High).get();
    folly::CancellationSource source;
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};

    auto caller = [this, reader, cells = std::move(cells)]() mutable
        -> folly::coro::Task<std::vector<AsyncCellResult>> {
        co_return co_await LoadCellsAsync(
            nullptr, std::move(cells), reader, Finalizer(), Options());
    };
    auto with_cancellation = [&source, caller = std::move(caller)]() mutable
        -> folly::coro::Task<std::vector<AsyncCellResult>> {
        co_return co_await folly::coro::co_withCancellation(source.getToken(),
                                                            caller());
    };
    auto future = Start(with_cancellation());

    EXPECT_FALSE(future.isReady());
    source.requestCancellation();
    auto ready_after_cancel = future.isReady();
    blocker.Release();

    EXPECT_TRUE(ready_after_cancel);
    try {
        std::move(future).get();
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(reader->AsyncCalls(), 0);
}

TEST_F(AsyncLoadPipelineTest, ComposesWithCallerCoroutine) {
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};

    auto caller = [this, reader, cells = std::move(cells)]() mutable
        -> folly::coro::Task<size_t> {
        auto results = co_await LoadCellsAsync(
            nullptr, std::move(cells), reader, Finalizer(), Options());
        co_return results.size();
    };

    EXPECT_EQ(folly::coro::blockingWait(caller()), 1);
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

    auto future = Start(LoadCellsAsync(
        nullptr, std::move(cells), reader, Finalizer(), Options()));

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

    auto future = Start(LoadCellsAsync(
        nullptr, std::move(cells), reader, std::move(finalizer), Options()));

    EXPECT_FALSE(future.isReady());
    EXPECT_EQ(finalized, 0);
    reader->CompleteDeferredRead();
    EXPECT_EQ(std::move(future).get().size(), 1);
    EXPECT_EQ(finalized, 1);
}

TEST_F(AsyncLoadPipelineTest, MapsLoadPriorityToFollyExecutorPriorities) {
    auto run = [this](milvus::proto::common::LoadPriority priority) {
        auto reader = std::make_shared<FakeChunkReader>(&executor_);
        std::vector<CellSpec> cells{{.cid = 0,
                                     .file_idx = 0,
                                     .local_rg_offset = 0,
                                     .rg_count = 1,
                                     .memory_size = 1}};

        auto results = folly::coro::blockingWait(LoadCellsAsync(
            nullptr, std::move(cells), reader, Finalizer(), Options(priority)));
        EXPECT_EQ(results.size(), 1);
    };

    run(milvus::proto::common::LoadPriority::HIGH);
    run(milvus::proto::common::LoadPriority::LOW);

    auto priorities = executor_.Priorities();
    EXPECT_NE(
        std::find(
            priorities.begin(), priorities.end(), folly::Executor::HI_PRI),
        priorities.end());
    EXPECT_NE(
        std::find(
            priorities.begin(), priorities.end(), folly::Executor::LO_PRI),
        priorities.end());
}

TEST_F(AsyncLoadPipelineTest, FollyPoolRunsQueuedHighLoadBeforeLowLoad) {
    folly::CPUThreadPoolExecutor executor(
        0, folly::CPUThreadPoolExecutor::makeDefaultPriorityQueue(2));
    std::vector<int64_t> read_order;
    auto make_load = [this, &executor, &read_order](
                         milvus::proto::common::LoadPriority priority,
                         int64_t marker) {
        auto reader = std::make_shared<FakeChunkReader>(nullptr);
        reader->SetOnAsyncCall(
            [&read_order, marker]() { read_order.push_back(marker); });
        std::vector<CellSpec> cells{{.cid = marker,
                                     .file_idx = 0,
                                     .local_rg_offset = 0,
                                     .rg_count = 1,
                                     .memory_size = 1}};
        auto options = Options(priority);
        options.executor = &executor;
        return Start(LoadCellsAsync(
            nullptr,
            std::move(cells),
            std::move(reader),
            [](const auto&, int64_t) { return std::make_unique<GroupChunk>(); },
            options));
    };

    auto low = make_load(milvus::proto::common::LoadPriority::LOW, 1);
    auto high = make_load(milvus::proto::common::LoadPriority::HIGH, 2);
    EXPECT_FALSE(low.isReady());
    EXPECT_FALSE(high.isReady());
    EXPECT_TRUE(read_order.empty());

    executor.setNumThreads(1);

    EXPECT_EQ(std::move(high).get().size(), 1);
    EXPECT_EQ(std::move(low).get().size(), 1);
    EXPECT_EQ(read_order, (std::vector<int64_t>{2, 1}));
}

TEST_F(AsyncLoadPipelineTest,
       FinalizesOnLoadExecutorWhenLocalFileIOIsNotRequested) {
    storage::LocalFileIOPool::GetInstance().Configure(1);
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    bool finalized_on_load_executor = false;

    auto results = folly::coro::blockingWait(LoadCellsAsync(
        nullptr,
        std::move(cells),
        reader,
        [this, &finalized_on_load_executor](const auto& tables, int64_t) {
            EXPECT_FALSE(tables.empty());
            finalized_on_load_executor = executor_.IsRunning();
            return std::make_unique<GroupChunk>();
        },
        Options()));

    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(finalized_on_load_executor);
}

TEST_F(AsyncLoadPipelineTest,
       FallsBackToLoadExecutorWhenLocalFileIOPoolIsDisabled) {
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(0);
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    bool finalized_on_load_executor = false;
    auto options = Options();
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };

    auto results = folly::coro::blockingWait(LoadCellsAsync(
        nullptr,
        std::move(cells),
        reader,
        [this, &finalized_on_load_executor](const auto&, int64_t) {
            finalized_on_load_executor = executor_.IsRunning();
            return std::make_unique<GroupChunk>();
        },
        std::move(options)));

    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(finalized_on_load_executor);
}

TEST_F(AsyncLoadPipelineTest,
       DisablingLocalFileIOPoolDoesNotWaitForRemoteRead) {
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    reader->DeferNextRead();
    auto read_started_promise = std::make_shared<std::promise<void>>();
    auto read_started = read_started_promise->get_future();
    reader->SetOnAsyncCall(
        [read_started_promise]() { read_started_promise->set_value(); });
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    bool finalized_on_load_executor = false;
    auto options = Options();
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };
    auto load = Start(LoadCellsAsync(
        nullptr,
        std::move(cells),
        reader,
        [this, &finalized_on_load_executor](const auto&, int64_t) {
            finalized_on_load_executor = executor_.IsRunning();
            return std::make_unique<GroupChunk>();
        },
        std::move(options)));

    ASSERT_EQ(read_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto configure_started_promise = std::make_shared<std::promise<void>>();
    auto configure_started = configure_started_promise->get_future();
    auto configure =
        std::async(std::launch::async, [&pool, configure_started_promise]() {
            configure_started_promise->set_value();
            pool.Configure(0);
        });
    if (configure_started.wait_for(std::chrono::seconds(2)) !=
        std::future_status::ready) {
        reader->CompleteDeferredRead();
        EXPECT_EQ(std::move(load).get().size(), 1);
        configure.get();
        FAIL() << "local file I/O pool configuration thread did not start";
    }
    auto configure_status = configure.wait_for(std::chrono::seconds(2));
    if (configure_status != std::future_status::ready) {
        reader->CompleteDeferredRead();
        EXPECT_EQ(std::move(load).get().size(), 1);
        configure.get();
        FAIL() << "disabling the local file I/O pool waited for remote read";
    }
    configure.get();

    reader->CompleteDeferredRead();
    EXPECT_EQ(std::move(load).get().size(), 1);
    EXPECT_TRUE(finalized_on_load_executor);
}

TEST_F(AsyncLoadPipelineTest,
       DisablingLocalFileIOPoolDrainsQueuedFinalization) {
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto io_executor = pool.GetExecutor();
    ASSERT_TRUE(io_executor);
    auto* worker_executor =
        dynamic_cast<folly::CPUThreadPoolExecutor*>(io_executor.get());
    ASSERT_NE(worker_executor, nullptr);

    auto blocker_started_promise = std::make_shared<std::promise<void>>();
    auto blocker_started = blocker_started_promise->get_future();
    auto release_blocker_promise = std::make_shared<std::promise<void>>();
    auto release_blocker = release_blocker_promise->get_future().share();
    bool blocker_released = false;
    auto release_guard = folly::makeGuard([&]() {
        if (!blocker_released) {
            release_blocker_promise->set_value();
        }
    });
    io_executor->add([blocker_started_promise, release_blocker]() {
        blocker_started_promise->set_value();
        release_blocker.wait();
    });
    ASSERT_EQ(blocker_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    bool finalized = false;
    auto options = Options();
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };
    auto load = Start(LoadCellsAsync(
        nullptr,
        std::move(cells),
        reader,
        [&finalized](const auto&, int64_t) {
            finalized = true;
            return std::make_unique<GroupChunk>();
        },
        std::move(options)));

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (worker_executor->getPendingTaskCount() == 0 && !load.isReady() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GT(worker_executor->getPendingTaskCount(), 0);
    EXPECT_FALSE(load.isReady());

    io_executor.reset();
    auto configure_started_promise = std::make_shared<std::promise<void>>();
    auto configure_started = configure_started_promise->get_future();
    auto configure =
        std::async(std::launch::async, [&pool, configure_started_promise]() {
            configure_started_promise->set_value();
            pool.Configure(0);
        });
    if (configure_started.wait_for(std::chrono::seconds(2)) !=
        std::future_status::ready) {
        release_blocker_promise->set_value();
        blocker_released = true;
        release_guard.dismiss();
        EXPECT_EQ(std::move(load).get().size(), 1);
        configure.get();
        FAIL() << "local file I/O pool configuration thread did not start";
    }
    EXPECT_EQ(configure.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);

    release_blocker_promise->set_value();
    blocker_released = true;
    release_guard.dismiss();
    EXPECT_EQ(std::move(load).get().size(), 1);
    EXPECT_TRUE(finalized);
    EXPECT_NO_THROW(configure.get());
}

TEST_F(AsyncLoadPipelineTest,
       LocalFileIOPoolRunsQueuedHighFinalizationBeforeLowFinalization) {
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto io_executor = pool.GetExecutor();
    ASSERT_TRUE(io_executor);
    auto blocker_started_promise = std::make_shared<std::promise<void>>();
    auto blocker_started = blocker_started_promise->get_future();
    auto release_blocker_promise = std::make_shared<std::promise<void>>();
    auto release_blocker = release_blocker_promise->get_future().share();
    bool blocker_released = false;
    auto release_guard = folly::makeGuard([&]() {
        if (!blocker_released) {
            release_blocker_promise->set_value();
        }
    });
    io_executor->add([blocker_started_promise, release_blocker]() {
        blocker_started_promise->set_value();
        release_blocker.wait();
    });
    ASSERT_EQ(blocker_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    std::vector<int64_t> finalize_order;
    auto make_load = [this, &finalize_order, &io_executor](
                         milvus::proto::common::LoadPriority priority,
                         int64_t marker) {
        auto reader = std::make_shared<FakeChunkReader>(&executor_);
        std::vector<CellSpec> cells{{.cid = marker,
                                     .file_idx = 0,
                                     .local_rg_offset = 0,
                                     .rg_count = 1,
                                     .memory_size = 1}};
        auto options = Options(priority);
        options.finalization_executor_provider = [&io_executor]() {
            return io_executor.copy();
        };
        return Start(LoadCellsAsync(
            nullptr,
            std::move(cells),
            std::move(reader),
            [&finalize_order](const auto&, int64_t cid) {
                finalize_order.push_back(cid);
                return std::make_unique<GroupChunk>();
            },
            std::move(options)));
    };

    auto low = make_load(milvus::proto::common::LoadPriority::LOW, 1);
    auto high = make_load(milvus::proto::common::LoadPriority::HIGH, 2);
    EXPECT_FALSE(low.isReady());
    EXPECT_FALSE(high.isReady());

    release_blocker_promise->set_value();
    blocker_released = true;
    release_guard.dismiss();
    EXPECT_EQ(std::move(high).get().size(), 1);
    EXPECT_EQ(std::move(low).get().size(), 1);
    EXPECT_EQ(finalize_order, (std::vector<int64_t>{2, 1}));
}

TEST_F(AsyncLoadPipelineTest, FinalizesOnLocalFileIOPoolWhenRequested) {
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    bool finalized_on_io_pool = false;
    auto options = Options();
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };

    auto results = folly::coro::blockingWait(LoadCellsAsync(
        nullptr,
        std::move(cells),
        reader,
        [this, &finalized_on_io_pool](const auto& tables, int64_t) {
            EXPECT_FALSE(tables.empty());
            auto thread_name = folly::getCurrentThreadName().value_or("");
            finalized_on_io_pool = !executor_.IsRunning() &&
                                   thread_name.rfind("MILVUS_LF_IO_", 0) == 0;
            return std::make_unique<GroupChunk>();
        },
        std::move(options)));

    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(finalized_on_io_pool);
}

TEST_F(AsyncLoadPipelineTest,
       QueuesFinalizationWithoutBlockingLoadExecutorOrReleasingBudget) {
    budget_.SetCapacityBytes(1);
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto io_executor = pool.GetExecutor();
    ASSERT_TRUE(io_executor);

    auto io_started_promise = std::make_shared<std::promise<void>>();
    auto io_started = io_started_promise->get_future();
    auto release_io_promise = std::make_shared<std::promise<void>>();
    auto release_io = release_io_promise->get_future().share();
    folly::CPUThreadPoolExecutor load_executor(1);
    bool io_released = false;
    auto release_guard = folly::makeGuard([&]() {
        if (!io_released) {
            release_io_promise->set_value();
        }
    });
    io_executor->add([io_started_promise, release_io]() {
        io_started_promise->set_value();
        release_io.wait();
    });
    ASSERT_EQ(io_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    reader->DeferNextRead();
    auto read_started_promise = std::make_shared<std::promise<void>>();
    auto read_started = read_started_promise->get_future();
    reader->SetOnAsyncCall(
        [read_started_promise]() { read_started_promise->set_value(); });
    auto finalized = std::make_shared<std::atomic<bool>>(false);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options();
    options.executor = &load_executor;
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };
    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              std::move(cells),
                              reader,
                              [finalized](const auto&, int64_t) {
                                  finalized->store(true);
                                  return std::make_unique<GroupChunk>();
                              },
                              options))
                    .semi()
                    .via(folly::getKeepAliveToken(&load_executor));

    ASSERT_EQ(read_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    reader->CompleteDeferredRead();
    auto marker_promise = std::make_shared<std::promise<void>>();
    auto marker = marker_promise->get_future();
    load_executor.add([marker_promise]() { marker_promise->set_value(); });

    EXPECT_EQ(marker.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_FALSE(finalized->load());
    EXPECT_FALSE(load.isReady());
    auto next_lease_promise =
        std::make_shared<std::promise<storage::TransientBudgetLease>>();
    auto next_lease_future = next_lease_promise->get_future();
    auto next_budget =
        budget_.AcquireAsync(1, storage::TransientBudgetPriority::High)
            .toUnsafeFuture()
            .thenValueInline(
                [next_lease_promise](storage::TransientBudgetLease lease) {
                    next_lease_promise->set_value(std::move(lease));
                });
    EXPECT_EQ(next_lease_future.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);

    release_io_promise->set_value();
    io_released = true;
    release_guard.dismiss();
    EXPECT_EQ(std::move(load).get().size(), 1);
    EXPECT_TRUE(finalized->load());
    ASSERT_EQ(next_lease_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto next_lease = next_lease_future.get();
    next_lease.Release();
    std::move(next_budget).get();
}

TEST_F(AsyncLoadPipelineTest, SkipsQueuedFinalizationAfterCancellation) {
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto io_executor = pool.GetExecutor();
    ASSERT_TRUE(io_executor);

    auto io_started_promise = std::make_shared<std::promise<void>>();
    auto io_started = io_started_promise->get_future();
    auto release_io_promise = std::make_shared<std::promise<void>>();
    auto release_io = release_io_promise->get_future().share();
    folly::CPUThreadPoolExecutor load_executor(1);
    bool io_released = false;
    auto release_guard = folly::makeGuard([&]() {
        if (!io_released) {
            release_io_promise->set_value();
        }
    });
    io_executor->add([io_started_promise, release_io]() {
        io_started_promise->set_value();
        release_io.wait();
    });
    ASSERT_EQ(io_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    reader->DeferNextRead();
    auto read_started_promise = std::make_shared<std::promise<void>>();
    auto read_started = read_started_promise->get_future();
    reader->SetOnAsyncCall(
        [read_started_promise]() { read_started_promise->set_value(); });
    auto finalized = std::make_shared<std::atomic<bool>>(false);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options();
    options.executor = &load_executor;
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };
    auto load = std::move(LoadCellsAsync(
                              &ctx,
                              std::move(cells),
                              reader,
                              [finalized](const auto&, int64_t) {
                                  finalized->store(true);
                                  return std::make_unique<GroupChunk>();
                              },
                              options))
                    .semi()
                    .via(folly::getKeepAliveToken(&load_executor));

    ASSERT_EQ(read_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    reader->CompleteDeferredRead();
    auto marker_promise = std::make_shared<std::promise<void>>();
    auto marker = marker_promise->get_future();
    load_executor.add([marker_promise]() { marker_promise->set_value(); });
    ASSERT_EQ(marker.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    source.requestCancellation();

    release_io_promise->set_value();
    io_released = true;
    release_guard.dismiss();
    try {
        std::move(load).get();
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_FALSE(finalized->load());
}

TEST_F(AsyncLoadPipelineTest, PreservesFinalizerErrorAcrossLocalFileIOPool) {
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};

    auto options = Options();
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };
    try {
        folly::coro::blockingWait(LoadCellsAsync(
            nullptr,
            std::move(cells),
            reader,
            [](const auto&, int64_t) -> std::unique_ptr<GroupChunk> {
                throw SegcoreError(ErrorCode::StorageError, "finalize failed");
            },
            std::move(options)));
        FAIL() << "expected finalizer error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::StorageError);
    }
}

TEST_F(AsyncLoadPipelineTest, PreservesFileWriteErrorAcrossLocalFileIOPool) {
    if (access("/dev/full", W_OK) != 0) {
        GTEST_SKIP() << "/dev/full is unavailable";
    }
    auto& pool = storage::LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto previous_mode = storage::FileWriter::GetMode();
    auto previous_buffer_size = storage::FileWriter::GetBufferSize();
    auto restore_writer_config = folly::makeGuard([&]() {
        storage::FileWriter::SetMode(previous_mode);
        storage::FileWriter::SetBufferSize(previous_buffer_size);
    });
    storage::FileWriter::SetMode(storage::FileWriter::WriteMode::BUFFERED);
    storage::FileWriter::SetBufferSize(
        storage::FileWriter::DEFAULT_BUFFER_SIZE);
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};

    auto options = Options();
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };
    try {
        folly::coro::blockingWait(LoadCellsAsync(
            nullptr,
            std::move(cells),
            reader,
            [](const auto&, int64_t) -> std::unique_ptr<GroupChunk> {
                storage::FileWriter writer("/dev/full");
                const char data = 'x';
                writer.Write(&data, sizeof(data));
                writer.Finish();
                return std::make_unique<GroupChunk>();
            },
            std::move(options)));
        FAIL() << "expected file write failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileWriteFailed);
    }
}

TEST_F(AsyncLoadPipelineTest, SupportsSinglePriorityCustomExecutor) {
    AddOnlyInlineExecutor executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options();
    options.executor = &executor;

    auto results = folly::coro::blockingWait(LoadCellsAsync(
        nullptr,
        std::move(cells),
        reader,
        [](const std::vector<std::shared_ptr<arrow::Table>>& tables, int64_t) {
            EXPECT_FALSE(tables.empty());
            return std::make_unique<GroupChunk>();
        },
        options));

    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(reader->AsyncCalls(), 1);
    EXPECT_GT(executor.Calls(), 0);
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

    auto low = Start(
        LoadCellsAsync(nullptr,
                       cell(),
                       low_reader,
                       Finalizer(),
                       Options(milvus::proto::common::LoadPriority::LOW)));
    auto high = Start(
        LoadCellsAsync(nullptr,
                       cell(),
                       high_reader,
                       Finalizer(),
                       Options(milvus::proto::common::LoadPriority::HIGH)));

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
    auto future = Start(
        LoadCellsAsync(&ctx, std::move(cells), reader, Finalizer(), Options()));

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
        folly::coro::blockingWait(LoadCellsAsync(
            &ctx, std::move(cells), reader, std::move(finalizer), Options()));
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
        folly::coro::blockingWait(LoadCellsAsync(
            &ctx, std::move(cells), reader, std::move(finalizer), Options()));
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
            folly::coro::blockingWait(LoadCellsAsync(
                nullptr, std::move(cells), reader, Finalizer(), Options()));
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
