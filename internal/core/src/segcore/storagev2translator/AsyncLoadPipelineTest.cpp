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
#include <cstdlib>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
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
#include "folly/ExceptionWrapper.h"
#include "folly/Executor.h"
#include "folly/OperationCancelled.h"
#include "folly/ScopeGuard.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Task.h"
#include "folly/coro/WithCancellation.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/ManualExecutor.h"
#include "folly/futures/Future.h"
#include "folly/futures/Promise.h"
#include "folly/system/ThreadName.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/reader.h"
#include "segcore/storagev2translator/AsyncChunkReader.h"
#include "segcore/storagev2translator/AsyncLoadExecutor.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/EntryStreamUtils.h"
#include "storage/FileWriter.h"
#include "storage/LocalFileIOPool.h"

namespace milvus::segcore::storagev2translator {
namespace {

using ChunkReadResult =
    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>;
using ChunkReadFuture = folly::SemiFuture<ChunkReadResult>;

constexpr int64_t kTestSegmentId = 100;
constexpr size_t kChunkReaderOpenConcurrencyLimit = 16;

struct AsyncLoadExceptionCase {
    const char* name;
    std::exception_ptr exception;
    ErrorCode expected_code;
};

// Exercise the exception channel independently of Arrow Status conversion.
std::vector<AsyncLoadExceptionCase>
AsyncLoadExceptionCases() {
    return {
        {"typed",
         std::make_exception_ptr(SegcoreError(ErrorCode::StorageTransientError,
                                              "typed load failure")),
         ErrorCode::StorageTransientError},
        {"allocation",
         std::make_exception_ptr(std::bad_alloc{}),
         ErrorCode::MemAllocateFailed},
        {"cancellation",
         std::make_exception_ptr(folly::OperationCancelled{}),
         ErrorCode::FollyCancel},
        {"future cancellation",
         std::make_exception_ptr(folly::FutureCancellation{}),
         ErrorCode::FollyCancel},
        {"future failure",
         std::make_exception_ptr(folly::FutureInvalid{}),
         ErrorCode::FollyOtherException},
        {"untyped",
         std::make_exception_ptr(std::runtime_error(
             "bad_alloc timeout: this text must not classify the error")),
         ErrorCode::UnexpectedError},
        {"non-standard",
         std::make_exception_ptr(42),
         ErrorCode::UnexpectedError},
    };
}

// Builds open requests whose column-group indices match their request order.
[[nodiscard]] std::vector<ChunkReaderOpenSpec>
MakeChunkReaderOpenSpecs(const size_t count) {
    std::vector<ChunkReaderOpenSpec> specs;
    specs.reserve(count);
    for (size_t request_index = 0; request_index < count; ++request_index) {
        specs.push_back(
            {.column_group_index = static_cast<int64_t>(request_index)});
    }
    return specs;
}

using LoadCellsAsyncReturn = decltype(LoadCellsAsync(
    std::declval<milvus::OpContext*>(),
    std::declval<int64_t>(),
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

class RecordingManualExecutor : public folly::ManualExecutor {
 public:
    void
    add(folly::Func func) override {
        Add(std::move(func), folly::Executor::MID_PRI);
    }

    void
    addWithPriority(folly::Func func, int8_t priority) override {
        Add(std::move(func), priority);
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
    Add(folly::Func func, int8_t priority) {
        {
            std::lock_guard lock(mutex_);
            priorities_.push_back(priority);
        }
        folly::ManualExecutor::add(
            [this, func = std::move(func), priority]() mutable {
                auto* previous_executor = running_executor_;
                auto previous_priority = running_priority_;
                running_executor_ = this;
                running_priority_ = priority;
                auto restore = folly::makeGuard([&]() {
                    running_executor_ = previous_executor;
                    running_priority_ = previous_priority;
                });
                func();
            });
    }

    static thread_local RecordingManualExecutor* running_executor_;
    static thread_local int8_t running_priority_;
    mutable std::mutex mutex_;
    std::vector<int8_t> priorities_;
};

thread_local RecordingManualExecutor*
    RecordingManualExecutor::running_executor_ = nullptr;
thread_local int8_t RecordingManualExecutor::running_priority_ =
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

class ThrowOnCopyExecutorProvider {
 public:
    explicit ThrowOnCopyExecutorProvider(
        std::shared_ptr<std::atomic<bool>> should_throw)
        : should_throw_(std::move(should_throw)) {
    }

    ThrowOnCopyExecutorProvider(const ThrowOnCopyExecutorProvider& other)
        : should_throw_(other.should_throw_) {
        if (should_throw_->load()) {
            throw std::runtime_error("executor provider copy failed");
        }
    }

    ThrowOnCopyExecutorProvider(ThrowOnCopyExecutorProvider&&) noexcept =
        default;

    folly::Executor::KeepAlive<>
    operator()() const {
        return {};
    }

 private:
    std::shared_ptr<std::atomic<bool>> should_throw_;
};

class FakeChunkReader : public milvus_storage::api::ChunkReader {
 public:
    explicit FakeChunkReader(RecordingManualExecutor* executor)
        : executor_(executor) {
    }

    FakeChunkReader(RecordingManualExecutor* executor, const int64_t identifier)
        : executor_(executor), identifier_(identifier) {
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
            auto returned_indices = chunk_indices;
            if (return_one_fewer_batch_ && !returned_indices.empty()) {
                returned_indices.pop_back();
            }
            return folly::makeSemiFuture(MakeBatches(returned_indices));
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
    ReturnOneFewerBatch() {
        return_one_fewer_batch_ = true;
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
    FailDeferredRead(
        folly::exception_wrapper error =
            folly::make_exception_wrapper<std::runtime_error>("read failed")) {
        ASSERT_TRUE(deferred_read_ != nullptr);
        deferred_read_->setException(std::move(error));
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

    std::optional<int64_t>
    Identifier() const {
        return identifier_;
    }

    std::vector<std::vector<int64_t>>
    RequestedIndices() const {
        return requested_indices_;
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

    RecordingManualExecutor* executor_;
    std::optional<int64_t> identifier_;
    arrow::Status status_;
    std::atomic<size_t> async_calls_{0};
    std::atomic<bool> called_on_executor_{false};
    std::atomic<bool> deferred_continuation_on_executor_{false};
    std::atomic<int8_t> deferred_continuation_priority_{
        folly::Executor::MID_PRI};
    std::atomic<size_t> parallelism_{0};
    bool observe_deferred_continuation_{false};
    bool return_one_fewer_batch_{false};
    std::vector<std::vector<int64_t>> requested_indices_;
    std::function<void()> on_async_call_;
    std::shared_ptr<folly::Promise<ChunkReadResult>> deferred_read_;
};

using ChunkReaderOpenResult =
    arrow::Result<std::unique_ptr<milvus_storage::api::ChunkReader>>;

class FakeReader final : public milvus_storage::api::Reader {
 public:
    explicit FakeReader(RecordingManualExecutor* executor)
        : executor_(executor) {
    }

    std::shared_ptr<milvus_storage::api::ColumnGroups>
    get_column_groups() const override {
        return std::make_shared<milvus_storage::api::ColumnGroups>();
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>
    get_record_batch_reader(const std::string& /*predicate*/) const override {
        return arrow::Status::NotImplemented("unused in async-open tests");
    }

    ChunkReaderOpenResult
    get_chunk_reader(int64_t column_group_index,
                     const std::shared_ptr<std::vector<std::string>>&
                     /*needed_columns*/) const override {
        sync_calls_.fetch_add(1);
        return std::make_unique<FakeChunkReader>(executor_, column_group_index);
    }

    folly::SemiFuture<ChunkReaderOpenResult>
    get_chunk_reader_async(const int64_t column_group_index,
                           const std::shared_ptr<std::vector<std::string>>&
                               needed_columns) const override {
        auto pending =
            std::make_shared<PendingOpen>(column_group_index, needed_columns);
        auto future = pending->promise.getSemiFuture();
        std::function<void()> on_async_call;
        {
            std::lock_guard lock(mutex_);
            called_on_executor_.push_back(executor_ && executor_->IsRunning());
            priorities_.push_back(executor_ ? executor_->CurrentPriority()
                                            : folly::Executor::MID_PRI);
            pending_opens_.push_back(std::move(pending));
            on_async_call = on_async_call_;
        }
        if (on_async_call) {
            on_async_call();
        }
        return future;
    }

    arrow::Result<std::shared_ptr<arrow::Table>>
    take(const std::vector<int64_t>& /*row_indices*/,
         size_t /*parallelism*/,
         const std::shared_ptr<std::vector<std::string>>&
         /*needed_columns*/) override {
        return arrow::Status::NotImplemented("unused in async-open tests");
    }

    void
    set_keyretriever(const std::function<std::string(const std::string&)>&
                     /*callback*/) override {
    }

    void
    Complete(const size_t request_index) {
        const auto pending = Pending(request_index);
        pending->promise.setValue(std::make_unique<FakeChunkReader>(
            executor_, pending->column_group_index));
    }

    void
    Fail(const size_t request_index, arrow::Status status) {
        Pending(request_index)->promise.setValue(std::move(status));
    }

    void
    FailWithException(const size_t request_index,
                      folly::exception_wrapper error) {
        Pending(request_index)->promise.setException(std::move(error));
    }

    void
    CompleteWithNull(const size_t request_index) {
        Pending(request_index)
            ->promise.setValue(
                std::unique_ptr<milvus_storage::api::ChunkReader>{});
    }

    void
    SetOnAsyncCall(std::function<void()> on_async_call) {
        std::lock_guard lock(mutex_);
        on_async_call_ = std::move(on_async_call);
    }

    size_t
    AsyncCalls() const {
        std::lock_guard lock(mutex_);
        return pending_opens_.size();
    }

    size_t
    SyncCalls() const {
        return sync_calls_.load();
    }

    std::vector<bool>
    CalledOnExecutor() const {
        std::lock_guard lock(mutex_);
        return called_on_executor_;
    }

    std::vector<int8_t>
    Priorities() const {
        std::lock_guard lock(mutex_);
        return priorities_;
    }

 private:
    struct PendingOpen {
        PendingOpen(const int64_t column_group_index,
                    std::shared_ptr<std::vector<std::string>> needed_columns)
            : column_group_index(column_group_index),
              needed_columns(std::move(needed_columns)) {
        }

        int64_t column_group_index;
        std::shared_ptr<std::vector<std::string>> needed_columns;
        folly::Promise<ChunkReaderOpenResult> promise;
    };

    std::shared_ptr<PendingOpen>
    Pending(const size_t request_index) const {
        std::lock_guard lock(mutex_);
        return pending_opens_.at(request_index);
    }

    RecordingManualExecutor* executor_;
    mutable std::mutex mutex_;
    mutable std::atomic<size_t> sync_calls_{0};
    mutable std::vector<std::shared_ptr<PendingOpen>> pending_opens_;
    mutable std::vector<bool> called_on_executor_;
    mutable std::vector<int8_t> priorities_;
    mutable std::function<void()> on_async_call_;
};

class SyncFallbackReader final : public milvus_storage::api::Reader {
 public:
    explicit SyncFallbackReader(RecordingManualExecutor* executor)
        : executor_(executor) {
    }

    std::shared_ptr<milvus_storage::api::ColumnGroups>
    get_column_groups() const override {
        return std::make_shared<milvus_storage::api::ColumnGroups>();
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>
    get_record_batch_reader(const std::string& /*predicate*/) const override {
        return arrow::Status::NotImplemented("unused in async-open tests");
    }

    ChunkReaderOpenResult
    get_chunk_reader(const int64_t column_group_index,
                     const std::shared_ptr<std::vector<std::string>>&
                     /*needed_columns*/) const override {
        called_on_executor_.store(executor_ && executor_->IsRunning());
        return std::make_unique<FakeChunkReader>(executor_, column_group_index);
    }

    arrow::Result<std::shared_ptr<arrow::Table>>
    take(const std::vector<int64_t>& /*row_indices*/,
         size_t /*parallelism*/,
         const std::shared_ptr<std::vector<std::string>>&
         /*needed_columns*/) override {
        return arrow::Status::NotImplemented("unused in async-open tests");
    }

    void
    set_keyretriever(const std::function<std::string(const std::string&)>&
                     /*callback*/) override {
    }

    bool
    CalledOnExecutor() const {
        return called_on_executor_.load();
    }

 private:
    RecordingManualExecutor* executor_;
    mutable std::atomic<bool> called_on_executor_{false};
};

class AsyncLoadPipelineTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        budget_.SetCapacityBytes(1024);
        budget_.SetCapacitySlots(0);
    }

    void
    TearDown() override {
        storage::LocalFileIOPool::GetInstance().Configure(0);
        budget_.SetCapacityBytes(0);
        budget_.SetCapacitySlots(0);
    }

    AsyncLoadPipelineOptions
    Options(milvus::proto::common::LoadPriority priority =
                milvus::proto::common::LoadPriority::HIGH) {
        return {.read_window_bytes = 8,
                .load_priority = priority,
                .executor = folly::getKeepAliveToken(executor_)};
    }

    AsyncChunkReaderOpenOptions
    OpenOptions(milvus::proto::common::LoadPriority priority =
                    milvus::proto::common::LoadPriority::HIGH) {
        return {.load_priority = priority,
                .executor = folly::getKeepAliveToken(executor_)};
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

    template <typename T>
    folly::Future<T>
    Start(folly::coro::Task<T> task) {
        auto future =
            std::move(task).semi().via(folly::getKeepAliveToken(&executor_));
        executor_.drain();
        return future;
    }

    template <typename T>
    T
    Get(folly::Future<T> future) {
        auto drain = folly::makeGuard([this]() { executor_.drain(); });
        executor_.waitFor(future);
        return std::move(future).get();
    }

    template <typename T>
    T
    Run(folly::coro::Task<T> task) {
        return Get(
            std::move(task).semi().via(folly::getKeepAliveToken(&executor_)));
    }

    // Assert the exception and the C boundary agree on the original category.
    template <typename F>
    void
    ExpectClassifiedFailure(F&& operation, const ErrorCode expected_code) {
        try {
            operation();
            FAIL() << "expected load failure";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), expected_code);
            auto status = FailureCStatus(&error);
            EXPECT_EQ(status.error_code, expected_code);
            EXPECT_STREQ(status.error_msg, error.what());
            std::free(const_cast<char*>(status.error_msg));
        } catch (const std::exception& error) {
            ADD_FAILURE() << "unclassified exception: " << error.what();
        } catch (...) {
            ADD_FAILURE() << "unclassified non-standard exception";
        }
    }

    storage::LoadAdmissionController& budget_ =
        storage::LoadAdmissionController::GetInstance();
    RecordingManualExecutor executor_;
};

TEST_F(AsyncLoadPipelineTest,
       OpensChunkReadersConcurrentlyAndPreservesRequestOrder) {
    auto reader = std::make_shared<FakeReader>(&executor_);
    std::vector<ChunkReaderOpenSpec> specs{
        {.column_group_index = 7,
         .needed_columns =
             std::make_shared<std::vector<std::string>>(1, "first")},
        {.column_group_index = 3,
         .needed_columns =
             std::make_shared<std::vector<std::string>>(1, "second")},
    };

    auto future = Start(OpenChunkReadersAsync(
        nullptr, kTestSegmentId, reader, std::move(specs), OpenOptions()));

    EXPECT_EQ(reader->SyncCalls(), 0);
    EXPECT_EQ(reader->AsyncCalls(), 2);
    EXPECT_FALSE(future.isReady());
    EXPECT_EQ(reader->CalledOnExecutor(), (std::vector<bool>{true, true}));
    EXPECT_EQ(reader->Priorities(),
              (std::vector<int8_t>{folly::Executor::HI_PRI,
                                   folly::Executor::HI_PRI}));

    reader->Complete(1);
    executor_.drain();
    EXPECT_FALSE(future.isReady());

    reader->Complete(0);
    const auto opened = Get(std::move(future));
    ASSERT_EQ(opened.size(), 2);
    ASSERT_NE(dynamic_cast<FakeChunkReader*>(opened[0].get()), nullptr);
    ASSERT_NE(dynamic_cast<FakeChunkReader*>(opened[1].get()), nullptr);
    EXPECT_EQ(dynamic_cast<FakeChunkReader*>(opened[0].get())->Identifier(),
              std::optional<int64_t>{7});
    EXPECT_EQ(dynamic_cast<FakeChunkReader*>(opened[1].get())->Identifier(),
              std::optional<int64_t>{3});
}

TEST_F(AsyncLoadPipelineTest, LimitsConcurrentChunkReaderOpens) {
    constexpr size_t kRequestCount = kChunkReaderOpenConcurrencyLimit + 1;

    auto reader = std::make_shared<FakeReader>(&executor_);
    auto specs = MakeChunkReaderOpenSpecs(kRequestCount);

    auto future = Start(OpenChunkReadersAsync(
        nullptr, kTestSegmentId, reader, std::move(specs), OpenOptions()));

    EXPECT_EQ(reader->AsyncCalls(), kChunkReaderOpenConcurrencyLimit);
    EXPECT_FALSE(future.isReady());

    reader->Complete(0);
    executor_.drain();
    EXPECT_EQ(reader->AsyncCalls(), kRequestCount);
    EXPECT_FALSE(future.isReady());

    for (size_t request_index = kRequestCount - 1; request_index > 0;
         --request_index) {
        reader->Complete(request_index);
    }
    const auto opened = Get(std::move(future));
    ASSERT_EQ(opened.size(), kRequestCount);
    for (size_t request_index = 0; request_index < kRequestCount;
         ++request_index) {
        const auto* chunk_reader =
            dynamic_cast<FakeChunkReader*>(opened[request_index].get());
        ASSERT_NE(chunk_reader, nullptr);
        EXPECT_EQ(chunk_reader->Identifier(),
                  std::optional<int64_t>{static_cast<int64_t>(request_index)});
    }
}

TEST_F(AsyncLoadPipelineTest, RunsSynchronousFallbackFactoryOnExecutor) {
    auto reader = std::make_shared<SyncFallbackReader>(&executor_);
    std::vector<ChunkReaderOpenSpec> specs{{.column_group_index = 5}};

    const auto opened = Run(OpenChunkReadersAsync(
        nullptr, kTestSegmentId, reader, std::move(specs), OpenOptions()));

    ASSERT_EQ(opened.size(), 1);
    EXPECT_TRUE(reader->CalledOnExecutor());
}

TEST_F(AsyncLoadPipelineTest, MapsChunkReaderOpenStorageErrors) {
    const std::vector<std::pair<arrow::Status, ErrorCode>> cases{
        {milvus_storage::MakeExtendError(
             milvus_storage::ExtendStatusCode::StorageTransientThrottling,
             "throttled"),
         ErrorCode::StorageTransientError},
        {arrow::Status::Invalid("corrupt metadata"),
         ErrorCode::DataFormatBroken},
        {arrow::Status::OutOfMemory("allocation failed"),
         ErrorCode::MemAllocateFailed},
    };

    for (const auto& [status, expected_error_code] : cases) {
        SCOPED_TRACE(status.ToString());
        auto reader = std::make_shared<FakeReader>(&executor_);
        std::vector<ChunkReaderOpenSpec> specs{{.column_group_index = 9}};
        auto future = Start(OpenChunkReadersAsync(
            nullptr, kTestSegmentId, reader, std::move(specs), OpenOptions()));
        ASSERT_EQ(reader->AsyncCalls(), 1);

        reader->Fail(0, status);
        try {
            Get(std::move(future));
            FAIL() << "expected storage error";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), expected_error_code);
        }
        EXPECT_EQ(reader->SyncCalls(), 0);
    }
}

TEST_F(AsyncLoadPipelineTest, ClassifiesChunkReaderOpenExceptions) {
    for (const auto& test_case : AsyncLoadExceptionCases()) {
        for (const bool synchronous : {false, true}) {
            SCOPED_TRACE(test_case.name);
            SCOPED_TRACE(synchronous);
            auto reader = std::make_shared<FakeReader>(&executor_);
            if (synchronous) {
                reader->SetOnAsyncCall([&test_case]() {
                    std::rethrow_exception(test_case.exception);
                });
            }
            auto future = Start(OpenChunkReadersAsync(
                nullptr,
                kTestSegmentId,
                reader,
                MakeChunkReaderOpenSpecs(kChunkReaderOpenConcurrencyLimit + 1),
                OpenOptions()));
            if (!synchronous) {
                ASSERT_EQ(reader->AsyncCalls(),
                          kChunkReaderOpenConcurrencyLimit);
                reader->FailWithException(
                    kChunkReaderOpenConcurrencyLimit - 1,
                    folly::exception_wrapper(test_case.exception));
                executor_.drain();
                EXPECT_EQ(reader->AsyncCalls(),
                          kChunkReaderOpenConcurrencyLimit);
                EXPECT_FALSE(future.isReady());
                // A later error must not replace the first exception while
                // already-issued opens are being drained.
                reader->Fail(0, arrow::Status::Invalid("later open failure"));
                for (size_t i = 1; i + 1 < kChunkReaderOpenConcurrencyLimit;
                     ++i) {
                    reader->Complete(i);
                }
            } else {
                EXPECT_EQ(reader->AsyncCalls(), 1);
            }
            ExpectClassifiedFailure([&]() { Get(std::move(future)); },
                                    test_case.expected_code);
        }
    }
}

TEST_F(AsyncLoadPipelineTest, ClassifiesReadExceptionsAndReleasesBudget) {
    budget_.SetCapacityBytes(1);
    budget_.SetCapacitySlots(1);
    for (const auto& test_case : AsyncLoadExceptionCases()) {
        for (const bool synchronous : {false, true}) {
            SCOPED_TRACE(test_case.name);
            SCOPED_TRACE(synchronous);
            auto reader = std::make_shared<FakeChunkReader>(&executor_);
            if (synchronous) {
                reader->SetOnAsyncCall([&test_case]() {
                    std::rethrow_exception(test_case.exception);
                });
            } else {
                reader->DeferNextRead();
            }
            auto options = Options();
            options.read_window_bytes = 1;
            auto future =
                Start(LoadCellsAsync(nullptr,
                                     kTestSegmentId,
                                     {{0, 0, 0, 1, 1}, {1, 0, 1, 1, 1}},
                                     reader,
                                     Finalizer(),
                                     std::move(options)));
            if (!synchronous) {
                EXPECT_FALSE(future.isReady());
                reader->FailDeferredRead(
                    folly::exception_wrapper(test_case.exception));
            }
            ExpectClassifiedFailure([&]() { Get(std::move(future)); },
                                    test_case.expected_code);
            EXPECT_EQ(reader->AsyncCalls(), 1);
            // Probe the full capacity without blocking if a lease leaked.
            const bool acquired = budget_.TryAcquire(
                {1, 1}, storage::LoadAdmissionPriority::High);
            EXPECT_TRUE(acquired);
            if (acquired) {
                budget_.Release({1, 1});
            }
        }
    }
}

TEST_F(AsyncLoadPipelineTest, ClassifiesFinalizerExceptionsAndReleasesBudget) {
    budget_.SetCapacityBytes(1);
    budget_.SetCapacitySlots(1);
    for (const auto& test_case : AsyncLoadExceptionCases()) {
        for (const bool dedicated_executor : {false, true}) {
            SCOPED_TRACE(test_case.name);
            SCOPED_TRACE(dedicated_executor);
            RecordingManualExecutor finalization_executor;
            auto reader = std::make_shared<FakeChunkReader>(&executor_);
            auto options = Options();
            options.read_window_bytes = 1;
            if (dedicated_executor) {
                options.finalization_executor_provider = [&]() {
                    return folly::getKeepAliveToken(finalization_executor);
                };
            }
            auto future = Start(LoadCellsAsync(
                nullptr,
                kTestSegmentId,
                {{0, 0, 0, 1, 1}, {1, 0, 1, 1, 1}},
                reader,
                [&test_case](const auto&,
                             int64_t) -> std::unique_ptr<GroupChunk> {
                    std::rethrow_exception(test_case.exception);
                },
                std::move(options)));
            finalization_executor.drain();
            ExpectClassifiedFailure([&]() { Get(std::move(future)); },
                                    test_case.expected_code);
            EXPECT_EQ(reader->AsyncCalls(), 1);
            const bool acquired = budget_.TryAcquire(
                {1, 1}, storage::LoadAdmissionPriority::High);
            EXPECT_TRUE(acquired);
            if (acquired) {
                budget_.Release({1, 1});
            }
        }
    }
}

TEST_F(AsyncLoadPipelineTest, ClassifiesSynchronousExecutorSetupExceptions) {
    class FailingExecutor : public RecordingManualExecutor {
     public:
        explicit FailingExecutor(std::exception_ptr failure)
            : failure_(std::move(failure)) {
        }

        uint8_t
        getNumPriorities() const override {
            std::rethrow_exception(failure_);
        }

     private:
        std::exception_ptr failure_;
    };

    for (const auto& test_case : AsyncLoadExceptionCases()) {
        SCOPED_TRACE(test_case.name);
        FailingExecutor executor(test_case.exception);
        auto reader = std::make_shared<FakeReader>(&executor_);
        auto chunk_reader = std::make_shared<FakeChunkReader>(&executor_);
        auto open_options = OpenOptions();
        open_options.executor = folly::getKeepAliveToken(executor);
        ExpectClassifiedFailure(
            [&]() {
                Run(OpenChunkReadersAsync(nullptr,
                                          kTestSegmentId,
                                          reader,
                                          MakeChunkReaderOpenSpecs(1),
                                          std::move(open_options)));
            },
            test_case.expected_code);
        auto options = Options();
        options.executor = folly::getKeepAliveToken(executor);
        ExpectClassifiedFailure(
            [&]() {
                Run(LoadCellsAsync(nullptr,
                                   kTestSegmentId,
                                   {{0, 0, 0, 1, 1}},
                                   chunk_reader,
                                   Finalizer(),
                                   std::move(options)));
            },
            test_case.expected_code);
    }
}

TEST_F(AsyncLoadPipelineTest, DoesNotRefillChunkReaderOpenWindowAfterFailure) {
    struct FailureCase {
        const char* name;
        std::function<void(FakeReader&, size_t)> fail;
        ErrorCode expected_code;
        const char* expected_message;
    };
    const std::vector<FailureCase> cases{
        {"throttling",
         [](FakeReader& reader, size_t index) {
             reader.Fail(index,
                         milvus_storage::MakeExtendError(
                             milvus_storage::ExtendStatusCode::
                                 StorageTransientThrottling,
                             "first open failure"));
         },
         ErrorCode::StorageTransientError,
         "first open failure"},
        {"invalid metadata",
         [](FakeReader& reader, size_t index) {
             reader.Fail(index, arrow::Status::Invalid("first open failure"));
         },
         ErrorCode::DataFormatBroken,
         "first open failure"},
        {"out of memory",
         [](FakeReader& reader, size_t index) {
             reader.Fail(index,
                         arrow::Status::OutOfMemory("first open failure"));
         },
         ErrorCode::MemAllocateFailed,
         "first open failure"},
        {"exceptional future",
         [](FakeReader& reader, size_t index) {
             reader.FailWithException(
                 index,
                 folly::make_exception_wrapper<SegcoreError>(
                     ErrorCode::StorageTransientError, "first open failure"));
         },
         ErrorCode::StorageTransientError,
         "first open failure"},
        {"null reader",
         [](FakeReader& reader, size_t index) {
             reader.CompleteWithNull(index);
         },
         ErrorCode::UnexpectedError,
         "request index 15"},
    };
    constexpr size_t kRequestCount = 2 * kChunkReaderOpenConcurrencyLimit + 1;
    constexpr size_t kFailedIndex = kChunkReaderOpenConcurrencyLimit - 1;

    for (const auto& test_case : cases) {
        SCOPED_TRACE(test_case.name);
        auto reader = std::make_shared<FakeReader>(&executor_);
        auto future =
            Start(OpenChunkReadersAsync(nullptr,
                                        kTestSegmentId,
                                        reader,
                                        MakeChunkReaderOpenSpecs(kRequestCount),
                                        OpenOptions()));
        ASSERT_EQ(reader->AsyncCalls(), kChunkReaderOpenConcurrencyLimit);

        test_case.fail(*reader, kFailedIndex);
        executor_.drain();
        EXPECT_EQ(reader->AsyncCalls(), kChunkReaderOpenConcurrencyLimit);
        EXPECT_FALSE(future.isReady());

        // A later failure at a lower request index must not replace the cause.
        reader->Fail(0, arrow::Status::IOError("later open failure"));
        executor_.drain();
        EXPECT_FALSE(future.isReady());

        // Drain even unexpected refills so a regression fails without hanging.
        for (size_t index = 1; index < reader->AsyncCalls(); ++index) {
            if (index != kFailedIndex) {
                reader->Complete(index);
                executor_.drain();
            }
        }
        EXPECT_EQ(reader->AsyncCalls(), kChunkReaderOpenConcurrencyLimit);
        try {
            Get(std::move(future));
            FAIL() << "expected the first open failure";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), test_case.expected_code);
            EXPECT_NE(
                std::string(error.what()).find(test_case.expected_message),
                std::string::npos);
        }
    }
}

TEST_F(AsyncLoadPipelineTest, StopsChunkReaderOpensAfterSynchronousFailure) {
    auto reader = std::make_shared<FakeReader>(&executor_);
    reader->SetOnAsyncCall([]() {
        throw SegcoreError(ErrorCode::StorageTransientError,
                           "synchronous open failure");
    });

    try {
        Run(OpenChunkReadersAsync(
            nullptr,
            kTestSegmentId,
            reader,
            MakeChunkReaderOpenSpecs(kChunkReaderOpenConcurrencyLimit + 1),
            OpenOptions()));
        FAIL() << "expected a synchronous factory failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::StorageTransientError);
        EXPECT_STREQ(error.what(), "synchronous open failure");
    }
    EXPECT_EQ(reader->AsyncCalls(), 1);
}

TEST_F(AsyncLoadPipelineTest,
       PreservesExternalCancellationDuringChunkReaderOpenFailureDrain) {
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeReader>(&executor_);
    auto future = Start(OpenChunkReadersAsync(&ctx,
                                              kTestSegmentId,
                                              reader,
                                              MakeChunkReaderOpenSpecs(2),
                                              OpenOptions()));
    reader->Fail(0, arrow::Status::Invalid("corrupt metadata"));
    executor_.drain();
    EXPECT_FALSE(future.isReady());

    source.requestCancellation();
    reader->Complete(1);
    try {
        Get(std::move(future));
        FAIL() << "expected external cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
}

TEST_F(AsyncLoadPipelineTest,
       CapturesChunkReaderOpenCancellationBeforeTaskStarts) {
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeReader>(&executor_);
    std::vector<ChunkReaderOpenSpec> specs{{.column_group_index = 2}};
    auto task = OpenChunkReadersAsync(
        &ctx, kTestSegmentId, reader, std::move(specs), OpenOptions());

    ctx.cancellation_token = {};
    source.requestCancellation();

    try {
        Run(std::move(task));
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(reader->AsyncCalls(), 0);
    EXPECT_EQ(reader->SyncCalls(), 0);
}

TEST_F(AsyncLoadPipelineTest,
       StopsDispatchingChunkReaderOpensAfterContextCancellation) {
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeReader>(&executor_);
    reader->SetOnAsyncCall([&source]() { source.requestCancellation(); });
    std::vector<ChunkReaderOpenSpec> specs{{.column_group_index = 2},
                                           {.column_group_index = 4},
                                           {.column_group_index = 6}};

    auto future = Start(OpenChunkReadersAsync(
        &ctx, kTestSegmentId, reader, std::move(specs), OpenOptions()));
    const auto dispatched_opens = reader->AsyncCalls();
    for (size_t i = 0; i < dispatched_opens; ++i) {
        reader->Complete(i);
    }

    try {
        Get(std::move(future));
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(dispatched_opens, 1);
}

TEST_F(AsyncLoadPipelineTest,
       DoesNotRefillChunkReaderOpenWindowAfterCancellation) {
    constexpr size_t kRequestCount = kChunkReaderOpenConcurrencyLimit + 1;

    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeReader>(&executor_);
    auto specs = MakeChunkReaderOpenSpecs(kRequestCount);

    auto future = Start(OpenChunkReadersAsync(
        &ctx, kTestSegmentId, reader, std::move(specs), OpenOptions()));
    const auto dispatched_opens = reader->AsyncCalls();
    EXPECT_EQ(dispatched_opens, kChunkReaderOpenConcurrencyLimit);

    source.requestCancellation();
    reader->Complete(0);
    executor_.drain();
    EXPECT_EQ(reader->AsyncCalls(), kChunkReaderOpenConcurrencyLimit);
    EXPECT_FALSE(future.isReady());

    for (size_t request_index = 1; request_index < dispatched_opens;
         ++request_index) {
        reader->Complete(request_index);
    }
    try {
        Get(std::move(future));
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(reader->AsyncCalls(), kChunkReaderOpenConcurrencyLimit);
}

TEST_F(AsyncLoadPipelineTest,
       DrainsPendingChunkReaderOpensBeforeReportingCancellation) {
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeReader>(&executor_);
    std::vector<ChunkReaderOpenSpec> specs{{.column_group_index = 2},
                                           {.column_group_index = 4}};
    auto future = Start(OpenChunkReadersAsync(
        &ctx, kTestSegmentId, reader, std::move(specs), OpenOptions()));
    ASSERT_EQ(reader->AsyncCalls(), 2);

    source.requestCancellation();
    reader->Complete(0);
    executor_.drain();
    EXPECT_FALSE(future.isReady());

    reader->Complete(1);
    try {
        Get(std::move(future));
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
}

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

    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    const auto results = Run(LoadCellsAsync(
        nullptr, kTestSegmentId, cells, reader, Finalizer(), Options()));

    ASSERT_EQ(results.size(), cells.size());
    const auto requested_indices = reader->RequestedIndices();
    ASSERT_EQ(requested_indices.size(), 3);
    EXPECT_EQ(requested_indices[0], (std::vector<int64_t>{0, 1, 2, 3}));
    EXPECT_EQ(requested_indices[1], (std::vector<int64_t>{4, 5}));
    EXPECT_EQ(requested_indices[2], (std::vector<int64_t>{8}));
}

TEST_F(AsyncLoadPipelineTest, RejectsZeroReadWindow) {
    std::vector<CellSpec> cells{
        {.cid = 0,
         .file_idx = 0,
         .local_rg_offset = 0,
         .rg_count = 1,
         .memory_size = 8},
    };

    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    auto options = Options();
    options.read_window_bytes = 0;
    EXPECT_THROW(Run(LoadCellsAsync(nullptr,
                                    kTestSegmentId,
                                    std::move(cells),
                                    std::move(reader),
                                    Finalizer(),
                                    std::move(options))),
                 SegcoreError);
}

TEST_F(AsyncLoadPipelineTest, AsyncReadWindowConfigRejectsNonPositiveValues) {
    auto previous = StorageV2AsyncLoadReadWindowSizeBytes();
    auto restore = folly::makeGuard(
        [previous]() { SetStorageV2AsyncLoadReadWindowSizeBytes(previous); });

    SetStorageV2AsyncLoadReadWindowSizeBytes(0);
    EXPECT_EQ(StorageV2AsyncLoadReadWindowSizeBytes(),
              kDefaultStorageV2AsyncLoadReadWindowSizeBytes);

    SetStorageV2AsyncLoadReadWindowSizeBytes(32 * 1024 * 1024);
    EXPECT_EQ(StorageV2AsyncLoadReadWindowSizeBytes(), 32 * 1024 * 1024);

    SetStorageV2AsyncLoadReadWindowSizeBytes(-1);
    EXPECT_EQ(StorageV2AsyncLoadReadWindowSizeBytes(),
              kDefaultStorageV2AsyncLoadReadWindowSizeBytes);
}

TEST_F(AsyncLoadPipelineTest, DefaultOptionsUseConfiguredReadWindow) {
    auto previous = StorageV2AsyncLoadReadWindowSizeBytes();
    auto restore = folly::makeGuard(
        [previous]() { SetStorageV2AsyncLoadReadWindowSizeBytes(previous); });
    std::vector<CellSpec> cells{
        {.cid = 0,
         .file_idx = 0,
         .local_rg_offset = 0,
         .rg_count = 1,
         .memory_size = 8},
        {.cid = 1,
         .file_idx = 0,
         .local_rg_offset = 1,
         .rg_count = 1,
         .memory_size = 8},
        {.cid = 2,
         .file_idx = 0,
         .local_rg_offset = 2,
         .rg_count = 1,
         .memory_size = 8},
    };
    auto run = [this, &cells](int64_t window_size) {
        SetStorageV2AsyncLoadReadWindowSizeBytes(window_size);
        auto reader = std::make_shared<FakeChunkReader>(&executor_);
        auto options = AsyncLoadPipelineOptions{};
        options.executor = folly::getKeepAliveToken(executor_);
        auto results = Run(LoadCellsAsync(
            nullptr,
            kTestSegmentId,
            cells,
            reader,
            [](const auto&, int64_t) { return std::make_unique<GroupChunk>(); },
            std::move(options)));
        EXPECT_EQ(results.size(), cells.size());
        return reader->AsyncCalls();
    };

    EXPECT_EQ(run(kDefaultStorageV2AsyncLoadReadWindowSizeBytes), 1);
    EXPECT_EQ(run(1), cells.size());
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

    auto results = Run(LoadCellsAsync(nullptr,
                                      kTestSegmentId,
                                      std::move(cells),
                                      reader,
                                      Finalizer(&finalized),
                                      Options()));

    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0].first, 2);
    EXPECT_EQ(results[1].first, 0);
    EXPECT_EQ(results[2].first, 1);
    EXPECT_EQ(reader->Parallelism(), 1);
    EXPECT_TRUE(reader->CalledOnExecutor());
}

TEST(AsyncLoadExecutorTest, ResizesExistingExecutorWithQueuedWork) {
    const int previous = GetAsyncLoadThreadPoolSize();
    auto restore =
        folly::makeGuard([&]() { SetAsyncLoadThreadPoolSize(previous); });
    SetAsyncLoadThreadPoolSize(1);
    auto executor =
        ResolveAsyncLoadExecutor({}, milvus::proto::common::LoadPriority::HIGH);
    std::promise<void> release;
    auto released = release.get_future().share();
    std::promise<void> first_started;
    std::promise<void> second_started;
    std::promise<void> first_done;
    std::promise<void> second_done;
    auto first_completion = first_done.get_future();
    auto second_completion = second_done.get_future();
    bool first_submitted = false;
    bool second_submitted = false;
    auto unblock = folly::makeGuard([&]() {
        release.set_value();
        if (first_submitted && first_completion.valid()) {
            first_completion.wait();
        }
        if (second_submitted && second_completion.valid()) {
            second_completion.wait();
        }
    });
    executor->add([&]() {
        first_started.set_value();
        released.wait();
        first_done.set_value();
    });
    first_submitted = true;
    EXPECT_EQ(first_started.get_future().wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    executor->add([&]() {
        second_started.set_value();
        released.wait();
        second_done.set_value();
    });
    second_submitted = true;
    auto second_start = second_started.get_future();
    EXPECT_EQ(second_start.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);

    SetAsyncLoadThreadPoolSize(2);
    EXPECT_EQ(GetAsyncLoadThreadPoolSize(), 2);
    EXPECT_EQ(second_start.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    release.set_value();
    unblock.dismiss();
    first_completion.get();
    second_completion.get();

    SetAsyncLoadThreadPoolSize(1);
    EXPECT_EQ(GetAsyncLoadThreadPoolSize(), 1);
    // A keep-alive acquired before resizing still submits to the same pool.
    std::promise<void> after_resize;
    auto completion = after_resize.get_future();
    executor->add([&]() { after_resize.set_value(); });
    EXPECT_EQ(completion.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    completion.get();
}

TEST(AsyncLoadExecutorTest, InvalidSizePreservesConfiguration) {
    const int previous = GetAsyncLoadThreadPoolSize();
    for (const int value : {0, -1}) {
        EXPECT_THROW(SetAsyncLoadThreadPoolSize(value), milvus::SegcoreError);
        EXPECT_EQ(GetAsyncLoadThreadPoolSize(), previous);
    }
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
    options.executor.reset();

    auto results = Run(LoadCellsAsync(
        nullptr,
        kTestSegmentId,
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

    auto future = std::move(LoadCellsAsync(nullptr,
                                           kTestSegmentId,
                                           std::move(cells),
                                           reader,
                                           Finalizer(),
                                           options))
                      .semi()
                      .via(folly::getKeepAliveToken(&caller_executor),
                           folly::Executor::HI_PRI);

    executor_.drain();
    ASSERT_FALSE(future.isReady());
    ASSERT_EQ(reader->AsyncCalls(), 1);
    reader->CompleteDeferredRead();
    auto results = Get(std::move(future));

    EXPECT_EQ(results.size(), 1);
    EXPECT_TRUE(reader->DeferredContinuationRanOnExecutor());
    EXPECT_EQ(reader->DeferredContinuationPriority(), folly::Executor::LO_PRI);
}

TEST_F(AsyncLoadPipelineTest, ReleasesBudgetWhenStorageFutureFails) {
    budget_.SetCapacityBytes(1);
    budget_.SetCapacitySlots(1);
    InlineRecordingExecutor caller_executor;
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    reader->DeferNextRead();
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options(milvus::proto::common::LoadPriority::LOW);
    auto load = std::move(LoadCellsAsync(nullptr,
                                         kTestSegmentId,
                                         std::move(cells),
                                         reader,
                                         Finalizer(),
                                         options))
                    .semi()
                    .via(folly::getKeepAliveToken(&caller_executor),
                         folly::Executor::HI_PRI);

    executor_.drain();
    ASSERT_FALSE(load.isReady());
    ASSERT_EQ(reader->AsyncCalls(), 1);

    auto next_budget =
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High);
    ASSERT_FALSE(next_budget.isReady());

    reader->FailDeferredRead();

    try {
        Get(std::move(load));
        FAIL() << "expected read failure";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "read failed");
    }
    ASSERT_TRUE(next_budget.isReady());
    auto next_lease = folly::coro::blockingWait(std::move(next_budget));
    next_lease.Release();
}

TEST_F(AsyncLoadPipelineTest, CancelsPendingWindowsAfterFirstReadFailure) {
    budget_.SetCapacityBytes(1);
    budget_.SetCapacitySlots(1);
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    reader->SetStatus(arrow::Status::IOError("read failed"));
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
    auto options = Options();
    options.read_window_bytes = 1;

    try {
        Run(LoadCellsAsync(nullptr,
                           kTestSegmentId,
                           std::move(cells),
                           reader,
                           Finalizer(),
                           std::move(options)));
        FAIL() << "expected read failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::StorageError);
    }
    EXPECT_EQ(reader->AsyncCalls(), 1);
}

TEST_F(AsyncLoadPipelineTest, PublishesReadFailureBeforeReleasingWindowBudget) {
    budget_.SetCapacityBytes(1);
    budget_.SetCapacitySlots(1);
    InlineRecordingExecutor load_executor;
    InlineRecordingExecutor caller_executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    reader->DeferNextRead();
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
    auto options = Options();
    options.read_window_bytes = 1;
    // Force the admitted window to run before Release() returns so the test
    // observes whether cancellation was published before the next admission.
    options.executor = folly::getKeepAliveToken(load_executor);
    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              kTestSegmentId,
                              std::move(cells),
                              reader,
                              [](const auto&, int64_t) {
                                  return std::make_unique<GroupChunk>();
                              },
                              std::move(options)))
                    .semi()
                    .via(folly::getKeepAliveToken(&caller_executor));

    ASSERT_FALSE(load.isReady());
    ASSERT_EQ(reader->AsyncCalls(), 1);

    reader->FailDeferredRead();

    EXPECT_EQ(reader->AsyncCalls(), 1);
    try {
        std::move(load).get();
        FAIL() << "expected read failure";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "read failed");
    }
}

TEST_F(AsyncLoadPipelineTest,
       PublishesFinalizationFailureBeforeReleasingWindowBudget) {
    budget_.SetCapacityBytes(1);
    budget_.SetCapacitySlots(1);
    InlineRecordingExecutor load_executor;
    RecordingManualExecutor finalization_executor;
    InlineRecordingExecutor caller_executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    reader->DeferNextRead();
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
    auto options = Options();
    options.read_window_bytes = 1;
    // Keep load admission reentrant while finalization runs on a separate
    // executor, matching the lease handoff in the production path.
    options.executor = folly::getKeepAliveToken(load_executor);
    options.finalization_executor_provider = [&finalization_executor]() {
        return folly::getKeepAliveToken(&finalization_executor);
    };
    auto load =
        std::move(LoadCellsAsync(
                      nullptr,
                      kTestSegmentId,
                      std::move(cells),
                      reader,
                      [](const auto&, int64_t) -> std::unique_ptr<GroupChunk> {
                          throw std::runtime_error("finalization failed");
                      },
                      std::move(options)))
            .semi()
            .via(folly::getKeepAliveToken(&caller_executor));

    ASSERT_FALSE(load.isReady());
    ASSERT_EQ(reader->AsyncCalls(), 1);

    reader->CompleteDeferredRead();
    ASSERT_EQ(finalization_executor.step(), 1);
    finalization_executor.drain();

    EXPECT_EQ(reader->AsyncCalls(), 1);
    try {
        std::move(load).get();
        FAIL() << "expected finalization failure";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "finalization failed");
    }
}

TEST_F(AsyncLoadPipelineTest, FailsReadBeforeRequestingFinalizationExecutor) {
    budget_.SetCapacityBytes(1);
    folly::ManualExecutor io_executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    reader->SetStatus(arrow::Status::IOError("read failed"));
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    int provider_calls = 0;
    auto options = Options();
    options.finalization_executor_provider = [&]() {
        ++provider_calls;
        return folly::getKeepAliveToken(&io_executor);
    };

    auto load = Start(LoadCellsAsync(nullptr,
                                     kTestSegmentId,
                                     std::move(cells),
                                     reader,
                                     Finalizer(),
                                     std::move(options)));
    auto completed_before_io = load.isReady();
    auto next_budget =
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High);
    auto released_budget_before_io = next_budget.isReady();

    if (!completed_before_io) {
        io_executor.drain();
        executor_.drain();
    }
    EXPECT_EQ(provider_calls, 0);
    EXPECT_TRUE(completed_before_io);
    EXPECT_TRUE(released_budget_before_io);
    try {
        std::move(load).get();
        FAIL() << "expected read failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::StorageError);
    }
    ASSERT_TRUE(next_budget.isReady());
    auto next_lease = folly::coro::blockingWait(std::move(next_budget));
    next_lease.Release();
}

TEST_F(AsyncLoadPipelineTest,
       RejectsUnexpectedBatchCountBeforeRequestingFinalizationExecutor) {
    folly::ManualExecutor io_executor;
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    reader->ReturnOneFewerBatch();
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    int provider_calls = 0;
    auto options = Options();
    options.finalization_executor_provider = [&]() {
        ++provider_calls;
        return folly::getKeepAliveToken(&io_executor);
    };

    try {
        Run(LoadCellsAsync(nullptr,
                           kTestSegmentId,
                           std::move(cells),
                           reader,
                           Finalizer(),
                           std::move(options)));
        FAIL() << "expected invalid batch count";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
    }
    EXPECT_EQ(provider_calls, 0);
}

TEST_F(AsyncLoadPipelineTest, MovesExecutorKeepAliveIntoLazyTask) {
    KeepAliveRecordingExecutor executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options();
    options.executor = folly::getKeepAliveToken(executor);
    EXPECT_EQ(executor.OutstandingKeepAlives(), 1);

    {
        auto task = LoadCellsAsync(nullptr,
                                   kTestSegmentId,
                                   std::move(cells),
                                   reader,
                                   Finalizer(),
                                   std::move(options));
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
    auto task = LoadCellsAsync(
        &ctx, kTestSegmentId, std::move(cells), reader, Finalizer(), Options());

    ctx.cancellation_token = {};
    source.requestCancellation();

    try {
        Run(std::move(task));
        FAIL() << "expected cancellation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_EQ(reader->AsyncCalls(), 0);
}

TEST_F(AsyncLoadPipelineTest,
       HonorsCallerCoroutineCancellationWhileWaitingForBudget) {
    budget_.SetCapacityBytes(1);
    auto blocker = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High));
    folly::CancellationSource source;
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};

    auto caller = [this, reader, cells = std::move(cells)]() mutable
        -> folly::coro::Task<std::vector<AsyncCellResult>> {
        co_return co_await LoadCellsAsync(nullptr,
                                          kTestSegmentId,
                                          std::move(cells),
                                          reader,
                                          Finalizer(),
                                          Options());
    };
    auto with_cancellation = [&source, caller = std::move(caller)]() mutable
        -> folly::coro::Task<std::vector<AsyncCellResult>> {
        co_return co_await folly::coro::co_withCancellation(source.getToken(),
                                                            caller());
    };
    auto future = Start(with_cancellation());

    EXPECT_FALSE(future.isReady());
    source.requestCancellation();
    executor_.drain();
    auto ready_after_cancel = future.isReady();
    blocker.Release();

    EXPECT_TRUE(ready_after_cancel);
    try {
        Get(std::move(future));
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
        auto results = co_await LoadCellsAsync(nullptr,
                                               kTestSegmentId,
                                               std::move(cells),
                                               reader,
                                               Finalizer(),
                                               Options());
        co_return results.size();
    };

    EXPECT_EQ(Run(caller()), 1);
}

TEST_F(AsyncLoadPipelineTest, SubmitsOnlyBudgetAdmittedWindowsToLoadExecutor) {
    budget_.SetCapacityBytes(2);
    RecordingManualExecutor work_executor;
    InlineRecordingExecutor caller_executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
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
        {.cid = 2,
         .file_idx = 0,
         .local_rg_offset = 2,
         .rg_count = 1,
         .memory_size = 1},
    };
    auto options = Options();
    options.executor = folly::getKeepAliveToken(work_executor);
    options.read_window_bytes = 1;
    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              kTestSegmentId,
                              std::move(cells),
                              reader,
                              [](const auto&, int64_t) {
                                  return std::make_unique<GroupChunk>();
                              },
                              std::move(options)))
                    .semi()
                    .via(folly::getKeepAliveToken(&caller_executor));

    ASSERT_FALSE(load.isReady());
    EXPECT_EQ(work_executor.Priorities().size(), 2);
    EXPECT_EQ(reader->AsyncCalls(), 0);

    work_executor.drain();
    ASSERT_TRUE(load.isReady());
    EXPECT_EQ(std::move(load).get().size(), 3);
    work_executor.drain();
}

TEST_F(AsyncLoadPipelineTest, SubmitsOnlySlotAdmittedWindowsToLoadExecutor) {
    budget_.SetCapacityBytes(0);
    budget_.SetCapacitySlots(2);
    RecordingManualExecutor work_executor;
    InlineRecordingExecutor caller_executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
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
        {.cid = 2,
         .file_idx = 0,
         .local_rg_offset = 2,
         .rg_count = 1,
         .memory_size = 1},
    };
    auto options = Options();
    options.executor = folly::getKeepAliveToken(work_executor);
    options.read_window_bytes = 1;
    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              kTestSegmentId,
                              std::move(cells),
                              reader,
                              [](const auto&, int64_t) {
                                  return std::make_unique<GroupChunk>();
                              },
                              std::move(options)))
                    .semi()
                    .via(folly::getKeepAliveToken(&caller_executor));

    ASSERT_FALSE(load.isReady());
    EXPECT_EQ(work_executor.Priorities().size(), 2);
    EXPECT_EQ(reader->AsyncCalls(), 0);

    work_executor.drain();
    ASSERT_TRUE(load.isReady());
    EXPECT_EQ(std::move(load).get().size(), 3);
    work_executor.drain();
}

TEST_F(AsyncLoadPipelineTest, RegistersBudgetAdmissionBeforeWindowWorkStarts) {
    budget_.SetCapacityBytes(1);
    auto blocker = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High));
    folly::ManualExecutor work_executor;
    InlineRecordingExecutor caller_executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options();
    options.executor = folly::getKeepAliveToken(work_executor);
    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              kTestSegmentId,
                              std::move(cells),
                              reader,
                              [](const auto&, int64_t) {
                                  return std::make_unique<GroupChunk>();
                              },
                              std::move(options)))
                    .semi()
                    .via(folly::getKeepAliveToken(&caller_executor));

    ASSERT_FALSE(load.isReady());
    EXPECT_EQ(reader->AsyncCalls(), 0);
    blocker.Release();
    auto probe =
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High);
    EXPECT_FALSE(probe.isReady());

    EXPECT_GT(work_executor.drain(), 0);

    ASSERT_TRUE(load.isReady());
    EXPECT_EQ(std::move(load).get().size(), 1);
    ASSERT_TRUE(probe.isReady());
    auto probe_lease = folly::coro::blockingWait(std::move(probe));
    probe_lease.Release();
    work_executor.drain();
}

TEST_F(AsyncLoadPipelineTest, ReleasesBudgetWhenWindowTaskConstructionFails) {
    budget_.SetCapacityBytes(10);
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({5, 1}, storage::LoadAdmissionPriority::High));
    folly::ManualExecutor work_executor;
    InlineRecordingExecutor caller_executor;
    auto should_throw = std::make_shared<std::atomic<bool>>(false);
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 5}};
    auto options = Options();
    options.executor = folly::getKeepAliveToken(work_executor);
    options.finalization_executor_provider =
        ThrowOnCopyExecutorProvider(should_throw);
    auto task = LoadCellsAsync(
        nullptr,
        kTestSegmentId,
        std::move(cells),
        reader,
        [](const auto&, int64_t) { return std::make_unique<GroupChunk>(); },
        std::move(options));
    should_throw->store(true);

    auto load =
        std::move(task).semi().via(folly::getKeepAliveToken(&caller_executor));

    ASSERT_TRUE(load.isReady());
    EXPECT_THROW(std::move(load).get(), std::runtime_error);
    auto fitting =
        budget_.AcquireAsync({5, 1}, storage::LoadAdmissionPriority::High);
    ASSERT_TRUE(fitting.isReady());
    auto fitting_lease = folly::coro::blockingWait(std::move(fitting));
    fitting_lease.Release();
    running.Release();
    work_executor.drain();
}

TEST_F(AsyncLoadPipelineTest, WaitsForAsyncBudgetBeforeReading) {
    budget_.SetCapacityBytes(1);
    auto blocker = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High));
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};

    auto future = Start(LoadCellsAsync(nullptr,
                                       kTestSegmentId,
                                       std::move(cells),
                                       reader,
                                       Finalizer(),
                                       Options()));

    EXPECT_FALSE(future.isReady());
    EXPECT_EQ(reader->AsyncCalls(), 0);
    blocker.Release();
    auto results = Get(std::move(future));
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

    auto future = Start(LoadCellsAsync(nullptr,
                                       kTestSegmentId,
                                       std::move(cells),
                                       reader,
                                       std::move(finalizer),
                                       Options()));

    EXPECT_FALSE(future.isReady());
    EXPECT_EQ(finalized, 0);
    reader->CompleteDeferredRead();
    EXPECT_EQ(Get(std::move(future)).size(), 1);
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

        auto results = Run(LoadCellsAsync(nullptr,
                                          kTestSegmentId,
                                          std::move(cells),
                                          reader,
                                          Finalizer(),
                                          Options(priority)));
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
        options.executor = folly::getKeepAliveToken(executor);
        return Start(LoadCellsAsync(
            nullptr,
            kTestSegmentId,
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

    EXPECT_EQ(Get(std::move(high)).size(), 1);
    EXPECT_EQ(Get(std::move(low)).size(), 1);
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

    auto results = Run(LoadCellsAsync(
        nullptr,
        kTestSegmentId,
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
       FinalizesInReadContinuationWithoutRequeueingLoadExecutor) {
    folly::ManualExecutor executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    reader->DeferNextRead();
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    bool finalized = false;
    auto options = Options();
    options.executor = folly::getKeepAliveToken(executor);

    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              kTestSegmentId,
                              std::move(cells),
                              reader,
                              [&finalized](const auto&, int64_t) {
                                  finalized = true;
                                  return std::make_unique<GroupChunk>();
                              },
                              std::move(options)))
                    .semi()
                    .via(folly::getKeepAliveToken(&executor));

    for (size_t i = 0; i < 10 && reader->AsyncCalls() == 0; ++i) {
        ASSERT_EQ(executor.step(), 1);
    }
    ASSERT_EQ(reader->AsyncCalls(), 1);
    ASSERT_FALSE(finalized);

    reader->CompleteDeferredRead();
    ASSERT_EQ(executor.step(), 1);
    EXPECT_TRUE(finalized);

    executor.drain();
    ASSERT_TRUE(load.isReady());
    EXPECT_EQ(std::move(load).get().size(), 1);
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

    auto results = Run(LoadCellsAsync(
        nullptr,
        kTestSegmentId,
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
        kTestSegmentId,
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
        EXPECT_EQ(Get(std::move(load)).size(), 1);
        configure.get();
        FAIL() << "local file I/O pool configuration thread did not start";
    }
    auto configure_status = configure.wait_for(std::chrono::seconds(2));
    if (configure_status != std::future_status::ready) {
        reader->CompleteDeferredRead();
        EXPECT_EQ(Get(std::move(load)).size(), 1);
        configure.get();
        FAIL() << "disabling the local file I/O pool waited for remote read";
    }
    configure.get();

    reader->CompleteDeferredRead();
    EXPECT_EQ(Get(std::move(load)).size(), 1);
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
        kTestSegmentId,
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
        EXPECT_EQ(Get(std::move(load)).size(), 1);
        configure.get();
        FAIL() << "local file I/O pool configuration thread did not start";
    }
    EXPECT_EQ(configure.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);

    release_blocker_promise->set_value();
    blocker_released = true;
    release_guard.dismiss();
    EXPECT_EQ(Get(std::move(load)).size(), 1);
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
            kTestSegmentId,
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
    EXPECT_EQ(Get(std::move(high)).size(), 1);
    EXPECT_EQ(Get(std::move(low)).size(), 1);
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

    auto results = Run(LoadCellsAsync(
        nullptr,
        kTestSegmentId,
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
       ReleasesBudgetAfterIOFinalizationBeforeResumingLoadExecutor) {
    budget_.SetCapacityBytes(1);
    folly::ManualExecutor load_executor;
    folly::ManualExecutor io_executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    bool finalized = false;
    auto options = Options();
    options.executor = folly::getKeepAliveToken(load_executor);
    options.finalization_executor_provider = [&io_executor]() {
        return folly::getKeepAliveToken(&io_executor);
    };
    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              kTestSegmentId,
                              std::move(cells),
                              reader,
                              [&finalized](const auto&, int64_t) {
                                  finalized = true;
                                  return std::make_unique<GroupChunk>();
                              },
                              std::move(options)))
                    .semi()
                    .via(folly::getKeepAliveToken(&load_executor));

    EXPECT_GT(load_executor.drain(), 0);
    EXPECT_FALSE(finalized);
    EXPECT_FALSE(load.isReady());
    auto next_budget =
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High);
    EXPECT_FALSE(next_budget.isReady());

    EXPECT_GT(io_executor.drain(), 0);

    EXPECT_TRUE(finalized);
    EXPECT_FALSE(load.isReady());
    EXPECT_TRUE(next_budget.isReady());
    if (!next_budget.isReady()) {
        load_executor.drain();
    }
    ASSERT_TRUE(next_budget.isReady());
    auto next_lease = folly::coro::blockingWait(std::move(next_budget));
    next_lease.Release();

    if (!load.isReady()) {
        EXPECT_GT(load_executor.drain(), 0);
    }
    ASSERT_TRUE(load.isReady());
    EXPECT_EQ(std::move(load).get().size(), 1);
    load_executor.drain();
    io_executor.drain();
}

TEST_F(AsyncLoadPipelineTest, HoldsSlotThroughIOFinalization) {
    budget_.SetCapacityBytes(0);
    budget_.SetCapacitySlots(1);
    folly::ManualExecutor load_executor;
    folly::ManualExecutor io_executor;
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    bool finalized = false;
    auto options = Options();
    options.executor = folly::getKeepAliveToken(load_executor);
    options.finalization_executor_provider = [&io_executor]() {
        return folly::getKeepAliveToken(&io_executor);
    };
    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              kTestSegmentId,
                              std::move(cells),
                              reader,
                              [&finalized](const auto&, int64_t) {
                                  finalized = true;
                                  return std::make_unique<GroupChunk>();
                              },
                              std::move(options)))
                    .semi()
                    .via(folly::getKeepAliveToken(&load_executor));

    EXPECT_GT(load_executor.drain(), 0);
    EXPECT_FALSE(finalized);
    EXPECT_FALSE(load.isReady());
    auto next_budget =
        budget_.AcquireAsync({0, 1}, storage::LoadAdmissionPriority::High);
    EXPECT_FALSE(next_budget.isReady());

    EXPECT_GT(io_executor.drain(), 0);

    EXPECT_TRUE(finalized);
    EXPECT_FALSE(load.isReady());
    EXPECT_TRUE(next_budget.isReady());
    if (!next_budget.isReady()) {
        load_executor.drain();
    }
    ASSERT_TRUE(next_budget.isReady());
    auto next_lease = folly::coro::blockingWait(std::move(next_budget));
    next_lease.Release();

    if (!load.isReady()) {
        EXPECT_GT(load_executor.drain(), 0);
    }
    ASSERT_TRUE(load.isReady());
    EXPECT_EQ(std::move(load).get().size(), 1);
    load_executor.drain();
    io_executor.drain();
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
    options.executor = folly::getKeepAliveToken(load_executor);
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };
    auto load = std::move(LoadCellsAsync(
                              nullptr,
                              kTestSegmentId,
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
    auto next_budget =
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High);
    EXPECT_FALSE(next_budget.isReady());

    release_io_promise->set_value();
    io_released = true;
    release_guard.dismiss();
    EXPECT_EQ(std::move(load).get().size(), 1);
    EXPECT_TRUE(finalized->load());
    ASSERT_TRUE(next_budget.isReady());
    auto next_lease = folly::coro::blockingWait(std::move(next_budget));
    next_lease.Release();
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
    options.executor = folly::getKeepAliveToken(load_executor);
    options.finalization_executor_provider = [&pool]() {
        return pool.GetExecutor();
    };
    auto load = std::move(LoadCellsAsync(
                              &ctx,
                              kTestSegmentId,
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
        Run(LoadCellsAsync(
            nullptr,
            kTestSegmentId,
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
        Run(LoadCellsAsync(
            nullptr,
            kTestSegmentId,
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
    folly::ManualExecutor executor;
    ASSERT_EQ(executor.getNumPriorities(), 1);
    auto reader = std::make_shared<FakeChunkReader>(nullptr);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto options = Options();
    options.executor = folly::getKeepAliveToken(executor);

    auto load =
        std::move(
            LoadCellsAsync(
                nullptr,
                kTestSegmentId,
                std::move(cells),
                reader,
                [](const std::vector<std::shared_ptr<arrow::Table>>& tables,
                   int64_t) {
                    EXPECT_FALSE(tables.empty());
                    return std::make_unique<GroupChunk>();
                },
                options))
            .semi()
            .via(folly::getKeepAliveToken(&executor));

    EXPECT_GT(executor.drain(), 0);
    ASSERT_TRUE(load.isReady());
    auto results = std::move(load).get();

    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(reader->AsyncCalls(), 1);
}

TEST_F(AsyncLoadPipelineTest, HighPriorityAdmissionPassesQueuedLowLoad) {
    budget_.SetCapacityBytes(1);
    auto blocker = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::Low));
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
                       kTestSegmentId,
                       cell(),
                       low_reader,
                       Finalizer(),
                       Options(milvus::proto::common::LoadPriority::LOW)));
    auto high = Start(
        LoadCellsAsync(nullptr,
                       kTestSegmentId,
                       cell(),
                       high_reader,
                       Finalizer(),
                       Options(milvus::proto::common::LoadPriority::HIGH)));

    blocker.Release();
    EXPECT_EQ(Get(std::move(high)).size(), 1);
    EXPECT_EQ(Get(std::move(low)).size(), 1);
    EXPECT_EQ(read_order, (std::vector<int64_t>{2, 1}));
}

TEST_F(AsyncLoadPipelineTest, CancelsWhileWaitingForBudget) {
    budget_.SetCapacityBytes(1);
    auto blocker = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, storage::LoadAdmissionPriority::High));
    folly::CancellationSource source;
    OpContext ctx(source.getToken());
    auto reader = std::make_shared<FakeChunkReader>(&executor_);
    std::vector<CellSpec> cells{{.cid = 0,
                                 .file_idx = 0,
                                 .local_rg_offset = 0,
                                 .rg_count = 1,
                                 .memory_size = 1}};
    auto future = Start(LoadCellsAsync(&ctx,
                                       kTestSegmentId,
                                       std::move(cells),
                                       reader,
                                       Finalizer(),
                                       Options()));

    source.requestCancellation();

    try {
        Get(std::move(future));
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
        Run(LoadCellsAsync(&ctx,
                           kTestSegmentId,
                           std::move(cells),
                           reader,
                           std::move(finalizer),
                           Options()));
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
        Run(LoadCellsAsync(&ctx,
                           kTestSegmentId,
                           std::move(cells),
                           reader,
                           std::move(finalizer),
                           Options()));
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
            Run(LoadCellsAsync(nullptr,
                               kTestSegmentId,
                               std::move(cells),
                               reader,
                               Finalizer(),
                               Options()));
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
