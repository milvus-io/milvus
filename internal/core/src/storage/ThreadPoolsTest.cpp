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

#include <chrono>
#include <algorithm>
#include <stdexcept>
#include <system_error>
#include "segcore/memory_planner.h"
#include "storage/LoadAdmissionController.h"
#include <vector>
#include <atomic>
#include <functional>
#include <folly/futures/Future.h>
#include <folly/coro/BlockingWait.h>
#include "monitor/Monitor.h"
#include "storage/Util.h"
#include <future>
#include <gtest/gtest.h>
#include <thread>

#include "folly/ScopeGuard.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPools.h"

namespace milvus {

struct ThreadPoolsTestAccess {
    static std::unique_lock<std::shared_mutex>
    LockPoolMap() {
        return std::unique_lock<std::shared_mutex>(ThreadPools::mutex_);
    }

    static ThreadPools::ExecutorPtr
    ReplaceExecutor(ThreadPoolPriority priority,
                    std::unique_ptr<folly::CPUThreadPoolExecutor> executor) {
        ThreadPools::ExecutorPtr replacement(executor.release(),
                                             ThreadPools::ExecutorDeleter{});
        auto lock = LockPoolMap();
        ThreadPools::thread_pool_map.at(priority).swap(replacement);
        return replacement;
    }

    static void
    RestoreExecutor(ThreadPoolPriority priority,
                    ThreadPools::ExecutorPtr original) {
        {
            auto lock = LockPoolMap();
            ThreadPools::thread_pool_map.at(priority).swap(original);
        }
        // Drain and destroy the replacement outside the registry lock.
    }
};

}  // namespace milvus

namespace {

bool
WaitForPoolState(const std::function<bool()>& ready) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!ready()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return true;
}

}  // namespace

TEST(ThreadPool, ThreadNum) {
    auto& threadPool =
        milvus::ThreadPools::GetExecutor(milvus::ThreadPoolPriority::HIGH);
    auto& lowThreadPool =
        milvus::ThreadPools::GetExecutor(milvus::ThreadPoolPriority::LOW);
    const auto max_thread_num = threadPool.numThreads();
    const auto low_max_thread_num = lowThreadPool.numThreads();
    auto restore = folly::makeGuard([&] {
        milvus::ThreadPools::ResizeThreadPool(
            milvus::ThreadPoolPriority::HIGH,
            static_cast<float>(max_thread_num) / milvus::CPU_NUM);
        milvus::ThreadPools::ResizeThreadPool(
            milvus::ThreadPoolPriority::LOW,
            static_cast<float>(low_max_thread_num) / milvus::CPU_NUM);
    });
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              max_thread_num + low_max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::HIGH,
                                          0.0);
    ASSERT_EQ(threadPool.numThreads(), max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(
        static_cast<milvus::ThreadPoolPriority>(6), 3.0);
    ASSERT_EQ(threadPool.numThreads(), max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::HIGH,
                                          2.0);
    ASSERT_EQ(threadPool.numThreads(),
              milvus::ClampThreadPoolMaxThreads(2 * milvus::CPU_NUM));
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              threadPool.numThreads() + lowThreadPool.numThreads());
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::LOW, 2.0);
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              threadPool.numThreads() + lowThreadPool.numThreads());

    milvus::ThreadPools::ResizeThreadPool(
        milvus::ThreadPoolPriority::HIGH,
        static_cast<float>(max_thread_num) / milvus::CPU_NUM);
    ASSERT_EQ(threadPool.numThreads(), max_thread_num);
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              max_thread_num + lowThreadPool.numThreads());
    milvus::ThreadPools::ResizeThreadPool(
        milvus::ThreadPoolPriority::LOW,
        static_cast<float>(low_max_thread_num) / milvus::CPU_NUM);
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              max_thread_num + low_max_thread_num);
}

TEST(ThreadPool, LoadExecutorWorkerCountUsesCacheAfterInitialization) {
    const auto expected = milvus::ThreadPools::GetLoadExecutorWorkers();
    auto pool_map_lock = milvus::ThreadPoolsTestAccess::LockPoolMap();
    std::promise<void> query_started;
    auto started = query_started.get_future();
    auto query = std::async(std::launch::async, [&query_started]() {
        query_started.set_value();
        return milvus::ThreadPools::GetLoadExecutorWorkers();
    });

    const auto started_status = started.wait_for(std::chrono::seconds(1));
    const auto query_status = query.wait_for(std::chrono::seconds(2));
    pool_map_lock.unlock();

    EXPECT_EQ(started_status, std::future_status::ready);
    EXPECT_EQ(query_status, std::future_status::ready);
    EXPECT_EQ(query.get(), expected);
}

TEST(ThreadPool, InitializedOverheadGroupsDoNotAcquirePoolMapLock) {
    milvus::storage::LoadMemoryOverheadController::GetInstance().GetOrCreate();
    milvus::storage::LoadFileOverheadController::GetInstance().GetOrCreate();
    auto pool_map_lock = milvus::ThreadPoolsTestAccess::LockPoolMap();
    std::promise<void> query_started;
    auto started = query_started.get_future();
    auto query = std::async(std::launch::async, [&query_started]() {
        query_started.set_value();
        return std::make_pair(
            milvus::storage::LoadMemoryOverheadController::GetInstance()
                .GetOrCreate(),
            milvus::storage::LoadFileOverheadController::GetInstance()
                .GetOrCreate());
    });

    const auto started_status = started.wait_for(std::chrono::seconds(1));
    const auto query_status = query.wait_for(std::chrono::seconds(2));
    pool_map_lock.unlock();

    EXPECT_EQ(started_status, std::future_status::ready);
    EXPECT_EQ(query_status, std::future_status::ready);
    const auto groups = query.get();
    EXPECT_NE(groups.first, nullptr);
    EXPECT_NE(groups.second, nullptr);
}

TEST(LoadPriorityTest, QueryOverrideDoesNotChangeBackgroundPriority) {
    using milvus::proto::common::LoadPriority;
    milvus::OpContext ctx;
    for (const auto configured : {LoadPriority::HIGH, LoadPriority::LOW}) {
        EXPECT_EQ(milvus::ResolveLoadPriority(nullptr, configured), configured);
        EXPECT_EQ(milvus::ResolveLoadPriority(&ctx, configured), configured);
        ctx.runtime_load_priority = LoadPriority::HIGH;
        EXPECT_EQ(milvus::ResolveLoadPriority(&ctx, configured),
                  LoadPriority::HIGH);
        ctx.runtime_load_priority = LoadPriority::LOW;
        EXPECT_EQ(milvus::ResolveLoadPriority(&ctx, configured),
                  LoadPriority::LOW);
        ctx.runtime_load_priority.reset();
        EXPECT_EQ(milvus::ResolveLoadPriority(&ctx, configured), configured);
    }
}

TEST(ThreadPool, RegistryMetricsObserveNativeSuccessfulAndFailedTasks) {
    using namespace milvus;
    auto& pool = ThreadPools::GetExecutor(ThreadPoolPriority::MIDDLE);
    const auto previous_ratio = MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.load();
    auto restore = folly::makeGuard([&] {
        ThreadPools::ResizeThreadPool(ThreadPoolPriority::MIDDLE,
                                      previous_ratio);
    });
    ThreadPools::ResizeThreadPool(ThreadPoolPriority::MIDDLE,
                                  1.0F / std::max(1, CPU_NUM));
    ASSERT_EQ(pool.numThreads(), 1);
    auto& submitted =
        monitor::internal_storage_pool_task_submitted_total_middle;
    auto& completed =
        monitor::internal_storage_pool_task_completed_total_middle;
    auto& queued = monitor::internal_storage_pool_queue_duration_seconds_middle;
    auto& executed =
        monitor::internal_storage_pool_execute_duration_seconds_middle;
    ASSERT_TRUE(WaitForPoolState([&] {
        return submitted.Value() == completed.Value() &&
               pool.getPoolStats().pendingTaskCount == 0;
    }));
    const auto before_submitted = submitted.Value();
    const auto before_completed = completed.Value();
    const auto before_queued = queued.Collect().histogram.sample_count;
    const auto before_executed = executed.Collect().histogram.sample_count;
    std::promise<void> started;
    auto first_started = started.get_future();
    std::promise<void> release;
    auto gate = release.get_future().share();
    auto first = folly::via(&pool, [&started, gate] {
                     started.set_value();
                     gate.wait();
                     return 42;
                 }).semi();
    auto second = folly::via(&pool, [] {
                      throw std::runtime_error("native metric task failure");
                  }).semi();
    bool released = false;
    auto drain = folly::makeGuard([&] {
        if (!released) {
            release.set_value();
        }
        if (first.valid()) {
            first.wait();
        }
        if (second.valid()) {
            second.wait();
        }
    });
    ASSERT_EQ(first_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(monitor::internal_storage_pool_capacity_middle.Value(), 1);
    EXPECT_TRUE(WaitForPoolState([&] {
        return monitor::internal_storage_pool_active_threads_middle.Value() ==
                   1 &&
               monitor::internal_storage_pool_idle_threads_middle.Value() ==
                   0 &&
               monitor::internal_storage_pool_queue_depth_middle.Value() == 1;
    }));
    EXPECT_EQ(submitted.Value(), before_submitted + 2);
    release.set_value();
    released = true;
    EXPECT_EQ(std::move(first).get(), 42);
    EXPECT_THROW(std::move(second).get(), std::runtime_error);
    // The observer runs after the promise is fulfilled; future readiness is insufficient.
    ASSERT_TRUE(WaitForPoolState(
        [&] { return completed.Value() == before_completed + 2; }));
    EXPECT_EQ(submitted.Value(), before_submitted + 2);
    EXPECT_EQ(queued.Collect().histogram.sample_count, before_queued + 2);
    EXPECT_EQ(executed.Collect().histogram.sample_count, before_executed + 2);
    EXPECT_TRUE(WaitForPoolState([&] {
        return monitor::internal_storage_pool_active_threads_middle.Value() ==
                   0 &&
               monitor::internal_storage_pool_queue_depth_middle.Value() == 0;
    }));
}

TEST(ThreadPool, RegistryWorkersExpandAndRetireToOneAfterIdleTimeout) {
    using namespace milvus;
    auto& pool = ThreadPools::GetExecutor(ThreadPoolPriority::MIDDLE);
    const auto previous_ratio = MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.load();
    const auto previous_limit = THREAD_POOL_MAX_THREADS_SIZE.load();
    auto restore = folly::makeGuard([&] {
        SetThreadPoolMaxThreadsSize(previous_limit);
        ThreadPools::ResizeThreadPool(ThreadPoolPriority::MIDDLE,
                                      previous_ratio);
    });
    SetThreadPoolMaxThreadsSize(4);
    ThreadPools::ResizeThreadPool(ThreadPoolPriority::MIDDLE,
                                  4.0F / std::max(1, CPU_NUM));
    ASSERT_EQ(pool.numThreads(), 4);
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::atomic<int> started{0};
    std::vector<folly::SemiFuture<folly::Unit>> tasks;
    tasks.reserve(4);
    bool released = false;
    auto drain = folly::makeGuard([&] {
        if (!released) {
            release.set_value();
        }
        storage::WaitAllFutures(tasks);
    });
    for (int i = 0; i < 4; ++i) {
        tasks.push_back(folly::via(&pool, [&started, gate] {
                            ++started;
                            gate.wait();
                        }).semi());
    }
    EXPECT_TRUE(WaitForPoolState([&] { return started.load() == 4; }));
    EXPECT_EQ(pool.numActiveThreads(), 4);
    release.set_value();
    released = true;
    storage::WaitAllFutures(tasks);
    tasks.clear();
    // Production config uses 2 s; Folly's default 60 s must not survive migration.
    EXPECT_TRUE(WaitForPoolState([&] { return pool.numActiveThreads() == 1; }));
    EXPECT_EQ(pool.numThreads(), 4);
    EXPECT_EQ(folly::via(&pool, [] { return 42; }).semi().get(), 42);
}

TEST(ThreadPool, RejectedBatchSubmissionReturnsAdmissionBytesAndSlots) {
    using namespace milvus;
    class RejectingExecutor : public folly::CPUThreadPoolExecutor {
     public:
        RejectingExecutor() : folly::CPUThreadPoolExecutor(1) {
        }
        void
        add(folly::Func) override {
            ++rejections;
            throw std::system_error(
                std::make_error_code(std::errc::not_enough_memory));
        }
        int rejections{0};
    };
    // Initialize the cached worker count before temporarily replacing HIGH.
    (void)ThreadPools::GetLoadExecutorWorkers();
    auto rejecting = std::make_unique<RejectingExecutor>();
    auto* rejected = rejecting.get();
    auto original = ThreadPoolsTestAccess::ReplaceExecutor(
        ThreadPoolPriority::HIGH, std::move(rejecting));
    auto restore_executor = folly::makeGuard([&] {
        ThreadPoolsTestAccess::RestoreExecutor(ThreadPoolPriority::HIGH,
                                               std::move(original));
    });
    auto& budget = storage::LoadAdmissionController::GetInstance();
    const auto previous_bytes = budget.CapacityBytes();
    const auto previous_slots = budget.CapacitySlots();
    budget.SetCapacityBytes(8);
    budget.SetCapacitySlots(1);
    bool submission_attempted = false;
    auto restore_budget = folly::makeGuard([&] {
        if (submission_attempted) {
            // Reacquire each released dimension; a failed probe identifies the
            // original leaked reservation. Either way, remove exactly (8, 1).
            (void)budget.TryAcquire({8, 0},
                                    storage::LoadAdmissionPriority::High);
            (void)budget.TryAcquire({0, 1},
                                    storage::LoadAdmissionPriority::High);
            budget.Release({8, 1});
        }
        budget.SetCapacityBytes(previous_bytes);
        budget.SetCapacitySlots(previous_slots);
    });
    ASSERT_TRUE(
        budget.TryAcquire({8, 1}, storage::LoadAdmissionPriority::High));
    budget.Release({8, 1});
    int reads = 0;
    int finalized = 0;
    submission_attempted = true;
    auto batches = segcore::LoadCellBatchAsync(
        nullptr,
        {{0, 0, 0, 1, 8}},
        [&](size_t, int64_t, int64_t, int64_t)
            -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
            ++reads;
            return arrow::Status::IOError("rejected task must not read");
        },
        8,
        proto::common::LoadPriority::HIGH,
        [&](const std::vector<std::shared_ptr<arrow::Table>>&, int64_t) {
            ++finalized;
            return std::make_unique<GroupChunk>();
        });
    ASSERT_EQ(batches.size(), 1);
    ASSERT_TRUE(batches.front().isReady());
    EXPECT_EQ(rejected->rejections, 1);
    EXPECT_EQ(reads, 0);
    EXPECT_EQ(finalized, 0);
    // A ready future must not retain admission until its result is consumed.
    const bool bytes_returned =
        budget.TryAcquire({8, 0}, storage::LoadAdmissionPriority::High);
    const bool slots_returned =
        budget.TryAcquire({0, 1}, storage::LoadAdmissionPriority::High);
    EXPECT_TRUE(bytes_returned) << "submission failure leaked transient bytes";
    EXPECT_TRUE(slots_returned) << "submission failure leaked a load slot";
    if (bytes_returned) {
        budget.Release({8, 0});
    }
    if (slots_returned) {
        budget.Release({0, 1});
    }
    try {
        std::move(batches.front()).get();
        FAIL() << "expected executor submission failure";
    } catch (const std::system_error& error) {
        EXPECT_EQ(error.code(),
                  std::make_error_code(std::errc::not_enough_memory));
    }
    EXPECT_THROW(
        folly::coro::blockingWait(segcore::LoadCellBatchesAsync(
            nullptr,
            {{0, 0, 0, 1, 8}},
            [&](size_t, int64_t, int64_t, int64_t)
                -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
                ++reads;
                return arrow::Status::IOError("rejected task must not read");
            },
            8,
            proto::common::LoadPriority::HIGH,
            [&](const std::vector<std::shared_ptr<arrow::Table>>&, int64_t) {
                ++finalized;
                return std::make_unique<GroupChunk>();
            })),
        std::system_error);
    EXPECT_EQ(rejected->rejections, 2);
    auto downloads = storage::GetObjectData(
        nullptr, {"rejected-field-data"}, ThreadPoolPriority::HIGH, false);
    ASSERT_EQ(downloads.size(), 1);
    EXPECT_THROW(std::move(downloads.front()).get(), std::system_error);
    EXPECT_EQ(rejected->rejections, 3);
    EXPECT_EQ(reads, 0);
    EXPECT_EQ(finalized, 0);
    const bool async_budget_returned =
        budget.TryAcquire({8, 1}, storage::LoadAdmissionPriority::High);
    EXPECT_TRUE(async_budget_returned);
    if (async_budget_returned) {
        budget.Release({8, 1});
    }
}
