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
#include <future>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>

#include "folly/CancellationToken.h"
#include "folly/ScopeGuard.h"
#include "gtest/gtest.h"
#include "monitor/Monitor.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"

namespace milvus {

struct ThreadPoolsTestAccess {
    static std::unique_lock<std::shared_mutex>
    LockPoolMap() {
        return std::unique_lock<std::shared_mutex>(ThreadPools::mutex_);
    }

    static std::unique_ptr<ThreadPool>
    ExtractPool(ThreadPoolPriority priority) {
        auto lock = LockPoolMap();
        auto node = ThreadPools::thread_pool_map.extract(priority);
        return node.empty() ? nullptr : std::move(node.mapped());
    }

    static void
    RestorePool(ThreadPoolPriority priority, std::unique_ptr<ThreadPool> pool) {
        auto lock = LockPoolMap();
        ThreadPools::thread_pool_map.erase(priority);
        if (pool) {
            ThreadPools::thread_pool_map.emplace(priority, std::move(pool));
        }
    }

    static bool
    HasPool(ThreadPoolPriority priority) {
        auto lock = LockPoolMap();
        return ThreadPools::thread_pool_map.contains(priority);
    }
};

}  // namespace milvus

namespace {

class SearchFanoutPoolTest : public testing::Test {
 protected:
    void
    SetUp() override {
        middle_ = milvus::ThreadPoolsTestAccess::ExtractPool(milvus::MIDDLE);
        search_ =
            milvus::ThreadPoolsTestAccess::ExtractPool(milvus::SEARCH_FANOUT);
        milvus::InitCpuNum(2);
        milvus::SetThreadPoolMaxThreadsSize(16);
        milvus::SetMiddlePriorityThreadCoreCoefficient(1.0);
    }

    void
    TearDown() override {
        milvus::ThreadPoolsTestAccess::RestorePool(milvus::MIDDLE,
                                                   std::move(middle_));
        milvus::ThreadPoolsTestAccess::RestorePool(milvus::SEARCH_FANOUT,
                                                   std::move(search_));
        milvus::InitCpuNum(cpu_num_);
        milvus::SetThreadPoolMaxThreadsSize(max_threads_);
        milvus::SetMiddlePriorityThreadCoreCoefficient(coefficient_);
    }

 private:
    const int cpu_num_ = milvus::CPU_NUM;
    const int max_threads_ = milvus::THREAD_POOL_MAX_THREADS_SIZE.load();
    const float coefficient_ =
        milvus::MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.load();
    std::unique_ptr<milvus::ThreadPool> middle_;
    std::unique_ptr<milvus::ThreadPool> search_;
};

}  // namespace

TEST_F(SearchFanoutPoolTest, ResizeBeforeFirstUse) {
    milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, 3.0);
    EXPECT_FALSE(milvus::ThreadPoolsTestAccess::HasPool(milvus::MIDDLE));
    EXPECT_FALSE(milvus::ThreadPoolsTestAccess::HasPool(milvus::SEARCH_FANOUT));
    auto& search = milvus::ThreadPools::GetThreadPool(milvus::SEARCH_FANOUT);
    auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
    EXPECT_EQ(middle.GetMaxThreadNum(), 6);
    EXPECT_EQ(search.GetMaxThreadNum(), 6);
    EXPECT_NE(&middle, &search);
}

TEST_F(SearchFanoutPoolTest, SearchCreatedAfterMiddleResize) {
    auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
    EXPECT_EQ(middle.GetMaxThreadNum(), 2);
    milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, 3.0);
    EXPECT_EQ(middle.GetMaxThreadNum(), 6);
    EXPECT_EQ(milvus::ThreadPools::GetThreadPool(milvus::SEARCH_FANOUT)
                  .GetMaxThreadNum(),
              6);
}

TEST_F(SearchFanoutPoolTest, ResizeWithOnlySearchCreated) {
    auto& search = milvus::ThreadPools::GetThreadPool(milvus::SEARCH_FANOUT);
    EXPECT_EQ(search.GetMaxThreadNum(), 2);
    milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, 3.0);
    EXPECT_EQ(search.GetMaxThreadNum(), 6);
    EXPECT_EQ(
        milvus::ThreadPools::GetThreadPool(milvus::MIDDLE).GetMaxThreadNum(),
        6);
}

TEST_F(SearchFanoutPoolTest, LiveResizeAndCapPreserveLoadWorkerCount) {
    const auto load_workers = milvus::ThreadPools::GetLoadExecutorWorkers();
    auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
    auto& search = milvus::ThreadPools::GetThreadPool(milvus::SEARCH_FANOUT);
    for (const auto cap : {3, 0, 16}) {
        milvus::SetThreadPoolMaxThreadsSize(cap);
        milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, 3.0);
        const auto expected = cap == 3 ? 3 : 6;
        EXPECT_EQ(middle.GetMaxThreadNum(), expected);
        EXPECT_EQ(search.GetMaxThreadNum(), expected);
        EXPECT_EQ(
            milvus::monitor::internal_storage_pool_capacity_middle.Value(),
            expected);
        EXPECT_EQ(milvus::monitor::internal_storage_pool_capacity_search_fanout
                      .Value(),
                  expected);
        EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(), load_workers);
    }
    milvus::ThreadPools::ResizeThreadPool(milvus::SEARCH_FANOUT, 1.0);
    EXPECT_EQ(middle.GetMaxThreadNum(), 2);
    EXPECT_EQ(search.GetMaxThreadNum(), 2);
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(), load_workers);
}

TEST_F(SearchFanoutPoolTest, InvalidResizeKeepsBothPoolsAndCoefficient) {
    auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
    auto& search = milvus::ThreadPools::GetThreadPool(milvus::SEARCH_FANOUT);
    for (const auto ratio : {0.0f, -1.0f}) {
        milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, ratio);
        EXPECT_EQ(middle.GetMaxThreadNum(), 2);
        EXPECT_EQ(search.GetMaxThreadNum(), 2);
        EXPECT_EQ(milvus::MIDDLE_PRIORITY_THREAD_CORE_COEFFICIENT.load(), 1.0);
    }
}

TEST_F(SearchFanoutPoolTest, ExecutesWhileMiddleIsBlockedWithSeparateMetrics) {
    milvus::SetThreadPoolMaxThreadsSize(1);
    auto& middle = milvus::ThreadPools::GetThreadPool(milvus::MIDDLE);
    auto& search = milvus::ThreadPools::GetThreadPool(milvus::SEARCH_FANOUT);
    ASSERT_NE(&middle, &search);
    std::promise<void> started;
    std::promise<void> unblock;
    auto gate = unblock.get_future().share();
    auto blocker = middle.Submit([&started, gate] {
        started.set_value();
        gate.wait();
    });
    auto release = folly::makeGuard([&] {
        unblock.set_value();
        blocker.wait();
    });
    ASSERT_EQ(started.get_future().wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto queued = middle.Submit([] { return 1; });
    EXPECT_EQ(queued.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    const auto middle_submitted =
        milvus::monitor::internal_storage_pool_task_submitted_total_middle
            .Value();
    const auto search_submitted =
        milvus::monitor::
            internal_storage_pool_task_submitted_total_search_fanout.Value();

    auto task = search.Submit([] { return 42; });

    ASSERT_EQ(task.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(task.get(), 42);
    EXPECT_EQ(milvus::monitor::internal_storage_pool_task_submitted_total_middle
                  .Value(),
              middle_submitted);
    EXPECT_EQ(
        milvus::monitor::
            internal_storage_pool_task_submitted_total_search_fanout.Value(),
        search_submitted + 1);
}

TEST_F(SearchFanoutPoolTest, TaskErrorsAndCancellationDoNotStopWorkers) {
    milvus::SetThreadPoolMaxThreadsSize(1);
    auto& search = milvus::ThreadPools::GetThreadPool(milvus::SEARCH_FANOUT);
    auto failed =
        search.Submit([] { throw std::runtime_error("fanout failure"); });
    EXPECT_THROW(failed.get(), std::runtime_error);
    auto canceled = search.Submit([] { throw folly::FutureCancellation(); });
    EXPECT_THROW(canceled.get(), folly::FutureCancellation);
    auto next = search.Submit([] { return 42; });
    ASSERT_EQ(next.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(next.get(), 42);
}

TEST_F(SearchFanoutPoolTest, CreationRacingResizeUsesLatestCoefficient) {
    std::promise<void> start;
    auto gate = start.get_future().share();
    auto creating = std::async(std::launch::async, [gate] {
        gate.wait();
        return &milvus::ThreadPools::GetThreadPool(milvus::SEARCH_FANOUT);
    });
    start.set_value();
    milvus::ThreadPools::ResizeThreadPool(milvus::MIDDLE, 3.0);
    EXPECT_EQ(creating.get()->GetMaxThreadNum(), 6);
}

TEST(ThreadPool, ThreadNum) {
    auto& threadPool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& lowThreadPool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    const auto max_thread_num = threadPool.GetMaxThreadNum();
    const auto low_max_thread_num = lowThreadPool.GetMaxThreadNum();
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
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(
        static_cast<milvus::ThreadPoolPriority>(6), 3.0);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::HIGH,
                                          2.0);
    ASSERT_EQ(threadPool.GetMaxThreadNum(),
              milvus::ClampThreadPoolMaxThreads(2 * milvus::CPU_NUM));
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              threadPool.GetMaxThreadNum() + lowThreadPool.GetMaxThreadNum());
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::LOW, 2.0);
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              threadPool.GetMaxThreadNum() + lowThreadPool.GetMaxThreadNum());

    milvus::ThreadPools::ResizeThreadPool(
        milvus::ThreadPoolPriority::HIGH,
        static_cast<float>(max_thread_num) / milvus::CPU_NUM);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    EXPECT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              max_thread_num + lowThreadPool.GetMaxThreadNum());
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
