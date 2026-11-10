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
#include <thread>

#include "folly/ScopeGuard.h"
#include "segcore/memory_planner.h"
#include "storage/EntryStreamUtils.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPool.h"
#include "storage/ThreadPools.h"

namespace milvus {

struct ThreadPoolsTestAccess {
    // Replace the executor before publishing the pool or starting its sampler.
    static std::unique_ptr<ThreadPool>
    MakePool(std::unique_ptr<folly::CPUThreadPoolExecutor> executor) {
        auto pool = std::make_unique<ThreadPool>(1.0, "rejected_load_test");
        pool->ShutDown();
        pool->executor_ = std::move(executor);
        return pool;
    }

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
};

}  // namespace milvus

namespace {

class RejectingLoadExecutor : public folly::CPUThreadPoolExecutor {
 public:
    RejectingLoadExecutor() : folly::CPUThreadPoolExecutor(1) {
    }

    void
    add(folly::Func) override {
        throw std::runtime_error("injected queue rejection");
    }
};

}  // namespace

TEST(ThreadPoolSubmission, RejectionDestroysCapturesBeforeFutureConsumption) {
    auto pool = milvus::ThreadPoolsTestAccess::MakePool(
        std::make_unique<RejectingLoadExecutor>());
    bool released = false;
    auto future = pool->Submit(
        [guard = folly::makeGuard([&released] { released = true; })] {});

    // Retaining an exceptional future must not retain the rejected callable.
    EXPECT_TRUE(released);
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)),
              std::future_status::ready);
    try {
        future.get();
        FAIL() << "expected broken promise";
    } catch (const std::future_error& error) {
        EXPECT_EQ(error.code(),
                  std::make_error_code(std::future_errc::broken_promise));
    }
}

TEST(ThreadPoolSubmission, RejectedCellBatchReturnsBudgetBeforeConsumption) {
    using namespace milvus::segcore;
    auto& budget =
        milvus::storage::TransientMemoryBudget::GetLoadTransientBudget();
    const auto old_capacity = budget.CapacityBytes();
    auto restore_budget =
        folly::makeGuard([&] { budget.SetCapacityBytes(old_capacity); });
    budget.SetCapacityBytes(1);

    for (const auto priority : {milvus::HIGH, milvus::LOW}) {
        auto previous = milvus::ThreadPoolsTestAccess::ExtractPool(priority);
        auto restore_pool = folly::makeGuard([&] {
            milvus::ThreadPoolsTestAccess::RestorePool(priority,
                                                       std::move(previous));
        });
        milvus::ThreadPoolsTestAccess::RestorePool(
            priority,
            milvus::ThreadPoolsTestAccess::MakePool(
                std::make_unique<RejectingLoadExecutor>()));
        int reader_calls = 0;
        auto futures = LoadCellBatchAsync(
            nullptr,
            {{0, 0, 0, 1, 1}},
            [&reader_calls](size_t, int64_t, int64_t, int64_t)
                -> arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> {
                ++reader_calls;
                return std::vector<std::shared_ptr<arrow::Table>>{};
            },
            1,
            priority == milvus::HIGH ? milvus::proto::common::LoadPriority::HIGH
                                     : milvus::proto::common::LoadPriority::LOW,
            [](const std::vector<std::shared_ptr<arrow::Table>>&, int64_t) {
                return std::make_unique<milvus::GroupChunk>();
            });

        ASSERT_EQ(futures.size(), 1);
        const bool acquired = budget.TryAcquire(1);
        EXPECT_TRUE(acquired)
            << "rejected batch kept its budget in an unconsumed future";
        if (acquired) {
            budget.Release(1);
        }
        EXPECT_EQ(reader_calls, 0);
        EXPECT_THROW(futures.front().get(), std::future_error);
        if (!acquired) {
            // Keep the global budget clean even against the pre-fix version:
            // either claim the now-released byte, or release the leaked byte.
            budget.TryAcquire(1);
            budget.Release(1);
        }
    }
}

TEST(ThreadPool, ThreadNum) {
    auto& threadPool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& lowThreadPool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    auto max_thread_num = threadPool.GetMaxThreadNum();
    ASSERT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              max_thread_num + lowThreadPool.GetMaxThreadNum());
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::HIGH,
                                          0.0);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(
        static_cast<milvus::ThreadPoolPriority>(6), 3.0);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    milvus::ThreadPools::ResizeThreadPool(milvus::ThreadPoolPriority::HIGH,
                                          2.0);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), 2.0 * milvus::CPU_NUM);
    ASSERT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              threadPool.GetMaxThreadNum() + lowThreadPool.GetMaxThreadNum());

    milvus::ThreadPools::ResizeThreadPool(
        milvus::ThreadPoolPriority::HIGH,
        static_cast<float>(max_thread_num) / milvus::CPU_NUM);
    ASSERT_EQ(threadPool.GetMaxThreadNum(), max_thread_num);
    ASSERT_EQ(milvus::ThreadPools::GetLoadExecutorWorkers(),
              max_thread_num + lowThreadPool.GetMaxThreadNum());
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
