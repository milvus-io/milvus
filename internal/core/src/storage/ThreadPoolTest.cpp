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

#include <atomic>
#include <chrono>
#include <concepts>
#include <future>
#include <limits>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>
#include <string>
#include <system_error>

#include <folly/ScopeGuard.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/futures/Future.h>
#include "common/EasyAssert.h"
#include "gtest/gtest.h"
#include "storage/LoadOverheadController.h"
#include "storage/LoadAdmissionController.h"
#include "storage/ThreadPools.h"
#include "storage/Util.h"

namespace milvus {

static_assert(
    std::same_as<storage::LoadMemoryOverheadController,
                 storage::LoadOverheadController<
                     cachinglayer::LoadingOverheadDimension::kMemory>>);
static_assert(std::same_as<storage::LoadFileOverheadController,
                           storage::LoadOverheadController<
                               cachinglayer::LoadingOverheadDimension::kFile>>);

template <typename T>
concept SupportsBudgetUpdate =
    requires(T& owner) { owner.UpdateBudgetBytes(size_t{0}); };

static_assert(SupportsBudgetUpdate<storage::LoadMemoryOverheadController>);
static_assert(!SupportsBudgetUpdate<storage::LoadFileOverheadController>);

TEST(LoadOverheadControllerTest, RejectsBudgetBeyondPolicyRange) {
    auto& controller = storage::LoadMemoryOverheadController::GetInstance();
    EXPECT_ANY_THROW(
        controller.UpdateBudgetBytes(std::numeric_limits<size_t>::max()));
}

class ThreadPoolTest : public testing::Test {
 protected:
    void
    SetUp() override {
        previous_cpu_ = CPU_NUM;
        previous_limit_ = THREAD_POOL_MAX_THREADS_SIZE.load();
        InitCpuNum(4);
        SetThreadPoolMaxThreadsSize(16);
    }

    void
    TearDown() override {
        InitCpuNum(previous_cpu_);
        SetThreadPoolMaxThreadsSize(previous_limit_);
    }

 private:
    int previous_cpu_;
    int previous_limit_;
};

TEST_F(ThreadPoolTest, ClampRespectsConfiguredBounds) {
    for (const auto value : {0, -1, -100}) {
        EXPECT_EQ(ClampThreadPoolMaxThreads(value), 1);
    }
    for (const auto value : {1, 8, 16}) {
        EXPECT_EQ(ClampThreadPoolMaxThreads(value), value);
    }
    for (const auto value : {17, 100, 1000}) {
        EXPECT_EQ(ClampThreadPoolMaxThreads(value), 16);
    }
    EXPECT_EQ(ComputeThreadPoolMaxThreads(10.0), 16);
    SetThreadPoolMaxThreadsSize(32);
    EXPECT_EQ(ComputeThreadPoolMaxThreads(10.0), 32);
    EXPECT_EQ(ClampThreadPoolMaxThreads(20), 20);
    SetThreadPoolMaxThreadsSize(8);
    EXPECT_EQ(ComputeThreadPoolMaxThreads(10.0), 8);
    for (const auto unlimited : {0, -1}) {
        SetThreadPoolMaxThreadsSize(unlimited);
        EXPECT_EQ(THREAD_POOL_MAX_THREADS_SIZE.load(), unlimited);
        EXPECT_EQ(ComputeThreadPoolMaxThreads(10.0), 40);
        EXPECT_EQ(ClampThreadPoolMaxThreads(100), 100);
        EXPECT_EQ(ClampThreadPoolMaxThreads(-1), 1);
    }
}

TEST_F(ThreadPoolTest, LoadFileOverheadControllerIsLazyAndStable) {
    auto& file_owner = storage::LoadFileOverheadController::GetInstance();
    auto& memory_owner = storage::LoadMemoryOverheadController::GetInstance();
    auto& admission = storage::LoadAdmissionController::GetInstance();
    const auto slots = admission.CapacitySlots();
    auto cleanup = folly::makeGuard(
        [&admission, slots]() { admission.SetCapacitySlots(slots); });

    admission.SetCapacitySlots(4);
    auto file_group = file_owner.GetOrCreate();
    auto same_file_group = file_owner.GetOrCreate();
    auto memory_group = memory_owner.GetOrCreate();

    ASSERT_NE(file_group, nullptr);
    EXPECT_EQ(file_group, same_file_group);
    ASSERT_NE(memory_group, nullptr);
    EXPECT_NE(file_group, memory_group);

    admission.SetCapacitySlots(8);
    EXPECT_EQ(file_group, file_owner.GetOrCreate());
}

TEST_F(ThreadPoolTest, WorkerSpawnFailureDoesNotFailQueuedTask) {
    class FailingThreadFactory : public folly::NamedThreadFactory {
     public:
        FailingThreadFactory() : folly::NamedThreadFactory("spawn_failure") {
        }
        std::thread
        newThread(folly::Func&& function) override {
            if (fail.load()) {
                ++failures;
                throw std::system_error(std::make_error_code(
                    std::errc::resource_unavailable_try_again));
            }
            return folly::NamedThreadFactory::newThread(std::move(function));
        }
        std::atomic<bool> fail{false};
        std::atomic<int> failures{0};
    };
    auto factory = std::make_shared<FailingThreadFactory>();
    folly::CPUThreadPoolExecutor pool(std::pair<size_t, size_t>{1, 1}, factory);
    std::promise<void> blocker_started;
    auto started = blocker_started.get_future();
    std::promise<void> release;
    auto gate = release.get_future().share();
    auto blocker = folly::via(&pool, [&blocker_started, gate] {
                       blocker_started.set_value();
                       gate.wait();
                   }).semi();
    auto cleanup = folly::makeGuard([&] {
        factory->fail = false;
        release.set_value();
        pool.join();
    });
    ASSERT_EQ(started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    // Grow the limit only after the sole worker is occupied, so enqueue must spawn.
    pool.setNumThreads(2);
    factory->fail = true;
    std::atomic<int> runs{0};
    auto queued = folly::via(&pool, [&] { ++runs; }).semi();
    EXPECT_GT(factory->failures.load(), 0);
    EXPECT_FALSE(queued.isReady());
    factory->fail = false;
    release.set_value();
    cleanup.dismiss();
    std::move(blocker).get();
    EXPECT_NO_THROW(std::move(queued).get(std::chrono::seconds(2)));
    pool.join();
    EXPECT_EQ(runs.load(), 1);
}

TEST_F(ThreadPoolTest, ViaPreservesValuesVoidAndExceptions) {
    folly::CPUThreadPoolExecutor pool(1);
    auto result =
        folly::via(&pool, [value = std::make_unique<int>(40)]() mutable {
            *value += 2;
            return std::move(value);
        }).semi();
    static_assert(std::same_as<decltype(result),
                               folly::SemiFuture<std::unique_ptr<int>>>);
    EXPECT_EQ(*std::move(result).get(), 42);
    EXPECT_FALSE(result.valid());
    auto empty = folly::via(&pool, [] {}).semi();
    static_assert(
        std::same_as<decltype(empty), folly::SemiFuture<folly::Unit>>);
    EXPECT_NO_THROW(std::move(empty).get());

    auto failed = folly::via(&pool, []() -> int {
                      ThrowInfo(ErrorCode::StorageTransientError,
                                "injected native task failure");
                  }).semi();
    try {
        std::move(failed).get();
        FAIL() << "expected original error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::StorageTransientError);
    }
    EXPECT_FALSE(failed.valid());
    EXPECT_EQ(folly::via(&pool, [] { return 7; }).semi().get(), 7);
    pool.join();
}

TEST_F(ThreadPoolTest, CoAwaitReleasesExecutorAndWaitsForWorkerOnCancellation) {
    folly::CPUThreadPoolExecutor pool(1);
    folly::CPUThreadPoolExecutor executor(1);
    folly::CancellationSource cancellation;
    std::promise<void> started;
    auto started_future = started.get_future();
    std::promise<void> release;
    auto gate = release.get_future().share();
    auto lifetime = std::make_shared<int>(42);
    std::weak_ptr<int> borrowed = lifetime;
    auto operation = folly::coro::co_invoke(
        [&, lifetime = std::move(lifetime)]() -> folly::coro::Task<int> {
            auto value = co_await folly::via(&pool, [&started, gate, borrowed] {
                             started.set_value();
                             gate.wait();
                             auto still_alive = borrowed.lock();
                             EXPECT_NE(still_alive, nullptr);
                             return still_alive ? *still_alive : 0;
                         }).semi();
            EXPECT_EQ(co_await folly::coro::co_current_executor, &executor);
            co_return value;
        });
    auto completion = folly::coro::co_withCancellation(cancellation.getToken(),
                                                       std::move(operation))
                          .semi()
                          .via(&executor);
    auto cleanup = folly::makeGuard([&] {
        release.set_value();
        completion.wait();
    });
    ASSERT_EQ(started_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto progressed = std::make_shared<std::promise<void>>();
    auto marker = progressed->get_future();
    executor.add([progressed] { progressed->set_value(); });
    EXPECT_EQ(marker.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    cancellation.requestCancellation();
    EXPECT_FALSE(completion.isReady());
    EXPECT_FALSE(borrowed.expired());
    release.set_value();
    cleanup.dismiss();
    EXPECT_EQ(std::move(completion).get(), 42);
    pool.join();
}

TEST_F(ThreadPoolTest, FailureDrainsOtherSubmittedTasks) {
    folly::CPUThreadPoolExecutor pool(2);
    std::promise<void> started;
    auto started_future = started.get_future();
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::atomic<bool> finished{false};
    std::vector<folly::SemiFuture<folly::Unit>> tasks;
    tasks.push_back(folly::via(&pool, [] {
                        throw std::runtime_error("read failed");
                    }).semi());
    tasks.push_back(folly::via(&pool, [&] {
                        started.set_value();
                        gate.wait();
                        finished = true;
                        throw std::runtime_error("second read failed");
                    }).semi());
    auto waiting =
        std::async(std::launch::async, [&] { storage::WaitAllFutures(tasks); });
    auto cleanup = folly::makeGuard([&] {
        release.set_value();
        pool.join();
    });
    ASSERT_EQ(started_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(waiting.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    release.set_value();
    cleanup.dismiss();
    EXPECT_THROW(waiting.get(), std::runtime_error);
    EXPECT_TRUE(finished.load());
    EXPECT_FALSE(tasks[0].valid());
    EXPECT_FALSE(tasks[1].valid());
    pool.join();
}

TEST_F(ThreadPoolTest, DiscardedFutureStillRunsBeforeShutdownReturns) {
    std::atomic<int> completed{0};
    {
        folly::CPUThreadPoolExecutor pool(2);
        for (int i = 0; i < 32; ++i) {
            (void)folly::via(&pool, [&] { ++completed; }).semi();
        }
        pool.join();
    }
    EXPECT_EQ(completed.load(), 32);
}

TEST_F(ThreadPoolTest, SubmissionFailureCompletesFuture) {
    class RejectingExecutor : public folly::CPUThreadPoolExecutor {
     public:
        RejectingExecutor() : folly::CPUThreadPoolExecutor(1) {
        }
        void
        add(folly::Func) override {
            throw std::system_error(
                std::make_error_code(std::errc::not_enough_memory));
        }
    };
    RejectingExecutor pool;
    bool ran = false;
    auto result = folly::via(&pool, [&] { ran = true; }).semi();
    ASSERT_TRUE(result.isReady());
    try {
        std::move(result).get();
        FAIL() << "expected submission error";
    } catch (const std::system_error& e) {
        EXPECT_EQ(e.code(), std::make_error_code(std::errc::not_enough_memory));
    }
    EXPECT_FALSE(ran);
}

TEST_F(ThreadPoolTest, RetainedResultDoesNotBlockShutdown) {
    folly::CPUThreadPoolExecutor pool(1);
    auto result = folly::via(&pool, [] { return 42; }).semi();
    auto shutdown = std::async(std::launch::async, [&] { pool.join(); });
    // Consume the result before failing, so a regression cannot hang cleanup.
    auto status = shutdown.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(std::move(result).get(), 42);
    shutdown.get();
    EXPECT_EQ(status, std::future_status::ready);
}

}  // namespace milvus
