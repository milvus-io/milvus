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

#include "storage/EntryStreamUtils.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "folly/CancellationToken.h"
#include "folly/OperationCancelled.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Promise.h"
#include "folly/coro/Task.h"
#include "folly/coro/WithCancellation.h"
#include "folly/executors/ManualExecutor.h"
#include "gtest/gtest.h"

namespace milvus::storage {
namespace {

using AsyncAcquireReturn =
    decltype(std::declval<TransientMemoryBudget&>().AcquireAsync(
        1, TransientBudgetPriority::High));

static_assert(std::is_same_v<AsyncAcquireReturn,
                             folly::coro::Future<TransientBudgetLease>>);

folly::coro::Task<TransientBudgetLease>
AwaitAdmission(folly::coro::Future<TransientBudgetLease> admission) {
    co_return co_await std::move(admission);
}

class TransientMemoryBudgetAsyncTest : public testing::Test {
 protected:
    void
    SetUp() override {
        budget_.SetCapacityBytes(0);
    }

    void
    TearDown() override {
        budget_.SetCapacityBytes(0);
    }

    TransientMemoryBudget& budget_ =
        TransientMemoryBudget::GetLoadTransientBudget();
};

TEST_F(TransientMemoryBudgetAsyncTest, AcquiresAndReleasesWithLease) {
    budget_.SetCapacityBytes(1);

    auto first = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    ASSERT_TRUE(first.isReady());
    auto first_lease = folly::coro::blockingWait(std::move(first));

    auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    EXPECT_FALSE(waiting.isReady());

    first_lease.Release();
    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
    waiting_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, LeaseDestructorReleasesBudget) {
    budget_.SetCapacityBytes(1);

    {
        auto acquired = budget_.AcquireAsync(1, TransientBudgetPriority::High);
        auto lease = folly::coro::blockingWait(std::move(acquired));
        EXPECT_FALSE(budget_.TryAcquire(1, TransientBudgetPriority::High));
    }

    EXPECT_TRUE(budget_.TryAcquire(1, TransientBudgetPriority::High));
    budget_.Release(1);
}

TEST_F(TransientMemoryBudgetAsyncTest, GrantsHighPriorityBeforeLowPriority) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto low_waiting = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto high_waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);

    running_lease.Release();
    EXPECT_TRUE(high_waiting.isReady());
    EXPECT_FALSE(low_waiting.isReady());

    auto high_lease = folly::coro::blockingWait(std::move(high_waiting));
    high_lease.Release();
    ASSERT_TRUE(low_waiting.isReady());
    auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
    low_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, CancelsPendingAdmission) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto waiting = budget_.AcquireAsync(
        1, TransientBudgetPriority::High, cancellation_source.getToken());
    ASSERT_FALSE(waiting.isReady());

    cancellation_source.requestCancellation();

    ASSERT_TRUE(waiting.isReady());
    EXPECT_THROW(folly::coro::blockingWait(std::move(waiting)),
                 folly::OperationCancelled);
    running_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, RejectsPreCancelledAdmission) {
    folly::CancellationSource cancellation_source;
    cancellation_source.requestCancellation();

    auto cancelled = budget_.AcquireAsync(
        1, TransientBudgetPriority::High, cancellation_source.getToken());

    ASSERT_TRUE(cancelled.isReady());
    EXPECT_THROW(folly::coro::blockingWait(std::move(cancelled)),
                 folly::OperationCancelled);
    EXPECT_TRUE(budget_.TryAcquire(1, TransientBudgetPriority::High));
    budget_.Release(1);
}

TEST_F(TransientMemoryBudgetAsyncTest, CancellingQueueHeadAdmitsNextRequest) {
    budget_.SetCapacityBytes(10);

    auto running = budget_.AcquireAsync(5, TransientBudgetPriority::High);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto blocked_head = budget_.AcquireAsync(
        10, TransientBudgetPriority::High, cancellation_source.getToken());
    auto fitting_next = budget_.AcquireAsync(5, TransientBudgetPriority::High);
    ASSERT_FALSE(blocked_head.isReady());
    ASSERT_FALSE(fitting_next.isReady());

    cancellation_source.requestCancellation();

    ASSERT_TRUE(blocked_head.isReady());
    EXPECT_THROW(folly::coro::blockingWait(std::move(blocked_head)),
                 folly::OperationCancelled);
    ASSERT_TRUE(fitting_next.isReady());
    auto fitting_lease = folly::coro::blockingWait(std::move(fitting_next));
    fitting_lease.Release();
    running_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest,
       ConsumerCancellationRemovesPendingQueueHead) {
    budget_.SetCapacityBytes(10);

    auto running = budget_.AcquireAsync(5, TransientBudgetPriority::High);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto blocked_head = budget_.AcquireAsync(10, TransientBudgetPriority::High);
    auto fitting_next = budget_.AcquireAsync(5, TransientBudgetPriority::High);
    ASSERT_FALSE(blocked_head.isReady());
    ASSERT_FALSE(fitting_next.isReady());

    folly::CancellationSource cancellation_source;
    folly::ManualExecutor executor;
    auto consumer = std::move(folly::coro::co_withCancellation(
                                  cancellation_source.getToken(),
                                  AwaitAdmission(std::move(blocked_head))))
                        .semi()
                        .via(folly::getKeepAliveToken(&executor));
    executor.drain();
    ASSERT_FALSE(consumer.isReady());

    cancellation_source.requestCancellation();
    executor.drain();

    ASSERT_TRUE(consumer.isReady());
    EXPECT_THROW(std::move(consumer).get(), folly::OperationCancelled);
    executor.drain();
    ASSERT_TRUE(fitting_next.isReady());
    auto fitting_lease = folly::coro::blockingWait(std::move(fitting_next));
    fitting_lease.Release();
    running_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest,
       ConsumerCancellationAndReleaseRaceResolvesOnce) {
    budget_.SetCapacityBytes(1);

    for (size_t i = 0; i < 100; ++i) {
        auto running = budget_.AcquireAsync(1, TransientBudgetPriority::High);
        auto running_lease = folly::coro::blockingWait(std::move(running));
        auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);
        folly::CancellationSource cancellation_source;
        folly::ManualExecutor executor;
        auto consumer = std::move(folly::coro::co_withCancellation(
                                      cancellation_source.getToken(),
                                      AwaitAdmission(std::move(waiting))))
                            .semi()
                            .via(folly::getKeepAliveToken(&executor));
        executor.drain();
        ASSERT_FALSE(consumer.isReady());

        std::thread cancel_thread([&cancellation_source]() {
            cancellation_source.requestCancellation();
        });
        running_lease.Release();
        cancel_thread.join();
        executor.drain();

        ASSERT_TRUE(consumer.isReady());
        try {
            auto waiting_lease = std::move(consumer).get();
            waiting_lease.Release();
        } catch (const folly::OperationCancelled&) {
        }
        executor.drain();

        ASSERT_TRUE(budget_.TryAcquire(1, TransientBudgetPriority::High));
        budget_.Release(1);
    }
}

TEST_F(TransientMemoryBudgetAsyncTest, ReleaseAndCancellationRaceResolvesOnce) {
    budget_.SetCapacityBytes(1);

    for (size_t i = 0; i < 100; ++i) {
        auto running = budget_.AcquireAsync(1, TransientBudgetPriority::High);
        auto running_lease = folly::coro::blockingWait(std::move(running));
        folly::CancellationSource cancellation_source;
        auto waiting = budget_.AcquireAsync(
            1, TransientBudgetPriority::High, cancellation_source.getToken());

        std::thread cancel_thread([&cancellation_source]() {
            cancellation_source.requestCancellation();
        });
        running_lease.Release();
        cancel_thread.join();

        ASSERT_TRUE(waiting.isReady());
        try {
            auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
            waiting_lease.Release();
        } catch (const folly::OperationCancelled&) {
        }

        ASSERT_TRUE(budget_.TryAcquire(1, TransientBudgetPriority::High));
        budget_.Release(1);
    }
}

TEST_F(TransientMemoryBudgetAsyncTest, CapacityUpdateWakesPendingAdmission) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    ASSERT_FALSE(waiting.isReady());

    budget_.SetCapacityBytes(2);

    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
    waiting_lease.Release();
    running_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, ZeroCapacityIsUnlimited) {
    budget_.SetCapacityBytes(0);

    auto high = budget_.AcquireAsync(100, TransientBudgetPriority::High);
    auto low = budget_.AcquireAsync(100, TransientBudgetPriority::Low);

    ASSERT_TRUE(high.isReady());
    ASSERT_TRUE(low.isReady());
    auto high_lease = folly::coro::blockingWait(std::move(high));
    auto low_lease = folly::coro::blockingWait(std::move(low));
    high_lease.Release();
    low_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, OversizedRequestRunsExclusively) {
    budget_.SetCapacityBytes(10);

    auto oversized = budget_.AcquireAsync(11, TransientBudgetPriority::Low);
    ASSERT_TRUE(oversized.isReady());
    auto oversized_lease = folly::coro::blockingWait(std::move(oversized));
    auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    EXPECT_FALSE(waiting.isReady());

    oversized_lease.Release();
    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
    waiting_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest,
       LegacyTryAcquireCannotBypassGrantedAsyncWaiter) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    ASSERT_FALSE(waiting.isReady());

    running_lease.Release();

    ASSERT_TRUE(waiting.isReady());
    EXPECT_FALSE(budget_.TryAcquire(1, TransientBudgetPriority::High));
    auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
    waiting_lease.Release();
    EXPECT_TRUE(budget_.TryAcquire(1, TransientBudgetPriority::High));
    budget_.Release(1);
}

TEST_F(TransientMemoryBudgetAsyncTest,
       HighPriorityLegacyAdmissionBypassesQueuedLowWaiter) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto low_waiting = budget_.AcquireAsync(2, TransientBudgetPriority::Low);
    ASSERT_FALSE(low_waiting.isReady());

    folly::CancellationToken token;
    EXPECT_TRUE(budget_.AcquireUntil(1, TransientBudgetPriority::High, token));
    budget_.Release(1);
    running_lease.Release();

    ASSERT_TRUE(low_waiting.isReady());
    auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
    low_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest,
       LowPriorityLegacyAdmissionCannotBypassQueuedLowWaiter) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto low_waiting = budget_.AcquireAsync(2, TransientBudgetPriority::Low);
    ASSERT_FALSE(low_waiting.isReady());

    EXPECT_FALSE(budget_.TryAcquire(1, TransientBudgetPriority::Low));
    running_lease.Release();

    ASSERT_TRUE(low_waiting.isReady());
    auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
    low_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest,
       QueuedHighLegacyAdmissionBlocksNewLowAsyncAdmission) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto high_legacy = std::async(
        std::launch::async, [&, token = cancellation_source.getToken()]() {
            return budget_.AcquireUntil(
                2, TransientBudgetPriority::High, token);
        });

    bool high_registered = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!budget_.TryAcquire(1, TransientBudgetPriority::Low)) {
            high_registered = true;
            break;
        }
        budget_.Release(1);
        std::this_thread::yield();
    }
    if (!high_registered) {
        cancellation_source.requestCancellation();
        ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_FALSE(high_legacy.get());
        FAIL() << "high-priority legacy waiter was not tracked";
    }

    auto low_waiting = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    ASSERT_FALSE(low_waiting.isReady());
    running_lease.Release();

    auto high_status = high_legacy.wait_for(std::chrono::seconds(2));
    bool high_won = high_status == std::future_status::ready;
    if (!high_won && low_waiting.isReady()) {
        auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
        low_lease.Release();
        high_status = high_legacy.wait_for(std::chrono::seconds(2));
    }

    ASSERT_EQ(high_status, std::future_status::ready);
    EXPECT_TRUE(high_legacy.get());
    budget_.Release(2);
    EXPECT_TRUE(high_won);

    if (high_won) {
        ASSERT_TRUE(low_waiting.isReady());
        auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
        low_lease.Release();
    }
}

TEST_F(TransientMemoryBudgetAsyncTest,
       QueuedHighLegacyAdmissionBlocksLaterHighAdmissions) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto high_legacy = std::async(
        std::launch::async, [&, token = cancellation_source.getToken()]() {
            return budget_.AcquireUntil(
                2, TransientBudgetPriority::High, token);
        });

    bool high_registered = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!budget_.TryAcquire(1, TransientBudgetPriority::Low)) {
            high_registered = true;
            break;
        }
        budget_.Release(1);
        std::this_thread::yield();
    }
    if (!high_registered) {
        cancellation_source.requestCancellation();
        ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_FALSE(high_legacy.get());
        FAIL() << "high-priority legacy waiter was not tracked";
    }

    bool try_acquire_overtook =
        budget_.TryAcquire(1, TransientBudgetPriority::High);
    if (try_acquire_overtook) {
        budget_.Release(1);
    }
    auto high_async = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    bool async_overtook = high_async.isReady();
    if (async_overtook) {
        auto async_lease = folly::coro::blockingWait(std::move(high_async));
        async_lease.Release();
    }

    running_lease.Release();
    ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_TRUE(high_legacy.get());
    budget_.Release(2);

    EXPECT_FALSE(try_acquire_overtook);
    EXPECT_FALSE(async_overtook);
    if (!async_overtook) {
        ASSERT_TRUE(high_async.isReady());
        auto async_lease = folly::coro::blockingWait(std::move(high_async));
        async_lease.Release();
    }
}

TEST_F(TransientMemoryBudgetAsyncTest,
       CancellingHighLegacyAdmissionUnblocksLowAsyncAdmission) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto high_legacy = std::async(
        std::launch::async, [&, token = cancellation_source.getToken()]() {
            return budget_.AcquireUntil(
                2, TransientBudgetPriority::High, token);
        });

    bool high_registered = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!budget_.TryAcquire(1, TransientBudgetPriority::Low)) {
            high_registered = true;
            break;
        }
        budget_.Release(1);
        std::this_thread::yield();
    }
    if (!high_registered) {
        cancellation_source.requestCancellation();
        ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_FALSE(high_legacy.get());
        FAIL() << "high-priority legacy waiter was not tracked";
    }

    auto low_waiting = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    ASSERT_FALSE(low_waiting.isReady());
    cancellation_source.requestCancellation();

    ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_FALSE(high_legacy.get());
    ASSERT_TRUE(low_waiting.isReady());
    auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
    low_lease.Release();
    running_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, PendingQueueOperationsScaleLinearly) {
    auto measure_queue_operations = [this](size_t waiter_count) {
        budget_.SetCapacityBytes(1);
        auto running = budget_.AcquireAsync(1, TransientBudgetPriority::High);
        auto running_lease = folly::coro::blockingWait(std::move(running));
        std::vector<folly::coro::Future<TransientBudgetLease>> waiters;
        waiters.reserve(waiter_count);
        std::vector<std::unique_ptr<folly::CancellationSource>>
            cancellation_sources;
        cancellation_sources.reserve(waiter_count);

        const auto registration_start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < waiter_count; ++i) {
            auto cancellation_source =
                std::make_unique<folly::CancellationSource>();
            waiters.push_back(
                budget_.AcquireAsync(1,
                                     TransientBudgetPriority::High,
                                     cancellation_source->getToken()));
            cancellation_sources.push_back(std::move(cancellation_source));
        }
        const auto registration_elapsed =
            std::chrono::steady_clock::now() - registration_start;

        const auto cancellation_start = std::chrono::steady_clock::now();
        for (const auto& cancellation_source : cancellation_sources) {
            cancellation_source->requestCancellation();
        }
        const auto cancellation_elapsed =
            std::chrono::steady_clock::now() - cancellation_start;

        EXPECT_TRUE(
            std::all_of(waiters.begin(), waiters.end(), [](const auto& waiter) {
                return waiter.isReady();
            }));
        waiters.clear();
        running_lease.Release();
        return std::pair{registration_elapsed, cancellation_elapsed};
    };

    constexpr size_t kSmallWaiterCount = 1024;
    constexpr size_t kLargeWaiterCount = 8192;
    const auto [small_registration, small_cancellation] =
        measure_queue_operations(kSmallWaiterCount);
    const auto [large_registration, large_cancellation] =
        measure_queue_operations(kLargeWaiterCount);
    const auto scheduling_slack = std::chrono::milliseconds(2);

    EXPECT_LT(large_registration, small_registration * 24 + scheduling_slack)
        << "registering 8x pending admissions should not repeatedly scan all "
           "existing waiters; small="
        << std::chrono::duration_cast<std::chrono::microseconds>(
               small_registration)
               .count()
        << "us large="
        << std::chrono::duration_cast<std::chrono::microseconds>(
               large_registration)
               .count()
        << "us";
    EXPECT_LT(large_cancellation, small_cancellation * 24 + scheduling_slack)
        << "cancelling 8x pending admissions should unlink each waiter "
           "directly; small="
        << std::chrono::duration_cast<std::chrono::microseconds>(
               small_cancellation)
               .count()
        << "us large="
        << std::chrono::duration_cast<std::chrono::microseconds>(
               large_cancellation)
               .count()
        << "us";
}

}  // namespace
}  // namespace milvus::storage
