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

#include <cstddef>
#include <thread>
#include <utility>

#include "folly/CancellationToken.h"
#include "folly/futures/Future.h"
#include "gtest/gtest.h"

namespace milvus::storage {
namespace {

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
    auto first_lease = std::move(first).get();

    auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    EXPECT_FALSE(waiting.isReady());

    first_lease.Release();
    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = std::move(waiting).get();
    waiting_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, LeaseDestructorReleasesBudget) {
    budget_.SetCapacityBytes(1);

    {
        auto acquired = budget_.AcquireAsync(1, TransientBudgetPriority::High);
        auto lease = std::move(acquired).get();
        EXPECT_FALSE(budget_.TryAcquire(1));
    }

    EXPECT_TRUE(budget_.TryAcquire(1));
    budget_.Release(1);
}

TEST_F(TransientMemoryBudgetAsyncTest, GrantsHighPriorityBeforeLowPriority) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = std::move(running).get();
    auto low_waiting = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto high_waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);

    running_lease.Release();
    EXPECT_TRUE(high_waiting.isReady());
    EXPECT_FALSE(low_waiting.isReady());

    auto high_lease = std::move(high_waiting).get();
    high_lease.Release();
    ASSERT_TRUE(low_waiting.isReady());
    auto low_lease = std::move(low_waiting).get();
    low_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, CancelsPendingAdmission) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    auto running_lease = std::move(running).get();
    folly::CancellationSource cancellation_source;
    auto waiting = budget_.AcquireAsync(
        1, TransientBudgetPriority::High, cancellation_source.getToken());
    ASSERT_FALSE(waiting.isReady());

    cancellation_source.requestCancellation();

    ASSERT_TRUE(waiting.isReady());
    EXPECT_THROW(std::move(waiting).get(), folly::FutureCancellation);
    running_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, RejectsPreCancelledAdmission) {
    folly::CancellationSource cancellation_source;
    cancellation_source.requestCancellation();

    auto cancelled = budget_.AcquireAsync(
        1, TransientBudgetPriority::High, cancellation_source.getToken());

    ASSERT_TRUE(cancelled.isReady());
    EXPECT_THROW(std::move(cancelled).get(), folly::FutureCancellation);
    EXPECT_TRUE(budget_.TryAcquire(1));
    budget_.Release(1);
}

TEST_F(TransientMemoryBudgetAsyncTest, CancellingQueueHeadAdmitsNextRequest) {
    budget_.SetCapacityBytes(10);

    auto running = budget_.AcquireAsync(5, TransientBudgetPriority::High);
    auto running_lease = std::move(running).get();
    folly::CancellationSource cancellation_source;
    auto blocked_head = budget_.AcquireAsync(
        10, TransientBudgetPriority::High, cancellation_source.getToken());
    auto fitting_next = budget_.AcquireAsync(5, TransientBudgetPriority::High);
    ASSERT_FALSE(blocked_head.isReady());
    ASSERT_FALSE(fitting_next.isReady());

    cancellation_source.requestCancellation();

    ASSERT_TRUE(blocked_head.isReady());
    EXPECT_THROW(std::move(blocked_head).get(), folly::FutureCancellation);
    ASSERT_TRUE(fitting_next.isReady());
    auto fitting_lease = std::move(fitting_next).get();
    fitting_lease.Release();
    running_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, ReleaseAndCancellationRaceResolvesOnce) {
    budget_.SetCapacityBytes(1);

    for (size_t i = 0; i < 100; ++i) {
        auto running = budget_.AcquireAsync(1, TransientBudgetPriority::High);
        auto running_lease = std::move(running).get();
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
            auto waiting_lease = std::move(waiting).get();
            waiting_lease.Release();
        } catch (const folly::FutureCancellation&) {
        }

        ASSERT_TRUE(budget_.TryAcquire(1));
        budget_.Release(1);
    }
}

TEST_F(TransientMemoryBudgetAsyncTest, CapacityUpdateWakesPendingAdmission) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    auto running_lease = std::move(running).get();
    auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    ASSERT_FALSE(waiting.isReady());

    budget_.SetCapacityBytes(2);

    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = std::move(waiting).get();
    waiting_lease.Release();
    running_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, ZeroCapacityIsUnlimited) {
    budget_.SetCapacityBytes(0);

    auto high = budget_.AcquireAsync(100, TransientBudgetPriority::High);
    auto low = budget_.AcquireAsync(100, TransientBudgetPriority::Low);

    ASSERT_TRUE(high.isReady());
    ASSERT_TRUE(low.isReady());
    auto high_lease = std::move(high).get();
    auto low_lease = std::move(low).get();
    high_lease.Release();
    low_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest, OversizedRequestRunsExclusively) {
    budget_.SetCapacityBytes(10);

    auto oversized = budget_.AcquireAsync(11, TransientBudgetPriority::Low);
    ASSERT_TRUE(oversized.isReady());
    auto oversized_lease = std::move(oversized).get();
    auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    EXPECT_FALSE(waiting.isReady());

    oversized_lease.Release();
    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = std::move(waiting).get();
    waiting_lease.Release();
}

TEST_F(TransientMemoryBudgetAsyncTest,
       LegacyTryAcquireCannotBypassGrantedAsyncWaiter) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync(1, TransientBudgetPriority::Low);
    auto running_lease = std::move(running).get();
    auto waiting = budget_.AcquireAsync(1, TransientBudgetPriority::High);
    ASSERT_FALSE(waiting.isReady());

    running_lease.Release();

    ASSERT_TRUE(waiting.isReady());
    EXPECT_FALSE(budget_.TryAcquire(1));
    auto waiting_lease = std::move(waiting).get();
    waiting_lease.Release();
    EXPECT_TRUE(budget_.TryAcquire(1));
    budget_.Release(1);
}

}  // namespace
}  // namespace milvus::storage
