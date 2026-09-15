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

#include "storage/LoadAdmissionController.h"

#include <algorithm>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <future>
#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "folly/CancellationToken.h"
#include "folly/ScopeGuard.h"
#include "folly/OperationCancelled.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Promise.h"
#include "folly/coro/Task.h"
#include "folly/coro/WithCancellation.h"
#include "folly/executors/ManualExecutor.h"
#include "gtest/gtest.h"
#include "monitor/monitor_c.h"
#include "cachinglayer/lrucache/DList.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LoadOverheadController.h"

namespace milvus::storage {
namespace {

using AsyncAcquireReturn =
    decltype(std::declval<LoadAdmissionController&>().AcquireAsync(
        {1, 1}, LoadAdmissionPriority::High));

static_assert(std::is_same_v<AsyncAcquireReturn,
                             folly::coro::Future<LoadAdmissionLease>>);

folly::coro::Task<LoadAdmissionLease>
AwaitAdmission(folly::coro::Future<LoadAdmissionLease> admission) {
    co_return co_await std::move(admission);
}

// Exercises the same text export consumed by the Go metrics registry.
std::string
ScrapeAdmissionMetrics() {
    const std::unique_ptr<char, decltype(&std::free)> text(GetCoreMetrics(),
                                                           &std::free);
    return std::string(text.get());
}

// Finds a sample independently of the serializer's label ordering.
std::optional<double>
FindAdmissionSample(const std::string& text,
                    std::string_view suffix,
                    std::initializer_list<std::string_view> labels = {}) {
    const std::string name = "internal_load_admission_" + std::string(suffix);
    std::istringstream lines(text);
    std::string line;
    while (std::getline(lines, line)) {
        if (!line.starts_with(name) || line.size() <= name.size() ||
            (line[name.size()] != ' ' && line[name.size()] != '{')) {
            continue;
        }
        if (std::all_of(labels.begin(), labels.end(), [&](auto label) {
                return line.find(label) != std::string::npos;
            })) {
            return std::stod(line.substr(line.find_last_of(' ') + 1));
        }
    }
    return std::nullopt;
}

class LoadAdmissionControllerAsyncTest : public testing::Test {
 protected:
    void
    SetUp() override {
        budget_.SetCapacityBytes(0);
        budget_.SetCapacitySlots(0);
    }

    void
    TearDown() override {
        budget_.SetCapacityBytes(0);
        budget_.SetCapacitySlots(0);
    }

    LoadAdmissionController& budget_ = LoadAdmissionController::GetInstance();
};

TEST_F(LoadAdmissionControllerAsyncTest, SharedOverheadFollowsAdmissionLimits) {
    using namespace cachinglayer;
    const auto memory =
        LoadMemoryOverheadController::GetInstance().GetOrCreate();
    const auto file = LoadFileOverheadController::GetInstance().GetOrCreate();
    const LoadingOverheadConfig config{LoadingOverheadGroupBinding{memory, 100},
                                       LoadingOverheadGroupBinding{file, 50}};
    // All operations on these singleton groups are sequential in this test.
    internal::DList list(false, {100000, 100000}, {}, {}, {});
    list.BindLoadingOverheadGroups(config);
    auto unbind =
        folly::makeGuard([&] { list.UnbindLoadingOverheadGroups(config); });
    const ResourceUsage overhead{1000, 500};
    const auto check = [&](ResourceUsage expected) {
        const auto result =
            list.ReserveLoadingResourceWithTimeout(
                    {}, overhead, &config, std::chrono::milliseconds(0))
                .get();
        ASSERT_TRUE(result.success);
        EXPECT_EQ(result.reserved, expected);
        EXPECT_EQ(list.ReleaseLoadingResource({}, overhead, &config), expected);
        EXPECT_EQ(LoadMemoryOverheadController::GetInstance().GetOrCreate(),
                  memory);
        EXPECT_EQ(LoadFileOverheadController::GetInstance().GetOrCreate(),
                  file);
    };

    check(
        {1000, 500});  // Both limits disabled: preserve full request overhead.
    budget_.SetCapacitySlots(3);
    check({300, 150});
    budget_.SetCapacityBytes(400);
    check({400, 150});  // Memory uses bytes; file overhead uses slots.
    budget_.SetCapacityBytes(80);
    check({100, 150});  // One oversized runtime unit must still fit.
    budget_.SetCapacitySlots(2);
    check({100, 100});
    budget_.SetCapacityBytes(0);
    check({200, 100});  // Disabling bytes uses the latest slot capacity.

    const auto previous_workers = GetAsyncLoadThreadPoolSize();
    const auto previous_enabled =
        segcore::storagev2translator::StorageV2AsyncLoadEnabled();
    auto restore = folly::makeGuard([&] {
        SetAsyncLoadThreadPoolSize(previous_workers);
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(
            previous_enabled);
    });
    SetAsyncLoadThreadPoolSize(1);
    for (const bool enabled : {false, true}) {
        segcore::storagev2translator::SetStorageV2AsyncLoadEnabled(enabled);
        check({200,
               100});  // Two slots can outlive one CPU worker in either mode.
    }
    SetAsyncLoadThreadPoolSize(4);
    check({200, 100});
    budget_.SetCapacitySlots(5);
    check({500, 250});
    auto lease = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 5}, LoadAdmissionPriority::High));
    budget_.SetCapacitySlots(2);
    check({500, 250});  // Shrinking must still cover all five admitted slots.
    budget_.SetCapacitySlots(3);
    check({500, 250});  // Expanding again cannot erase that in-flight bound.
    lease.Release();
    check({300, 150});  // Once drained, the latest configured limit applies.
    budget_.SetCapacitySlots(0);
    check({1000, 500});
    budget_.SetCapacitySlots(std::numeric_limits<size_t>::max());
    check({1000, 500});  // The policy conversion must not overflow.
}

TEST_F(LoadAdmissionControllerAsyncTest,
       RejectedSlotExpansionKeepsAdmissionBound) {
    using namespace cachinglayer;
    const LoadingOverheadConfig incomplete{
        LoadingOverheadGroupBinding{
            LoadMemoryOverheadController::GetInstance().GetOrCreate(),
            std::nullopt},
        std::nullopt};
    internal::DList list(false, {100000, 100000}, {}, {}, {});
    list.BindLoadingOverheadGroups(incomplete);
    auto unbind =
        folly::makeGuard([&] { list.UnbindLoadingOverheadGroups(incomplete); });

    // A missing runtime bound is legal for Passthrough but rejects slot policy.
    // Tightening admission first is safe even when the policy update fails.
    budget_.SetCapacitySlots(1);
    EXPECT_EQ(budget_.CapacitySlots(), 1);
    budget_.SetCapacitySlots(2);
    EXPECT_EQ(budget_.CapacitySlots(), 1);
    auto lease = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    EXPECT_FALSE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High));
    lease.Release();

    list.UnbindLoadingOverheadGroups(incomplete);
    unbind.dismiss();
    budget_.SetCapacitySlots(2);
    EXPECT_EQ(budget_.CapacitySlots(), 2);
    lease = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 2}, LoadAdmissionPriority::High));
}

TEST_F(LoadAdmissionControllerAsyncTest, MetricsExposeReservationsAndCapacity) {
    budget_.SetCapacityBytes(10);
    budget_.SetCapacitySlots(3);
    auto lease = folly::coro::blockingWait(
        budget_.AcquireAsync({4, 2}, LoadAdmissionPriority::High));
    auto metrics = ScrapeAdmissionMetrics();
    EXPECT_EQ(FindAdmissionSample(metrics, "reserved_bytes"), 4);
    EXPECT_EQ(FindAdmissionSample(metrics, "capacity_bytes"), 10);
    EXPECT_EQ(FindAdmissionSample(metrics, "reserved_slots"), 2);
    EXPECT_EQ(FindAdmissionSample(metrics, "capacity_slots"), 3);

    // A shrink does not revoke reservations, and zero is the unlimited value.
    budget_.SetCapacitySlots(1);
    budget_.SetCapacityBytes(0);
    metrics = ScrapeAdmissionMetrics();
    EXPECT_EQ(FindAdmissionSample(metrics, "reserved_bytes"), 4);
    EXPECT_EQ(FindAdmissionSample(metrics, "capacity_bytes"), 0);
    EXPECT_EQ(FindAdmissionSample(metrics, "reserved_slots"), 2);
    EXPECT_EQ(FindAdmissionSample(metrics, "capacity_slots"), 1);
    lease.Release();
    metrics = ScrapeAdmissionMetrics();
    EXPECT_EQ(FindAdmissionSample(metrics, "reserved_bytes"), 0);
    EXPECT_EQ(FindAdmissionSample(metrics, "reserved_slots"), 0);
    EXPECT_EQ(
        FindAdmissionSample(metrics, "pending_requests", {"priority=\"high\""}),
        0);
    EXPECT_EQ(FindAdmissionSample(
                  metrics, "oldest_wait_seconds", {"priority=\"high\""}),
              0);
}

TEST_F(LoadAdmissionControllerAsyncTest, MetricsTrackQueueAgeAndOutcomes) {
    budget_.SetCapacitySlots(1);
    const auto before = ScrapeAdmissionMetrics();
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    folly::CancellationSource cancellation;
    auto high_cancelled = budget_.AcquireAsync(
        {1, 1}, LoadAdmissionPriority::High, cancellation.getToken());
    auto low_cancelled = budget_.AcquireAsync(
        {1, 1}, LoadAdmissionPriority::Low, cancellation.getToken());
    auto high_admitted =
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    auto low_admitted =
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    const auto queued = ScrapeAdmissionMetrics();
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    const auto aged = ScrapeAdmissionMetrics();
    for (const auto priority : {"priority=\"high\"", "priority=\"low\""}) {
        EXPECT_EQ(FindAdmissionSample(queued, "pending_requests", {priority}),
                  2);
        const auto first =
            FindAdmissionSample(queued, "oldest_wait_seconds", {priority});
        const auto second =
            FindAdmissionSample(aged, "oldest_wait_seconds", {priority});
        EXPECT_TRUE(first.has_value() && second.has_value() &&
                    *second > *first);
    }
    cancellation.requestCancellation();
    EXPECT_THROW(folly::coro::blockingWait(std::move(high_cancelled)),
                 folly::OperationCancelled);
    EXPECT_THROW(folly::coro::blockingWait(std::move(low_cancelled)),
                 folly::OperationCancelled);
    running.Release();
    auto high_lease = folly::coro::blockingWait(std::move(high_admitted));
    EXPECT_FALSE(low_admitted.isReady());
    high_lease.Release();
    auto low_lease = folly::coro::blockingWait(std::move(low_admitted));
    low_lease.Release();

    const auto after = ScrapeAdmissionMetrics();
    for (const auto priority : {"priority=\"high\"", "priority=\"low\""}) {
        EXPECT_EQ(FindAdmissionSample(after, "pending_requests", {priority}),
                  0);
        EXPECT_EQ(FindAdmissionSample(after, "oldest_wait_seconds", {priority}),
                  0);
        for (const auto outcome :
             {"outcome=\"admitted\"", "outcome=\"cancelled\""}) {
            const auto start = FindAdmissionSample(
                before, "queue_wait_seconds_count", {priority, outcome});
            const auto end = FindAdmissionSample(
                after, "queue_wait_seconds_count", {priority, outcome});
            EXPECT_TRUE(start.has_value() && end.has_value() &&
                        *end == *start + 1);
            const auto start_sum = FindAdmissionSample(
                before, "queue_wait_seconds_sum", {priority, outcome});
            const auto end_sum = FindAdmissionSample(
                after, "queue_wait_seconds_sum", {priority, outcome});
            EXPECT_TRUE(start_sum.has_value() && end_sum.has_value() &&
                        *end_sum > *start_sum);
            EXPECT_EQ(FindAdmissionSample(after,
                                          "queue_wait_seconds_bucket",
                                          {priority, outcome, "le=\"+Inf\""}),
                      end);
        }
    }
}

TEST_F(LoadAdmissionControllerAsyncTest, MetricsExcludeRequestsThatNeverQueue) {
    budget_.SetCapacitySlots(1);
    const auto before = ScrapeAdmissionMetrics();
    for (const auto priority :
         {LoadAdmissionPriority::High, LoadAdmissionPriority::Low}) {
        budget_.Acquire({1, 1}, priority);
        EXPECT_FALSE(budget_.TryAcquire({1, 1}, priority));
        budget_.Release({1, 1});
        ASSERT_TRUE(budget_.TryAcquire({1, 1}, priority));
        budget_.Release({1, 1});
        auto lease =
            folly::coro::blockingWait(budget_.AcquireAsync({1, 1}, priority));
        folly::CancellationSource cancellation;
        cancellation.requestCancellation();
        EXPECT_FALSE(
            budget_.AcquireUntil({1, 1}, priority, cancellation.getToken()));
        EXPECT_THROW(folly::coro::blockingWait(budget_.AcquireAsync(
                         {1, 1}, priority, cancellation.getToken())),
                     folly::OperationCancelled);
        lease.Release();
    }
    const auto after = ScrapeAdmissionMetrics();
    for (const auto priority : {"priority=\"high\"", "priority=\"low\""}) {
        for (const auto outcome :
             {"outcome=\"admitted\"", "outcome=\"cancelled\""}) {
            const auto start = FindAdmissionSample(
                before, "queue_wait_seconds_count", {priority, outcome});
            ASSERT_TRUE(start.has_value());
            EXPECT_EQ(
                FindAdmissionSample(
                    after, "queue_wait_seconds_count", {priority, outcome}),
                start);
        }
    }
}

TEST_F(LoadAdmissionControllerAsyncTest, MetricsCountBlockingQueueOutcomes) {
    budget_.SetCapacitySlots(1);
    const auto before = ScrapeAdmissionMetrics();
    for (const bool cancel : {false, true}) {
        auto running = folly::coro::blockingWait(
            budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
        folly::CancellationSource cancellation;
        auto waiter = std::async(std::launch::async, [&] {
            const bool admitted = budget_.AcquireUntil(
                {1, 1}, LoadAdmissionPriority::High, cancellation.getToken());
            if (admitted) {
                budget_.Release({1, 1});
            }
            return admitted;
        });
        bool queued = false;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            if (FindAdmissionSample(ScrapeAdmissionMetrics(),
                                    "pending_requests",
                                    {"priority=\"high\""}) == 1) {
                queued = true;
                break;
            }
            std::this_thread::yield();
        }
        if (cancel || !queued) {
            cancellation.requestCancellation();
        } else {
            budget_.SetCapacitySlots(2);
        }
        EXPECT_TRUE(queued);
        EXPECT_EQ(waiter.get(), !cancel && queued);
        running.Release();
        budget_.SetCapacitySlots(1);
    }
    const auto after = ScrapeAdmissionMetrics();
    for (const auto outcome :
         {"outcome=\"admitted\"", "outcome=\"cancelled\""}) {
        const auto start = FindAdmissionSample(
            before, "queue_wait_seconds_count", {"priority=\"high\"", outcome});
        const auto end = FindAdmissionSample(
            after, "queue_wait_seconds_count", {"priority=\"high\"", outcome});
        EXPECT_TRUE(start.has_value() && end.has_value() && *end == *start + 1);
    }
}

TEST_F(LoadAdmissionControllerAsyncTest,
       ConcurrentMetricScrapesKeepResourceSnapshotConsistent) {
    budget_.SetCapacitySlots(1);
    std::jthread worker([&](std::stop_token stop) {
        while (!stop.stop_requested()) {
            budget_.Acquire({7, 1}, LoadAdmissionPriority::High);
            budget_.Release({7, 1});
        }
    });
    std::vector<std::future<bool>> scrapers;
    scrapers.reserve(4);
    for (size_t i = 0; i < 4; ++i) {
        scrapers.push_back(std::async(std::launch::async, [] {
            for (size_t j = 0; j < 8; ++j) {
                const auto metrics = ScrapeAdmissionMetrics();
                const auto bytes =
                    FindAdmissionSample(metrics, "reserved_bytes");
                const auto slots =
                    FindAdmissionSample(metrics, "reserved_slots");
                if (!bytes || !slots || *bytes != 7 * *slots ||
                    FindAdmissionSample(metrics, "capacity_slots") != 1) {
                    return false;
                }
            }
            return true;
        }));
    }
    for (auto& scraper : scrapers) {
        EXPECT_TRUE(scraper.get());
    }
}

TEST_F(LoadAdmissionControllerAsyncTest,
       SlotsLimitAdmissionWithUnlimitedBytes) {
    budget_.SetCapacitySlots(1);
    auto first = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    auto lease = folly::coro::blockingWait(std::move(first));
    auto waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    EXPECT_FALSE(waiting.isReady());
    lease.Release();
    ASSERT_TRUE(waiting.isReady());
    auto next = folly::coro::blockingWait(std::move(waiting));
    next.Release();
    budget_.SetCapacitySlots(0);
}

TEST_F(LoadAdmissionControllerAsyncTest, SlotLeaseReleasesWhenBytesAreZero) {
    budget_.SetCapacitySlots(1);
    {
        auto lease = folly::coro::blockingWait(
            budget_.AcquireAsync({0, 1}, LoadAdmissionPriority::High));
        EXPECT_FALSE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
    }
    ASSERT_TRUE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
    budget_.Release({0, 1});
}

TEST_F(LoadAdmissionControllerAsyncTest,
       MoveTransfersBothResourcesExactlyOnce) {
    budget_.SetCapacityBytes(10);
    budget_.SetCapacitySlots(2);
    auto first = folly::coro::blockingWait(
        budget_.AcquireAsync({4, 1}, LoadAdmissionPriority::High));
    auto second = folly::coro::blockingWait(
        budget_.AcquireAsync({6, 1}, LoadAdmissionPriority::High));
    LoadAdmissionLease moved(std::move(first));
    first.Release();
    EXPECT_FALSE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
    second = std::move(moved);
    moved.Release();
    ASSERT_TRUE(budget_.TryAcquire({6, 1}, LoadAdmissionPriority::High));
    budget_.Release({6, 1});
    second.Release();
    second.Release();
    ASSERT_TRUE(budget_.TryAcquire({10, 2}, LoadAdmissionPriority::High));
    budget_.Release({10, 2});
}

TEST_F(LoadAdmissionControllerAsyncTest, ByteWaiterDoesNotReserveSlots) {
    budget_.SetCapacityBytes(10);
    budget_.SetCapacitySlots(2);
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({10, 1}, LoadAdmissionPriority::High));
    folly::CancellationSource cancellation;
    auto waiting = budget_.AcquireAsync(
        {1, 1}, LoadAdmissionPriority::Low, cancellation.getToken());
    ASSERT_FALSE(waiting.isReady());
    // High priority may bypass the low waiter and use the remaining slot.
    ASSERT_TRUE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
    budget_.Release({0, 1});
    cancellation.requestCancellation();
    EXPECT_THROW(folly::coro::blockingWait(std::move(waiting)),
                 folly::OperationCancelled);
}

TEST_F(LoadAdmissionControllerAsyncTest, SlotWaiterDoesNotReserveBytes) {
    budget_.SetCapacityBytes(10);
    budget_.SetCapacitySlots(1);
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    folly::CancellationSource cancellation;
    auto waiting = budget_.AcquireAsync(
        {9, 1}, LoadAdmissionPriority::Low, cancellation.getToken());
    ASSERT_FALSE(waiting.isReady());
    ASSERT_TRUE(budget_.TryAcquire({9, 0}, LoadAdmissionPriority::High));
    budget_.Release({9, 0});
    cancellation.requestCancellation();
    EXPECT_THROW(folly::coro::blockingWait(std::move(waiting)),
                 folly::OperationCancelled);
}

TEST_F(LoadAdmissionControllerAsyncTest, OversizedBytesCannotBypassSlots) {
    budget_.SetCapacityBytes(10);
    budget_.SetCapacitySlots(1);
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({0, 1}, LoadAdmissionPriority::High));
    auto oversized = budget_.AcquireAsync({11, 1}, LoadAdmissionPriority::High);
    EXPECT_FALSE(oversized.isReady());
    running.Release();
    ASSERT_TRUE(oversized.isReady());
    auto lease = folly::coro::blockingWait(std::move(oversized));
    EXPECT_FALSE(budget_.TryAcquire({1, 0}, LoadAdmissionPriority::High));
}

TEST_F(LoadAdmissionControllerAsyncTest, CapacityUpdatesRespectBothDimensions) {
    budget_.SetCapacityBytes(1);
    budget_.SetCapacitySlots(1);
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    auto waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    budget_.SetCapacityBytes(2);
    EXPECT_FALSE(waiting.isReady());
    budget_.SetCapacitySlots(2);
    EXPECT_EQ(budget_.CapacitySlots(), 2);
    ASSERT_TRUE(waiting.isReady());
    auto lease = folly::coro::blockingWait(std::move(waiting));
}

TEST_F(LoadAdmissionControllerAsyncTest, ShrinkingSlotsWaitsForInflightWork) {
    budget_.SetCapacitySlots(0);
    auto first = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    auto second = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    budget_.SetCapacitySlots(1);
    auto waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    EXPECT_FALSE(waiting.isReady());
    first.Release();
    EXPECT_FALSE(waiting.isReady());
    second.Release();
    ASSERT_TRUE(waiting.isReady());
    auto lease = folly::coro::blockingWait(std::move(waiting));
}

TEST_F(LoadAdmissionControllerAsyncTest, DisablingSlotsWakesWaiters) {
    budget_.SetCapacitySlots(1);
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    auto waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    ASSERT_FALSE(waiting.isReady());
    budget_.SetCapacitySlots(0);
    ASSERT_TRUE(waiting.isReady());
    auto lease = folly::coro::blockingWait(std::move(waiting));
}

TEST_F(LoadAdmissionControllerAsyncTest, CancellingSlotQueueHeadPreservesFifo) {
    budget_.SetCapacitySlots(2);
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High));
    folly::CancellationSource cancellation;
    auto head = budget_.AcquireAsync(
        {1, 2}, LoadAdmissionPriority::High, cancellation.getToken());
    auto next = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    auto low = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    EXPECT_FALSE(next.isReady());
    EXPECT_FALSE(low.isReady());
    cancellation.requestCancellation();
    EXPECT_THROW(folly::coro::blockingWait(std::move(head)),
                 folly::OperationCancelled);
    ASSERT_TRUE(next.isReady());
    EXPECT_FALSE(low.isReady());
    auto next_lease = folly::coro::blockingWait(std::move(next));
    next_lease.Release();
    ASSERT_TRUE(low.isReady());
    auto low_lease = folly::coro::blockingWait(std::move(low));
}

TEST_F(LoadAdmissionControllerAsyncTest,
       WeightedSlotsWaitForSufficientCapacity) {
    budget_.SetCapacitySlots(1);
    auto waiting = budget_.AcquireAsync({1, 2}, LoadAdmissionPriority::High);
    EXPECT_FALSE(waiting.isReady());
    budget_.SetCapacitySlots(2);
    ASSERT_TRUE(waiting.isReady());
    auto lease = folly::coro::blockingWait(std::move(waiting));
    EXPECT_FALSE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
}

TEST_F(LoadAdmissionControllerAsyncTest, AcquiresAndReleasesWithLease) {
    budget_.SetCapacityBytes(1);

    auto first = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    ASSERT_TRUE(first.isReady());
    auto first_lease = folly::coro::blockingWait(std::move(first));

    auto waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    EXPECT_FALSE(waiting.isReady());

    first_lease.Release();
    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
    waiting_lease.Release();
}

TEST_F(LoadAdmissionControllerAsyncTest, LeaseDestructorReleasesBudget) {
    budget_.SetCapacityBytes(1);

    {
        auto acquired =
            budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
        auto lease = folly::coro::blockingWait(std::move(acquired));
        EXPECT_FALSE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High));
    }

    EXPECT_TRUE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High));
    budget_.Release({1, 1});
}

TEST_F(LoadAdmissionControllerAsyncTest, GrantsHighPriorityBeforeLowPriority) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto low_waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto high_waiting =
        budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);

    running_lease.Release();
    EXPECT_TRUE(high_waiting.isReady());
    EXPECT_FALSE(low_waiting.isReady());

    auto high_lease = folly::coro::blockingWait(std::move(high_waiting));
    high_lease.Release();
    ASSERT_TRUE(low_waiting.isReady());
    auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
    low_lease.Release();
}

TEST_F(LoadAdmissionControllerAsyncTest, CancelsPendingAdmission) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto waiting = budget_.AcquireAsync(
        {1, 1}, LoadAdmissionPriority::High, cancellation_source.getToken());
    ASSERT_FALSE(waiting.isReady());

    cancellation_source.requestCancellation();

    ASSERT_TRUE(waiting.isReady());
    EXPECT_THROW(folly::coro::blockingWait(std::move(waiting)),
                 folly::OperationCancelled);
    running_lease.Release();
}

TEST_F(LoadAdmissionControllerAsyncTest, RejectsPreCancelledAdmission) {
    folly::CancellationSource cancellation_source;
    cancellation_source.requestCancellation();

    auto cancelled = budget_.AcquireAsync(
        {1, 1}, LoadAdmissionPriority::High, cancellation_source.getToken());

    ASSERT_TRUE(cancelled.isReady());
    EXPECT_THROW(folly::coro::blockingWait(std::move(cancelled)),
                 folly::OperationCancelled);
    EXPECT_TRUE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High));
    budget_.Release({1, 1});
}

TEST_F(LoadAdmissionControllerAsyncTest,
       RejectsPreCancelledBlockingAdmissionWithAvailableCapacity) {
    budget_.SetCapacitySlots(1);
    folly::CancellationSource cancellation;
    cancellation.requestCancellation();

    EXPECT_FALSE(budget_.AcquireUntil(
        {0, 1}, LoadAdmissionPriority::High, cancellation.getToken()));
    ASSERT_TRUE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
    budget_.Release({0, 1});
}

TEST_F(LoadAdmissionControllerAsyncTest,
       UnconsumedImmediateFutureReleasesReservation) {
    budget_.SetCapacitySlots(1);
    folly::CancellationSource cancellation;
    {
        auto future = budget_.AcquireAsync(
            {0, 1}, LoadAdmissionPriority::High, cancellation.getToken());
        ASSERT_TRUE(future.isReady());
        cancellation.requestCancellation();
        EXPECT_FALSE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
    }
    ASSERT_TRUE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
    budget_.Release({0, 1});
}

TEST_F(LoadAdmissionControllerAsyncTest,
       BlockingAdmissionRacesWithCancellationAndRelease) {
    budget_.SetCapacitySlots(1);
    for (size_t i = 0; i < 100; ++i) {
        auto running = folly::coro::blockingWait(
            budget_.AcquireAsync({0, 1}, LoadAdmissionPriority::High));
        folly::CancellationSource cancellation;
        std::barrier start(3);
        auto waiter = std::async(std::launch::async, [&] {
            start.arrive_and_wait();
            const bool admitted = budget_.AcquireUntil(
                {0, 1}, LoadAdmissionPriority::High, cancellation.getToken());
            if (admitted) {
                budget_.Release({0, 1});
            }
            return admitted;
        });
        std::thread cancel_thread([&] {
            start.arrive_and_wait();
            cancellation.requestCancellation();
        });
        start.arrive_and_wait();
        running.Release();
        cancel_thread.join();

        ASSERT_EQ(waiter.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        waiter.get();
        ASSERT_TRUE(budget_.TryAcquire({0, 1}, LoadAdmissionPriority::High));
        budget_.Release({0, 1});
    }
}

TEST_F(LoadAdmissionControllerAsyncTest, SlotExpansionWakesBlockingAdmission) {
    budget_.SetCapacitySlots(1);
    auto running = folly::coro::blockingWait(
        budget_.AcquireAsync({0, 1}, LoadAdmissionPriority::High));
    folly::CancellationSource cancellation;
    auto waiter = std::async(std::launch::async, [&] {
        const bool admitted = budget_.AcquireUntil(
            {0, 2}, LoadAdmissionPriority::High, cancellation.getToken());
        if (admitted) {
            budget_.Release({0, 2});
        }
        return admitted;
    });
    bool queued = false;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!budget_.TryAcquire({0, 0}, LoadAdmissionPriority::Low)) {
            queued = true;
            break;
        }
        budget_.Release({0, 0});
        std::this_thread::yield();
    }

    budget_.SetCapacitySlots(3);
    const auto status = waiter.wait_for(std::chrono::seconds(2));
    if (status != std::future_status::ready) {
        cancellation.requestCancellation();
    }
    ASSERT_EQ(waiter.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_TRUE(waiter.get());
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(queued);
    running.Release();
    ASSERT_TRUE(budget_.TryAcquire({0, 3}, LoadAdmissionPriority::High));
    budget_.Release({0, 3});
}

TEST_F(LoadAdmissionControllerAsyncTest, CancellingQueueHeadAdmitsNextRequest) {
    budget_.SetCapacityBytes(10);

    auto running = budget_.AcquireAsync({5, 1}, LoadAdmissionPriority::High);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto blocked_head = budget_.AcquireAsync(
        {10, 1}, LoadAdmissionPriority::High, cancellation_source.getToken());
    auto fitting_next =
        budget_.AcquireAsync({5, 1}, LoadAdmissionPriority::High);
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

TEST_F(LoadAdmissionControllerAsyncTest,
       ConsumerCancellationRemovesPendingQueueHead) {
    budget_.SetCapacityBytes(10);

    auto running = budget_.AcquireAsync({5, 1}, LoadAdmissionPriority::High);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto blocked_head =
        budget_.AcquireAsync({10, 1}, LoadAdmissionPriority::High);
    auto fitting_next =
        budget_.AcquireAsync({5, 1}, LoadAdmissionPriority::High);
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

TEST_F(LoadAdmissionControllerAsyncTest,
       ConsumerCancellationAndReleaseRaceResolvesOnce) {
    budget_.SetCapacityBytes(0);
    budget_.SetCapacitySlots(1);

    for (size_t i = 0; i < 100; ++i) {
        auto running =
            budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
        auto running_lease = folly::coro::blockingWait(std::move(running));
        auto waiting =
            budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
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

        ASSERT_TRUE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High));
        budget_.Release({1, 1});
    }
}

TEST_F(LoadAdmissionControllerAsyncTest,
       ReleaseAndCancellationRaceResolvesOnce) {
    budget_.SetCapacityBytes(0);
    budget_.SetCapacitySlots(1);

    const auto before = ScrapeAdmissionMetrics();
    for (size_t i = 0; i < 100; ++i) {
        auto running =
            budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
        auto running_lease = folly::coro::blockingWait(std::move(running));
        folly::CancellationSource cancellation_source;
        auto waiting = budget_.AcquireAsync({1, 1},
                                            LoadAdmissionPriority::High,
                                            cancellation_source.getToken());

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

        ASSERT_TRUE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High));
        budget_.Release({1, 1});
    }
    const auto after = ScrapeAdmissionMetrics();
    double resolved = 0;
    for (const auto outcome :
         {"outcome=\"admitted\"", "outcome=\"cancelled\""}) {
        const auto start = FindAdmissionSample(
            before, "queue_wait_seconds_count", {"priority=\"high\"", outcome});
        const auto end = FindAdmissionSample(
            after, "queue_wait_seconds_count", {"priority=\"high\"", outcome});
        ASSERT_TRUE(start.has_value());
        ASSERT_TRUE(end.has_value());
        resolved += *end - *start;
    }
    EXPECT_EQ(resolved, 100);
}

TEST_F(LoadAdmissionControllerAsyncTest, CapacityUpdateWakesPendingAdmission) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    ASSERT_FALSE(waiting.isReady());

    budget_.SetCapacityBytes(2);

    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
    waiting_lease.Release();
    running_lease.Release();
}

TEST_F(LoadAdmissionControllerAsyncTest, ZeroCapacityIsUnlimited) {
    budget_.SetCapacityBytes(0);
    budget_.SetCapacitySlots(0);

    auto high = budget_.AcquireAsync({100, 1}, LoadAdmissionPriority::High);
    auto low = budget_.AcquireAsync({100, 1}, LoadAdmissionPriority::Low);

    ASSERT_TRUE(high.isReady());
    ASSERT_TRUE(low.isReady());
    auto high_lease = folly::coro::blockingWait(std::move(high));
    auto low_lease = folly::coro::blockingWait(std::move(low));
    high_lease.Release();
    low_lease.Release();
}

TEST_F(LoadAdmissionControllerAsyncTest, OversizedRequestRunsExclusively) {
    budget_.SetCapacityBytes(10);

    auto oversized = budget_.AcquireAsync({11, 1}, LoadAdmissionPriority::Low);
    ASSERT_TRUE(oversized.isReady());
    auto oversized_lease = folly::coro::blockingWait(std::move(oversized));
    auto waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    EXPECT_FALSE(waiting.isReady());

    oversized_lease.Release();
    ASSERT_TRUE(waiting.isReady());
    auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
    waiting_lease.Release();
}

TEST_F(LoadAdmissionControllerAsyncTest,
       LegacyTryAcquireCannotBypassGrantedAsyncWaiter) {
    budget_.SetCapacityBytes(1);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    ASSERT_FALSE(waiting.isReady());

    running_lease.Release();

    ASSERT_TRUE(waiting.isReady());
    EXPECT_FALSE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High));
    auto waiting_lease = folly::coro::blockingWait(std::move(waiting));
    waiting_lease.Release();
    EXPECT_TRUE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High));
    budget_.Release({1, 1});
}

TEST_F(LoadAdmissionControllerAsyncTest,
       HighPriorityLegacyAdmissionBypassesQueuedLowWaiter) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto low_waiting = budget_.AcquireAsync({2, 1}, LoadAdmissionPriority::Low);
    ASSERT_FALSE(low_waiting.isReady());

    folly::CancellationToken token;
    EXPECT_TRUE(
        budget_.AcquireUntil({1, 1}, LoadAdmissionPriority::High, token));
    budget_.Release({1, 1});
    running_lease.Release();

    ASSERT_TRUE(low_waiting.isReady());
    auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
    low_lease.Release();
}

TEST_F(LoadAdmissionControllerAsyncTest,
       LowPriorityLegacyAdmissionCannotBypassQueuedLowWaiter) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    auto low_waiting = budget_.AcquireAsync({2, 1}, LoadAdmissionPriority::Low);
    ASSERT_FALSE(low_waiting.isReady());

    EXPECT_FALSE(budget_.TryAcquire({1, 1}, LoadAdmissionPriority::Low));
    running_lease.Release();

    ASSERT_TRUE(low_waiting.isReady());
    auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
    low_lease.Release();
}

TEST_F(LoadAdmissionControllerAsyncTest,
       QueuedHighLegacyAdmissionBlocksNewLowAsyncAdmission) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto high_legacy = std::async(
        std::launch::async, [&, token = cancellation_source.getToken()]() {
            return budget_.AcquireUntil(
                {2, 1}, LoadAdmissionPriority::High, token);
        });

    bool high_registered = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!budget_.TryAcquire({1, 1}, LoadAdmissionPriority::Low)) {
            high_registered = true;
            break;
        }
        budget_.Release({1, 1});
        std::this_thread::yield();
    }
    if (!high_registered) {
        cancellation_source.requestCancellation();
        ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_FALSE(high_legacy.get());
        FAIL() << "high-priority legacy waiter was not tracked";
    }

    auto low_waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
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
    budget_.Release({2, 1});
    EXPECT_TRUE(high_won);

    if (high_won) {
        ASSERT_TRUE(low_waiting.isReady());
        auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
        low_lease.Release();
    }
}

TEST_F(LoadAdmissionControllerAsyncTest,
       BlockingAndAsyncAdmissionsShareSlotPriorityQueue) {
    budget_.SetCapacitySlots(2);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto high_legacy = std::async(
        std::launch::async, [&, token = cancellation_source.getToken()]() {
            return budget_.AcquireUntil(
                {0, 2}, LoadAdmissionPriority::High, token);
        });

    bool high_registered = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!budget_.TryAcquire({1, 1}, LoadAdmissionPriority::Low)) {
            high_registered = true;
            break;
        }
        budget_.Release({1, 1});
        std::this_thread::yield();
    }
    if (!high_registered) {
        cancellation_source.requestCancellation();
        ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_FALSE(high_legacy.get());
        FAIL() << "high-priority legacy waiter was not tracked";
    }

    auto low_waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
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
    budget_.Release({0, 2});
    EXPECT_TRUE(high_won);

    if (high_won) {
        ASSERT_TRUE(low_waiting.isReady());
        auto low_lease = folly::coro::blockingWait(std::move(low_waiting));
        low_lease.Release();
    }
}

TEST_F(LoadAdmissionControllerAsyncTest,
       QueuedHighLegacyAdmissionBlocksLaterHighAdmissions) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto high_legacy = std::async(
        std::launch::async, [&, token = cancellation_source.getToken()]() {
            return budget_.AcquireUntil(
                {2, 1}, LoadAdmissionPriority::High, token);
        });

    bool high_registered = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!budget_.TryAcquire({1, 1}, LoadAdmissionPriority::Low)) {
            high_registered = true;
            break;
        }
        budget_.Release({1, 1});
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
        budget_.TryAcquire({1, 1}, LoadAdmissionPriority::High);
    if (try_acquire_overtook) {
        budget_.Release({1, 1});
    }
    auto high_async = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
    bool async_overtook = high_async.isReady();
    if (async_overtook) {
        auto async_lease = folly::coro::blockingWait(std::move(high_async));
        async_lease.Release();
    }

    running_lease.Release();
    ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_TRUE(high_legacy.get());
    budget_.Release({2, 1});

    EXPECT_FALSE(try_acquire_overtook);
    EXPECT_FALSE(async_overtook);
    if (!async_overtook) {
        ASSERT_TRUE(high_async.isReady());
        auto async_lease = folly::coro::blockingWait(std::move(high_async));
        async_lease.Release();
    }
}

TEST_F(LoadAdmissionControllerAsyncTest,
       CancellingHighLegacyAdmissionUnblocksLowAsyncAdmission) {
    budget_.SetCapacityBytes(2);

    auto running = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
    auto running_lease = folly::coro::blockingWait(std::move(running));
    folly::CancellationSource cancellation_source;
    auto high_legacy = std::async(
        std::launch::async, [&, token = cancellation_source.getToken()]() {
            return budget_.AcquireUntil(
                {2, 1}, LoadAdmissionPriority::High, token);
        });

    bool high_registered = false;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (!budget_.TryAcquire({1, 1}, LoadAdmissionPriority::Low)) {
            high_registered = true;
            break;
        }
        budget_.Release({1, 1});
        std::this_thread::yield();
    }
    if (!high_registered) {
        cancellation_source.requestCancellation();
        ASSERT_EQ(high_legacy.wait_for(std::chrono::seconds(2)),
                  std::future_status::ready);
        EXPECT_FALSE(high_legacy.get());
        FAIL() << "high-priority legacy waiter was not tracked";
    }

    auto low_waiting = budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::Low);
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

TEST_F(LoadAdmissionControllerAsyncTest, PendingQueueOperationsScaleLinearly) {
    auto measure_queue_operations = [this](size_t waiter_count) {
        budget_.SetCapacityBytes(1);
        auto running =
            budget_.AcquireAsync({1, 1}, LoadAdmissionPriority::High);
        auto running_lease = folly::coro::blockingWait(std::move(running));
        std::vector<folly::coro::Future<LoadAdmissionLease>> waiters;
        waiters.reserve(waiter_count);
        std::vector<std::unique_ptr<folly::CancellationSource>>
            cancellation_sources;
        cancellation_sources.reserve(waiter_count);

        const auto registration_start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < waiter_count; ++i) {
            auto cancellation_source =
                std::make_unique<folly::CancellationSource>();
            waiters.push_back(
                budget_.AcquireAsync({1, 1},
                                     LoadAdmissionPriority::High,
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
