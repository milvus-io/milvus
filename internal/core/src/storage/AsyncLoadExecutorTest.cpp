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

#include "storage/AsyncLoadExecutor.h"

#include <atomic>
#include <array>
#include <chrono>
#include <thread>
#include <future>
#include <vector>

#ifdef __linux__
#include <sys/resource.h>
#endif

#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "folly/ScopeGuard.h"
#include "folly/coro/Baton.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Task.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "gtest/gtest.h"
#include "folly/executors/ManualExecutor.h"

namespace milvus::storage {
namespace {

#ifdef __linux__
TEST(AsyncLoadExecutorTest, LocalPrioritySurvivesLegacyRebinding) {
    folly::ManualExecutor custom;
    auto urgent = ResolveAsyncLoadExecutor(
        folly::getKeepAliveToken(&custom), AsyncLoadPriority::Urgent, 42);
    EXPECT_EQ(ResolveAsyncLoadPriority(urgent.get(),
                                       proto::common::LoadPriority::LOW),
              AsyncLoadPriority::Urgent);
    EXPECT_EQ(GetAsyncLoadSequence(urgent.get()), 42);
    auto rebound = ResolveAsyncLoadExecutor(urgent.copy(),
                                            proto::common::LoadPriority::HIGH);
    EXPECT_EQ(ResolveAsyncLoadPriority(rebound.get(),
                                       proto::common::LoadPriority::HIGH),
              AsyncLoadPriority::Urgent);
    EXPECT_EQ(GetAsyncLoadSequence(rebound.get()), 42);
    auto high = ResolveAsyncLoadExecutor(folly::getKeepAliveToken(&custom),
                                         AsyncLoadPriority::High);
    EXPECT_EQ(
        ResolveAsyncLoadPriority(high.get(), proto::common::LoadPriority::HIGH),
        AsyncLoadPriority::High);
    auto demoted =
        ResolveAsyncLoadExecutor(high.copy(), proto::common::LoadPriority::LOW);
    EXPECT_EQ(ResolveAsyncLoadPriority(demoted.get(),
                                       proto::common::LoadPriority::LOW),
              AsyncLoadPriority::Low);
    auto explicit_demotion =
        ResolveAsyncLoadExecutor(urgent.copy(), AsyncLoadPriority::Low);
    EXPECT_EQ(ResolveAsyncLoadPriority(explicit_demotion.get(),
                                       proto::common::LoadPriority::LOW),
              AsyncLoadPriority::Low);
    EXPECT_EQ(
        ResolveAsyncLoadPriority(nullptr, proto::common::LoadPriority::LOW),
        AsyncLoadPriority::Low);
}

TEST(AsyncLoadExecutorTest, DefaultWorkersUseNice19ForBothPriorities) {
    const int caller_nice = getpriority(PRIO_PROCESS, 0);
    auto worker_nice = [](folly::Executor::KeepAlive<> executor) {
        std::promise<int> result;
        auto value = result.get_future();
        executor->add(
            [&]() { result.set_value(getpriority(PRIO_PROCESS, 0)); });
        return value.get();
    };
    for (const auto priority : {proto::common::LoadPriority::HIGH,
                                proto::common::LoadPriority::LOW}) {
        EXPECT_EQ(worker_nice(ResolveAsyncLoadExecutor({}, priority)), 19);
    }
    EXPECT_EQ(getpriority(PRIO_PROCESS, 0), caller_nice);

    folly::CPUThreadPoolExecutor custom(1);
    EXPECT_EQ(worker_nice(
                  ResolveAsyncLoadExecutor(folly::getKeepAliveToken(&custom),
                                           proto::common::LoadPriority::HIGH)),
              caller_nice);
}
#endif

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
#ifdef __linux__
        EXPECT_EQ(getpriority(PRIO_PROCESS, 0), 19);
#endif
        first_started.set_value();
        released.wait();
        first_done.set_value();
    });
    first_submitted = true;
    EXPECT_EQ(first_started.get_future().wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    executor->add([&]() {
#ifdef __linux__
        EXPECT_EQ(getpriority(PRIO_PROCESS, 0), 19);
#endif
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

TEST(AsyncLoadExecutorTest, OrdersPriorityThenSequenceWithStableTies) {
    const int previous = GetAsyncLoadThreadPoolSize();
    auto restore =
        folly::makeGuard([&] { SetAsyncLoadThreadPoolSize(previous); });
    SetAsyncLoadThreadPoolSize(1);
    std::promise<void> blocked, release, finished;
    auto released = release.get_future().share();
    auto executor =
        ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::HIGH);
    executor->add([&] {
        blocked.set_value();
        released.wait();
    });
    blocked.get_future().get();
    auto unblock = folly::makeGuard([&] { release.set_value(); });
    std::vector<int> order;
    order.reserve(6);
    const auto record = [&](int id) {
        order.push_back(id);
        if (order.size() == 6) {
            finished.set_value();
        }
    };
    const auto submit =
        [&](proto::common::LoadPriority priority, uint64_t sequence, int id) {
            ResolveAsyncLoadExecutor({}, priority, sequence)->add([&, id] {
                record(id);
            });
        };
    const OpContext low;
    const OpContext older;
    const OpContext newer;
    submit(proto::common::LoadPriority::LOW, low.sequence_id, 5);
    auto bound = ResolveAsyncLoadExecutor(
        {}, proto::common::LoadPriority::LOW, newer.sequence_id);
    auto rebound = ResolveAsyncLoadExecutor(std::move(bound),
                                            proto::common::LoadPriority::HIGH);
    EXPECT_EQ(GetAsyncLoadSequence(rebound.get()), newer.sequence_id);
    rebound->add([&] { record(2); });
    submit(proto::common::LoadPriority::HIGH, 0, 4);
    submit(proto::common::LoadPriority::HIGH, older.sequence_id, 1);
    submit(proto::common::LoadPriority::LOW, 0, 6);
    submit(proto::common::LoadPriority::HIGH, newer.sequence_id, 3);
    release.set_value();
    unblock.dismiss();
    finished.get_future().get();
    EXPECT_EQ(order, (std::vector<int>{1, 2, 3, 4, 5, 6}));
}

// One worker makes task reservations observable in execution order. Keep all
// classes backlogged for two complete rounds, then verify empty-class skipping.
TEST(AsyncLoadExecutorTest, WeightedPrioritiesKeepSequenceAndMakeProgress) {
    const int previous = GetAsyncLoadThreadPoolSize();
    auto restore = folly::makeGuard([&] {
        SetAsyncLoadThreadPoolSize(previous);
        SetAsyncLoadSchedulingWeights(32, 8, 1);
    });
    SetAsyncLoadThreadPoolSize(1);
    for (const auto weights : {std::array{1, 1, 1},
                               std::array{32, 8, 1},
                               std::array{8, 2, 1},
                               std::array{0, 1, 1}}) {
        SetAsyncLoadSchedulingWeights(weights[0], weights[1], weights[2]);
        const auto effective = weights[0] > 0 ? weights : std::array{32, 8, 1};
        for (int burst = 0; burst < 2; ++burst) {
            std::promise<void> blocked, release, finished;
            auto released = release.get_future().share();
            ResolveAsyncLoadExecutor({}, AsyncLoadPriority::High)->add([&] {
                blocked.set_value();
                released.wait();
            });
            blocked.get_future().get();
            auto unblock = folly::makeGuard([&] { release.set_value(); });
            std::vector<std::pair<int, int>> order;
            order.reserve(192);
            const std::array priorities{AsyncLoadPriority::Urgent,
                                        AsyncLoadPriority::High,
                                        AsyncLoadPriority::Low};
            // Enqueue later loads first, so sequence ordering is tested separately
            // from both priority selection and enqueue order.
            for (int cls = 2; cls >= 0; --cls) {
                for (int seq : {2, 1}) {
                    auto executor =
                        ResolveAsyncLoadExecutor({}, priorities[cls], seq);
                    for (int n = 0; n < 32; ++n) {
                        executor->add([&, cls, seq, n] {
                            order.emplace_back(cls, (seq - 1) * 32 + n);
                            if (order.size() == 192)
                                finished.set_value();
                        });
                    }
                }
            }
            release.set_value();
            unblock.dismiss();
            finished.get_future().get();
            size_t pos = 0;
            for (int round = 0; round < 2; ++round) {
                for (int cls = 0; cls < 3; ++cls) {
                    const int quota = effective[cls];
                    for (int n = 0; n < quota; ++n) {
                        EXPECT_EQ(order[pos++].first, cls);
                    }
                }
            }
            std::array<int, 3> seen{};
            for (const auto& [cls, ordinal] : order) {
                EXPECT_EQ(ordinal, seen[cls]++);
            }
            EXPECT_EQ(seen, (std::array{64, 64, 64}));
        }
    }
}

TEST(AsyncLoadExecutorTest, WeightedPrioritySurvivesCoroutineResumption) {
    const int previous = GetAsyncLoadThreadPoolSize();
    auto restore =
        folly::makeGuard([&] { SetAsyncLoadThreadPoolSize(previous); });
    SetAsyncLoadThreadPoolSize(1);
    folly::coro::Baton resume;
    std::promise<void> suspended, blocked, release, done;
    auto released = release.get_future().share();
    std::vector<int> order;
    auto urgent = [&]() -> folly::coro::Task<void> {
        suspended.set_value();
        co_await resume;
        EXPECT_EQ(
            GetAsyncLoadSequence(co_await folly::coro::co_current_executor), 9);
        order.push_back(0);
    };
    auto task = folly::coro::co_withExecutor(
                    ResolveAsyncLoadExecutor({}, AsyncLoadPriority::Urgent, 9),
                    urgent())
                    .start();
    suspended.get_future().get();
    ResolveAsyncLoadExecutor({}, AsyncLoadPriority::High)->add([&] {
        blocked.set_value();
        released.wait();
    });
    blocked.get_future().get();
    auto unblock = folly::makeGuard([&] { release.set_value(); });
    ResolveAsyncLoadExecutor({}, AsyncLoadPriority::High, 1)->add([&] {
        order.push_back(1);
        done.set_value();
    });
    resume.post();
    release.set_value();
    unblock.dismiss();
    std::move(task).get();
    done.get_future().get();
    EXPECT_EQ(order, (std::vector<int>{0, 1}));
}

TEST(AsyncLoadExecutorTest, CoroutineResumeAndExecutorSwitchRetainSequence) {
    const int previous = GetAsyncLoadThreadPoolSize();
    auto restore =
        folly::makeGuard([&] { SetAsyncLoadThreadPoolSize(previous); });
    SetAsyncLoadThreadPoolSize(1);
    folly::CPUThreadPoolExecutor other(1);
    folly::coro::Baton resume;
    std::promise<void> suspended, blocked, release, later_done;
    auto released = release.get_future().share();
    std::vector<int> order;
    order.reserve(2);
    auto old_load = [&]() -> folly::coro::Task<void> {
        EXPECT_EQ(
            GetAsyncLoadSequence(co_await folly::coro::co_current_executor),
            10);
        auto on_other = [&]() -> folly::coro::Task<void> {
            EXPECT_EQ(
                GetAsyncLoadSequence(co_await folly::coro::co_current_executor),
                10);
            co_return;
        };
        co_await folly::coro::co_withExecutor(
            ResolveAsyncLoadExecutor(
                folly::getKeepAliveToken(&other),
                proto::common::LoadPriority::HIGH,
                GetAsyncLoadSequence(
                    co_await folly::coro::co_current_executor)),
            on_other());
        suspended.set_value();
        co_await resume;
        order.push_back(1);
    };
    auto old_result = std::async(std::launch::async, [&] {
        folly::coro::blockingWait(folly::coro::co_withExecutor(
            ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::HIGH, 10),
            old_load()));
    });
    suspended.get_future().get();
    auto executor =
        ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::HIGH);
    executor->add([&] {
        blocked.set_value();
        released.wait();
    });
    blocked.get_future().get();
    auto unblock = folly::makeGuard([&] { release.set_value(); });
    ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::HIGH, 20)
        ->add([&] {
            order.push_back(2);
            later_done.set_value();
        });
    // The producer has no load context; the resuming executor must supply it.
    resume.post();
    release.set_value();
    unblock.dismiss();
    old_result.get();
    later_done.get_future().get();
    EXPECT_EQ(order, (std::vector<int>{1, 2}));
}

TEST(AsyncLoadExecutorTest, ConcurrentSubmissionsAndGroupReactivation) {
    const int previous = GetAsyncLoadThreadPoolSize();
    auto restore =
        folly::makeGuard([&] { SetAsyncLoadThreadPoolSize(previous); });
    SetAsyncLoadThreadPoolSize(8);
    constexpr size_t producers = 8;
    constexpr size_t per_producer = 2048;
    constexpr size_t total = producers * per_producer;
    std::vector<folly::Executor::KeepAlive<>> executors;
    executors.reserve(producers);
    // Several independently created adapters share the same load and priority.
    for (size_t i = 0; i < producers; ++i) {
        executors.push_back(
            ResolveAsyncLoadExecutor({},
                                     i < 4 ? proto::common::LoadPriority::HIGH
                                           : proto::common::LoadPriority::LOW,
                                     100 + i % 2));
    }
    for (int round = 0; round < 8; ++round) {
        std::vector<std::atomic<int>> seen(total);
        std::atomic<size_t> remaining{total};
        std::promise<void> done;
        auto completion = done.get_future();
        std::vector<std::thread> threads;
        threads.reserve(producers);
        for (size_t producer = 0; producer < producers; ++producer) {
            threads.emplace_back([&, producer] {
                for (size_t i = 0; i < per_producer; ++i) {
                    const auto id = producer * per_producer + i;
                    executors[producer]->add([&, id] {
                        seen[id].fetch_add(1, std::memory_order_relaxed);
                        if (remaining.fetch_sub(1, std::memory_order_acq_rel) ==
                            1) {
                            done.set_value();
                        }
                    });
                    if (i % 32 == 0) {
                        std::this_thread::yield();
                    }
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        EXPECT_EQ(completion.wait_for(std::chrono::seconds(30)),
                  std::future_status::ready);
        // Keep captured state alive if a failure leaves callbacks running.
        completion.get();
        for (const auto& count : seen) {
            EXPECT_EQ(count.load(std::memory_order_relaxed), 1);
        }
    }
}

TEST(AsyncLoadExecutorTest, RecreatedBindingsKeepEqualSequenceFifo) {
    const int previous = GetAsyncLoadThreadPoolSize();
    auto restore =
        folly::makeGuard([&] { SetAsyncLoadThreadPoolSize(previous); });
    SetAsyncLoadThreadPoolSize(1);
    std::promise<void> started, release, done;
    auto released = release.get_future().share();
    auto first =
        ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::HIGH, 10);
    first->add([&] {
        started.set_value();
        released.wait();
    });
    started.get_future().get();
    auto unblock = folly::makeGuard([&] { release.set_value(); });
    std::vector<int> seen;
    constexpr int count = 1024;
    seen.reserve(count);
    for (int i = 0; i < count; ++i) {
        ResolveAsyncLoadExecutor({}, proto::common::LoadPriority::HIGH, 10)
            ->add([&, i] {
                seen.push_back(i);
                if (seen.size() == count) {
                    done.set_value();
                }
            });
    }
    first.reset();
    release.set_value();
    unblock.dismiss();
    done.get_future().get();
    ASSERT_EQ(seen.size(), count);
    for (int i = 0; i < count; ++i) {
        EXPECT_EQ(seen[i], i);
    }
}

}  // namespace
}  // namespace milvus::storage
