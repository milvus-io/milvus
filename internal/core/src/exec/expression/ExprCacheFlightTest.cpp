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

#include <gtest/gtest.h>
#include <folly/ScopeGuard.h>
#include <future>
#include <filesystem>
#include <thread>
#include <vector>

#include "common/init_c.h"
#include "exec/expression/ExprCacheFlight.h"
#include "exec/expression/ExprCacheHelper.h"
#include "segcore/ChunkedSegmentSealedImpl.h"

namespace milvus::exec {
class ExprCacheFlightTestPeer {
 public:
    static std::unique_lock<std::shared_mutex>
    HoldBackend(ExprResCacheManager& manager) {
        return std::unique_lock(manager.state_mutex_);
    }
};
namespace {
using namespace std::chrono_literals;
using Coordinator = ExprCacheFlightCoordinator;

class ExprCacheFlightTest : public ::testing::TestWithParam<CacheMode> {
 protected:
    ExprResCacheManager& manager = ExprResCacheManager::Instance();
    Coordinator& flights = manager.Flights();
    CacheConfig cache_config;
    std::filesystem::path directory;
    ExprCacheFlightConfig flight_config;
    decltype(segcore::CreateSealedSegment(
        std::make_shared<Schema>(), empty_index_meta, 1)) segment;
    std::shared_ptr<const segcore::SegmentReadSnapshot> snapshot;
    ExprCacheFlightOptions options;
    static constexpr int64_t rows = 128;

    void
    SetUp() override {
        ExprResCacheManager::SetEnabled(false);
        manager.Clear();
        directory =
            std::filesystem::temp_directory_path() /
            ("expr-flight-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
        cache_config.mode = GetParam();
        cache_config.disk_base_path = directory.string();
        cache_config.disk_min_eval_duration_us = 0;
        cache_config.mem_max_bytes = 1 << 20;
        cache_config.materialization_max_bytes = 1 << 20;
        cache_config.mem_min_eval_duration_us = 0;
        cache_config.admission_threshold = 1;
        ASSERT_TRUE(manager.SetConfig(cache_config));
        ExprResCacheManager::SetEnabled(true);
        flight_config.enabled = true;
        flight_config.max_wait_ms = 5000;
        ASSERT_TRUE(flights.Configure(flight_config));
        auto schema = std::make_shared<Schema>();
        schema->set_primary_field_id(
            schema->AddDebugField("pk", DataType::INT64));
        segment = segcore::CreateSealedSegment(schema, empty_index_meta, 99001);
        snapshot = segment->CaptureReadSnapshot();
    }
    void
    TearDown() override {
        EXPECT_EQ(flights.GetStats().waiters, 0);
        EXPECT_EQ(flights.GetStats().flights, 0);
        EXPECT_EQ(manager.GetMaterializationBytes(), 0);
        flight_config.enabled = false;
        flights.Configure(flight_config);
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
        std::filesystem::remove_all(directory);
    }
    Coordinator::Key
    Key(std::string signature = "phrase") {
        return {{segment->get_segment_id(),
                 std::move(signature),
                 snapshot->GetExprCacheGeneration()},
                rows,
                manager.ConfigEpoch()};
    }
    std::shared_ptr<Coordinator::Bitmaps>
    Bits(int64_t size = rows) {
        auto lease = manager.TryAcquireMaterialization(size);
        if (!lease)
            return {};
        return std::make_shared<Coordinator::Bitmaps>(std::move(*lease),
                                                      TargetBitmap(size, true),
                                                      TargetBitmap(size, true));
    }
    template <typename Fn>
    auto
    Compute(Fn&& fn,
            bool write = true,
            const ExprCacheFlightOptions* opts = nullptr) {
        return ExprCacheHelper::GetOrCompute(
            segment.get(),
            snapshot.get(),
            [] { return "phrase"; },
            rows,
            std::forward<Fn>(fn),
            write,
            nullptr,
            opts ? opts : &options);
    }
    static ExprCacheHelper::ComputeResult
    Full() {
        return {TargetBitmap(rows, true), TargetBitmap(rows, true)};
    }
    bool
    Waiters(size_t count) {
        const auto until = std::chrono::steady_clock::now() + 3s;
        while (flights.GetStats().waiters != count &&
               std::chrono::steady_clock::now() < until) {
            std::this_thread::yield();
        }
        return flights.GetStats().waiters == count;
    }
};

TEST_P(ExprCacheFlightTest, ReadyBeforeWaitAndReaderOutlivesMapEntry) {
    flight_config.max_flights = 1;
    flights.Configure(flight_config);
    auto key = Key();
    auto leader = flights.TryJoin(key);
    auto follower = flights.TryJoin(key);
    ASSERT_TRUE(leader.IsLeader());
    ASSERT_FALSE(follower.IsLeader());
    flights.Complete(leader, Bits());
    leader.Reset();
    // Map entry is gone, but the follower still owns the old Flight.
    EXPECT_FALSE(flights.TryJoin(key));
    auto ready = flights.Wait(follower, nullptr, [] { return true; });
    ASSERT_EQ(ready.status, Coordinator::WaitStatus::Ready);
    EXPECT_EQ(ready.bitmaps->result.count(), rows);
    follower.Reset();
    EXPECT_EQ(flights.GetStats().flights, 0);
    auto next = flights.TryJoin(key);
    EXPECT_TRUE(next.IsLeader());
    EXPECT_EQ(flights.GetStats().flights, 1);
    EXPECT_GT(manager.GetMaterializationBytes(), 0);
    ready.bitmaps.reset();
    EXPECT_EQ(manager.GetMaterializationBytes(), 0);
}

TEST_P(ExprCacheFlightTest, LeaderScopeExitWakesFollowers) {
    auto leader = flights.TryJoin(Key());
    auto follower = flights.TryJoin(Key());
    auto waiting = std::async(std::launch::async, [&] {
        return flights.Wait(follower, nullptr, [] { return true; }).status;
    });
    leader.Reset();
    EXPECT_EQ(waiting.get(), Coordinator::WaitStatus::Bypass);
}

TEST_P(ExprCacheFlightTest, IncompleteResultsAreNotPublished) {
    auto leader = flights.TryJoin(Key());
    auto follower = flights.TryJoin(Key());
    flights.Complete(leader, Bits(rows - 1));
    EXPECT_EQ(flights.Wait(follower, nullptr, [] { return true; }).status,
              Coordinator::WaitStatus::Bypass);
}

TEST_P(ExprCacheFlightTest, TimeoutDoesNotRemoveOrCancelLeader) {
    flight_config.max_wait_ms = 20;
    flights.Configure(flight_config);
    auto leader = flights.TryJoin(Key());
    auto follower = flights.TryJoin(Key());
    auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(flights.Wait(follower, nullptr, [] { return true; }).status,
              Coordinator::WaitStatus::Bypass);
    EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);
    follower.Reset();
    auto another = flights.TryJoin(Key());
    EXPECT_FALSE(another.IsLeader());
    flights.Complete(leader, Bits());
    EXPECT_EQ(flights.Wait(another, nullptr, [] { return true; }).status,
              Coordinator::WaitStatus::Ready);
}

TEST_P(ExprCacheFlightTest, CancelAndHotDisableExitWithoutLeaderNotification) {
    auto leader = flights.TryJoin(Key());
    auto follower = flights.TryJoin(Key());
    folly::CancellationSource source;
    auto token = source.getToken();
    auto waiting = std::async(std::launch::async, [&] {
        return flights.Wait(follower, &token, [] { return true; }).status;
    });
    source.requestCancellation();
    EXPECT_EQ(waiting.get(), Coordinator::WaitStatus::Cancelled);
    flight_config.enabled = false;
    flights.Configure(flight_config);
    EXPECT_EQ(flights.Wait(follower, nullptr, [] { return true; }).status,
              Coordinator::WaitStatus::Bypass);
    EXPECT_FALSE(flights.TryJoin(Key("other")));
    EXPECT_EQ(flights.GetStats().flights, 1);
}

TEST_P(ExprCacheFlightTest, IdentityLimitsAndNestedExecution) {
    flight_config.max_waiters_per_key = 1;
    flight_config.max_waiters = 1;
    flight_config.max_flights = 3;
    flights.Configure(flight_config);
    auto leader = flights.TryJoin(Key());
    auto follower = flights.TryJoin(Key());
    EXPECT_FALSE(flights.TryJoin(Key()));
    auto other_key = Key("different-slop");
    auto other = flights.TryJoin(other_key);
    EXPECT_TRUE(other.IsLeader());
    EXPECT_FALSE(flights.TryJoin(other_key));
    auto generation_key = Key();
    generation_key.cache_key.generation =
        std::make_shared<ExprCacheGeneration>();
    auto generation = flights.TryJoin(generation_key);
    EXPECT_TRUE(generation.IsLeader());
    EXPECT_FALSE(flights.TryJoin(Key("full")));
    {
        Coordinator::ComputeScope scope;
        EXPECT_FALSE(flights.TryJoin(Key()));
    }
    follower.Reset();
    other.Reset();
    generation.Reset();
    auto count_key = Key();
    ++count_key.active_count;
    auto count = flights.TryJoin(count_key);
    EXPECT_TRUE(count.IsLeader());
    auto epoch_key = Key();
    ++epoch_key.config_epoch;
    auto epoch = flights.TryJoin(epoch_key);
    EXPECT_TRUE(epoch.IsLeader());
}

TEST_P(ExprCacheFlightTest, RetiredGenerationBypass) {
    auto key = Key();
    key.cache_key.generation->Retire();
    EXPECT_FALSE(flights.TryJoin(key));
}

TEST_P(ExprCacheFlightTest, SevenMissesComputeOnceAndReturnPrivateResults) {
    // Two logical misses must meet frequency admission even though only one
    // compute/Put takes place.
    cache_config.admission_threshold = 2;
    manager.SetConfig(cache_config);
    std::atomic<int> computations{0};
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    auto leader = std::async(std::launch::async, [&] {
        return Compute([&] {
            ++computations;
            entered.set_value();
            released.wait();
            return Full();
        });
    });
    entered.get_future().wait();
    std::vector<std::future<ExprCacheHelper::CachedBitmaps>> followers;
    auto unblock = folly::makeGuard([&] { release.set_value(); });
    for (int i = 0; i < 6; ++i) {
        followers.push_back(std::async(std::launch::async, [&] {
            return Compute([&] {
                ++computations;
                return Full();
            });
        }));
    }
    EXPECT_TRUE(Waiters(6));
    release.set_value();
    unblock.dismiss();
    auto first = leader.get();
    std::vector<ExprCacheHelper::CachedBitmaps> results;
    for (auto& follower : followers) results.push_back(follower.get());
    EXPECT_EQ(computations, 1);
    EXPECT_EQ(manager.GetEntryCount(), 1);
    first.result->flip();
    results[0].valid->flip();
    for (size_t i = 1; i < results.size(); ++i) {
        EXPECT_EQ(results[i].result->count(), rows);
        EXPECT_EQ(results[i].valid->count(), rows);
    }
    // Private follower buffers remain charged after Flight cleanup.
    EXPECT_EQ(flights.GetStats().flights, 0);
    EXPECT_GT(manager.GetMaterializationBytes(), 0);
}

TEST_P(ExprCacheFlightTest, AdmissionRejectionAndReadOnlyLeaderStillShare) {
    for (bool write : {true, false}) {
        cache_config.mem_min_eval_duration_us = write ? 1000000000 : 0;
        cache_config.disk_min_eval_duration_us = write ? 1000000000 : 0;
        manager.SetConfig(cache_config);
        std::atomic<int> computations{0};
        std::promise<void> entered, release;
        auto released = release.get_future().share();
        auto leader = std::async(std::launch::async, [&] {
            return Compute(
                [&] {
                    ++computations;
                    entered.set_value();
                    released.wait();
                    return Full();
                },
                write);
        });
        entered.get_future().wait();
        std::future<ExprCacheHelper::CachedBitmaps> follower;
        auto unblock = folly::makeGuard([&] { release.set_value(); });
        follower = std::async(std::launch::async, [&] {
            return Compute([&] {
                ++computations;
                return Full();
            });
        });
        EXPECT_TRUE(Waiters(1));
        release.set_value();
        unblock.dismiss();
        EXPECT_EQ(leader.get().result->count(), rows);
        EXPECT_EQ(follower.get().result->count(), rows);
        EXPECT_EQ(computations, 1);
        EXPECT_EQ(manager.GetEntryCount(), 0);
    }
}

TEST_P(ExprCacheFlightTest,
       ComputeExceptionPropagatesAndFollowerComputesWithoutPut) {
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    auto leader = std::async(std::launch::async, [&] {
        return Compute([&]() -> ExprCacheHelper::ComputeResult {
            entered.set_value();
            released.wait();
            throw SegcoreError(ErrorCode::DataFormatBroken,
                               "injected compute error");
        });
    });
    entered.get_future().wait();
    std::future<ExprCacheHelper::CachedBitmaps> follower;
    auto unblock = folly::makeGuard([&] { release.set_value(); });
    std::atomic<int> fallback{0};
    follower = std::async(std::launch::async, [&] {
        return Compute([&] {
            ++fallback;
            return Full();
        });
    });
    EXPECT_TRUE(Waiters(1));
    release.set_value();
    unblock.dismiss();
    try {
        leader.get();
        FAIL() << "expected compute error";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
    }
    EXPECT_EQ(follower.get().result->count(), rows);
    EXPECT_EQ(fallback, 1);
    EXPECT_EQ(manager.GetEntryCount(), 0);
}

TEST_P(ExprCacheFlightTest, CancelledFollowerDoesNotComputeOrCancelLeader) {
    auto leader = flights.TryJoin(Key());
    folly::CancellationSource source;
    auto token = source.getToken();
    ExprCacheFlightOptions opts{&token};
    std::atomic<int> fallback{0};
    auto follower = std::async(std::launch::async, [&] {
        return Compute(
            [&] {
                ++fallback;
                return Full();
            },
            true,
            &opts);
    });
    EXPECT_TRUE(Waiters(1));
    source.requestCancellation();
    EXPECT_THROW(follower.get(), folly::FutureCancellation);
    EXPECT_EQ(fallback, 0);
    EXPECT_EQ(flights.GetStats().flights, 1);
}

TEST_P(ExprCacheFlightTest, ClearDuringComputeRejectsOldWriteAndFollowerReuse) {
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    auto leader = std::async(std::launch::async, [&] {
        return Compute([&] {
            entered.set_value();
            released.wait();
            return Full();
        });
    });
    entered.get_future().wait();
    std::future<ExprCacheHelper::CachedBitmaps> follower;
    auto unblock = folly::makeGuard([&] { release.set_value(); });
    std::atomic<int> fallback{0};
    follower = std::async(std::launch::async, [&] {
        return Compute([&] {
            ++fallback;
            return Full();
        });
    });
    EXPECT_TRUE(Waiters(1));
    manager.Clear();
    // Follower must exit even while the producer is still held at the barrier.
    EXPECT_EQ(follower.wait_for(1s), std::future_status::ready);
    release.set_value();
    unblock.dismiss();
    leader.get();
    follower.get();
    EXPECT_EQ(fallback, 1);
    EXPECT_EQ(manager.GetEntryCount(), 0);
}

TEST_P(ExprCacheFlightTest, ResourceLimitDoesNotJoinFlight) {
    auto value = ExprResCacheManager::Value{};
    value.result = std::make_shared<TargetBitmap>(rows, true);
    value.valid_result = std::make_shared<TargetBitmap>(rows, true);
    value.active_count = rows;
    cache_config.materialization_max_bytes = 1;
    manager.SetConfig(cache_config);
    manager.Put(Key().cache_key, value);
    auto leader = flights.TryJoin(Key());
    int computed = 0;
    auto result = Compute([&] {
        ++computed;
        return Full();
    });
    EXPECT_EQ(computed, 1);
    EXPECT_EQ(result.result->count(), rows);
    EXPECT_EQ(flights.GetStats().waiters, 0);
}

TEST_P(ExprCacheFlightTest,
       NativeSwitchDoesNotClearCacheAndDisabledHelperDoesNotJoin) {
    auto value = ExprResCacheManager::Value{};
    value.result = std::make_shared<TargetBitmap>(rows, true);
    value.valid_result = std::make_shared<TargetBitmap>(rows, true);
    value.active_count = rows;
    manager.Put(Key("cached").cache_key, value);
    const auto epoch = manager.ConfigEpoch();
    EXPECT_TRUE(SetExprResCacheSingleFlightConfig(false, 1000, 1024, 64, 8));
    EXPECT_EQ(manager.ConfigEpoch(), epoch);
    EXPECT_EQ(manager.GetEntryCount(), 1);
    int computed = 0;
    auto result = Compute(
        [&] {
            ++computed;
            EXPECT_EQ(flights.GetStats().flights, 0);
            return Full();
        },
        false);
    EXPECT_EQ(computed, 1);
    EXPECT_FALSE(SetExprResCacheSingleFlightConfig(true, -1, 1024, 64, 8));
    EXPECT_FALSE(flights.IsEnabled());
}

TEST_P(ExprCacheFlightTest, FollowerReturnsWhileLeaderIsBlockedWritingCache) {
    std::promise<void> entered, release;
    auto released = release.get_future().share();
    auto leader = std::async(std::launch::async, [&] {
        return Compute([&] {
            entered.set_value();
            released.wait();
            return Full();
        });
    });
    entered.get_future().wait();
    std::future<ExprCacheHelper::CachedBitmaps> follower;
    auto unblock = folly::makeGuard([&] { release.set_value(); });
    follower = std::async(std::launch::async, [&] {
        return Compute([] {
            ADD_FAILURE() << "unexpected fallback";
            return Full();
        });
    });
    EXPECT_TRUE(Waiters(1));
    auto backend_lock = ExprCacheFlightTestPeer::HoldBackend(manager);
    release.set_value();
    unblock.dismiss();
    EXPECT_EQ(follower.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(leader.wait_for(0ms), std::future_status::timeout);
    backend_lock.unlock();
    EXPECT_EQ(follower.get().result->count(), rows);
    EXPECT_EQ(leader.get().result->count(), rows);
}

TEST_P(ExprCacheFlightTest, FollowerCopyBudgetFailureFallsBackWithoutPut) {
    cache_config.materialization_max_bytes = 2 * ((rows + 63) / 64) * 8;
    manager.SetConfig(cache_config);
    auto leader = flights.TryJoin(Key());
    flights.Complete(leader,
                     Bits());  // The shared pair fills the entire budget.
    int computed = 0;
    auto result = Compute([&] {
        ++computed;
        return Full();
    });
    EXPECT_EQ(computed, 1);
    EXPECT_EQ(result.result->count(), rows);
    EXPECT_EQ(manager.GetEntryCount(), 0);
}

TEST_P(ExprCacheFlightTest, RetireWakesFollowerAndDoesNotWaitOnFlight) {
    auto key = Key();
    auto leader = flights.TryJoin(key);
    std::atomic<int> fallback{0};
    auto follower = std::async(std::launch::async, [&] {
        return Compute([&] {
            ++fallback;
            return Full();
        });
    });
    EXPECT_TRUE(Waiters(1));
    key.cache_key.generation->Retire();
    EXPECT_EQ(follower.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(follower.get().result->count(), rows);
    EXPECT_EQ(fallback, 1);
    EXPECT_FALSE(flights.TryJoin(key));
    EXPECT_EQ(manager.GetEntryCount(), 0);
}

TEST_P(ExprCacheFlightTest,
       UnusedFlightRetirementDoesNotWaitOnBackendOrLeader) {
    auto key = Key();
    std::promise<void> entered, resume;
    auto entered_future = entered.get_future();
    auto resumed = resume.get_future().share();
    std::atomic<int> computations{0};
    std::future<ExprCacheHelper::CachedBitmaps> leader, follower;
    std::future<void> retirement;
    auto release = folly::makeGuard([&] { resume.set_value(); });
    leader = std::async(std::launch::async, [&] {
        return Compute([&] {
            ++computations;
            entered.set_value();
            resumed.wait();
            return Full();
        });
    });
    ASSERT_EQ(entered_future.wait_for(3s), std::future_status::ready);
    follower = std::async(std::launch::async, [&] {
        return Compute([&] {
            ++computations;
            return Full();
        });
    });
    EXPECT_TRUE(Waiters(1));
    // A live flight and admitted misses do not imply backend resources exist.
    // Retire while an unrelated operation owns the global backend lock.
    auto backend_lock = ExprCacheFlightTestPeer::HoldBackend(manager);
    retirement = std::async(std::launch::async, [&] {
        auto retired = manager.RetireGeneration(key.cache_key.segment_id,
                                                key.cache_key.generation);
    });
    EXPECT_EQ(retirement.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(follower.wait_for(1s), std::future_status::ready);
    resume.set_value();
    release.dismiss();
    // The old leader can finish its query but must not Put or publish Ready.
    EXPECT_EQ(leader.wait_for(1s), std::future_status::ready);
    backend_lock.unlock();
    retirement.get();
    EXPECT_EQ(follower.get().result->count(), rows);
    EXPECT_EQ(leader.get().result->count(), rows);
    EXPECT_EQ(computations, 2);
    EXPECT_EQ(manager.GetEntryCount(), 0);
    EXPECT_FALSE(flights.TryJoin(key));
    EXPECT_FALSE(key.cache_key.generation->Retire());
}

TEST_P(ExprCacheFlightTest, WarmHitDoesNotJoinRegisteredFlight) {
    auto first = Compute([] { return Full(); });
    auto leader = flights.TryJoin(Key());
    int computed = 0;
    auto second = Compute([&] {
        ++computed;
        return Full();
    });
    EXPECT_EQ(computed, 0);
    EXPECT_EQ(second.result->count(), rows);
    EXPECT_EQ(flights.GetStats().waiters, 0);
}

TEST_P(ExprCacheFlightTest, CancelledLeaderReleasesUncancelledFollower) {
    folly::CancellationSource source;
    auto token = source.getToken();
    ExprCacheFlightOptions opts{&token};
    std::promise<void> entered, resume;
    auto resumed = resume.get_future().share();
    std::future<ExprCacheHelper::CachedBitmaps> leader, follower;
    auto release = folly::makeGuard([&] { resume.set_value(); });
    leader = std::async(std::launch::async, [&] {
        return Compute(
            [&] {
                entered.set_value();
                resumed.wait();
                return Full();
            },
            true,
            &opts);
    });
    ASSERT_EQ(entered.get_future().wait_for(3s), std::future_status::ready);
    std::atomic<int> fallback{0};
    follower = std::async(std::launch::async, [&] {
        return Compute([&] {
            ++fallback;
            return Full();
        });
    });
    EXPECT_TRUE(Waiters(1));
    source.requestCancellation();
    resume.set_value();
    release.dismiss();
    EXPECT_THROW(leader.get(), folly::FutureCancellation);
    EXPECT_EQ(follower.get().result->count(), rows);
    EXPECT_EQ(fallback, 1);
    EXPECT_EQ(manager.GetEntryCount(), 0);
}

TEST_P(ExprCacheFlightTest, TimedOutFollowerDoesNotWriteOrRemoveProducer) {
    flight_config.max_wait_ms = 20;
    flights.Configure(flight_config);
    auto producer = flights.TryJoin(Key());
    int fallback = 0;
    auto result = Compute([&] {
        ++fallback;
        return Full();
    });
    EXPECT_EQ(fallback, 1);
    EXPECT_EQ(result.result->count(), rows);
    EXPECT_EQ(manager.GetEntryCount(), 0);
    EXPECT_EQ(flights.GetStats().flights, 1);
    EXPECT_EQ(flights.GetStats().waiters, 0);
    flights.Complete(producer, Bits());
    auto reused = Compute([] {
        ADD_FAILURE() << "timeout must not destroy the producer's result";
        return Full();
    });
    EXPECT_EQ(reused.result->count(), rows);
    EXPECT_EQ(manager.GetEntryCount(), 0);
}

TEST_P(ExprCacheFlightTest, BackendRefreshAndDisableRejectInflightResults) {
    for (bool disable : {false, true}) {
        SCOPED_TRACE(disable);
        ExprResCacheManager::SetEnabled(true);
        manager.Clear();
        std::promise<void> entered, resume;
        auto resumed = resume.get_future().share();
        std::future<ExprCacheHelper::CachedBitmaps> leader, follower;
        auto release = folly::makeGuard([&] { resume.set_value(); });
        leader = std::async(std::launch::async, [&] {
            return Compute([&] {
                entered.set_value();
                resumed.wait();
                return Full();
            });
        });
        ASSERT_EQ(entered.get_future().wait_for(3s), std::future_status::ready);
        std::atomic<int> fallback{0};
        follower = std::async(std::launch::async, [&] {
            return Compute([&] {
                ++fallback;
                return Full();
            });
        });
        EXPECT_TRUE(Waiters(1));
        if (disable)
            ExprResCacheManager::SetEnabled(false);
        else
            ASSERT_TRUE(manager.SetConfig(cache_config));
        EXPECT_EQ(follower.wait_for(1s), std::future_status::ready);
        resume.set_value();
        release.dismiss();
        EXPECT_EQ(leader.get().result->count(), rows);
        EXPECT_EQ(follower.get().result->count(), rows);
        EXPECT_EQ(fallback, 1);
        EXPECT_EQ(manager.GetEntryCount(), 0);
        EXPECT_EQ(manager.GetMaterializationBytes(), 0);
    }
}

TEST_P(ExprCacheFlightTest,
       PolicyRefreshBypassesOldFlightWithoutClearingCache) {
    auto cached = Compute([] { return Full(); });
    auto key = Key("policy-change");
    auto producer = flights.TryJoin(key);
    auto follower = flights.TryJoin(key);
    auto epoch = manager.ConfigEpoch();
    ++flight_config.max_waiters;
    ASSERT_TRUE(flights.Configure(flight_config));
    EXPECT_EQ(manager.ConfigEpoch(), epoch);
    EXPECT_EQ(manager.GetEntryCount(), 1);
    EXPECT_EQ(flights.Wait(follower, nullptr, [] { return true; }).status,
              Coordinator::WaitStatus::Bypass);
    EXPECT_FALSE(flights.TryJoin(key));
    follower.Reset();
    producer.Reset();
    auto next = flights.TryJoin(key);
    EXPECT_TRUE(next.IsLeader());
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         ExprCacheFlightTest,
                         ::testing::Values(CacheMode::Memory, CacheMode::Disk),
                         [](const ::testing::TestParamInfo<CacheMode>& info) {
                             return info.param == CacheMode::Memory ? "Memory"
                                                                    : "Disk";
                         });

}  // namespace
}  // namespace milvus::exec
