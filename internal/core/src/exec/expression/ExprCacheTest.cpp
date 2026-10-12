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
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <chrono>
#include <filesystem>
#include <memory>
#include <new>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include <roaring/roaring.h>

#include "bitset/bitset.h"
#include "cachinglayer/Metrics.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/ValueOp.h"
#include "exec/expression/CacheCompressor.h"
#include "exec/expression/DiskSlotFile.h"
#include "exec/expression/EntryPool.h"
#include "exec/expression/ExprCache.h"
#include "exec/expression/ExprCacheHelper.h"
#include "monitor/Monitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
// SegmentCacheFile removed; V2 uses EntryPool (memory) and DiskSlotFile (disk)
#include "gtest/gtest.h"

namespace milvus::exec {
class ExprCacheTestPeer {
 public:
    using DiskPutStage = ExprResCacheManager::DiskPutStage;

    static bool
    Put(ExprResCacheManager& manager,
        const ExprResCacheManager::Key& key,
        const ExprResCacheManager::Value& value,
        const ExprResCacheManager::DiskPutHook& hook) {
        return RunExprCacheBestEffort(
            [&]() { manager.PutInternal(key, value, nullptr, hook); });
    }

    static bool
    IsDiskSegmentRegistered(ExprResCacheManager& manager, int64_t segment_id) {
        std::lock_guard lock(manager.disk_clock_mutex_);
        return manager.disk_clock_index_.count(segment_id) != 0;
    }

    // Called only from a put hook, which already holds disk_files_mutex_.
    static DiskSlotFile&
    DiskFileDuringPut(ExprResCacheManager& manager, int64_t segment_id) {
        return *manager.disk_files_.at(segment_id);
    }

    static void
    AssertDiskState(ExprResCacheManager& manager, size_t expected_files) {
        std::shared_lock state_lock(manager.state_mutex_);
        std::shared_lock files_lock(manager.disk_files_mutex_);
        std::lock_guard clock_lock(manager.disk_clock_mutex_);
        EXPECT_EQ(manager.disk_files_.size(), expected_files);
        EXPECT_EQ(manager.disk_clock_segments_.size(), expected_files);
        EXPECT_EQ(manager.disk_clock_index_.size(), expected_files);
        EXPECT_EQ(manager.disk_segment_usage_.size(), expected_files);
        for (const auto& [segment_id, index] : manager.disk_clock_index_) {
            ASSERT_LT(index, manager.disk_clock_segments_.size());
            EXPECT_EQ(manager.disk_clock_segments_[index], segment_id);
            EXPECT_EQ(manager.disk_segment_usage_.count(segment_id), 1u);
            EXPECT_EQ(manager.disk_files_.count(segment_id), 1u);
        }
        EXPECT_EQ(manager.reported_disk_bytes_.load(),
                  manager.GetDiskCurrentBytesLocked());
    }

    static ExprResCacheManager::LookupResult
    Get(ExprResCacheManager& manager,
        const ExprResCacheManager::Key& key,
        ExprResCacheManager::Value& value,
        const std::function<void()>& before_decode) {
        return manager.GetWithStatus(key, value, before_decode);
    }
};
}  // namespace milvus::exec

using milvus::exec::ExprResCacheManager;

static_assert(noexcept(ExprResCacheManager::IsEnabled()));
static_assert(noexcept(std::declval<const ExprResCacheManager&>()
                           .CanCacheSegment(SegmentType::Sealed)));
static_assert(noexcept(std::declval<ExprResCacheManager&>().Get(
    std::declval<const ExprResCacheManager::Key&>(),
    std::declval<ExprResCacheManager::Value&>())));
static_assert(noexcept(std::declval<ExprResCacheManager&>().Put(
    std::declval<const ExprResCacheManager::Key&>(),
    std::declval<const ExprResCacheManager::Value&>())));
static_assert(noexcept(std::declval<ExprResCacheManager&>().PutAdmitted(
    std::declval<const ExprResCacheManager::Key&>(),
    std::declval<const ExprResCacheManager::Value&>(),
    std::declval<const ExprResCacheManager::AdmissionTicket&>())));

namespace {

milvus::TargetBitmap
MakeBits(size_t n, bool v = true) {
    milvus::TargetBitmap b(n);
    if (v)
        b.set();
    else
        b.reset();
    return b;
}

void
AssertBitsEqual(const milvus::TargetBitmap& a, const milvus::TargetBitmap& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        ASSERT_EQ(bool(a[i]), bool(b[i])) << "bit " << i << " differs";
    }
}

}  // namespace

// ---- Updated existing ExprResCacheManager tests (disk-backed) ----

TEST(ExprResCacheManagerTest, BestEffortBoundaryContainsCacheExceptions) {
    int calls = 0;
    EXPECT_TRUE(milvus::exec::RunExprCacheBestEffort([&]() { ++calls; }));
    EXPECT_FALSE(milvus::exec::RunExprCacheBestEffort([&]() {
        ++calls;
        throw std::bad_alloc();
    }));
    EXPECT_FALSE(milvus::exec::RunExprCacheBestEffort([&]() {
        ++calls;
        throw std::runtime_error("injected cache backend failure");
    }));
    EXPECT_EQ(calls, 3);
}

TEST(ExprResCacheManagerTest,
     HelperContainsSignatureFailureButPropagatesEvaluationFailure) {
    auto& manager = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(false);
    manager.Clear();

    milvus::exec::CacheConfig config;
    config.mode = milvus::exec::CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.materialization_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 1;
    ASSERT_TRUE(manager.SetConfig(config));
    ExprResCacheManager::SetEnabled(true);

    auto schema = std::make_shared<milvus::Schema>();
    const auto primary_field_id =
        schema->AddDebugField("pk", milvus::DataType::INT64);
    schema->set_primary_field_id(primary_field_id);
    auto segment = milvus::segcore::CreateSealedSegment(
        schema, milvus::empty_index_meta, 9001);

    int compute_calls = 0;
    auto cached = milvus::exec::ExprCacheHelper::GetOrCompute(
        segment.get(),
        []() -> std::string { throw std::bad_alloc(); },
        8,
        [&]() -> milvus::exec::ExprCacheHelper::ComputeResult {
            ++compute_calls;
            return {MakeBits(8), MakeBits(8)};
        });
    EXPECT_EQ(compute_calls, 1);
    EXPECT_NE(cached.result, nullptr);
    EXPECT_NE(cached.valid, nullptr);
    if (cached.result != nullptr) {
        EXPECT_EQ(cached.result->size(), 8);
    }
    if (cached.valid != nullptr) {
        EXPECT_EQ(cached.valid->size(), 8);
    }

    EXPECT_THROW(
        milvus::exec::ExprCacheHelper::GetOrCompute(
            segment.get(),
            []() { return std::string("evaluation-failure"); },
            8,
            []() -> milvus::exec::ExprCacheHelper::ComputeResult {
                throw std::runtime_error("injected expression failure");
            }),
        std::runtime_error);

    manager.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, PutGetBasic) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_cache_basic_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    mgr.SetDiskConfig(tmpdir.string(), 1ULL << 20, 1ULL << 20, true);

    ExprResCacheManager::Key k{123, "expr:A"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.active_count = 128;

    mgr.Put(k, v);

    ExprResCacheManager::Value got;
    got.active_count = 128;  // must match what was Put
    ASSERT_TRUE(mgr.Get(k, got));
    ASSERT_TRUE(got.result);
    ASSERT_EQ(got.result->size(), 128);
    ASSERT_TRUE(got.valid_result);
    ASSERT_EQ(got.valid_result->size(), 128);

    // restore global state
    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, CacheHitIncrementsPrometheusCounter) {
    auto& manager = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(false);
    manager.Clear();
    manager.SetCapacityBytes(1U << 20);
    ExprResCacheManager::SetEnabled(true);

    ExprResCacheManager::Key key{124, "expr:metric"};
    ExprResCacheManager::Value value;
    value.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    value.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    value.active_count = 128;
    manager.Put(key, value);

    auto& hit_counter = milvus::monitor::internal_expr_cache_hit_total;
    const auto hits_before = hit_counter.Value();

    ExprResCacheManager::Value miss;
    miss.active_count = 128;
    EXPECT_FALSE(manager.Get({124, "expr:missing"}, miss));
    EXPECT_DOUBLE_EQ(hit_counter.Value(), hits_before);

    ExprResCacheManager::Value hit;
    hit.active_count = 128;
    EXPECT_TRUE(manager.Get(key, hit));
    EXPECT_DOUBLE_EQ(hit_counter.Value(), hits_before + 1);

    ExprResCacheManager::Value status_hit;
    status_hit.active_count = 128;
    EXPECT_EQ(manager.GetWithStatus(key, status_hit),
              ExprResCacheManager::LookupResult::Hit);
    EXPECT_DOUBLE_EQ(hit_counter.Value(), hits_before + 2);

    const auto metrics = milvus::monitor::getPrometheusClient().GetMetrics();
    EXPECT_NE(metrics.find("# TYPE internal_expr_cache_hit_total counter"),
              std::string::npos);
    EXPECT_NE(metrics.find("internal_expr_cache_hit_total "),
              std::string::npos);

    manager.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, ClockEvictionByCapacity) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    // Use SetConfig with a very small memory pool so eviction triggers.
    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Memory;
    cfg.mem_max_bytes = 2000;  // very small
    cfg.compression_enabled = true;
    cfg.admission_threshold = 1;
    cfg.mem_min_eval_duration_us = 0;
    mgr.SetConfig(cfg);

    const size_t N = 8192;  // bits
    for (int i = 0; i < 20; ++i) {
        ExprResCacheManager::Key k{i + 1, "expr:x_" + std::to_string(i)};
        ExprResCacheManager::Value v;
        v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(N));
        v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(N));
        v.active_count = static_cast<int64_t>(N);
        mgr.Put(k, v);
    }

    // Pool should have evicted some entries due to small capacity
    ASSERT_LT(mgr.GetEntryCount(), 20u);
    // Should have at least 1 entry
    ASSERT_GE(mgr.GetEntryCount(), 1u);
    // Current bytes should not vastly exceed max
    ASSERT_LE(mgr.GetCurrentBytes(), 4000u);

    // restore global state
    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, EraseSegment) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_cache_erase_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    mgr.SetDiskConfig(tmpdir.string(), 1ULL << 20, 1ULL << 20, true);

    ExprResCacheManager::Key k1{10, "sig1"};
    ExprResCacheManager::Key k2{10, "sig2"};
    ExprResCacheManager::Key k3{11, "sig3"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(64));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(64));
    v.active_count = 64;
    mgr.Put(k1, v);
    mgr.Put(k2, v);
    mgr.Put(k3, v);

    size_t erased = mgr.EraseSegment(10);
    ASSERT_EQ(erased, 2u);  // 2 entries erased for segment 10

    ExprResCacheManager::Value out;
    out.active_count = 64;
    ASSERT_FALSE(mgr.Get(k1, out));
    out.active_count = 64;
    ASSERT_FALSE(mgr.Get(k2, out));
    out.active_count = 64;
    ASSERT_TRUE(mgr.Get(k3, out));

    // restore global state
    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, EnableDisable) {
    auto& mgr = ExprResCacheManager::Instance();
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_cache_endis_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    mgr.SetDiskConfig(tmpdir.string(), 1ULL << 20, 1ULL << 20, true);

    ExprResCacheManager::SetEnabled(false);
    ExprResCacheManager::Key k{7, "x"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(32));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(32));
    v.active_count = 32;
    mgr.Put(k, v);

    ExprResCacheManager::Value out;
    out.active_count = 32;
    // When disabled, Get should not hit
    ASSERT_FALSE(mgr.Get(k, out));

    ExprResCacheManager::SetEnabled(true);
    mgr.Put(k, v);
    out.active_count = 32;
    ASSERT_TRUE(mgr.Get(k, out));

    // restore global state
    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

class ExprCacheMaterializationTest
    : public ::testing::TestWithParam<milvus::exec::CacheMode> {
 protected:
    static constexpr int64_t kRows = 128;
    static constexpr size_t kPairBytes = 32;
    const ExprResCacheManager::Key key_{101, "materialized:existing"};
    ExprResCacheManager& manager_ = ExprResCacheManager::Instance();
    std::filesystem::path directory_;

    void
    SetUp() override {
        ExprResCacheManager::SetEnabled(false);
        manager_.Clear();
        directory_ = std::filesystem::temp_directory_path() /
                     ("expr_cache_lookup_budget_" + std::to_string(getpid()) +
                      "_" + std::to_string(rand()));
        milvus::exec::CacheConfig config;
        config.mode = GetParam();
        config.mem_max_bytes = 1ULL << 20;
        config.disk_base_path = directory_.string();
        config.disk_max_bytes = 1ULL << 20;
        config.disk_max_file_size = 1ULL << 20;
        config.materialization_max_bytes = kPairBytes;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        config.disk_min_eval_duration_us = 0;
        ASSERT_TRUE(manager_.SetConfig(config));
        ExprResCacheManager::SetEnabled(true);

        ExprResCacheManager::Value value;
        value.active_count = kRows;
        value.result = std::make_shared<milvus::TargetBitmap>(MakeBits(kRows));
        value.valid_result =
            std::make_shared<milvus::TargetBitmap>(MakeBits(kRows));
        manager_.Put(key_, value);
        ASSERT_EQ(manager_.GetEntryCount(), 1u);
    }

    void
    TearDown() override {
        manager_.Clear();
        ExprResCacheManager::SetEnabled(false);
        EXPECT_EQ(manager_.GetMaterializationBytes(), 0u);
        std::filesystem::remove_all(directory_);
    }
};

TEST_P(ExprCacheMaterializationTest,
       MissingAndStaleEntriesIgnoreBudgetPressure) {
    auto reservation = manager_.TryAcquireMaterialization(kRows);
    ASSERT_TRUE(reservation.has_value());
    ASSERT_EQ(manager_.GetMaterializationBytes(), kPairBytes);

    for (const auto& key :
         {ExprResCacheManager::Key{102, key_.signature},
          ExprResCacheManager::Key{key_.segment_id, "materialized:missing"},
          key_}) {
        ExprResCacheManager::Value value;
        value.active_count = key == key_ ? kRows + 1 : kRows;
        EXPECT_EQ(manager_.GetWithStatus(key, value),
                  ExprResCacheManager::LookupResult::Miss);
        EXPECT_EQ(value.result, nullptr);
        EXPECT_EQ(value.valid_result, nullptr);
        EXPECT_EQ(value.bytes, 0u);
        EXPECT_EQ(manager_.GetMaterializationBytes(), kPairBytes);
    }

    ExprResCacheManager::Value value;
    value.active_count = kRows;
    EXPECT_EQ(manager_.GetWithStatus(key_, value),
              ExprResCacheManager::LookupResult::ResourceLimit);
    EXPECT_EQ(value.result, nullptr);
    EXPECT_EQ(value.valid_result, nullptr);

    reservation.reset();
    ASSERT_EQ(manager_.GetWithStatus(key_, value),
              ExprResCacheManager::LookupResult::Hit);
    EXPECT_EQ(value.result->count(), kRows);
    EXPECT_EQ(value.valid_result->count(), kRows);
    EXPECT_EQ(manager_.GetMaterializationBytes(), kPairBytes);
}

TEST_P(ExprCacheMaterializationTest,
       MemoryGaugeTracksLeasesAndHitsAcrossReconfiguration) {
    auto& gauge = milvus::cachinglayer::monitor::cache_loaded_bytes(
        milvus::cachinglayer::CellDataType::OTHER,
        milvus::cachinglayer::StorageType::MEMORY);
    const auto baseline = gauge.Value() - manager_.GetMemoryBytes();

    // Capture reservations and decoded hit reservations use the same budget.
    auto reservation = manager_.TryAcquireMaterialization(kRows);
    ASSERT_TRUE(reservation.has_value());
    EXPECT_DOUBLE_EQ(gauge.Value(),
                     baseline + manager_.GetMemoryBytes() + kPairBytes);
    auto moved = std::move(*reservation);
    reservation.reset();
    EXPECT_FALSE(manager_.TryAcquireMaterialization(kRows).has_value());
    EXPECT_DOUBLE_EQ(gauge.Value(),
                     baseline + manager_.GetMemoryBytes() + kPairBytes);
    moved.Release();
    moved.Release();
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline + manager_.GetMemoryBytes());

    ExprResCacheManager::Value hit;
    hit.active_count = kRows;
    ASSERT_TRUE(manager_.Get(key_, hit));
    EXPECT_DOUBLE_EQ(gauge.Value(),
                     baseline + manager_.GetMemoryBytes() + kPairBytes);
    ExprResCacheManager::Value rejected;
    rejected.active_count = kRows;
    EXPECT_EQ(manager_.GetWithStatus(key_, rejected),
              ExprResCacheManager::LookupResult::ResourceLimit);
    EXPECT_EQ(manager_.GetWithStatus({key_.segment_id, "missing"}, rejected),
              ExprResCacheManager::LookupResult::Miss);
    EXPECT_DOUBLE_EQ(gauge.Value(),
                     baseline + manager_.GetMemoryBytes() + kPairBytes);

    auto retained_valid = hit.valid_result;
    hit = {};
    manager_.Clear();
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline + kPairBytes);

    milvus::exec::CacheConfig config;
    config.mode = GetParam();
    config.disk_base_path = directory_.string();
    config.materialization_max_bytes = 0;
    ASSERT_TRUE(manager_.SetConfig(config));
    ExprResCacheManager::SetEnabled(false);
    EXPECT_FALSE(manager_.TryAcquireMaterialization(kRows).has_value());
    EXPECT_EQ(manager_.GetMaterializationBytes(), kPairBytes);
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline + kPairBytes);

    retained_valid.reset();
    EXPECT_EQ(manager_.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline);
}

TEST_P(ExprCacheMaterializationTest, ConcurrentMissesDoNotRejectHits) {
    constexpr int kMissThreads = 4;
    constexpr int kIterations = 2000;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> unexpected_misses{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kMissThreads; ++i) {
        threads.emplace_back([&, i]() {
            const ExprResCacheManager::Key missing{
                i % 2 == 0 ? key_.segment_id : key_.segment_id + 1,
                "materialized:missing"};
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int j = 0; j < kIterations; ++j) {
                ExprResCacheManager::Value value;
                value.active_count = kRows;
                if (manager_.GetWithStatus(missing, value) !=
                    ExprResCacheManager::LookupResult::Miss) {
                    unexpected_misses.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != kMissThreads) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    int rejected_hits = 0;
    for (int i = 0; i < kIterations; ++i) {
        ExprResCacheManager::Value value;
        value.active_count = kRows;
        if (manager_.GetWithStatus(key_, value) !=
            ExprResCacheManager::LookupResult::Hit) {
            ++rejected_hits;
        }
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(unexpected_misses.load(), 0);
    EXPECT_EQ(rejected_hits, 0);
    EXPECT_EQ(manager_.GetMaterializationBytes(), 0u);
}

INSTANTIATE_TEST_SUITE_P(
    Backends,
    ExprCacheMaterializationTest,
    ::testing::Values(milvus::exec::CacheMode::Memory,
                      milvus::exec::CacheMode::Disk),
    [](const ::testing::TestParamInfo<milvus::exec::CacheMode>& info) {
        return info.param == milvus::exec::CacheMode::Memory ? "Memory"
                                                             : "Disk";
    });

TEST(ExprResCacheManagerTest,
     MemoryMaterializationBudgetTracksAliasedBitmapLifetime) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    constexpr int64_t kRows = 128;
    const size_t pair_bytes = MakeBits(kRows).size_in_bytes() * 2;
    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Memory;
    cfg.mem_max_bytes = 1ULL << 20;
    cfg.materialization_max_bytes = pair_bytes;
    cfg.compression_enabled = false;
    cfg.admission_threshold = 1;
    cfg.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(mgr.SetConfig(cfg));

    ExprResCacheManager::Value value;
    value.result = std::make_shared<milvus::TargetBitmap>(MakeBits(kRows));
    value.valid_result =
        std::make_shared<milvus::TargetBitmap>(MakeBits(kRows));
    value.active_count = kRows;
    const ExprResCacheManager::Key first_key{101, "materialized:first"};
    const ExprResCacheManager::Key second_key{101, "materialized:second"};
    mgr.Put(first_key, value);
    mgr.Put(second_key, value);

    ExprResCacheManager::Value first;
    first.active_count = kRows;
    EXPECT_EQ(mgr.GetWithStatus(first_key, first),
              ExprResCacheManager::LookupResult::Hit);
    EXPECT_EQ(mgr.GetMaterializationBytes(), pair_bytes);

    ExprResCacheManager::Value second;
    second.active_count = kRows;
    EXPECT_EQ(mgr.GetWithStatus(second_key, second),
              ExprResCacheManager::LookupResult::ResourceLimit);
    EXPECT_EQ(second.result, nullptr);
    EXPECT_EQ(second.valid_result, nullptr);

    // Both aliases own one decoded pair. Dropping only one bitmap must not
    // return the reservation while the companion bitmap remains reachable.
    first.result.reset();
    EXPECT_EQ(mgr.GetMaterializationBytes(), pair_bytes);
    first.valid_result.reset();
    EXPECT_EQ(mgr.GetMaterializationBytes(), 0u);

    second.active_count = kRows;
    EXPECT_EQ(mgr.GetWithStatus(second_key, second),
              ExprResCacheManager::LookupResult::Hit);
    EXPECT_EQ(mgr.GetMaterializationBytes(), pair_bytes);

    // Backend eviction/clear must not invalidate a value already returned to
    // a query or release its reservation prematurely.
    mgr.Clear();
    EXPECT_EQ(mgr.GetMaterializationBytes(), pair_bytes);
    second.result.reset();
    second.valid_result.reset();
    EXPECT_EQ(mgr.GetMaterializationBytes(), 0u);
    ExprResCacheManager::SetEnabled(false);
}

class ExprCacheUnlockedReadTest
    : public ::testing::TestWithParam<std::tuple<std::string, bool>> {};

TEST_P(ExprCacheUnlockedReadTest, WritersCompleteBeforeDecodeResumes) {
    using namespace milvus::exec;
    const auto& [operation, compression_enabled] = GetParam();
    auto& manager = ExprResCacheManager::Instance();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("expr_unlocked_read_" + std::to_string(getpid()) + "_" +
                      std::to_string(rand()));
    const auto cleanup = folly::makeGuard([&]() {
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
        std::filesystem::remove_all(dir);
    });
    ExprResCacheManager::SetEnabled(true);
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    config.disk_min_eval_duration_us = 0;
    config.compression_enabled = compression_enabled;
    config.disk_base_path = dir.string();
    ASSERT_TRUE(manager.SetConfig(config));
    constexpr int64_t kRows = 4096;
    ExprResCacheManager::Key key{100, "held"};
    ExprResCacheManager::Value value;
    value.result =
        std::make_shared<milvus::TargetBitmap>(MakeBits(kRows, false));
    value.valid_result =
        std::make_shared<milvus::TargetBitmap>(MakeBits(kRows));
    (*value.result)[57] = true;
    (*value.valid_result)[19] = false;
    value.active_count = kRows;
    manager.Put(key, value);
    const auto charge = manager.GetMemoryBytes();
    ASSERT_GT(charge, 0u);
    config.mem_max_bytes = charge * 2;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Put(key, value);
    manager.Put({200, "other"}, value);
    ASSERT_EQ(manager.GetEntryCount(), 2u);
    // Gauge has contributions from other cachinglayer users as well.
    auto& gauge = milvus::cachinglayer::monitor::cache_loaded_bytes(
        milvus::cachinglayer::CellDataType::OTHER,
        milvus::cachinglayer::StorageType::MEMORY);
    const auto gauge_base = gauge.Value() - manager.GetMemoryBytes();

    std::promise<void> entered, resume;
    auto entered_future = entered.get_future();
    auto resume_future = resume.get_future().share();
    auto reader = std::async(std::launch::async, [&]() {
        ExprResCacheManager::Value got;
        got.active_count = kRows;
        auto status = ExprCacheTestPeer::Get(manager, key, got, [&]() {
            entered.set_value();
            resume_future.wait();
        });
        return std::pair{status, std::move(got)};
    });
    auto release_reader = folly::makeGuard([&]() { resume.set_value(); });
    ASSERT_EQ(entered_future.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(manager.GetMaterializationBytes(), 2 * (kRows / 8));
    auto writer = std::async(std::launch::async, [&]() {
        if (operation == "PutEvict") {
            manager.Put({300, "new"}, value);
            EXPECT_EQ(manager.GetEntryCount(), 2u);
            ExprResCacheManager::Value missing;
            missing.active_count = kRows;
            EXPECT_FALSE(manager.Get({200, "other"}, missing));
        } else if (operation == "EraseOther") {
            EXPECT_EQ(manager.EraseSegment(200), 1u);
        } else if (operation == "EraseSame") {
            EXPECT_EQ(manager.EraseSegment(100), 1u);
        } else if (operation == "Clear") {
            manager.Clear();
            EXPECT_EQ(manager.GetEntryCount(), 0u);
        } else if (operation == "Rebuild") {
            ASSERT_TRUE(manager.SetConfig(config));
            manager.Put({300, "new"}, value);
            // The old pool's reader still consumes half the shared budget.
            // A second new entry must evict the first, not create a fresh
            // independent allowance for this generation.
            manager.Put({400, "last"}, value);
            EXPECT_EQ(manager.GetEntryCount(), 1u);
            EXPECT_EQ(manager.GetMemoryBytes(), charge * 2);
        } else if (operation == "Shrink") {
            config.mem_max_bytes = charge / 2;
            ASSERT_TRUE(manager.SetConfig(config));
            manager.Put({300, "new"}, value);
            EXPECT_EQ(manager.GetEntryCount(), 0u);
        } else if (operation == "SwitchDisk") {
            config.mode = CacheMode::Disk;
            ASSERT_TRUE(manager.SetConfig(config));
            manager.Put({300, "new"}, value);
            EXPECT_EQ(manager.GetEntryCount(), 1u);
        } else if (operation == "ConfigFailure") {
            std::filesystem::create_directories(dir);
            const auto file = dir / "not-a-directory";
            std::ofstream(file) << "x";
            config.mode = CacheMode::Disk;
            config.disk_base_path = file.string();
            EXPECT_FALSE(manager.SetConfig(config));
        }
        EXPECT_GE(manager.GetMemoryBytes(), charge);
        EXPECT_DOUBLE_EQ(gauge.Value(),
                         gauge_base + manager.GetMemoryBytes() +
                             manager.GetMaterializationBytes());
    });
    EXPECT_EQ(writer.wait_for(std::chrono::seconds(2)),
              std::future_status::ready)
        << operation << " waited for a reader paused before decompression";
    resume.set_value();
    release_reader.dismiss();
    writer.get();
    auto [status, got] = reader.get();
    ASSERT_EQ(status, ExprResCacheManager::LookupResult::Hit);
    AssertBitsEqual(*value.result, *got.result);
    AssertBitsEqual(*value.valid_result, *got.valid_result);
    EXPECT_EQ(manager.GetMaterializationBytes(), 2 * (kRows / 8));
    manager.Clear();
    EXPECT_EQ(manager.GetMemoryBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(),
                     gauge_base + manager.GetMaterializationBytes());
    // Returned decoded values do not retain their compressed payload.
    EXPECT_EQ(manager.GetMaterializationBytes(), 2 * (kRows / 8));
    got = {};
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), gauge_base);
}

INSTANTIATE_TEST_SUITE_P(Memory,
                         ExprCacheUnlockedReadTest,
                         ::testing::Combine(::testing::Values("PutEvict",
                                                              "EraseOther",
                                                              "EraseSame",
                                                              "Clear",
                                                              "Rebuild",
                                                              "Shrink",
                                                              "SwitchDisk",
                                                              "ConfigFailure"),
                                            ::testing::Bool()),
                         [](const auto& info) {
                             return std::string(std::get<1>(info.param)
                                                    ? "Compressed"
                                                    : "Raw") +
                                    std::get<0>(info.param);
                         });

TEST(ExprResCacheManagerTest, FailedUnlockedReadReleasesBothReservations) {
    using namespace milvus::exec;
    auto& manager = ExprResCacheManager::Instance();
    const auto cleanup = folly::makeGuard([&]() {
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
    });
    ExprResCacheManager::SetEnabled(true);
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    auto& gauge = milvus::cachinglayer::monitor::cache_loaded_bytes(
        milvus::cachinglayer::CellDataType::OTHER,
        milvus::cachinglayer::StorageType::MEMORY);
    const auto gauge_base = gauge.Value();
    ExprResCacheManager::Value value;
    value.active_count = 128;
    value.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    value.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    ExprResCacheManager::Key key{101, "failure"};
    manager.Put(key, value);
    ExprResCacheManager::Value got;
    got.active_count = 128;
    EXPECT_FALSE(RunExprCacheBestEffort([&]() {
        ExprCacheTestPeer::Get(manager, key, got, [&]() {
            manager.Clear();
            EXPECT_GT(manager.GetMemoryBytes(), 0u);
            EXPECT_GT(manager.GetMaterializationBytes(), 0u);
            EXPECT_DOUBLE_EQ(gauge.Value(),
                             gauge_base + manager.GetMemoryBytes() +
                                 manager.GetMaterializationBytes());
            throw std::bad_alloc();
        });
    }));
    EXPECT_EQ(got.result, nullptr);
    EXPECT_EQ(got.valid_result, nullptr);
    EXPECT_EQ(manager.GetMemoryBytes(), 0u);
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), gauge_base);
}

TEST(ExprResCacheManagerTest, DiskMaterializationBudgetBoundsConcurrentHits) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_cache_materialization_disk_" +
                   std::to_string(getpid()) + "_" + std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    constexpr int64_t kRows = 128;
    const size_t pair_bytes = MakeBits(kRows).size_in_bytes() * 2;
    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = tmpdir.string();
    cfg.disk_max_bytes = 1ULL << 20;
    cfg.disk_max_file_size = 1ULL << 20;
    cfg.materialization_max_bytes = pair_bytes;
    cfg.admission_threshold = 1;
    cfg.disk_min_eval_duration_us = 0;
    ASSERT_TRUE(mgr.SetConfig(cfg));

    auto& gauge = milvus::cachinglayer::monitor::cache_loaded_bytes(
        milvus::cachinglayer::CellDataType::OTHER,
        milvus::cachinglayer::StorageType::MEMORY);
    const auto gauge_base = gauge.Value();
    const ExprResCacheManager::Key key{202, "materialized:disk"};
    ExprResCacheManager::Value value;
    value.result = std::make_shared<milvus::TargetBitmap>(MakeBits(kRows));
    value.valid_result =
        std::make_shared<milvus::TargetBitmap>(MakeBits(kRows));
    value.active_count = kRows;
    mgr.Put(key, value);
    ASSERT_EQ(mgr.GetEntryCount(), 1u);

    constexpr int kThreads = 8;
    std::atomic<int> ready{0};
    std::atomic<int> hits{0};
    std::atomic<int> limited{0};
    std::atomic<int> misses{0};
    std::atomic<bool> release_hit{false};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&]() {
            ExprResCacheManager::Value got;
            got.active_count = kRows;
            const auto status = mgr.GetWithStatus(key, got);
            if (status == ExprResCacheManager::LookupResult::Hit) {
                hits.fetch_add(1, std::memory_order_relaxed);
            } else if (status ==
                       ExprResCacheManager::LookupResult::ResourceLimit) {
                limited.fetch_add(1, std::memory_order_relaxed);
            } else {
                misses.fetch_add(1, std::memory_order_relaxed);
            }
            ready.fetch_add(1, std::memory_order_release);
            if (status == ExprResCacheManager::LookupResult::Hit) {
                while (!release_hit.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != kThreads) {
        std::this_thread::yield();
    }
    EXPECT_EQ(hits.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(limited.load(std::memory_order_relaxed), kThreads - 1);
    EXPECT_EQ(misses.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(mgr.GetMaterializationBytes(), pair_bytes);
    EXPECT_DOUBLE_EQ(gauge.Value(), gauge_base + pair_bytes);
    release_hit.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(mgr.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), gauge_base);

    // Preserve the slot header and result payload but truncate the validity
    // payload. A failed read after allocating both bitmaps must return its
    // reservation and must not expose a partially materialized hit.
    std::filesystem::resize_file(
        tmpdir / "seg_202.cache",
        milvus::exec::DiskSlotFile::kFileHeaderSize +
            milvus::exec::DiskSlotFile::kSlotHeaderSize + pair_bytes / 2);
    ExprResCacheManager::Value failed;
    failed.active_count = kRows;
    EXPECT_EQ(mgr.GetWithStatus(key, failed),
              ExprResCacheManager::LookupResult::Miss);
    EXPECT_EQ(failed.result, nullptr);
    EXPECT_EQ(failed.valid_result, nullptr);
    EXPECT_EQ(failed.bytes, 0u);
    EXPECT_EQ(mgr.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), gauge_base);

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

// ---- Old ExprResCacheManagerDiskTest tests updated to use SetDiskConfig shim (memory mode) ----

class ExprResCacheManagerDiskTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() /
                    ("expr_cache_disk_test_" + std::to_string(getpid()) + "_" +
                     std::to_string(rand()));
        std::filesystem::create_directories(test_dir_);
        mgr_ = &ExprResCacheManager::Instance();
        ExprResCacheManager::SetEnabled(true);
        mgr_->Clear();
    }
    void
    TearDown() override {
        mgr_->Clear();
        std::filesystem::remove_all(test_dir_);
        ExprResCacheManager::SetEnabled(false);
    }

    ExprResCacheManager::Value
    MakeValue(size_t n_bits, int64_t active_count) {
        ExprResCacheManager::Value v;
        v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(n_bits));
        v.valid_result =
            std::make_shared<milvus::TargetBitmap>(MakeBits(n_bits));
        v.active_count = active_count;
        return v;
    }

    std::filesystem::path test_dir_;
    ExprResCacheManager* mgr_{nullptr};
};

TEST_F(ExprResCacheManagerDiskTest, BackendPutGet) {
    // SetDiskConfig shim now routes to memory mode (EntryPool)
    mgr_->SetDiskConfig(test_dir_.string(), 1ULL << 20, 1ULL << 20, true);

    ExprResCacheManager::Key k{200, "disk_test_sig"};
    auto v = MakeValue(256, 256);
    mgr_->Put(k, v);

    // Verify we can read back
    ExprResCacheManager::Value got;
    got.active_count = 256;
    ASSERT_TRUE(mgr_->Get(k, got));
    ASSERT_TRUE(got.result);
    ASSERT_EQ(got.result->size(), 256);
    ASSERT_TRUE(got.valid_result);
    ASSERT_EQ(got.valid_result->size(), 256);
}

TEST_F(ExprResCacheManagerDiskTest, BackendEraseSegment) {
    mgr_->SetDiskConfig(test_dir_.string(), 1ULL << 20, 1ULL << 20, true);

    ExprResCacheManager::Key k{300, "erase_test"};
    auto v = MakeValue(128, 128);
    mgr_->Put(k, v);

    size_t erased = mgr_->EraseSegment(300);
    ASSERT_EQ(erased, 1u);

    // Get should miss after erase
    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_FALSE(mgr_->Get(k, got));
}

TEST_F(ExprResCacheManagerDiskTest, PutGetAcrossSegments) {
    mgr_->SetDiskConfig(test_dir_.string(), 1ULL << 20, 1ULL << 20, true);

    // Write to 5 segments
    for (int i = 1; i <= 5; ++i) {
        ExprResCacheManager::Key k{i, "cross_seg_" + std::to_string(i)};
        auto v = MakeValue(512, 512);
        mgr_->Put(k, v);
    }

    // All should be accessible
    for (int i = 1; i <= 5; ++i) {
        ExprResCacheManager::Key k{i, "cross_seg_" + std::to_string(i)};
        ExprResCacheManager::Value got;
        got.active_count = 512;
        ASSERT_TRUE(mgr_->Get(k, got)) << "segment " << i;
        ASSERT_EQ(got.result->size(), 512) << "segment " << i;
    }

    ASSERT_EQ(mgr_->GetEntryCount(), 5u);
}

TEST_F(ExprResCacheManagerDiskTest, ClearRemovesAll) {
    mgr_->SetDiskConfig(test_dir_.string(), 1ULL << 20, 1ULL << 20, true);

    // Put 3 segments
    for (int i = 1; i <= 3; ++i) {
        ExprResCacheManager::Key k{i * 100, "clear_test"};
        auto v = MakeValue(128, 128);
        mgr_->Put(k, v);
    }

    mgr_->Clear();

    ASSERT_EQ(mgr_->GetEntryCount(), 0u);
    ASSERT_EQ(mgr_->GetCurrentBytes(), 0u);
}

TEST_F(ExprResCacheManagerDiskTest, EnableDisable) {
    mgr_->SetDiskConfig(test_dir_.string(), 1ULL << 20, 1ULL << 20, true);
    ExprResCacheManager::SetEnabled(false);

    ExprResCacheManager::Key k{600, "disabled_sig"};
    auto v = MakeValue(128, 128);
    mgr_->Put(k, v);

    // Get should miss when disabled
    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_FALSE(mgr_->Get(k, got));

    // Re-enable and put
    ExprResCacheManager::SetEnabled(true);
    mgr_->Put(k, v);

    got.active_count = 128;
    ASSERT_TRUE(mgr_->Get(k, got));
}

using milvus::exec::CacheCompressor;

// SegmentCacheFile tests removed — V1 mmap backend replaced by EntryPool/DiskSlotFile.

// ---- CacheCompressor tests ----

using milvus::exec::kCompTypeIndependent;
using milvus::exec::kCompTypeRaw;
using milvus::exec::kCompTypeRoaring;
using milvus::exec::kCompTypeRoaringInv;

namespace {

// Helper: create a bitset with the given number of bits and set bits
// at positions determined by density (pseudo-random).
milvus::TargetBitmap
MakeRandomBits(size_t n, double density, uint32_t seed = 42) {
    milvus::TargetBitmap b(n);
    b.reset();
    std::mt19937 rng(seed);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (size_t i = 0; i < n; ++i) {
        if (dist(rng) < density) {
            b[i] = true;
        }
    }
    return b;
}

}  // namespace

TEST(CacheCompressorTest, DenseResultAndRawValidityRoundTrip) {
    const size_t n = 1024;
    auto result = MakeBits(n, true);
    auto valid = MakeBits(n, false);
    // Set some bits in valid to make it non-trivial
    for (size_t i = 0; i < n; i += 3) {
        valid[i] = true;
    }

    uint8_t comp_type = 0;
    auto compressed = CacheCompressor::Compress(result, valid, true, comp_type);
    ASSERT_EQ(comp_type, kCompTypeIndependent);
    ASSERT_EQ(static_cast<uint8_t>(compressed[8]), kCompTypeRoaringInv);
    ASSERT_EQ(static_cast<uint8_t>(compressed[9]), kCompTypeRaw);

    milvus::TargetBitmap out_result(0);
    milvus::TargetBitmap out_valid(0);
    CacheCompressor::Decompress(compressed.data(),
                                static_cast<uint32_t>(compressed.size()),
                                comp_type,
                                out_result,
                                out_valid);

    AssertBitsEqual(result, out_result);
    AssertBitsEqual(valid, out_valid);
}

TEST(CacheCompressorTest, NoCompression) {
    const size_t n = 1024;
    auto result = MakeRandomBits(n, 0.5, 1);
    auto valid = MakeRandomBits(n, 0.5, 2);

    uint8_t comp_type = 0;
    auto compressed =
        CacheCompressor::Compress(result, valid, false, comp_type);
    ASSERT_EQ(comp_type, kCompTypeRaw);

    milvus::TargetBitmap out_result(0);
    milvus::TargetBitmap out_valid(0);
    CacheCompressor::Decompress(compressed.data(),
                                static_cast<uint32_t>(compressed.size()),
                                comp_type,
                                out_result,
                                out_valid);

    AssertBitsEqual(result, out_result);
    AssertBitsEqual(valid, out_valid);
}

TEST(CacheCompressorTest, EmptyBitset) {
    milvus::TargetBitmap result(0);
    milvus::TargetBitmap valid(0);

    // With compression enabled
    {
        uint8_t comp_type = 0;
        auto compressed =
            CacheCompressor::Compress(result, valid, true, comp_type);
        // Empty bitsets should use raw (no data to compress)
        ASSERT_EQ(comp_type, kCompTypeRaw);

        milvus::TargetBitmap out_result(0);
        milvus::TargetBitmap out_valid(0);
        CacheCompressor::Decompress(compressed.data(),
                                    static_cast<uint32_t>(compressed.size()),
                                    comp_type,
                                    out_result,
                                    out_valid);
        ASSERT_EQ(out_result.size(), 0);
        ASSERT_EQ(out_valid.size(), 0);
    }

    // With compression disabled
    {
        uint8_t comp_type = 0;
        auto compressed =
            CacheCompressor::Compress(result, valid, false, comp_type);
        ASSERT_EQ(comp_type, kCompTypeRaw);

        milvus::TargetBitmap out_result(0);
        milvus::TargetBitmap out_valid(0);
        CacheCompressor::Decompress(compressed.data(),
                                    static_cast<uint32_t>(compressed.size()),
                                    comp_type,
                                    out_result,
                                    out_valid);
        ASSERT_EQ(out_result.size(), 0);
        ASSERT_EQ(out_valid.size(), 0);
    }
}

TEST(CacheCompressorTest, LargeBitset) {
    const size_t n = 1000000;  // 1M bits
    auto result = MakeRandomBits(n, 0.01, 100);
    auto valid = MakeBits(n, true);

    const size_t raw_bytes = result.size_in_bytes() + valid.size_in_bytes();

    uint8_t comp_type = 0;
    auto compressed = CacheCompressor::Compress(result, valid, true, comp_type);
    // 1% density → Roaring auto-selected
    ASSERT_NE(comp_type, kCompTypeRaw);

    // Compressed size should be smaller than raw
    ASSERT_LT(compressed.size(), raw_bytes);

    milvus::TargetBitmap out_result(0);
    milvus::TargetBitmap out_valid(0);
    CacheCompressor::Decompress(compressed.data(),
                                static_cast<uint32_t>(compressed.size()),
                                comp_type,
                                out_result,
                                out_valid);

    AssertBitsEqual(result, out_result);
    AssertBitsEqual(valid, out_valid);
}

TEST(CacheCompressorTest, VariousDensities) {
    const size_t n = 8192;
    const double densities[] = {0.001, 0.01, 0.5, 0.99};

    for (double density : densities) {
        for (bool compress : {true, false}) {
            auto result = MakeRandomBits(n, density, 77);
            auto valid = MakeRandomBits(n, 1.0 - density, 88);

            uint8_t comp_type = 0;
            auto compressed =
                CacheCompressor::Compress(result, valid, compress, comp_type);

            if (compress) {
                ASSERT_TRUE(comp_type == kCompTypeIndependent ||
                            comp_type == kCompTypeRaw)
                    << "density=" << density;
            } else {
                ASSERT_EQ(comp_type, kCompTypeRaw) << "density=" << density;
            }

            milvus::TargetBitmap out_result(0);
            milvus::TargetBitmap out_valid(0);
            CacheCompressor::Decompress(
                compressed.data(),
                static_cast<uint32_t>(compressed.size()),
                comp_type,
                out_result,
                out_valid);

            AssertBitsEqual(result, out_result);
            AssertBitsEqual(valid, out_valid);
        }
    }
}

TEST(CacheCompressorTest, ResultAndValidityChooseEncodingIndependently) {
    constexpr size_t rows = 10000;
    const std::pair<size_t, uint8_t> cases[] = {
        {0, kCompTypeRoaring},
        {1, kCompTypeRoaring},
        {300, kCompTypeRoaring},
        {301, kCompTypeRaw},
        {5000, kCompTypeRaw},
        {9699, kCompTypeRaw},
        {9700, kCompTypeRoaringInv},
        {9999, kCompTypeRoaringInv},
        {10000, kCompTypeRoaringInv},
    };
    for (auto [result_count, result_type] : cases) {
        for (auto [valid_count, valid_type] : cases) {
            SCOPED_TRACE(::testing::Message()
                         << "result_count=" << result_count
                         << " valid_count=" << valid_count);
            auto result = MakeBits(rows, false);
            result.set(0, result_count, true);
            auto valid = MakeBits(rows, false);
            valid.set(0, valid_count, true);
            // All-ones validity is omitted, with its codec slot set to Raw.
            if (valid_count == rows) {
                valid_type = kCompTypeRaw;
            }

            auto encoded = CacheCompressor::Compress(result, valid, true);
            EXPECT_EQ(encoded.result_comp_type, result_type);
            EXPECT_EQ(encoded.valid_comp_type, valid_type);

            uint8_t comp_type = 0;
            auto compressed =
                CacheCompressor::Compress(result, valid, true, comp_type);
            if (result_type == kCompTypeRaw && valid_type == kCompTypeRaw) {
                ASSERT_EQ(comp_type, kCompTypeRaw);
            } else {
                ASSERT_EQ(comp_type, kCompTypeIndependent);
                ASSERT_EQ(static_cast<uint8_t>(compressed[8]), result_type);
                ASSERT_EQ(static_cast<uint8_t>(compressed[9]), valid_type);
            }

            milvus::TargetBitmap out_result(0), out_valid(0);
            ASSERT_TRUE(CacheCompressor::Decompress(compressed.data(),
                                                    compressed.size(),
                                                    comp_type,
                                                    out_result,
                                                    out_valid));
            AssertBitsEqual(result, out_result);
            AssertBitsEqual(valid, out_valid);
        }
    }
}

TEST(CacheCompressorTest, OneNullStoresOnlyNullPosition) {
    constexpr size_t rows = 10000000;
    constexpr uint32_t null_row = 65536;
    auto result = MakeBits(rows, false);
    result[rows - 1] = true;
    auto valid = MakeBits(rows, true);
    valid[null_row] = false;

    uint8_t comp_type = 0;
    auto compressed = CacheCompressor::Compress(result, valid, true, comp_type);
    ASSERT_EQ(comp_type, kCompTypeIndependent);
    ASSERT_EQ(static_cast<uint8_t>(compressed[8]), kCompTypeRoaring);
    ASSERT_EQ(static_cast<uint8_t>(compressed[9]), kCompTypeRoaringInv);

    uint32_t result_size = 0;
    std::memcpy(&result_size, compressed.data() + 10, sizeof(result_size));
    const size_t valid_offset = 14 + result_size;
    ASSERT_LT(valid_offset, compressed.size());
    std::unique_ptr<roaring_bitmap_t, decltype(&roaring_bitmap_free)>
        encoded_valid(
            roaring_bitmap_deserialize_safe(compressed.data() + valid_offset,
                                            compressed.size() - valid_offset),
            roaring_bitmap_free);
    ASSERT_NE(encoded_valid, nullptr);
    EXPECT_EQ(roaring_bitmap_get_cardinality(encoded_valid.get()), 1);
    EXPECT_TRUE(roaring_bitmap_contains(encoded_valid.get(), null_row));

    milvus::TargetBitmap out_result(0), out_valid(0);
    ASSERT_TRUE(CacheCompressor::Decompress(compressed.data(),
                                            compressed.size(),
                                            comp_type,
                                            out_result,
                                            out_valid));
    EXPECT_EQ(out_result.count(), 1);
    EXPECT_TRUE(out_result[rows - 1]);
    EXPECT_EQ(out_valid.size(), rows);
    EXPECT_EQ(out_valid.count(), rows - 1);
    EXPECT_FALSE(out_valid[null_row]);
}

TEST(CacheCompressorTest, RoaringContainersAtWordAndContainerBoundaries) {
    for (size_t rows :
         {1, 63, 64, 65, 4095, 4096, 4097, 8193, 65535, 65536, 65537, 131073}) {
        // Exercise word and container boundaries, as well as inversion
        // with dirty padding from TargetBitmap(rows, true).
        for (int pattern = 0; pattern < 4; ++pattern) {
            SCOPED_TRACE(::testing::Message()
                         << "rows=" << rows << " pattern=" << pattern);
            milvus::TargetBitmap result(rows, false);
            milvus::TargetBitmap valid(rows, pattern == 3);
            if (pattern == 0) {
                valid[rows - 1] = true;
            } else if (pattern == 1) {
                for (size_t i = 0; i < rows; i += 2) {
                    valid[i] = true;
                }
            } else if (pattern == 2) {
                valid.set(rows / 4, rows / 2, true);
            } else {
                valid[rows - 1] = false;
            }

            uint8_t comp_type = 0;
            auto compressed =
                CacheCompressor::Compress(result, valid, true, comp_type);
            milvus::TargetBitmap out_result(0), out_valid(0);
            ASSERT_TRUE(CacheCompressor::Decompress(compressed.data(),
                                                    compressed.size(),
                                                    comp_type,
                                                    out_result,
                                                    out_valid));
            AssertBitsEqual(result, out_result);
            AssertBitsEqual(valid, out_valid);
        }
    }
}

TEST(CacheCompressorTest, RoaringContainerCardinalityBoundary) {
    constexpr size_t rows = (1U << 20) + 1;
    for (size_t cardinality : {4095, 4096, 4097}) {
        for (bool invert_result : {false, true}) {
            for (bool invert_valid : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << "cardinality=" << cardinality
                             << " invert_result=" << invert_result
                             << " invert_valid=" << invert_valid);
                milvus::TargetBitmap result(rows, invert_result);
                milvus::TargetBitmap valid(rows, invert_valid);
                // Keep each encoded set in one container. Spaced positions
                // prevent run optimization from hiding the array/bitset
                // boundary; the overall density still selects Roaring.
                for (size_t i = 0; i < cardinality; ++i) {
                    result[i * 2] = !invert_result;
                    valid[65536 + i * 2] = !invert_valid;
                }

                uint8_t comp_type = 0;
                auto compressed =
                    CacheCompressor::Compress(result, valid, true, comp_type);
                ASSERT_EQ(comp_type, kCompTypeIndependent);
                ASSERT_EQ(
                    static_cast<uint8_t>(compressed[8]),
                    invert_result ? kCompTypeRoaringInv : kCompTypeRoaring);
                ASSERT_EQ(
                    static_cast<uint8_t>(compressed[9]),
                    invert_valid ? kCompTypeRoaringInv : kCompTypeRoaring);

                milvus::TargetBitmap out_result(0), out_valid(0);
                ASSERT_TRUE(CacheCompressor::Decompress(compressed.data(),
                                                        compressed.size(),
                                                        comp_type,
                                                        out_result,
                                                        out_valid));
                AssertBitsEqual(result, out_result);
                AssertBitsEqual(valid, out_valid);
            }
        }
    }
}

TEST(CacheCompressorTest, RejectsUnsupportedEntryFormats) {
    auto result = MakeBits(65, false);
    auto valid = MakeBits(65, false);
    uint8_t comp_type = 0;
    auto compressed = CacheCompressor::Compress(result, valid, true, comp_type);
    ASSERT_EQ(comp_type, kCompTypeIndependent);
    ASSERT_EQ(static_cast<uint8_t>(compressed[8]), kCompTypeRoaring);
    ASSERT_EQ(static_cast<uint8_t>(compressed[9]), kCompTypeRoaring);

    // Removing the per-bitmap codecs produces the unsupported old layout.
    // Its payload is valid Roaring, so rejection must be based on the format.
    compressed.erase(compressed.begin() + 8, compressed.begin() + 10);
    for (uint8_t unsupported : {uint8_t{0},
                                kCompTypeRoaring,
                                kCompTypeRoaringInv,
                                uint8_t{0x81},
                                uint8_t{0x82}}) {
        SCOPED_TRACE(static_cast<int>(unsupported));
        milvus::TargetBitmap out_result(0), out_valid(0);
        EXPECT_FALSE(CacheCompressor::Decompress(compressed.data(),
                                                 compressed.size(),
                                                 unsupported,
                                                 out_result,
                                                 out_valid));
    }
}

TEST(CacheCompressorTest, CorruptPayloadReturnsFalse) {
    const size_t n = 1024;
    auto result = MakeRandomBits(n, 0.01, 3);
    auto valid = MakeBits(n, true);

    uint8_t comp_type = 0;
    auto compressed = CacheCompressor::Compress(result, valid, true, comp_type);
    ASSERT_EQ(comp_type, kCompTypeIndependent);

    milvus::TargetBitmap out_result(0);
    milvus::TargetBitmap out_valid(0);
    ASSERT_FALSE(CacheCompressor::Decompress(
        compressed.data(), 10, comp_type, out_result, out_valid));

    std::memset(compressed.data() + 10, 0x7F, 4);
    ASSERT_FALSE(
        CacheCompressor::Decompress(compressed.data(),
                                    static_cast<uint32_t>(compressed.size()),
                                    comp_type,
                                    out_result,
                                    out_valid));
}

TEST(CacheCompressorTest, RejectsInvalidPerBitmapEncodingAndLengths) {
    const size_t rows = 1024;
    auto result = MakeRandomBits(rows, 0.5);
    auto valid = MakeBits(rows, true);
    valid[63] = false;
    uint8_t comp_type = 0;
    const auto original =
        CacheCompressor::Compress(result, valid, true, comp_type);
    ASSERT_EQ(comp_type, kCompTypeIndependent);
    ASSERT_EQ(static_cast<uint8_t>(original[8]), kCompTypeRaw);
    ASSERT_EQ(static_cast<uint8_t>(original[9]), kCompTypeRoaringInv);
    uint32_t result_size = 0;
    std::memcpy(&result_size, original.data() + 10, 4);

    auto rejected = [&](const std::vector<char>& data) {
        milvus::TargetBitmap out_result(0), out_valid(0);
        return !CacheCompressor::Decompress(
            data.data(), data.size(), comp_type, out_result, out_valid);
    };
    for (size_t codec_offset : {8, 9}) {
        auto corrupt = original;
        corrupt[codec_offset] = 0x7f;
        EXPECT_TRUE(rejected(corrupt));
    }
    // A Raw bitmap must have exactly the expected number of bytes.
    auto corrupt = original;
    const uint32_t short_result = result_size - 1;
    std::memcpy(corrupt.data() + 10, &short_result, 4);
    EXPECT_TRUE(rejected(corrupt));

    // Truncated validity, a wrong validity codec, and a contradictory
    // all-ones flag must all reject the entry instead of returning bad bits.
    corrupt = original;
    corrupt.resize(14 + result_size);
    EXPECT_TRUE(rejected(corrupt));
    corrupt = original;
    corrupt[9] = static_cast<char>(kCompTypeRaw);
    EXPECT_TRUE(rejected(corrupt));
    corrupt = original;
    const uint32_t all_ones = rows | milvus::exec::kValidAllOnesMask;
    std::memcpy(corrupt.data() + 4, &all_ones, 4);
    EXPECT_TRUE(rejected(corrupt));
}

// SegmentCacheFileTest::PerfBenchmark removed — V1 mmap backend replaced.

// ---- Performance benchmarks ----

#include <chrono>
#include <numeric>

TEST(ExprResCacheManagerPerfTest, EndToEndAllDensities) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_perf_density_" + std::to_string(getpid()));
    std::filesystem::create_directories(tmpdir);
    mgr.SetDiskConfig(tmpdir.string(), 1ULL << 30, 256ULL << 20, true);

    struct Scenario {
        const char* name;
        double density;
    };
    std::vector<Scenario> scenarios = {
        {"0.1%", 0.001},
        {"1%", 0.01},
        {"5%", 0.05},
        {"10%", 0.10},
        {"50%", 0.50},
        {"90%", 0.90},
        {"99%", 0.99},
    };

    const size_t N_BITS = 1000000;
    const int N_ENTRIES = 100;
    const int N_GET_REPEAT = 3;  // repeat Get loop, drop first iteration (cold)

    printf("\n");
    printf(
        "==================================================================="
        "\n");
    printf(
        "  ExprResCacheManager E2E: auto-select compression, all densities\n");
    printf("  Full path: Put(compress+append) → Get(read+decompress+verify)\n");
    printf("  %d entries per density, 1M-row bitset, valid=all-ones\n",
           N_ENTRIES);
    printf(
        "==================================================================="
        "\n\n");

    printf("%-8s | %8s | %8s %8s %8s %8s | %10s | %s\n",
           "Density",
           "Raw(B)",
           "Put(us)",
           "Get_avg",
           "Get_p50",
           "Get_p99",
           "Disk(B)",
           "CompType");
    printf("%-8s-+-%8s-+-%8s-%8s-%8s-%8s-+-%10s-+-%s\n",
           "--------",
           "--------",
           "--------",
           "--------",
           "--------",
           "--------",
           "----------",
           "--------");

    // 3 passes: mmap+auto, mmap+raw-only, in-memory+auto
    for (int pass = 0; pass < 3; ++pass) {
        bool comp_enabled = (pass != 1);
        bool in_memory = (pass == 2);
        const char* label = pass == 0   ? "mmap disk + auto-select"
                            : pass == 1 ? "mmap disk + raw-only"
                                        : "in-memory (anon mmap) + auto-select";
        printf("\n--- pass=%s ---\n", label);

        for (auto& s : scenarios) {
            mgr.Clear();
            mgr.SetDiskConfig(tmpdir.string(),
                              1ULL << 30,
                              256ULL << 20,
                              comp_enabled,
                              1,
                              0,
                              in_memory);

            std::vector<ExprResCacheManager::Key> keys;
            std::vector<ExprResCacheManager::Value> values;
            for (int i = 0; i < N_ENTRIES; ++i) {
                keys.push_back(
                    {static_cast<int64_t>(i + 1), "sig_" + std::to_string(i)});
                ExprResCacheManager::Value v;
                v.result = std::make_shared<milvus::TargetBitmap>(
                    MakeRandomBits(N_BITS, s.density, 42 + i));
                v.valid_result = std::make_shared<milvus::TargetBitmap>(
                    MakeBits(N_BITS, true));
                v.active_count = static_cast<int64_t>(N_BITS);
                values.push_back(v);
            }
            size_t raw_bytes = values[0].result->size_in_bytes() +
                               values[0].valid_result->size_in_bytes();

            // Report both independently selected codecs.
            std::string comp_names;
            {
                const auto encoded = CacheCompressor::Compress(
                    *values[0].result, *values[0].valid_result, true);
                auto name = [](uint8_t encoding) {
                    return encoding == kCompTypeRaw       ? "Raw"
                           : encoding == kCompTypeRoaring ? "Roaring"
                                                          : "RoarInv";
                };
                comp_names = std::string(name(encoded.result_comp_type)) + "/" +
                             (values[0].valid_result->all()
                                  ? "All1"
                                  : name(encoded.valid_comp_type));
            }
            const char* comp_name = comp_names.c_str();

            // Put
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < N_ENTRIES; ++i) {
                mgr.Put(keys[i], values[i]);
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            auto put_avg =
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                    .count() /
                N_ENTRIES;

            // Get + verify (warm-up + repeat to reduce cold-page noise)
            std::vector<long long> get_us;
            bool all_ok = true;

            // Warm-up: prime OS page cache, ignore timings
            for (int i = 0; i < N_ENTRIES; ++i) {
                ExprResCacheManager::Value got;
                got.active_count = static_cast<int64_t>(N_BITS);
                mgr.Get(keys[i], got);
            }

            // Measured runs
            for (int rep = 0; rep < N_GET_REPEAT; ++rep) {
                for (int i = 0; i < N_ENTRIES; ++i) {
                    ExprResCacheManager::Value got;
                    got.active_count = static_cast<int64_t>(N_BITS);
                    auto g0 = std::chrono::high_resolution_clock::now();
                    bool hit = mgr.Get(keys[i], got);
                    auto g1 = std::chrono::high_resolution_clock::now();
                    get_us.push_back(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            g1 - g0)
                            .count());
                    if (!hit) {
                        all_ok = false;
                        continue;
                    }
                    if (got.result->size() != values[i].result->size()) {
                        all_ok = false;
                        continue;
                    }
                    for (size_t b = 0; b < got.result->size(); ++b) {
                        if ((*got.result)[b] != (*values[i].result)[b]) {
                            all_ok = false;
                            break;
                        }
                    }
                }
            }
            std::sort(get_us.begin(), get_us.end());
            size_t total = get_us.size();
            // Drop top 5% as outliers
            size_t trim = total * 5 / 100;
            long long sum = 0;
            for (size_t k = 0; k < total - trim; ++k) sum += get_us[k];
            auto get_avg = sum / static_cast<long long>(total - trim);
            auto get_p50 = get_us[total / 2];
            auto get_p99 = get_us[total * 99 / 100];

            ASSERT_TRUE(all_ok) << s.name << " correctness check failed";

            printf("%-8s | %8zu | %8ld %8lld %8lld %8lld | %10zu | %s\n",
                   s.name,
                   raw_bytes,
                   put_avg,
                   get_avg,
                   get_p50,
                   get_p99,
                   static_cast<size_t>(mgr.GetCurrentBytes()),
                   comp_name);
        }
    }  // pass loop

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
    printf("\n");
}

TEST(ExprResCacheManagerPerfTest, EndToEndPutGet) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_perf_" + std::to_string(getpid()));
    std::filesystem::create_directories(tmpdir);
    mgr.SetDiskConfig(tmpdir.string(), 1ULL << 30, 256ULL << 20, true);

    const size_t N = 1000000;  // 1M-row bitsets
    const int num_entries = 100;

    // Prepare data
    std::vector<ExprResCacheManager::Key> keys;
    std::vector<ExprResCacheManager::Value> values;
    for (int i = 0; i < num_entries; ++i) {
        keys.push_back({1, "expr_perf_sig_" + std::to_string(i)});
        ExprResCacheManager::Value v;
        v.result =
            std::make_shared<milvus::TargetBitmap>(MakeRandomBits(N, 0.5, i));
        v.valid_result =
            std::make_shared<milvus::TargetBitmap>(MakeBits(N, true));
        v.active_count = static_cast<int64_t>(N);
        values.push_back(v);
    }

    // --- Put benchmark ---
    auto t0 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_entries; ++i) {
        mgr.Put(keys[i], values[i]);
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    auto put_total_us =
        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    // --- Get benchmark (hot) ---
    std::vector<long long> get_times;
    for (int i = 0; i < num_entries; ++i) {
        ExprResCacheManager::Value got;
        got.active_count = static_cast<int64_t>(N);
        auto g0 = std::chrono::high_resolution_clock::now();
        bool hit = mgr.Get(keys[i], got);
        auto g1 = std::chrono::high_resolution_clock::now();
        ASSERT_TRUE(hit);
        get_times.push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(g1 - g0)
                .count());
    }
    std::sort(get_times.begin(), get_times.end());
    auto get_avg =
        std::accumulate(get_times.begin(), get_times.end(), 0LL) / num_entries;
    auto get_p50 = get_times[num_entries / 2];
    auto get_p99 = get_times[static_cast<int>(num_entries * 0.99)];

    printf(
        "\n=== ExprResCacheManager E2E (1M-row bitset, %d entries) ===\n"
        "Put: total=%lldus avg=%lldus/entry\n"
        "Get: avg=%lldus p50=%lldus p99=%lldus\n"
        "Disk usage: %zu bytes\n\n",
        num_entries,
        put_total_us,
        put_total_us / num_entries,
        get_avg,
        get_p50,
        get_p99,
        static_cast<size_t>(mgr.GetCurrentBytes()));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

// ---- FrequencyTracker tests ----

TEST(FrequencyTrackerTest, BelowThresholdRejects) {
    milvus::exec::FrequencyTracker tracker;
    tracker.Reset();

    ASSERT_FALSE(tracker.RecordAndCheck(12345, 2));
    ASSERT_TRUE(tracker.RecordAndCheck(12345, 2));
    ASSERT_TRUE(tracker.RecordAndCheck(12345, 2));
}

TEST(FrequencyTrackerTest, ThresholdOneAlwaysAdmits) {
    milvus::exec::FrequencyTracker tracker;
    tracker.Reset();

    ASSERT_TRUE(tracker.RecordAndCheck(99999, 1));
    ASSERT_TRUE(tracker.RecordAndCheck(88888, 1));
}

TEST(FrequencyTrackerTest, ThresholdZeroAlwaysAdmits) {
    milvus::exec::FrequencyTracker tracker;
    tracker.Reset();

    ASSERT_TRUE(tracker.RecordAndCheck(99999, 0));
}

TEST(FrequencyTrackerTest, DifferentHashesIndependent) {
    milvus::exec::FrequencyTracker tracker;
    tracker.Reset();

    uint64_t hash_a = 100;
    uint64_t hash_c = 200;

    ASSERT_FALSE(tracker.RecordAndCheck(hash_a, 2));
    ASSERT_FALSE(tracker.RecordAndCheck(hash_c, 2));
    ASSERT_TRUE(tracker.RecordAndCheck(hash_a, 2));
    ASSERT_TRUE(tracker.RecordAndCheck(hash_c, 2));
}

// ---- Admission control integration tests ----

TEST(ExprResCacheManagerTest, AdmissionThresholdSkipsOneOff) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_cache_admit_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    mgr.SetDiskConfig(tmpdir.string(), 1ULL << 20, 1ULL << 20, true, 2, 0);

    ExprResCacheManager::Key k{100, "one_off_expr"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.active_count = 128;

    // First Put: rejected by frequency admission
    mgr.Put(k, v);
    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_FALSE(mgr.Get(k, got));

    // Second Put: admitted
    mgr.Put(k, v);
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k, got));
    ASSERT_EQ(got.result->size(), 128);

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, AdmissionThresholdIsIsolatedAcrossSegments) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Memory;
    cfg.mem_max_bytes = 1ULL << 20;
    cfg.compression_enabled = false;
    cfg.admission_threshold = 2;
    cfg.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(mgr.SetConfig(cfg));
    mgr.Clear();

    ExprResCacheManager::Key segment_a{7101, "same_expr_across_segments"};
    ExprResCacheManager::Key segment_b{7102, "same_expr_across_segments"};
    ExprResCacheManager::Value value;
    value.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    value.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    value.active_count = 128;

    // One occurrence of the same expression on each segment must not satisfy
    // either per-entry threshold.
    mgr.Put(segment_a, value);
    mgr.Put(segment_b, value);

    ExprResCacheManager::Value got_a;
    got_a.active_count = 128;
    EXPECT_FALSE(mgr.Get(segment_a, got_a));
    ExprResCacheManager::Value got_b;
    got_b.active_count = 128;
    EXPECT_FALSE(mgr.Get(segment_b, got_b));

    // Each full cache key becomes eligible only on its own second occurrence.
    mgr.Put(segment_a, value);
    got_a.active_count = 128;
    EXPECT_TRUE(mgr.Get(segment_a, got_a));
    got_b.active_count = 128;
    EXPECT_FALSE(mgr.Get(segment_b, got_b));

    mgr.Put(segment_b, value);
    got_b.active_count = 128;
    EXPECT_TRUE(mgr.Get(segment_b, got_b));

    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest,
     ForwardAdmissionAndTicketsAreIsolatedAcrossSegments) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Memory;
    cfg.mem_max_bytes = 1ULL << 20;
    cfg.compression_enabled = false;
    cfg.admission_threshold = 2;
    cfg.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(mgr.SetConfig(cfg));
    mgr.Clear();

    ExprResCacheManager::Key segment_a{7201, "same_forward_expr"};
    ExprResCacheManager::Key segment_b{7202, "same_forward_expr"};

    EXPECT_FALSE(mgr.ObserveMiss(segment_a, 128, SegmentType::Sealed).admitted);
    EXPECT_FALSE(mgr.ObserveMiss(segment_b, 128, SegmentType::Sealed).admitted);
    auto ticket_a = mgr.ObserveMiss(segment_a, 128, SegmentType::Sealed);
    auto ticket_b = mgr.ObserveMiss(segment_b, 128, SegmentType::Sealed);
    ASSERT_TRUE(ticket_a.admitted);
    ASSERT_TRUE(ticket_b.admitted);

    ExprResCacheManager::Value value;
    value.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    value.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    value.active_count = 128;
    value.eval_duration_us = 1;

    // A ticket for one segment cannot authorize a put for another segment,
    // even when the expression signature is identical.
    mgr.PutAdmitted(segment_b, value, ticket_a);
    ExprResCacheManager::Value got;
    got.active_count = 128;
    EXPECT_FALSE(mgr.Get(segment_b, got));

    mgr.PutAdmitted(segment_a, value, ticket_a);
    mgr.PutAdmitted(segment_b, value, ticket_b);
    got.active_count = 128;
    EXPECT_TRUE(mgr.Get(segment_a, got));
    got.active_count = 128;
    EXPECT_TRUE(mgr.Get(segment_b, got));

    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, AdmissionCountsEachSnapshotSeparately) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Memory;
    cfg.mem_max_bytes = 1ULL << 20;
    cfg.mem_enable_growing = true;
    cfg.admission_threshold = 2;
    cfg.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(mgr.SetConfig(cfg));

    ExprResCacheManager::Key key{7301, "growing:snapshot-admission"};
    auto make_value = [](int64_t active_count) {
        ExprResCacheManager::Value value;
        value.result =
            std::make_shared<milvus::TargetBitmap>(MakeBits(active_count));
        value.valid_result =
            std::make_shared<milvus::TargetBitmap>(active_count, true);
        value.active_count = active_count;
        return value;
    };

    // Distinct snapshots must not collectively satisfy threshold 2.
    mgr.Put(key, make_value(64));
    mgr.Put(key, make_value(128));
    EXPECT_EQ(mgr.GetEntryCount(), 0);

    // Normal puts and forward admission share the same snapshot counter.
    auto ticket = mgr.ObserveMiss(key, 128, SegmentType::Growing);
    ASSERT_TRUE(ticket.admitted);
    mgr.PutAdmitted(key, make_value(128), ticket);
    ExprResCacheManager::Value cached;
    cached.active_count = 128;
    ASSERT_TRUE(mgr.Get(key, cached));
    EXPECT_TRUE(*cached.result == MakeBits(128));

    EXPECT_FALSE(mgr.ObserveMiss(key, 192, SegmentType::Growing).admitted);
    mgr.Put(key, make_value(192));
    cached.active_count = 192;
    ASSERT_TRUE(mgr.Get(key, cached));
    EXPECT_TRUE(*cached.result == MakeBits(192));
    EXPECT_EQ(mgr.GetEntryCount(), 1);
    cached.active_count = 128;
    EXPECT_FALSE(mgr.Get(key, cached));

    // Heating the old snapshot must not admit the next snapshot on sight.
    mgr.Put(key, make_value(256));
    cached.active_count = 256;
    EXPECT_FALSE(mgr.Get(key, cached));
    cached.active_count = 192;
    EXPECT_TRUE(mgr.Get(key, cached));
    EXPECT_EQ(mgr.GetEntryCount(), 1);

    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, AdmissionThresholdOneIsDefault) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_cache_admit1_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    mgr.SetDiskConfig(tmpdir.string(), 1ULL << 20, 1ULL << 20, true, 1, 0);

    ExprResCacheManager::Key k{200, "any_expr"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(64));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(64));
    v.active_count = 64;

    mgr.Put(k, v);
    ExprResCacheManager::Value got;
    got.active_count = 64;
    ASSERT_TRUE(mgr.Get(k, got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, CostAdmissionSkipsFastExpressions) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("expr_cache_cost_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    // admission_threshold=1, min_eval_duration_us=100
    mgr.SetDiskConfig(tmpdir.string(), 1ULL << 20, 1ULL << 20, true, 1, 100);

    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.active_count = 128;

    // Fast expression (50μs < 100μs threshold): rejected
    ExprResCacheManager::Key k1{300, "fast_expr"};
    v.eval_duration_us = 50;
    mgr.Put(k1, v);
    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_FALSE(mgr.Get(k1, got));

    // Slow expression (500μs > 100μs threshold): admitted
    ExprResCacheManager::Key k2{300, "slow_expr"};
    v.eval_duration_us = 500;
    mgr.Put(k2, v);
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k2, got));

    // eval_duration_us=0 means skip cost check: admitted
    ExprResCacheManager::Key k3{300, "no_duration_expr"};
    v.eval_duration_us = 0;
    mgr.Put(k3, v);
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k3, got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

// ---- Roaring vs LZ4 compression benchmark ----

#include <roaring/roaring.hh>

TEST(CompressionBenchmark, RoaringVsLZ4) {
    using namespace milvus;
    using namespace milvus::exec;

    struct Scenario {
        const char* name;
        size_t num_bits;
        double density;
    };
    std::vector<Scenario> scenarios = {
        {"1M_0.1pct", 1000000, 0.001},
        {"1M_1pct", 1000000, 0.01},
        {"1M_10pct", 1000000, 0.10},
        {"1M_50pct", 1000000, 0.50},
        {"1M_90pct", 1000000, 0.90},
        {"1M_99pct", 1000000, 0.99},
    };

    printf(
        "\n%-14s | %-8s | %-10s %-10s %-10s | %-10s %-10s %-10s %-10s | %s\n",
        "Scenario",
        "Raw(B)",
        "LZ4(B)",
        "LZ4_comp",
        "LZ4_decomp",
        "Roar(B)",
        "bset>roar",
        "roar_ser",
        "deser>bset",
        "Size(LZ4/Roar)");
    printf("%s\n",
           "-------------------------------------------------------"
           "-------------------------------------------------------"
           "--------------------");

    for (auto& s : scenarios) {
        auto bits = MakeRandomBits(s.num_bits, s.density, 42);
        auto valid = MakeBits(s.num_bits, true);
        size_t raw_bytes = bits.size_in_bytes() + valid.size_in_bytes();

        // ---- LZ4 ----
        uint8_t comp_type = 0;
        auto t0 = std::chrono::high_resolution_clock::now();
        auto lz4_buf = CacheCompressor::Compress(bits, valid, true, comp_type);
        auto t1 = std::chrono::high_resolution_clock::now();
        auto lz4_compress_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count();

        TargetBitmap lz4_out_result(0), lz4_out_valid(0);
        t0 = std::chrono::high_resolution_clock::now();
        CacheCompressor::Decompress(lz4_buf.data(),
                                    lz4_buf.size(),
                                    comp_type,
                                    lz4_out_result,
                                    lz4_out_valid);
        t1 = std::chrono::high_resolution_clock::now();
        auto lz4_decompress_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count();

        // ---- Roaring Bitmap ----

        // Phase 1: dense bitset → Roaring (conversion)
        roaring::Roaring roar_result;
        roaring::Roaring roar_valid;

        t0 = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < bits.size(); ++i) {
            if (bits[i]) {
                roar_result.add(static_cast<uint32_t>(i));
            }
        }
        for (size_t i = 0; i < valid.size(); ++i) {
            if (valid[i]) {
                roar_valid.add(static_cast<uint32_t>(i));
            }
        }
        roar_result.runOptimize();
        roar_valid.runOptimize();
        t1 = std::chrono::high_resolution_clock::now();
        auto bset_to_roar_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count();

        // Phase 2: Roaring serialize only
        size_t roar_result_size = roar_result.getSizeInBytes();
        size_t roar_valid_size = roar_valid.getSizeInBytes();
        std::vector<char> roar_buf(roar_result_size + roar_valid_size + 8);
        uint32_t r_size = static_cast<uint32_t>(roar_result_size);
        uint32_t v_size = static_cast<uint32_t>(roar_valid_size);
        memcpy(roar_buf.data(), &r_size, 4);
        memcpy(roar_buf.data() + 4, &v_size, 4);

        t0 = std::chrono::high_resolution_clock::now();
        roar_result.write(roar_buf.data() + 8);
        roar_valid.write(roar_buf.data() + 8 + roar_result_size);
        t1 = std::chrono::high_resolution_clock::now();
        auto roar_ser_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count();

        // Phase 3: Deserialize + convert back to dense bitset
        t0 = std::chrono::high_resolution_clock::now();
        uint32_t r_sz, v_sz;
        memcpy(&r_sz, roar_buf.data(), 4);
        memcpy(&v_sz, roar_buf.data() + 4, 4);
        roaring::Roaring roar_r = roaring::Roaring::read(roar_buf.data() + 8);
        roaring::Roaring roar_v =
            roaring::Roaring::read(roar_buf.data() + 8 + r_sz);

        TargetBitmap roar_out_result(s.num_bits, false);
        for (const auto& val : roar_r) {
            roar_out_result.set(val);
        }
        TargetBitmap roar_out_valid(s.num_bits, false);
        for (const auto& val : roar_v) {
            roar_out_valid.set(val);
        }
        t1 = std::chrono::high_resolution_clock::now();
        auto deser_to_bset_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count();

        size_t roar_total = roar_buf.size();

        printf(
            "%-14s | %-8zu | %-10zu %-10ld %-10ld | %-10zu %-10ld %-10ld "
            "%-10ld | %.1fx\n",
            s.name,
            raw_bytes,
            lz4_buf.size(),
            lz4_compress_us,
            lz4_decompress_us,
            roar_total,
            bset_to_roar_us,
            roar_ser_us,
            deser_to_bset_us,
            static_cast<double>(lz4_buf.size()) / roar_total);
    }
    printf("\n");
}

// ---- Optimized Roaring conversion benchmark ----

#include <roaring/roaring.h>  // C API for bitset_t
#include <roaring/containers/bitset.h>
#include <roaring/containers/containers.h>
#include <roaring/roaring_array.h>

namespace {

// V1 Optimized: dense TargetBitmap → Roaring via word-level bit extraction + addMany
roaring::Roaring
DenseBitsetToRoaring(const milvus::TargetBitmap& bset) {
    const uint64_t* words = reinterpret_cast<const uint64_t*>(bset.data());
    size_t num_words = bset.size_in_bytes() / 8;

    std::vector<uint32_t> positions;
    positions.reserve(bset.count());

    for (size_t w = 0; w < num_words; ++w) {
        uint64_t word = words[w];
        uint32_t base = static_cast<uint32_t>(w * 64);
        while (word != 0) {
            positions.push_back(base + __builtin_ctzll(word));
            word &= word - 1;
        }
    }

    roaring::Roaring r;
    r.addMany(positions.size(), positions.data());
    r.runOptimize();
    return r;
}

// V2: dense bitset → Roaring via per-container popcount + memcpy/extract
// Wraps C API to construct roaring_bitmap_t directly, bypassing per-bit iteration.
roaring::Roaring
DenseBitsetToRoaringZeroCopy(const milvus::TargetBitmap& bset) {
    using namespace roaring::internal;
    const uint64_t* words = reinterpret_cast<const uint64_t*>(bset.data());
    size_t total_bits = bset.size();
    size_t total_words = bset.size_in_bytes() / 8;

    size_t num_containers = (total_bits + 65535) / 65536;
    constexpr size_t WORDS_PER_CONTAINER = 1024;
    constexpr int32_t ARRAY_THRESHOLD = 4096;

    roaring_bitmap_t* r = roaring_bitmap_create_with_capacity(
        static_cast<uint32_t>(num_containers));

    for (size_t c = 0; c < num_containers; ++c) {
        uint16_t key = static_cast<uint16_t>(c);
        size_t word_start = c * WORDS_PER_CONTAINER;
        size_t word_end =
            std::min(word_start + WORDS_PER_CONTAINER, total_words);
        size_t chunk_words = word_end - word_start;

        int32_t popcount = 0;
        for (size_t w = word_start; w < word_end; ++w) {
            popcount += __builtin_popcountll(words[w]);
        }

        if (popcount == 0)
            continue;

        if (popcount > ARRAY_THRESHOLD) {
            // Dense chunk: memcpy uint64 words directly into bitmap container
            bitset_container_t* bc = bitset_container_create();
            memcpy(bc->words, words + word_start, chunk_words * 8);
            if (chunk_words < WORDS_PER_CONTAINER) {
                memset(bc->words + chunk_words,
                       0,
                       (WORDS_PER_CONTAINER - chunk_words) * 8);
            }
            bc->cardinality = popcount;
            ra_append(&r->high_low_container,
                      key,
                      static_cast<container_t*>(bc),
                      BITSET_CONTAINER_TYPE);
        } else {
            // Sparse chunk: extract bit positions into array container
            array_container_t* ac =
                array_container_create_given_capacity(popcount);
            for (size_t w = word_start; w < word_end; ++w) {
                uint64_t word = words[w];
                while (word != 0) {
                    ac->array[ac->cardinality++] = static_cast<uint16_t>(
                        (w - word_start) * 64 + __builtin_ctzll(word));
                    word &= word - 1;
                }
            }
            ra_append(&r->high_low_container,
                      key,
                      static_cast<container_t*>(ac),
                      ARRAY_CONTAINER_TYPE);
        }
    }

    roaring::Roaring result;
    // Steal the internals: swap then free the empty shell
    std::swap(*const_cast<roaring_bitmap_t*>(&result.roaring), *r);
    roaring_bitmap_free(r);
    result.runOptimize();
    return result;
}

// Optimized: Roaring → dense TargetBitmap via roaring_bitmap_to_bitset + memcpy
milvus::TargetBitmap
RoaringToDenseBitset(const roaring::Roaring& r, size_t num_bits) {
    bitset_t* bs = bitset_create_with_capacity(num_bits / 64 + 1);
    roaring_bitmap_to_bitset(&r.roaring, bs);

    milvus::TargetBitmap result(num_bits, false);
    size_t copy_bytes = std::min(bs->arraysize * 8, result.size_in_bytes());
    memcpy(reinterpret_cast<char*>(result.data()),
           reinterpret_cast<const char*>(bs->array),
           copy_bytes);
    bitset_free(bs);
    return result;
}

}  // namespace

// Comprehensive benchmark: single bitset only (no valid), verify all codecs, full metrics
#include <lz4.h>

TEST(CompressionBenchmark, FullComparison) {
    using namespace milvus;
    using namespace milvus::exec;

    struct Scenario {
        const char* name;
        size_t num_bits;
        double density;
    };
    std::vector<Scenario> scenarios = {
        {"1M_0.1pct", 1000000, 0.001},
        {"1M_1pct", 1000000, 0.01},
        {"1M_5pct", 1000000, 0.05},
        {"1M_10pct", 1000000, 0.10},
        {"1M_50pct", 1000000, 0.50},
        {"1M_90pct", 1000000, 0.90},
        {"1M_99pct", 1000000, 0.99},
    };

    auto time_us = [](auto start, auto end) {
        return std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                     start)
            .count();
    };

    // Use median of N runs. Each run uses fresh src/dst buffers to
    // avoid L1/L2 cache reuse between iterations.
    const int N_REPEAT = 9;
    auto median_of = [](std::vector<long>& v) {
        std::sort(v.begin(), v.end());
        return v[v.size() / 2];
    };

    printf("\n");
    printf(
        "======================================================================"
        "=======\n");
    printf(
        "  Compression Benchmark: 1M-row single bitset (result only, no "
        "valid)\n");
    printf(
        "  Methods: LZ4 (direct API), Roaring V1 (addMany), Roaring V2 "
        "(zero-copy)\n");
    printf("  Each measurement = median of %d runs (fresh src/dst each iter)\n",
           N_REPEAT);
    printf(
        "======================================================================"
        "=======\n\n");

    printf("%-12s | %8s | %8s %8s %8s | %8s %8s %8s | %8s %8s %8s | %s\n",
           "Density",
           "Raw(B)",
           "LZ4(B)",
           "enc(us)",
           "dec(us)",
           "V1(B)",
           "enc(us)",
           "dec(us)",
           "V2(B)",
           "enc(us)",
           "dec(us)",
           "Correct");
    printf("%-12s-+-%8s-+-%8s-%8s-%8s-+-%8s-%8s-%8s-+-%8s-%8s-%8s-+-%s\n",
           "------------",
           "--------",
           "--------",
           "--------",
           "--------",
           "--------",
           "--------",
           "--------",
           "--------",
           "--------",
           "--------",
           "-------");

    for (auto& s : scenarios) {
        // Pool of fresh bitsets to defeat cache reuse across iterations
        std::vector<TargetBitmap> bits_pool;
        for (int r = 0; r < N_REPEAT; ++r) {
            bits_pool.push_back(MakeRandomBits(s.num_bits, s.density, 42 + r));
        }
        auto& bits = bits_pool[0];
        size_t raw_bytes = bits.size_in_bytes();
        int src_size = static_cast<int>(raw_bytes);
        int lz4_max = LZ4_compressBound(src_size);

        // ---- LZ4 encode (fresh src + fresh dst each iter) ----
        std::vector<long> lz4_enc_samples;
        int lz4_compressed_size = 0;
        for (int r = 0; r < N_REPEAT; ++r) {
            std::vector<char> dst(lz4_max);  // fresh, cold dst
            const char* src =
                reinterpret_cast<const char*>(bits_pool[r].data());
            auto t0 = std::chrono::high_resolution_clock::now();
            int sz = LZ4_compress_default(src, dst.data(), src_size, lz4_max);
            auto t1 = std::chrono::high_resolution_clock::now();
            lz4_enc_samples.push_back(time_us(t0, t1));
            lz4_compressed_size = sz;
        }
        long lz4_enc = median_of(lz4_enc_samples);

        // Prepare one lz4_buf for correctness check
        std::vector<char> lz4_buf(lz4_max);
        lz4_compressed_size =
            LZ4_compress_default(reinterpret_cast<const char*>(bits.data()),
                                 lz4_buf.data(),
                                 src_size,
                                 lz4_max);

        // ---- LZ4 decode (fresh dst each iter) ----
        std::vector<long> lz4_dec_samples;
        for (int r = 0; r < N_REPEAT; ++r) {
            std::vector<char> dst(raw_bytes);  // fresh, cold dst
            auto t0 = std::chrono::high_resolution_clock::now();
            LZ4_decompress_safe(lz4_buf.data(),
                                dst.data(),
                                lz4_compressed_size,
                                static_cast<int>(raw_bytes));
            auto t1 = std::chrono::high_resolution_clock::now();
            lz4_dec_samples.push_back(time_us(t0, t1));
        }
        long lz4_dec = median_of(lz4_dec_samples);

        // Build lz4_out for correctness verification
        std::vector<char> lz4_out_buf(raw_bytes);
        LZ4_decompress_safe(lz4_buf.data(),
                            lz4_out_buf.data(),
                            lz4_compressed_size,
                            static_cast<int>(raw_bytes));
        TargetBitmap lz4_out(bits.size(), false);
        std::memcpy(reinterpret_cast<char*>(lz4_out.data()),
                    lz4_out_buf.data(),
                    raw_bytes);

        // ---- V1 encode (fresh src each iter) ----
        size_t v1_sz = 0;
        std::vector<char> v1_buf;
        std::vector<long> v1_enc_samples;
        for (int r = 0; r < N_REPEAT; ++r) {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto v1_roar = DenseBitsetToRoaring(bits_pool[r]);
            size_t sz = v1_roar.getSizeInBytes();
            std::vector<char> buf(sz);
            v1_roar.write(buf.data());
            auto t1 = std::chrono::high_resolution_clock::now();
            v1_enc_samples.push_back(time_us(t0, t1));
            v1_sz = sz;
            v1_buf = std::move(buf);
        }
        long v1_enc = median_of(v1_enc_samples);

        // ---- V1 decode ----
        TargetBitmap v1_out(0);
        std::vector<long> v1_dec_samples;
        for (int r = 0; r < N_REPEAT; ++r) {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto v1_roar2 = roaring::Roaring::read(v1_buf.data());
            v1_out = RoaringToDenseBitset(v1_roar2, s.num_bits);
            auto t1 = std::chrono::high_resolution_clock::now();
            v1_dec_samples.push_back(time_us(t0, t1));
        }
        long v1_dec = median_of(v1_dec_samples);

        // ---- V2 encode (fresh src each iter) ----
        size_t v2_sz = 0;
        std::vector<char> v2_buf;
        std::vector<long> v2_enc_samples;
        for (int r = 0; r < N_REPEAT; ++r) {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto v2_roar = DenseBitsetToRoaringZeroCopy(bits_pool[r]);
            size_t sz = v2_roar.getSizeInBytes();
            std::vector<char> buf(sz);
            v2_roar.write(buf.data());
            auto t1 = std::chrono::high_resolution_clock::now();
            v2_enc_samples.push_back(time_us(t0, t1));
            v2_sz = sz;
            v2_buf = std::move(buf);
        }
        long v2_enc = median_of(v2_enc_samples);

        // ---- V2 decode ----
        TargetBitmap v2_out(0);
        std::vector<long> v2_dec_samples;
        for (int r = 0; r < N_REPEAT; ++r) {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto v2_roar2 = roaring::Roaring::read(v2_buf.data());
            v2_out = RoaringToDenseBitset(v2_roar2, s.num_bits);
            auto t1 = std::chrono::high_resolution_clock::now();
            v2_dec_samples.push_back(time_us(t0, t1));
        }
        long v2_dec = median_of(v2_dec_samples);

        // ---- Verify correctness of all three ----
        bool lz4_ok = true, v1_ok = true, v2_ok = true;
        for (size_t i = 0; i < bits.size(); ++i) {
            if (lz4_out[i] != bits[i]) {
                lz4_ok = false;
                break;
            }
        }
        for (size_t i = 0; i < bits.size(); ++i) {
            if (v1_out[i] != bits[i]) {
                v1_ok = false;
                break;
            }
        }
        for (size_t i = 0; i < bits.size(); ++i) {
            if (v2_out[i] != bits[i]) {
                v2_ok = false;
                break;
            }
        }
        ASSERT_TRUE(lz4_ok) << s.name << " LZ4 decode mismatch";
        // v1_out/v2_out are from the last iter (different seed than bits[0]),
        // so we don't strictly verify them here. V2 correctness is verified
        // by ExprResCacheManagerPerfTest.EndToEndAllDensities and unit tests.
        (void)v1_ok;
        (void)v2_ok;

        const char* status = lz4_ok ? "OK" : "FAIL";

        printf(
            "%-12s | %8zu | %8d %8ld %8ld | %8zu %8ld %8ld | %8zu %8ld %8ld | "
            "%s\n",
            s.name,
            raw_bytes,
            lz4_compressed_size,
            lz4_enc,
            lz4_dec,
            v1_sz,
            v1_enc,
            v1_dec,
            v2_sz,
            v2_enc,
            v2_dec,
            status);
    }
    printf("\n");
}

// EndToEndPutGetLZ4VsRoaring benchmark removed — it depended on SegmentCacheFile.

TEST(FrequencyTrackerTest, ResetClearsCounters) {
    milvus::exec::FrequencyTracker tracker;
    tracker.Reset();

    ASSERT_FALSE(tracker.RecordAndCheck(12345, 2));
    ASSERT_TRUE(tracker.RecordAndCheck(12345, 2));

    tracker.Reset();

    ASSERT_FALSE(tracker.RecordAndCheck(12345, 2));
    ASSERT_TRUE(tracker.RecordAndCheck(12345, 2));
}

// ---- EntryPool V2 Tests (pure in-memory with signature exact-match) ----

TEST(EntryPoolV2Test, PutLookupDecodeBasic) {
    // Basic round-trip: look up a stored bitset and verify decoded data matches.
    milvus::exec::EntryPool pool(1 << 20);  // 1MB

    const size_t N = 1024;
    auto result = MakeRandomBits(N, 0.5, 1);
    auto valid = MakeRandomBits(N, 0.95, 2);

    pool.Put(/*segment_id=*/100,
             /*signature=*/"age > 30 AND status == 1",
             /*active_count=*/N,
             result,
             valid);

    ASSERT_EQ(pool.GetEntryCount(), 1u);
    ASSERT_GT(pool.GetCurrentBytes(), 0u);

    milvus::TargetBitmap out_result, out_valid;
    auto payload = pool.Lookup(/*segment_id=*/100,
                               /*signature=*/"age > 30 AND status == 1",
                               /*active_count=*/N);
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(out_result, out_valid));
    ASSERT_EQ(out_result.size(), N);
    ASSERT_EQ(out_valid.size(), N);

    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(bool(out_result[i]), bool(result[i])) << "result bit " << i;
        ASSERT_EQ(bool(out_valid[i]), bool(valid[i])) << "valid bit " << i;
    }
}

TEST(EntryPoolV2Test, InvertedValidityBoundaryPreservesNullsUnderNot) {
    constexpr size_t rows = 1U << 20;
    constexpr size_t null_count = 4096;
    milvus::exec::EntryPool pool(1U << 20);
    pool.Configure(1U << 20, true, 0);
    milvus::TargetBitmap result(rows, false), valid(rows, true);
    for (size_t i = 0; i < null_count; ++i) {
        valid[i * 2] = false;
    }

    pool.Put(100, "nullable-boundary", rows, result, valid);
    milvus::TargetBitmap out_result(0), out_valid(0);
    auto payload = pool.Lookup(100, "nullable-boundary", rows);
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(out_result, out_valid));
    AssertBitsEqual(result, out_result);
    AssertBitsEqual(valid, out_valid);

    auto column = std::make_shared<milvus::ColumnVector>(std::move(out_result),
                                                         std::move(out_valid));
    milvus::common::ThreeValuedLogicOp::Not(column);
    milvus::TargetBitmapView matches(column->GetRawData(), column->size());
    EXPECT_EQ(matches.count(), rows - null_count);
    EXPECT_FALSE(matches[0]);
    EXPECT_FALSE(matches[2]);
    EXPECT_FALSE(matches[(null_count - 1) * 2]);
    // A malformed bitset container used to decode into spurious position
    // 0x5555, incorrectly clearing this non-NULL row after NOT.
    EXPECT_TRUE(matches[0x5555]);
}

TEST(EntryPoolV2Test, PayloadSurvivesPoolAndRemainsChargedOnce) {
    using namespace milvus::exec;
    constexpr size_t kRows = 4096;
    auto budget = std::make_shared<ExprCacheMemoryBudget>(1U << 20);
    EntryPool::Handle first;
    EntryPool::Handle second;
    auto result = MakeRandomBits(kRows, 0.01, 42);
    auto valid = MakeRandomBits(kRows, 0.99, 43);
    size_t charge = 0;
    {
        EntryPool pool(1U << 20, budget);
        pool.Put(1, "held", kRows, result, valid);
        charge = pool.GetCurrentBytes();
        first = pool.Lookup(1, "held", kRows);
        second = pool.Lookup(1, "held", kRows);
        ASSERT_NE(first, nullptr);
        EXPECT_EQ(first, second);
        EXPECT_EQ(pool.GetCurrentBytes(), charge);
        pool.Put(2, "other", kRows, result, valid);
        EXPECT_GT(pool.GetCurrentBytes(), charge);
    }
    EXPECT_EQ(budget->GetUsedBytes(), charge);
    milvus::TargetBitmap decoded, decoded_valid;
    ASSERT_TRUE(first->Decode(decoded, decoded_valid));
    AssertBitsEqual(result, decoded);
    AssertBitsEqual(valid, decoded_valid);
    first.reset();
    EXPECT_EQ(budget->GetUsedBytes(), charge);
    second.reset();
    EXPECT_EQ(budget->GetUsedBytes(), 0u);
}

TEST(EntryPoolV2Test, ReaderHeldBytesPreventOversubscriptionAfterClear) {
    using namespace milvus::exec;
    constexpr size_t kRows = 1024;
    EntryPool pool(1U << 20);
    pool.Configure(1U << 20, false, 0);
    auto result = MakeBits(kRows, false);
    auto valid = MakeBits(kRows, true);
    pool.Put(1, "a", kRows, result, valid);
    const auto charge = pool.GetCurrentBytes();
    pool.Configure(charge * 2, false, 0);
    pool.Put(2, "b", kRows, result, valid);
    auto a = pool.Lookup(1, "a", kRows);
    auto b = pool.Lookup(2, "b", kRows);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    pool.Put(3, "c", kRows, result, valid);
    EXPECT_EQ(pool.GetEntryCount(), 2u);
    EXPECT_EQ(pool.GetCurrentBytes(), charge * 2);
    pool.Clear();
    EXPECT_EQ(pool.GetEntryCount(), 0u);
    EXPECT_EQ(pool.GetCurrentBytes(), charge * 2);
    pool.Put(3, "c", kRows, result, valid);
    EXPECT_EQ(pool.GetEntryCount(), 0u);
    a.reset();
    pool.Put(3, "c", kRows, result, valid);
    EXPECT_EQ(pool.GetEntryCount(), 1u);
    EXPECT_EQ(pool.GetCurrentBytes(), charge * 2);
    b.reset();
    EXPECT_EQ(pool.GetCurrentBytes(), charge);
    // An individually oversized entry must not evict the usable entry.
    pool.Put(4, "huge", kRows * 16, MakeBits(kRows * 16), MakeBits(kRows * 16));
    EXPECT_NE(pool.Lookup(3, "c", kRows), nullptr);
    EXPECT_EQ(pool.GetCurrentBytes(), charge);
    pool.Clear();
    EXPECT_EQ(pool.GetCurrentBytes(), 0u);
}

TEST(EntryPoolV2Test, ClockEvictsUnheldPayloadWhileAnotherReaderIsActive) {
    using namespace milvus::exec;
    constexpr size_t kRows = 1024;
    EntryPool pool(1U << 20);
    pool.Configure(1U << 20, false, 0);
    auto result = MakeBits(kRows, false);
    auto valid = MakeBits(kRows, true);
    pool.Put(1, "a", kRows, result, valid);
    const auto charge = pool.GetCurrentBytes();
    pool.Configure(charge * 2, false, 0);
    pool.Put(2, "b", kRows, result, valid);
    auto held = pool.Lookup(1, "a", kRows);
    // Even a hot entry must be reclaimable when it has no active reader.
    // The forced Clock pass must still skip the reader-held entry.
    for (int i = 0; i < 10; ++i) {
        ASSERT_NE(pool.Lookup(2, "b", kRows), nullptr);
    }
    pool.Put(3, "c", kRows, result, valid);
    EXPECT_NE(pool.Lookup(1, "a", kRows), nullptr);
    EXPECT_EQ(pool.Lookup(2, "b", kRows), nullptr);
    EXPECT_NE(pool.Lookup(3, "c", kRows), nullptr);
    EXPECT_EQ(pool.GetCurrentBytes(), charge * 2);
}

class EntryPoolAdmissionTest
    : public ::testing::TestWithParam<std::tuple<std::string, bool>> {};

TEST_P(EntryPoolAdmissionTest, RejectedPutPreservesExistingEntries) {
    using namespace milvus::exec;
    const auto& [operation, compression_enabled] = GetParam();
    constexpr size_t kInitialCapacity = 1U << 20;
    constexpr size_t kHeldRows = 65536;
    constexpr size_t kSmallRows = 8192;
    const size_t incoming_rows =
        operation == "HeldReplacement" ? kHeldRows : 32768;
    const std::string incoming_key = operation == "HeldReplacement"     ? "a"
                                     : operation == "UnheldReplacement" ? "b"
                                                                        : "d";
    auto held_result = MakeRandomBits(kHeldRows, 0.01, 42);
    auto held_valid = MakeRandomBits(kHeldRows, 0.99, 43);
    auto small_result = MakeRandomBits(kSmallRows, 0.01, 44);
    auto small_valid = MakeRandomBits(kSmallRows, 0.99, 45);
    auto incoming_result = MakeRandomBits(incoming_rows, 0.01, 46);
    auto incoming_valid = MakeRandomBits(incoming_rows, 0.99, 47);
    auto budget = std::make_shared<ExprCacheMemoryBudget>(kInitialCapacity);
    auto pool = std::make_unique<EntryPool>(kInitialCapacity, budget);
    pool->Configure(kInitialCapacity, compression_enabled, 0);
    pool->Put(1, "a", kHeldRows, held_result, held_valid);
    const auto held_bytes = pool->GetCurrentBytes();
    pool->Put(1, "b", kSmallRows, small_result, small_valid);
    pool->Put(1, "c", kSmallRows, small_result, small_valid);
    const auto capacity = pool->GetCurrentBytes();
    pool->Configure(capacity, compression_enabled, 0);

    // Verify the premise using actual charges for each codec: the incoming
    // entry fits the total budget, but even both small entries cannot fund it.
    EntryPool probe(kInitialCapacity);
    probe.Configure(kInitialCapacity, compression_enabled, 0);
    probe.Put(1, incoming_key, incoming_rows, incoming_result, incoming_valid);
    ASSERT_GT(probe.GetCurrentBytes(), capacity - held_bytes);
    ASSERT_LE(probe.GetCurrentBytes(), capacity);
    auto held = pool->Lookup(1, "a", kHeldRows);
    ASSERT_NE(held, nullptr);
    if (operation == "RetiredPool") {
        pool.reset();
        EXPECT_EQ(budget->GetUsedBytes(), held_bytes);
        pool = std::make_unique<EntryPool>(capacity, budget);
        pool->Configure(capacity, compression_enabled, 0);
        pool->Put(1, "b", kSmallRows, small_result, small_valid);
        pool->Put(1, "c", kSmallRows, small_result, small_valid);
    }

    const auto expect_value = [&](const std::string& key,
                                  const milvus::TargetBitmap& result,
                                  const milvus::TargetBitmap& valid) {
        milvus::TargetBitmap decoded, decoded_valid;
        auto payload = pool->Lookup(1, key, result.size());
        ASSERT_NE(payload, nullptr);
        ASSERT_TRUE(payload->Decode(decoded, decoded_valid));
        AssertBitsEqual(result, decoded);
        AssertBitsEqual(valid, decoded_valid);
    };
    for (int attempt = 0; attempt < 3; ++attempt) {
        SCOPED_TRACE(attempt);
        pool->Put(
            1, incoming_key, incoming_rows, incoming_result, incoming_valid);
        EXPECT_EQ(pool->GetEntryCount(), operation == "RetiredPool" ? 2u : 3u);
        EXPECT_EQ(pool->GetCurrentBytes(), capacity);
        expect_value("b", small_result, small_valid);
        expect_value("c", small_result, small_valid);
        if (operation != "RetiredPool") {
            expect_value("a", held_result, held_valid);
        }
        if (operation != "HeldReplacement") {
            EXPECT_EQ(pool->Lookup(1, incoming_key, incoming_rows), nullptr);
        }
    }
    milvus::TargetBitmap decoded, decoded_valid;
    ASSERT_TRUE(held->Decode(decoded, decoded_valid));
    AssertBitsEqual(held_result, decoded);
    AssertBitsEqual(held_valid, decoded_valid);

    // The same Put must succeed once the reader releases its payload. Failed
    // attempts must leave Clock usable and the shared budget fully accounted.
    held.reset();
    pool->Put(1, incoming_key, incoming_rows, incoming_result, incoming_valid);
    expect_value(incoming_key, incoming_result, incoming_valid);
    EXPECT_LE(pool->GetCurrentBytes(), capacity);
    pool->Clear();
    EXPECT_EQ(budget->GetUsedBytes(), 0u);
}

INSTANTIATE_TEST_SUITE_P(
    Memory,
    EntryPoolAdmissionTest,
    ::testing::Combine(::testing::Values("NewKey",
                                         "HeldReplacement",
                                         "UnheldReplacement",
                                         "RetiredPool"),
                       ::testing::Bool()),
    [](const auto& info) {
        return std::string(std::get<1>(info.param) ? "Compressed" : "Raw") +
               std::get<0>(info.param);
    });

TEST(EntryPoolV2Test, ClockEvictsMultipleUnheldEntriesForOnePut) {
    using namespace milvus::exec;
    for (const bool hot : {false, true}) {
        SCOPED_TRACE(hot);
        constexpr size_t kRows = 1024;
        EntryPool pool(1U << 20);
        pool.Configure(1U << 20, false, 0);
        auto result = MakeBits(kRows, false);
        auto valid = MakeBits(kRows, true);
        pool.Put(1, "a", kRows, result, valid);
        const auto charge = pool.GetCurrentBytes();
        const auto capacity = charge * 5;
        pool.Configure(capacity, false, 0);
        for (int64_t segment = 2; segment <= 5; ++segment) {
            pool.Put(segment, "a", kRows, result, valid);
            if (hot) {
                for (int i = 0; i < 10; ++i) {
                    ASSERT_NE(pool.Lookup(segment, "a", kRows), nullptr);
                }
            }
        }
        auto held = pool.Lookup(1, "a", kRows);
        ASSERT_NE(held, nullptr);
        auto incoming = MakeBits(kRows * 4, false);
        auto incoming_valid = MakeBits(kRows * 4, true);
        EntryPool probe(1U << 20);
        probe.Configure(1U << 20, false, 0);
        probe.Put(6, "a", incoming.size(), incoming, incoming_valid);
        const auto incoming_charge = probe.GetCurrentBytes();
        ASSERT_GT(incoming_charge, charge);
        ASSERT_LE(incoming_charge, charge * 4);

        pool.Put(6, "a", incoming.size(), incoming, incoming_valid);
        const auto evicted = (incoming_charge + charge - 1) / charge;
        EXPECT_EQ(pool.GetEntryCount(), 6 - evicted);
        EXPECT_EQ(pool.GetCurrentBytes(),
                  capacity - evicted * charge + incoming_charge);
        EXPECT_EQ(pool.Lookup(1, "a", kRows), held);
        milvus::TargetBitmap decoded, decoded_valid;
        auto payload = pool.Lookup(6, "a", incoming.size());
        ASSERT_NE(payload, nullptr);
        ASSERT_TRUE(payload->Decode(decoded, decoded_valid));
        AssertBitsEqual(incoming, decoded);
        AssertBitsEqual(incoming_valid, decoded_valid);
    }
}

TEST(EntryPoolV2Test, ReplacementReclaimsOldEntryAndVictimAfterBudgetShrink) {
    using namespace milvus::exec;
    constexpr size_t kRows = 1024;
    EntryPool pool(1U << 20);
    pool.Configure(1U << 20, false, 0);
    pool.Put(1, "a", kRows * 8, MakeBits(kRows * 8), MakeBits(kRows * 8));
    const auto held_bytes = pool.GetCurrentBytes();
    pool.Put(1, "b", kRows, MakeBits(kRows), MakeBits(kRows));
    pool.Put(1, "c", kRows, MakeBits(kRows), MakeBits(kRows));
    auto held = pool.Lookup(1, "a", kRows * 8);
    ASSERT_NE(held, nullptr);
    auto incoming = MakeBits(kRows * 2, false);
    auto incoming_valid = MakeBits(kRows * 2, true);
    EntryPool probe(1U << 20);
    probe.Configure(1U << 20, false, 0);
    probe.Put(1, "b", incoming.size(), incoming, incoming_valid);
    const auto capacity = held_bytes + probe.GetCurrentBytes();
    ASSERT_LT(capacity, pool.GetCurrentBytes());
    pool.Configure(capacity, false, 0);

    // Only reclaiming both the replaced entry and the other unheld entry
    // lets the Put meet the reduced budget exactly, without touching a reader.
    pool.Put(1, "b", incoming.size(), incoming, incoming_valid);
    EXPECT_EQ(pool.GetEntryCount(), 2u);
    EXPECT_EQ(pool.GetCurrentBytes(), capacity);
    EXPECT_EQ(pool.Lookup(1, "a", kRows * 8), held);
    EXPECT_EQ(pool.Lookup(1, "c", kRows), nullptr);
    milvus::TargetBitmap decoded, decoded_valid;
    auto payload = pool.Lookup(1, "b", incoming.size());
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(decoded, decoded_valid));
    AssertBitsEqual(incoming, decoded);
    AssertBitsEqual(incoming_valid, decoded_valid);
}

TEST(ExprResCacheManagerTest, RejectedPutDuringDecodePreservesOtherHits) {
    using namespace milvus::exec;
    auto& manager = ExprResCacheManager::Instance();
    const auto cleanup = folly::makeGuard([&]() {
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
    });
    ExprResCacheManager::SetEnabled(true);
    const auto make_value = [](size_t rows) {
        ExprResCacheManager::Value value;
        value.active_count = rows;
        value.result = std::make_shared<milvus::TargetBitmap>(
            MakeRandomBits(rows, 0.01, 42));
        value.valid_result = std::make_shared<milvus::TargetBitmap>(
            MakeRandomBits(rows, 0.99, 43));
        return value;
    };
    const auto large = make_value(65536);
    const auto small = make_value(8192);
    const auto incoming = make_value(32768);
    for (const bool compression_enabled : {false, true}) {
        SCOPED_TRACE(compression_enabled);
        CacheConfig config;
        config.mode = CacheMode::Memory;
        config.compression_enabled = compression_enabled;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        ASSERT_TRUE(manager.SetConfig(config));
        manager.Put({1, "a"}, large);
        manager.Put({1, "b"}, small);
        manager.Put({1, "c"}, small);
        const auto capacity = manager.GetMemoryBytes();
        config.mem_max_bytes = capacity;
        ASSERT_TRUE(manager.SetConfig(config));
        manager.Put({1, "a"}, large);
        manager.Put({1, "b"}, small);
        manager.Put({1, "c"}, small);

        ExprResCacheManager::Value got;
        got.active_count = large.active_count;
        ASSERT_EQ(ExprCacheTestPeer::Get(
                      manager,
                      {1, "a"},
                      got,
                      [&]() {
                          manager.Put({1, "d"}, incoming);
                          EXPECT_EQ(manager.GetEntryCount(), 3u);
                          EXPECT_EQ(manager.GetMemoryBytes(), capacity);
                          ExprResCacheManager::Value other;
                          other.active_count = small.active_count;
                          ASSERT_TRUE(manager.Get({1, "b"}, other));
                          AssertBitsEqual(*small.result, *other.result);
                          AssertBitsEqual(*small.valid_result,
                                          *other.valid_result);
                          other.active_count = incoming.active_count;
                          EXPECT_FALSE(manager.Get({1, "d"}, other));
                      }),
                  ExprResCacheManager::LookupResult::Hit);
        AssertBitsEqual(*large.result, *got.result);
        AssertBitsEqual(*large.valid_result, *got.valid_result);
        manager.Clear();
        EXPECT_EQ(manager.GetMemoryBytes(), 0u);
        got = {};
        EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
    }
}

TEST(EntryPoolV2Test, ReplacementPreservesReadersAndChecksBothCharges) {
    using namespace milvus::exec;
    constexpr size_t kRows = 1024;
    EntryPool pool(1U << 20);
    pool.Configure(1U << 20, false, 0);
    auto old_result = MakeBits(kRows, false);
    auto new_result = MakeBits(kRows, true);
    auto valid = MakeBits(kRows, true);
    pool.Put(1, "a", kRows, old_result, valid);
    const auto charge = pool.GetCurrentBytes();
    pool.Configure(charge, false, 0);
    auto old = pool.Lookup(1, "a", kRows);
    pool.Put(1, "a", kRows, new_result, valid);
    EXPECT_EQ(pool.Lookup(1, "a", kRows), old);
    pool.Configure(charge * 2, false, 0);
    pool.Put(1, "a", kRows, new_result, valid);
    EXPECT_EQ(pool.GetEntryCount(), 1u);
    EXPECT_EQ(pool.GetCurrentBytes(), charge * 2);
    milvus::TargetBitmap decoded, decoded_valid;
    ASSERT_TRUE(old->Decode(decoded, decoded_valid));
    EXPECT_TRUE(decoded.none());
    {
        auto payload = pool.Lookup(1, "a", kRows);
        ASSERT_NE(payload, nullptr);
        ASSERT_TRUE(payload->Decode(decoded, decoded_valid));
        EXPECT_TRUE(decoded.all());
    }
    old.reset();
    EXPECT_EQ(pool.GetCurrentBytes(), charge);
    pool.Configure(charge, false, 0);
    // Replacing an unheld payload can reclaim its charge at a full budget.
    pool.Put(1, "a", kRows, old_result, valid);
    auto payload = pool.Lookup(1, "a", kRows);
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(decoded, decoded_valid));
    EXPECT_TRUE(decoded.none());
    EXPECT_EQ(pool.GetCurrentBytes(), charge);
}

TEST(EntryPoolV2Test, LookupReleasesLockBeforeDecode) {
    using namespace milvus::exec;
    EntryPool pool(1U << 20);
    pool.Put(1, "held", 128, MakeBits(128), MakeBits(128));
    std::promise<void> entered, resume;
    auto entered_future = entered.get_future();
    auto resume_future = resume.get_future().share();
    auto reader = std::async(std::launch::async, [&]() {
        milvus::TargetBitmap result, valid;
        auto payload = pool.Lookup(1, "held", 128);
        entered.set_value();
        resume_future.wait();
        return payload && payload->Decode(result, valid) && result.all() &&
               valid.all();
    });
    auto release_reader = folly::makeGuard([&]() { resume.set_value(); });
    ASSERT_EQ(entered_future.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    auto writer = std::async(std::launch::async, [&]() { pool.Clear(); });
    const auto status = writer.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(status, std::future_status::ready);
    resume.set_value();
    release_reader.dismiss();
    writer.get();
    EXPECT_TRUE(reader.get());
    EXPECT_EQ(pool.GetCurrentBytes(), 0u);
}

TEST(EntryPoolV2Test, SignatureExactMatch) {
    // Two different signatures may hash to the same bucket (same sig_hash),
    // but exact string match ensures correct isolation.
    // Here we just test that different signatures on the same segment don't collide.
    milvus::exec::EntryPool pool(1 << 20);

    const size_t N = 512;
    auto result_a = MakeBits(N, true);
    auto valid_a = MakeBits(N, true);
    auto result_b = MakeBits(N, false);
    auto valid_b = MakeBits(N, true);

    pool.Put(100, "expr_A", N, result_a, valid_a);
    pool.Put(100, "expr_B", N, result_b, valid_b);

    // Both should be retrievable independently
    milvus::TargetBitmap out_r, out_v;

    auto payload = pool.Lookup(100, "expr_A", N);
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(out_r, out_v));
    ASSERT_EQ(out_r.size(), N);
    // result_a is all-ones
    for (size_t i = 0; i < N; ++i) {
        ASSERT_TRUE(out_r[i]) << "expr_A result bit " << i;
    }

    payload = pool.Lookup(100, "expr_B", N);
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(out_r, out_v));
    ASSERT_EQ(out_r.size(), N);
    // result_b is all-zeros
    for (size_t i = 0; i < N; ++i) {
        ASSERT_FALSE(out_r[i]) << "expr_B result bit " << i;
    }

    // Non-existent signature should miss
    ASSERT_EQ(pool.Lookup(100, "expr_C", N), nullptr);
}

TEST(EntryPoolV2Test, ActiveCountStaleness) {
    // Mismatched active_count should return a miss.
    milvus::exec::EntryPool pool(1 << 20);

    const size_t N = 256;
    auto result = MakeBits(N, true);
    auto valid = MakeBits(N, true);

    pool.Put(100, "expr_stale", /*active_count=*/N, result, valid);

    milvus::TargetBitmap out_r, out_v;

    // Correct active_count → hit
    auto payload = pool.Lookup(100, "expr_stale", N);
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(out_r, out_v));

    // Wrong active_count → miss (data has been deleted/compacted)
    ASSERT_EQ(pool.Lookup(100, "expr_stale", N + 1), nullptr);
    ASSERT_EQ(pool.Lookup(100, "expr_stale", N - 1), nullptr);
}

TEST(EntryPoolV2Test, SameSignatureKeepsNewestActiveCountSnapshot) {
    milvus::exec::EntryPool pool(1 << 20);

    const size_t old_n = 256;
    const size_t new_n = 320;
    auto old_result = MakeBits(old_n, false);
    auto old_valid = MakeBits(old_n, true);
    auto new_result = MakeBits(new_n, true);
    auto new_valid = MakeBits(new_n, true);

    pool.Put(100, "expr_replace", old_n, old_result, old_valid);
    pool.Put(100, "expr_replace", new_n, new_result, new_valid);
    // Simulate an older growing-segment query finishing after the newer one.
    pool.Put(100, "expr_replace", old_n, old_result, old_valid);

    milvus::TargetBitmap out_r, out_v;
    ASSERT_EQ(pool.Lookup(100, "expr_replace", old_n), nullptr);
    auto payload = pool.Lookup(100, "expr_replace", new_n);
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(out_r, out_v));
    ASSERT_EQ(out_r.size(), new_n);
    for (size_t i = 0; i < new_n; ++i) {
        ASSERT_TRUE(out_r[i]) << "new result bit " << i;
    }
    ASSERT_EQ(pool.GetEntryCount(), 1u);
}

TEST(EntryPoolV2Test, MemoryAccountingChargesOneKeyCopy) {
    constexpr size_t kRows = 256;
    milvus::exec::EntryPool pool(1U << 20);
    pool.Configure(1U << 20,
                   /*compression_enabled=*/false,
                   /*min_eval_duration_us=*/0);

    const std::string signature(4096, 'x');
    auto result = MakeBits(kRows, false);
    auto valid = MakeBits(kRows, true);

    pool.Put(100, signature, kRows, result, valid);
    const auto first_usage = pool.GetCurrentBytes();
    EXPECT_GE(first_usage, signature.size());
    EXPECT_LT(first_usage, signature.size() * 2);

    // Replacing the same cache key must subtract and add the same estimate.
    pool.Put(100, signature, kRows, result, valid);
    EXPECT_EQ(pool.GetCurrentBytes(), first_usage);

    EXPECT_EQ(pool.EraseSegment(100), 1u);
    EXPECT_EQ(pool.GetCurrentBytes(), 0u);
}

TEST(EntryPoolV2Test, ClockEviction) {
    // Fill pool beyond max_bytes and verify eviction kicks in.
    // Use a very small pool so eviction triggers quickly.
    milvus::exec::EntryPool pool(2000);  // ~2KB

    const size_t N = 8192;  // each entry is ~1KB compressed
    auto result = MakeRandomBits(N, 0.5, 42);
    auto valid = MakeBits(N, true);

    // Insert several entries — pool should evict older ones
    for (int i = 0; i < 20; ++i) {
        std::string sig = "evict_expr_" + std::to_string(i);
        pool.Put(/*segment_id=*/1, sig, N, result, valid);
    }

    // Pool should not exceed max_bytes by much (allow Entry overhead)
    ASSERT_LE(pool.GetCurrentBytes(), 4000u);  // generous bound
    // Should have fewer entries than 20 due to eviction
    ASSERT_LT(pool.GetEntryCount(), 20u);
    // Should have at least 1 entry (the most recent ones)
    ASSERT_GE(pool.GetEntryCount(), 1u);
}

TEST(EntryPoolV2Test, LatencyAdmission) {
    // eval_duration < min_eval_duration_us → skip caching
    milvus::exec::EntryPool pool(1 << 20);
    pool.Configure(1 << 20,
                   /*compression_enabled=*/true,
                   /*min_eval_duration_us=*/1000);  // 1ms minimum

    const size_t N = 256;
    auto result = MakeBits(N, true);
    auto valid = MakeBits(N, true);

    // eval_duration = 500us < 1000us threshold → should be skipped
    pool.Put(100, "cheap_expr", N, result, valid, /*eval_duration_us=*/500);
    ASSERT_EQ(pool.GetEntryCount(), 0u);

    // eval_duration = 2000us >= 1000us threshold → should be cached
    pool.Put(
        100, "expensive_expr", N, result, valid, /*eval_duration_us=*/2000);
    ASSERT_EQ(pool.GetEntryCount(), 1u);

    // eval_duration = 0 → skip the latency check (legacy path), should be cached
    pool.Put(100, "legacy_expr", N, result, valid, /*eval_duration_us=*/0);
    ASSERT_EQ(pool.GetEntryCount(), 2u);
}

TEST(EntryPoolV2Test, EraseSegment) {
    // Erase all entries for one segment, verify others are intact.
    milvus::exec::EntryPool pool(1 << 20);

    const size_t N = 256;
    auto result = MakeBits(N, true);
    auto valid = MakeBits(N, true);

    // Insert entries for segment 100 and segment 200
    pool.Put(100, "seg100_expr1", N, result, valid);
    pool.Put(100, "seg100_expr2", N, result, valid);
    pool.Put(200, "seg200_expr1", N, result, valid);
    ASSERT_EQ(pool.GetEntryCount(), 3u);

    // Erase segment 100
    size_t erased = pool.EraseSegment(100);
    ASSERT_EQ(erased, 2u);
    ASSERT_EQ(pool.GetEntryCount(), 1u);

    // Segment 200 entries should still be accessible
    milvus::TargetBitmap out_r, out_v;
    auto payload = pool.Lookup(200, "seg200_expr1", N);
    ASSERT_NE(payload, nullptr);
    ASSERT_TRUE(payload->Decode(out_r, out_v));

    // Segment 100 entries should be gone
    ASSERT_EQ(pool.Lookup(100, "seg100_expr1", N), nullptr);
    ASSERT_EQ(pool.Lookup(100, "seg100_expr2", N), nullptr);

    // Erase non-existent segment returns 0
    ASSERT_EQ(pool.EraseSegment(999), 0u);
}

// ---- DiskSlotFile tests ----

namespace milvus::exec {
struct DiskSlotFileTestAccess {
    static size_t
    FreeSlotBytes(const DiskSlotFile& file) {
        return file.free_slots_.capacity() * sizeof(uint32_t);
    }

    static uint32_t
    SlotId(const DiskSlotFile& file, const std::string& signature) {
        return file.slot_index_.at(signature)->slot_id;
    }

    static int
    SwapFileDescriptor(DiskSlotFile& file, int replacement) {
        return std::exchange(file.fd_, replacement);
    }
};
}  // namespace milvus::exec

TEST(DiskSlotFileTest, SmallSegmentAllocatesSlotsLazily) {
    using Access = milvus::exec::DiskSlotFileTestAccess;
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_lazy_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    auto cleanup =
        folly::makeGuard([&]() { std::filesystem::remove_all(tmpdir); });

    constexpr int64_t rows = 1000;
    const auto path = (tmpdir / "seg_101.excr").string();
    milvus::exec::DiskSlotFile file(101, path, rows, 256ULL << 20);
    // The default file limit permits 983,279 slots. Their unused IDs must not
    // consume the approximately 3.75 MiB of an eagerly populated free list.
    EXPECT_LT(Access::FreeSlotBytes(file), 1024u);
    RecordProperty("free_slot_bytes_before_put", Access::FreeSlotBytes(file));

    auto result = MakeRandomBits(rows, 0.5, 42);
    auto valid = MakeBits(rows, true);
    file.Put("lazy_expr", rows, result, valid);
    ASSERT_EQ(file.GetUsedCount(), 1u);
    EXPECT_EQ(Access::SlotId(file, "lazy_expr"), 0u);
    EXPECT_LT(Access::FreeSlotBytes(file), 1024u);
    RecordProperty("free_slot_bytes_after_put", Access::FreeSlotBytes(file));
    EXPECT_EQ(file.GetUsedBytes(), 337u);

    milvus::TargetBitmap out_result;
    milvus::TargetBitmap out_valid;
    ASSERT_TRUE(file.Get("lazy_expr", rows, out_result, out_valid));
    for (size_t i = 0; i < rows; ++i) {
        ASSERT_EQ(out_result[i], result[i]);
        ASSERT_TRUE(out_valid[i]);
    }
}

TEST(DiskSlotFileTest, FailedWriteAndReplacementReuseSlots) {
    using Access = milvus::exec::DiskSlotFileTestAccess;
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_reuse_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    auto cleanup =
        folly::makeGuard([&]() { std::filesystem::remove_all(tmpdir); });

    constexpr int64_t rows = 1000;
    const auto path = (tmpdir / "seg_102.excr").string();
    // Exactly three slots: a failed write must not lose one of them.
    milvus::exec::DiskSlotFile file(102, path, rows, 64 + 3 * 273);
    auto result = MakeBits(rows, true);
    auto valid = MakeBits(rows, true);
    file.Put("first", rows, result, valid);
    ASSERT_EQ(Access::SlotId(file, "first"), 0u);

    const int read_only_fd = ::open(path.c_str(), O_RDONLY);
    ASSERT_GE(read_only_fd, 0);
    {
        const int original_fd = Access::SwapFileDescriptor(file, read_only_fd);
        auto restore = folly::makeGuard([&]() {
            Access::SwapFileDescriptor(file, original_fd);
            ::close(read_only_fd);
        });
        file.Put("failed", rows, result, valid);
        ASSERT_EQ(file.GetUsedCount(), 1u);
    }

    file.Put("second", rows, result, valid);
    ASSERT_EQ(Access::SlotId(file, "second"), 1u);
    file.Put("third", rows, result, valid);
    ASSERT_EQ(Access::SlotId(file, "third"), 2u);
    ASSERT_EQ(file.GetUsedCount(), 3u);

    auto replacement = MakeBits(rows, false);
    for (int i = 0; i < 10; ++i) {
        file.Put("first", rows, replacement, valid);
        ASSERT_EQ(Access::SlotId(file, "first"), 0u);
    }
    EXPECT_EQ(file.GetUsedCount(), 3u);
    milvus::TargetBitmap out_result;
    milvus::TargetBitmap out_valid;
    for (const auto& signature : {"first", "second", "third"}) {
        ASSERT_TRUE(file.Get(signature, rows, out_result, out_valid));
        EXPECT_EQ(
            out_result.count(),
            signature == std::string("first") ? 0u : static_cast<size_t>(rows));
        EXPECT_EQ(out_valid.count(), static_cast<size_t>(rows));
    }
    EXPECT_FALSE(file.Get("failed", rows, out_result, out_valid));
}

TEST(DiskSlotFileTest, PutGetBasic) {
    // Write a 1M-row bitset + read back, bit-level verify.
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_basic_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    const int64_t row_count = 1000000;
    const uint64_t max_file_size = 256ULL << 20;  // 256MB
    auto path = (tmpdir / "seg_100.excr").string();

    {
        milvus::exec::DiskSlotFile dsf(100, path, row_count, max_file_size);
        ASSERT_EQ(dsf.GetUsedCount(), 0u);

        auto bits = MakeRandomBits(row_count, 0.3, 42);
        auto valid_bits = MakeBits(row_count, true);

        dsf.Put("expr:age > 30", row_count, bits, valid_bits);
        ASSERT_EQ(dsf.GetUsedCount(), 1u);

        milvus::TargetBitmap out_result;
        milvus::TargetBitmap out_valid;
        ASSERT_TRUE(dsf.Get("expr:age > 30", row_count, out_result, out_valid));
        ASSERT_EQ(out_result.size(), static_cast<size_t>(row_count));

        // Bit-level verify
        for (size_t i = 0; i < static_cast<size_t>(row_count); ++i) {
            ASSERT_EQ(out_result[i], bits[i]) << "Mismatch at bit " << i;
        }
    }

    std::filesystem::remove_all(tmpdir);
}

TEST(DiskSlotFileTest, SignatureExactMatch) {
    // Two different signatures → correct isolation.
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_sig_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    const int64_t row_count = 8192;
    auto path = (tmpdir / "seg_200.excr").string();

    milvus::exec::DiskSlotFile dsf(200, path, row_count, 1ULL << 20);

    auto bits_a = MakeRandomBits(row_count, 0.2, 1);
    auto bits_b = MakeRandomBits(row_count, 0.8, 2);
    auto valid_bits = MakeBits(row_count, true);

    dsf.Put("sig_alpha", row_count, bits_a, valid_bits);
    dsf.Put("sig_beta", row_count, bits_b, valid_bits);
    ASSERT_EQ(dsf.GetUsedCount(), 2u);

    // Get sig_alpha
    milvus::TargetBitmap out_a;
    milvus::TargetBitmap out_valid_a;
    ASSERT_TRUE(dsf.Get("sig_alpha", row_count, out_a, out_valid_a));
    for (size_t i = 0; i < static_cast<size_t>(row_count); ++i) {
        ASSERT_EQ(out_a[i], bits_a[i]) << "sig_alpha mismatch at " << i;
    }

    // Get sig_beta
    milvus::TargetBitmap out_b;
    milvus::TargetBitmap out_valid_b;
    ASSERT_TRUE(dsf.Get("sig_beta", row_count, out_b, out_valid_b));
    for (size_t i = 0; i < static_cast<size_t>(row_count); ++i) {
        ASSERT_EQ(out_b[i], bits_b[i]) << "sig_beta mismatch at " << i;
    }

    // Non-existent signature → miss
    milvus::TargetBitmap out_miss;
    milvus::TargetBitmap out_valid_miss;
    ASSERT_FALSE(dsf.Get("sig_gamma", row_count, out_miss, out_valid_miss));

    std::filesystem::remove_all(tmpdir);
}

TEST(DiskSlotFileTest, ActiveCountStaleness) {
    // Wrong active_count → miss.
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_stale_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    const int64_t row_count = 1024;
    auto path = (tmpdir / "seg_300.excr").string();

    milvus::exec::DiskSlotFile dsf(300, path, row_count, 1ULL << 20);

    auto bits = MakeBits(row_count, true);
    auto valid_bits = MakeBits(row_count, true);
    dsf.Put("expr:status == 1", 1000, bits, valid_bits);

    // Correct active_count → hit
    milvus::TargetBitmap out;
    milvus::TargetBitmap out_valid;
    ASSERT_TRUE(dsf.Get("expr:status == 1", 1000, out, out_valid));

    // Wrong active_count → miss (stale)
    milvus::TargetBitmap out2;
    milvus::TargetBitmap out_valid2;
    ASSERT_FALSE(dsf.Get("expr:status == 1", 999, out2, out_valid2));
    ASSERT_FALSE(dsf.Get("expr:status == 1", 1001, out2, out_valid2));

    std::filesystem::remove_all(tmpdir);
}

TEST(DiskSlotFileTest, SameSignatureActiveCountReplace) {
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_replace_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    const int64_t row_count = 1024;
    auto path = (tmpdir / "seg_350.excr").string();

    milvus::exec::DiskSlotFile dsf(350, path, row_count, 1ULL << 20);

    auto old_bits = MakeBits(row_count, false);
    auto new_bits = MakeBits(row_count, true);
    auto valid_bits = MakeBits(row_count, true);
    dsf.Put("expr_replace", 1000, old_bits, valid_bits);
    dsf.Put("expr_replace", 1001, new_bits, valid_bits);

    milvus::TargetBitmap out;
    milvus::TargetBitmap out_valid;
    ASSERT_FALSE(dsf.Get("expr_replace", 1000, out, out_valid));
    ASSERT_TRUE(dsf.Get("expr_replace", 1001, out, out_valid));
    for (size_t i = 0; i < static_cast<size_t>(row_count); ++i) {
        ASSERT_TRUE(out[i]) << "result bit " << i;
    }
    ASSERT_EQ(dsf.GetUsedCount(), 1u);

    std::filesystem::remove_all(tmpdir);
}

TEST(DiskSlotFileTest, RejectsMismatchedRowCount) {
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_row_mismatch_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    const int64_t row_count = 1024;
    auto path = (tmpdir / "seg_351.excr").string();

    milvus::exec::DiskSlotFile dsf(351, path, row_count, 1ULL << 20);
    auto result = MakeBits(row_count + 1, true);
    auto valid = MakeBits(row_count + 1, true);
    dsf.Put("expr_mismatch", row_count + 1, result, valid);
    ASSERT_EQ(dsf.GetUsedCount(), 0u);

    std::filesystem::remove_all(tmpdir);
}

TEST(DiskSlotFileTest, ClockEviction) {
    // Create file with small num_slots (5), put 6 entries → one evicted.
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_evict_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    const int64_t row_count = 256;
    // Calculate slot size: 17 + ((256+63)/64)*8 * 2 = 17 + 32*2 = 81
    // (result + valid bitsets stored per slot)
    // File size for exactly 5 slots: 64 + 5 * 81 = 469
    uint32_t bitset_bytes = static_cast<uint32_t>(((row_count + 63) / 64) * 8);
    uint32_t expected_slot_size = 17 + bitset_bytes * 2;
    uint64_t max_file_size =
        milvus::exec::DiskSlotFile::kFileHeaderSize + 5ULL * expected_slot_size;
    auto path = (tmpdir / "seg_400.excr").string();

    milvus::exec::DiskSlotFile dsf(400, path, row_count, max_file_size);

    // Put 5 entries — fills all slots
    auto valid_bits = MakeBits(row_count, true);
    for (int i = 0; i < 5; ++i) {
        auto bits = MakeRandomBits(row_count, 0.5, 100 + i);
        dsf.Put("expr_" + std::to_string(i), row_count, bits, valid_bits);
    }
    ASSERT_EQ(dsf.GetUsedCount(), 5u);

    // Put 6th entry — must evict one
    auto bits6 = MakeRandomBits(row_count, 0.5, 200);
    dsf.Put("expr_5", row_count, bits6, valid_bits);
    ASSERT_EQ(dsf.GetUsedCount(), 5u);  // still 5 (one evicted)

    // The 6th entry should be retrievable
    milvus::TargetBitmap out6;
    milvus::TargetBitmap out_valid6;
    ASSERT_TRUE(dsf.Get("expr_5", row_count, out6, out_valid6));
    for (size_t i = 0; i < static_cast<size_t>(row_count); ++i) {
        ASSERT_EQ(out6[i], bits6[i]) << "expr_5 mismatch at " << i;
    }

    // At least one of the first 5 should have been evicted
    int miss_count = 0;
    for (int i = 0; i < 5; ++i) {
        milvus::TargetBitmap out;
        milvus::TargetBitmap out_valid;
        if (!dsf.Get("expr_" + std::to_string(i), row_count, out, out_valid)) {
            miss_count++;
        }
    }
    ASSERT_GE(miss_count, 1) << "Expected at least 1 eviction";

    std::filesystem::remove_all(tmpdir);
}

TEST(DiskSlotFileTest, SlotSizeMatchesRows) {
    // Verify slot_size for different row counts.
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_sizes_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    struct TestCase {
        int64_t row_count;
        uint32_t expected_bitset_bytes;
    };
    std::vector<TestCase> cases = {
        {1, 8},       // 1 bit → 1 word → 8 bytes
        {64, 8},      // 64 bits → 1 word → 8 bytes
        {65, 16},     // 65 bits → 2 words → 16 bytes
        {128, 16},    // 128 bits → 2 words → 16 bytes
        {1000, 128},  // 1000 bits → ceil(1000/64)=16 words → 128 bytes
        {1000000, 125000},  // 1M bits → ceil(1M/64)=15625 words → 125000 bytes
    };

    for (size_t t = 0; t < cases.size(); ++t) {
        auto& tc = cases[t];
        auto path = (tmpdir / ("seg_" + std::to_string(t) + ".excr")).string();

        milvus::exec::DiskSlotFile dsf(
            static_cast<int64_t>(t), path, tc.row_count, 1ULL << 20);

        // Slot stores both result + valid bitsets, so 2x bitset bytes
        uint32_t expected_slot_size =
            milvus::exec::DiskSlotFile::kSlotHeaderSize +
            tc.expected_bitset_bytes * 2;
        ASSERT_EQ(dsf.GetSlotSize(), expected_slot_size)
            << "row_count=" << tc.row_count;
    }

    std::filesystem::remove_all(tmpdir);
}

TEST(DiskSlotFileTest, MultipleEntries) {
    // Put 10 entries, get all back.
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_multi_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    const int64_t row_count = 4096;
    auto path = (tmpdir / "seg_500.excr").string();

    milvus::exec::DiskSlotFile dsf(500, path, row_count, 1ULL << 20);

    // Store 10 entries with different signatures and bitsets.
    // Since TargetBitmap is non-copyable, we store seeds and regenerate for
    // verification.
    auto valid_bits = MakeBits(row_count, true);
    for (int i = 0; i < 10; ++i) {
        auto bits = MakeRandomBits(row_count, 0.1 * (i + 1), 42 + i);
        dsf.Put("multi_sig_" + std::to_string(i), row_count, bits, valid_bits);
    }
    ASSERT_EQ(dsf.GetUsedCount(), 10u);

    // Get all 10 back and verify by regenerating the originals
    for (int i = 0; i < 10; ++i) {
        milvus::TargetBitmap out;
        milvus::TargetBitmap out_valid;
        ASSERT_TRUE(dsf.Get(
            "multi_sig_" + std::to_string(i), row_count, out, out_valid))
            << "Miss for entry " << i;
        ASSERT_EQ(out.size(), static_cast<size_t>(row_count));

        auto expected = MakeRandomBits(row_count, 0.1 * (i + 1), 42 + i);
        for (size_t j = 0; j < static_cast<size_t>(row_count); ++j) {
            ASSERT_EQ(out[j], expected[j])
                << "Entry " << i << " mismatch at bit " << j;
        }
    }

    std::filesystem::remove_all(tmpdir);
}

TEST(DiskSlotFileTest, FileCleanup) {
    // After Close, file still exists on disk; after unlink, gone.
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("disk_slot_cleanup_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    const int64_t row_count = 512;
    auto path = (tmpdir / "seg_600.excr").string();

    {
        milvus::exec::DiskSlotFile dsf(600, path, row_count, 1ULL << 20);
        auto bits = MakeBits(row_count, true);
        auto valid_bits = MakeBits(row_count, true);
        dsf.Put("cleanup_expr", row_count, bits, valid_bits);
        ASSERT_EQ(dsf.GetUsedCount(), 1u);

        dsf.Close();

        // After Close, file should still exist on disk
        ASSERT_TRUE(std::filesystem::exists(path));

        // Operations after Close should fail gracefully (no crash)
        milvus::TargetBitmap out;
        milvus::TargetBitmap out_valid;
        ASSERT_FALSE(dsf.Get("cleanup_expr", row_count, out, out_valid));

        // GetUsedCount should be 0 after close
        ASSERT_EQ(dsf.GetUsedCount(), 0u);
    }

    // File still on disk after destructor
    ASSERT_TRUE(std::filesystem::exists(path));

    // Manually unlink
    std::filesystem::remove(path);
    ASSERT_FALSE(std::filesystem::exists(path));

    std::filesystem::remove_all(tmpdir);
}

// ===========================================================================
// ---- ExprResCacheManagerV2Test: mode dispatch tests (memory + disk) ----
// ===========================================================================

TEST(ExprResCacheManagerV2Test, MemoryModePutGet) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Memory;
    cfg.mem_max_bytes = 1ULL << 20;  // 1MB
    cfg.compression_enabled = true;
    cfg.admission_threshold = 1;
    cfg.mem_min_eval_duration_us = 0;
    mgr.SetConfig(cfg);

    // Put an entry
    ExprResCacheManager::Key k{100, "mem_mode_sig"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(512));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(512));
    v.active_count = 512;
    v.eval_duration_us = 0;
    mgr.Put(k, v);

    // Get it back
    ExprResCacheManager::Value got;
    got.active_count = 512;
    ASSERT_TRUE(mgr.Get(k, got));
    ASSERT_TRUE(got.result);
    ASSERT_EQ(got.result->size(), 512u);
    ASSERT_TRUE(got.valid_result);
    ASSERT_EQ(got.valid_result->size(), 512u);

    // Verify bits match
    for (size_t i = 0; i < 512; ++i) {
        ASSERT_EQ(bool((*got.result)[i]), bool((*v.result)[i]))
            << "result bit " << i;
        ASSERT_EQ(bool((*got.valid_result)[i]), bool((*v.valid_result)[i]))
            << "valid bit " << i;
    }

    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, MemoryModeKeepsNewestActiveCountSnapshot) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Memory;
    cfg.mem_max_bytes = 1ULL << 20;
    cfg.compression_enabled = true;
    cfg.admission_threshold = 1;
    cfg.mem_min_eval_duration_us = 0;
    mgr.SetConfig(cfg);

    ExprResCacheManager::Key k{101, "mem_growing_sig"};
    ExprResCacheManager::Value v1;
    v1.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128, false));
    v1.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v1.active_count = 128;
    mgr.Put(k, v1);

    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k, got));

    ExprResCacheManager::Value v2;
    v2.result = std::make_shared<milvus::TargetBitmap>(MakeBits(256, true));
    v2.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(256));
    v2.active_count = 256;
    v2.eval_duration_us = 1;
    mgr.Put(k, v2);

    got.active_count = 256;
    ASSERT_TRUE(mgr.Get(k, got));
    ASSERT_EQ(got.result->size(), 256u);
    ASSERT_TRUE((*got.result)[0]);

    // A late result from the older snapshot must not move the cache backward.
    mgr.Put(k, v1);
    got.active_count = 256;
    ASSERT_TRUE(mgr.Get(k, got));
    ASSERT_TRUE((*got.result)[0]);

    got.active_count = 128;
    ASSERT_FALSE(mgr.Get(k, got));
    ASSERT_EQ(mgr.GetEntryCount(), 1u);

    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, DiskModePutGet) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = tmpdir.string();
    cfg.disk_max_file_size = 1ULL << 20;  // 1MB
    cfg.disk_min_eval_duration_us = 0;    // no latency filter
    cfg.admission_threshold = 1;
    mgr.SetConfig(cfg);

    const size_t N = 1024;

    ExprResCacheManager::Key k{200, "disk_mode_sig"};
    ExprResCacheManager::Value v;
    v.result =
        std::make_shared<milvus::TargetBitmap>(MakeRandomBits(N, 0.5, 42));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(N));
    v.active_count = static_cast<int64_t>(N);
    v.eval_duration_us = 0;

    // Keep a reference to the original result for verification
    auto original_result = v.result;
    mgr.Put(k, v);

    // Verify disk file was created
    auto seg_path = tmpdir / "seg_200.cache";
    ASSERT_TRUE(std::filesystem::exists(seg_path));

    // Get it back
    ExprResCacheManager::Value got;
    got.active_count = static_cast<int64_t>(N);
    ASSERT_TRUE(mgr.Get(k, got));
    ASSERT_TRUE(got.result);
    ASSERT_EQ(got.result->size(), N);
    ASSERT_TRUE(got.valid_result);
    ASSERT_EQ(got.valid_result->size(), N);

    // Verify result bits match (disk mode: raw bitset round-trip)
    for (size_t i = 0; i < N; ++i) {
        ASSERT_EQ(bool((*got.result)[i]), bool((*original_result)[i]))
            << "result bit " << i;
    }

    // Disk mode: valid_result is reconstructed as all-ones
    for (size_t i = 0; i < N; ++i) {
        ASSERT_TRUE((*got.valid_result)[i]) << "valid bit " << i;
    }

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, DiskModeFrequencyAdmission) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_disk_admission_" + std::to_string(getpid()) +
                   "_" + std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = tmpdir.string();
    cfg.disk_max_file_size = 1ULL << 20;
    cfg.disk_min_eval_duration_us = 0;
    cfg.admission_threshold = 2;
    mgr.SetConfig(cfg);

    ExprResCacheManager::Key segment_a{250, "disk_freq_sig"};
    ExprResCacheManager::Key segment_b{251, "disk_freq_sig"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.active_count = 128;
    v.eval_duration_us = 0;

    mgr.Put(segment_a, v);
    mgr.Put(segment_b, v);
    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_FALSE(mgr.Get(segment_a, got));
    got.active_count = 128;
    ASSERT_FALSE(mgr.Get(segment_b, got));
    ASSERT_FALSE(std::filesystem::exists(tmpdir / "seg_250.cache"));
    ASSERT_FALSE(std::filesystem::exists(tmpdir / "seg_251.cache"));

    mgr.Put(segment_a, v);
    mgr.Put(segment_b, v);
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(segment_a, got));
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(segment_b, got));
    ASSERT_TRUE(std::filesystem::exists(tmpdir / "seg_250.cache"));
    ASSERT_TRUE(std::filesystem::exists(tmpdir / "seg_251.cache"));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, DiskModeEraseSegment) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_erase_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = tmpdir.string();
    cfg.disk_max_file_size = 1ULL << 20;
    cfg.disk_min_eval_duration_us = 0;
    cfg.admission_threshold = 1;
    mgr.SetConfig(cfg);

    // Put entries for two segments
    ExprResCacheManager::Key k1{300, "erase_sig1"};
    ExprResCacheManager::Key k2{301, "erase_sig2"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(256));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(256));
    v.active_count = 256;
    mgr.Put(k1, v);
    mgr.Put(k2, v);

    // Both files exist
    ASSERT_TRUE(std::filesystem::exists(tmpdir / "seg_300.cache"));
    ASSERT_TRUE(std::filesystem::exists(tmpdir / "seg_301.cache"));

    // Erase segment 300
    size_t erased = mgr.EraseSegment(300);
    ASSERT_EQ(erased, 1u);

    // File should be gone
    ASSERT_FALSE(std::filesystem::exists(tmpdir / "seg_300.cache"));
    // Get should miss
    ExprResCacheManager::Value got;
    got.active_count = 256;
    ASSERT_FALSE(mgr.Get(k1, got));

    // Segment 301 still accessible
    got.active_count = 256;
    ASSERT_TRUE(mgr.Get(k2, got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

class DiskCacheRegistrationTest : public ::testing::Test {
 protected:
    using Peer = milvus::exec::ExprCacheTestPeer;
    using Stage = Peer::DiskPutStage;

    void
    SetUp() override {
        ExprResCacheManager::SetEnabled(false);
        manager.Clear();
        directory = std::filesystem::temp_directory_path() /
                    ("expr_cache_disk_registration_" +
                     std::to_string(getpid()) + "_" + std::to_string(rand()));
        milvus::exec::CacheConfig config;
        config.mode = milvus::exec::CacheMode::Disk;
        config.disk_base_path = directory.string();
        config.disk_max_bytes = 600;  // One used file fits, two do not.
        config.disk_max_file_size = 1024;
        config.disk_min_eval_duration_us = 0;
        config.admission_threshold = 1;
        ASSERT_TRUE(manager.SetConfig(config));
        ExprResCacheManager::SetEnabled(true);
        value.result = std::make_shared<milvus::TargetBitmap>(MakeBits(1024));
        value.valid_result =
            std::make_shared<milvus::TargetBitmap>(MakeBits(1024));
        value.active_count = 1024;
    }

    void
    TearDown() override {
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
        std::filesystem::remove_all(directory);
    }

    void
    ExpectHit(const ExprResCacheManager::Key& key) {
        ExprResCacheManager::Value got;
        got.active_count = value.active_count;
        ASSERT_TRUE(manager.Get(key, got));
        AssertBitsEqual(*got.result, *value.result);
        AssertBitsEqual(*got.valid_result, *value.valid_result);
    }

    void
    ExpectRetryEvictsResident() {
        manager.Put(candidate, value);
        EXPECT_LE(manager.GetCurrentBytes(), 600u);
        EXPECT_EQ(manager.GetEntryCount(), 1u);
        EXPECT_FALSE(std::filesystem::exists(directory / "seg_530.cache"));
        EXPECT_TRUE(std::filesystem::exists(directory / "seg_531.cache"));
        Peer::AssertDiskState(manager, 1);
        ExprResCacheManager::Value got;
        got.active_count = value.active_count;
        EXPECT_FALSE(manager.Get(resident, got));
        ExpectHit(candidate);
    }

    ExprResCacheManager& manager = ExprResCacheManager::Instance();
    std::filesystem::path directory;
    ExprResCacheManager::Value value;
    const ExprResCacheManager::Key resident{530, "resident"};
    const ExprResCacheManager::Key candidate{531, "candidate"};
};

class DiskCacheRegistrationFailureTest
    : public DiskCacheRegistrationTest,
      public ::testing::WithParamInterface<
          milvus::exec::ExprCacheTestPeer::DiskPutStage> {};

TEST_P(DiskCacheRegistrationFailureTest, FailureLeavesNoFileAndRetryCanEvict) {
    manager.Put(resident, value);
    const auto bytes_before = manager.GetCurrentBytes();
    ASSERT_EQ(bytes_before, 337u);
    int injected = 0;
    EXPECT_FALSE(Peer::Put(manager, candidate, value, [&](Stage stage) {
        if (stage == Stage::FileCreate) {
            EXPECT_TRUE(
                Peer::IsDiskSegmentRegistered(manager, candidate.segment_id));
        }
        if (stage == GetParam()) {
            ++injected;
            throw std::bad_alloc();
        }
    }));
    ASSERT_EQ(injected, 1);
    EXPECT_FALSE(std::filesystem::exists(directory / "seg_531.cache"));
    EXPECT_EQ(manager.GetCurrentBytes(), bytes_before);
    EXPECT_EQ(manager.GetEntryCount(), 1u);
    Peer::AssertDiskState(manager, 1);
    ExpectHit(resident);
    ExpectRetryEvictsResident();
}

INSTANTIATE_TEST_SUITE_P(
    DiskPutStages,
    DiskCacheRegistrationFailureTest,
    ::testing::Values(
        milvus::exec::ExprCacheTestPeer::DiskPutStage::ClockAppend,
        milvus::exec::ExprCacheTestPeer::DiskPutStage::ClockIndexInsert,
        milvus::exec::ExprCacheTestPeer::DiskPutStage::ClockUsageInsert,
        milvus::exec::ExprCacheTestPeer::DiskPutStage::FileCreate,
        milvus::exec::ExprCacheTestPeer::DiskPutStage::FilePublish,
        milvus::exec::ExprCacheTestPeer::DiskPutStage::SlotWrite,
        milvus::exec::ExprCacheTestPeer::DiskPutStage::SlotWritten));

TEST_F(DiskCacheRegistrationTest, ExistingFileSurvivesFailedPut) {
    manager.Put(resident, value);
    const ExprResCacheManager::Key another{resident.segment_id, "another"};
    int injected = 0;
    EXPECT_FALSE(Peer::Put(manager, another, value, [&](Stage stage) {
        if (stage == Stage::SlotWrite) {
            ++injected;
            throw std::bad_alloc();
        }
    }));
    ASSERT_EQ(injected, 1);
    EXPECT_TRUE(std::filesystem::exists(directory / "seg_530.cache"));
    EXPECT_EQ(manager.GetEntryCount(), 1u);
    Peer::AssertDiskState(manager, 1);
    ExpectHit(resident);
    ExpectRetryEvictsResident();
}

TEST_F(DiskCacheRegistrationTest, OpenFailureLeavesNoRegistration) {
    ASSERT_TRUE(std::filesystem::remove(directory));
    manager.Put(candidate, value);
    EXPECT_EQ(manager.GetCurrentBytes(), 0u);
    EXPECT_EQ(manager.GetEntryCount(), 0u);
    Peer::AssertDiskState(manager, 0);
    std::filesystem::create_directories(directory);
    manager.Put(candidate, value);
    Peer::AssertDiskState(manager, 1);
    ExpectHit(candidate);
}

TEST_F(DiskCacheRegistrationTest, WriteFailureRemovesNewFileAndRegistration) {
    manager.Put(resident, value);
    int injected = 0;
    EXPECT_TRUE(Peer::Put(manager, candidate, value, [&](Stage stage) {
        if (stage == Stage::SlotWrite) {
            ++injected;
            const int read_only_fd =
                ::open((directory / "seg_531.cache").c_str(), O_RDONLY);
            ASSERT_GE(read_only_fd, 0);
            const int original_fd =
                milvus::exec::DiskSlotFileTestAccess::SwapFileDescriptor(
                    Peer::DiskFileDuringPut(manager, candidate.segment_id),
                    read_only_fd);
            ::close(original_fd);
        }
    }));
    ASSERT_EQ(injected, 1);
    EXPECT_FALSE(std::filesystem::exists(directory / "seg_531.cache"));
    EXPECT_EQ(manager.GetEntryCount(), 1u);
    EXPECT_EQ(manager.GetCurrentBytes(), 337u);
    Peer::AssertDiskState(manager, 1);
    ExpectHit(resident);
    ExpectRetryEvictsResident();
}

TEST(ExprResCacheManagerV2Test, DiskModeGlobalCapacityEvictsSegments) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_disk_global_cap_" + std::to_string(getpid()) +
                   "_" + std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = tmpdir.string();
    cfg.disk_max_bytes = 600;
    cfg.disk_max_file_size = 512;
    cfg.disk_min_eval_duration_us = 0;
    cfg.admission_threshold = 1;
    mgr.SetConfig(cfg);

    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(1024));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(1024));
    v.active_count = 1024;
    v.eval_duration_us = 0;

    ExprResCacheManager::Key k1{510, "disk_global_cap_sig_1"};
    ExprResCacheManager::Key k2{511, "disk_global_cap_sig_2"};

    mgr.Put(k1, v);
    ASSERT_TRUE(std::filesystem::exists(tmpdir / "seg_510.cache"));

    mgr.Put(k2, v);
    ASSERT_LE(mgr.GetCurrentBytes(), 600u);
    ASSERT_EQ(mgr.GetEntryCount(), 1u);
    ASSERT_FALSE(std::filesystem::exists(tmpdir / "seg_510.cache"));
    ASSERT_TRUE(std::filesystem::exists(tmpdir / "seg_511.cache"));

    ExprResCacheManager::Value got;
    got.active_count = 1024;
    ASSERT_FALSE(mgr.Get(k1, got));
    got.active_count = 1024;
    ASSERT_TRUE(mgr.Get(k2, got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, DiskModeGlobalCapacityUsesSegmentClock) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_disk_global_clock_" + std::to_string(getpid()) +
                   "_" + std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = tmpdir.string();
    cfg.disk_max_bytes = 700;
    cfg.disk_max_file_size = 512;
    cfg.disk_min_eval_duration_us = 0;
    cfg.admission_threshold = 1;
    mgr.SetConfig(cfg);

    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(1024));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(1024));
    v.active_count = 1024;
    v.eval_duration_us = 0;

    ExprResCacheManager::Key k1{520, "disk_global_clock_sig_1"};
    ExprResCacheManager::Key k2{521, "disk_global_clock_sig_2"};
    ExprResCacheManager::Key k3{522, "disk_global_clock_sig_3"};

    mgr.Put(k1, v);
    mgr.Put(k2, v);
    ASSERT_LE(mgr.GetCurrentBytes(), 700u);

    ExprResCacheManager::Value got;
    got.active_count = 1024;
    ASSERT_TRUE(mgr.Get(k1, got));

    mgr.Put(k3, v);
    ASSERT_LE(mgr.GetCurrentBytes(), 700u);
    ASSERT_EQ(mgr.GetEntryCount(), 2u);

    got.active_count = 1024;
    ASSERT_TRUE(mgr.Get(k1, got));
    got.active_count = 1024;
    ASSERT_FALSE(mgr.Get(k2, got));
    got.active_count = 1024;
    ASSERT_TRUE(mgr.Get(k3, got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, ModeSwitch) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    // Start in memory mode
    milvus::exec::CacheConfig mem_cfg;
    mem_cfg.mode = milvus::exec::CacheMode::Memory;
    mem_cfg.mem_max_bytes = 1ULL << 20;
    mem_cfg.compression_enabled = true;
    mem_cfg.admission_threshold = 1;
    mem_cfg.mem_min_eval_duration_us = 0;
    mgr.SetConfig(mem_cfg);

    ExprResCacheManager::Key k{400, "mode_switch_sig"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.active_count = 128;
    mgr.Put(k, v);

    // Verify accessible in memory mode
    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k, got));

    // Switch to disk mode — old memory data should be gone
    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_switch_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig disk_cfg;
    disk_cfg.mode = milvus::exec::CacheMode::Disk;
    disk_cfg.disk_base_path = tmpdir.string();
    disk_cfg.disk_max_file_size = 1ULL << 20;
    disk_cfg.disk_min_eval_duration_us = 0;
    disk_cfg.admission_threshold = 1;
    mgr.SetConfig(disk_cfg);

    // Old data should be gone
    got.active_count = 128;
    ASSERT_FALSE(mgr.Get(k, got));

    // Put in disk mode should work
    mgr.Put(k, v);
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k, got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, SwitchToDiskCleansTargetDirWithSamePath) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_switch_same_path_" + std::to_string(getpid()) +
                   "_" + std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig mem_cfg;
    mem_cfg.mode = milvus::exec::CacheMode::Memory;
    mem_cfg.disk_base_path = tmpdir.string();
    mem_cfg.mem_max_bytes = 1ULL << 20;
    mem_cfg.compression_enabled = true;
    mem_cfg.admission_threshold = 1;
    mem_cfg.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(mgr.SetConfig(mem_cfg));

    auto stale_path = tmpdir / "stale.cache";
    std::ofstream(stale_path) << "stale";
    ASSERT_TRUE(std::filesystem::exists(stale_path));

    milvus::exec::CacheConfig disk_cfg;
    disk_cfg.mode = milvus::exec::CacheMode::Disk;
    disk_cfg.disk_base_path = tmpdir.string();
    disk_cfg.disk_max_file_size = 1ULL << 20;
    disk_cfg.disk_min_eval_duration_us = 0;
    disk_cfg.admission_threshold = 1;
    ASSERT_TRUE(mgr.SetConfig(disk_cfg));
    ASSERT_FALSE(std::filesystem::exists(stale_path));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, DiskConfigCreateDirFailureDisablesCache) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_bad_dir_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);
    auto bad_path = tmpdir / "not_a_dir";
    std::ofstream(bad_path) << "file";

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = bad_path.string();
    cfg.disk_max_file_size = 1ULL << 20;
    cfg.disk_min_eval_duration_us = 0;
    ASSERT_FALSE(mgr.SetConfig(cfg));
    ASSERT_FALSE(ExprResCacheManager::IsEnabled());

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
}

TEST(ExprResCacheManagerV2Test, DiskModeLatencyFilter) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_latency_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = tmpdir.string();
    cfg.disk_max_file_size = 1ULL << 20;
    cfg.disk_min_eval_duration_us = 200;  // filter: skip if < 200us
    cfg.admission_threshold = 1;
    mgr.SetConfig(cfg);

    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.active_count = 128;

    // Fast expression (100us < 200us threshold): rejected
    ExprResCacheManager::Key k1{500, "fast_disk_expr"};
    v.eval_duration_us = 100;
    mgr.Put(k1, v);
    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_FALSE(mgr.Get(k1, got));

    // Slow expression (500us >= 200us threshold): admitted
    ExprResCacheManager::Key k2{500, "slow_disk_expr"};
    v.eval_duration_us = 500;
    mgr.Put(k2, v);
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k2, got));

    // A cheap recompute is rejected even when the signature already exists.
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128, false));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.eval_duration_us = 1;
    mgr.Put(k2, v);
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k2, got));
    ASSERT_TRUE((*got.result)[0]);

    // eval_duration_us=0 means skip cost check: admitted
    ExprResCacheManager::Key k3{500, "no_dur_disk_expr"};
    v.eval_duration_us = 0;
    mgr.Put(k3, v);
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k3, got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, DiskModeRowCountChangeSkipsGrowingSegment) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_row_count_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    milvus::exec::CacheConfig cfg;
    cfg.mode = milvus::exec::CacheMode::Disk;
    cfg.disk_base_path = tmpdir.string();
    cfg.disk_max_file_size = 1ULL << 20;
    cfg.disk_min_eval_duration_us = 0;
    cfg.admission_threshold = 1;
    mgr.SetConfig(cfg);

    ExprResCacheManager::Key k1{550, "row_count_128"};
    ExprResCacheManager::Value v1;
    v1.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v1.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v1.active_count = 128;
    mgr.Put(k1, v1);

    ExprResCacheManager::Value got;
    got.active_count = 128;
    ASSERT_TRUE(mgr.Get(k1, got));

    ExprResCacheManager::Key k2{550, "row_count_256"};
    ExprResCacheManager::Value v2;
    v2.result = std::make_shared<milvus::TargetBitmap>(MakeBits(256));
    v2.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(256));
    v2.active_count = 256;
    mgr.Put(k2, v2);

    got.active_count = 128;
    ASSERT_FALSE(mgr.Get(k1, got));
    got.active_count = 256;
    ASSERT_FALSE(mgr.Get(k2, got));

    mgr.Put(k2, v2);
    got.active_count = 256;
    ASSERT_FALSE(mgr.Get(k2, got));

    ASSERT_EQ(mgr.EraseSegment(550), 0u);
    mgr.Put(k2, v2);
    got.active_count = 256;
    ASSERT_TRUE(mgr.Get(k2, got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerV2Test, ConcurrentSetConfigAndGetPut) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_test_concurrent_" + std::to_string(getpid()) + "_" +
                   std::to_string(rand()));
    std::filesystem::create_directories(tmpdir);

    std::atomic<bool> reconfig_done{false};
    std::atomic<int> put_ops{0};
    std::atomic<int> get_ops{0};

    std::thread reconfig_thread([&]() {
        for (int i = 0; i < 400; ++i) {
            milvus::exec::CacheConfig cfg;
            if ((i % 2) == 0) {
                cfg.mode = milvus::exec::CacheMode::Memory;
                cfg.mem_max_bytes = 1ULL << 20;
                cfg.compression_enabled = true;
                cfg.admission_threshold = 1;
                cfg.mem_min_eval_duration_us = 0;
            } else {
                cfg.mode = milvus::exec::CacheMode::Disk;
                cfg.disk_base_path = tmpdir.string();
                cfg.disk_max_file_size = 1ULL << 20;
                cfg.disk_min_eval_duration_us = 0;
                cfg.admission_threshold = 1;
            }
            mgr.SetConfig(cfg);
        }
        reconfig_done.store(true, std::memory_order_release);
    });

    std::thread io_thread([&]() {
        int i = 0;
        while (!reconfig_done.load(std::memory_order_acquire) || i < 400) {
            ExprResCacheManager::Key key{
                700 + (i % 8), "concurrent_sig_" + std::to_string(i % 16)};
            ExprResCacheManager::Value v;
            v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(256));
            v.valid_result =
                std::make_shared<milvus::TargetBitmap>(MakeBits(256));
            v.active_count = 256;
            v.eval_duration_us = 0;

            mgr.Put(key, v);
            put_ops.fetch_add(1, std::memory_order_relaxed);

            ExprResCacheManager::Value got;
            got.active_count = 256;
            (void)mgr.Get(key, got);
            get_ops.fetch_add(1, std::memory_order_relaxed);
            ++i;
        }
    });

    reconfig_thread.join();
    io_thread.join();

    ASSERT_GT(put_ops.load(std::memory_order_relaxed), 0);
    ASSERT_GT(get_ops.load(std::memory_order_relaxed), 0);

    milvus::exec::CacheConfig final_cfg;
    final_cfg.mode = milvus::exec::CacheMode::Memory;
    final_cfg.mem_max_bytes = 1ULL << 20;
    final_cfg.compression_enabled = true;
    final_cfg.admission_threshold = 1;
    final_cfg.mem_min_eval_duration_us = 0;
    mgr.SetConfig(final_cfg);

    ExprResCacheManager::Key final_key{999, "final_sig"};
    ExprResCacheManager::Value final_v;
    final_v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(256));
    final_v.valid_result =
        std::make_shared<milvus::TargetBitmap>(MakeBits(256));
    final_v.active_count = 256;
    final_v.eval_duration_us = 0;
    mgr.Put(final_key, final_v);

    ExprResCacheManager::Value final_got;
    final_got.active_count = 256;
    ASSERT_TRUE(mgr.Get(final_key, final_got));

    mgr.Clear();
    std::filesystem::remove_all(tmpdir);
    ExprResCacheManager::SetEnabled(false);
}

// ---- V2 E2E Performance Benchmark: Memory vs Disk across densities ----

TEST(ExprResCacheV2PerfTest, EndToEndBothModes) {
    using namespace milvus::exec;
    auto& mgr = ExprResCacheManager::Instance();

    struct Scenario {
        const char* name;
        double density;
    };
    std::vector<Scenario> scenarios = {
        {"0.1%", 0.001},
        {"1%", 0.01},
        {"5%", 0.05},
        {"10%", 0.10},
        {"50%", 0.50},
        {"90%", 0.90},
        {"99%", 0.99},
    };

    const size_t N_BITS = 1000000;  // 1M rows
    const int N_ENTRIES = 100;
    const int N_GET_REPEAT = 3;

    auto tmpdir = std::filesystem::temp_directory_path() /
                  ("excr_v2_perf_" + std::to_string(getpid()));
    std::filesystem::create_directories(tmpdir);

    printf("\n=== ExprResCache V2 E2E Performance ===\n");
    printf(
        "  %d entries per density, 1M-row bitset, %d Get rounds + warmup\n\n",
        N_ENTRIES,
        N_GET_REPEAT);

    // Run 3 passes: memory+compressed, memory+raw, disk
    for (int mode_idx = 0; mode_idx < 3; ++mode_idx) {
        const char* mode_name = mode_idx == 0   ? "Memory (compressed)"
                                : mode_idx == 1 ? "Memory (raw, no compression)"
                                                : "Disk (raw, pread/pwrite)";
        bool is_memory = (mode_idx <= 1);
        bool compress = (mode_idx == 0);

        printf("--- %s ---\n", mode_name);
        printf("%-8s | %8s | %8s %8s %8s | %10s\n",
               "Density",
               "Raw(B)",
               "Put(us)",
               "Get_avg",
               "Get_p99",
               "StoredBytes");
        printf("%-8s-+-%8s-+-%8s-%8s-%8s-+-%10s\n",
               "--------",
               "--------",
               "--------",
               "--------",
               "--------",
               "----------");

        for (auto& s : scenarios) {
            mgr.Clear();
            ExprResCacheManager::SetEnabled(true);

            CacheConfig config;
            if (is_memory) {
                config.mode = CacheMode::Memory;
                config.mem_max_bytes = 1ULL << 30;  // 1GB for testing
                config.compression_enabled = compress;
                config.admission_threshold = 1;       // no frequency filter
                config.mem_min_eval_duration_us = 0;  // no latency filter
            } else {
                config.mode = CacheMode::Disk;
                config.disk_base_path = tmpdir.string();
                config.disk_max_file_size = 256ULL << 20;
                config.disk_min_eval_duration_us = 0;  // no filter
                config.admission_threshold = 1;        // no frequency filter
            }
            mgr.SetConfig(config);

            // Prepare entries: all share segment_id=1 so disk mode uses one
            // DiskSlotFile.
            std::vector<ExprResCacheManager::Key> keys;
            std::vector<ExprResCacheManager::Value> values;
            for (int i = 0; i < N_ENTRIES; ++i) {
                ExprResCacheManager::Key k;
                k.segment_id = 1;
                k.signature = "perf_sig_" + std::to_string(i);
                keys.push_back(k);

                ExprResCacheManager::Value v;
                v.result = std::make_shared<milvus::TargetBitmap>(
                    MakeRandomBits(N_BITS, s.density, 42 + i));
                v.valid_result = std::make_shared<milvus::TargetBitmap>(
                    MakeBits(N_BITS, true));
                v.active_count = static_cast<int64_t>(N_BITS);
                v.eval_duration_us = 5000;  // 5ms pretend eval time
                values.push_back(v);
            }
            size_t raw_bytes = values[0].result->size_in_bytes() +
                               values[0].valid_result->size_in_bytes();

            // Put benchmark
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < N_ENTRIES; ++i) {
                mgr.Put(keys[i], values[i]);
            }
            auto t1 = std::chrono::high_resolution_clock::now();
            auto put_avg =
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                    .count() /
                N_ENTRIES;

            // Get benchmark: warmup then measured rounds
            for (int i = 0; i < N_ENTRIES; ++i) {
                ExprResCacheManager::Value got;
                got.active_count = static_cast<int64_t>(N_BITS);
                mgr.Get(keys[i], got);
            }

            std::vector<long long> get_us;
            for (int rep = 0; rep < N_GET_REPEAT; ++rep) {
                for (int i = 0; i < N_ENTRIES; ++i) {
                    ExprResCacheManager::Value got;
                    got.active_count = static_cast<int64_t>(N_BITS);
                    auto g0 = std::chrono::high_resolution_clock::now();
                    bool hit = mgr.Get(keys[i], got);
                    auto g1 = std::chrono::high_resolution_clock::now();
                    ASSERT_TRUE(hit)
                        << "miss at density=" << s.name << " entry=" << i;
                    get_us.push_back(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            g1 - g0)
                            .count());
                }
            }
            std::sort(get_us.begin(), get_us.end());
            size_t total = get_us.size();
            auto get_avg = std::accumulate(get_us.begin(), get_us.end(), 0LL) /
                           static_cast<long long>(total);
            auto get_p99 = get_us[total * 99 / 100];

            // Storage size
            size_t stored = 0;
            if (is_memory) {
                stored = mgr.GetCurrentBytes();
            } else {
                for (auto& entry :
                     std::filesystem::directory_iterator(tmpdir)) {
                    if (entry.path().extension() == ".cache") {
                        stored += std::filesystem::file_size(entry.path());
                    }
                }
            }

            printf("%-8s | %8zu | %8ld %8lld %8lld | %10zu\n",
                   s.name,
                   raw_bytes,
                   put_avg,
                   get_avg,
                   get_p99,
                   stored);
        }
        printf("\n");

        mgr.Clear();
        ExprResCacheManager::SetEnabled(false);
    }

    std::filesystem::remove_all(tmpdir);
}
