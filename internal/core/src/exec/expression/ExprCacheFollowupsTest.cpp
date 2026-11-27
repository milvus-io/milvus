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

#include "exec/expression/ExprCacheHelper.h"
#include "exec/expression/DiskSlotFile.h"
#include "expr/ITypeExpr.h"
#include "test_utils/ExprCacheTestUtils.h"

#include <gtest/gtest.h>
#include <folly/ScopeGuard.h>
#include <future>
#include <thread>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <roaring/memory.h>

namespace milvus {
struct ExprCacheSignatureTestAccess {
    static const std::string*
    CanonicalStorage(const ExprCacheSignature& signature) {
        return signature.identity_ ? signature.identity_->text.get() : nullptr;
    }
};
}  // namespace milvus

namespace milvus::exec {
namespace {

// Fail any CRoaring allocation after cache publication. Read hits must only
// allocate their output bitmaps, never deserialize a hidden set of containers.
std::atomic<size_t> roaring_read_allocations{0};
void*
FailRoaringReadMalloc(size_t) {
    ++roaring_read_allocations;
    return nullptr;
}
void*
FailRoaringReadRealloc(void*, size_t) {
    ++roaring_read_allocations;
    return nullptr;
}
void*
FailRoaringReadCalloc(size_t, size_t) {
    ++roaring_read_allocations;
    return nullptr;
}
void*
FailRoaringReadAlignedMalloc(size_t, size_t) {
    ++roaring_read_allocations;
    return nullptr;
}
void*
DefaultRoaringAlignedMalloc(size_t alignment, size_t size) {
    void* pointer = nullptr;
    return posix_memalign(&pointer, alignment, size) == 0 ? pointer : nullptr;
}

TEST(ExprCacheReadViewTest, FullHitsReuseChargedViewWithoutRoaringAllocations) {
    auto& manager = ExprResCacheManager::Instance();
    const auto cleanup = folly::makeGuard([&] {
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
        EXPECT_TRUE(manager.SetConfig(CacheConfig{}));
    });
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    config.materialization_max_bytes = 2 * ((16777217 + 63) / 64) * 8;
    ASSERT_TRUE(manager.SetConfig(config));
    ExprResCacheManager::SetEnabled(true);
    constexpr int64_t rows = 16777217;
    ExprResCacheManager::Value value;
    value.active_count = rows;
    value.result = std::make_shared<TargetBitmap>(rows, false);
    value.valid_result = std::make_shared<TargetBitmap>(rows, true);
    for (int64_t i = 0; i < rows; i += 1000) (*value.result)[i] = true;
    for (int64_t i = 7; i < rows; i += 1001) (*value.valid_result)[i] = false;
    const test::ExprCacheKey key{82001, "read-view"};
    manager.Put(key, value);
    auto reader = manager.Lookup(key, rows);
    ASSERT_TRUE(reader.memory);
    ASSERT_GT(reader.memory->ReadViewBytes(), 0u);
    const auto charge = manager.GetMemoryBytes();
    EXPECT_GE(charge,
              sizeof(EntryPool::Payload) + reader.memory->data.capacity() +
                  reader.memory->ReadViewBytes());
    const auto* payload = reader.memory.get();
    manager.Clear();
    EXPECT_EQ(manager.GetMemoryBytes(), charge);

    roaring_read_allocations = 0;
    const auto restore_allocator = folly::makeGuard([] {
        roaring_init_memory_hook({std::malloc,
                                  std::realloc,
                                  std::calloc,
                                  std::free,
                                  DefaultRoaringAlignedMalloc,
                                  std::free});
    });
    roaring_init_memory_hook({FailRoaringReadMalloc,
                              FailRoaringReadRealloc,
                              FailRoaringReadCalloc,
                              std::free,
                              FailRoaringReadAlignedMalloc,
                              std::free});
    for (int32_t batch = 0; batch < 20; ++batch) {
        const std::vector<int32_t> selected{batch * 1000,
                                            batch * 1001 + 7,
                                            65536,
                                            65535,
                                            rows - 1,
                                            batch * 1000};
        ExprResCacheManager::Value output;
        ASSERT_EQ(manager.ReadWithStatus(reader, output),
                  ExprResCacheManager::LookupResult::Hit);
        for (size_t i = 0; i < selected.size(); ++i) {
            EXPECT_EQ((*output.result)[selected[i]],
                      (*value.result)[selected[i]]);
            EXPECT_EQ((*output.valid_result)[selected[i]],
                      (*value.valid_result)[selected[i]]);
        }
        EXPECT_EQ(manager.GetMaterializationBytes(),
                  config.materialization_max_bytes);
        ExprResCacheManager::Value blocked;
        EXPECT_EQ(manager.ReadWithStatus(reader, blocked),
                  ExprResCacheManager::LookupResult::ResourceLimit);
        EXPECT_EQ(reader.memory.get(), payload);
        EXPECT_EQ(manager.GetMemoryBytes(), charge);
    }
    EXPECT_EQ(roaring_read_allocations.load(), 0u);
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
    reader = {};
    EXPECT_EQ(manager.GetMemoryBytes(), 0u);
}

TEST(ExprCacheReadViewTest, AdmissionIncludesViewMetadata) {
    auto budget = std::make_shared<ExprCacheMemoryBudget>(1 << 20);
    EntryPool pool(1 << 20, budget);
    constexpr size_t rows = 1048576;
    TargetBitmap result(rows, false), valid(rows, true);
    for (size_t i = 0; i < rows; i += 1000) result[i] = true;
    pool.Put(1, "view", rows, result, valid);
    auto reader = pool.Lookup(1, "view", rows);
    ASSERT_TRUE(reader);
    ASSERT_GT(reader->ReadViewBytes(), 0u);
    const auto charge = pool.GetCurrentBytes();
    pool.Clear();
    EXPECT_EQ(pool.GetCurrentBytes(), charge);
    reader.reset();
    EXPECT_EQ(pool.GetCurrentBytes(), 0u);
    pool.Configure(charge - 1, true, 0);
    pool.Put(1, "view", rows, result, valid);
    EXPECT_EQ(pool.GetEntryCount(), 0u);
    EXPECT_EQ(pool.GetCurrentBytes(), 0u);
}

TEST(CacheCompressorTest, ReadViewHandlesUnalignedMixedContainersAndInversion) {
    constexpr size_t rows = 1048577;
    TargetBitmap original(rows, false);
    // A bitset container, a run container and two array containers. Overall
    // density still selects Roaring. The tail also checks partial words.
    for (size_t i = 0; i < 10000; i += 2) original[i] = true;
    for (size_t i = 65536 + 63; i < 65536 + 1000; ++i) original[i] = true;
    original[2 * 65536 + 17] = original[rows - 1] = true;
    for (bool inverted : {false, true}) {
        auto result = original.clone();
        if (inverted)
            result.flip();
        auto valid = original.clone();
        valid.flip();
        uint8_t encoding;
        auto bytes = CacheCompressor::Compress(result, valid, true, encoding);
        ASSERT_EQ(encoding, kCompTypeIndependent);
        for (size_t shift : {1, 3, 7}) {
            std::vector<char> unaligned(bytes.size() + shift);
            std::memcpy(unaligned.data() + shift, bytes.data(), bytes.size());
            CacheCompressor::ReadView view(
                unaligned.data() + shift, bytes.size(), encoding, rows);
            ASSERT_TRUE(view.IsValid());
            TargetBitmap full, full_valid;
            ASSERT_TRUE(view.Decode(full, full_valid));
            for (size_t i = 0; i < rows; ++i) {
                ASSERT_EQ(full[i], result[i]) << i;
                ASSERT_EQ(full_valid[i], valid[i]) << i;
            }
        }
        for (size_t size :
             {size_t{0}, size_t{7}, size_t{13}, bytes.size() - 1}) {
            CacheCompressor::ReadView truncated(
                bytes.data(), size, encoding, rows);
            EXPECT_FALSE(truncated.IsValid());
        }
        auto corrupt = bytes;
        std::memset(corrupt.data() + 14, 0xff, 4);  // Invalid portable cookie.
        CacheCompressor::ReadView invalid(
            corrupt.data(), corrupt.size(), encoding, rows);
        EXPECT_FALSE(invalid.IsValid());
    }
}

TEST(CacheCompressorTest, ReadViewHandlesOptionalPortableOffsetTable) {
    constexpr size_t rows = 262145;
    for (size_t count : {0, 1, 3, 4}) {
        TargetBitmap bits(rows, false), valid(rows, true);
        std::vector<int32_t> offsets{0, rows - 1};
        for (size_t key = 0; key < count; ++key) {
            for (size_t j = 31; j <= 80; ++j) bits[key * 65536 + j] = true;
            for (size_t j : {30, 31, 80, 81})
                offsets.push_back(key * 65536 + j);
        }
        uint8_t encoding;
        auto bytes = CacheCompressor::Compress(bits, valid, true, encoding);
        CacheCompressor::ReadView view(
            bytes.data(), bytes.size(), encoding, rows);
        ASSERT_TRUE(view.IsValid()) << count;
        TargetBitmap selected, selected_valid;
        ASSERT_TRUE(view.Decode(selected, selected_valid));
        for (size_t i = 0; i < offsets.size(); ++i) {
            EXPECT_EQ(selected[offsets[i]], bits[offsets[i]]);
            EXPECT_TRUE(selected_valid[offsets[i]]);
        }
    }
}

class ExprCacheFollowupsTest : public ::testing::TestWithParam<CacheMode> {
 protected:
    void
    SetUp() override {
        path_ =
            std::filesystem::temp_directory_path() /
            ("expr_followups_" + std::to_string(::getpid()) + "_" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
        config_.mode = GetParam();
        config_.disk_base_path = path_.string();
        config_.disk_max_file_size = 1 << 20;
        config_.admission_threshold = 1;
        config_.mem_min_eval_duration_us = config_.disk_min_eval_duration_us =
            0;
        // Some tests deliberately start below the full-pair materialization size.
        config_.materialization_max_bytes = 16;
        ASSERT_TRUE(manager_.SetConfig(config_));
        ExprResCacheManager::SetEnabled(true);
    }
    void
    TearDown() override {
        manager_.Clear();
        ExprResCacheManager::SetEnabled(false);
        EXPECT_TRUE(manager_.SetConfig(CacheConfig{}));
        EXPECT_EQ(manager_.GetMaterializationBytes(), 0);
        std::filesystem::remove_all(path_);
    }
    ExprResCacheManager& manager_ = ExprResCacheManager::Instance();
    CacheConfig config_;
    std::filesystem::path path_;
};

TEST_P(ExprCacheFollowupsTest, FullHitsDecodeAllCodecsAndKeepMovedOutputLease) {
    constexpr int64_t rows = 131073;
    const auto pair_bytes = 2 * ((rows + 63) / 64) * 8;
    config_.materialization_max_bytes = pair_bytes;
    ASSERT_TRUE(manager_.SetConfig(config_));
    // Exercise raw, sparse Roaring, and inverted Roaring independently for
    // result/validity, including omitted all-valid payloads and partial words.
    for (int result_kind = 0; result_kind < 3; ++result_kind) {
        for (int valid_kind = 0; valid_kind < 4; ++valid_kind) {
            manager_.Clear();
            test::ExprCacheKey key{81001, "offsets"};
            ExprResCacheManager::Value value;
            value.active_count = rows;
            value.result = std::make_shared<TargetBitmap>(rows, false);
            value.valid_result = std::make_shared<TargetBitmap>(rows, false);
            for (int64_t i = 0; i < rows; ++i) {
                (*value.result)[i] = result_kind == 0   ? i % 2 == 0
                                     : result_kind == 1 ? i % 1000 == 0
                                                        : i % 1000 != 0;
                (*value.valid_result)[i] = valid_kind == 0   ? i % 3 != 0
                                           : valid_kind == 1 ? i % 1001 == 0
                                           : valid_kind == 2 ? i % 1001 != 0
                                                             : true;
            }
            manager_.Put(key, value);
            auto reader = manager_.Lookup(key, rows);
            ASSERT_TRUE(reader);
            EXPECT_EQ(manager_.GetMaterializationBytes(), 0);
            ExprResCacheManager::Value subset;
            ASSERT_EQ(manager_.ReadWithStatus(reader, subset),
                      ExprResCacheManager::LookupResult::Hit);
            EXPECT_TRUE(*subset.result == *value.result);
            EXPECT_TRUE(*subset.valid_result == *value.valid_result);
            EXPECT_EQ(manager_.GetMaterializationBytes(), pair_bytes);
            const auto* result_buffer = subset.result->data();
            const auto* valid_buffer = subset.valid_result->data();
            auto output = ExprCacheHelper::MoveToColumn(
                std::move(subset.result), std::move(subset.valid_result));
            EXPECT_EQ(output->GetRawData(), result_buffer);
            EXPECT_EQ(output->GetValidRawData(), valid_buffer);
            reader = {};
            manager_.Clear();
            EXPECT_EQ(manager_.GetMaterializationBytes(), pair_bytes);
            output.reset();
            EXPECT_EQ(manager_.GetMaterializationBytes(), 0);
        }
    }
}

TEST_P(ExprCacheFollowupsTest,
       FullHitsRequireWholePairBudgetAndLiveGeneration) {
    test::ExprCacheKey key{81002, "offsets"};
    ExprResCacheManager::Value value;
    value.active_count = 128;
    value.result = std::make_shared<TargetBitmap>(128, true);
    value.valid_result = std::make_shared<TargetBitmap>(128, true);
    manager_.Put(key, value);
    auto reader = manager_.Lookup(key, 128);
    ASSERT_TRUE(reader);
    ExprResCacheManager::Value output;
    EXPECT_EQ(manager_.ReadWithStatus(reader, output),
              ExprResCacheManager::LookupResult::ResourceLimit);
    EXPECT_EQ(manager_.GetMaterializationBytes(), 0);
    EXPECT_EQ(output.result, nullptr);
    EXPECT_EQ(output.valid_result, nullptr);
    ExprResCacheManager::Key invalid{key.segment_id, key.signature, nullptr};
    manager_.Put(invalid, value);
    EXPECT_FALSE(manager_.Lookup(invalid, 128));
    EXPECT_FALSE(
        manager_.ObserveMiss(invalid, 128, SegmentType::Sealed).admitted);
    EXPECT_EQ(manager_.GetEntryCount(), 1);
}

TEST_P(ExprCacheFollowupsTest, ReadHandleSurvivesRetirementAndReconfiguration) {
    config_.materialization_max_bytes = 1024;
    ASSERT_TRUE(manager_.SetConfig(config_));
    test::ExprCacheKey key{81003, "held"};
    ExprResCacheManager::Value value;
    value.active_count = 128;
    value.result = std::make_shared<TargetBitmap>(128, true);
    value.valid_result = std::make_shared<TargetBitmap>(128, true);
    manager_.Put(key, value);
    auto held = manager_.Lookup(key, 128);
    ASSERT_TRUE(held);
    const auto old_path =
        test::CacheFile(path_, key.segment_id, key.generation->id);
    {
        auto retired =
            manager_.RetireGeneration(key.segment_id, key.generation);
    }
    ASSERT_TRUE(manager_.SetConfig(config_));
    EXPECT_FALSE(manager_.Lookup(key, 128));
    test::ExprCacheKey next{key.segment_id, key.signature};
    manager_.Put(next, value);
    ExprResCacheManager::Value output;
    ASSERT_EQ(manager_.ReadWithStatus(held, output),
              ExprResCacheManager::LookupResult::Hit);
    EXPECT_TRUE(output.result->all());
    if (GetParam() == CacheMode::Disk) {
        EXPECT_TRUE(std::filesystem::exists(old_path));
        EXPECT_NE(old_path,
                  test::CacheFile(path_, next.segment_id, next.generation->id));
    }
    held = {};
    EXPECT_FALSE(std::filesystem::exists(old_path));
    EXPECT_TRUE(manager_.Lookup(next, 128));
}

TEST_P(ExprCacheFollowupsTest, HashCollisionsCannotAliasResultsOrDerivedKeys) {
    const ExprCacheSignature first("field=100:value=1");
    ExprCacheSignature different("field=100:value=2");
    // Force a collision in BOTH hash halves; no probabilistic search needed.
    different.low = first.low;
    different.high = first.high;
    auto ttl_100 = first.Derive(3).Derive(101).Derive(100);
    auto ttl_200 = first.Derive(3).Derive(101).Derive(200);
    ttl_200.low = ttl_100.low;
    ttl_200.high = ttl_100.high;
    const std::vector<std::pair<ExprCacheSignature, ExprCacheSignature>> cases{
        {first, different},
        {first.Derive(1), different.Derive(1)},
        {ttl_100, ttl_200}};
    for (const auto& [left, right] : cases) {
        manager_.Clear();
        ASSERT_EQ(left.low, right.low);
        ASSERT_EQ(left.high, right.high);
        ASSERT_NE(left, right);
        test::ExprCacheKey left_key{81005, left};
        test::ExprCacheKey right_key{81005, right};
        const auto make_value = [](bool result, bool valid) {
            ExprResCacheManager::Value value;
            value.active_count = 64;
            value.result = std::make_shared<TargetBitmap>(64, result);
            value.valid_result = std::make_shared<TargetBitmap>(64, valid);
            return value;
        };
        const auto expect_value =
            [&](const ExprResCacheManager::Key& key, bool result, bool valid) {
                ExprResCacheManager::Value found;
                found.active_count = 64;
                ASSERT_TRUE(manager_.Get(key, found));
                EXPECT_EQ(found.result->count(), result ? 64 : 0);
                EXPECT_EQ(found.valid_result->count(), valid ? 64 : 0);
            };
        manager_.Put(left_key, make_value(true, true));
        EXPECT_FALSE(manager_.Lookup(right_key, 64));
        manager_.Put(right_key, make_value(false, false));
        EXPECT_EQ(manager_.GetEntryCount(), 2);
        expect_value(left_key, true, true);
        expect_value(right_key, false, false);
        // Replacing one colliding key must not overwrite the other slot/entry.
        manager_.Put(left_key, make_value(false, true));
        EXPECT_EQ(manager_.GetEntryCount(), 2);
        expect_value(left_key, false, true);
        expect_value(right_key, false, false);
    }
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         ExprCacheFollowupsTest,
                         ::testing::Values(CacheMode::Memory, CacheMode::Disk));

TEST(ExprCacheSignatureTest, LogicalSignatureIsComputedOnceAcrossThreads) {
    struct CountedExpr : expr::ITypeFilterExpr {
        mutable std::atomic<int> calls{0};
        std::string
        ToString() const override {
            ++calls;
            return "shared-expression";
        }
    };
    const auto logical = std::make_shared<CountedExpr>();
    std::vector<std::future<ExprCacheSignature>> signatures;
    for (int i = 0; i < 16; ++i) {
        signatures.push_back(std::async(std::launch::async, [logical] {
            return logical->CacheSignature();
        }));
    }
    for (auto& signature : signatures) {
        EXPECT_EQ(signature.get(), ExprCacheSignature("shared-expression"));
    }
    EXPECT_EQ(logical->calls, 1);
}

TEST(ExprCacheSignatureTest, TermSetsNormalizeOrderButPreserveTypesAndDomains) {
    proto::plan::GenericValue one, two, text;
    one.set_int64_val(1);
    two.set_int64_val(2);
    text.set_string_val("1");
    const expr::ColumnInfo column(FieldId(100), DataType::JSON, {"value"});
    expr::TermFilterExpr first(column, {one, two, one});
    expr::TermFilterExpr reordered(column, {two, one});
    expr::TermFilterExpr different_type(column, {text, two});
    expr::TermFilterExpr reverse(column, {one, two}, true);
    EXPECT_EQ(first.CacheSignature(), reordered.CacheSignature());
    EXPECT_NE(first.CacheSignature(), different_type.CacheSignature());
    EXPECT_NE(first.CacheSignature(), reverse.CacheSignature());
    EXPECT_NE(first.CacheSignature(), first.CacheSignature().Derive(1));
    EXPECT_NE(first.CacheSignature().Derive(1),
              first.CacheSignature().Derive(2));
    EXPECT_EQ(first.vals_.size(), 3);
}

TEST(ExprCacheSignatureTest, PoolComparesBothHashHalves) {
    EntryPool pool(1 << 20);
    pool.Configure(1 << 20, false, 0);
    ExprCacheSignature first, second;
    first.low = second.high = 17;
    first.high = second.low = 31;  // same hasher bucket
    pool.Put({1, 1}, first, 64, TargetBitmap(64, true), TargetBitmap(64, true));
    pool.Put(
        {1, 1}, second, 64, TargetBitmap(64, false), TargetBitmap(64, true));
    TargetBitmap result, valid;
    ASSERT_TRUE(pool.Lookup({1, 1}, first, 64)->Decode(result, valid));
    EXPECT_TRUE(result.all());
    ASSERT_TRUE(pool.Lookup({1, 1}, second, 64)->Decode(result, valid));
    EXPECT_TRUE(result.none());
}

TEST(ExprCacheSignatureTest, ExactIdentityIncludesEveryDomainAndSharesText) {
    ExprCacheSignature base(std::string(200000, 'x'));
    const auto copy = base;
    const auto raw = base.Derive(1);
    const auto full = base.Derive(3).Derive(101).Derive(100);
    const ExprCacheSignature same_text(std::string(200000, 'x'));
    EXPECT_EQ(base, same_text);
    EXPECT_EQ(full, same_text.Derive(3).Derive(101).Derive(100));
    EXPECT_EQ(ExprCacheSignatureTestAccess::CanonicalStorage(base),
              ExprCacheSignatureTestAccess::CanonicalStorage(copy));
    EXPECT_EQ(ExprCacheSignatureTestAccess::CanonicalStorage(base),
              ExprCacheSignatureTestAccess::CanonicalStorage(raw));
    EXPECT_EQ(ExprCacheSignatureTestAccess::CanonicalStorage(base),
              ExprCacheSignatureTestAccess::CanonicalStorage(full));
    for (auto different : {base,
                           base.Derive(2),
                           base.Derive(3).Derive(102).Derive(100),
                           base.Derive(3).Derive(101).Derive(200)}) {
        different.low = full.low;
        different.high = full.high;
        EXPECT_NE(full, different);
    }
}

TEST(ExprCacheSignatureTest, PoolChargesRetainedCanonicalKeyOnReplacement) {
    constexpr size_t capacity = 1 << 20;
    EntryPool pool(capacity);
    pool.Configure(capacity, false, 0);
    std::string input("key");
    input.reserve(200000);
    ExprCacheSignature large_capacity(std::move(input));
    const ExprCacheSignature compact("key");
    ASSERT_EQ(large_capacity, compact);
    ASSERT_GT(large_capacity.RetainedBytes(), compact.RetainedBytes());
    TargetBitmap bits(64, true);
    pool.Put({1, 1}, large_capacity, 64, bits, bits);
    const auto charged = pool.GetCurrentBytes();
    EXPECT_GE(charged, large_capacity.RetainedBytes());
    pool.Put({1, 1}, compact, 64, bits, bits);
    // The existing map key survives replacement, including its large capacity.
    EXPECT_EQ(pool.GetCurrentBytes(), charged);
    pool.Clear();
    EXPECT_EQ(pool.GetCurrentBytes(), 0);
    pool.Configure(1024, false, 0);
    pool.Put({1, 1}, large_capacity, 64, bits, bits);
    EXPECT_EQ(pool.GetEntryCount(), 0);
}

TEST(ExprCacheFollowupClockTest, LargeAdmissionCannotScanTheWholePool) {
    EntryPool pool(16 << 20);
    pool.Configure(16 << 20, false, 0);
    constexpr size_t rows = 1024;
    TargetBitmap bits(rows, true);
    for (int i = 0; i < 4096; ++i) {
        pool.Put({i, 1}, "x", rows, bits, bits);
    }
    const auto used = pool.GetCurrentBytes();
    pool.Configure(used, false, 0);
    // Needs more entries than one bounded sweep can evict; rejection must
    // restore every provisional victim instead of emptying most of the cache.
    const size_t large_rows = used * 3;
    TargetBitmap large(large_rows, true);
    pool.Put({9000, 1}, "large", large_rows, large, large);
    EXPECT_EQ(pool.GetEntryCount(), 4096);
    EXPECT_EQ(pool.Lookup({9000, 1}, "large", large_rows), nullptr);
    pool.Put({9001, 1}, "small", rows, bits, bits);
    EXPECT_NE(pool.Lookup({9001, 1}, "small", rows), nullptr);
}

TEST(ExprCacheFollowupDiskTest, ReadersSeeOldSlotUntilReplacementPublishes) {
    const auto path = std::filesystem::temp_directory_path() /
                      ("expr_staged_" + std::to_string(::getpid()) + ".cache");
    auto file = std::make_shared<DiskSlotFile>(1, path.string(), 128, 4096);
    file->Put("same", 128, TargetBitmap(128, true), TargetBitmap(128, true));
    std::promise<void> entered, resume;
    auto entered_future = entered.get_future();
    auto proceed = resume.get_future().share();
    std::future<void> writer;
    auto release = folly::makeGuard([&] { resume.set_value(); });
    writer = std::async(std::launch::async, [&] {
        file->Put("same",
                  128,
                  TargetBitmap(128, false),
                  TargetBitmap(128, true),
                  [&] {
                      entered.set_value();
                      proceed.wait();
                  });
    });
    ASSERT_EQ(entered_future.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    auto reader = std::async(std::launch::async, [&] {
        TargetBitmap result, valid;
        return file->Get("same", 128, result, valid) && result.all();
    });
    EXPECT_EQ(reader.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    resume.set_value();
    release.dismiss();
    EXPECT_TRUE(reader.get());
    writer.get();
    TargetBitmap result, valid;
    ASSERT_TRUE(file->Get("same", 128, result, valid));
    EXPECT_TRUE(result.none());
    EXPECT_THROW(file->Put("same",
                           128,
                           TargetBitmap(128, true),
                           TargetBitmap(128, true),
                           [] { throw std::bad_alloc(); }),
                 std::bad_alloc);
    ASSERT_TRUE(file->Get("same", 128, result, valid));
    EXPECT_TRUE(result.none());
    file.reset();
    EXPECT_FALSE(std::filesystem::exists(path));
}

}  // namespace
}  // namespace milvus::exec
