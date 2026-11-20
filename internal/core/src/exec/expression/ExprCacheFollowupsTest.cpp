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
        // Enough for eight selected rows, deliberately too small for the segment.
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

TEST_P(ExprCacheFollowupsTest, SparseHitsRespectOrderDuplicatesAndSmallBudget) {
    constexpr int64_t rows = 131073;
    const std::vector<int32_t> selected{rows - 1, 64, 0, 63, 64, 65536, 1};
    const std::span<const int32_t> offsets(selected);
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
            ExprResCacheManager::Value full, subset;
            full.active_count = rows;
            EXPECT_EQ(manager_.GetWithStatus(key, full),
                      ExprResCacheManager::LookupResult::ResourceLimit);
            ASSERT_EQ(manager_.ReadWithStatus(reader, subset, &offsets),
                      ExprResCacheManager::LookupResult::Hit);
            ASSERT_EQ(subset.result->size(), selected.size());
            for (size_t i = 0; i < selected.size(); ++i) {
                EXPECT_EQ((*subset.result)[i], (*value.result)[selected[i]]);
                EXPECT_EQ((*subset.valid_result)[i],
                          (*value.valid_result)[selected[i]]);
            }
            EXPECT_EQ(manager_.GetMaterializationBytes(), 16);
            const auto* result_buffer = subset.result->data();
            const auto* valid_buffer = subset.valid_result->data();
            auto output = ExprCacheHelper::MoveToColumn(
                std::move(subset.result), std::move(subset.valid_result));
            EXPECT_EQ(output->GetRawData(), result_buffer);
            EXPECT_EQ(output->GetValidRawData(), valid_buffer);
            reader = {};
            manager_.Clear();
            EXPECT_EQ(manager_.GetMaterializationBytes(), 16);
            output.reset();
            EXPECT_EQ(manager_.GetMaterializationBytes(), 0);
        }
    }
}

TEST_P(ExprCacheFollowupsTest, InvalidOffsetsAndMissingGenerationCannotHit) {
    test::ExprCacheKey key{81002, "offsets"};
    ExprResCacheManager::Value value;
    value.active_count = 128;
    value.result = std::make_shared<TargetBitmap>(128, true);
    value.valid_result = std::make_shared<TargetBitmap>(128, true);
    manager_.Put(key, value);
    auto reader = manager_.Lookup(key, 128);
    ASSERT_TRUE(reader);
    for (int32_t offset : {-1, 128}) {
        const std::span<const int32_t> selected(&offset, 1);
        ExprResCacheManager::Value output;
        EXPECT_EQ(manager_.ReadWithStatus(reader, output, &selected),
                  ExprResCacheManager::LookupResult::Miss);
        EXPECT_EQ(manager_.GetMaterializationBytes(), 0);
    }
    const std::span<const int32_t> empty;
    ExprResCacheManager::Value output;
    ASSERT_EQ(manager_.ReadWithStatus(reader, output, &empty),
              ExprResCacheManager::LookupResult::Hit);
    EXPECT_EQ(output.result->size(), 0);
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
                found = {};
                const auto reader = manager_.Lookup(key, 64);
                const std::array<int32_t, 3> positions{63, 0, 63};
                const std::span<const int32_t> offsets(positions);
                ASSERT_EQ(manager_.ReadWithStatus(reader, found, &offsets),
                          ExprResCacheManager::LookupResult::Hit);
                EXPECT_EQ(found.result->count(), result ? 3 : 0);
                EXPECT_EQ(found.valid_result->count(), valid ? 3 : 0);
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
