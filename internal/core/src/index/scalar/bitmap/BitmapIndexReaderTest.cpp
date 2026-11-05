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

#include "index/scalar/bitmap/BitmapIndexLoader.h"
#include "index/test_utils/StorageTestUtils.h"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "index/contracts/query/INullReader.h"
#include "index/scalar/bitmap/BitmapIndexReader.h"
#include "index/SingleCellIndexStorageTranslator.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

template <typename Posting>
void
CheckBitmapCache(bool offset_cache) {
    const cachinglayer::ResourceUsage limit{16 * 1024 * 1024, 0};
    auto dlist = std::make_shared<cachinglayer::internal::DList>(
        false, limit, limit, limit, cachinglayer::EvictionConfig{});
    std::vector<int32_t> priorities;
    auto load = [&](OpContext* ctx)
        -> folly::coro::Task<
            std::unique_ptr<BitmapPostingStorage<std::string, Posting>>> {
        priorities.push_back(ctx->runtime_load_priority.value_or(-1));
        BitmapPostingMap<std::string, Posting> postings;
        if constexpr (std::is_same_v<Posting, roaring::Roaring>) {
            postings["apple"].add(0);
        } else {
            auto posting = TargetBitmap(2, false);
            posting.set(0);
            postings.emplace("apple", std::move(posting));
        }
        auto validity = TargetBitmap(2, false);
        validity.set(0);
        co_return std::make_unique<BitmapPostingStorage<std::string, Posting>>(
            std::move(postings),
            BitmapStorageOptions{.valid_bitset = std::move(validity),
                                 .total_num_rows = 2,
                                 .value_type = DataType::VARCHAR,
                                 .offset_cache = offset_cache});
    };
    IndexCacheConfig cache{
        "bitmap-storage-test",
        cachinglayer::Meta{cachinglayer::StorageType::MEMORY,
                           cachinglayer::CellIdMappingMode::IDENTICAL,
                           cachinglayer::CellDataType::OTHER,
                           CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                           true},
        {1024 * 1024, 0},
        {},
        128};
    std::weak_ptr<
        cachinglayer::CacheSlot<BitmapPostingStorage<std::string, Posting>>>
        slot_lifetime;
    auto make_index = [&] {
        auto slot = std::make_shared<cachinglayer::CacheSlot<
            BitmapPostingStorage<std::string, Posting>>>(
            std::make_unique<SingleCellIndexStorageTranslator<
                BitmapPostingStorage<std::string, Posting>>>(
                cache, false, load),
            dlist.get(),
            true,
            true,
            true,
            std::chrono::seconds(5),
            std::chrono::seconds(5));
        slot_lifetime = slot;
        return CreateBitmapIndex<std::string, Posting>(
            slot,
            IndexMetadata{ReaderCaps{.predicate = true,
                                     .pattern_match = true,
                                     .value_lookup = true,
                                     .cheap_value_lookup = offset_cache},
                          Domain::Row,
                          DataType::VARCHAR,
                          2});
    };
    auto index = make_index();
    OpContext first_ctx;
    first_ctx.runtime_load_priority = 0;
    OpContext second_ctx;
    second_ctx.runtime_load_priority = 1;
    auto first = index->CreateReader(&first_ctx);
    EXPECT_EQ(first->Count(), 2);
    EXPECT_TRUE(priorities.empty());
    const auto* values =
        dynamic_cast<const IScalarValueReader<std::string_view>*>(first.get());
    ASSERT_NE(values, nullptr);
    const int64_t rows[] = {0, 1};
    values->Gather(
        rows, 2, [&](int64_t i, const std::string_view* value, bool valid) {
            EXPECT_EQ(valid, i == 0);
            if (valid)
                EXPECT_EQ(*value, "apple");
        });
    EXPECT_EQ(values->Lookup(0), "apple");
    const auto scanned_bytes =
        first_ctx.storage_usage.scanned_total_bytes.load();
    ASSERT_GT(scanned_bytes, 0);
    EXPECT_FALSE(values->Lookup(1).has_value());
    EXPECT_EQ(values->Lookup(0), "apple");
    const auto* predicates =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
            first.get());
    ASSERT_NE(predicates, nullptr);
    const std::string_view key = "apple";
    EXPECT_EQ(predicates->In(1, &key).count(), 1);
    EXPECT_EQ(predicates->NotIn(1, &key).count(), 0);
    EXPECT_EQ(predicates->Range(key, CompareOp::NotEqual).count(), 0);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0}));
    EXPECT_EQ(first_ctx.storage_usage.scanned_total_bytes.load(),
              scanned_bytes);
    index.reset();
    EXPECT_FALSE(slot_lifetime.expired());
    first.reset();
    EXPECT_TRUE(slot_lifetime.expired());
    index = make_index();
    auto second = index->CreateReader(&second_ctx);
    const auto* nulls = dynamic_cast<const INullReader*>(second.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->IsNull().count(), 1);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0, 1}));
    index.reset();
    second.reset();
    EXPECT_TRUE(slot_lifetime.expired());
}

TEST(BitmapIndexCacheTest, OperationsSharePinUntilReaderDestruction) {
    for (const bool offset_cache : {false, true}) {
        CheckBitmapCache<roaring::Roaring>(offset_cache);
        CheckBitmapCache<TargetBitmap>(offset_cache);
    }
}

TEST(BitmapIndexOwnershipTest, RequestViewsShareKeysAndRetainTheIndex) {
    BitmapRoaringPostingMap<std::string> postings;
    postings["first"].add(0);
    postings["second"].add(1);
    TargetBitmap validity(3, true);
    validity.reset(2);
    auto index = CreateBitmapIndex<std::string>(
        std::move(postings),
        BitmapStorageOptions{.valid_bitset = std::move(validity),
                             .total_num_rows = 3,
                             .value_type = DataType::VARCHAR,
                             .offset_cache = true});
    std::weak_ptr<IIndex> index_lifetime = index;
    auto first = index->CreateReader(nullptr);
    auto second = index->CreateReader(nullptr);
    ASSERT_NE(first.get(), second.get());
    EXPECT_EQ(index->Metadata().count, 3);
    EXPECT_TRUE(index->Metadata().caps.cheap_value_lookup);
    index.reset();
    EXPECT_FALSE(index_lifetime.expired());

    const auto* first_values =
        dynamic_cast<const IScalarValueReader<std::string_view>*>(first.get());
    const auto* second_values =
        dynamic_cast<const IScalarValueReader<std::string_view>*>(second.get());
    ASSERT_NE(first_values, nullptr);
    ASSERT_NE(second_values, nullptr);
    const int64_t offset = 1;
    const char* first_key = nullptr;
    first_values->Gather(
        &offset, 1, [&](int64_t, const std::string_view* value, bool valid) {
            ASSERT_TRUE(valid);
            ASSERT_NE(value, nullptr);
            first_key = value->data();
        });
    second_values->Gather(
        &offset, 1, [&](int64_t, const std::string_view* value, bool valid) {
            ASSERT_TRUE(valid);
            ASSERT_NE(value, nullptr);
            EXPECT_EQ(value->data(), first_key);
            EXPECT_EQ(*value, "second");
        });
    auto owned = first_values->Lookup(offset);
    first.reset();
    EXPECT_FALSE(index_lifetime.expired());
    const auto* predicates =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
            second.get());
    ASSERT_NE(predicates, nullptr);
    const std::string_view key = "first";
    EXPECT_EQ(predicates->NotIn(1, &key).count(), 1);
    EXPECT_EQ(predicates->Range(key, CompareOp::Equal).count(), 1);
    second.reset();
    EXPECT_TRUE(index_lifetime.expired());
    EXPECT_EQ(owned, "second");
}

template <typename T>
void
ExpectPredicateProfiles(std::string_view type_name) {
    for (const auto suffix : {std::string_view{},
                              std::string_view{"Mmap"},
                              std::string_view{"NonNull"},
                              std::string_view{"NonNullMmap"}}) {
        const auto name =
            "Bitmap" + std::string(type_name) + std::string(suffix);
        const auto& backend = ScalarReaderBackends().Get<T>(name);
        EXPECT_TRUE(backend.DeriveCaps().predicate) << name;
    }
}

TEST(BitmapIndexReaderTest, AdvertisesPrimitivePredicateMatrix) {
    ExpectPredicateProfiles<bool>("Bool");
    ExpectPredicateProfiles<int8_t>("Int8");
    ExpectPredicateProfiles<int16_t>("Int16");
    ExpectPredicateProfiles<int32_t>("Int32");
    ExpectPredicateProfiles<int64_t>("Int64");
    ExpectPredicateProfiles<float>("Float");
    ExpectPredicateProfiles<double>("Double");
    ExpectPredicateProfiles<std::string_view>("Varchar");
}

TEST(BitmapIndexReaderTest, ProvidesInt64Queries) {
    // This promise is checked without capability filtering, so a regression
    // cannot silently remove this backend from the shared query suites.
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    const auto caps = backend.DeriveCaps();
    EXPECT_TRUE(caps.predicate);
    EXPECT_TRUE(caps.value_lookup);

    const std::array<int64_t, 1> values{42};
    const std::array<ScalarBuildBatch<int64_t>, 1> batches{
        ScalarBuildBatch<int64_t>{values, {}}};
    auto reader = backend.Create(ScalarBuildInput<int64_t>{batches});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_TRUE(reader->Caps().value_lookup);
    EXPECT_NE(
        dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get()),
        nullptr);
    EXPECT_NE(dynamic_cast<const IScalarValueReader<int64_t>*>(reader.get()),
              nullptr);
}

TEST(BitmapIndexReaderTest, ProvidesVarcharQueries) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("BitmapVarchar");
    const auto caps = backend.DeriveCaps();
    EXPECT_TRUE(caps.predicate);
    EXPECT_TRUE(caps.pattern_match);
    EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
              PatternQueryPolicy::UseAndRun);

    const std::array<std::string_view, 1> values{"value"};
    const std::array<ScalarBuildBatch<std::string_view>, 1> batches{
        ScalarBuildBatch<std::string_view>{values, {}}};
    auto reader = backend.Create(ScalarBuildInput<std::string_view>{batches});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_TRUE(reader->Caps().pattern_match);
    EXPECT_NE(dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                  reader.get()),
              nullptr);
    EXPECT_NE(dynamic_cast<const IPatternMatchReader*>(reader.get()), nullptr);
}

TEST(BitmapIndexReaderTest, NullablePackedWordBoundaryKeepsFinalAllocation) {
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        const auto& backend = ScalarReaderBackends().Get<int32_t>(
            mmap ? "BitmapInt32Mmap" : "BitmapInt32");
        std::array<int64_t, 2> resident{};
        for (const size_t rows : {size_t{64}, size_t{65}}) {
            ScalarTestData<int32_t> data(
                std::vector<int32_t>(rows, int32_t{7}));
            data.validity.reset(0);
            const ScalarTestInput<int32_t> input(data);
            auto artifact = backend.Build(input.View(), {.row_count = rows});
            auto storage =
                OpenV3Storage<BitmapIndexLoader, int32_t, TargetBitmap>(
                    backend, SerializeV3(*artifact), {.row_count = rows});
            resident[rows == 64 ? 0 : 1] = storage->CellByteSize().memory_bytes;
            auto index = CreateBitmapIndex(std::move(storage));
            auto reader = index->CreateReader(nullptr);
            ASSERT_NE(reader, nullptr);
            const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<int32_t>*>(
                    reader.get());
            ASSERT_NE(nulls, nullptr);
            ASSERT_NE(predicate, nullptr);
            ExpectHits(nulls->IsNull(), rows, {0});
            const int32_t key = 7;
            EXPECT_EQ(predicate->In(1, &key).count(), rows - 1);
        }
        // The 65th row expands one validity word and one dense posting word.
        // Retaining a second validity staging buffer would add another word.
        EXPECT_EQ(resident[1] - resident[0], 2 * sizeof(uint64_t));
    }
}

}  // namespace
}  // namespace milvus::index::test
