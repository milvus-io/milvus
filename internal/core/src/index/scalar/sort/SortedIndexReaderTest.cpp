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

#include "index/scalar/sort/SortedIndexLoader.h"
#include "index/test_utils/StorageTestUtils.h"

#include <array>
#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "index/contracts/query/IPatternMatchReader.h"
#include "index/scalar/sort/SortedIndexReader.h"
#include "index/SingleCellIndexStorageTranslator.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

TEST(SortedIndexCacheTest, OperationsSharePinUntilReaderDestruction) {
    const cachinglayer::ResourceUsage limit{16 * 1024 * 1024, 0};
    auto dlist = std::make_shared<cachinglayer::internal::DList>(
        false, limit, limit, limit, cachinglayer::EvictionConfig{});
    std::vector<int32_t> priorities;
    auto load = [&](OpContext* ctx)
        -> folly::coro::Task<
            std::unique_ptr<SortedIndexStorage<std::string_view>>> {
        priorities.push_back(ctx->runtime_load_priority.value_or(-1));
        auto layout = SortedStringLayout::FromHeap({"apple"}, {{0}}, 2);
        auto offsets = std::make_shared<const std::vector<int32_t>>(
            std::initializer_list<int32_t>{0, -1});
        auto validity = std::make_shared<TargetBitmap>(2, false);
        validity->set(0);
        SortedIndexStorage<std::string_view>::OpenArgs args;
        args.state.valid_bitset = std::move(validity);
        args.state.idx_to_offsets = offsets->data();
        args.state.idx_to_offsets_size = offsets->size();
        args.state.idx_to_offsets_owner = std::move(offsets);
        args.state.total_num_rows = 2;
        args.storage.layout = std::move(layout);
        co_return std::make_unique<SortedIndexStorage<std::string_view>>(
            std::move(args));
    };
    IndexCacheConfig cache{
        "sorted-storage-test",
        cachinglayer::Meta{cachinglayer::StorageType::MEMORY,
                           cachinglayer::CellIdMappingMode::IDENTICAL,
                           cachinglayer::CellDataType::OTHER,
                           CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                           true},
        {1024 * 1024, 0},
        {},
        128};
    std::weak_ptr<cachinglayer::CacheSlot<SortedIndexStorage<std::string_view>>>
        slot_lifetime;
    auto make_index = [&] {
        auto slot = std::make_shared<
            cachinglayer::CacheSlot<SortedIndexStorage<std::string_view>>>(
            std::make_unique<SingleCellIndexStorageTranslator<
                SortedIndexStorage<std::string_view>>>(cache, false, load),
            dlist.get(),
            true,
            true,
            true,
            std::chrono::seconds(5),
            std::chrono::seconds(5));
        slot_lifetime = slot;
        return std::make_shared<SortedIndex<std::string_view>>(
            slot,
            IndexMetadata{ReaderCaps{.predicate = true,
                                     .pattern_match = true,
                                     .value_lookup = true,
                                     .cheap_value_lookup = true},
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
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
            first.get());
    ASSERT_NE(predicate, nullptr);
    const std::string_view key = "apple";
    EXPECT_EQ(predicate->Range(key, CompareOp::Equal).count(), 1);
    const auto scanned_bytes =
        first_ctx.storage_usage.scanned_total_bytes.load();
    ASSERT_GT(scanned_bytes, 0);
    EXPECT_EQ(predicate->NotIn(1, &key).count(), 0);
    const int64_t rows[] = {0, 1};
    values->Gather(
        rows, 2, [&](int64_t i, const std::string_view* value, bool valid) {
            EXPECT_EQ(valid, i == 0);
            if (valid)
                EXPECT_EQ(*value, "apple");
        });
    EXPECT_EQ(priorities, (std::vector<int32_t>{0}));
    EXPECT_EQ(values->Lookup(0), "apple");
    EXPECT_FALSE(values->Lookup(1).has_value());
    EXPECT_EQ(values->Lookup(0), "apple");
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

TEST(SortedIndexOwnershipTest, ReadersShareLayoutAndPreserveNulls) {
    auto layout = SortedStringLayout::FromHeap({"apple"}, {{0}}, 2);
    std::weak_ptr<const SortedStringLayout> lifetime = layout;
    auto offsets = std::make_shared<const std::vector<int32_t>>(
        std::initializer_list<int32_t>{0, -1});
    auto validity = std::make_shared<TargetBitmap>(2, false);
    validity->set(0);
    SortedIndexStorage<std::string_view>::OpenArgs args;
    args.state.valid_bitset = std::move(validity);
    args.state.idx_to_offsets = offsets->data();
    args.state.idx_to_offsets_size = offsets->size();
    args.state.idx_to_offsets_owner = std::move(offsets);
    args.state.total_num_rows = 2;
    args.storage.layout = std::move(layout);
    auto index =
        std::make_shared<SortedIndex<std::string_view>>(std::move(args));
    auto first = index->CreateReader(nullptr);
    auto second = index->CreateReader(nullptr);
    EXPECT_NE(first.get(), second.get());
    index.reset();
    first.reset();
    EXPECT_FALSE(lifetime.expired());
    const auto* predicates =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
            second.get());
    ASSERT_NE(predicates, nullptr);
    const std::string_view value = "missing";
    const auto hits = predicates->NotIn(1, &value);
    ASSERT_EQ(hits.size(), 2);
    EXPECT_TRUE(hits[0]);
    EXPECT_FALSE(hits[1]);
    second.reset();
    EXPECT_TRUE(lifetime.expired());
}

template <typename T>
void
ExpectPredicateProfiles(std::string_view type_name) {
    for (const auto suffix : {std::string_view{},
                              std::string_view{"Mmap"},
                              std::string_view{"NonNull"},
                              std::string_view{"NonNullMmap"}}) {
        const auto name =
            "Sorted" + std::string(type_name) + std::string(suffix);
        const auto& backend = ScalarReaderBackends().Get<T>(name);
        EXPECT_TRUE(backend.DeriveCaps().predicate) << name;
    }
}

TEST(SortedIndexReaderTest, AdvertisesPrimitivePredicateMatrix) {
    ExpectPredicateProfiles<bool>("Bool");
    ExpectPredicateProfiles<int8_t>("Int8");
    ExpectPredicateProfiles<int16_t>("Int16");
    ExpectPredicateProfiles<int32_t>("Int32");
    ExpectPredicateProfiles<int64_t>("Int64");
    ExpectPredicateProfiles<float>("Float");
    ExpectPredicateProfiles<double>("Double");
    ExpectPredicateProfiles<std::string_view>("Varchar");
}

TEST(SortedIndexReaderTest, ProvidesInt64Queries) {
    // This promise is checked without capability filtering, so a regression
    // cannot silently remove this backend from the shared query suites.
    const auto& backend = ScalarReaderBackends().Get<int64_t>("SortedInt64");
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

TEST(SortedIndexReaderTest, ProvidesVarcharQueries) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarchar");
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

TEST(SortedIndexReaderTest, MmapAccountsValidityWordOnce) {
    constexpr size_t rows = 65;
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64Mmap");
    std::vector<int64_t> values(rows);
    for (size_t row = 0; row < rows; ++row)
        values[row] = static_cast<int64_t>(row);
    ScalarTestData<int64_t> data(std::move(values));
    data.validity.reset(0);
    const ScalarTestInput<int64_t> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = rows});
    auto storage = OpenV3Storage<SortedIndexLoader, int64_t>(
        backend, SerializeV3(*artifact), {.row_count = rows});
    const auto usage = storage->CellByteSize();
    EXPECT_EQ(usage.memory_bytes,
              static_cast<int64_t>(TargetBitmap(rows).size_in_bytes()));
    EXPECT_GT(usage.file_bytes, 0);
    auto index = std::make_shared<SortedIndex<int64_t>>(std::move(storage));
    auto reader = index->CreateReader(nullptr);
    const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    ExpectHits(nulls->IsNull(), rows, {0});
}

template <typename T>
void
ExpectMembershipAfterReload(const std::string& name,
                            const std::vector<ScalarTestValue<T>>& rows,
                            const std::vector<T>& terms) {
    for (const auto& suffix : {std::string{}, std::string{"Mmap"}}) {
        const auto& backend = ScalarReaderBackends().Get<T>(name + suffix);
        for (int null_mode : {0, 1, 2}) {
            SCOPED_TRACE(name + suffix +
                         ":null_mode=" + std::to_string(null_mode));
            ScalarTestData<T> data(rows);
            for (size_t row = 0; row < rows.size(); ++row) {
                if (null_mode == 2 || (null_mode == 1 && row % 3 == 0)) {
                    data.validity.reset(row);
                }
            }
            const ScalarTestInput<T> input(data);
            auto artifact = backend.Build(input.View());
            auto reader = OpenV3(
                backend, SerializeV3(*artifact), {.row_count = rows.size()});
            ASSERT_NE(reader, nullptr);
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<T>*>(reader.get());
            ASSERT_NE(predicate, nullptr);
            for (size_t count : {0, 1, 127, 128, 129}) {
                std::vector<T> queries;
                for (size_t i = 0; i < count; ++i) {
                    queries.push_back(terms[i % terms.size()]);
                }
                for (int order : {0, 1, 2}) {
                    if (order == 1) {
                        std::sort(queries.begin(), queries.end());
                    }
                    if (order == 2) {
                        std::reverse(queries.begin(), queries.end());
                    }
                    const auto original = queries;
                    const auto in =
                        predicate->In(queries.size(), queries.data());
                    const auto not_in =
                        predicate->NotIn(queries.size(), queries.data());
                    ASSERT_EQ(in.size(), rows.size());
                    ASSERT_EQ(not_in.size(), rows.size());
                    for (size_t row = 0; row < rows.size(); ++row) {
                        const bool hit = std::find(queries.begin(),
                                                   queries.end(),
                                                   rows[row]) != queries.end();
                        EXPECT_EQ(in[row], data.validity[row] && hit) << row;
                        EXPECT_EQ(not_in[row], data.validity[row] && !hit)
                            << row;
                    }
                    EXPECT_EQ(queries, original);
                }
            }
        }
    }
}

TEST(SortedIndexReaderTest, MembershipMatchesScanAfterHeapAndMmapReload) {
    std::vector<int64_t> integers;
    std::vector<double> reals;
    std::vector<std::string> strings;
    const std::vector<std::string> dictionary{"",
                                              "alpha",
                                              std::string("alpha\0beta", 10),
                                              "中文",
                                              std::string(256, 'x')};
    for (size_t row = 0; row < 129; ++row) {
        integers.push_back(static_cast<int64_t>(row % 17) - 8);
        reals.push_back(row % 5 == 0 ? -0.0
                                     : static_cast<double>(row % 17) - 8.25);
        strings.push_back(dictionary[row % dictionary.size()]);
    }
    ExpectMembershipAfterReload<int64_t>(
        "SortedInt64", integers, {7, -8, 99, 7, 0});
    ExpectMembershipAfterReload<double>(
        "SortedDouble", reals, {0.0, -8.25, 99.0, -0.0, 7.75});
    ExpectMembershipAfterReload<std::string_view>(
        "SortedVarchar",
        strings,
        {dictionary[4], dictionary[2], "missing", "", dictionary[2]});
}

}  // namespace
}  // namespace milvus::index::test
