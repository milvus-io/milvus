// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the
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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <tuple>
#include <utility>
#include <vector>

#include "index/Families.h"
#include "index/Meta.h"
#include "index/SingleCellIndexStorageTranslator.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/Utils.h"
#include "index/scalar/json/JsonFlatIndexReader.h"
#include "index/scalar/json/JsonPathIndexReader.h"
#include "index/scalar/json/JsonProjectedIndexArtifact.h"
#include "index/scalar/bitmap/BitmapIndexReader.h"
#include "storage/artifact/LocalDirectory.h"
#include "tantivy-wrapper.h"
#include "test_utils/Constants.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/CaseTestDriver.h"

namespace milvus::index::test {
void
CheckCachedJsonFlat() {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("JsonFlatV7");
    ScalarTestData<std::string_view> data({R"({"flag":false})",
                                           R"({"flag":true})",
                                           R"({"flag":true})",
                                           R"({"flag":false})"});
    data.validity.reset(3);
    const ScalarTestInput<std::string_view> input(data);
    auto artifact =
        backend.Build(input.View(), {.row_count = data.values.size()});
    for (const bool known_count : {false, true}) {
        for (const bool mmap : {false, true}) {
            for (const bool packed : {false, true}) {
                for (const bool async : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << packed << async << mmap << known_count);
                    auto staging = storage::LocalDirectory::CreateOwned(
                        std::filesystem::temp_directory_path().string(),
                        "jsonflat-cache-XXXXXX",
                        "JsonFlat cache test");
                    const auto persisted = packed ? SerializeV3(*artifact)
                                                  : SerializeV1V2(*artifact);
                    auto source = [&]() -> OpenedIndexSource {
                        if (!packed) {
                            return LegacyIndexSource{
                                std::make_shared<TestArtifactSource>(
                                    persisted, storage::Generation::V1V2),
                                async};
                        }
                        auto input =
                            std::make_shared<storage::RemoteInputStream>(
                                std::make_shared<arrow::io::BufferReader>(
                                    MakePackedArtifactBuffer(persisted)));
                        if (async) {
                            return PackedIndexSource{std::shared_ptr<
                                storage::AsyncIndexEntryReader>(
                                folly::coro::blockingWait(
                                    folly::coro::co_withExecutor(
                                        storage::ResolveAsyncLoadExecutor(
                                            {},
                                            proto::common::LoadPriority::HIGH),
                                        storage::AsyncIndexEntryReader::Open(
                                            input,
                                            0,
                                            proto::common::LoadPriority::
                                                HIGH))))};
                        }
                        return PackedIndexSource{
                            std::shared_ptr<storage::IndexEntryReader>(
                                storage::IndexEntryReader::Open(
                                    input, input->Size()))};
                    }();
                    storage::LoadOptions options;
                    options.params = backend.LoadParams({.row_count = 4});
                    options.enable_mmap = mmap;
                    if (known_count)
                        options.params[INDEX_NUM_ROWS_KEY] = 4;
                    else
                        options.params.erase(INDEX_NUM_ROWS_KEY);
                    options.mmap_dir_path = staging->Path();
                    IndexCacheConfig cache{
                        staging->Path(),
                        cachinglayer::Meta{
                            mmap ? cachinglayer::StorageType::DISK
                                 : cachinglayer::StorageType::MEMORY,
                            cachinglayer::CellIdMappingMode::IDENTICAL,
                            cachinglayer::CellDataType::OTHER,
                            CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                            true},
                        {1024 * 1024, 1024 * 1024},
                        {},
                        4096};
                    auto index = LoaderRegistry::Instance()
                                     .Lookup(backend.Family())
                                     .Load({std::move(source), options, cache});
                    EXPECT_EQ(index->Metadata().count.has_value(), known_count);
                    folly::CancellationSource cancellation;
                    cancellation.requestCancellation();
                    OpContext cancelled(cancellation.getToken());
                    auto reader = index->CreateReader(&cancelled);
                    if (known_count)
                        EXPECT_EQ(reader->Count(), 4);
                    else
                        ExpectSegcoreError(ErrorCode::FollyCancel,
                                           [&] { (void)reader->Count(); });
                    EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
                    const auto* nulls =
                        dynamic_cast<const INullReader*>(reader.get());
                    ASSERT_NE(nulls, nullptr);
                    ExpectSegcoreError(ErrorCode::FollyCancel,
                                       [&] { (void)nulls->IsNull(); });
                    index->CancelWarmup();
                    OpContext retry;
                    reader = index->CreateReader(&retry);
                    index.reset();
                    EXPECT_EQ(reader->Count(), 4);
                    nulls = dynamic_cast<const INullReader*>(reader.get());
                    ASSERT_NE(nulls, nullptr);
                    const auto result = nulls->IsNull();
                    EXPECT_EQ(result.count(), 1);
                    EXPECT_TRUE(result[3]);
                    const auto* json =
                        dynamic_cast<const IJsonIndexReader*>(reader.get());
                    ASSERT_NE(json, nullptr);
                    auto path = json->Resolve("/flag",
                                              JsonCastType::FromString("BOOL"));
                    const auto* predicate =
                        dynamic_cast<const IScalarPredicateReader<bool>*>(
                            path.get());
                    ASSERT_NE(predicate, nullptr);
                    const bool value = true;
                    ExpectBitmap(predicate->In(1, &value), Hits(4, {1, 2}));
                    ExpectBitmap(predicate->NotIn(1, &value), Hits(4, {0}));
                }
            }
        }
    }
}

void
CheckCachedStringProjection(const char* backend_name,
                            bool corrupt = false,
                            bool roaring = false) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>(backend_name);
    std::vector<std::string> values{"apple", "pear", "", ""};
    const size_t row_count = roaring ? 600 : 4;
    values.reserve(row_count);
    for (size_t i = values.size(); i < row_count; ++i) {
        values.push_back("value-" + std::to_string(i));
    }
    ScalarTestData<std::string_view> data(std::move(values));
    data.validity.reset(2);
    data.validity.reset(3);
    const ScalarTestInput<std::string_view> input(data);
    auto artifact = std::make_unique<JsonProjectedIndexArtifact>(
        backend.Build(input.View(), {.row_count = row_count}),
        "/a",
        JsonCastType::FromString("VARCHAR"),
        row_count,
        std::vector<size_t>{3},
        true);
    for (const bool known_count : {false, true}) {
        for (const bool mmap : {false, true}) {
            for (const bool packed : {false, true}) {
                for (const bool async : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << packed << async << mmap << known_count);
                    auto staging = storage::LocalDirectory::CreateOwned(
                        std::filesystem::temp_directory_path().string(),
                        "json-marisa-cache-XXXXXX",
                        "JSON Marisa cache test");
                    auto persisted = packed ? SerializeV3(*artifact)
                                            : SerializeV1V2(*artifact);
                    if (corrupt) {
                        const size_t bad_offset = row_count;
                        auto& bytes = persisted.entries.at(
                            std::string(INDEX_NON_EXIST_OFFSET_FILE_NAME));
                        std::memcpy(
                            bytes.data(), &bad_offset, sizeof(bad_offset));
                    }
                    auto source = [&]() -> OpenedIndexSource {
                        if (!packed) {
                            return LegacyIndexSource{
                                std::make_shared<TestArtifactSource>(
                                    persisted, storage::Generation::V1V2),
                                async};
                        }
                        auto input =
                            std::make_shared<storage::RemoteInputStream>(
                                std::make_shared<arrow::io::BufferReader>(
                                    MakePackedArtifactBuffer(persisted)));
                        if (async) {
                            return PackedIndexSource{std::shared_ptr<
                                storage::AsyncIndexEntryReader>(
                                folly::coro::blockingWait(
                                    folly::coro::co_withExecutor(
                                        storage::ResolveAsyncLoadExecutor(
                                            {},
                                            proto::common::LoadPriority::HIGH),
                                        storage::AsyncIndexEntryReader::Open(
                                            input,
                                            0,
                                            proto::common::LoadPriority::
                                                HIGH))))};
                        }
                        return PackedIndexSource{
                            std::shared_ptr<storage::IndexEntryReader>(
                                storage::IndexEntryReader::Open(
                                    input, input->Size()))};
                    }();
                    storage::LoadOptions options;
                    options.params =
                        backend.LoadParams({.row_count = row_count});
                    options.params["field_type"] = DataType::JSON;
                    options.params[INDEX_TYPE] =
                        backend.Family() == families::kMarisa
                            ? HYBRID_INDEX_TYPE
                        : backend.Family() == families::kNgram
                            ? NGRAM_INDEX_TYPE
                        : backend.Family() == families::kSort ? ASCENDING_SORT
                        : backend.Family() == families::kBitmap
                            ? BITMAP_INDEX_TYPE
                            : INVERTED_INDEX_TYPE;
                    options.params[JSON_PATH] = "/a";
                    options.params[JSON_CAST_TYPE] = "VARCHAR";
                    options.enable_mmap = mmap;
                    if (known_count)
                        options.params[INDEX_NUM_ROWS_KEY] = row_count;
                    else
                        options.params.erase(INDEX_NUM_ROWS_KEY);
                    options.mmap_dir_path = staging->Path();
                    if (auto* legacy =
                            std::get_if<LegacyIndexSource>(&source)) {
                        options.params = AnnotateJsonProjectionCompleteness(
                            std::move(options.params), *legacy->source);
                    } else {
                        const auto& packed_source =
                            std::get<PackedIndexSource>(source);
                        options.params = AnnotateJsonProjectionCompleteness(
                            std::move(options.params),
                            packed_source.Directory(),
                            packed_source.Metadata());
                    }
                    IndexCacheConfig cache{
                        staging->Path(),
                        cachinglayer::Meta{
                            mmap ? cachinglayer::StorageType::DISK
                                 : cachinglayer::StorageType::MEMORY,
                            cachinglayer::CellIdMappingMode::IDENTICAL,
                            cachinglayer::CellDataType::OTHER,
                            CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                            true},
                        {1024 * 1024, 1024 * 1024},
                        {},
                        4096};
                    if (corrupt) {
                        cache.meta.cache_warmup_policy =
                            CacheWarmupPolicy::CacheWarmupPolicy_Sync;
                        ExpectSegcoreError(ErrorCode::UnexpectedError, [&] {
                            (void)LoaderRegistry::Instance()
                                .Lookup(backend.Family())
                                .Load({std::move(source), options, cache});
                        });
                        EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
                        continue;
                    }
                    auto index = LoaderRegistry::Instance()
                                     .Lookup(backend.Family())
                                     .Load({std::move(source), options, cache});
                    const bool count_known =
                        known_count || backend.Family() == families::kSort ||
                        backend.Family() == families::kBitmap ||
                        (packed && backend.Family() == families::kMarisa);
                    EXPECT_EQ(index->Metadata().count.has_value(), count_known);
                    folly::CancellationSource cancellation;
                    cancellation.requestCancellation();
                    OpContext cancelled(cancellation.getToken());
                    auto reader = index->CreateReader(&cancelled);
                    if (count_known)
                        EXPECT_EQ(reader->Count(), row_count);
                    else
                        ExpectSegcoreError(ErrorCode::FollyCancel,
                                           [&] { (void)reader->Count(); });
                    EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
                    const auto* json =
                        dynamic_cast<const IJsonIndexReader*>(reader.get());
                    ASSERT_NE(json, nullptr);
                    EXPECT_TRUE(json->Resolve(
                        "/a", JsonCastType::FromString("VARCHAR")));
                    EXPECT_FALSE(json->Resolve(
                        "/wrong", JsonCastType::FromString("VARCHAR")));
                    ExpectSegcoreError(ErrorCode::FollyCancel,
                                       [&] { (void)json->Exists("/a"); });
                    index->CancelWarmup();
                    OpContext retry;
                    reader = index->CreateReader(&retry);
                    index.reset();
                    EXPECT_EQ(reader->Count(), row_count);
                    json = dynamic_cast<const IJsonIndexReader*>(reader.get());
                    ASSERT_NE(json, nullptr);
                    TargetBitmap expected_exists(row_count, true);
                    expected_exists.reset(3);
                    ExpectBitmap(json->Exists("/a"), expected_exists);
                    auto path = json->Resolve(
                        "/a", JsonCastType::FromString("VARCHAR"));
                    if (backend.Family() == families::kNgram) {
                        const auto* ngram =
                            dynamic_cast<const INgramReader*>(path.get());
                        ASSERT_NE(ngram, nullptr);
                        TargetBitmap candidates(row_count, true);
                        ngram->Candidates(
                            "app", PatternOp::InnerMatch, candidates);
                        ExpectBitmap(candidates, Hits(row_count, {0}));
                    } else {
                        const auto* predicate = dynamic_cast<
                            const IScalarPredicateReader<std::string_view>*>(
                            path.get());
                        ASSERT_NE(predicate, nullptr);
                        const std::string_view value = "apple";
                        ExpectBitmap(predicate->In(1, &value),
                                     Hits(row_count, {0}));
                        TargetBitmap expected_not_in(row_count, true);
                        for (const auto row : {0, 2, 3})
                            expected_not_in.reset(row);
                        ExpectBitmap(predicate->NotIn(1, &value),
                                     expected_not_in);
                    }
                }
            }
        }
    }
}

TEST(JsonPathIndexCacheTest, MarisaStorageLoadsExistenceWithRequestContext) {
    CheckCachedStringProjection("MarisaVarchar");
}

TEST(JsonPathIndexCacheTest,
     MarisaWarmupRejectsInvalidExistenceBeforePublication) {
    CheckCachedStringProjection("MarisaVarchar", true);
}

TEST(JsonPathIndexCacheTest, NgramStorageLoadsExistenceWithRequestContext) {
    CheckCachedStringProjection("NgramVarcharMin2Max4Heap");
}
TEST(JsonPathIndexCacheTest,
     NgramWarmupRejectsInvalidExistenceBeforePublication) {
    CheckCachedStringProjection("NgramVarcharMin2Max4Heap", true);
}

TEST(JsonPathIndexCacheTest, InvertedStorageLoadsExistenceWithRequestContext) {
    CheckCachedStringProjection("InvertedVarchar");
}
TEST(JsonPathIndexCacheTest,
     InvertedWarmupRejectsInvalidExistenceBeforePublication) {
    CheckCachedStringProjection("InvertedVarchar", true);
}

void
CheckCachedArrayProjection(const char* backend_name) {
    const auto& backend = ScalarReaderBackends().Get<ArrayView>(backend_name);
    auto data = ScalarDataSets()
                    .Get<ArrayView>("JsonProjectedArrayVarchar")
                    .make_data();
    const ScalarTestInput<ArrayView> input(data);
    const BackendCaseMetadata metadata{data.values.size(), data.metadata};
    auto artifact = backend.Build(input.View(), metadata);
    for (const bool packed : {false, true}) {
        for (const bool known_count : {false, true}) {
            auto staging = storage::LocalDirectory::CreateOwned(
                std::filesystem::temp_directory_path().string(),
                "json-array-cache-XXXXXX",
                "JSON array cache test");
            const auto persisted =
                packed ? SerializeV3(*artifact) : SerializeV1V2(*artifact);
            auto source = [&]() -> OpenedIndexSource {
                if (!packed) {
                    return LegacyIndexSource{
                        std::make_shared<TestArtifactSource>(
                            persisted, storage::Generation::V1V2),
                        false};
                }
                auto stream = std::make_shared<storage::RemoteInputStream>(
                    std::make_shared<arrow::io::BufferReader>(
                        MakePackedArtifactBuffer(persisted)));
                return PackedIndexSource{
                    std::shared_ptr<storage::IndexEntryReader>(
                        storage::IndexEntryReader::Open(stream,
                                                        stream->Size()))};
            }();
            storage::LoadOptions options;
            options.params = backend.LoadParams(metadata);
            options.mmap_dir_path = staging->Path();
            if (known_count)
                options.params[INDEX_NUM_ROWS_KEY] = 7;
            else
                options.params.erase(INDEX_NUM_ROWS_KEY);
            if (auto* legacy = std::get_if<LegacyIndexSource>(&source)) {
                options.params = AnnotateJsonProjectionCompleteness(
                    std::move(options.params), *legacy->source);
            } else {
                const auto& packed_source = std::get<PackedIndexSource>(source);
                options.params = AnnotateJsonProjectionCompleteness(
                    std::move(options.params),
                    packed_source.Directory(),
                    packed_source.Metadata());
            }
            IndexCacheConfig cache{
                staging->Path(),
                cachinglayer::Meta{cachinglayer::StorageType::MEMORY,
                                   cachinglayer::CellIdMappingMode::IDENTICAL,
                                   cachinglayer::CellDataType::OTHER,
                                   CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                                   true},
                {1024 * 1024, 1024 * 1024},
                {},
                4096};
            const auto family =
                packed ? ResolvePackedLoadFamily(
                             backend.Family(),
                             std::get<PackedIndexSource>(source).Metadata(),
                             options.params)
                       : ResolveLoadFamily(
                             backend.Family(),
                             *std::get<LegacyIndexSource>(source).source);
            if (backend.Family() == families::kHybrid) {
                EXPECT_EQ(family, families::kBitmap);
            }
            auto index = LoaderRegistry::Instance().Lookup(family).Load(
                {std::move(source), options, cache});
            EXPECT_EQ(index->Metadata().count.has_value(),
                      known_count || family == families::kSort ||
                          family == families::kBitmap);
            EXPECT_FALSE(index->Metadata().caps.nested);
            OpContext ctx;
            auto reader = index->CreateReader(&ctx);
            EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
            EXPECT_EQ(reader->Count(), 7);
            const auto* json =
                dynamic_cast<const IJsonIndexReader*>(reader.get());
            ASSERT_NE(json, nullptr);
            ExpectBitmap(json->Exists("/a"), Hits(7, {0, 1, 2, 3}));
            auto path =
                json->Resolve("/a", JsonCastType::FromString("ARRAY_VARCHAR"));
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                    path.get());
            ASSERT_NE(predicate, nullptr);
            const std::string_view key = "alpha";
            ExpectBitmap(predicate->In(1, &key), Hits(7, {0, 3}));
            ExpectBitmap(predicate->NotIn(1, &key), Hits(7, {1, 2}));
        }
    }
}

TEST(JsonPathIndexCacheTest,
     InvertedArrayUsesParentRowsForExistenceAndPredicates) {
    CheckCachedArrayProjection("JsonProjectedInvertedArrayVarchar");
}
TEST(JsonPathIndexCacheTest,
     SortedArrayUsesParentRowsForExistenceAndPredicates) {
    CheckCachedArrayProjection("JsonProjectedSortedArrayVarchar");
}

TEST(JsonPathIndexCacheTest, SortedStorageLoadsExistenceWithRequestContext) {
    CheckCachedStringProjection("SortedVarchar");
}
TEST(JsonPathIndexCacheTest,
     SortedWarmupRejectsInvalidExistenceBeforePublication) {
    CheckCachedStringProjection("SortedVarchar", true);
}

TEST(JsonPathIndexCacheTest, BitmapStorageLoadsExistenceWithRequestContext) {
    for (const bool roaring : {false, true})
        CheckCachedStringProjection("BitmapVarchar", false, roaring);
}
TEST(JsonPathIndexCacheTest,
     BitmapWarmupRejectsInvalidExistenceBeforePublication) {
    for (const bool roaring : {false, true})
        CheckCachedStringProjection("BitmapVarchar", true, roaring);
}
TEST(JsonPathIndexCacheTest,
     BitmapArrayUsesParentRowsForExistenceAndPredicates) {
    CheckCachedArrayProjection("JsonProjectedHybridArrayVarchar");
}

TEST(JsonFlatIndexArtifactTest, CachedStorageLoadsWithRequestContext) {
    CheckCachedJsonFlat();
}

TEST(JsonFlatIndexArtifactTest, WarmupRejectsCountMismatchBeforePublication) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("JsonFlatV7");
    ScalarTestData<std::string_view> data({R"({"flag":false})",
                                           R"({"flag":true})",
                                           R"({"flag":true})",
                                           R"({"flag":false})"});
    data.validity.reset(3);
    const ScalarTestInput<std::string_view> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = 4});
    for (const bool packed : {false, true}) {
        SCOPED_TRACE(packed);
        auto staging = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "jsonflat-bad-count-XXXXXX",
            "jsonflat count test");
        const auto persisted =
            packed ? SerializeV3(*artifact) : SerializeV1V2(*artifact);
        auto source = [&]() -> OpenedIndexSource {
            if (!packed)
                return LegacyIndexSource{
                    std::make_shared<TestArtifactSource>(
                        persisted, storage::Generation::V1V2),
                    false};
            auto input = std::make_shared<storage::RemoteInputStream>(
                std::make_shared<arrow::io::BufferReader>(
                    MakePackedArtifactBuffer(persisted)));
            return PackedIndexSource{std::shared_ptr<storage::IndexEntryReader>(
                storage::IndexEntryReader::Open(input, input->Size()))};
        }();
        storage::LoadOptions options;
        options.params = backend.LoadParams({.row_count = 4});
        options.params[INDEX_NUM_ROWS_KEY] = 3;
        options.enable_mmap = true;
        options.mmap_dir_path = staging->Path();
        IndexCacheConfig cache{
            staging->Path(),
            cachinglayer::Meta{cachinglayer::StorageType::DISK,
                               cachinglayer::CellIdMappingMode::IDENTICAL,
                               cachinglayer::CellDataType::OTHER,
                               CacheWarmupPolicy::CacheWarmupPolicy_Sync,
                               true},
            {1024 * 1024, 1024 * 1024},
            {},
            4096};
        ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
            (void)LoaderRegistry::Instance()
                .Lookup(backend.Family())
                .Load({std::move(source), options, cache});
        });
        EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
    }
}

TEST(JsonFlatIndexCacheTest, FreshSlotUsesEachReadersContext) {
    const cachinglayer::ResourceUsage limit{16 * 1024 * 1024, 0};
    auto dlist = std::make_shared<cachinglayer::internal::DList>(
        false, limit, limit, limit, cachinglayer::EvictionConfig{});
    std::vector<int32_t> priorities;
    auto load = [&](OpContext* ctx)
        -> folly::coro::Task<std::unique_ptr<JsonFlatIndexStorage>> {
        priorities.push_back(ctx->runtime_load_priority.value_or(-1));
        auto directory = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "jsonflat-pin-XXXXXX",
            "jsonflat pin test");
        auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
            "json",
            TantivyDataType::JSON,
            directory->Path().c_str(),
            TANTIVY_INDEX_LATEST_VERSION,
            false,
            false);
        const std::vector<Json> values{
            Json(std::string_view(R"({"flag":true})")),
            Json(std::string_view(R"({"flag":false})"))};
        engine->add_json_data(values.data(), values.size(), 0);
        engine->finish();
        engine->create_reader(SetBitsetSealed);
        co_return JsonFlatIndexStorage::Create(
            std::move(directory),
            std::move(engine),
            "",
            std::make_shared<const std::vector<size_t>>(),
            true,
            0,
            0);
    };
    IndexCacheConfig cache{
        "jsonflat-storage-test",
        cachinglayer::Meta{cachinglayer::StorageType::MEMORY,
                           cachinglayer::CellIdMappingMode::IDENTICAL,
                           cachinglayer::CellDataType::OTHER,
                           CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                           true},
        {1024 * 1024, 0},
        {},
        4096};
    std::weak_ptr<cachinglayer::CacheSlot<JsonFlatIndexStorage>> slot_lifetime;
    auto make_index = [&] {
        auto slot =
            std::make_shared<cachinglayer::CacheSlot<JsonFlatIndexStorage>>(
                std::make_unique<
                    SingleCellIndexStorageTranslator<JsonFlatIndexStorage>>(
                    cache, false, load),
                dlist.get(),
                true,
                true,
                true,
                std::chrono::seconds(5),
                std::chrono::seconds(5));
        slot_lifetime = slot;
        return std::make_shared<JsonFlatIndex>(slot, 2, "");
    };
    auto index = make_index();
    OpContext first_ctx;
    first_ctx.runtime_load_priority = 0;
    OpContext second_ctx;
    second_ctx.runtime_load_priority = 1;
    auto first = index->CreateReader(&first_ctx);
    EXPECT_EQ(first->Count(), 2);
    EXPECT_TRUE(priorities.empty());
    const auto* json = dynamic_cast<const IJsonIndexReader*>(first.get());
    ASSERT_NE(json, nullptr);
    EXPECT_FALSE(json->CastTypesOf("/flag").empty());
    EXPECT_TRUE(priorities.empty());
    auto path = json->Resolve("/flag", JsonCastType::FromString("BOOL"));
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<bool>*>(path.get());
    ASSERT_NE(predicate, nullptr);
    const bool value = true;
    ExpectBitmap(predicate->In(1, &value), Hits(2, {0}));
    const auto scanned = first_ctx.storage_usage.scanned_total_bytes.load();
    ASSERT_GT(scanned, 0);
    EXPECT_EQ(json->Exists("/flag").count(), 2);
    EXPECT_EQ(dynamic_cast<const INullReader*>(first.get())->IsNull().count(),
              0);
    EXPECT_EQ(first_ctx.storage_usage.scanned_total_bytes.load(), scanned);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0}));
    index.reset();
    EXPECT_FALSE(slot_lifetime.expired());
    first.reset();
    EXPECT_FALSE(slot_lifetime.expired());
    path = {};
    EXPECT_TRUE(slot_lifetime.expired());
    index = make_index();
    auto second = index->CreateReader(&second_ctx);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0}));
    const auto* nulls = dynamic_cast<const INullReader*>(second.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->IsNull().count(), 0);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0, 1}));
    EXPECT_GT(second_ctx.storage_usage.scanned_total_bytes.load(), 0);
    index.reset();
    second.reset();
    EXPECT_TRUE(slot_lifetime.expired());
}

TEST(JsonFlatIndexOwnershipTest, ReaderRetainsStorageAfterIndexRelease) {
    auto directory = storage::LocalDirectory::CreateOwned(
        TestLocalPath, "json-owner-XXXXXX", "JSON ownership test");
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        "json",
        TantivyDataType::JSON,
        directory->Path().c_str(),
        TANTIVY_INDEX_LATEST_VERSION,
        false,
        false);
    const std::vector<Json> values{Json(std::string_view(R"({"flag":true})")),
                                   Json(std::string_view(R"({"flag":false})"))};
    engine->add_json_data(values.data(), values.size(), 0);
    engine->finish();
    engine->create_reader(SetBitsetSealed);
    std::shared_ptr<const JsonFlatIndexStorage> state =
        JsonFlatIndexStorage::Create(
            std::move(directory),
            std::move(engine),
            "",
            std::make_shared<const std::vector<size_t>>(),
            true,
            0,
            0);
    std::weak_ptr<const JsonFlatIndexStorage> lifetime = state;
    auto index = std::make_shared<JsonFlatIndex>(std::move(state));
    EXPECT_EQ(index->Metadata().count, 2);
    auto first = index->CreateReader(nullptr);
    auto second = index->CreateReader(nullptr);
    EXPECT_NE(first.get(), second.get());
    index.reset();
    first.reset();
    EXPECT_FALSE(lifetime.expired());
    const auto* json = dynamic_cast<const IJsonIndexReader*>(second.get());
    ASSERT_NE(json, nullptr);
    auto exists = json->Exists("/flag");
    ASSERT_EQ(exists.size(), 2);
    EXPECT_TRUE(exists[0]);
    EXPECT_TRUE(exists[1]);
    second.reset();
    EXPECT_TRUE(lifetime.expired());
}

TEST(JsonPathIndexOwnershipTest, RequestsShareMetadataAndRetainTypedIndex) {
    for (const auto* cast_name : {"BOOL", "ARRAY_BOOL"}) {
        BitmapRoaringPostingMap<bool> postings;
        postings[true].add(0);
        BitmapStorageOptions options;
        options.total_num_rows = 3;
        options.valid_bitset = TargetBitmap(3);
        options.valid_bitset.set(0);
        options.value_type = DataType::BOOL;
        auto child =
            CreateBitmapIndex<bool>(std::move(postings), std::move(options));
        std::weak_ptr<IIndex> child_lifetime = child;
        const auto cast = JsonCastType::FromString(cast_name);
        auto index = std::make_shared<JsonPathIndex>(
            std::move(child),
            JsonProjectedIndexSpec("/flag", cast, 3),
            std::vector<size_t>{1});
        auto first = index->CreateReader(nullptr);
        auto second = index->CreateReader(nullptr);
        const auto* json = dynamic_cast<const IJsonIndexReader*>(second.get());
        ASSERT_NE(json, nullptr);
        index.reset();
        first.reset();
        EXPECT_FALSE(child_lifetime.expired());
        const auto exists = json->Exists("/flag");
        EXPECT_TRUE(exists[0]);
        EXPECT_FALSE(exists[1]);
        EXPECT_TRUE(exists[2]);
        EXPECT_EQ(second->Count(), 3);
        const auto resolved = json->Resolve("/flag", cast);
        const auto* nulls = dynamic_cast<const INullReader*>(resolved.get());
        ASSERT_NE(nulls, nullptr);
        const auto missing_or_null = nulls->IsNull();
        EXPECT_FALSE(missing_or_null[0]);
        EXPECT_TRUE(missing_or_null[1]);
        EXPECT_TRUE(missing_or_null[2]);
        second.reset();
        EXPECT_TRUE(child_lifetime.expired());
    }
}

namespace {

JsonCastType
Cast(std::string_view name) {
    return JsonCastType::FromString(std::string(name));
}

bool
SameCast(JsonCastType left, JsonCastType right) {
    return left.data_type() == right.data_type() &&
           left.element_type() == right.element_type();
}

bool
ContainsCast(const std::vector<JsonCastType>& casts, JsonCastType target) {
    for (const auto cast : casts) {
        if (SameCast(cast, target)) {
            return true;
        }
    }
    return false;
}

DataType
ResolvedValueType(JsonCastType cast) {
    switch (cast.element_type()) {
        case JsonCastType::DataType::BOOL:
            return DataType::BOOL;
        case JsonCastType::DataType::DOUBLE:
            return DataType::DOUBLE;
        case JsonCastType::DataType::VARCHAR:
            return DataType::VARCHAR;
        default:
            return DataType::NONE;
    }
}

void
ExpectResolvedMetadata(const IIndexReaderBase& reader,
                       size_t count,
                       JsonCastType cast) {
    EXPECT_EQ(reader.Count(), count);
    EXPECT_EQ(reader.CoordDomain(), Domain::Row);
    const auto expected_type = ResolvedValueType(cast);
    ASSERT_NE(expected_type, DataType::NONE);
    EXPECT_TRUE(ScalarValueTypesMatch(reader.ValueType(), expected_type));

    const auto caps = reader.Caps();
    EXPECT_EQ(dynamic_cast<const IPatternMatchReader*>(&reader) != nullptr,
              caps.pattern_match);
    EXPECT_EQ(dynamic_cast<const INgramReader*>(&reader) != nullptr,
              caps.ngram_candidates);
    EXPECT_FALSE(caps.text_match);
    EXPECT_FALSE(caps.spatial);
    EXPECT_FALSE(caps.nested);
    EXPECT_FALSE(caps.json_paths);
}

void
ExpectProjectedOuterRoutesOnlyJson(const IIndexReaderBase& reader) {
    EXPECT_EQ(dynamic_cast<const IScalarPredicateReader<bool>*>(&reader),
              nullptr);
    EXPECT_EQ(dynamic_cast<const IScalarPredicateReader<int64_t>*>(&reader),
              nullptr);
    EXPECT_EQ(dynamic_cast<const IScalarPredicateReader<double>*>(&reader),
              nullptr);
    EXPECT_EQ(
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(&reader),
        nullptr);
    EXPECT_EQ(dynamic_cast<const IPatternMatchReader*>(&reader), nullptr);
    EXPECT_EQ(dynamic_cast<const INgramReader*>(&reader), nullptr);
}

void
ExpectProjectedExistsProtocol(const IJsonIndexReader& json) {
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(json.Exists("/wrong")); });
}

const IJsonIndexReader*
JsonReader(const IIndexReaderBasePtr& reader) {
    const auto* json = dynamic_cast<const IJsonIndexReader*>(reader.get());
    if (json == nullptr) {
        ADD_FAILURE()
            << "json_paths reader does not implement IJsonIndexReader";
    }
    return json;
}

template <typename T>
const IScalarPredicateReader<T>*
ResolvedPredicate(const IJsonIndexReader& json,
                  std::string_view path,
                  JsonCastType cast,
                  size_t count,
                  JsonResolvedReader& resolved) {
    resolved = json.Resolve(path, cast);
    if (!resolved) {
        ADD_FAILURE() << "supported JSON path/cast did not resolve";
        return nullptr;
    }
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<T>*>(resolved.get());
    if (predicate == nullptr) {
        ADD_FAILURE() << "resolved JSON reader lacks predicate interface";
        return nullptr;
    }
    ExpectResolvedMetadata(*resolved.get(), count, cast);
    EXPECT_TRUE(resolved->Caps().predicate);
    EXPECT_TRUE(resolved->Caps().exact);
    return predicate;
}

template <typename T>
TargetBitmap
Membership(const IScalarPredicateReader<T>& reader,
           const std::vector<ScalarTestValue<T>>& values,
           bool negate) {
    if constexpr (std::is_same_v<T, bool>) {
        auto copied = std::make_unique<bool[]>(values.size());
        for (size_t i = 0; i < values.size(); ++i) {
            copied[i] = values[i];
        }
        const auto* data = values.empty() ? nullptr : copied.get();
        return negate ? reader.NotIn(values.size(), data)
                      : reader.In(values.size(), data);
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        const auto views = MakeStringViews(values);
        const auto* data = views.empty() ? nullptr : views.data();
        return negate ? reader.NotIn(views.size(), data)
                      : reader.In(views.size(), data);
    } else {
        const auto* data = values.empty() ? nullptr : values.data();
        return negate ? reader.NotIn(values.size(), data)
                      : reader.In(values.size(), data);
    }
}

template <typename InputT, typename Observer>
void
AddJsonCase(IndexTestCases& cases,
            std::string name,
            std::string dataset,
            BackendInputShape input_shape,
            Observer observe,
            bool ReaderCaps::*capability = &ReaderCaps::json_paths,
            std::vector<std::string> backends = {}) {
    cases.Add(IndexTestCase<InputT>{
        .name = std::move(name),
        .dataset = std::move(dataset),
        .input_shape = input_shape,
        .input_lifetime = InputLifetime::ReleaseBeforeBody,
        .backends = std::move(backends),
        .body =
            Observe<InputT>{
                .capability = capability,
                .run = std::move(observe),
            },
    });
}

template <typename InputT, typename QueryT>
void
AddMembershipCase(IndexTestCases& cases,
                  std::string name,
                  std::string dataset,
                  BackendInputShape input_shape,
                  std::string path,
                  JsonCastType cast,
                  std::vector<ScalarTestValue<QueryT>> values,
                  bool negate,
                  std::vector<size_t> expected) {
    AddJsonCase<InputT>(
        cases,
        std::move(name),
        std::move(dataset),
        input_shape,
        [path = std::move(path),
         cast,
         values = std::move(values),
         negate,
         expected = std::move(expected)](
            const auto&, const auto& data, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            JsonResolvedReader resolved;
            const auto* predicate = ResolvedPredicate<QueryT>(
                *json, path, cast, data.values.size(), resolved);
            ASSERT_NE(predicate, nullptr);
            ExpectHits(Membership(*predicate, values, negate),
                       data.values.size(),
                       expected);
        });
}

template <typename InputT, typename QueryT>
void
AddUnaryRangeCase(IndexTestCases& cases,
                  std::string name,
                  std::string dataset,
                  BackendInputShape input_shape,
                  std::string path,
                  JsonCastType cast,
                  ScalarTestValue<QueryT> value,
                  CompareOp op,
                  std::vector<size_t> expected) {
    AddJsonCase<InputT>(
        cases,
        std::move(name),
        std::move(dataset),
        input_shape,
        [path = std::move(path),
         cast,
         value = std::move(value),
         op,
         expected = std::move(expected)](
            const auto&, const auto& data, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            JsonResolvedReader resolved;
            const auto* predicate = ResolvedPredicate<QueryT>(
                *json, path, cast, data.values.size(), resolved);
            ASSERT_NE(predicate, nullptr);
            if constexpr (std::is_same_v<QueryT, std::string_view>) {
                const std::string_view view(value);
                ExpectHits(
                    predicate->Range(view, op), data.values.size(), expected);
            } else {
                ExpectHits(
                    predicate->Range(value, op), data.values.size(), expected);
            }
        });
}

template <typename InputT, typename QueryT>
void
AddIntervalCase(IndexTestCases& cases,
                std::string name,
                std::string dataset,
                BackendInputShape input_shape,
                std::string path,
                JsonCastType cast,
                ScalarTestValue<QueryT> lo,
                bool lo_inc,
                ScalarTestValue<QueryT> hi,
                bool hi_inc,
                std::vector<size_t> expected) {
    AddJsonCase<InputT>(
        cases,
        std::move(name),
        std::move(dataset),
        input_shape,
        [path = std::move(path),
         cast,
         lo = std::move(lo),
         lo_inc,
         hi = std::move(hi),
         hi_inc,
         expected = std::move(expected)](
            const auto&, const auto& data, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            JsonResolvedReader resolved;
            const auto* predicate = ResolvedPredicate<QueryT>(
                *json, path, cast, data.values.size(), resolved);
            ASSERT_NE(predicate, nullptr);
            if constexpr (std::is_same_v<QueryT, std::string_view>) {
                const std::string_view lo_view(lo);
                const std::string_view hi_view(hi);
                ExpectHits(predicate->Range(lo_view, lo_inc, hi_view, hi_inc),
                           data.values.size(),
                           expected);
            } else {
                ExpectHits(predicate->Range(lo, lo_inc, hi, hi_inc),
                           data.values.size(),
                           expected);
            }
        });
}

template <typename InputT>
void
AddPatternCase(IndexTestCases& cases,
               std::string name,
               std::string dataset,
               BackendInputShape input_shape,
               std::string path,
               std::string pattern,
               PatternOp op,
               std::vector<size_t> expected) {
    AddJsonCase<InputT>(
        cases,
        std::move(name),
        std::move(dataset),
        input_shape,
        [path = std::move(path),
         pattern = std::move(pattern),
         op,
         expected = std::move(expected)](
            const auto& backend, const auto& data, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            auto resolved = json->Resolve(path, Cast("VARCHAR"));
            ASSERT_TRUE(resolved);
            ExpectResolvedMetadata(
                *resolved.get(), data.values.size(), Cast("VARCHAR"));
            const auto* matcher =
                dynamic_cast<const IPatternMatchReader*>(resolved.get());
            ASSERT_NE(matcher, nullptr);
            EXPECT_TRUE(resolved->Caps().predicate);
            EXPECT_TRUE(resolved->Caps().pattern_match);
            EXPECT_TRUE(resolved->Caps().exact);
            if (backend.Family() == families::kJsonFlat &&
                op == PatternOp::RegexMatch) {
                EXPECT_FALSE(matcher->ShouldUseForOp(op, pattern));
                ExpectSegcoreError(Unsupported, [&] {
                    static_cast<void>(matcher->PatternMatch(pattern, op));
                });
                return;
            }
            ExpectHits(matcher->PatternMatch(pattern, op),
                       data.values.size(),
                       expected);
        },
        &ReaderCaps::json_paths);
}

template <typename InputT>
void
AddExistsCase(IndexTestCases& cases,
              std::string name,
              std::string dataset,
              BackendInputShape input_shape,
              std::string path,
              std::vector<size_t> expected) {
    AddJsonCase<InputT>(
        cases,
        std::move(name),
        std::move(dataset),
        input_shape,
        [path = std::move(path), expected = std::move(expected)](
            const auto&, const auto& data, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            ExpectHits(json->Exists(path), data.values.size(), expected);
        });
}

template <typename InputT>
void
AddResolvedNullCase(IndexTestCases& cases,
                    std::string name,
                    std::string dataset,
                    BackendInputShape input_shape,
                    std::string path,
                    JsonCastType cast,
                    std::vector<size_t> expected_not_null) {
    AddJsonCase<InputT>(
        cases,
        std::move(name),
        std::move(dataset),
        input_shape,
        [path = std::move(path),
         cast,
         expected_not_null = std::move(expected_not_null)](
            const auto&, const auto& data, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            auto resolved = json->Resolve(path, cast);
            ASSERT_TRUE(resolved);
            ExpectResolvedMetadata(*resolved.get(), data.values.size(), cast);
            const auto* nulls =
                dynamic_cast<const INullReader*>(resolved.get());
            ASSERT_NE(nulls, nullptr);
            EXPECT_TRUE(resolved->Caps().predicate);
            EXPECT_TRUE(resolved->Caps().exact);
            auto not_null = nulls->IsNotNull();
            ExpectHits(not_null, data.values.size(), expected_not_null);
            ASSERT_EQ(not_null.size(), data.values.size());
            TargetBitmap expected_null(data.values.size(), false);
            for (size_t i = 0; i < not_null.size(); ++i) {
                if (!not_null[i]) {
                    expected_null.set(i);
                }
            }
            auto actual_null = nulls->IsNull();
            EXPECT_TRUE(actual_null == expected_null);
        });
}

void
AddJsonFlatRoutingCases(IndexTestCases& cases) {
    constexpr auto shape = BackendInputShape::JsonDocument;
    AddExistsCase<std::string_view>(cases,
                                    "PreferredNameExists",
                                    "JsonEmployees",
                                    shape,
                                    "/profile/name/preferred_name",
                                    {0});
    AddExistsCase<std::string_view>(cases,
                                    "ObjectSubpathExists",
                                    "JsonTypeFamilies",
                                    shape,
                                    "/a",
                                    {0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 13});
    AddExistsCase<std::string_view>(
        cases, "SupportedPathWithNoValues", "JsonAllMissing", shape, "/a", {});
    AddJsonCase<std::string_view>(
        cases,
        "AbsentValuesDoNotMakeSupportedPathUnresolvable",
        "JsonAllMissing",
        shape,
        [](const auto&, const auto& data, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            const auto casts = json->CastTypesOf("/a");
            ASSERT_FALSE(casts.empty());
            for (const auto cast : casts) {
                auto resolved = json->Resolve("/a", cast);
                ASSERT_TRUE(resolved);
                EXPECT_EQ(resolved->Count(), data.values.size());
            }
            ExpectHits(json->Exists("/a"), data.values.size(), {});
        });
    AddJsonCase<std::string_view>(
        cases,
        "RepeatedResolutionKeepsEarlierHandleUsable",
        "JsonEmployees",
        shape,
        [](const auto&, const auto& data, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            const auto cast = Cast("VARCHAR");
            JsonResolvedReader first;
            const auto* first_predicate = ResolvedPredicate<std::string_view>(
                *json, "/profile/name/first", cast, data.values.size(), first);
            ASSERT_NE(first_predicate, nullptr);
            JsonResolvedReader second;
            const auto* second_predicate = ResolvedPredicate<std::string_view>(
                *json, "/profile/name/first", cast, data.values.size(), second);
            ASSERT_NE(second_predicate, nullptr);

            ExpectHits(Membership(*first_predicate, {"Alice"}, false),
                       data.values.size(),
                       {0});
            ExpectHits(Membership(*second_predicate, {"Bob"}, false),
                       data.values.size(),
                       {1});
            first = std::move(second);
            EXPECT_FALSE(second);
            ASSERT_TRUE(first);
            const auto* moved_predicate =
                dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                    first.get());
            ASSERT_NE(moved_predicate, nullptr);
            ExpectHits(Membership(*moved_predicate, {"Alice"}, false),
                       data.values.size(),
                       {0});
        });

    AddJsonCase<std::string_view>(
        cases,
        "FieldNullIsIndependentOfJsonNullAndMissing",
        "JsonFieldNullable",
        shape,
        [](const auto&, const auto& data, auto& reader) {
            const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
            ASSERT_NE(nulls, nullptr);
            ExpectHits(nulls->IsNull(), data.values.size(), {1});
            ExpectHits(nulls->IsNotNull(), data.values.size(), {0, 2, 3, 4});
        });
    AddResolvedNullCase<std::string_view>(cases,
                                          "ResolvedNullMeansNoComparableString",
                                          "JsonFieldNullable",
                                          shape,
                                          "/a",
                                          Cast("VARCHAR"),
                                          {0, 2});
    AddResolvedNullCase<std::string_view>(cases,
                                          "ResolvedNumericNullMask",
                                          "JsonTypeFamilies",
                                          shape,
                                          "/a",
                                          Cast("DOUBLE"),
                                          {0, 1, 4});
    AddResolvedNullCase<std::string_view>(cases,
                                          "ResolvedBoolNullMask",
                                          "JsonTypeFamilies",
                                          shape,
                                          "/a",
                                          Cast("BOOL"),
                                          {3, 6});

    AddJsonCase<std::string_view>(
        cases,
        "CastVocabularyAdvertisesOnlyResolvableReaders",
        "JsonTypeFamilies",
        shape,
        [](const auto&, const auto&, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            const auto casts = json->CastTypesOf("/a");
            ASSERT_FALSE(casts.empty());
            for (const auto cast : casts) {
                EXPECT_TRUE(json->Resolve("/a", cast));
            }
        });
    AddJsonCase<std::string_view>(
        cases,
        "ResolvableReadersAreInCastVocabulary",
        "JsonTypeFamilies",
        shape,
        [](const auto&, const auto&, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            const auto casts = json->CastTypesOf("/a");
            for (const auto cast : {Cast("BOOL"),
                                    Cast("DOUBLE"),
                                    Cast("VARCHAR"),
                                    Cast("ARRAY_BOOL"),
                                    Cast("ARRAY_DOUBLE"),
                                    Cast("ARRAY_VARCHAR")}) {
                ASSERT_TRUE(json->Resolve("/a", cast));
                EXPECT_TRUE(ContainsCast(casts, cast));
            }
        });
    AddJsonCase<std::string_view>(
        cases,
        "UnknownCastDoesNotResolve",
        "JsonTypeFamilies",
        shape,
        [](const auto&, const auto&, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            EXPECT_FALSE(json->Resolve("/a", JsonCastType::UNKNOWN));
        });
    AddJsonCase<std::string_view>(
        cases,
        "NumericArrayPositionIsUnsupported",
        "JsonEscapedPaths",
        shape,
        [](const auto&, const auto&, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            EXPECT_TRUE(json->CastTypesOf("/arr/0").empty());
            EXPECT_FALSE(json->Resolve("/arr/0", Cast("VARCHAR")));
        });
    AddJsonCase<std::string_view>(
        cases,
        "MalformedPointerIsRejected",
        "JsonEscapedPaths",
        shape,
        [](const auto&, const auto&, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            try {
                static_cast<void>(json->CastTypesOf("not/a/pointer"));
                ADD_FAILURE() << "malformed JSON pointer was accepted";
            } catch (const SegcoreError& error) {
                EXPECT_EQ(error.get_error_code(), ErrorCode::DataTypeInvalid);
            }
        });
    AddJsonCase<std::string_view>(
        cases,
        "RootPrefixAcceptsExactSubpathAndRejectsSibling",
        "JsonRootPrefix",
        shape,
        [](const auto&, const auto&, auto& reader) {
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            EXPECT_FALSE(json->CastTypesOf("/profile/name").empty());
            EXPECT_TRUE(json->CastTypesOf("/profiled/name").empty());
            EXPECT_TRUE(json->CastTypesOf("/other").empty());
            EXPECT_TRUE(json->Resolve("/profile/name", Cast("VARCHAR")));
            EXPECT_FALSE(json->Resolve("/profiled/name", Cast("VARCHAR")));
        });
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "EscapedSlashKey",
                                                          "JsonEscapedPaths",
                                                          shape,
                                                          "/a~1b",
                                                          Cast("VARCHAR"),
                                                          {"slash"},
                                                          false,
                                                          {0});
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "EscapedTildeKey",
                                                          "JsonEscapedPaths",
                                                          shape,
                                                          "/m~0n",
                                                          Cast("VARCHAR"),
                                                          {"tilde"},
                                                          false,
                                                          {0});
    for (const auto& [name, path, value] :
         {std::tuple{"LiteralDotKey", "/a.b", "dot"},
          std::tuple{"LiteralBackslashKey", "/a\\b", "backslash"},
          std::tuple{"EmptyNestedKey", "/empty/", "empty"}}) {
        AddMembershipCase<std::string_view, std::string_view>(
            cases,
            name,
            "JsonEscapedPaths",
            shape,
            path,
            Cast("VARCHAR"),
            {value},
            false,
            {0});
    }
}

void
AddJsonFlatStringCases(IndexTestCases& cases) {
    constexpr auto shape = BackendInputShape::JsonDocument;
    const auto cast = Cast("VARCHAR");
    const std::string path = "/profile/name/first";
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "StringIn",
                                                          "JsonEmployees",
                                                          shape,
                                                          path,
                                                          cast,
                                                          {"Alice", "Bob"},
                                                          false,
                                                          {0, 1});
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "StringNotIn",
                                                          "JsonEmployees",
                                                          shape,
                                                          path,
                                                          cast,
                                                          {"Bob"},
                                                          true,
                                                          {0, 2});
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "StringEmptyIn",
                                                          "JsonEmployees",
                                                          shape,
                                                          path,
                                                          cast,
                                                          {},
                                                          false,
                                                          {});
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "StringEmptyNotIn",
                                                          "JsonEmployees",
                                                          shape,
                                                          path,
                                                          cast,
                                                          {},
                                                          true,
                                                          {0, 1, 2});

    AddUnaryRangeCase<std::string_view, std::string_view>(cases,
                                                          "StringEqual",
                                                          "JsonEmployees",
                                                          shape,
                                                          path,
                                                          cast,
                                                          "Bob",
                                                          CompareOp::Equal,
                                                          {1});
    AddUnaryRangeCase<std::string_view, std::string_view>(cases,
                                                          "StringNotEqual",
                                                          "JsonEmployees",
                                                          shape,
                                                          path,
                                                          cast,
                                                          "Bob",
                                                          CompareOp::NotEqual,
                                                          {0, 2});
    AddUnaryRangeCase<std::string_view, std::string_view>(
        cases,
        "StringGreaterThan",
        "JsonEmployees",
        shape,
        path,
        cast,
        "Bob",
        CompareOp::GreaterThan,
        {2});
    AddUnaryRangeCase<std::string_view, std::string_view>(
        cases,
        "StringGreaterEqual",
        "JsonEmployees",
        shape,
        path,
        cast,
        "Bob",
        CompareOp::GreaterEqual,
        {1, 2});
    AddUnaryRangeCase<std::string_view, std::string_view>(cases,
                                                          "StringLessThan",
                                                          "JsonEmployees",
                                                          shape,
                                                          path,
                                                          cast,
                                                          "Bob",
                                                          CompareOp::LessThan,
                                                          {0});
    AddUnaryRangeCase<std::string_view, std::string_view>(cases,
                                                          "StringLessEqual",
                                                          "JsonEmployees",
                                                          shape,
                                                          path,
                                                          cast,
                                                          "Bob",
                                                          CompareOp::LessEqual,
                                                          {0, 1});
    for (const auto& [name, lo_inc, hi_inc, expected] :
         std::vector<std::tuple<std::string, bool, bool, std::vector<size_t>>>{
             {"StringClosedInterval", true, true, {0, 1, 2}},
             {"StringLeftClosedInterval", true, false, {0, 1}},
             {"StringRightClosedInterval", false, true, {1, 2}},
             {"StringOpenInterval", false, false, {1}},
         }) {
        AddIntervalCase<std::string_view, std::string_view>(cases,
                                                            name,
                                                            "JsonEmployees",
                                                            shape,
                                                            path,
                                                            cast,
                                                            "Alice",
                                                            lo_inc,
                                                            "Charlie",
                                                            hi_inc,
                                                            expected);
    }
    AddIntervalCase<std::string_view, std::string_view>(
        cases,
        "StringReversedInterval",
        "JsonEmployees",
        shape,
        path,
        cast,
        "Charlie",
        true,
        "Alice",
        true,
        {});

    AddPatternCase<std::string_view>(cases,
                                     "LikePattern",
                                     "JsonEmployees",
                                     shape,
                                     path,
                                     "A%ice",
                                     PatternOp::Match,
                                     {0});
    AddPatternCase<std::string_view>(cases,
                                     "LikeSingleCharacterWildcard",
                                     "JsonEmployees",
                                     shape,
                                     path,
                                     "B_b",
                                     PatternOp::Match,
                                     {1});
    AddPatternCase<std::string_view>(cases,
                                     "LikeSuffixAfterWildcard",
                                     "JsonEmployees",
                                     shape,
                                     path,
                                     "A%e",
                                     PatternOp::Match,
                                     {0});
    AddPatternCase<std::string_view>(cases,
                                     "PrefixPattern",
                                     "JsonEmployees",
                                     shape,
                                     path,
                                     "B",
                                     PatternOp::PrefixMatch,
                                     {1});
    AddPatternCase<std::string_view>(cases,
                                     "PostfixPattern",
                                     "JsonEmployees",
                                     shape,
                                     path,
                                     "lie",
                                     PatternOp::PostfixMatch,
                                     {2});
    AddPatternCase<std::string_view>(cases,
                                     "InnerPattern",
                                     "JsonEmployees",
                                     shape,
                                     path,
                                     "lic",
                                     PatternOp::InnerMatch,
                                     {0});
    AddPatternCase<std::string_view>(cases,
                                     "RegexPattern",
                                     "JsonEmployees",
                                     shape,
                                     path,
                                     "^(Alice|Bob)$",
                                     PatternOp::RegexMatch,
                                     {0, 1});

    AddMembershipCase<std::string_view, std::string_view>(
        cases,
        "EmbeddedNullString",
        "JsonTypeFamilies",
        shape,
        "/a",
        cast,
        {std::string("a\0b", 3)},
        false,
        {11});
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "Utf8String",
                                                          "JsonTypeFamilies",
                                                          shape,
                                                          "/a",
                                                          cast,
                                                          {"猫"},
                                                          false,
                                                          {12});
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "RootPrefixUnicode",
                                                          "JsonRootPrefix",
                                                          shape,
                                                          "/profile/name",
                                                          cast,
                                                          {"猫"},
                                                          false,
                                                          {2});
}

void
AddJsonFlatBoolCases(IndexTestCases& cases) {
    constexpr auto shape = BackendInputShape::JsonDocument;
    const auto cast = Cast("BOOL");
    const std::string path = "/profile/is_active";
    AddMembershipCase<std::string_view, bool>(cases,
                                              "BoolIn",
                                              "JsonEmployees",
                                              shape,
                                              path,
                                              cast,
                                              {true},
                                              false,
                                              {0, 2});
    AddMembershipCase<std::string_view, bool>(cases,
                                              "BoolNotIn",
                                              "JsonEmployees",
                                              shape,
                                              path,
                                              cast,
                                              {false},
                                              true,
                                              {0, 2});
    for (const auto& [name, op, expected] :
         std::vector<std::tuple<std::string, CompareOp, std::vector<size_t>>>{
             {"BoolEqualFalse", CompareOp::Equal, {1}},
             {"BoolNotEqualFalse", CompareOp::NotEqual, {0, 2}},
             {"BoolGreaterThanFalse", CompareOp::GreaterThan, {0, 2}},
             {"BoolGreaterEqualFalse", CompareOp::GreaterEqual, {0, 1, 2}},
             {"BoolLessThanFalse", CompareOp::LessThan, {}},
             {"BoolLessEqualFalse", CompareOp::LessEqual, {1}},
         }) {
        AddUnaryRangeCase<std::string_view, bool>(cases,
                                                  name,
                                                  "JsonEmployees",
                                                  shape,
                                                  path,
                                                  cast,
                                                  false,
                                                  op,
                                                  expected);
    }
    for (const auto& [name, lo_inc, hi_inc, expected] :
         std::vector<std::tuple<std::string, bool, bool, std::vector<size_t>>>{
             {"BoolClosedInterval", true, true, {0, 1, 2}},
             {"BoolLeftClosedInterval", true, false, {1}},
             {"BoolRightClosedInterval", false, true, {0, 2}},
             {"BoolOpenInterval", false, false, {}},
         }) {
        AddIntervalCase<std::string_view, bool>(cases,
                                                name,
                                                "JsonEmployees",
                                                shape,
                                                path,
                                                cast,
                                                false,
                                                lo_inc,
                                                true,
                                                hi_inc,
                                                expected);
    }
}

void
AddJsonFlatNumericCases(IndexTestCases& cases) {
    constexpr auto shape = BackendInputShape::JsonDocument;
    const auto cast = Cast("DOUBLE");
    const std::string path = "/profile/employee_id";
    AddMembershipCase<std::string_view, int64_t>(cases,
                                                 "Int64In",
                                                 "JsonEmployees",
                                                 shape,
                                                 path,
                                                 cast,
                                                 {1001, 1002},
                                                 false,
                                                 {0, 1});
    AddMembershipCase<std::string_view, int64_t>(cases,
                                                 "Int64NotIn",
                                                 "JsonEmployees",
                                                 shape,
                                                 path,
                                                 cast,
                                                 {1003},
                                                 true,
                                                 {0, 1});
    for (const auto& [name, op, expected] :
         std::vector<std::tuple<std::string, CompareOp, std::vector<size_t>>>{
             {"Int64Equal", CompareOp::Equal, {1}},
             {"Int64NotEqual", CompareOp::NotEqual, {0, 2}},
             {"Int64GreaterThan", CompareOp::GreaterThan, {2}},
             {"Int64GreaterEqual", CompareOp::GreaterEqual, {1, 2}},
             {"Int64LessThan", CompareOp::LessThan, {0}},
             {"Int64LessEqual", CompareOp::LessEqual, {0, 1}},
         }) {
        AddUnaryRangeCase<std::string_view, int64_t>(cases,
                                                     name,
                                                     "JsonEmployees",
                                                     shape,
                                                     path,
                                                     cast,
                                                     1002,
                                                     op,
                                                     expected);
    }
    for (const auto& [name, lo_inc, hi_inc, expected] :
         std::vector<std::tuple<std::string, bool, bool, std::vector<size_t>>>{
             {"Int64ClosedInterval", true, true, {0, 1, 2}},
             {"Int64LeftClosedInterval", true, false, {0, 1}},
             {"Int64RightClosedInterval", false, true, {1, 2}},
             {"Int64OpenInterval", false, false, {1}},
         }) {
        AddIntervalCase<std::string_view, int64_t>(cases,
                                                   name,
                                                   "JsonEmployees",
                                                   shape,
                                                   path,
                                                   cast,
                                                   1001,
                                                   lo_inc,
                                                   1003,
                                                   hi_inc,
                                                   expected);
    }
    AddIntervalCase<std::string_view, int64_t>(cases,
                                               "Int64ReversedInterval",
                                               "JsonEmployees",
                                               shape,
                                               path,
                                               cast,
                                               1003,
                                               true,
                                               1001,
                                               true,
                                               {});

    const std::string precision_path = "/a";
    AddMembershipCase<std::string_view, int64_t>(cases,
                                                 "MixedIntegerNotInOne",
                                                 "JsonPrecision",
                                                 shape,
                                                 precision_path,
                                                 cast,
                                                 {1},
                                                 true,
                                                 {0, 2, 3, 4, 5});
    AddMembershipCase<std::string_view, int64_t>(
        cases,
        "MixedIntegerNotInMaxInt64",
        "JsonPrecision",
        shape,
        precision_path,
        cast,
        {std::numeric_limits<int64_t>::max()},
        true,
        {0, 1, 2, 3, 4, 5});
    AddUnaryRangeCase<std::string_view, int64_t>(cases,
                                                 "MixedIntegerGreaterThanNine",
                                                 "JsonPrecision",
                                                 shape,
                                                 precision_path,
                                                 cast,
                                                 9,
                                                 CompareOp::GreaterThan,
                                                 {2, 3, 4, 5});
    AddIntervalCase<std::string_view, int64_t>(cases,
                                               "MixedIntegerZeroThroughNine",
                                               "JsonPrecision",
                                               shape,
                                               precision_path,
                                               cast,
                                               0,
                                               true,
                                               9,
                                               true,
                                               {1});

    AddMembershipCase<std::string_view, double>(cases,
                                                "DoubleInFraction",
                                                "JsonPrecision",
                                                shape,
                                                precision_path,
                                                cast,
                                                {10.5},
                                                false,
                                                {3});
    for (const auto& [name, op, expected] :
         std::vector<std::tuple<std::string, CompareOp, std::vector<size_t>>>{
             {"DoubleEqual", CompareOp::Equal, {2}},
             {"DoubleNotEqual", CompareOp::NotEqual, {0, 1, 3, 4, 5}},
             {"DoubleGreaterThan", CompareOp::GreaterThan, {3, 4, 5}},
             {"DoubleGreaterEqual", CompareOp::GreaterEqual, {2, 3, 4, 5}},
             {"DoubleLessThan", CompareOp::LessThan, {0, 1}},
             {"DoubleLessEqual", CompareOp::LessEqual, {0, 1, 2}},
         }) {
        AddUnaryRangeCase<std::string_view, double>(cases,
                                                    name,
                                                    "JsonPrecision",
                                                    shape,
                                                    precision_path,
                                                    cast,
                                                    10.0,
                                                    op,
                                                    expected);
    }
    for (const auto& [name, lo_inc, hi_inc, expected] :
         std::vector<std::tuple<std::string, bool, bool, std::vector<size_t>>>{
             {"DoubleClosedInterval", true, true, {0, 1, 2, 3}},
             {"DoubleLeftClosedInterval", true, false, {0, 1, 2}},
             {"DoubleRightClosedInterval", false, true, {1, 2, 3}},
             {"DoubleOpenInterval", false, false, {1, 2}},
         }) {
        AddIntervalCase<std::string_view, double>(cases,
                                                  name,
                                                  "JsonPrecision",
                                                  shape,
                                                  precision_path,
                                                  cast,
                                                  -10.0,
                                                  lo_inc,
                                                  10.5,
                                                  hi_inc,
                                                  expected);
    }
    AddIntervalCase<std::string_view, double>(cases,
                                              "DoubleReversedInterval",
                                              "JsonPrecision",
                                              shape,
                                              precision_path,
                                              cast,
                                              10.5,
                                              true,
                                              -10.0,
                                              true,
                                              {});

    AddMembershipCase<std::string_view, int64_t>(cases,
                                                 "NumericArrayIn",
                                                 "JsonEmployees",
                                                 shape,
                                                 "/profile/scores",
                                                 cast,
                                                 {95},
                                                 false,
                                                 {0});
    AddUnaryRangeCase<std::string_view, int64_t>(cases,
                                                 "NumericArrayGreaterThan",
                                                 "JsonEmployees",
                                                 shape,
                                                 "/profile/scores",
                                                 cast,
                                                 90,
                                                 CompareOp::GreaterThan,
                                                 {0, 2});
    AddIntervalCase<std::string_view, int64_t>(cases,
                                               "NumericArrayClosedInterval",
                                               "JsonEmployees",
                                               shape,
                                               "/profile/scores",
                                               cast,
                                               90,
                                               true,
                                               92,
                                               true,
                                               {0, 1, 2});
}

void
AddJsonFlatArrayStringCases(IndexTestCases& cases) {
    constexpr auto shape = BackendInputShape::JsonDocument;
    const auto cast = Cast("VARCHAR");
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "StringArrayCpp",
                                                          "JsonEmployees",
                                                          shape,
                                                          "/profile/skills",
                                                          cast,
                                                          {"cpp"},
                                                          false,
                                                          {0});
    AddMembershipCase<std::string_view, std::string_view>(cases,
                                                          "StringArrayPython",
                                                          "JsonEmployees",
                                                          shape,
                                                          "/profile/skills",
                                                          cast,
                                                          {"python"},
                                                          false,
                                                          {0, 1, 2});
}

template <typename InputT>
void
AddProjectedRoutingCases(IndexTestCases& cases,
                         std::string dataset,
                         JsonCastType cast,
                         std::vector<size_t> expected_exists,
                         std::vector<size_t> expected_not_null) {
    constexpr auto shape = BackendInputShape::JsonProjected;
    AddJsonCase<InputT>(
        cases,
        "ProjectedExactPathAndCast",
        dataset,
        shape,
        [cast, expected_exists = std::move(expected_exists)](
            const auto&, const auto& data, auto& reader) {
            ExpectProjectedOuterRoutesOnlyJson(*reader);
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            const auto casts = json->CastTypesOf("/a");
            ASSERT_EQ(casts.size(), 1);
            EXPECT_TRUE(SameCast(casts.front(), cast));
            auto resolved = json->Resolve("/a", cast);
            ASSERT_TRUE(resolved);
            ExpectResolvedMetadata(*resolved.get(), data.values.size(), cast);
            EXPECT_FALSE(json->Resolve("/wrong", cast));
            EXPECT_TRUE(json->CastTypesOf("/wrong").empty());
            EXPECT_FALSE(json->Resolve("/a", JsonCastType::UNKNOWN));
            ExpectHits(json->Exists("/a"), data.values.size(), expected_exists);
            ExpectProjectedExistsProtocol(*json);
        });
    AddResolvedNullCase<InputT>(cases,
                                "ProjectedComparableNullMask",
                                std::move(dataset),
                                shape,
                                "/a",
                                cast,
                                std::move(expected_not_null));
}

void
AddProjectedScalarCases(IndexTestCases& cases) {
    constexpr auto shape = BackendInputShape::JsonProjected;
    AddProjectedRoutingCases<double>(cases,
                                     "JsonProjectedDoubleTriState",
                                     Cast("DOUBLE"),
                                     {0, 3, 5},
                                     {0, 5});
    AddMembershipCase<double, double>(cases,
                                      "ProjectedDoubleIn",
                                      "JsonProjectedDoubleTriState",
                                      shape,
                                      "/a",
                                      Cast("DOUBLE"),
                                      {1.0},
                                      false,
                                      {0});
    AddMembershipCase<double, double>(cases,
                                      "ProjectedDoubleNotInRejectsUnknown",
                                      "JsonProjectedDoubleTriState",
                                      shape,
                                      "/a",
                                      Cast("DOUBLE"),
                                      {1.0},
                                      true,
                                      {5});
    AddUnaryRangeCase<double, double>(cases,
                                      "ProjectedDoubleGreaterThan",
                                      "JsonProjectedDoubleTriState",
                                      shape,
                                      "/a",
                                      Cast("DOUBLE"),
                                      1.0,
                                      CompareOp::GreaterThan,
                                      {5});
    AddIntervalCase<double, double>(cases,
                                    "ProjectedDoubleClosedInterval",
                                    "JsonProjectedDoubleTriState",
                                    shape,
                                    "/a",
                                    Cast("DOUBLE"),
                                    1.0,
                                    true,
                                    3.0,
                                    true,
                                    {0, 5});
    AddUnaryRangeCase<double, double>(cases,
                                      "ProjectedDoubleAbsentValidity",
                                      "JsonProjectedDoubleAllValid",
                                      shape,
                                      "/a",
                                      Cast("DOUBLE"),
                                      10.0,
                                      CompareOp::LessEqual,
                                      {0, 1, 2});
    AddExistsCase<double>(cases,
                          "ProjectedDoubleMultiBatchExists",
                          "JsonProjectedDoubleMultiBatch",
                          shape,
                          "/a",
                          {0, 3, 5});

    AddProjectedRoutingCases<bool>(
        cases, "JsonProjectedBoolTriState", Cast("BOOL"), {0, 3, 5}, {0, 5});
    AddMembershipCase<bool, bool>(cases,
                                  "ProjectedBoolIn",
                                  "JsonProjectedBoolTriState",
                                  shape,
                                  "/a",
                                  Cast("BOOL"),
                                  {true},
                                  false,
                                  {0, 5});
    AddMembershipCase<bool, bool>(cases,
                                  "ProjectedBoolNotInRejectsUnknown",
                                  "JsonProjectedBoolTriState",
                                  shape,
                                  "/a",
                                  Cast("BOOL"),
                                  {false},
                                  true,
                                  {0, 5});
    AddUnaryRangeCase<bool, bool>(cases,
                                  "ProjectedBoolGreaterThanFalse",
                                  "JsonProjectedBoolTriState",
                                  shape,
                                  "/a",
                                  Cast("BOOL"),
                                  false,
                                  CompareOp::GreaterThan,
                                  {0, 5});
    AddIntervalCase<bool, bool>(cases,
                                "ProjectedBoolAbsentValidity",
                                "JsonProjectedBoolAllValid",
                                shape,
                                "/a",
                                Cast("BOOL"),
                                false,
                                true,
                                true,
                                true,
                                {0, 1, 2, 3});

    AddProjectedRoutingCases<std::string_view>(cases,
                                               "JsonProjectedVarcharTriState",
                                               Cast("VARCHAR"),
                                               {0, 3, 5},
                                               {0, 5});
    AddMembershipCase<std::string_view, std::string_view>(
        cases,
        "ProjectedVarcharIn",
        "JsonProjectedVarcharTriState",
        shape,
        "/a",
        Cast("VARCHAR"),
        {"alpha"},
        false,
        {0});
    AddMembershipCase<std::string_view, std::string_view>(
        cases,
        "ProjectedVarcharNotInRejectsUnknown",
        "JsonProjectedVarcharTriState",
        shape,
        "/a",
        Cast("VARCHAR"),
        {"alpha"},
        true,
        {5});
    AddUnaryRangeCase<std::string_view, std::string_view>(
        cases,
        "ProjectedVarcharGreaterThan",
        "JsonProjectedVarcharTriState",
        shape,
        "/a",
        Cast("VARCHAR"),
        "alpha",
        CompareOp::GreaterThan,
        {5});
    AddIntervalCase<std::string_view, std::string_view>(
        cases,
        "ProjectedVarcharClosedInterval",
        "JsonProjectedVarcharTriState",
        shape,
        "/a",
        Cast("VARCHAR"),
        "alpha",
        true,
        "beta",
        true,
        {0, 5});
    AddMembershipCase<std::string_view, std::string_view>(
        cases,
        "ProjectedVarcharEmbeddedNull",
        "JsonProjectedVarcharAllValid",
        shape,
        "/a",
        Cast("VARCHAR"),
        {std::string("a\0b", 3)},
        false,
        {3});
    AddPatternCase<std::string_view>(cases,
                                     "ProjectedLike",
                                     "JsonProjectedVarcharAllValid",
                                     shape,
                                     "/a",
                                     "alpha%",
                                     PatternOp::Match,
                                     {1, 2});
    AddPatternCase<std::string_view>(cases,
                                     "ProjectedPrefix",
                                     "JsonProjectedVarcharAllValid",
                                     shape,
                                     "/a",
                                     "alpha",
                                     PatternOp::PrefixMatch,
                                     {1, 2});
    AddPatternCase<std::string_view>(cases,
                                     "ProjectedPostfix",
                                     "JsonProjectedVarcharAllValid",
                                     shape,
                                     "/a",
                                     "bet",
                                     PatternOp::PostfixMatch,
                                     {2});
    AddPatternCase<std::string_view>(cases,
                                     "ProjectedInner",
                                     "JsonProjectedVarcharAllValid",
                                     shape,
                                     "/a",
                                     "pha",
                                     PatternOp::InnerMatch,
                                     {1, 2});
    AddPatternCase<std::string_view>(cases,
                                     "ProjectedRegex",
                                     "JsonProjectedVarcharAllValid",
                                     shape,
                                     "/a",
                                     "^alpha$",
                                     PatternOp::RegexMatch,
                                     {1});
}

void
AddProjectedArrayCases(IndexTestCases& cases) {
    constexpr auto shape = BackendInputShape::JsonProjected;
    AddProjectedRoutingCases<ArrayView>(cases,
                                        "JsonProjectedArrayBool",
                                        Cast("ARRAY_BOOL"),
                                        {0, 1, 2, 3},
                                        {0, 1, 2, 3});
    AddMembershipCase<ArrayView, bool>(cases,
                                       "ArrayBoolIn",
                                       "JsonProjectedArrayBool",
                                       shape,
                                       "/a",
                                       Cast("ARRAY_BOOL"),
                                       {true},
                                       false,
                                       {0, 3});
    AddMembershipCase<ArrayView, bool>(cases,
                                       "ArrayBoolNotIn",
                                       "JsonProjectedArrayBool",
                                       shape,
                                       "/a",
                                       Cast("ARRAY_BOOL"),
                                       {true},
                                       true,
                                       {1, 2});
    AddUnaryRangeCase<ArrayView, bool>(cases,
                                       "ArrayBoolRange",
                                       "JsonProjectedArrayBool",
                                       shape,
                                       "/a",
                                       Cast("ARRAY_BOOL"),
                                       false,
                                       CompareOp::GreaterThan,
                                       {0, 3});

    AddProjectedRoutingCases<ArrayView>(cases,
                                        "JsonProjectedArrayDouble",
                                        Cast("ARRAY_DOUBLE"),
                                        {0, 1, 2, 3},
                                        {0, 1, 2, 3});
    AddMembershipCase<ArrayView, double>(cases,
                                         "ArrayDoubleIn",
                                         "JsonProjectedArrayDouble",
                                         shape,
                                         "/a",
                                         Cast("ARRAY_DOUBLE"),
                                         {2.0},
                                         false,
                                         {0, 3});
    AddMembershipCase<ArrayView, double>(cases,
                                         "ArrayDoubleNotIn",
                                         "JsonProjectedArrayDouble",
                                         shape,
                                         "/a",
                                         Cast("ARRAY_DOUBLE"),
                                         {2.0},
                                         true,
                                         {1, 2});
    AddUnaryRangeCase<ArrayView, double>(cases,
                                         "ArrayDoubleRange",
                                         "JsonProjectedArrayDouble",
                                         shape,
                                         "/a",
                                         Cast("ARRAY_DOUBLE"),
                                         3.0,
                                         CompareOp::GreaterThan,
                                         {2});
    AddIntervalCase<ArrayView, double>(cases,
                                       "ArrayDoubleInterval",
                                       "JsonProjectedArrayDouble",
                                       shape,
                                       "/a",
                                       Cast("ARRAY_DOUBLE"),
                                       2.0,
                                       true,
                                       3.5,
                                       true,
                                       {0, 2, 3});

    AddProjectedRoutingCases<ArrayView>(cases,
                                        "JsonProjectedArrayVarchar",
                                        Cast("ARRAY_VARCHAR"),
                                        {0, 1, 2, 3},
                                        {0, 1, 2, 3});
    AddMembershipCase<ArrayView, std::string_view>(cases,
                                                   "ArrayVarcharIn",
                                                   "JsonProjectedArrayVarchar",
                                                   shape,
                                                   "/a",
                                                   Cast("ARRAY_VARCHAR"),
                                                   {"alpha"},
                                                   false,
                                                   {0, 3});
    AddMembershipCase<ArrayView, std::string_view>(cases,
                                                   "ArrayVarcharNotIn",
                                                   "JsonProjectedArrayVarchar",
                                                   shape,
                                                   "/a",
                                                   Cast("ARRAY_VARCHAR"),
                                                   {"alpha"},
                                                   true,
                                                   {1, 2});
    AddUnaryRangeCase<ArrayView, std::string_view>(cases,
                                                   "ArrayVarcharRange",
                                                   "JsonProjectedArrayVarchar",
                                                   shape,
                                                   "/a",
                                                   Cast("ARRAY_VARCHAR"),
                                                   "beta",
                                                   CompareOp::GreaterThan,
                                                   {2});
}

void
AddProjectedNgramRoutingCases(IndexTestCases& cases) {
    constexpr auto shape = BackendInputShape::JsonProjected;
    AddJsonCase<JsonProjectedString>(
        cases,
        "ProjectedNgramRoutingAndExists",
        "JsonProjectedNgramTriState",
        shape,
        [](const auto&, const auto& data, auto& reader) {
            ExpectProjectedOuterRoutesOnlyJson(*reader);
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            const auto varchar = Cast("VARCHAR");
            const auto casts = json->CastTypesOf("/a");
            ASSERT_EQ(casts.size(), 1);
            EXPECT_TRUE(SameCast(casts.front(), varchar));
            auto resolved = json->Resolve("/a", varchar);
            ASSERT_TRUE(resolved);
            ExpectResolvedMetadata(
                *resolved.get(), data.values.size(), varchar);
            const auto* ngram =
                dynamic_cast<const INgramReader*>(resolved.get());
            ASSERT_NE(ngram, nullptr);
            EXPECT_TRUE(resolved->Caps().ngram_candidates);
            EXPECT_FALSE(resolved->Caps().predicate);
            EXPECT_FALSE(resolved->Caps().exact);
            EXPECT_FALSE(json->Resolve("/wrong", varchar));
            EXPECT_FALSE(json->Resolve("/a", Cast("DOUBLE")));
            ExpectHits(json->Exists("/a"), data.values.size(), {0, 3, 5});
            ExpectProjectedExistsProtocol(*json);
        });
    AddJsonCase<JsonProjectedString>(
        cases,
        "ProjectedNgramAbsentValidity",
        "JsonProjectedNgramAllValid",
        shape,
        [](const auto&, const auto& data, auto& reader) {
            ExpectProjectedOuterRoutesOnlyJson(*reader);
            const auto* json = JsonReader(reader);
            ASSERT_NE(json, nullptr);
            const auto varchar = Cast("VARCHAR");
            auto resolved = json->Resolve("/a", varchar);
            ASSERT_TRUE(resolved);
            ExpectResolvedMetadata(
                *resolved.get(), data.values.size(), varchar);
            EXPECT_NE(dynamic_cast<const INgramReader*>(resolved.get()),
                      nullptr);
            EXPECT_TRUE(resolved->Caps().ngram_candidates);
            EXPECT_FALSE(resolved->Caps().predicate);
            EXPECT_FALSE(resolved->Caps().exact);
            ExpectHits(json->Exists("/a"), data.values.size(), {0, 1, 2, 3});
            ExpectProjectedExistsProtocol(*json);
        });
}

const IndexTestCases&
JsonCases() {
    static const auto cases = [] {
        IndexTestCases result;
        AddJsonFlatRoutingCases(result);
        AddJsonFlatStringCases(result);
        AddJsonFlatBoolCases(result);
        AddJsonFlatNumericCases(result);
        AddJsonFlatArrayStringCases(result);
        AddProjectedScalarCases(result);
        AddProjectedArrayCases(result);
        AddProjectedNgramRoutingCases(result);
        return result;
    }();
    return cases;
}

class TrackingReader final : public IIndexReaderBase {
 public:
    explicit TrackingReader(size_t* destroyed) : destroyed_(destroyed) {
    }

    ~TrackingReader() override {
        ++*destroyed_;
    }

    ReaderCaps
    Caps() const override {
        return {};
    }

    Domain
    CoordDomain() const override {
        return Domain::Row;
    }

    int64_t
    Count() const override {
        return 1;
    }

    DataType
    ValueType() const override {
        return DataType::INT64;
    }

 private:
    size_t* destroyed_;
};

TEST(JsonResolvedReaderTest, EmptyBorrowedOwnedAndMovePreserveOwnership) {
    JsonResolvedReader empty;
    EXPECT_FALSE(empty);
    EXPECT_EQ(empty.get(), nullptr);
    EXPECT_FALSE(JsonResolvedReader::Owned({}));
    EXPECT_FALSE(JsonResolvedReader::Borrowed(nullptr));

    size_t borrowed_destroyed = 0;
    {
        TrackingReader borrowed_reader(&borrowed_destroyed);
        auto borrowed = JsonResolvedReader::Borrowed(&borrowed_reader);
        ASSERT_TRUE(borrowed);
        EXPECT_EQ(borrowed.get(), &borrowed_reader);
        auto moved = std::move(borrowed);
        EXPECT_FALSE(borrowed);
        EXPECT_EQ(moved.get(), &borrowed_reader);
        auto reassigned = JsonResolvedReader::Borrowed(&borrowed_reader);
        reassigned = std::move(moved);
        EXPECT_FALSE(moved);
        EXPECT_EQ(reassigned.get(), &borrowed_reader);
        EXPECT_EQ(borrowed_destroyed, 0);
    }
    EXPECT_EQ(borrowed_destroyed, 1);

    size_t owned_destroyed = 0;
    {
        auto first = JsonResolvedReader::Owned(
            std::make_unique<TrackingReader>(&owned_destroyed));
        const auto* first_address = first.get();
        auto moved = std::move(first);
        EXPECT_FALSE(first);
        EXPECT_EQ(moved.get(), first_address);

        auto replacement = JsonResolvedReader::Owned(
            std::make_unique<TrackingReader>(&owned_destroyed));
        const auto* replacement_address = replacement.get();
        moved = std::move(replacement);
        EXPECT_FALSE(replacement);
        EXPECT_EQ(moved.get(), replacement_address);
        EXPECT_EQ(owned_destroyed, 1);
    }
    EXPECT_EQ(owned_destroyed, 2);
}

class JsonIndexReaderTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(JsonIndexReaderTest, RoutesAndQueriesExpectedJsonValues) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(JsonReaders,
                         JsonIndexReaderTest,
                         ::testing::ValuesIn(JsonCases().All()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
