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

#include "index/test_utils/LoaderTestAccess.h"
#include "folly/coro/BlockingWait.h"

#include <gtest/gtest.h>

#include "index/scalar/text/TextIndexLoader.h"
#include "index/test_utils/StorageTestUtils.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/Meta.h"
#include "index/SingleCellIndexStorageTranslator.h"
#include "index/scalar/text/TextIndexReader.h"
#include "index/scalar/text/TextIndexBuilder.h"
#include "storage/artifact/LocalDirectory.h"
#include "index/scalar/text/TextIndexLoader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"
#include "tantivy-wrapper.h"

namespace milvus::index::test {
namespace {

void
CheckCachedText() {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharV7Heap");
    ScalarTestData<std::string_view> data(
        {"apple", "banana", "banana", "pear"});
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
                        "text-cache-XXXXXX",
                        "Text cache test");
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
                    const auto* text =
                        dynamic_cast<const ITextMatchReader*>(reader.get());
                    ASSERT_NE(text, nullptr);
                    EXPECT_EQ(text->MatchQuery("banana", 1).count(), 2);
                    EXPECT_EQ(text->PhraseMatchQuery("banana", 0).count(), 2);
                    EXPECT_EQ(text->FuzzyMatchQuery("banan", 1).count(), 2);
                }
            }
        }
    }
}

TEST(TextIndexArtifactTest, CachedStorageLoadsWithRequestContext) {
    CheckCachedText();
}

TEST(TextIndexArtifactTest, WarmupRejectsCountMismatchBeforePublication) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharV7Heap");
    ScalarTestData<std::string_view> data(
        {"apple", "banana", "banana", "pear"});
    data.validity.reset(3);
    const ScalarTestInput<std::string_view> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = 4});
    for (const bool packed : {false, true}) {
        SCOPED_TRACE(packed);
        auto staging = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "text-bad-count-XXXXXX",
            "text count test");
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

TEST(TextIndexCacheTest, FreshSlotUsesEachReadersContext) {
    const cachinglayer::ResourceUsage limit{16 * 1024 * 1024, 0};
    auto dlist = std::make_shared<cachinglayer::internal::DList>(
        false, limit, limit, limit, cachinglayer::EvictionConfig{});
    std::vector<int32_t> priorities;
    auto load = [&](OpContext* ctx)
        -> folly::coro::Task<std::unique_ptr<TextIndexStorage>> {
        priorities.push_back(ctx->runtime_load_priority.value_or(-1));
        auto directory = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "text-pin-XXXXXX",
            "text pin test");
        auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
            "values",
            true,
            "",
            TANTIVY_INDEX_LATEST_VERSION,
            "milvus_tokenizer",
            R"({"tokenizer":"standard"})",
            "");
        const std::string docs[] = {"apple", "banana"};
        engine->add_data<std::string>(docs, 2, 0);
        engine->commit();
        engine = engine->create_snapshot_reader(SetBitsetSealed);
        engine->register_tokenizer("milvus_tokenizer",
                                   R"({"tokenizer":"standard"})");
        co_return std::make_unique<TextIndexStorage>(
            std::move(directory),
            std::move(engine),
            std::make_shared<const std::vector<size_t>>(),
            2,
            DataType::VARCHAR,
            false,
            0);
    };
    IndexCacheConfig cache{
        "text-storage-test",
        cachinglayer::Meta{cachinglayer::StorageType::MEMORY,
                           cachinglayer::CellIdMappingMode::IDENTICAL,
                           cachinglayer::CellDataType::OTHER,
                           CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                           true},
        {1024 * 1024, 0},
        {},
        4096};
    std::weak_ptr<cachinglayer::CacheSlot<TextIndexStorage>> slot_lifetime;
    auto make_index = [&] {
        auto slot = std::make_shared<cachinglayer::CacheSlot<TextIndexStorage>>(
            std::make_unique<
                SingleCellIndexStorageTranslator<TextIndexStorage>>(
                cache, false, load),
            dlist.get(),
            true,
            true,
            true,
            std::chrono::seconds(5),
            std::chrono::seconds(5));
        slot_lifetime = slot;
        return std::make_shared<TextIndex>(
            slot,
            IndexMetadata{ReaderCaps{.text_match = true},
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
    const auto* text = dynamic_cast<const ITextMatchReader*>(first.get());
    ASSERT_NE(text, nullptr);
    EXPECT_EQ(text->MatchQuery("apple", 1).count(), 1);
    const auto scanned = first_ctx.storage_usage.scanned_total_bytes.load();
    ASSERT_GT(scanned, 0);
    EXPECT_EQ(text->MatchQuery("banana", 1).count(), 1);
    auto validity =
        dynamic_cast<const TextIndexReader*>(first.get())->Validity();
    EXPECT_EQ(validity->Count(), 2);
    EXPECT_EQ(first_ctx.storage_usage.scanned_total_bytes.load(), scanned);
    EXPECT_EQ(priorities, (std::vector<int32_t>{0}));
    index.reset();
    EXPECT_FALSE(slot_lifetime.expired());
    first.reset();
    EXPECT_FALSE(slot_lifetime.expired());
    EXPECT_EQ(validity->Count(), 2);
    validity.reset();
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

TEST(TextIndexArtifactTest, WriterBudgetConstantsMatchBuildAndGrowingModes) {
    EXPECT_EQ(tantivy::DEFAULT_OVERALL_MEMORY_BUDGET_IN_BYTES,
              500UL * 1024 * 1024);
    EXPECT_EQ(tantivy::GROWING_TEXT_MEMORY_BUDGET_IN_BYTES, 15UL * 1024 * 1024);
}

storage::ArtifactPtr
BuildText(const ReaderBackend& backend, bool nullable) {
    ScalarTestData<std::string_view> data(
        {"alpha beta", "beta gamma", "alpha"});
    if (nullable) {
        data.validity.reset(1);
    } else {
        data.validity_present = false;
    }
    data.batch_sizes = {1, 2};
    const ScalarTestInput<std::string_view> input(data);
    return backend.Build(input.View(), {.row_count = data.values.size()});
}

TEST(TextIndexArtifactTest, PackedStorageAccountsNullableHeapAndMmap) {
    for (const auto name : {"TextVarcharV7Heap", "TextVarcharV7Mmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        auto artifact = BuildText(backend, true);
        auto storage = OpenV3Storage<TextIndexLoader>(
            backend, SerializeV3(*artifact), {.row_count = 3});
        const auto usage = storage->CellByteSize();
        EXPECT_EQ(usage.memory_bytes, storage->MemoryUsage());
        EXPECT_GE(usage.memory_bytes, TargetBitmap(3).size_in_bytes());
        EXPECT_EQ(usage.file_bytes > 0, backend.MmapRequested());
        auto index = std::make_shared<TextIndex>(std::move(storage));
        auto reader = index->CreateReader(nullptr);
        const auto* text = dynamic_cast<const ITextMatchReader*>(reader.get());
        ASSERT_NE(text, nullptr);
        EXPECT_EQ(text->MatchQuery("alpha", 1).count(), 2);
    }
}

TEST(TextIndexArtifactTest, LegacyV5DirectoryAndValidityRoundTrip) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharV5Heap");
    auto artifact = BuildText(backend, true);
    auto buffers = SerializeV1V2(*artifact);
    auto reader = OpenV1V2(backend, buffers);
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 3);

    const auto* text = dynamic_cast<const ITextMatchReader*>(reader.get());
    ASSERT_NE(text, nullptr);
    const auto alpha = text->MatchQuery("alpha", 1);
    ASSERT_EQ(alpha.size(), 3);
    EXPECT_TRUE(alpha[0]);
    EXPECT_FALSE(alpha[1]);
    EXPECT_TRUE(alpha[2]);

    const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    const auto is_null = nulls->IsNull();
    EXPECT_FALSE(is_null[0]);
    EXPECT_TRUE(is_null[1]);
    EXPECT_FALSE(is_null[2]);
}

TEST(TextIndexArtifactTest, LocalBuildStorageOutlivesArtifactAndInput) {
    std::unique_ptr<TextIndexStorage> storage;
    {
        ScalarTestData<std::string_view> data(
            {"alpha", "ignored", "alpha beta"});
        data.validity.reset(1);
        const ScalarTestInput<std::string_view> input(data);
        const auto view = input.View();
        TextIndexBuildSource source{.row_count = 3,
                                    .produce = [&](const auto& consume) {
                                        for (const auto& batch : view.batches) {
                                            consume(batch);
                                        }
                                    }};
        TextIndexBuildParams params{
            .field_name = "text",
            .tantivy_index_version = TANTIVY_INDEX_LATEST_VERSION,
            .unique_id = "local-storage-test"};
        auto artifact =
            std::move(TextIndexBuilder(std::move(params))).Build(source);
        storage = std::move(*artifact).IntoStorage();
    }
    ASSERT_NE(storage, nullptr);
    EXPECT_EQ(storage->count_, 3);
    EXPECT_GT(storage->CellByteSize().memory_bytes, 0);
    EXPECT_EQ(storage->CellByteSize().file_bytes, 0);
    auto index = std::make_shared<TextIndex>(std::move(storage));
    auto reader = index->CreateReader(nullptr);
    index.reset();
    const auto* text = dynamic_cast<const ITextMatchReader*>(reader.get());
    ASSERT_NE(text, nullptr);
    const auto matches = text->MatchQuery("alpha", 1);
    ASSERT_EQ(matches.size(), 3);
    EXPECT_TRUE(matches[0]);
    EXPECT_FALSE(matches[1]);
    EXPECT_TRUE(matches[2]);
    const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    const auto is_null = nulls->IsNull();
    EXPECT_FALSE(is_null[0]);
    EXPECT_TRUE(is_null[1]);
    EXPECT_FALSE(is_null[2]);
}

TEST(TextIndexArtifactTest, RamArtifactExplicitlyRejectsSerialization) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharRamV7NonNull");
    auto artifact = BuildText(backend, false);
    TestArtifactData persisted;
    TestArtifactSink sink(persisted);
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { artifact->Serialize(sink); });
    EXPECT_TRUE(persisted.entries.empty());
    EXPECT_TRUE(persisted.metadata.empty());
}

TEST(TextIndexArtifactTest, V3PublishesFileInventoryAndNullState) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharV7Heap");
    auto artifact = BuildText(backend, true);
    const auto persisted = SerializeV3(*artifact);
    const auto files =
        persisted.metadata.at(FILE_NAMES).get<std::vector<std::string>>();
    EXPECT_FALSE(files.empty());
    EXPECT_EQ(persisted.metadata.at(HAS_NULL), true);
    EXPECT_TRUE(persisted.entries.contains(INDEX_NULL_OFFSET));
    for (const auto& file : files) {
        EXPECT_TRUE(persisted.entries.contains(file));
    }
}

TEST(TextIndexArtifactTest, PackedTargetsRetainMmapFilesAfterPlanRelease) {
    for (const auto name : {"TextVarcharV7Heap", "TextVarcharV7Mmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        auto artifact = BuildText(backend, true);
        const auto persisted = SerializeV3(*artifact);
        nlohmann::json encoded = {{"entries", nlohmann::json::array()}};
        size_t offset = 0;
        for (const auto& [entry_name, bytes] : persisted.entries) {
            encoded["entries"].push_back({{"name", entry_name},
                                          {"offset", offset},
                                          {"size", bytes.size()},
                                          {"crc32", "00000000"}});
            offset += bytes.size();
        }
        const auto serialized = encoded.dump();
        auto [directory, encryption] = storage::ParseIndexEntryDirectory(
            std::span(reinterpret_cast<const uint8_t*>(serialized.data()),
                      serialized.size()),
            offset + serialized.size() + 4096);
        storage::LoadOptions opts;
        opts.params = backend.LoadParams({.row_count = 3});
        opts.enable_mmap = backend.MmapRequested();
        const nlohmann::json metadata = persisted.metadata;
        IIndexReaderBasePtr reader;
        std::vector<std::pair<std::string, bool>> paths;
        {
            auto plan = LoaderTestAccess::Plan<TextIndexLoader>(
                directory, metadata, opts);
            for (auto& entry : plan.entries) {
                const auto& bytes = persisted.entries.at(entry.name);
                if (auto* file =
                        std::get_if<storage::FileEntryTarget>(&entry.target)) {
                    paths.emplace_back(file->staging->path,
                                       file->staging->retain_on_success);
                    file->staging->Prepare(storage::io::Priority::MIDDLE);
                    file->staging->WriteAt(0, bytes.data(), bytes.size());
                    file->staging->Finish();
                } else {
                    const auto& memory =
                        std::get<storage::MemoryEntryTarget>(entry.target);
                    ASSERT_EQ(memory.bytes, bytes.size());
                    std::memcpy(memory.data, bytes.data(), bytes.size());
                }
            }
            reader =
                std::make_shared<TextIndex>(
                    folly::coro::blockingWait(
                        LoaderTestAccess::StorageFinisher<TextIndexLoader>()(
                            plan, opts, false)))
                    ->CreateReader(nullptr);
            plan.Commit();
        }
        ASSERT_NE(reader, nullptr);
        const auto* text = dynamic_cast<const ITextMatchReader*>(reader.get());
        ASSERT_NE(text, nullptr);
        const auto matches = text->MatchQuery("alpha", 1);
        ASSERT_EQ(matches.size(), 3);
        EXPECT_TRUE(matches[0]);
        EXPECT_FALSE(matches[1]);
        EXPECT_TRUE(matches[2]);
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        EXPECT_TRUE(nulls->IsNull()[1]);
        for (const auto& [path, retained] : paths) {
            EXPECT_EQ(std::filesystem::exists(path), retained);
        }
        reader.reset();
        for (const auto& [path, retained] : paths) {
            EXPECT_FALSE(std::filesystem::exists(path));
        }
    }
}

TEST(TextIndexArtifactTest, MissingFileInventoryIsRejected) {
    const auto& backend = ScalarReaderBackends().Get<std::string_view>(
        "TextVarcharV7HeapNonNull");
    auto artifact = BuildText(backend, false);
    auto persisted = SerializeV3(*artifact);
    persisted.metadata.erase(FILE_NAMES);

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(TextIndexArtifactTest, MissingDeclaredEngineFileIsRejected) {
    const auto& backend = ScalarReaderBackends().Get<std::string_view>(
        "TextVarcharV7HeapNonNull");
    auto artifact = BuildText(backend, false);
    auto persisted = SerializeV3(*artifact);
    const auto files =
        persisted.metadata.at(FILE_NAMES).get<std::vector<std::string>>();
    ASSERT_FALSE(files.empty());
    persisted.entries.erase(files.front());

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(TextIndexArtifactTest, NullSidecarMetadataMismatchIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharV7Heap");
    auto artifact = BuildText(backend, true);
    auto persisted = SerializeV3(*artifact);
    persisted.metadata[HAS_NULL] = false;

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(TextIndexArtifactTest, InvalidEngineFilesAreRejected) {
    const auto& backend = ScalarReaderBackends().Get<std::string_view>(
        "TextVarcharV7HeapNonNull");
    auto artifact = BuildText(backend, false);
    auto persisted = SerializeV3(*artifact);
    const auto files =
        persisted.metadata.at(FILE_NAMES).get<std::vector<std::string>>();
    ASSERT_FALSE(files.empty());
    for (const auto& file : files) {
        persisted.entries[file] = {0x00, 0x01};
    }

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(TextIndexArtifactTest, TextRamArtifactConvertsWithoutPersistence) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharRamV7");
    ScalarTestData<std::string_view> data({"alpha beta", "gamma"});
    data.validity_present = false;
    const ScalarTestInput<std::string_view> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = 2});

    auto* text_artifact = dynamic_cast<TextIndexArtifact*>(artifact.get());
    ASSERT_NE(text_artifact, nullptr);
    auto reader =
        std::make_shared<TextIndex>(std::move(*text_artifact).IntoStorage())
            ->CreateReader(nullptr);
    artifact.reset();
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 2);
    EXPECT_EQ(reader->CoordDomain(), Domain::Row);
    EXPECT_EQ(reader->ValueType(), DataType::VARCHAR);
    EXPECT_TRUE(reader->Caps().text_match);
    EXPECT_NE(dynamic_cast<const INullReader*>(reader.get()), nullptr);
}

TEST(TextIndexArtifactTest, PersistedArtifactCanThenBeConsumed) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("TextVarcharV7Heap");
    ScalarTestData<std::string_view> data({"alpha beta", "gamma"});
    data.validity_present = false;
    const ScalarTestInput<std::string_view> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = 2});
    ASSERT_NE(artifact, nullptr);

    TestArtifactData persisted;
    TestArtifactSink sink(persisted, storage::Generation::V1V2);
    artifact->Serialize(sink);
    sink.Finish();
    EXPECT_FALSE(persisted.entries.empty());

    auto* text_artifact = dynamic_cast<TextIndexArtifact*>(artifact.get());
    ASSERT_NE(text_artifact, nullptr);
    auto reader =
        std::make_shared<TextIndex>(std::move(*text_artifact).IntoStorage())
            ->CreateReader(nullptr);
    artifact.reset();
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 2);
    EXPECT_EQ(reader->CoordDomain(), Domain::Row);
    EXPECT_EQ(reader->ValueType(), DataType::VARCHAR);
    EXPECT_TRUE(reader->Caps().text_match);
    EXPECT_NE(dynamic_cast<const INullReader*>(reader.get()), nullptr);
}

}  // namespace
}  // namespace milvus::index::test
