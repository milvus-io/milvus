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

#include <cstring>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "index/Meta.h"
#include "index/contracts/query/INullReader.h"
#include "storage/artifact/LocalDirectory.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

storage::ArtifactPtr
BuildInverted(const ReaderBackend& backend, bool nullable) {
    ScalarTestData<int64_t> data({1, 2, 2, 3});
    if (nullable) {
        data.validity.reset(3);
    } else {
        data.validity_present = false;
    }
    const ScalarTestInput<int64_t> input(data);
    return backend.Build(input.View(), {.row_count = data.values.size()});
}

template <typename T>
void
CheckCachedInverted(std::string_view backend_name,
                    ScalarTestData<T> data,
                    T key) {
    const auto& backend = ScalarReaderBackends().Get<T>(backend_name);
    const ScalarTestInput<T> input(data);
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
                        "inverted-cache-XXXXXX",
                        "Inverted cache test");
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
                    const auto* predicate =
                        dynamic_cast<const IScalarPredicateReader<T>*>(
                            reader.get());
                    ASSERT_NE(predicate, nullptr);
                    EXPECT_EQ(predicate->In(1, &key).count(), 2);
                    EXPECT_EQ(predicate->NotIn(1, &key).count(), 1);
                    EXPECT_EQ(predicate->Range(key, CompareOp::Equal).count(),
                              2);
                }
            }
        }
    }
}

TEST(InvertedIndexArtifactTest, CachedStorageLoadsWithRequestContext) {
    ScalarTestData<int64_t> numbers({1, 2, 2, 3});
    numbers.validity.reset(3);
    CheckCachedInverted<int64_t>("InvertedInt64", std::move(numbers), 2);
    ScalarTestData<std::string_view> strings({"a", "b", "b", "c"});
    strings.validity.reset(3);
    CheckCachedInverted<std::string_view>(
        "InvertedVarchar", std::move(strings), "b");
}

TEST(InvertedIndexArtifactTest, WarmupRejectsCountMismatchBeforePublication) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("InvertedInt64");
    auto artifact = BuildInverted(backend, true);
    for (const bool packed : {false, true}) {
        SCOPED_TRACE(packed);
        auto staging = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "inverted-bad-count-XXXXXX",
            "inverted count test");
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

TEST(InvertedIndexArtifactTest, LegacyDirectoryRoundTrip) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("InvertedInt64NonNull");
    auto artifact = BuildInverted(backend, false);
    auto buffers = SerializeV1V2(*artifact);
    auto reader = OpenV1V2(backend, buffers);
    ASSERT_NE(reader, nullptr);
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    const int64_t key = 2;
    const auto hits = predicate->In(1, &key);
    ASSERT_EQ(hits.size(), 4);
    EXPECT_EQ(hits.count(), 2);
}

TEST(InvertedIndexArtifactTest, V3PublishesFileInventoryAndNullSidecar) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("InvertedInt64");
    auto artifact = BuildInverted(backend, true);
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

TEST(InvertedIndexArtifactTest, MissingFileInventoryIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("InvertedInt64NonNull");
    auto artifact = BuildInverted(backend, false);
    auto persisted = SerializeV3(*artifact);
    persisted.metadata.erase(FILE_NAMES);

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(InvertedIndexArtifactTest, EmptyFileInventoryIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("InvertedInt64NonNull");
    auto artifact = BuildInverted(backend, false);
    auto persisted = SerializeV3(*artifact);
    persisted.metadata[FILE_NAMES] = std::vector<std::string>{};

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(InvertedIndexArtifactTest, ReservedFileInventoryEntryIsRejected) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("InvertedInt64");
    auto artifact = BuildInverted(backend, true);
    auto persisted = SerializeV3(*artifact);
    ASSERT_TRUE(persisted.entries.contains(INDEX_NULL_OFFSET));
    persisted.metadata[FILE_NAMES] =
        std::vector<std::string>{INDEX_NULL_OFFSET};

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(InvertedIndexArtifactTest, MissingDeclaredEngineFileIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("InvertedInt64NonNull");
    auto artifact = BuildInverted(backend, false);
    auto persisted = SerializeV3(*artifact);
    const auto files =
        persisted.metadata.at(FILE_NAMES).get<std::vector<std::string>>();
    ASSERT_FALSE(files.empty());
    persisted.entries.erase(files.front());

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(InvertedIndexArtifactTest, NullSidecarMetadataMismatchIsRejected) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("InvertedInt64");
    auto artifact = BuildInverted(backend, true);
    auto persisted = SerializeV3(*artifact);
    persisted.metadata[HAS_NULL] = false;

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(InvertedIndexArtifactTest, OutOfRangeNullOffsetIsRejected) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("InvertedInt64");
    auto artifact = BuildInverted(backend, true);
    auto persisted = SerializeV3(*artifact);
    auto& offsets = persisted.entries.at(INDEX_NULL_OFFSET);
    ASSERT_GE(offsets.size(), sizeof(size_t));
    const size_t invalid = 4;
    std::memcpy(offsets.data(), &invalid, sizeof(invalid));

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(InvertedIndexArtifactTest, InvalidEnginePayloadIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("InvertedInt64NonNull");
    auto artifact = BuildInverted(backend, false);
    auto persisted = SerializeV3(*artifact);
    const auto files =
        persisted.metadata.at(FILE_NAMES).get<std::vector<std::string>>();
    ASSERT_FALSE(files.empty());
    for (const auto& file : files) {
        persisted.entries[file] = {0x00, 0x01, 0x02};
    }

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

}  // namespace
}  // namespace milvus::index::test
