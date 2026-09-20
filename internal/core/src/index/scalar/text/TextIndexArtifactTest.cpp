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

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/Meta.h"
#include "index/scalar/text/TextIndexLoader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

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
            reader = LoaderTestAccess::Finish<TextIndexLoader>(plan, opts);
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

}  // namespace
}  // namespace milvus::index::test
