// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "index/IndexLoaderFactory.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>

#include "index/Meta.h"
#include "storage/artifact/LocalDirectory.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

constexpr std::string_view kCsrFormatVersionMeta = "marisa_csr_format_version";
constexpr std::string_view kCsrNumKeysMeta = "csr_num_keys";

storage::ArtifactPtr
BuildMarisa(const ReaderBackend& backend) {
    ScalarTestData<std::string_view> data(
        {"alpha", "beta", "beta", "gamma", "delta"});
    if (backend.Nullable()) {
        data.validity.reset(3);
    } else {
        data.validity_present = false;
    }
    const ScalarTestInput<std::string_view> input(data);
    return backend.Build(input.View(), {.row_count = data.values.size()});
}

TestArtifactData
SerializedMarisa(std::string_view backend_name) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>(backend_name);
    auto artifact = BuildMarisa(backend);
    return SerializeV3(*artifact);
}

void
ExpectBetaRows(const IIndexReaderBase& reader) {
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(&reader);
    ASSERT_NE(predicate, nullptr);
    const std::string_view key = "beta";
    const auto hits = predicate->In(1, &key);
    ASSERT_EQ(hits.size(), 5);
    EXPECT_TRUE(hits[1]);
    EXPECT_TRUE(hits[2]);
    EXPECT_EQ(hits.count(), 2);
}

TEST(MarisaIndexArtifactTest, EmbeddedNulSurvivesBothFormats) {
    for (const auto* name :
         {"MarisaVarcharNonNull", "MarisaVarcharNonNullMmap"}) {
        SCOPED_TRACE(name);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        ScalarTestData<std::string_view> data({"a",
                                               std::string("a\0b", 3),
                                               std::string("a\0c", 3),
                                               "",
                                               std::string("\0", 1)});
        data.validity_present = false;
        const ScalarTestInput<std::string_view> input(data);
        auto artifact = backend.Build(input.View(), {.row_count = 5});
        for (const bool packed : {false, true}) {
            SCOPED_TRACE(packed);
            auto reader = packed ? OpenV3(backend, SerializeV3(*artifact))
                                 : OpenV1V2(backend, SerializeV1V2(*artifact));
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                    reader.get());
            const auto* values =
                dynamic_cast<const IScalarValueReader<std::string_view>*>(
                    reader.get());
            const auto* pattern =
                dynamic_cast<const IPatternMatchReader*>(reader.get());
            ASSERT_NE(predicate, nullptr);
            ASSERT_NE(values, nullptr);
            ASSERT_NE(pattern, nullptr);
            for (size_t row = 0; row < data.values.size(); ++row) {
                const std::string_view key = data.values[row];
                const auto hits = predicate->In(1, &key);
                EXPECT_EQ(hits.count(), 1);
                EXPECT_TRUE(hits[row]);
                ASSERT_TRUE(values->Lookup(row).has_value());
                EXPECT_EQ(*values->Lookup(row), data.values[row]);
            }
            const auto prefix = pattern->PatternMatch(
                std::string_view("a\0", 2), PatternOp::PrefixMatch);
            EXPECT_EQ(prefix.count(), 2);
            EXPECT_TRUE(prefix[1]);
            EXPECT_TRUE(prefix[2]);
        }
    }
}

TEST(MarisaIndexArtifactTest, LegacyRoundTripRebuildsCsr) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharMmap");
    auto artifact = BuildMarisa(backend);
    auto buffers = SerializeV1V2(*artifact);

    EXPECT_TRUE(buffers.entries.contains(MARISA_TRIE_INDEX));
    EXPECT_TRUE(buffers.entries.contains(MARISA_STR_IDS));
    EXPECT_FALSE(buffers.entries.contains(MARISA_CSR_INDEX));
    EXPECT_FALSE(buffers.entries.contains(MARISA_CSR_OFFSETS));

    auto reader = OpenV1V2(
        backend, buffers, {.row_count = 5, .values = Config::object()});
    artifact.reset();
    buffers = {};
    ASSERT_NE(reader, nullptr);
    ExpectBetaRows(*reader);
    const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->IsNull().count(), 1);
}

TEST(MarisaIndexArtifactTest, V3MmapReaderOwnsPersistedState) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharMmap");
    auto artifact = BuildMarisa(backend);
    auto persisted = SerializeV3(*artifact);

    EXPECT_TRUE(persisted.entries.contains(MARISA_TRIE_INDEX));
    EXPECT_TRUE(persisted.entries.contains(MARISA_STR_IDS));
    EXPECT_TRUE(persisted.entries.contains(MARISA_CSR_INDEX));
    EXPECT_TRUE(persisted.entries.contains(MARISA_CSR_OFFSETS));
    EXPECT_EQ(persisted.metadata.at(std::string(kCsrFormatVersionMeta)), 1);
    EXPECT_GT(
        persisted.metadata.at(std::string(kCsrNumKeysMeta)).get<uint64_t>(), 0);

    auto reader = OpenV3(
        backend, persisted, {.row_count = 5, .values = Config::object()});
    artifact.reset();
    persisted.entries.clear();
    persisted.metadata.clear();
    ASSERT_NE(reader, nullptr);
    ExpectBetaRows(*reader);
    EXPECT_GE(reader->MemoryUsage(), 0);
    EXPECT_GT(reader->CellByteSize().file_bytes, 0);
}

TEST(MarisaIndexArtifactTest, AsyncHeapAndMmapRoundTrip) {
    for (const auto name : {"MarisaVarchar", "MarisaVarcharMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        const auto persisted = SerializedMarisa(name);
        auto reader = OpenV3(backend, persisted, {.row_count = 5}, true);
        ASSERT_NE(reader, nullptr);
        ExpectBetaRows(*reader);
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        EXPECT_EQ(nulls->IsNull().count(), 1);
    }
}

// Cancel only payload reads, after planning has created its staging directory.
class CancelMarisaPayloadStream final : public storage::RemoteInputStream {
 public:
    CancelMarisaPayloadStream(std::shared_ptr<arrow::Buffer> bytes,
                              folly::CancellationSource& cancellation)
        : storage::RemoteInputStream(
              std::make_shared<arrow::io::BufferReader>(std::move(bytes))),
          cancellation_(cancellation) {
    }
    bool armed{false};
    folly::SemiFuture<size_t>
    ReadAtAsync(void* data, size_t offset, size_t size) override {
        if (armed) {
            cancellation_.requestCancellation();
        }
        return storage::RemoteInputStream::ReadAtAsync(data, offset, size);
    }

 private:
    folly::CancellationSource& cancellation_;
};

TEST(MarisaIndexArtifactTest, AsyncCancellationRemovesPlannedFiles) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharMmap");
    auto staging = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "marisa-cancel-XXXXXX",
        "Marisa cancellation test");
    folly::CancellationSource cancellation;
    auto input = std::make_shared<CancelMarisaPayloadStream>(
        MakePackedArtifactBuffer(SerializedMarisa(backend.Name())),
        cancellation);
    auto load = [&]() -> folly::coro::Task<IIndexReaderBasePtr> {
        const auto priority = proto::common::LoadPriority::HIGH;
        auto source =
            co_await storage::AsyncIndexEntryReader::Open(input, 0, priority);
        storage::LoadOptions opts;
        opts.enable_mmap = true;
        opts.mmap_dir_path = staging->Path();
        opts.params = AnnotateJsonProjectionCompleteness(
            backend.LoadParams({.row_count = 5}),
            source->Directory(),
            source->IndexMeta());
        const auto loader = LoaderRegistry::Instance().Lookup(backend.Family());
        input->armed = true;
        IndexOpenRequest request{
            OpenedIndexInput{PackedIndexSource{
                std::shared_ptr<storage::AsyncIndexEntryReader>(
                    std::move(source))}},
            opts};
        co_return co_await folly::coro::co_withCancellation(
            cancellation.getToken(),
            LoadIndexAsync(loader, std::move(request)));
    };
    ExpectSegcoreError(ErrorCode::FollyCancel, [&] {
        folly::coro::blockingWait(folly::coro::co_withExecutor(
            storage::ResolveAsyncLoadExecutor(
                {}, proto::common::LoadPriority::HIGH),
            load()));
    });
    EXPECT_TRUE(cancellation.isCancellationRequested());
    EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
}

struct MissingEntryCase {
    const char* name;
    const char* entry;
};

class MarisaMissingEntryTest
    : public ::testing::TestWithParam<MissingEntryCase> {};

TEST_P(MarisaMissingEntryTest, RejectsMissingRequiredEntry) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    persisted.entries.erase(GetParam().entry);

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

INSTANTIATE_TEST_SUITE_P(
    RequiredEntries,
    MarisaMissingEntryTest,
    ::testing::Values(MissingEntryCase{"MissingTrie", MARISA_TRIE_INDEX},
                      MissingEntryCase{"MissingRowIds", MARISA_STR_IDS}),
    [](const auto& info) { return info.param.name; });

TEST(MarisaIndexArtifactTest, RejectsTruncatedRowIds) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& ids = persisted.entries.at(MARISA_STR_IDS);
    ASSERT_FALSE(ids.empty());
    ids.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsOutOfRangeRowId) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& ids = persisted.entries.at(MARISA_STR_IDS);
    ASSERT_GE(ids.size(), sizeof(int64_t));
    const auto invalid = std::numeric_limits<int64_t>::max();
    std::memcpy(ids.data(), &invalid, sizeof(invalid));

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsCorruptTriePayload) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    persisted.entries[MARISA_TRIE_INDEX] = {0x00, 0x01, 0x02};

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

struct MissingCsrPartCase {
    const char* name;
    bool metadata;
    const char* key;
};

class MarisaIncompleteCsrTest
    : public ::testing::TestWithParam<MissingCsrPartCase> {};

TEST_P(MarisaIncompleteCsrTest, RejectsIncompleteCsrSideData) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    if (GetParam().metadata) {
        persisted.metadata.erase(GetParam().key);
    } else {
        persisted.entries.erase(GetParam().key);
    }

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

INSTANTIATE_TEST_SUITE_P(
    CsrParts,
    MarisaIncompleteCsrTest,
    ::testing::Values(
        MissingCsrPartCase{"MissingIndex", false, MARISA_CSR_INDEX},
        MissingCsrPartCase{"MissingOffsets", false, MARISA_CSR_OFFSETS},
        MissingCsrPartCase{
            "MissingVersion", true, kCsrFormatVersionMeta.data()},
        MissingCsrPartCase{"MissingKeyCount", true, kCsrNumKeysMeta.data()}),
    [](const auto& info) { return info.param.name; });

TEST(MarisaIndexArtifactTest, RejectsUnknownCsrVersion) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    persisted.metadata[std::string(kCsrFormatVersionMeta)] = 2;

    ExpectSegcoreError(ErrorCode::Unsupported, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsCsrKeyCountDisagreement) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& key_count = persisted.metadata[std::string(kCsrNumKeysMeta)];
    key_count = key_count.get<uint64_t>() + 1;

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsTruncatedCsrIndex) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& index = persisted.entries.at(MARISA_CSR_INDEX);
    ASSERT_FALSE(index.empty());
    index.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsTruncatedCsrOffsets) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& offsets = persisted.entries.at(MARISA_CSR_OFFSETS);
    ASSERT_FALSE(offsets.empty());
    offsets.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsCsrOffsetForWrongRow) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& offsets = persisted.entries.at(MARISA_CSR_OFFSETS);
    ASSERT_GE(offsets.size(), sizeof(uint32_t));
    const uint32_t invalid = 100;
    std::memcpy(offsets.data(), &invalid, sizeof(invalid));

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

}  // namespace
}  // namespace milvus::index::test
