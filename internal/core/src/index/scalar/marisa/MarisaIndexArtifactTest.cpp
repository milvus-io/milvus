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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>

#include "index/Meta.h"
#include "index/contracts/query/INullReader.h"
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

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
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
