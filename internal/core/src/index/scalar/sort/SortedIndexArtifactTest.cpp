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

#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>
#include <vector>

#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"
#include <string>
#include "index/scalar/sort/SortedIndexReader.h"
#include <filesystem>
#include <type_traits>
#include <unistd.h>
#include <variant>
#include <limits>
#include "index/scalar/sort/SortedIndexLoader.h"
#include "index/test_utils/LoaderTestAccess.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index::test {
namespace {

template <typename T>
storage::ArtifactPtr
BuildSorted(const ReaderBackend& backend, ScalarTestData<T> data) {
    const ScalarTestInput<T> input(data);
    return backend.Build(input.View(), {.row_count = data.values.size()});
}

template <typename T>
void
ExpectLegacyRoundTrip(std::string_view backend_name,
                      ScalarTestData<T> data,
                      const T& key) {
    const auto& backend = ScalarReaderBackends().Get<T>(backend_name);
    auto artifact = BuildSorted(backend, std::move(data));
    auto buffers = SerializeV1V2(*artifact);
    auto reader = OpenV1V2(backend, buffers);
    ASSERT_NE(reader, nullptr);
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<T>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    const auto hits = predicate->In(1, &key);
    ASSERT_EQ(hits.size(), 4);
    EXPECT_EQ(hits.count(), 2);
}

TEST(SortedIndexArtifactTest, LegacyNumericRoundTripRebuildsAuxiliaryState) {
    ScalarTestData<int64_t> data({1, 2, 2, 3});
    data.validity_present = false;
    ExpectLegacyRoundTrip<int64_t>("SortedInt64NonNull", std::move(data), 2);
}

TEST(SortedIndexArtifactTest, EmptyInputIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64NonNull");
    ScalarTestData<int64_t> data(std::vector<int64_t>{});
    data.validity_present = false;
    const ScalarTestInput<int64_t> input(data);
    ExpectSegcoreError(ErrorCode::DataIsEmpty, [&] {
        static_cast<void>(backend.Build(input.View(), {.row_count = 0}));
    });
}

TEST(SortedIndexArtifactTest, LegacyStringRoundTripRebuildsOffsets) {
    ScalarTestData<std::string_view> data({"a", "b", "b", "c"});
    data.validity_present = false;
    ExpectLegacyRoundTrip<std::string_view>(
        "SortedVarcharNonNull", std::move(data), "b");
}

TEST(SortedIndexArtifactTest, V3NumericPublishesCompleteAuxiliaryState) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("SortedInt64");
    ScalarTestData<int64_t> data({1, 2, 2, 3});
    data.validity.reset(3);
    auto artifact = BuildSorted(backend, std::move(data));
    const auto persisted = SerializeV3(*artifact);

    EXPECT_EQ(persisted.metadata.at(std::string(sort_format::kNumRows)), 4);
    EXPECT_EQ(persisted.metadata.at(std::string(sort_format::kNested)), false);
    EXPECT_TRUE(
        persisted.entries.contains(std::string(sort_format::kIndexData)));
    EXPECT_TRUE(
        persisted.entries.contains(std::string(sort_format::kIdxToOffsets)));
    EXPECT_TRUE(
        persisted.entries.contains(std::string(sort_format::kValidBitset)));
}

TEST(SortedIndexArtifactTest, MissingRequiredEntryIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.entries.erase(std::string(sort_format::kIndexData));

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, NestedMetadataDisagreementIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.metadata[std::string(sort_format::kNested)] = true;

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, NumericCountDisagreementIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.metadata[std::string(sort_format::kNumRows)] = 0;

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, IncompleteNumericAuxiliaryStateIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.entries.erase(std::string(sort_format::kIdxToOffsets));

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, NumericReverseOffsetSizeIsChecked) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    auto& offsets =
        persisted.entries.at(std::string(sort_format::kIdxToOffsets));
    ASSERT_FALSE(offsets.empty());
    offsets.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, UnsupportedStringVersionIsRejectedExactly) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarcharNonNull");
    ScalarTestData<std::string_view> data({"a", "b", "c"});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.metadata[std::string(sort_format::kVersion)] =
        sort_format::kStringVersion + 1;

    ExpectSegcoreError(ErrorCode::Unsupported,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, StringReverseOffsetSizeIsChecked) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarcharNonNull");
    ScalarTestData<std::string_view> data({"a", "b", "c"});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    auto& offsets =
        persisted.entries.at(std::string(sort_format::kIdxToOffsets));
    ASSERT_FALSE(offsets.empty());
    offsets.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, StringOffsetRebuildRejectsPostingRowBeyondCount) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarcharNonNull");
    ScalarTestData<std::string_view> data({"a", "b", "c"});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.entries.erase(std::string(sort_format::kIdxToOffsets));
    auto& payload = persisted.entries.at(std::string(sort_format::kIndexData));
    auto read_u32 = [&](size_t offset) {
        EXPECT_LE(offset + sizeof(uint32_t), payload.size());
        uint32_t value = 0;
        if (offset + sizeof(uint32_t) <= payload.size())
            std::memcpy(&value, payload.data() + offset, sizeof(value));
        return value;
    };
    const auto unique = read_u32(0);
    ASSERT_GT(unique, 0);
    const size_t offsets_start = sizeof(uint32_t);
    const size_t strings_start = offsets_start + unique * sizeof(uint32_t);
    ASSERT_LE(strings_start, payload.size());
    const auto last_string =
        read_u32(offsets_start + (unique - 1) * sizeof(uint32_t));
    const size_t last_length_at = strings_start + last_string;
    const size_t postings_offsets_start =
        last_length_at + sizeof(uint32_t) + read_u32(last_length_at);
    ASSERT_LE(postings_offsets_start + unique * sizeof(uint32_t),
              payload.size());
    const size_t first_posting = postings_offsets_start +
                                 unique * sizeof(uint32_t) +
                                 read_u32(postings_offsets_start);
    ASSERT_LE(first_posting + 2 * sizeof(uint32_t), payload.size());
    ASSERT_GT(read_u32(first_posting), 0);
    const uint32_t out_of_range_row = 3;
    std::memcpy(payload.data() + first_posting + sizeof(uint32_t),
                &out_of_range_row,
                sizeof(out_of_range_row));

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, TruncatedStringPayloadIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarcharNonNull");
    ScalarTestData<std::string_view> data({"a", "b", "c"});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    auto& bytes = persisted.entries.at(std::string(sort_format::kIndexData));
    ASSERT_FALSE(bytes.empty());
    bytes.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, HeapMemoryCountsInlinePostingStorageOnce) {
    std::vector<std::string> values{"a", "b"};
    std::vector<SortedStringPostingList> postings{{0, 1, 2, 3}, {4}};
    const auto expected = values.capacity() * sizeof(std::string) +
                          postings.capacity() * sizeof(SortedStringPostingList);
    auto layout =
        SortedStringLayout::FromHeap(std::move(values), std::move(postings), 5);
    EXPECT_EQ(layout->MemoryUsage(), expected);
    EXPECT_EQ(layout->Posting(0).size, 4);
    EXPECT_EQ(layout->Posting(0).At(3), 3);
}

TEST(SortedIndexArtifactTest, HeapMemoryIncludesSpilledPostingStorage) {
    std::vector<std::string> values{"a"};
    std::vector<SortedStringPostingList> postings{{0, 1, 2, 3, 4, 5, 6, 7}};
    const auto expected =
        values.capacity() * sizeof(std::string) +
        postings.capacity() * sizeof(SortedStringPostingList) +
        postings.front().capacity() * sizeof(uint32_t);
    auto layout =
        SortedStringLayout::FromHeap(std::move(values), std::move(postings), 8);
    EXPECT_EQ(layout->MemoryUsage(), expected);
    EXPECT_EQ(layout->Posting(0).At(7), 7);
}

template <typename T>
void
ExpectUntouchedMmapFiles(const char* backend_name) {
    const auto& backend = ScalarReaderBackends().Get<T>(backend_name);
    ScalarTestData<T> data = [] {
        if constexpr (std::is_same_v<T, std::string_view>) {
            return ScalarTestData<T>({"c", "a", "b"});
        } else {
            return ScalarTestData<T>({3, 1, 2});
        }
    }();
    const auto artifact = SerializeV3(*BuildSorted(backend, std::move(data)));
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            MakePackedArtifactBuffer(artifact)));
    auto source = storage::IndexEntryReader::Open(input, input->Size());
    auto staging = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "sorted-aux-unread-XXXXXX",
        "sorted lazy auxiliary test");
    storage::LoadOptions options;
    options.enable_mmap = true;
    options.mmap_dir_path = staging->Path();
    options.params = backend.LoadParams({.row_count = 3});
    auto plan = LoaderTestAccess::Plan<SortedIndexLoader>(
        source->Directory(), source->IndexMeta(), options);
    const auto files = storage::CollectIndexFileTargets(plan.entries);
    for (const auto& file : files) {
        file->Prepare(storage::io::Priority::HIGH);
    }
    for (const auto& entry : plan.entries) {
        const auto& bytes = artifact.entries.at(entry.name);
        if (const auto* memory =
                std::get_if<storage::MemoryEntryTarget>(&entry.target)) {
            std::memcpy(memory->data, bytes.data(), bytes.size());
        } else {
            const auto& file = std::get<storage::FileEntryTarget>(entry.target);
            file.staging->WriteAt(file.offset, bytes.data(), bytes.size());
        }
    }
    for (const auto& file : files) {
        file->Finish();
    }
    const auto& offsets = std::get<storage::FileEntryTarget>(
        plan.At(sort_format::kIdxToOffsets).target);
    ASSERT_EQ(::truncate(offsets.staging->path.c_str(), 0), 0);
    if constexpr (!std::is_same_v<T, std::string_view>) {
        const auto& data = std::get<storage::FileEntryTarget>(
            plan.At(sort_format::kIndexData).target);
        ASSERT_EQ(::truncate(data.staging->path.c_str(), 0), 0);
    }
    // Linux permits these file mappings, but any payload dereference raises
    // SIGBUS. The child must finish and destroy the reader without touching
    // numeric data/offsets or string offsets; string header pages remain valid.
    auto reader = LoaderTestAccess::Finish<SortedIndexLoader>(plan, options);
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 3);
}

TEST(SortedIndexArtifactDeathTest,
     MmapNumericFinalizationDoesNotTouchPayloads) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(
        {
            ExpectUntouchedMmapFiles<int64_t>("SortedInt64Mmap");
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(SortedIndexArtifactDeathTest, MmapStringFinalizationDoesNotTouchOffsets) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(
        {
            ExpectUntouchedMmapFiles<std::string_view>("SortedVarcharMmap");
            ::_exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST(SortedIndexArtifactTest, StringValueBoundsAreCheckedWhenConsumed) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarcharNonNull");
    ScalarTestData<std::string_view> data({"a", "b", "c"});
    data.validity_present = false;
    auto persisted = SerializeV3(*BuildSorted(backend, std::move(data)));
    auto& payload = persisted.entries.at(std::string(sort_format::kIndexData));
    const uint32_t invalid = std::numeric_limits<uint32_t>::max();
    // Damage an interior section entry, leaving the terminal boundaries valid.
    std::memcpy(payload.data() + sizeof(uint32_t), &invalid, sizeof(invalid));
    auto reader = OpenV3(backend, persisted, {.row_count = 3});
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
            reader.get());
    ASSERT_NE(predicate, nullptr);
    const std::string_view key = "a";
    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(predicate->In(1, &key)); });
}

}  // namespace
}  // namespace milvus::index::test
