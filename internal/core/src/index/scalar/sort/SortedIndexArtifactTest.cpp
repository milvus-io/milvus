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
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

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

TEST(SortedIndexArtifactTest, NumericMultipleBatchesPreserveRowsAndElements) {
    for (const bool nested : {false, true}) {
        SCOPED_TRACE(nested);
        const auto& backend = ScalarReaderBackends().Get<int64_t>(
            nested ? "SortedInt64Nested" : "SortedInt64");
        ScalarTestData<int64_t> data(
            {4, 3, 1, 3, 8, 2, 3, 0, 5, 3, 9, 1, 3, 6, 2, 3, 7});
        // Include empty batches, an all-null batch in the row case, and
        // validity subviews that start between packed-byte boundaries.
        data.batch_sizes = {0, 3, 0, 5, 1, 8, 0};
        if (nested) {
            data.domain = Domain::Element;
            data.validity_present = false;
        } else {
            for (const size_t row : {1, 3, 4, 5, 6, 7, 9, 15}) {
                data.validity.reset(row);
            }
        }
        const ScalarTestInput<int64_t> input(data);
        auto artifact =
            backend.Build(input.View(), {.row_count = data.values.size()});
        for (const bool packed : {false, true}) {
            SCOPED_TRACE(packed);
            auto reader = packed ? OpenV3(backend, SerializeV3(*artifact))
                                 : OpenV1V2(backend, SerializeV1V2(*artifact));
            ASSERT_NE(reader, nullptr);
            EXPECT_EQ(reader->Count(), data.values.size());
            EXPECT_EQ(reader->CoordDomain(),
                      nested ? Domain::Element : Domain::Row);
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<int64_t>*>(
                    reader.get());
            const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
            ASSERT_NE(predicate, nullptr);
            ASSERT_NE(nulls, nullptr);
            const int64_t key = 3;
            const auto in = predicate->In(1, &key);
            const auto not_in = predicate->NotIn(1, &key);
            const auto range = predicate->Range(2, true, 6, false);
            const auto is_null = nulls->IsNull();
            const auto is_not_null = nulls->IsNotNull();
            ASSERT_EQ(in.size(), data.values.size());
            ASSERT_EQ(not_in.size(), data.values.size());
            ASSERT_EQ(range.size(), data.values.size());
            ASSERT_EQ(is_null.size(), data.values.size());
            ASSERT_EQ(is_not_null.size(), data.values.size());
            for (size_t row = 0; row < data.values.size(); ++row) {
                const bool valid = nested || data.validity[row];
                EXPECT_EQ(in[row], valid && data.values[row] == key) << row;
                EXPECT_EQ(not_in[row], valid && data.values[row] != key) << row;
                EXPECT_EQ(
                    range[row],
                    valid && data.values[row] >= 2 && data.values[row] < 6)
                    << row;
                EXPECT_EQ(is_null[row], !valid) << row;
                EXPECT_EQ(is_not_null[row], valid) << row;
            }
        }
    }
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

TEST(SortedIndexArtifactTest, InvalidNumericReverseOffsetIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("SortedInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    auto& offsets =
        persisted.entries.at(std::string(sort_format::kIdxToOffsets));
    ASSERT_GE(offsets.size(), sizeof(int32_t));
    const auto invalid = std::numeric_limits<int32_t>::max();
    std::memcpy(offsets.data(), &invalid, sizeof(invalid));

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

TEST(SortedIndexArtifactTest, InvalidStringReverseOffsetIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarcharNonNull");
    ScalarTestData<std::string_view> data({"a", "b", "c"});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    auto& offsets =
        persisted.entries.at(std::string(sort_format::kIdxToOffsets));
    ASSERT_GE(offsets.size(), sizeof(int32_t));
    const auto invalid = std::numeric_limits<int32_t>::max();
    std::memcpy(offsets.data(), &invalid, sizeof(invalid));

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(SortedIndexArtifactTest, StringPostingRowBeyondCountIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SortedVarcharNonNull");
    ScalarTestData<std::string_view> data({"a", "b", "c"});
    data.validity_present = false;
    auto artifact = BuildSorted(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
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

}  // namespace
}  // namespace milvus::index::test
