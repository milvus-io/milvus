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
