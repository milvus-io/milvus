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
#include <string_view>
#include <utility>
#include <vector>

#include "index/Meta.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

template <typename T>
storage::ArtifactPtr
BuildBitmap(const ReaderBackend& backend, ScalarTestData<T> data) {
    const ScalarTestInput<T> input(data);
    return backend.Build(input.View(), {.row_count = data.values.size()});
}

template <typename T>
void
ExpectLegacyRoundTrip(std::string_view backend_name,
                      ScalarTestData<T> data,
                      const T& key,
                      size_t expected_hits) {
    const auto& backend = ScalarReaderBackends().Get<T>(backend_name);
    auto artifact = BuildBitmap(backend, std::move(data));
    auto buffers = SerializeV1V2(*artifact);
    auto reader = OpenV1V2(backend, buffers);
    ASSERT_NE(reader, nullptr);
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<T>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    const auto hits = predicate->In(1, &key);
    ASSERT_EQ(hits.size(), 4);
    EXPECT_EQ(hits.count(), expected_hits);
}

TEST(BitmapIndexArtifactTest, LegacyNumericRoundTrip) {
    ScalarTestData<int64_t> data({1, 2, 2, 3});
    data.validity_present = false;
    ExpectLegacyRoundTrip<int64_t>("BitmapInt64NonNull", std::move(data), 2, 2);
}

TEST(BitmapIndexArtifactTest, EmptyInputIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("BitmapInt64NonNull");
    ScalarTestData<int64_t> data(std::vector<int64_t>{});
    data.validity_present = false;
    const ScalarTestInput<int64_t> input(data);
    ExpectSegcoreError(ErrorCode::DataIsEmpty, [&] {
        static_cast<void>(backend.Build(input.View(), {.row_count = 0}));
    });
}

TEST(BitmapIndexArtifactTest, LegacyStringRoundTrip) {
    ScalarTestData<std::string_view> data({"a", "b", "b", "c"});
    data.validity_present = false;
    ExpectLegacyRoundTrip<std::string_view>(
        "BitmapVarcharNonNull", std::move(data), "b", 2);
}

TEST(BitmapIndexArtifactTest, V3PublishesTypedMetadataAndEntries) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    ScalarTestData<int64_t> data({1, 2, 2, 3});
    data.validity.reset(3);
    auto artifact = BuildBitmap(backend, std::move(data));
    const auto persisted = SerializeV3(*artifact);

    EXPECT_TRUE(persisted.metadata.contains(BITMAP_INDEX_LENGTH));
    EXPECT_EQ(persisted.metadata.at(BITMAP_INDEX_NUM_ROWS), 4);
    EXPECT_EQ(persisted.metadata.at("is_nested"), false);
    EXPECT_TRUE(persisted.entries.contains(BITMAP_INDEX_DATA));
    EXPECT_TRUE(persisted.entries.contains(BITMAP_INDEX_VALID_BITSET));
}

TEST(BitmapIndexArtifactTest, MissingRequiredV3MetadataIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("BitmapInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildBitmap(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.metadata.erase(BITMAP_INDEX_NUM_ROWS);

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(BitmapIndexArtifactTest, MissingRequiredV3EntryIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("BitmapInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildBitmap(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.entries.erase(BITMAP_INDEX_DATA);

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(BitmapIndexArtifactTest, InvalidCountRelationshipIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("BitmapInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildBitmap(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.metadata[BITMAP_INDEX_NUM_ROWS] = 0;

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(BitmapIndexArtifactTest, TruncatedValidityIsRejected) {
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity.reset(1);
    auto artifact = BuildBitmap(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    auto& validity = persisted.entries.at(BITMAP_INDEX_VALID_BITSET);
    ASSERT_FALSE(validity.empty());
    validity.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(BitmapIndexArtifactTest, NestedMetadataDisagreementIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("BitmapInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildBitmap(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    persisted.metadata["is_nested"] = true;

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

TEST(BitmapIndexArtifactTest, TruncatedPostingPayloadIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<int64_t>("BitmapInt64NonNull");
    ScalarTestData<int64_t> data({1, 2, 3});
    data.validity_present = false;
    auto artifact = BuildBitmap(backend, std::move(data));
    auto persisted = SerializeV3(*artifact);
    ASSERT_FALSE(persisted.entries.at(BITMAP_INDEX_DATA).empty());
    persisted.entries.at(BITMAP_INDEX_DATA).pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken,
                       [&] { static_cast<void>(OpenV3(backend, persisted)); });
}

}  // namespace
}  // namespace milvus::index::test
