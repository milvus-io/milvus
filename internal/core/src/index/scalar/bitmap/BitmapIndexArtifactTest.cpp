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
#include <roaring/roaring.hh>

#include "common/Consts.h"
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

TEST(BitmapIndexArtifactTest, MalformedPostingsWithValidPackedCrcAreRejected) {
    const int32_t key = 7;
    roaring::Roaring posting;
    posting.add(0);
    std::vector<uint8_t> valid(sizeof(key) + posting.getSizeInBytes());
    std::memcpy(valid.data(), &key, sizeof(key));
    posting.write(reinterpret_cast<char*>(valid.data() + sizeof(key)));
    std::vector<std::vector<uint8_t>> invalid;
    invalid.emplace_back(valid.begin(), valid.begin() + sizeof(key) - 1);
    invalid.emplace_back(valid.begin(), valid.end() - 1);
    invalid.push_back(valid);
    invalid.back().push_back(0);
    posting = roaring::Roaring();
    posting.add(100);
    invalid.emplace_back(sizeof(key) + posting.getSizeInBytes());
    std::memcpy(invalid.back().data(), &key, sizeof(key));
    posting.write(reinterpret_cast<char*>(invalid.back().data() + sizeof(key)));
    const uint32_t malformed_header[] = {
        static_cast<uint32_t>(key),
        roaring::internal::SERIAL_COOKIE_NO_RUNCONTAINER,
        UINT32_MAX};
    const auto* header = reinterpret_cast<const uint8_t*>(malformed_header);
    invalid.emplace_back(header, header + sizeof(malformed_header));

    for (bool mmap : {false, true}) {
        const auto& backend = ScalarReaderBackends().Get<int32_t>(
            mmap ? "BitmapInt32NonNullMmap" : "BitmapInt32NonNull");
        ScalarTestData<int32_t> data({key});
        data.validity_present = false;
        auto artifact = BuildBitmap(backend, std::move(data));
        const auto baseline = SerializeV3(*artifact);
        for (size_t shape = 0; shape < invalid.size(); ++shape) {
            SCOPED_TRACE(mmap);
            SCOPED_TRACE(shape);
            auto corrupted = baseline;
            corrupted.entries[BITMAP_INDEX_DATA] = invalid[shape];
            corrupted.metadata[BITMAP_INDEX_LENGTH] =
                mmap ? DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND + 1 : 1;
            // OpenV3 packs every mutated entry with a fresh CRC. Failures
            // therefore come from posting validation, not the envelope.
            ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
                static_cast<void>(OpenV3(backend, corrupted));
            });
        }
    }
}

}  // namespace
}  // namespace milvus::index::test
