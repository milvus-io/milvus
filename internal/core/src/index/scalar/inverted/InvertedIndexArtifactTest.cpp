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
