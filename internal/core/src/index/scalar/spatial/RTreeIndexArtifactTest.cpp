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

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Geometry.h"
#include "index/Meta.h"
#include "index/contracts/query/INullReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

std::string
Wkb(std::string_view wkt) {
    return Geometry(GetThreadLocalGEOSContext(), std::string(wkt).c_str())
        .to_wkb_string();
}

storage::ArtifactPtr
BuildRTree(const ReaderBackend& backend, bool nullable) {
    ScalarTestData<std::string_view> data(
        {Wkb("POINT(0 0)"), Wkb("POINT(1 1)"), Wkb("POINT(2 2)")});
    if (nullable) {
        data.validity.reset(1);
    } else {
        data.validity_present = false;
    }
    const ScalarTestInput<std::string_view> input(data);
    return backend.Build(input.View(), {.row_count = data.values.size()});
}

TEST(RTreeIndexArtifactTest, V3RoundTripPreservesFileAndNullInventory) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SpatialRTreeHeap");
    auto artifact = BuildRTree(backend, true);
    const auto persisted = SerializeV3(*artifact);
    const auto files =
        persisted.metadata.at(FILE_NAMES).get<std::vector<std::string>>();
    EXPECT_FALSE(files.empty());
    EXPECT_EQ(std::count_if(files.begin(),
                            files.end(),
                            [](const auto& file) {
                                return std::string_view(file).ends_with(".bgi");
                            }),
              1);
    for (const auto& file : files) {
        EXPECT_TRUE(persisted.entries.contains(file));
    }
    EXPECT_EQ(persisted.metadata.at(HAS_NULL), true);
    EXPECT_TRUE(persisted.entries.contains(INDEX_NULL_OFFSET));

    auto reader = OpenV3(backend, persisted, {.row_count = 3});
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), 3);
    const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_TRUE(nulls->IsNull()[1]);
}

TEST(RTreeIndexArtifactTest, MissingFileInventoryIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SpatialRTreeHeapNonNull");
    auto artifact = BuildRTree(backend, false);
    auto persisted = SerializeV3(*artifact);
    persisted.metadata.erase(FILE_NAMES);

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 3}));
    });
}

TEST(RTreeIndexArtifactTest, MissingDeclaredArchiveIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SpatialRTreeHeapNonNull");
    auto artifact = BuildRTree(backend, false);
    auto persisted = SerializeV3(*artifact);
    const auto files =
        persisted.metadata.at(FILE_NAMES).get<std::vector<std::string>>();
    const auto archive =
        std::find_if(files.begin(), files.end(), [](const auto& file) {
            return std::string_view(file).ends_with(".bgi");
        });
    ASSERT_NE(archive, files.end());
    persisted.entries.erase(*archive);

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 3}));
    });
}

TEST(RTreeIndexArtifactTest, NullSidecarMetadataMismatchIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SpatialRTreeHeap");
    auto artifact = BuildRTree(backend, true);
    auto persisted = SerializeV3(*artifact);
    persisted.metadata[HAS_NULL] = false;

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 3}));
    });
}

TEST(RTreeIndexArtifactTest, InvalidArchivePayloadIsRejected) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("SpatialRTreeHeapNonNull");
    auto artifact = BuildRTree(backend, false);
    auto persisted = SerializeV3(*artifact);
    const auto files =
        persisted.metadata.at(FILE_NAMES).get<std::vector<std::string>>();
    const auto archive =
        std::find_if(files.begin(), files.end(), [](const auto& file) {
            return std::string_view(file).ends_with(".bgi");
        });
    ASSERT_NE(archive, files.end());
    persisted.entries[*archive] = {0x00, 0x01, 0x02};

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 3}));
    });
}

}  // namespace
}  // namespace milvus::index::test
