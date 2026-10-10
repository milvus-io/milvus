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
#include <filesystem>
#include <string>
#include <vector>

#include "common/Geometry.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/ISpatialReader.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"

namespace milvus::indexbuilder::test {
namespace {

std::string
Wkb(const std::string& wkt) {
    return Geometry(GetThreadLocalGEOSContext(), wkt.c_str()).to_wkb_string();
}

class RTreeBuildSessionTest : public SourceBuildTest {
 protected:
    PreparedBuild
    WriteGeometry(const std::vector<std::string>& values,
                  const std::vector<uint8_t>& validity = {}) {
        const auto prepared = Prepare(DataType::GEOMETRY,
                                       DataType::NONE,
                                       !validity.empty(),
                                       index::RTREE_INDEX_TYPE,
                                       values.size(),
                                       0);
        auto data = storage::CreateFieldData(
            DataType::GEOMETRY, DataType::NONE, !validity.empty());
        if (validity.empty()) {
            data->FillFieldData(values.data(), values.size());
        } else {
            data->FillFieldData(values.data(), validity.data(), values.size(), 0);
        }
        WriteInsert(prepared, data);
        return prepared;
    }
};

// Replaces the duplicate config/metadata and two-row end-to-end tests: both
// exercised the same binlog -> packed artifact -> independent load lifecycle.
TEST_F(RTreeBuildSessionTest, BinlogBuildPublishesOnePackedArtifact) {
    const std::vector<std::string> values{Wkb("POINT(0 0)"), Wkb("POINT(2 2)")};
    const auto prepared = WriteGeometry(values);
    const auto stats = Publish(prepared);
    ASSERT_EQ(stats.Files().size(), 1);
    EXPECT_GT(stats.Files().front().file_size, 0);
    const auto reader = Open(prepared, stats);
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), values.size());
    const auto* spatial = dynamic_cast<const index::ISpatialReader*>(reader.get());
    ASSERT_NE(spatial, nullptr);
    const Geometry query(GetThreadLocalGEOSContext(), "POINT(0 0)");
    const auto candidates = spatial->Candidates(index::SpatialOp::Intersects, query);
    ASSERT_EQ(candidates.size(), values.size());
    EXPECT_TRUE(candidates[0]);
    EXPECT_FALSE(candidates[1]);
}

TEST_F(RTreeBuildSessionTest, PackedLoadResolvesBasenameAndFullPath) {
    const std::vector<std::string> values{Wkb("POINT(6 6)"), Wkb("POINT(7 7)")};
    const auto prepared = WriteGeometry(values);
    const auto stats = Publish(prepared);
    ASSERT_EQ(stats.Files().size(), 1);
    const auto basename =
        std::filesystem::path(stats.Files().front().file_name).filename().string();
    const auto by_name = Open(prepared, stats, false, {basename});
    const auto by_path =
        Open(prepared, stats, false, {directory_->Path() + "/" + basename});
    ASSERT_NE(by_name, nullptr);
    ASSERT_NE(by_path, nullptr);
    EXPECT_EQ(by_name->Count(), values.size());
    EXPECT_EQ(by_path->Count(), values.size());
}

TEST_F(RTreeBuildSessionTest, MissingRemoteArtifactFailsLoad) {
    const auto prepared = Prepare(DataType::GEOMETRY,
                                   DataType::NONE,
                                   false,
                                   index::RTREE_INDEX_TYPE,
                                   2,
                                   0);
    EXPECT_THROW(static_cast<void>(Open(prepared,
                                        storage::ArtifactStats{},
                                        false,
                                        {"does_not_exist.bgi"})),
                 SegcoreError);
}

TEST_F(RTreeBuildSessionTest, LargeBinlogRoundTripPreservesRows) {
    constexpr size_t rows = 10000;
    std::vector<std::string> values;
    values.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const auto coordinate = std::to_string(row);
        values.push_back(Wkb("POINT(" + coordinate + " " + coordinate + ")"));
    }
    const auto prepared = WriteGeometry(values);
    const auto stats = Publish(prepared);
    ASSERT_FALSE(stats.Files().empty());
    const auto reader = Open(prepared, stats);
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(reader->Count(), rows);
}

TEST_F(RTreeBuildSessionTest, BinlogNullAndCorruptWkbKeepAbsoluteRows) {
    std::vector<std::string> values{Wkb("POINT(0 0)"),
                                     Wkb("POINT(1 1)"),
                                     Wkb("POINT(2 2)"),
                                     Wkb("POINT(3 3)"),
                                     Wkb("POINT(4 4)")};
    values[3].resize(values[3].size() / 2);
    const auto prepared = WriteGeometry(values, {0b00011101});
    const auto stats = Publish(prepared);
    const auto reader = Open(prepared, stats);
    ASSERT_NE(reader, nullptr);
    ASSERT_EQ(reader->Count(), values.size());
    const auto* nulls = dynamic_cast<const index::INullReader*>(reader.get());
    const auto* spatial = dynamic_cast<const index::ISpatialReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    ASSERT_NE(spatial, nullptr);
    const auto null_bits = nulls->IsNull();
    ASSERT_EQ(null_bits.size(), values.size());
    for (size_t row = 0; row < values.size(); ++row) {
        EXPECT_EQ(null_bits[row], row == 1);
    }
    const Geometry query(GetThreadLocalGEOSContext(), "POINT(0 0)");
    const auto candidates = spatial->Candidates(index::SpatialOp::Intersects, query);
    ASSERT_EQ(candidates.size(), values.size());
    EXPECT_TRUE(candidates[0]);
    EXPECT_TRUE(candidates[3]);
    EXPECT_FALSE(candidates[1]);
}

}  // namespace
}  // namespace milvus::indexbuilder::test
