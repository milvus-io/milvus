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

#include <cstddef>
#include <cstdint>
#include <initializer_list>

#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "storage/artifact/ArtifactStats.h"

namespace milvus::indexbuilder {
namespace {

using storage::ArtifactStats;
using storage::SerializedFileInfo;

// Keep the oracle independent of SerializedFileInfo construction and of the
// native Files() result: both representations must match these literal values.
struct ExpectedFile {
    const char* name;
    int64_t size;
};

void
ExpectStatsAndCapiProjection(const ArtifactStats& stats,
                            int64_t expected_mem_size,
                            std::initializer_list<ExpectedFile> expected_files) {
    const auto projected = AdaptArtifactStats(stats);
    EXPECT_EQ(stats.MemSize(), expected_mem_size);
    EXPECT_EQ(projected.mem_size(), expected_mem_size);
    ASSERT_EQ(stats.Files().size(), expected_files.size());
    ASSERT_EQ(static_cast<size_t>(projected.serialized_index_infos_size()),
              expected_files.size());

    size_t index = 0;
    for (const auto& expected : expected_files) {
        SCOPED_TRACE(::testing::Message() << "file[" << index << "]");
        const auto& native = stats.Files()[index];
        const auto& wire =
            projected.serialized_index_infos(static_cast<int>(index));
        EXPECT_EQ(native.file_name, expected.name);
        EXPECT_EQ(native.file_size, expected.size);
        EXPECT_EQ(wire.file_name(), expected.name);
        EXPECT_EQ(wire.file_size(), expected.size);
        ++index;
    }
}

TEST(ArtifactStatsCapiProjectionTest,
     ConstructionAndAppendPreserveOrderedFileMetadata) {
    constexpr int64_t large_file_size = (int64_t{1} << 32) + 17;
    // Deliberately use a different order from filename sorting, and accounting
    // that differs from the file-size total before and after each append.
    ArtifactStats stats(37,
                        {SerializedFileInfo("z-last.bin", 19),
                         SerializedFileInfo("a/empty.meta", 0)});
    ExpectStatsAndCapiProjection(
        stats, 37, {{"z-last.bin", 19}, {"a/empty.meta", 0}});

    stats.Append(SerializedFileInfo("0-first.bin", large_file_size));
    ExpectStatsAndCapiProjection(stats,
                                37,
                                {{"z-last.bin", 19},
                                 {"a/empty.meta", 0},
                                 {"0-first.bin", large_file_size}});

    // Appending an existing name adds another entry without overwriting the
    // earlier entry or merging their sizes.
    stats.Append(SerializedFileInfo("z-last.bin", 23));
    ExpectStatsAndCapiProjection(stats,
                                37,
                                {{"z-last.bin", 19},
                                 {"a/empty.meta", 0},
                                 {"0-first.bin", large_file_size},
                                 {"z-last.bin", 23}});
}

TEST(ArtifactStatsCapiProjectionTest,
     DefaultStatsKeepZeroMemSizeWhenFilesAreAppended) {
    ArtifactStats stats;
    ExpectStatsAndCapiProjection(stats, 0, {});

    stats.Append(SerializedFileInfo("z/payload.bin", 99));
    ExpectStatsAndCapiProjection(stats, 0, {{"z/payload.bin", 99}});

    stats.Append(SerializedFileInfo("a/empty.meta", 0));
    ExpectStatsAndCapiProjection(
        stats, 0, {{"z/payload.bin", 99}, {"a/empty.meta", 0}});
}

TEST(ArtifactStatsCapiProjectionTest,
     NonzeroMemSizeSurvivesEmptyFilesAndLaterAppends) {
    constexpr int64_t mem_size = (int64_t{1} << 40) + 41;
    ArtifactStats stats(mem_size, {});
    ExpectStatsAndCapiProjection(stats, mem_size, {});

    stats.Append(SerializedFileInfo("empty.meta", 0));
    ExpectStatsAndCapiProjection(stats, mem_size, {{"empty.meta", 0}});

    stats.Append(SerializedFileInfo("small.index", 7));
    ExpectStatsAndCapiProjection(
        stats, mem_size, {{"empty.meta", 0}, {"small.index", 7}});
}

}  // namespace
}  // namespace milvus::indexbuilder
