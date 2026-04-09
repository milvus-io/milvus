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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cachinglayer/Utils.h"
#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "mmap/ChunkedColumnGroup.h"
#include "pb/common.pb.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "segcore/storagev2translator/ManifestGroupTranslator.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/ManifestTestUtil.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::segcore::storagev2translator;

class ManifestGroupTranslatorTest : public ::testing::TestWithParam<bool> {
    void
    SetUp() override {
        schema_ = CreateTestSchema();
        base_path_ = "manifest_translator_test";
        mmap_dir_ = TestMmapPath + "/manifest_translator_mmap/";
        std::filesystem::create_directories(mmap_dir_);

        test_data_ = std::make_unique<milvus::test::V3SegmentTestData>(
            schema_, n_batch_, per_batch_, dim_, TestLocalPath, base_path_);
    }

 protected:
    ~ManifestGroupTranslatorTest() override {
        if (std::filesystem::exists(mmap_dir_)) {
            std::filesystem::remove_all(mmap_dir_);
        }
    }

    // Helper to create a ManifestGroupTranslator for a given column group
    std::unique_ptr<ManifestGroupTranslator>
    MakeTranslator(int64_t cg_index, bool use_mmap) {
        auto chunk_reader = test_data_->CreateChunkReader(cg_index);
        auto field_metas = test_data_->GetFieldMetas(cg_index);
        return std::make_unique<ManifestGroupTranslator>(
            segment_id_,
            GroupChunkType::DEFAULT,
            cg_index,
            std::move(chunk_reader),
            field_metas,
            use_mmap,
            /*mmap_populate=*/true,
            mmap_dir_,
            field_metas.size(),
            milvus::proto::common::LoadPriority::LOW,
            /*eager_load=*/true,
            /*warmup_policy=*/"");
    }

    SchemaPtr schema_;
    std::unique_ptr<milvus::test::V3SegmentTestData> test_data_;
    std::string base_path_;
    std::string mmap_dir_;
    int64_t segment_id_ = 0;
    int64_t n_batch_ = 8;
    int64_t per_batch_ = 1000;
    int64_t dim_ = 128;
};

// Test the scalar column group (cg 0): num_cells, cell_id_of, key,
// estimated_byte_size, get_cells, ChunkedColumnGroup integration, mmap files.
TEST_P(ManifestGroupTranslatorTest, TestScalarColumnGroup) {
    // Verify schema_based policy produces 2 column groups
    ASSERT_EQ(test_data_->NumColumnGroups(), 2);
    ASSERT_EQ(test_data_->TotalRows(), n_batch_ * per_batch_);

    auto use_mmap = GetParam();
    auto translator = MakeTranslator(/*cg_index=*/0, use_mmap);

    // Verify scalar group field metas
    auto field_metas = test_data_->GetFieldMetas(0);
    for (const auto& [fid, meta] : field_metas) {
        EXPECT_FALSE(IsVectorDataType(meta.get_data_type()))
            << "cg 0 field " << fid.get() << " should be scalar";
    }

    // num_cells
    auto num_cells = translator->num_cells();
    auto chunk_reader = test_data_->CreateChunkReader(0);
    auto expected_num_chunks = chunk_reader->total_number_of_chunks();
    auto expected_num_cells =
        (expected_num_chunks + kRowGroupsPerCell - 1) / kRowGroupsPerCell;
    EXPECT_EQ(num_cells, expected_num_cells);

    // cell_id_of — identity mapping
    for (size_t i = 0; i < num_cells; ++i) {
        EXPECT_EQ(translator->cell_id_of(i), i);
    }

    // key
    EXPECT_EQ(translator->key(), "seg_0_cg_0");

    // estimated byte size
    for (size_t i = 0; i < num_cells; ++i) {
        auto [loading, storage] = translator->estimated_byte_size_of_cell(i);
        if (use_mmap) {
            EXPECT_GT(loading.file_bytes, 0) << "cid=" << i;
            EXPECT_GT(storage.file_bytes, 0) << "cid=" << i;
        } else {
            EXPECT_GT(loading.memory_bytes, 0) << "cid=" << i;
            EXPECT_GT(storage.memory_bytes, 0) << "cid=" << i;
        }
    }

    // get_cells — load all cells
    std::vector<cachinglayer::cid_t> cids;
    for (size_t i = 0; i < num_cells; ++i) {
        cids.push_back(static_cast<cachinglayer::cid_t>(i));
    }
    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), cids.size());
    for (size_t i = 0; i < cells.size(); ++i) {
        EXPECT_EQ(cells[i].first, cids[i]);
        EXPECT_NE(cells[i].second, nullptr);
    }

    // DataByteSize from meta
    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    size_t expected_total_size = 0;
    for (const auto& chunk_size : meta->chunk_memory_size_) {
        expected_total_size += chunk_size;
    }
    EXPECT_GT(expected_total_size, 0);

    // ChunkedColumnGroup integration
    auto saved_num_cells = num_cells;
    auto chunked_column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));
    EXPECT_EQ(meta->chunk_memory_size_.size(), saved_num_cells);
    EXPECT_EQ(expected_total_size, chunked_column_group->memory_size());

    // Verify mmap files if in mmap mode (file names include generation suffix)
    if (use_mmap) {
        size_t mmap_file_count = 0;
        for (const auto& entry :
             std::filesystem::directory_iterator(mmap_dir_)) {
            auto name = entry.path().filename().string();
            if (name.rfind("seg_0_cg_0_", 0) == 0) {
                mmap_file_count++;
            }
        }
        EXPECT_EQ(mmap_file_count, saved_num_cells);
    }
}

// Test the vector column group (cg 1): similar checks, single vector field.
TEST_P(ManifestGroupTranslatorTest, TestVectorColumnGroup) {
    ASSERT_EQ(test_data_->NumColumnGroups(), 2);

    auto use_mmap = GetParam();
    auto translator = MakeTranslator(/*cg_index=*/1, use_mmap);

    // Verify vector group field metas
    auto field_metas = test_data_->GetFieldMetas(1);
    EXPECT_EQ(field_metas.size(), 1);
    for (const auto& [fid, meta] : field_metas) {
        EXPECT_TRUE(IsVectorDataType(meta.get_data_type()))
            << "cg 1 field " << fid.get() << " should be vector";
    }

    // num_cells
    auto num_cells = translator->num_cells();
    EXPECT_GT(num_cells, 0);

    // key
    EXPECT_EQ(translator->key(), "seg_0_cg_1");

    // estimated byte size
    for (size_t i = 0; i < num_cells; ++i) {
        auto [loading, storage] = translator->estimated_byte_size_of_cell(i);
        if (use_mmap) {
            EXPECT_GT(loading.file_bytes, 0) << "cid=" << i;
        } else {
            EXPECT_GT(loading.memory_bytes, 0) << "cid=" << i;
        }
    }

    // get_cells
    std::vector<cachinglayer::cid_t> cids;
    for (size_t i = 0; i < num_cells; ++i) {
        cids.push_back(static_cast<cachinglayer::cid_t>(i));
    }
    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), cids.size());
    for (size_t i = 0; i < cells.size(); ++i) {
        EXPECT_EQ(cells[i].first, cids[i]);
        EXPECT_NE(cells[i].second, nullptr);
    }

    // ChunkedColumnGroup integration
    auto saved_num_cells = num_cells;
    auto chunked_column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));
    EXPECT_EQ(chunked_column_group->num_chunks(), saved_num_cells);
    EXPECT_GT(chunked_column_group->memory_size(), 0);

    // Verify mmap files if in mmap mode (file names include generation suffix)
    if (use_mmap) {
        size_t mmap_file_count = 0;
        for (const auto& entry :
             std::filesystem::directory_iterator(mmap_dir_)) {
            auto name = entry.path().filename().string();
            if (name.rfind("seg_0_cg_1_", 0) == 0) {
                mmap_file_count++;
            }
        }
        EXPECT_EQ(mmap_file_count, saved_num_cells);
    }
}

// Test get_cells with cids in reverse order to verify order preservation.
TEST_P(ManifestGroupTranslatorTest, TestGetCellsOrderPreservation) {
    auto use_mmap = GetParam();
    auto translator = MakeTranslator(/*cg_index=*/0, use_mmap);
    auto num_cells = translator->num_cells();
    ASSERT_GT(num_cells, 1);

    // Request cells in reverse order
    std::vector<cachinglayer::cid_t> reverse_cids;
    for (size_t i = num_cells; i > 0; --i) {
        reverse_cids.push_back(static_cast<cachinglayer::cid_t>(i - 1));
    }
    auto cells = translator->get_cells(nullptr, reverse_cids);
    EXPECT_EQ(cells.size(), reverse_cids.size());

    // Returned cids should be in the same order as input (reverse)
    for (size_t i = 0; i < cells.size(); ++i) {
        EXPECT_EQ(cells[i].first, reverse_cids[i])
            << "Order mismatch at index " << i;
    }

    // Also test with a subset of cids
    if (num_cells >= 3) {
        std::vector<cachinglayer::cid_t> subset_cids = {
            static_cast<cachinglayer::cid_t>(num_cells - 1), 0, 1};
        auto subset_cells = translator->get_cells(nullptr, subset_cids);
        EXPECT_EQ(subset_cells.size(), subset_cids.size());
        for (size_t i = 0; i < subset_cells.size(); ++i) {
            EXPECT_EQ(subset_cells[i].first, subset_cids[i]);
        }
    }
}

// Test cells_storage_bytes sums up correctly.
TEST_P(ManifestGroupTranslatorTest, TestCellsStorageBytes) {
    auto use_mmap = GetParam();
    auto translator = MakeTranslator(/*cg_index=*/0, use_mmap);
    auto num_cells = translator->num_cells();
    auto meta = static_cast<GroupCTMeta*>(translator->meta());

    // cells_storage_bytes for all cids
    std::vector<cachinglayer::cid_t> all_cids;
    for (size_t i = 0; i < num_cells; ++i) {
        all_cids.push_back(static_cast<cachinglayer::cid_t>(i));
    }
    auto total_bytes = translator->cells_storage_bytes(all_cids);
    EXPECT_GT(total_bytes, 0);

    // Should match summing individual cells (with min 1MB per cell)
    constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
    int64_t expected_total = 0;
    for (size_t i = 0; i < num_cells; ++i) {
        expected_total +=
            std::max(meta->chunk_memory_size_[i], MIN_STORAGE_BYTES);
    }
    EXPECT_EQ(total_bytes, expected_total);
}

// Test num_rows_until_chunk prefix sums are correct.
TEST_P(ManifestGroupTranslatorTest, TestNumRowsUntilChunk) {
    auto use_mmap = GetParam();

    for (int64_t cg_idx = 0; cg_idx < test_data_->NumColumnGroups(); ++cg_idx) {
        auto translator = MakeTranslator(cg_idx, use_mmap);
        auto meta = static_cast<GroupCTMeta*>(translator->meta());

        // num_rows_until_chunk_ is prefix sum with size = num_cells + 1
        EXPECT_EQ(meta->num_rows_until_chunk_.size(),
                  translator->num_cells() + 1);

        // First element should be 0
        EXPECT_EQ(meta->num_rows_until_chunk_[0], 0);

        // Should be monotonically increasing
        for (size_t i = 1; i < meta->num_rows_until_chunk_.size(); ++i) {
            EXPECT_GT(meta->num_rows_until_chunk_[i],
                      meta->num_rows_until_chunk_[i - 1])
                << "non-increasing at index " << i << " for cg " << cg_idx;
        }

        // Total rows across cells should match what the chunk reader
        // reports. The Parquet writer may split input batches into more
        // row groups, so this can exceed n_batch * per_batch.
        auto chunk_reader = test_data_->CreateChunkReader(cg_idx);
        auto chunk_rows_result = chunk_reader->get_chunk_rows();
        ASSERT_TRUE(chunk_rows_result.ok());
        int64_t expected_total_rows = 0;
        for (auto r : chunk_rows_result.ValueOrDie()) {
            expected_total_rows += static_cast<int64_t>(r);
        }
        EXPECT_EQ(meta->num_rows_until_chunk_.back(), expected_total_rows)
            << "total rows mismatch for cg " << cg_idx;
    }
}

// Test cell_row_group_ranges_ cover all row groups without gaps.
TEST_P(ManifestGroupTranslatorTest, TestRowGroupRangesCoverage) {
    auto use_mmap = GetParam();
    auto translator = MakeTranslator(/*cg_index=*/0, use_mmap);
    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    auto num_cells = translator->num_cells();

    EXPECT_EQ(meta->cell_row_group_ranges_.size(), num_cells);

    // Ranges should be contiguous and cover [0, total_row_groups_)
    size_t expected_start = 0;
    for (size_t cid = 0; cid < num_cells; ++cid) {
        auto [start, end] = meta->get_row_group_range(cid);
        EXPECT_EQ(start, expected_start) << "gap at cid " << cid;
        EXPECT_GT(end, start) << "empty range at cid " << cid;
        EXPECT_LE(end - start, kRowGroupsPerCell)
            << "range too large at cid " << cid;
        expected_start = end;
    }
    EXPECT_EQ(expected_start, meta->total_row_groups_);
}

INSTANTIATE_TEST_SUITE_P(ManifestGroupTranslator,
                         ManifestGroupTranslatorTest,
                         testing::Bool());
