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

enum class ColumnEstimateMode {
    UNAVAILABLE,
    ZERO,
    FIRST_ZERO,
};

class ColumnEstimateTestChunkReader : public milvus_storage::api::ChunkReader {
 public:
    explicit ColumnEstimateTestChunkReader(
        std::unique_ptr<milvus_storage::api::ChunkReader> delegate,
        bool total_estimate_available = true,
        ColumnEstimateMode column_estimate_mode =
            ColumnEstimateMode::UNAVAILABLE,
        size_t* named_lookup_count = nullptr)
        : delegate_(std::move(delegate)),
          total_estimate_available_(total_estimate_available),
          column_estimate_mode_(column_estimate_mode),
          named_lookup_count_(named_lookup_count) {
    }

    size_t
    total_number_of_chunks() const override {
        return delegate_->total_number_of_chunks();
    }

    arrow::Result<std::vector<int64_t>>
    get_chunk_indices(const std::vector<int64_t>& row_indices) override {
        return delegate_->get_chunk_indices(row_indices);
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    get_chunk(int64_t chunk_index) override {
        return delegate_->get_chunk(chunk_index);
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
    get_chunks(const std::vector<int64_t>& chunk_indices,
               size_t parallelism) override {
        return delegate_->get_chunks(chunk_indices, parallelism);
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_estimated_size() override {
        if (!total_estimate_available_) {
            return arrow::Status::NotImplemented(
                "total estimate unavailable for last-resort test");
        }
        return delegate_->get_chunk_estimated_size();
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_column_estimated_size(const std::string& field_name) override {
        if (named_lookup_count_ != nullptr) {
            ++*named_lookup_count_;
        }
        if (column_estimate_mode_ == ColumnEstimateMode::UNAVAILABLE) {
            return arrow::Status::NotImplemented(
                "column estimate unavailable for fallback test");
        }
        auto result = delegate_->get_chunk_column_estimated_size(field_name);
        if (!result.ok()) {
            return result.status();
        }
        auto sizes = std::move(result).ValueOrDie();
        if (column_estimate_mode_ == ColumnEstimateMode::ZERO) {
            std::fill(sizes.begin(), sizes.end(), 0);
        } else if (column_estimate_mode_ == ColumnEstimateMode::FIRST_ZERO &&
                   !sizes.empty()) {
            sizes.front() = 0;
        }
        return sizes;
    }

    arrow::Result<std::vector<std::vector<uint64_t>>>
    get_chunk_column_estimated_size() override {
        if (column_estimate_mode_ == ColumnEstimateMode::UNAVAILABLE) {
            return arrow::Status::NotImplemented(
                "column estimate unavailable for fallback test");
        }
        auto result = delegate_->get_chunk_column_estimated_size();
        if (!result.ok()) {
            return result.status();
        }
        auto sizes = std::move(result).ValueOrDie();
        if (column_estimate_mode_ == ColumnEstimateMode::ZERO) {
            for (auto& column_sizes : sizes) {
                std::fill(column_sizes.begin(), column_sizes.end(), 0);
            }
        } else if (column_estimate_mode_ == ColumnEstimateMode::FIRST_ZERO) {
            for (auto& column_sizes : sizes) {
                if (!column_sizes.empty()) {
                    column_sizes.front() = 0;
                }
            }
        }
        return sizes;
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_rows() override {
        return delegate_->get_chunk_rows();
    }

 private:
    std::unique_ptr<milvus_storage::api::ChunkReader> delegate_;
    bool total_estimate_available_;
    ColumnEstimateMode column_estimate_mode_;
    size_t* named_lookup_count_;
};

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
            test_data_->GetColumnGroups()->at(cg_index)->columns,
            /*full_projection=*/true,
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
    auto row_group_sizes =
        chunk_reader->get_chunk_estimated_size().ValueOrDie();
    auto rgs_per_cell =
        ComputeRowGroupsPerCell(row_group_sizes, GetCellTargetSizeBytes());
    auto expected_num_cells =
        (expected_num_chunks + rgs_per_cell - 1) / rgs_per_cell;
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

TEST_P(ManifestGroupTranslatorTest, TestFullProjectionUsesTotalEstimate) {
    const auto& column_group = test_data_->GetColumnGroups()->at(0);
    ASSERT_GT(column_group->columns.size(), 1);

    auto needed_columns =
        std::make_shared<std::vector<std::string>>(column_group->columns);
    auto delegate = test_data_->CreateChunkReader(0, needed_columns);

    auto total_sizes_result = delegate->get_chunk_estimated_size();
    ASSERT_TRUE(total_sizes_result.ok())
        << total_sizes_result.status().ToString();
    const auto& total_sizes = total_sizes_result.ValueOrDie();

    auto rgs_per_cell =
        ComputeRowGroupsPerCell(total_sizes, GetCellTargetSizeBytes());
    std::vector<int64_t> expected_cell_sizes;
    for (size_t start = 0; start < total_sizes.size(); start += rgs_per_cell) {
        const auto end = std::min(start + rgs_per_cell, total_sizes.size());
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cell_size += static_cast<int64_t>(total_sizes[i]);
        }
        expected_cell_sizes.push_back(cell_size);
    }

    auto field_metas = test_data_->GetFieldMetas(0);
    size_t named_lookup_count = 0;
    auto chunk_reader = std::make_unique<ColumnEstimateTestChunkReader>(
        std::move(delegate),
        /*total_estimate_available=*/true,
        ColumnEstimateMode::UNAVAILABLE,
        &named_lookup_count);
    auto translator = std::make_unique<ManifestGroupTranslator>(
        segment_id_,
        GroupChunkType::DEFAULT,
        /*column_group_index=*/0,
        std::move(chunk_reader),
        field_metas,
        *needed_columns,
        /*full_projection=*/true,
        GetParam(),
        /*mmap_populate=*/true,
        mmap_dir_,
        field_metas.size(),
        milvus::proto::common::LoadPriority::LOW,
        /*eager_load=*/true,
        /*warmup_policy=*/"");

    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    EXPECT_EQ(named_lookup_count, 0);
    EXPECT_EQ(meta->chunk_memory_size_, expected_cell_sizes);
}

TEST_P(ManifestGroupTranslatorTest,
       TestProjectedColumnUsesProjectedEstimatedSize) {
    const auto& column_group = test_data_->GetColumnGroups()->at(0);
    ASSERT_GT(column_group->columns.size(), 1);

    const auto& projected_column = column_group->columns.front();
    auto needed_columns = std::make_shared<std::vector<std::string>>(
        std::initializer_list<std::string>{projected_column});
    auto chunk_reader = test_data_->CreateChunkReader(0, needed_columns);

    auto projected_sizes_result =
        chunk_reader->get_chunk_column_estimated_size(projected_column);
    ASSERT_TRUE(projected_sizes_result.ok())
        << projected_sizes_result.status().ToString();
    const auto& projected_sizes = projected_sizes_result.ValueOrDie();

    auto total_sizes_result = chunk_reader->get_chunk_estimated_size();
    ASSERT_TRUE(total_sizes_result.ok())
        << total_sizes_result.status().ToString();
    const auto& total_sizes = total_sizes_result.ValueOrDie();
    ASSERT_EQ(projected_sizes.size(), total_sizes.size());

    uint64_t projected_total = 0;
    uint64_t total = 0;
    for (size_t i = 0; i < projected_sizes.size(); ++i) {
        projected_total += projected_sizes[i];
        total += total_sizes[i];
    }
    ASSERT_LT(projected_total, total);

    auto all_field_metas = test_data_->GetFieldMetas(0);
    auto projected_field_id = FieldId(std::stoll(projected_column));
    std::unordered_map<FieldId, FieldMeta> projected_field_metas;
    projected_field_metas.emplace(projected_field_id,
                                  all_field_metas.at(projected_field_id));

    auto translator = std::make_unique<ManifestGroupTranslator>(
        segment_id_,
        GroupChunkType::DEFAULT,
        /*column_group_index=*/0,
        std::move(chunk_reader),
        projected_field_metas,
        *needed_columns,
        /*full_projection=*/false,
        GetParam(),
        /*mmap_populate=*/true,
        mmap_dir_,
        projected_field_metas.size(),
        milvus::proto::common::LoadPriority::LOW,
        /*eager_load=*/true,
        /*warmup_policy=*/"",
        projected_column);

    auto rgs_per_cell =
        ComputeRowGroupsPerCell(projected_sizes, GetCellTargetSizeBytes());
    std::vector<int64_t> expected_cell_sizes;
    for (size_t start = 0; start < projected_sizes.size();
         start += rgs_per_cell) {
        const auto end = std::min(start + rgs_per_cell, projected_sizes.size());
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cell_size += static_cast<int64_t>(projected_sizes[i]);
        }
        expected_cell_sizes.push_back(cell_size);
    }

    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    EXPECT_EQ(meta->chunk_memory_size_, expected_cell_sizes);
}

TEST_P(ManifestGroupTranslatorTest,
       TestProjectedColumnEstimateTakesPrecedenceOverFallback) {
    const auto& column_group = test_data_->GetColumnGroups()->at(0);
    ASSERT_GT(column_group->columns.size(), 1);

    const auto& projected_column = column_group->columns.front();
    auto needed_columns = std::make_shared<std::vector<std::string>>(
        std::initializer_list<std::string>{projected_column});
    auto chunk_reader = test_data_->CreateChunkReader(0, needed_columns);

    auto projected_sizes_result =
        chunk_reader->get_chunk_column_estimated_size(projected_column);
    ASSERT_TRUE(projected_sizes_result.ok())
        << projected_sizes_result.status().ToString();
    const auto& projected_sizes = projected_sizes_result.ValueOrDie();

    constexpr int64_t kFallbackBytesPerRow = 8 * 1024;
    auto rgs_per_cell =
        ComputeRowGroupsPerCell(projected_sizes, GetCellTargetSizeBytes());

    std::vector<int64_t> expected_cell_sizes;
    for (size_t start = 0; start < projected_sizes.size();
         start += rgs_per_cell) {
        const auto end = std::min(start + rgs_per_cell, projected_sizes.size());
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cell_size += static_cast<int64_t>(projected_sizes[i]);
        }
        expected_cell_sizes.push_back(cell_size);
    }

    auto all_field_metas = test_data_->GetFieldMetas(0);
    auto projected_field_id = FieldId(std::stoll(projected_column));
    std::unordered_map<FieldId, FieldMeta> projected_field_metas;
    projected_field_metas.emplace(projected_field_id,
                                  all_field_metas.at(projected_field_id));

    auto translator = std::make_unique<ManifestGroupTranslator>(
        segment_id_,
        GroupChunkType::DEFAULT,
        /*column_group_index=*/0,
        std::move(chunk_reader),
        projected_field_metas,
        *needed_columns,
        /*full_projection=*/false,
        GetParam(),
        /*mmap_populate=*/true,
        mmap_dir_,
        projected_field_metas.size(),
        milvus::proto::common::LoadPriority::LOW,
        /*eager_load=*/false,
        /*warmup_policy=*/"",
        projected_column + "_fallback",
        kFallbackBytesPerRow);

    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    EXPECT_EQ(meta->chunk_memory_size_, expected_cell_sizes);
}

TEST_P(ManifestGroupTranslatorTest,
       TestLiveRowsWithZeroProjectedEstimateUsePositiveFallback) {
    const auto& column_group = test_data_->GetColumnGroups()->at(0);
    ASSERT_GT(column_group->columns.size(), 1);

    const auto& projected_column = column_group->columns.front();
    auto needed_columns = std::make_shared<std::vector<std::string>>(
        std::initializer_list<std::string>{projected_column});
    auto delegate = test_data_->CreateChunkReader(0, needed_columns);

    auto row_group_rows_result = delegate->get_chunk_rows();
    ASSERT_TRUE(row_group_rows_result.ok())
        << row_group_rows_result.status().ToString();
    const auto& row_group_rows = row_group_rows_result.ValueOrDie();
    ASSERT_FALSE(row_group_rows.empty());
    ASSERT_GT(row_group_rows.front(), 0);

    auto all_field_metas = test_data_->GetFieldMetas(0);
    auto projected_field_id = FieldId(std::stoll(projected_column));
    std::unordered_map<FieldId, FieldMeta> projected_field_metas;
    projected_field_metas.emplace(projected_field_id,
                                  all_field_metas.at(projected_field_id));

    auto chunk_reader = std::make_unique<ColumnEstimateTestChunkReader>(
        std::move(delegate),
        /*total_estimate_available=*/true,
        ColumnEstimateMode::ZERO);
    auto translator = std::make_unique<ManifestGroupTranslator>(
        segment_id_,
        GroupChunkType::DEFAULT,
        /*column_group_index=*/0,
        std::move(chunk_reader),
        projected_field_metas,
        *needed_columns,
        /*full_projection=*/false,
        GetParam(),
        /*mmap_populate=*/true,
        mmap_dir_,
        projected_field_metas.size(),
        milvus::proto::common::LoadPriority::LOW,
        /*eager_load=*/false,
        /*warmup_policy=*/"",
        projected_column + "_zero_estimate");

    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    ASSERT_FALSE(meta->chunk_memory_size_.empty());
    auto [start, end] = meta->get_row_group_range(0);
    int64_t first_cell_rows = 0;
    for (size_t i = start; i < end; ++i) {
        first_cell_rows += static_cast<int64_t>(row_group_rows[i]);
    }
    constexpr int64_t kLastResortBytesPerRow = 4096;
    EXPECT_EQ(meta->chunk_memory_size_.front(),
              first_cell_rows * kLastResortBytesPerRow);

    auto cells = translator->get_cells(nullptr, {0});
    ASSERT_EQ(cells.size(), 1);
    EXPECT_EQ(cells.front().first, 0);
}

TEST_P(ManifestGroupTranslatorTest,
       TestMixedZeroProjectedEstimateUsesPerRowGroupFallback) {
    const auto& column_group = test_data_->GetColumnGroups()->at(0);
    ASSERT_GT(column_group->columns.size(), 1);

    const auto& projected_column = column_group->columns.front();
    auto needed_columns = std::make_shared<std::vector<std::string>>(
        std::initializer_list<std::string>{projected_column});
    auto delegate = test_data_->CreateChunkReader(0, needed_columns);

    auto row_group_rows_result = delegate->get_chunk_rows();
    ASSERT_TRUE(row_group_rows_result.ok())
        << row_group_rows_result.status().ToString();
    const auto& row_group_rows = row_group_rows_result.ValueOrDie();

    auto projected_sizes_result =
        delegate->get_chunk_column_estimated_size(projected_column);
    ASSERT_TRUE(projected_sizes_result.ok())
        << projected_sizes_result.status().ToString();
    auto expected_row_group_sizes = projected_sizes_result.ValueOrDie();
    ASSERT_EQ(expected_row_group_sizes.size(), row_group_rows.size());
    ASSERT_GT(expected_row_group_sizes.size(), 1);
    ASSERT_GT(expected_row_group_sizes.front(), 0);
    ASSERT_GT(row_group_rows.front(), 0);

    constexpr uint64_t kLastResortBytesPerRow = 4096;
    expected_row_group_sizes.front() =
        row_group_rows.front() * kLastResortBytesPerRow;
    auto rgs_per_cell = ComputeRowGroupsPerCell(expected_row_group_sizes,
                                                GetCellTargetSizeBytes());
    std::vector<int64_t> expected_cell_sizes;
    for (size_t start = 0; start < expected_row_group_sizes.size();
         start += rgs_per_cell) {
        const auto end =
            std::min(start + rgs_per_cell, expected_row_group_sizes.size());
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cell_size += static_cast<int64_t>(expected_row_group_sizes[i]);
        }
        expected_cell_sizes.push_back(cell_size);
    }

    auto all_field_metas = test_data_->GetFieldMetas(0);
    auto projected_field_id = FieldId(std::stoll(projected_column));
    std::unordered_map<FieldId, FieldMeta> projected_field_metas;
    projected_field_metas.emplace(projected_field_id,
                                  all_field_metas.at(projected_field_id));

    auto chunk_reader = std::make_unique<ColumnEstimateTestChunkReader>(
        std::move(delegate),
        /*total_estimate_available=*/true,
        ColumnEstimateMode::FIRST_ZERO);
    auto translator = std::make_unique<ManifestGroupTranslator>(
        segment_id_,
        GroupChunkType::DEFAULT,
        /*column_group_index=*/0,
        std::move(chunk_reader),
        projected_field_metas,
        *needed_columns,
        /*full_projection=*/false,
        GetParam(),
        /*mmap_populate=*/true,
        mmap_dir_,
        projected_field_metas.size(),
        milvus::proto::common::LoadPriority::LOW,
        /*eager_load=*/false,
        /*warmup_policy=*/"",
        projected_column + "_mixed_zero_estimate");

    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    EXPECT_EQ(meta->chunk_memory_size_, expected_cell_sizes);
}

TEST_P(ManifestGroupTranslatorTest,
       TestProjectedColumnEstimateErrorUsesSampledFallback) {
    const auto& column_group = test_data_->GetColumnGroups()->at(0);
    ASSERT_GT(column_group->columns.size(), 1);

    const auto& projected_column = column_group->columns.front();
    auto needed_columns = std::make_shared<std::vector<std::string>>(
        std::initializer_list<std::string>{projected_column});
    auto delegate = test_data_->CreateChunkReader(0, needed_columns);

    auto row_group_rows_result = delegate->get_chunk_rows();
    ASSERT_TRUE(row_group_rows_result.ok())
        << row_group_rows_result.status().ToString();
    const auto& row_group_rows = row_group_rows_result.ValueOrDie();
    ASSERT_GT(row_group_rows.size(), 1);

    constexpr int64_t kFallbackBytesPerRow = 8 * 1024;
    std::vector<int64_t> fallback_row_group_sizes;
    fallback_row_group_sizes.reserve(row_group_rows.size());
    for (auto rows : row_group_rows) {
        fallback_row_group_sizes.push_back(static_cast<int64_t>(rows) *
                                           kFallbackBytesPerRow);
    }
    auto rgs_per_cell = ComputeRowGroupsPerCell(fallback_row_group_sizes,
                                                GetCellTargetSizeBytes());

    std::vector<int64_t> expected_cell_sizes;
    for (size_t start = 0; start < fallback_row_group_sizes.size();
         start += rgs_per_cell) {
        const auto end =
            std::min(start + rgs_per_cell, fallback_row_group_sizes.size());
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cell_size += fallback_row_group_sizes[i];
        }
        expected_cell_sizes.push_back(cell_size);
    }

    auto all_field_metas = test_data_->GetFieldMetas(0);
    auto projected_field_id = FieldId(std::stoll(projected_column));
    std::unordered_map<FieldId, FieldMeta> projected_field_metas;
    projected_field_metas.emplace(projected_field_id,
                                  all_field_metas.at(projected_field_id));

    auto chunk_reader =
        std::make_unique<ColumnEstimateTestChunkReader>(std::move(delegate));
    auto translator = std::make_unique<ManifestGroupTranslator>(
        segment_id_,
        GroupChunkType::DEFAULT,
        /*column_group_index=*/0,
        std::move(chunk_reader),
        projected_field_metas,
        *needed_columns,
        /*full_projection=*/false,
        GetParam(),
        /*mmap_populate=*/true,
        mmap_dir_,
        projected_field_metas.size(),
        milvus::proto::common::LoadPriority::LOW,
        /*eager_load=*/false,
        /*warmup_policy=*/"",
        projected_column + "_fallback_unavailable",
        kFallbackBytesPerRow);

    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    EXPECT_EQ(meta->chunk_memory_size_, expected_cell_sizes);
}

TEST_P(ManifestGroupTranslatorTest,
       TestProjectedColumnEstimateErrorUsesTotalEstimateWithoutSample) {
    const auto& column_group = test_data_->GetColumnGroups()->at(0);
    ASSERT_GT(column_group->columns.size(), 1);

    const auto& projected_column = column_group->columns.front();
    auto needed_columns = std::make_shared<std::vector<std::string>>(
        std::initializer_list<std::string>{projected_column});
    auto delegate = test_data_->CreateChunkReader(0, needed_columns);

    auto total_sizes_result = delegate->get_chunk_estimated_size();
    ASSERT_TRUE(total_sizes_result.ok())
        << total_sizes_result.status().ToString();
    const auto& total_sizes = total_sizes_result.ValueOrDie();
    auto rgs_per_cell =
        ComputeRowGroupsPerCell(total_sizes, GetCellTargetSizeBytes());

    std::vector<int64_t> expected_cell_sizes;
    for (size_t start = 0; start < total_sizes.size(); start += rgs_per_cell) {
        const auto end = std::min(start + rgs_per_cell, total_sizes.size());
        int64_t cell_size = 0;
        for (size_t i = start; i < end; ++i) {
            cell_size += static_cast<int64_t>(total_sizes[i]);
        }
        expected_cell_sizes.push_back(cell_size);
    }

    auto all_field_metas = test_data_->GetFieldMetas(0);
    auto projected_field_id = FieldId(std::stoll(projected_column));
    std::unordered_map<FieldId, FieldMeta> projected_field_metas;
    projected_field_metas.emplace(projected_field_id,
                                  all_field_metas.at(projected_field_id));

    auto chunk_reader =
        std::make_unique<ColumnEstimateTestChunkReader>(std::move(delegate));
    auto translator = std::make_unique<ManifestGroupTranslator>(
        segment_id_,
        GroupChunkType::DEFAULT,
        /*column_group_index=*/0,
        std::move(chunk_reader),
        projected_field_metas,
        *needed_columns,
        /*full_projection=*/false,
        GetParam(),
        /*mmap_populate=*/true,
        mmap_dir_,
        projected_field_metas.size(),
        milvus::proto::common::LoadPriority::LOW,
        /*eager_load=*/false,
        /*warmup_policy=*/"",
        projected_column + "_total_fallback");

    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    EXPECT_EQ(meta->chunk_memory_size_, expected_cell_sizes);
}

TEST_P(ManifestGroupTranslatorTest,
       TestUnavailableEstimatesUseLastResortPerRow) {
    const auto& column_group = test_data_->GetColumnGroups()->at(0);
    ASSERT_GT(column_group->columns.size(), 1);

    const auto& projected_column = column_group->columns.front();
    auto needed_columns = std::make_shared<std::vector<std::string>>(
        std::initializer_list<std::string>{projected_column});
    auto delegate = test_data_->CreateChunkReader(0, needed_columns);

    auto row_group_rows_result = delegate->get_chunk_rows();
    ASSERT_TRUE(row_group_rows_result.ok())
        << row_group_rows_result.status().ToString();
    const auto& row_group_rows = row_group_rows_result.ValueOrDie();

    constexpr int64_t kLastResortBytesPerRow = 4096;
    std::vector<int64_t> expected_cell_sizes;
    expected_cell_sizes.reserve(row_group_rows.size());
    for (auto rows : row_group_rows) {
        expected_cell_sizes.push_back(static_cast<int64_t>(rows) *
                                      kLastResortBytesPerRow);
    }

    auto all_field_metas = test_data_->GetFieldMetas(0);
    auto projected_field_id = FieldId(std::stoll(projected_column));
    std::unordered_map<FieldId, FieldMeta> projected_field_metas;
    projected_field_metas.emplace(projected_field_id,
                                  all_field_metas.at(projected_field_id));

    auto chunk_reader = std::make_unique<ColumnEstimateTestChunkReader>(
        std::move(delegate),
        /*total_estimate_available=*/false);
    auto translator = std::make_unique<ManifestGroupTranslator>(
        segment_id_,
        GroupChunkType::DEFAULT,
        /*column_group_index=*/0,
        std::move(chunk_reader),
        projected_field_metas,
        *needed_columns,
        /*full_projection=*/false,
        GetParam(),
        /*mmap_populate=*/true,
        mmap_dir_,
        projected_field_metas.size(),
        milvus::proto::common::LoadPriority::LOW,
        /*eager_load=*/false,
        /*warmup_policy=*/"",
        projected_column + "_last_resort");

    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    EXPECT_EQ(meta->chunk_memory_size_, expected_cell_sizes);
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

    auto chunk_reader = test_data_->CreateChunkReader(0);
    auto row_group_sizes =
        chunk_reader->get_chunk_estimated_size().ValueOrDie();
    auto rgs_per_cell =
        ComputeRowGroupsPerCell(row_group_sizes, GetCellTargetSizeBytes());

    // Ranges should be contiguous and cover [0, total_row_groups_)
    size_t expected_start = 0;
    for (size_t cid = 0; cid < num_cells; ++cid) {
        auto [start, end] = meta->get_row_group_range(cid);
        EXPECT_EQ(start, expected_start) << "gap at cid " << cid;
        EXPECT_GT(end, start) << "empty range at cid " << cid;
        EXPECT_LE(end - start, rgs_per_cell)
            << "range too large at cid " << cid;
        expected_start = end;
    }
    EXPECT_EQ(expected_start, meta->total_row_groups_);
}

INSTANTIATE_TEST_SUITE_P(ManifestGroupTranslator,
                         ManifestGroupTranslatorTest,
                         testing::Bool());
