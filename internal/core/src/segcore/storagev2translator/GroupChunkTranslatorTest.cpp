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

#include <folly/Conv.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>
#include <cstdint>
#include "arrow/type_fwd.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "index/Utils.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "test_utils/DataGen.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"
#include "mmap/ChunkedColumnGroup.h"

#include <memory>
#include <string>
#include <vector>
#include <filesystem>

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::segcore::storagev2translator;

class GroupChunkTranslatorTest : public ::testing::TestWithParam<bool> {
    void
    SetUp() override {
        auto conf = milvus_storage::ArrowFileSystemConfig();
        conf.storage_type = "local";
        conf.root_path = path_;
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);
        fs_ = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();
        schema_ = CreateTestSchema();
        arrow_schema_ = schema_->ConvertToArrowSchema();
        int64_t per_batch = 1000;
        int64_t n_batch = 3;
        int64_t dim = 128;
        // Write data to storage v2
        paths_ = std::vector<std::string>{path_ + "/19530.parquet"};
        auto column_groups = std::vector<std::vector<int>>{
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}};
        auto writer_memory = 16 * 1024 * 1024;
        auto storage_config = milvus_storage::StorageConfig();
        milvus_storage::PackedRecordBatchWriter writer(fs_,
                                                       paths_,
                                                       arrow_schema_,
                                                       storage_config,
                                                       column_groups,
                                                       writer_memory);
        int64_t total_rows = 0;
        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema_, per_batch);
            auto record_batch =
                ConvertToArrowRecordBatch(dataset, dim, arrow_schema_);
            total_rows += record_batch->num_rows();

            EXPECT_TRUE(writer.Write(record_batch).ok());
        }
        EXPECT_TRUE(writer.Close().ok());
    }

 protected:
    ~GroupChunkTranslatorTest() {
        if (GetParam()) {  // if use_mmap is true
            std::string mmap_dir = std::to_string(segment_id_);
            if (std::filesystem::exists(mmap_dir)) {
                std::filesystem::remove_all(mmap_dir);
            }
        }
    }

    SchemaPtr schema_;
    milvus_storage::ArrowFileSystemPtr fs_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    std::string path_ = "/tmp";

    std::vector<std::string> paths_;
    int64_t segment_id_ = 0;
};

TEST_P(GroupChunkTranslatorTest, TestWithMmap) {
    auto use_mmap = GetParam();
    std::unordered_map<FieldId, FieldMeta> field_metas = schema_->get_fields();
    auto column_group_info = FieldDataInfo(0, 3000, "");

    auto translator = std::make_unique<GroupChunkTranslator>(
        segment_id_,
        field_metas,
        column_group_info,
        paths_,
        use_mmap,
        schema_->get_field_ids().size(),
        milvus::proto::common::LoadPriority::LOW);

    // num cells - get the expected number from the file directly
    auto fr =
        std::make_shared<milvus_storage::FileRowGroupReader>(fs_, paths_[0]);
    auto expected_num_cells =
        fr->file_metadata()->GetRowGroupMetadataVector().size();
    auto row_group_metadata_vector =
        fr->file_metadata()->GetRowGroupMetadataVector();
    auto status = fr->Close();
    AssertInfo(status.ok(), "failed to close file reader");
    EXPECT_EQ(translator->num_cells(), expected_num_cells);

    // cell id of
    for (size_t i = 0; i < translator->num_cells(); ++i) {
        EXPECT_EQ(translator->cell_id_of(i), i);
    }

    // key
    EXPECT_EQ(translator->key(), "seg_0_cg_0");

    // estimated byte size
    for (size_t i = 0; i < translator->num_cells(); ++i) {
        auto [file_idx, row_group_idx] =
            translator->get_file_and_row_group_index(i);
        // Get the expected size from the file directly
        auto expected_size = static_cast<int64_t>(
            row_group_metadata_vector.Get(row_group_idx).memory_size());
        auto usage = translator->estimated_byte_size_of_cell(i).first;
        if (use_mmap) {
            EXPECT_EQ(usage.file_bytes, expected_size);
        } else {
            EXPECT_EQ(usage.memory_bytes, expected_size);
        }
    }

    // getting cells
    std::vector<cachinglayer::cid_t> cids = {0, 1};
    auto cells = translator->get_cells(cids);
    EXPECT_EQ(cells.size(), cids.size());

    // Test DataByteSize from meta
    auto meta = static_cast<GroupCTMeta*>(translator->meta());
    size_t expected_total_size = 0;
    for (const auto& chunk_size : meta->chunk_memory_size_) {
        expected_total_size += chunk_size;
    }
    EXPECT_GT(expected_total_size, 0);
    auto num_cells = translator->num_cells();

    auto chunked_column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));

    EXPECT_EQ(meta->chunk_memory_size_.size(), num_cells);
    EXPECT_EQ(expected_total_size, chunked_column_group->memory_size());

    // Verify mmap directory and files if in mmap mode
    if (use_mmap) {
        std::string mmap_dir = std::to_string(segment_id_);
        EXPECT_TRUE(std::filesystem::exists(mmap_dir));

        // DO NOT Verify each field has a corresponding file: files are unlinked immediately after being mmaped.
        // for (size_t i = 0; i < field_id_list.size(); ++i) {
        //     auto field_id = field_id_list.Get(i);
        //     std::string field_file = mmap_dir + "/" + std::to_string(field_id);
        //     EXPECT_TRUE(std::filesystem::exists(field_file));
        // }
    }
}

TEST_P(GroupChunkTranslatorTest, TestMultipleFiles) {
    auto use_mmap = GetParam();
    std::unordered_map<FieldId, FieldMeta> field_metas = schema_->get_fields();

    // Create multiple files for testing
    std::vector<std::string> multi_file_paths;
    std::vector<int64_t> expected_row_groups_per_file;
    int64_t total_rows = 0;

    // Create 3 files with different numbers of row groups
    for (int file_idx = 0; file_idx < 3; ++file_idx) {
        std::string file_path =
            path_ + "/multi_file_" + std::to_string(file_idx) + ".parquet";
        multi_file_paths.push_back(file_path);

        int64_t per_batch = 1000;
        int64_t n_batch =
            2 + file_idx;  // Different number of batches per file: 2, 3, 4
        int64_t dim = 128;

        auto column_groups = std::vector<std::vector<int>>{
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}};
        auto writer_memory = 16 * 1024 * 1024;
        auto storage_config = milvus_storage::StorageConfig();
        std::vector<std::string> single_file_paths{file_path};
        milvus_storage::PackedRecordBatchWriter writer(fs_,
                                                       single_file_paths,
                                                       arrow_schema_,
                                                       storage_config,
                                                       column_groups,
                                                       writer_memory);

        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema_, per_batch);
            auto record_batch =
                ConvertToArrowRecordBatch(dataset, dim, arrow_schema_);
            total_rows += record_batch->num_rows();
            EXPECT_TRUE(writer.Write(record_batch).ok());
        }
        EXPECT_TRUE(writer.Close().ok());

        // Get the number of row groups in this file
        auto fr = std::make_shared<milvus_storage::FileRowGroupReader>(
            fs_, file_path);
        expected_row_groups_per_file.push_back(
            fr->file_metadata()->GetRowGroupMetadataVector().size());
        auto status = fr->Close();
        AssertInfo(status.ok(), "failed to close file reader");
    }

    auto column_group_info = FieldDataInfo(0, total_rows, "");

    auto translator = std::make_unique<GroupChunkTranslator>(
        segment_id_,
        field_metas,
        column_group_info,
        multi_file_paths,
        use_mmap,
        schema_->get_field_ids().size(),
        milvus::proto::common::LoadPriority::LOW);

    // Test total number of cells across all files
    int64_t expected_total_cells = 0;
    for (auto row_groups : expected_row_groups_per_file) {
        expected_total_cells += row_groups;
    }
    EXPECT_EQ(translator->num_cells(), expected_total_cells);

    // Test get_file_and_row_group_index for cids across different files
    int64_t cid_offset = 0;
    for (size_t file_idx = 0; file_idx < expected_row_groups_per_file.size();
         ++file_idx) {
        for (int64_t row_group_idx = 0;
             row_group_idx < expected_row_groups_per_file[file_idx];
             ++row_group_idx) {
            auto cid = cid_offset + row_group_idx;
            auto [actual_file_idx, actual_row_group_idx] =
                translator->get_file_and_row_group_index(cid);
            EXPECT_EQ(actual_file_idx, file_idx);
            EXPECT_EQ(actual_row_group_idx, row_group_idx);
        }
        cid_offset += expected_row_groups_per_file[file_idx];
    }

    // Test get_cells with cids from the same file
    std::vector<cachinglayer::cid_t> same_file_cids = {0,
                                                       1};  // Both from file 0
    auto same_file_cells = translator->get_cells(same_file_cids);
    EXPECT_EQ(same_file_cells.size(), same_file_cids.size());
    int i = 0;
    for (const auto& [cid, chunk] : same_file_cells) {
        EXPECT_EQ(cid, same_file_cids[i++]);
    }

    // Test get_cells with cids in reverse order to test sorting
    std::vector<cachinglayer::cid_t> cross_file_cids = {4, 7, 0};
    auto cells = translator->get_cells(cross_file_cids);
    std::vector<cachinglayer::cid_t> returned_cids = {0, 4, 7};
    i = 0;
    for (const auto& [cid, chunk] : cells) {
        EXPECT_EQ(cid, returned_cids[i++]);
    }

    // Test estimated byte size for cids across different files
    for (size_t i = 0; i < translator->num_cells(); ++i) {
        auto [file_idx, row_group_idx] =
            translator->get_file_and_row_group_index(i);
        auto usage = translator->estimated_byte_size_of_cell(i).first;

        // Get the expected memory size from the corresponding file
        auto fr = std::make_shared<milvus_storage::FileRowGroupReader>(
            fs_, multi_file_paths[file_idx]);
        auto row_group_metadata_vector =
            fr->file_metadata()->GetRowGroupMetadataVector();
        auto expected_size = static_cast<int64_t>(
            row_group_metadata_vector.Get(row_group_idx).memory_size());
        auto status = fr->Close();
        AssertInfo(status.ok(), "failed to close file reader");

        if (use_mmap) {
            EXPECT_EQ(usage.file_bytes, expected_size);
        } else {
            EXPECT_EQ(usage.memory_bytes, expected_size);
        }
    }

    // Clean up test files
    for (const auto& file_path : multi_file_paths) {
        if (std::filesystem::exists(file_path)) {
            std::filesystem::remove(file_path);
        }
    }
}

TEST_P(GroupChunkTranslatorTest, TestGetSkipIndex) {
    auto use_mmap = GetParam();
    std::unordered_map<FieldId, FieldMeta> field_metas = schema_->get_fields();

    // Create multiple files WITH SkipIndex
    std::vector<std::string> multi_file_paths;
    std::vector<int64_t> expected_row_groups_per_file;
    int64_t total_rows = 0;

    for (int file_idx = 0; file_idx < 3; ++file_idx) {
        std::string file_path =
            path_ + "/skipindex_" + std::to_string(file_idx) + ".parquet";
        multi_file_paths.push_back(file_path);

        int64_t per_batch = 1000;
        int64_t n_batch = 2 + file_idx;  // 2, 3, 4 batches per file
        int64_t dim = 128;

        auto column_groups = std::vector<std::vector<int>>{
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}};
        auto writer_memory = 16 * 1024 * 1024;
        auto storage_config = milvus_storage::StorageConfig();
        std::vector<std::string> single_file_paths{file_path};

        milvus_storage::PackedRecordBatchWriter writer(fs_,
                                                       single_file_paths,
                                                       arrow_schema_,
                                                       storage_config,
                                                       column_groups,
                                                       writer_memory);

        auto status = writer.AddMetadataBuilder(
            milvus::ChunkSkipIndex::KEY,
            []() { return std::make_unique<milvus::ChunkSkipIndexBuilder>(); });
        AssertInfo(status.ok(), "failed to add metadata builder");

        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema_, per_batch);
            auto record_batch =
                ConvertToArrowRecordBatch(dataset, dim, arrow_schema_);
            total_rows += record_batch->num_rows();
            EXPECT_TRUE(writer.Write(record_batch).ok());
        }
        EXPECT_TRUE(writer.Close().ok());

        // Get expected row group count
        auto fr = std::make_shared<milvus_storage::FileRowGroupReader>(
            fs_, file_path);
        expected_row_groups_per_file.push_back(
            fr->file_metadata()->GetRowGroupMetadataVector().size());
        auto s = fr->Close();
        AssertInfo(s.ok(), "failed to close file reader");
    }

    auto column_group_info = FieldDataInfo(0, total_rows, "");

    auto translator = std::make_unique<GroupChunkTranslator>(
        segment_id_,
        field_metas,
        column_group_info,
        multi_file_paths,
        use_mmap,
        schema_->get_field_ids().size(),
        milvus::proto::common::LoadPriority::LOW);

    auto skip_indexes = translator->GetSkipIndex(arrow_schema_);

    int64_t expected_total_skip_indexes = 0;
    for (auto count : expected_row_groups_per_file) {
        expected_total_skip_indexes += count;
    }
    EXPECT_EQ(skip_indexes.size(), expected_total_skip_indexes);
    for (size_t i = 0; i < skip_indexes.size(); ++i) {
        EXPECT_NE(skip_indexes[i], nullptr)
            << "SkipIndex at position " << i << " should not be null";
    }

    // Verify each SkipIndex is valid and contains field metrics
    for (size_t i = 0; i < skip_indexes.size(); ++i) {
        EXPECT_NE(skip_indexes[i], nullptr)
            << "SkipIndex at chunk " << i << " should not be null";

        EXPECT_FALSE(skip_indexes[i]->IsEmpty())
            << "SkipIndex at chunk " << i << " should not be empty";

        // Verify SkipIndex contains metrics for scalar fields
        const auto& metrics = skip_indexes[i]->GetMetrics();
        EXPECT_GT(metrics.size(), 0)
            << "SkipIndex at chunk " << i << " should have field metrics";

        for (const auto& [field_id, field_meta] : field_metas) {
            auto data_type = field_meta.get_data_type();

            if (index::SupportsSkipIndex(data_type)) {
                bool found = false;
                for (const auto& [metric_field_id, _] : metrics) {
                    if (metric_field_id == field_id) {
                        found = true;
                        break;
                    }
                }
                EXPECT_TRUE(found) << "Field " << field_id.get()
                                   << " should have SkipIndex metrics";
            }
        }
    }

    // Test LoadSkipIndex integration
    auto skip_index = std::make_shared<milvus::SkipIndex>();
    size_t num_chunks = skip_indexes.size();
    skip_index->LoadSkipIndex(std::move(skip_indexes));

    std::vector<FieldId> fields_with_skip_index;
    for (const auto& [field_id, field_meta] : field_metas) {
        if (index::SupportsSkipIndex(field_meta.get_data_type())) {
            fields_with_skip_index.push_back(field_id);
        }
    }
    EXPECT_GT(fields_with_skip_index.size(), 0)
        << "Schema should have at least one field supporting SkipIndex";

    for (const auto& field_id : fields_with_skip_index) {
        auto field_meta = schema_->operator[](field_id);
        auto data_type = field_meta.get_data_type();

        for (size_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
            // Get FieldChunkMetrics for this field and chunk
            auto field_chunk_metrics =
                skip_index->GetFieldChunkMetrics(field_id, chunk_id);

            EXPECT_NE(field_chunk_metrics, nullptr)
                << "Field " << field_id.get()
                << " should have FieldChunkMetrics in chunk " << chunk_id;

            if (field_chunk_metrics != nullptr) {
                // Verify the metrics has correct data type
                auto arrow_type = field_chunk_metrics->GetDataType();
                EXPECT_NE(arrow_type, arrow::Type::NA)
                    << "FieldChunkMetrics for field " << field_id.get()
                    << " in chunk " << chunk_id
                    << " should have valid data type";

                // Verify the metrics has at least one metric (MINMAX, SET, BLOOM_FILTER, or NGRAM_FILTER)
                EXPECT_GT(field_chunk_metrics->Size(), 0)
                    << "FieldChunkMetrics for field " << field_id.get()
                    << " in chunk " << chunk_id
                    << " should have at least one metric";

                // Verify specific metrics based on data type
                bool should_have_minmax = data_type == DataType::INT8 ||
                                          data_type == DataType::INT16 ||
                                          data_type == DataType::INT32 ||
                                          data_type == DataType::INT64 ||
                                          data_type == DataType::FLOAT ||
                                          data_type == DataType::DOUBLE ||
                                          data_type == DataType::VARCHAR ||
                                          data_type == DataType::TIMESTAMPTZ;

                if (should_have_minmax) {
                    EXPECT_TRUE(field_chunk_metrics->HasMetric(
                        FieldChunkMetricType::MINMAX))
                        << "Field " << field_id.get()
                        << " (type: " << static_cast<int>(data_type)
                        << ") should have MINMAX metric in chunk " << chunk_id;
                }

                // INT types should have either SET or BLOOM_FILTER
                bool is_int_type = data_type == DataType::INT8 ||
                                   data_type == DataType::INT16 ||
                                   data_type == DataType::INT32 ||
                                   data_type == DataType::INT64 ||
                                   data_type == DataType::TIMESTAMPTZ;

                if (is_int_type) {
                    bool has_set_or_bloom =
                        field_chunk_metrics->HasMetric(
                            FieldChunkMetricType::SET) ||
                        field_chunk_metrics->HasMetric(
                            FieldChunkMetricType::BLOOM_FILTER);

                    EXPECT_TRUE(has_set_or_bloom)
                        << "Field " << field_id.get()
                        << " (type: " << static_cast<int>(data_type)
                        << ") should have either SET or BLOOM_FILTER metric in "
                           "chunk "
                        << chunk_id;
                }

                // VARCHAR should have SET or BLOOM_FILTER, and potentially NGRAM_FILTER
                if (data_type == DataType::VARCHAR) {
                    bool has_set_or_bloom =
                        field_chunk_metrics->HasMetric(
                            FieldChunkMetricType::SET) ||
                        field_chunk_metrics->HasMetric(
                            FieldChunkMetricType::BLOOM_FILTER);

                    EXPECT_TRUE(has_set_or_bloom)
                        << "VARCHAR field " << field_id.get()
                        << " should have either SET or BLOOM_FILTER metric in "
                           "chunk "
                        << chunk_id;
                }

                // BOOL should have SET metric
                if (data_type == DataType::BOOL) {
                    EXPECT_TRUE(field_chunk_metrics->HasMetric(
                        FieldChunkMetricType::SET))
                        << "BOOL field " << field_id.get()
                        << " should have SET metric in chunk " << chunk_id;
                }
            }
        }
    }

    // Clean up
    for (const auto& file_path : multi_file_paths) {
        if (std::filesystem::exists(file_path)) {
            std::filesystem::remove(file_path);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(GroupChunkTranslatorTest,
                         GroupChunkTranslatorTest,
                         testing::Bool());