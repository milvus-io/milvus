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
#include "common/Schema.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "test_utils/DataGen.h"
#include "segcore/storagev2translator/GroupChunkTranslator.h"

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
    // Get row group metadata
    std::vector<milvus_storage::RowGroupMetadataVector> row_group_meta_list;
    auto fr =
        std::make_shared<milvus_storage::FileRowGroupReader>(fs_, paths_[0]);
    auto field_id_list =
        fr->file_metadata()->GetGroupFieldIDList().GetFieldIDList(0);

    row_group_meta_list.push_back(
        fr->file_metadata()->GetRowGroupMetadataVector());

    GroupChunkTranslator translator(segment_id_,
                                    field_metas,
                                    column_group_info,
                                    paths_,
                                    use_mmap,
                                    row_group_meta_list,
                                    field_id_list);

    // num cells
    EXPECT_EQ(translator.num_cells(), row_group_meta_list[0].size());

    // cell id of
    for (size_t i = 0; i < translator.num_cells(); ++i) {
        EXPECT_EQ(translator.cell_id_of(i), i);
    }

    // key
    EXPECT_EQ(translator.key(), "seg_0_cg_0");

    // estimated byte size
    for (size_t i = 0; i < translator.num_cells(); ++i) {
        auto [file_idx, row_group_idx] =
            translator.get_file_and_row_group_index(i);
        auto& row_group_meta = row_group_meta_list[file_idx].Get(row_group_idx);
        auto usage = translator.estimated_byte_size_of_cell(i);
        EXPECT_EQ(usage.memory_bytes,
                  static_cast<int64_t>(row_group_meta.memory_size()));
    }

    // getting cells
    std::vector<cachinglayer::cid_t> cids = {0, 1};
    auto cells = translator.get_cells(cids);
    EXPECT_EQ(cells.size(), cids.size());

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

INSTANTIATE_TEST_SUITE_P(GroupChunkTranslatorTest,
                         GroupChunkTranslatorTest,
                         testing::Bool());