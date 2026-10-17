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

#include <arrow/api.h>
#include <parquet/arrow/writer.h>

#include <filesystem>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <vector>

#include "common/Schema.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::storage::test {
namespace {

class StorageV2FieldDataVisitorTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        original_slice_size_ = FILE_SLICE_SIZE.load();
        FILE_SLICE_SIZE.store(DEFAULT_FIELD_MAX_MEMORY_LIMIT / kParallelDegree);
        directory_ = LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "milvus-storage-v2-visitor-XXXXXX",
            "storage-v2 visitor test");
        StorageConfig config;
        config.storage_type = "local";
        config.root_path = directory_->Path();
        fs_ = InitArrowFileSystem(config);
        auto schema = std::make_shared<Schema>();
        field_id_ = schema->AddDebugField("value", DataType::INT64).get();
        schema_ = schema->ConvertToArrowSchema();
    }

    void
    TearDown() override {
        FILE_SLICE_SIZE.store(original_slice_size_);
    }

    void
    WriteFile(int file_id, int64_t first_value) {
        const auto parent =
            directory_->Path() + "/" + std::to_string(field_id_);
        ASSERT_TRUE(fs_->CreateDir(parent).ok());
        const auto path = parent + "/" + std::to_string(file_id);
        auto sink_result = fs_->OpenOutputStream(path);
        ASSERT_TRUE(sink_result.ok()) << sink_result.status().ToString();
        auto sink = sink_result.ValueOrDie();
        auto writer_result = ::parquet::arrow::FileWriter::Open(
            *schema_, arrow::default_memory_pool(), sink);
        ASSERT_TRUE(writer_result.ok()) << writer_result.status().ToString();
        auto writer = std::move(writer_result).ValueOrDie();
        milvus_storage::RowGroupMetadataVector row_groups;
        // ParquetFileWriter::Flush retains batches below the 1 MiB
        // row-group threshold. Write explicit physical groups so this reader
        // fixture can exercise many small groups without large payloads.
        for (int64_t group = 0; group < kGroups; ++group) {
            arrow::Int64Builder builder;
            for (int64_t row = 0; row < kRowsPerGroup; ++row) {
                ASSERT_TRUE(
                    builder.Append(first_value + group * kRowsPerGroup + row)
                        .ok());
            }
            std::shared_ptr<arrow::Array> values;
            ASSERT_TRUE(builder.Finish(&values).ok());
            ASSERT_TRUE(writer->NewBufferedRowGroup().ok());
            ASSERT_TRUE(writer
                            ->WriteRecordBatch(*arrow::RecordBatch::Make(
                                schema_, kRowsPerGroup, {values}))
                            .ok());
            row_groups.Add(milvus_storage::RowGroupMetadata(
                kRowsPerGroup * sizeof(int64_t),
                kRowsPerGroup,
                group * kRowsPerGroup));
        }
        milvus_storage::GroupFieldIDList groups;
        groups.AddFieldIDList(milvus_storage::FieldIDList({field_id_}));
        ASSERT_TRUE(
            writer
                ->AddKeyValueMetadata(arrow::key_value_metadata(
                    {milvus_storage::GROUP_FIELD_ID_LIST_META_KEY,
                     milvus_storage::ROW_GROUP_META_KEY,
                     milvus_storage::STORAGE_VERSION_KEY},
                    {groups.Serialize(), row_groups.Serialize(), "1.0.0"}))
                .ok());
        ASSERT_TRUE(writer->Close().ok());
        ASSERT_TRUE(sink->Close().ok());
        auto reader_result =
            milvus_storage::FileRowGroupReader::Make(fs_, path);
        ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
        auto reader = reader_result.ValueOrDie();
        const auto metadata = reader->file_metadata();
        const auto actual_groups = metadata->GetRowGroupMetadataVector();
        ASSERT_EQ(actual_groups.size(), kGroups);
        ASSERT_GT(actual_groups.size(), kParallelDegree);
        const auto parquet_metadata = metadata->GetParquetMetadata();
        ASSERT_EQ(parquet_metadata->num_row_groups(), kGroups);
        for (int64_t group = 0; group < kGroups; ++group) {
            ASSERT_EQ(actual_groups.Get(group).row_num(), kRowsPerGroup);
            ASSERT_EQ(actual_groups.Get(group).row_offset(),
                      group * kRowsPerGroup);
            ASSERT_EQ(parquet_metadata->RowGroup(group)->num_rows(),
                      kRowsPerGroup);
        }
        ASSERT_TRUE(reader->Close().ok());
        if (files_.empty()) {
            files_.emplace_back();
        }
        files_.front().push_back(path);
    }

    static std::vector<int64_t>
    Values(const std::vector<FieldDataPtr>& batches) {
        std::vector<int64_t> values;
        for (const auto& batch : batches) {
            const auto* data = static_cast<const int64_t*>(batch->Data());
            values.insert(values.end(), data, data + batch->get_num_rows());
        }
        return values;
    }

    static std::vector<int64_t>
    Sequence(int64_t first, size_t count) {
        std::vector<int64_t> values(count);
        std::iota(values.begin(), values.end(), first);
        return values;
    }

    static constexpr size_t kParallelDegree = 4;
    static constexpr int64_t kRowsPerGroup = 4;
    static constexpr int64_t kGroups = 20;
    static constexpr int64_t kRowsPerFile = kRowsPerGroup * kGroups;
    int64_t original_slice_size_;
    int64_t field_id_;
    std::shared_ptr<LocalDirectory> directory_;
    milvus_storage::ArrowFileSystemPtr fs_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::vector<std::string>> files_;
};

TEST_F(StorageV2FieldDataVisitorTest, PreservesRowGroupAndFileOrder) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(1000, 0));
    ASSERT_NO_FATAL_FAILURE(WriteFile(1001, kRowsPerFile));
    std::vector<FieldDataPtr> batches;
    const auto caller = std::this_thread::get_id();
    const auto outcome = VisitFieldDataFromStorageV2(
        files_,
        field_id_,
        DataType::INT64,
        DataType::NONE,
        1,
        fs_,
        [&](FieldDataPtr batch) {
            EXPECT_EQ(std::this_thread::get_id(), caller);
            batches.push_back(std::move(batch));
            return VisitControl::Continue;
        });
    EXPECT_EQ(outcome, VisitOutcome::Exhausted);
    EXPECT_EQ(Values(batches), Sequence(0, 2 * kRowsPerFile));
    // The bounded strategy returns blocks rather than starting a reader for
    // every row group.
    ASSERT_EQ(batches.size(), 2 * kParallelDegree);
    for (const auto& batch : batches) {
        EXPECT_EQ(batch->get_num_rows(), kRowsPerFile / kParallelDegree);
    }
}

TEST_F(StorageV2FieldDataVisitorTest, StopDrainsPendingProducer) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(1000, 0));
    size_t calls = 0;
    EXPECT_EQ(VisitFieldDataFromStorageV2(
                  files_,
                  field_id_,
                  DataType::INT64,
                  DataType::NONE,
                  1,
                  fs_,
                  [&](FieldDataPtr batch) {
                      EXPECT_EQ(Values({batch}),
                                Sequence(0, kRowsPerFile / kParallelDegree));
                      ++calls;
                      return VisitControl::Stop;
                  }),
              VisitOutcome::Stopped);
    EXPECT_EQ(calls, 1);
    const auto full = GetFieldDatasFromStorageV2(
        files_, field_id_, DataType::INT64, DataType::NONE, 1, fs_);
    EXPECT_EQ(Values(full), Sequence(0, kRowsPerFile));
}

TEST_F(StorageV2FieldDataVisitorTest, VisitorExceptionDrainsAndPreservesError) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(1000, 0));
    size_t calls = 0;
    try {
        VisitFieldDataFromStorageV2(
            files_,
            field_id_,
            DataType::INT64,
            DataType::NONE,
            1,
            fs_,
            [&](FieldDataPtr) -> VisitControl {
                ++calls;
                throw std::runtime_error("visitor failure");
            });
        FAIL() << "expected visitor failure";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "visitor failure");
    }
    EXPECT_EQ(calls, 1);
    const auto full = GetFieldDatasFromStorageV2(
        files_, field_id_, DataType::INT64, DataType::NONE, 1, fs_);
    EXPECT_EQ(Values(full), Sequence(0, kRowsPerFile));
}

TEST_F(StorageV2FieldDataVisitorTest, PaginationKeepsWholeSelectedRowGroups) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(1000, 0));
    const auto page = GetFieldDatasFromStorageV2(
        files_, field_id_, DataType::INT64, DataType::NONE, 1, fs_, 9, 10);
    EXPECT_EQ(Values(page), Sequence(8, 12));
    const auto last = GetFieldDatasFromStorageV2(files_,
                                                 field_id_,
                                                 DataType::INT64,
                                                 DataType::NONE,
                                                 1,
                                                 fs_,
                                                 9,
                                                 kRowsPerFile - kRowsPerGroup);
    EXPECT_EQ(Values(last),
              Sequence(kRowsPerFile - kRowsPerGroup, kRowsPerGroup));
    const auto past_end = GetFieldDatasFromStorageV2(files_,
                                                     field_id_,
                                                     DataType::INT64,
                                                     DataType::NONE,
                                                     1,
                                                     fs_,
                                                     9,
                                                     kRowsPerFile);
    EXPECT_TRUE(past_end.empty());
}

}  // namespace
}  // namespace milvus::storage::test
