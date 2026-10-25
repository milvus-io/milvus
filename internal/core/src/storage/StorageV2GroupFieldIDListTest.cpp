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
#include <unistd.h>

#include <arrow/api.h>
#include <arrow/filesystem/localfs.h>
#include <parquet/arrow/writer.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "storage/Util.h"

namespace milvus::storage {
namespace {

// Legacy StorageV2 column groups carry no FieldBinlog.ChildFields, so the
// field set of a multi-field group is read from the parquet footers of its
// files (#54613).
class StorageV2GroupFieldIDListTest : public ::testing::Test {
 protected:
    static constexpr int64_t kGroupId = 0;

    void
    SetUp() override {
        root_ = (std::filesystem::temp_directory_path() /
                 ("milvus_group_field_id_list_" + std::to_string(::getpid())))
                    .string();
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(root_ + "/" +
                                            std::to_string(kGroupId));
        fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
    }

    void
    TearDown() override {
        std::filesystem::remove_all(root_);
    }

    std::string
    Path(int64_t log_id) const {
        return root_ + "/" + std::to_string(kGroupId) + "/" +
               std::to_string(log_id);
    }

    // Writes one legacy-format file of group kGroupId: one INT64 column per
    // field, footer metadata without any ChildFields counterpart.
    void
    WriteGroupFile(int64_t log_id,
                   const std::vector<int64_t>& field_ids,
                   const std::vector<int64_t>& values) const {
        arrow::FieldVector fields;
        arrow::ArrayVector columns;
        for (auto field_id : field_ids) {
            fields.push_back(arrow::field(
                std::to_string(field_id),
                arrow::int64(),
                false,
                arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                          {std::to_string(field_id)})));
            arrow::Int64Builder builder;
            std::vector<int64_t> column(values.size());
            for (size_t i = 0; i < values.size(); ++i) {
                column[i] = values[i] * 1000 + field_id;
            }
            ASSERT_TRUE(builder.AppendValues(column).ok());
            std::shared_ptr<arrow::Array> array;
            ASSERT_TRUE(builder.Finish(&array).ok());
            columns.push_back(array);
        }
        auto schema = arrow::schema(fields);
        auto sink_result = fs_->OpenOutputStream(Path(log_id));
        ASSERT_TRUE(sink_result.ok()) << sink_result.status().ToString();
        auto sink = sink_result.ValueOrDie();
        auto writer_result = ::parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), sink);
        ASSERT_TRUE(writer_result.ok()) << writer_result.status().ToString();
        auto writer = std::move(writer_result).ValueOrDie();
        ASSERT_TRUE(writer->NewBufferedRowGroup().ok());
        ASSERT_TRUE(writer
                        ->WriteRecordBatch(*arrow::RecordBatch::Make(
                            schema, values.size(), columns))
                        .ok());
        milvus_storage::GroupFieldIDList groups;
        groups.AddFieldIDList(milvus_storage::FieldIDList(field_ids));
        milvus_storage::RowGroupMetadataVector row_groups;
        row_groups.Add(milvus_storage::RowGroupMetadata(
            values.size() * 8 * field_ids.size(), values.size(), 0));
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
    }

    std::string root_;
    milvus_storage::ArrowFileSystemPtr fs_;
};

TEST_F(StorageV2GroupFieldIDListTest, MatchingFilesReturnSharedFieldList) {
    ASSERT_NO_FATAL_FAILURE(WriteGroupFile(2, {0, 1, 100}, {1, 2}));
    ASSERT_NO_FATAL_FAILURE(WriteGroupFile(10, {0, 1, 100}, {3, 4}));

    auto field_id_list = GetColumnGroupFieldIDList(
        FieldId(kGroupId), {Path(2), Path(10)}, nullptr, fs_);
    EXPECT_EQ(field_id_list, milvus_storage::FieldIDList({0, 1, 100}));
}

TEST_F(StorageV2GroupFieldIDListTest, DifferingFilesThrowNamingTheFile) {
    ASSERT_NO_FATAL_FAILURE(WriteGroupFile(2, {0, 1, 100}, {1, 2}));
    ASSERT_NO_FATAL_FAILURE(WriteGroupFile(10, {0, 1, 100, 101}, {3, 4}));

    try {
        GetColumnGroupFieldIDList(
            FieldId(kGroupId), {Path(2), Path(10)}, nullptr, fs_);
        FAIL() << "differing field id lists in one group must be rejected";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
        EXPECT_NE(std::string(error.what()).find(Path(10)), std::string::npos)
            << error.what();
    }
}

TEST_F(StorageV2GroupFieldIDListTest, SingleFieldGroupReadsNoFooter) {
    // A user-field group is its own field list; the files are not opened.
    auto field_id_list = GetColumnGroupFieldIDList(
        FieldId(101), {Path(404), Path(405)}, nullptr, fs_);
    EXPECT_EQ(field_id_list, milvus_storage::FieldIDList({101}));
}

TEST_F(StorageV2GroupFieldIDListTest, VisitReadsFieldFromMatchingGroup) {
    ASSERT_NO_FATAL_FAILURE(WriteGroupFile(10, {0, 1, 100}, {3, 4}));
    ASSERT_NO_FATAL_FAILURE(WriteGroupFile(2, {0, 1, 100}, {1, 2}));

    std::vector<int64_t> values;
    auto outcome = VisitFieldDataFromStorageV2(
        {{Path(10), Path(2)}},
        100,
        DataType::INT64,
        DataType::NONE,
        1,
        fs_,
        [&](FieldDataPtr batch) {
            const auto* data = static_cast<const int64_t*>(batch->Data());
            values.insert(values.end(), data, data + batch->get_num_rows());
            return VisitControl::Continue;
        });
    EXPECT_EQ(outcome, VisitOutcome::Exhausted);
    EXPECT_EQ(values, (std::vector<int64_t>{1100, 2100, 3100, 4100}));
}

TEST_F(StorageV2GroupFieldIDListTest, VisitRejectsFieldMissingFromFirstFile) {
    // Field 101 is stored by log 10 only. Reading the field set from the
    // first file alone used to report it as absent.
    ASSERT_NO_FATAL_FAILURE(WriteGroupFile(2, {0, 1, 100}, {1, 2}));
    ASSERT_NO_FATAL_FAILURE(WriteGroupFile(10, {0, 1, 100, 101}, {3, 4}));

    try {
        VisitFieldDataFromStorageV2(
            {{Path(2), Path(10)}},
            101,
            DataType::INT64,
            DataType::NONE,
            1,
            fs_,
            [](FieldDataPtr) { return VisitControl::Continue; });
        FAIL() << "field missing from the first file must not be ignored";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
    }
}

}  // namespace
}  // namespace milvus::storage
