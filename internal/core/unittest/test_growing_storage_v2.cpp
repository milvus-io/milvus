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
#include <algorithm>
#include <cstdint>
#include "arrow/table_builder.h"
#include "arrow/type_fwd.h"
#include "common/FieldDataInterface.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/writer.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>

class TestPackedLoad : public ::testing::Test {
    void
    SetUp() override {
        auto conf = milvus_storage::ArrowFileSystemConfig();
        conf.storage_type = "local";
        conf.root_path = path_;
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);
        fs_ = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();
        SetUpCommonData();
    }

    void
    SetUpCommonData() {
        record_batch_ = randomRecordBatch();
        table_ = arrow::Table::FromRecordBatches({record_batch_}).ValueOrDie();
        schema_ = table_->schema();
    }

    std::shared_ptr<arrow::RecordBatch>
    randomRecordBatch() {
        arrow::Int64Builder ts_builder;
        arrow::Int64Builder pk_builder;
        arrow::StringBuilder str_builder;

        ts_values = {rand() % 10000, rand() % 10000, rand() % 10000};
        pk_values = {rand() % 10000000, rand() % 10000000, rand() % 10000000};
        str_values = {
            random_string(10000), random_string(10000), random_string(10000)};

        ts_builder.AppendValues(ts_values).ok();
        pk_builder.AppendValues(pk_values).ok();
        str_builder.AppendValues(str_values).ok();

        std::shared_ptr<arrow::Array> ts_array;
        std::shared_ptr<arrow::Array> pk_array;
        std::shared_ptr<arrow::Array> str_array;

        ts_builder.Finish(&ts_array).ok();
        pk_builder.Finish(&pk_array).ok();
        str_builder.Finish(&str_array).ok();

        std::vector<std::shared_ptr<arrow::Array>> arrays = {
            ts_array, pk_array, str_array};
        auto schema = arrow::schema(
            {arrow::field("ts",
                          arrow::int64(),
                          true,
                          arrow::key_value_metadata(
                              {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"})),
             arrow::field("pk",
                          arrow::int64(),
                          true,
                          arrow::key_value_metadata(
                              {milvus_storage::ARROW_FIELD_ID_KEY}, {"101"})),
             arrow::field("str",
                          arrow::utf8(),
                          true,
                          arrow::key_value_metadata(
                              {milvus_storage::ARROW_FIELD_ID_KEY}, {"102"}))});
        return arrow::RecordBatch::Make(schema, 3, arrays);
    }

    std::string
    random_string(size_t length) {
        auto randchar = []() -> char {
            const char charset[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
            const size_t max_index = (sizeof(charset) - 1);
            return charset[rand() % max_index];
        };
        std::string str(length, 0);
        std::generate_n(str.begin(), length, randchar);
        return str;
    }

 protected:
    milvus_storage::ArrowFileSystemPtr fs_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::RecordBatch> record_batch_;
    std::shared_ptr<arrow::Table> table_;
    std::string path_ = "/tmp";

    std::vector<int64_t> ts_values;
    std::vector<int64_t> pk_values;
    std::vector<std::basic_string<char>> str_values;
};

TEST_F(TestPackedLoad, TestPackedLoad) {
    int batch_size = 1000;

    auto paths = std::vector<std::string>{path_ + "/10000.parquet",
                                          path_ + "/10001.parquet"};
    auto column_groups = std::vector<std::vector<int>>{{2}, {0, 1}};
    auto writer_memory = 16 * 1024 * 1024;
    auto storage_config = milvus_storage::StorageConfig();
    milvus_storage::PackedRecordBatchWriter writer(
        fs_, paths, schema_, storage_config, column_groups, writer_memory);
    for (int i = 0; i < batch_size; ++i) {
        EXPECT_TRUE(writer.Write(record_batch_).ok());
    }
    EXPECT_TRUE(writer.Close().ok());

    auto schema = std::make_shared<milvus::Schema>();
    auto ts_fid = schema->AddDebugField("ts", milvus::DataType::INT64, true);
    auto pk_fid = schema->AddDebugField("pk", milvus::DataType::INT64, true);
    auto str_fid =
        schema->AddDebugField("str", milvus::DataType::VARCHAR, true);
    schema->set_primary_field_id(pk_fid);
    std::cout << schema->ConvertToArrowSchema()->ToString(true) << std::endl;
    auto segment = milvus::segcore::CreateGrowingSegment(
        schema, milvus::segcore::empty_index_meta);
    LoadFieldDataInfo load_info;
    load_info.field_infos = {
        {0,
         FieldBinlogInfo{0,
                         3000,
                         std::vector<int64_t>{3000},
                         false,
                         std::vector<std::string>{paths[0]}}},
        {1,
         FieldBinlogInfo{1,
                         3000,
                         std::vector<int64_t>{3000},
                         false,
                         std::vector<std::string>{paths[1]}}},
    };
    load_info.storage_version = 2;
    segment->LoadFieldData(load_info);
}