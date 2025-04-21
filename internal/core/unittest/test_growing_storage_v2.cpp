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
#include "milvus-storage/format/parquet/file_reader.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "test_utils/DataGen.h"
#include "pb/schema.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace milvus;
using namespace milvus::segcore;
namespace pb = milvus::proto;

class TestGrowingStorageV2 : public ::testing::Test {
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
                          false,
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

TEST_F(TestGrowingStorageV2, LoadFieldData) {
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
    auto pk_fid = schema->AddDebugField("pk", milvus::DataType::INT64, false);
    auto str_fid =
        schema->AddDebugField("str", milvus::DataType::VARCHAR, true);
    schema->set_primary_field_id(pk_fid);
    auto segment =
        milvus::segcore::CreateGrowingSegment(schema, milvus::empty_index_meta);
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

TEST_F(TestGrowingStorageV2, LoadWithStrategy) {
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

    auto channel = std::make_shared<milvus::ArrowReaderChannel>();
    int64_t memory_limit = 1024 * 1024 * 1024;  // 1GB
    uint64_t parallel_degree = 2;

    // read all row groups
    auto fr = std::make_shared<milvus_storage::FileRowGroupReader>(
        fs_, paths[0], schema_);
    auto row_group_metadata = fr->file_metadata()->GetRowGroupMetadataVector();
    std::vector<int64_t> row_groups(row_group_metadata.size());
    std::iota(row_groups.begin(), row_groups.end(), 0);
    std::vector<std::vector<int64_t>> row_group_lists = {row_groups};

    // Test MemoryBasedSplitStrategy
    {
        auto strategy =
            std::make_unique<MemoryBasedSplitStrategy>(row_group_metadata);
        milvus::segcore::LoadWithStrategy({paths[0]},
                                          channel,
                                          memory_limit,
                                          std::move(strategy),
                                          row_group_lists);

        // Verify each batch matches row group metadata
        std::shared_ptr<milvus::ArrowDataWrapper> wrapper;
        int64_t total_rows = 0;
        int64_t current_row_group = 0;

        while (channel->pop(wrapper)) {
            for (const auto& table : wrapper->arrow_tables) {
                // Verify batch size matches row group metadata
                EXPECT_EQ(table->num_rows(),
                          row_group_metadata.Get(current_row_group).row_num());
                total_rows += table->num_rows();
                current_row_group++;
            }
        }

        // Verify total rows match sum of all row groups
        int64_t expected_total_rows = 0;
        for (size_t i = 0; i < row_group_metadata.size(); ++i) {
            expected_total_rows += row_group_metadata.Get(i).row_num();
        }
        EXPECT_EQ(total_rows, expected_total_rows);
    }

    // Test ParallelDegreeSplitStrategy
    {
        channel = std::make_shared<milvus::ArrowReaderChannel>();
        auto strategy =
            std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);
        milvus::segcore::LoadWithStrategy({paths[0]},
                                          channel,
                                          memory_limit,
                                          std::move(strategy),
                                          row_group_lists);

        std::shared_ptr<milvus::ArrowDataWrapper> wrapper;
        int64_t total_rows = 0;
        int64_t current_row_group = 0;

        while (channel->pop(wrapper)) {
            for (const auto& table : wrapper->arrow_tables) {
                // Verify batch size matches row group metadata
                EXPECT_EQ(table->num_rows(),
                          row_group_metadata.Get(current_row_group).row_num());
                total_rows += table->num_rows();
                current_row_group++;
            }
        }

        // Verify total rows match sum of all row groups
        int64_t expected_total_rows = 0;
        for (size_t i = 0; i < row_group_metadata.size(); ++i) {
            expected_total_rows += row_group_metadata.Get(i).row_num();
        }
        EXPECT_EQ(total_rows, expected_total_rows);

        // Test with non-continuous row groups
        channel = std::make_shared<milvus::ArrowReaderChannel>();
        row_group_lists = {{0, 2}};  // Skip middle row group
        strategy =
            std::make_unique<ParallelDegreeSplitStrategy>(parallel_degree);
        milvus::segcore::LoadWithStrategy({paths[0]},
                                          channel,
                                          memory_limit,
                                          std::move(strategy),
                                          row_group_lists);

        total_rows = 0;
        current_row_group = 0;
        std::vector<int64_t> selected_row_groups = {0, 2};

        while (channel->pop(wrapper)) {
            for (const auto& table : wrapper->arrow_tables) {
                EXPECT_EQ(table->num_rows(),
                          row_group_metadata
                              .Get(selected_row_groups[current_row_group])
                              .row_num());
                total_rows += table->num_rows();
                current_row_group++;
            }
        }

        // Verify total rows match sum of selected row groups
        expected_total_rows = 0;
        for (int64_t rg : selected_row_groups) {
            expected_total_rows += row_group_metadata.Get(rg).row_num();
        }
        EXPECT_EQ(total_rows, expected_total_rows);
    }
}

TEST_F(TestGrowingStorageV2, TestAllDataTypes) {
    auto schema = std::make_shared<milvus::Schema>();
    auto bool_field =
        schema->AddDebugField("bool", milvus::DataType::BOOL, true);
    auto int8_field =
        schema->AddDebugField("int8", milvus::DataType::INT8, true);
    auto int16_field =
        schema->AddDebugField("int16", milvus::DataType::INT16, true);
    auto int32_field =
        schema->AddDebugField("int32", milvus::DataType::INT32, true);
    auto int64_field = schema->AddDebugField("int64", milvus::DataType::INT64);
    auto float_field =
        schema->AddDebugField("float", milvus::DataType::FLOAT, true);
    auto double_field =
        schema->AddDebugField("double", milvus::DataType::DOUBLE, true);
    auto varchar_field =
        schema->AddDebugField("varchar", milvus::DataType::VARCHAR, true);
    auto json_field =
        schema->AddDebugField("json", milvus::DataType::JSON, true);
    auto int_array_field = schema->AddDebugField(
        "int_array", milvus::DataType::ARRAY, milvus::DataType::INT8, true);
    auto long_array_field = schema->AddDebugField(
        "long_array", milvus::DataType::ARRAY, milvus::DataType::INT64, true);
    auto bool_array_field = schema->AddDebugField(
        "bool_array", milvus::DataType::ARRAY, milvus::DataType::BOOL, true);
    auto string_array_field = schema->AddDebugField("string_array",
                                                    milvus::DataType::ARRAY,
                                                    milvus::DataType::VARCHAR,
                                                    true);
    auto double_array_field = schema->AddDebugField("double_array",
                                                    milvus::DataType::ARRAY,
                                                    milvus::DataType::DOUBLE,
                                                    true);
    auto float_array_field = schema->AddDebugField(
        "float_array", milvus::DataType::ARRAY, milvus::DataType::FLOAT, true);
    auto vec = schema->AddDebugField("embeddings",
                                     milvus::DataType::VECTOR_FLOAT,
                                     128,
                                     knowhere::metric::L2);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", knowhere::metric::L2},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    int64_t dim = 128;
    // Write data to storage v2
    auto paths = std::vector<std::string>{path_ + "/19530.parquet",
                                          path_ + "/19531.parquet"};
    auto column_groups = std::vector<std::vector<int>>{
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, {15}};
    auto writer_memory = 16 * 1024 * 1024;
    auto storage_config = milvus_storage::StorageConfig();
    auto arrow_schema = schema->ConvertToArrowSchema();
    milvus_storage::PackedRecordBatchWriter writer(
        fs_, paths, arrow_schema, storage_config, column_groups, writer_memory);
    int64_t total_rows = 0;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);
        auto record_batch =
            ConvertToArrowRecordBatch(dataset, dim, arrow_schema);
        total_rows += record_batch->num_rows();

        EXPECT_TRUE(writer.Write(record_batch).ok());
    }
    EXPECT_TRUE(writer.Close().ok());

    // Load data back from storage v2
    LoadFieldDataInfo load_info;
    load_info.field_infos = {
        {0,
         FieldBinlogInfo{0,
                         total_rows,
                         std::vector<int64_t>{total_rows},
                         false,
                         std::vector<std::string>{paths[0]}}},
        {1,
         FieldBinlogInfo{1,
                         total_rows,
                         std::vector<int64_t>{total_rows},
                         false,
                         std::vector<std::string>{paths[1]}}},
    };
    load_info.storage_version = 2;
    segment->LoadFieldData(load_info);
}