// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <algorithm>
#include <filesystem>

#include "segcore/default_fs.h"
#include "segcore/segment_c.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/DataGen.h"
#include "storage/Util.h"
#include "storage/loon_ffi/property_singleton.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/transaction/transaction.h"

using namespace milvus;
using namespace milvus::segcore;

namespace fs = std::filesystem;

class FlushGrowingSegmentTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // create a temporary directory for test output
        test_dir_ = "/tmp/flush_growing_test_" + std::to_string(time(nullptr));
        fs::create_directories(test_dir_);

        // Arrow filesystem is initialized by init_gtest.cpp
    }

    void
    TearDown() override {
        // cleanup test directory
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }

    std::string test_dir_;

    std::vector<FieldDataPtr>
    ReadFlushedFieldData(const std::string& segment_path,
                         const CFlushResult& result,
                         FieldId field_id,
                         DataType data_type,
                         bool nullable,
                         int64_t dim,
                         DataType element_type = DataType::NONE) {
        auto properties =
            storage::LoonFFIPropertiesSingleton::GetInstance().GetProperties();
        EXPECT_NE(properties, nullptr);
        auto field_meta = gen_field_meta(
            1, 2, 3, field_id.get(), data_type, element_type, nullable);
        std::string manifest_json =
            "{\"base_path\":\"" + segment_path +
            "\",\"ver\":" + std::to_string(result.committed_version) + "}";
        return storage::GetFieldDatasFromManifest(manifest_json,
                                                  properties,
                                                  field_meta,
                                                  data_type,
                                                  dim,
                                                  element_type);
    }

    void
    AssertManifestHasColumn(const std::string& segment_path,
                            int64_t version,
                            FieldId field_id) {
        auto fs = GetDefaultArrowFileSystem();
        ASSERT_NE(fs, nullptr);

        auto txn_result = milvus_storage::api::transaction::Transaction::Open(
            fs, segment_path, version);
        ASSERT_TRUE(txn_result.ok()) << txn_result.status().ToString();
        auto txn = std::move(txn_result).ValueOrDie();

        auto manifest_result = txn->GetManifest();
        ASSERT_TRUE(manifest_result.ok())
            << manifest_result.status().ToString();
        auto manifest = manifest_result.ValueOrDie();

        auto column_name = std::to_string(field_id.get());
        bool found = false;
        for (const auto& column_group : manifest->columnGroups()) {
            ASSERT_NE(column_group, nullptr);
            found |= std::find(column_group->columns.begin(),
                               column_group->columns.end(),
                               column_name) != column_group->columns.end();
        }

        EXPECT_TRUE(found) << "missing field " << column_name
                           << " in committed manifest";
    }

    void
    AssertManifestColumnGroupsUseFormat(const std::string& segment_path,
                                        int64_t version,
                                        const std::string& format) {
        auto fs = GetDefaultArrowFileSystem();
        ASSERT_NE(fs, nullptr);

        auto txn_result = milvus_storage::api::transaction::Transaction::Open(
            fs, segment_path, version);
        ASSERT_TRUE(txn_result.ok()) << txn_result.status().ToString();
        auto txn = std::move(txn_result).ValueOrDie();

        auto manifest_result = txn->GetManifest();
        ASSERT_TRUE(manifest_result.ok())
            << manifest_result.status().ToString();
        auto manifest = manifest_result.ValueOrDie();

        ASSERT_FALSE(manifest->columnGroups().empty());
        for (const auto& column_group : manifest->columnGroups()) {
            ASSERT_NE(column_group, nullptr);
            EXPECT_EQ(column_group->format, format);
        }
    }
};

// test basic flush with scalar fields
TEST_F(FlushGrowingSegmentTest, BasicFlushScalarFields) {
    // create schema with scalar fields
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i32_fid = schema->AddDebugField("i32_field", DataType::INT32);
    auto f32_fid = schema->AddDebugField("f32_field", DataType::FLOAT);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 100;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(result.manifest_path, nullptr);
    ASSERT_EQ(result.num_rows, N);
    ASSERT_GT(result.committed_version, 0);
    AssertManifestHasColumn(segment_path, result.committed_version, RowFieldID);

    // Note: manifest path may be relative to ArrowFileSystem root path
    // The actual file existence should be verified via ArrowFileSystem API
    // For this unit test, we just verify the flush completed successfully

    // cleanup
    FreeFlushResult(&result);
}

// test explicit writer format in flush config
TEST_F(FlushGrowingSegmentTest, FlushUsesWriterFormatFromConfig) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    int N = 10;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_writer_format";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.writer_format = LOON_FORMAT_PARQUET;

    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);
    AssertManifestColumnGroupsUseFormat(
        segment_path, result.committed_version, LOON_FORMAT_PARQUET);

    FreeFlushResult(&result);
}

// test flush with vector fields
TEST_F(FlushGrowingSegmentTest, FlushWithVectorFields) {
    // create schema with vector field
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 128, "L2");
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_vec";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with string/varchar fields
TEST_F(FlushGrowingSegmentTest, FlushWithStringFields) {
    // create schema with varchar field
    auto schema = std::make_shared<Schema>();
    auto str_fid = schema->AddDebugField("str_pk", DataType::VARCHAR);
    auto i64_fid = schema->AddDebugField("i64_field", DataType::INT64);
    schema->set_primary_field_id(str_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 30;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_str";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with partial range (not from the beginning)
TEST_F(FlushGrowingSegmentTest, FlushPartialRange) {
    // create schema
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i32_fid = schema->AddDebugField("i32_field", DataType::INT32);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 100;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_partial";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush only rows 20-50
    int start = 20;
    int end = 50;
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), start, end, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, end - start);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with TEXT column config
TEST_F(FlushGrowingSegmentTest, FlushWithTextColumnConfig) {
#ifndef BUILD_VORTEX_BRIDGE
    GTEST_SKIP() << "Vortex support is not enabled";
#endif
    // create schema with varchar field (simulating TEXT)
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto text_fid = schema->AddDebugField("text_field", DataType::VARCHAR);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 20;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config with TEXT column
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_text";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;

    // configure TEXT column
    int64_t text_field_ids[] = {text_fid.get()};
    std::string text_lob_path =
        test_dir_ + "/lobs_text/field_" + std::to_string(text_fid.get());
    const char* text_lob_paths[] = {text_lob_path.c_str()};
    config.text_field_ids = text_field_ids;
    config.text_lob_paths = text_lob_paths;
    config.num_text_columns = 1;

    // flush data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with nullable fields
TEST_F(FlushGrowingSegmentTest, FlushWithNullableFields) {
    // create schema with nullable field
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable_i32", DataType::INT32, true);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_nullable";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushNullableFloatVectorKeepsCompactMapping) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 2, "L2", true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    bool valid_data[N] = {false, true, true};
    std::vector<float> compact_vectors = {1.0F, 2.0F, 3.0F, 4.0F};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto vec_array = CreateVectorDataArrayFrom(
        compact_vectors.data(), valid_data, N, 2, (*schema)[vec_fid]);
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_nullable_vec";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_NE(result.manifest_path, nullptr);
    ASSERT_EQ(result.num_rows, N);

    auto field_datas = ReadFlushedFieldData(
        segment_path, result, vec_fid, DataType::VECTOR_FLOAT, true, 2);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), N);
    ASSERT_EQ(field_data->get_valid_rows(), 2);
    EXPECT_FALSE(field_data->is_valid(0));
    ASSERT_TRUE(field_data->is_valid(1));
    ASSERT_TRUE(field_data->is_valid(2));

    auto row1 = static_cast<const float*>(field_data->RawValue(1));
    auto row2 = static_cast<const float*>(field_data->RawValue(2));
    ASSERT_NE(row1, nullptr);
    ASSERT_NE(row2, nullptr);
    EXPECT_FLOAT_EQ(row1[0], 1.0F);
    EXPECT_FLOAT_EQ(row1[1], 2.0F);
    EXPECT_FLOAT_EQ(row2[0], 3.0F);
    EXPECT_FLOAT_EQ(row2[1], 4.0F);

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushNullableInt8VectorKeepsCompactMapping) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_INT8, 4, "L2", true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    bool valid_data[N] = {true, false, true};
    std::vector<int8> compact_vectors = {1, 2, 3, 4, 5, 6, 7, 8};

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto vec_array = CreateVectorDataArrayFrom(
        compact_vectors.data(), valid_data, N, 2, (*schema)[vec_fid]);
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_int8_vec";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto field_datas = ReadFlushedFieldData(
        segment_path, result, vec_fid, DataType::VECTOR_INT8, true, 4);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), N);
    ASSERT_EQ(field_data->get_valid_rows(), 2);
    ASSERT_TRUE(field_data->is_valid(0));
    EXPECT_FALSE(field_data->is_valid(1));
    ASSERT_TRUE(field_data->is_valid(2));

    auto row0 = static_cast<const int8*>(field_data->RawValue(0));
    auto row2 = static_cast<const int8*>(field_data->RawValue(2));
    ASSERT_NE(row0, nullptr);
    ASSERT_NE(row2, nullptr);
    EXPECT_EQ(std::vector<int8>(row0, row0 + 4),
              std::vector<int8>({1, 2, 3, 4}));
    EXPECT_EQ(std::vector<int8>(row2, row2 + 4),
              std::vector<int8>({5, 6, 7, 8}));

    FreeFlushResult(&result);
}

TEST_F(FlushGrowingSegmentTest, FlushNullableSparseVectorKeepsCompactMapping) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "vec", DataType::VECTOR_SPARSE_U32_F32, 0, std::nullopt, true);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    constexpr int N = 3;
    std::vector<int64_t> row_ids = {0, 1, 2};
    std::vector<Timestamp> timestamps = {10, 11, 12};
    std::vector<int64_t> pks = {100, 101, 102};
    bool valid_data[N] = {false, true, true};
    auto sparse_vectors = GenerateRandomSparseFloatVector(2, 16, 0.5);

    auto insert_data = std::make_unique<InsertRecordProto>();
    insert_data->set_num_rows(N);
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, N, (*schema)[pk_fid]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    auto vec_array = CreateVectorDataArrayFrom(
        sparse_vectors.get(), valid_data, N, 2, (*schema)[vec_fid]);
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

    segment->PreInsert(N);
    segment->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_sparse_vec";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    auto field_datas = ReadFlushedFieldData(segment_path,
                                            result,
                                            vec_fid,
                                            DataType::VECTOR_SPARSE_U32_F32,
                                            true,
                                            0);
    ASSERT_EQ(field_datas.size(), 1);
    auto field_data = field_datas[0];
    ASSERT_EQ(field_data->get_num_rows(), N);
    ASSERT_EQ(field_data->get_valid_rows(), 2);
    EXPECT_FALSE(field_data->is_valid(0));
    ASSERT_TRUE(field_data->is_valid(1));
    ASSERT_TRUE(field_data->is_valid(2));

    auto row1 =
        static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
            field_data->RawValue(1));
    auto row2 =
        static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
            field_data->RawValue(2));
    ASSERT_NE(row1, nullptr);
    ASSERT_NE(row2, nullptr);
    EXPECT_EQ(row1->data_byte_size(), sparse_vectors[0].data_byte_size());
    EXPECT_EQ(row2->data_byte_size(), sparse_vectors[1].data_byte_size());

    FreeFlushResult(&result);
}

// test flush with empty range
TEST_F(FlushGrowingSegmentTest, FlushEmptyRange) {
    // create schema
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_empty";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush empty range (start == end)
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 10, 10, &config, &result);

    // should fail or return 0 rows
    if (status.error_code == Success) {
        EXPECT_EQ(result.num_rows, 0);
        FreeFlushResult(&result);
    }
}

// test flush with large data spanning multiple chunks
TEST_F(FlushGrowingSegmentTest, FlushLargeDataMultipleChunks) {
    // create schema
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 64, "L2");
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert large data that spans multiple chunks
    // default chunk size is typically 32K rows
    int N = 50000;  // should span multiple chunks
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_large";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush all data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with multiple TEXT columns
TEST_F(FlushGrowingSegmentTest, FlushMultipleTextColumns) {
#ifndef BUILD_VORTEX_BRIDGE
    GTEST_SKIP() << "Vortex support is not enabled";
#endif
    // create schema with multiple varchar fields (simulating TEXT)
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto text1_fid = schema->AddDebugField("text1", DataType::VARCHAR);
    auto text2_fid = schema->AddDebugField("text2", DataType::VARCHAR);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 20;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config with multiple TEXT columns
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_multi_text";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;

    // configure multiple TEXT columns
    int64_t text_field_ids[] = {text1_fid.get(), text2_fid.get()};
    std::string text1_lob_path =
        test_dir_ + "/lobs/field_" + std::to_string(text1_fid.get());
    std::string text2_lob_path =
        test_dir_ + "/lobs/field_" + std::to_string(text2_fid.get());
    const char* text_lob_paths[] = {text1_lob_path.c_str(),
                                    text2_lob_path.c_str()};
    config.text_field_ids = text_field_ids;
    config.text_lob_paths = text_lob_paths;
    config.num_text_columns = 2;

    // flush data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with bool field
TEST_F(FlushGrowingSegmentTest, FlushWithBoolField) {
    // create schema with bool field
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto bool_fid = schema->AddDebugField("bool_field", DataType::BOOL);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_bool";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with all numeric types
TEST_F(FlushGrowingSegmentTest, FlushAllNumericTypes) {
    // create schema with all numeric types
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i8_fid = schema->AddDebugField("i8_field", DataType::INT8);
    auto i16_fid = schema->AddDebugField("i16_field", DataType::INT16);
    auto i32_fid = schema->AddDebugField("i32_field", DataType::INT32);
    auto f32_fid = schema->AddDebugField("f32_field", DataType::FLOAT);
    auto f64_fid = schema->AddDebugField("f64_field", DataType::DOUBLE);
    schema->set_primary_field_id(pk_fid);

    // create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // generate and insert data
    int N = 50;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_numeric";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush data
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(result.num_rows, N);

    // cleanup
    FreeFlushResult(&result);
}

// test flush with different vector types
TEST_F(FlushGrowingSegmentTest, FlushDifferentVectorTypes) {
    // test float16 vector
    {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto vec_fid =
            schema->AddDebugField("vec", DataType::VECTOR_FLOAT16, 64, "L2");
        schema->set_primary_field_id(pk_fid);

        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        ASSERT_NE(segment, nullptr);

        int N = 30;
        auto dataset = DataGen(schema, N);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        CFlushConfig config{};
        std::string segment_path = test_dir_ + "/segment_fp16";
        config.segment_path = segment_path.c_str();
        config.read_version = -1;
        config.retry_limit = 3;
        config.text_field_ids = nullptr;
        config.text_lob_paths = nullptr;
        config.num_text_columns = 0;

        CFlushResult result;
        auto status =
            FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        ASSERT_EQ(result.num_rows, N);

        FreeFlushResult(&result);
    }

    // test bfloat16 vector
    {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto vec_fid =
            schema->AddDebugField("vec", DataType::VECTOR_BFLOAT16, 64, "L2");
        schema->set_primary_field_id(pk_fid);

        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        ASSERT_NE(segment, nullptr);

        int N = 30;
        auto dataset = DataGen(schema, N);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        CFlushConfig config{};
        std::string segment_path = test_dir_ + "/segment_bf16";
        config.segment_path = segment_path.c_str();
        config.read_version = -1;
        config.retry_limit = 3;
        config.text_field_ids = nullptr;
        config.text_lob_paths = nullptr;
        config.num_text_columns = 0;

        CFlushResult result;
        auto status =
            FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        ASSERT_EQ(result.num_rows, N);

        FreeFlushResult(&result);
    }

    // test binary vector
    {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto vec_fid = schema->AddDebugField(
            "vec", DataType::VECTOR_BINARY, 128, "JACCARD");
        schema->set_primary_field_id(pk_fid);

        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        ASSERT_NE(segment, nullptr);

        int N = 30;
        auto dataset = DataGen(schema, N);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        CFlushConfig config{};
        std::string segment_path = test_dir_ + "/segment_binary";
        config.segment_path = segment_path.c_str();
        config.read_version = -1;
        config.retry_limit = 3;
        config.text_field_ids = nullptr;
        config.text_lob_paths = nullptr;
        config.num_text_columns = 0;

        CFlushResult result;
        auto status =
            FlushGrowingSegmentData(segment.get(), 0, N, &config, &result);

        ASSERT_EQ(status.error_code, Success) << status.error_msg;
        ASSERT_EQ(result.num_rows, N);

        FreeFlushResult(&result);
    }
}

// test FreeFlushResult with null manifest_path
TEST_F(FlushGrowingSegmentTest, FreeFlushResultNull) {
    CFlushResult result;
    result.manifest_path = nullptr;
    result.committed_version = 0;
    result.num_rows = 0;

    // should not crash
    FreeFlushResult(&result);
}

// test invalid segment type (sealed segment should fail)
TEST_F(FlushGrowingSegmentTest, FlushSealedSegmentFails) {
    // create schema
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    // create sealed segment (not growing)
    auto segment = CreateSealedSegment(schema, empty_index_meta);
    ASSERT_NE(segment, nullptr);

    // prepare flush config
    CFlushConfig config{};
    std::string segment_path = test_dir_ + "/segment_sealed";
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.text_field_ids = nullptr;
    config.text_lob_paths = nullptr;
    config.num_text_columns = 0;

    // flush should fail for sealed segment
    CFlushResult result;
    auto status =
        FlushGrowingSegmentData(segment.get(), 0, 10, &config, &result);

    EXPECT_NE(status.error_code, Success);
    free(const_cast<char*>(status.error_msg));
}
