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
#include <filesystem>

#include "segcore/segment_c.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/DataGen.h"
#include "milvus-storage/filesystem/fs.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::test;

namespace fs = std::filesystem;

class FlushGrowingSegmentTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // create a temporary directory for test output
        test_dir_ = "/tmp/flush_growing_test_" + std::to_string(time(nullptr));
        fs::create_directories(test_dir_);

        // initialize Arrow filesystem singleton (local filesystem for testing)
        auto local_fs = arrow::fs::LocalFileSystem::Make();
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(local_fs);
    }

    void
    TearDown() override {
        // cleanup test directory
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }

    std::string test_dir_;
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment";
    std::string lob_base_path = test_dir_ + "/lobs";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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

    // verify manifest file exists
    EXPECT_TRUE(fs::exists(result.manifest_path));

    // cleanup
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_vec";
    std::string lob_base_path = test_dir_ + "/lobs_vec";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_str";
    std::string lob_base_path = test_dir_ + "/lobs_str";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_partial";
    std::string lob_base_path = test_dir_ + "/lobs_partial";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_text";
    std::string lob_base_path = test_dir_ + "/lobs_text";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_nullable";
    std::string lob_base_path = test_dir_ + "/lobs_nullable";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_empty";
    std::string lob_base_path = test_dir_ + "/lobs_empty";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_large";
    std::string lob_base_path = test_dir_ + "/lobs_large";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_multi_text";
    std::string lob_base_path = test_dir_ + "/lobs_multi_text";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_bool";
    std::string lob_base_path = test_dir_ + "/lobs_bool";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_numeric";
    std::string lob_base_path = test_dir_ + "/lobs_numeric";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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

        CFlushConfig config;
        std::string segment_path = test_dir_ + "/segment_fp16";
        std::string lob_base_path = test_dir_ + "/lobs_fp16";
        config.segment_path = segment_path.c_str();
        config.lob_base_path = lob_base_path.c_str();
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

        CFlushConfig config;
        std::string segment_path = test_dir_ + "/segment_bf16";
        std::string lob_base_path = test_dir_ + "/lobs_bf16";
        config.segment_path = segment_path.c_str();
        config.lob_base_path = lob_base_path.c_str();
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

        CFlushConfig config;
        std::string segment_path = test_dir_ + "/segment_binary";
        std::string lob_base_path = test_dir_ + "/lobs_binary";
        config.segment_path = segment_path.c_str();
        config.lob_base_path = lob_base_path.c_str();
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
    CFlushConfig config;
    std::string segment_path = test_dir_ + "/segment_sealed";
    std::string lob_base_path = test_dir_ + "/lobs_sealed";
    config.segment_path = segment_path.c_str();
    config.lob_base_path = lob_base_path.c_str();
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
}
