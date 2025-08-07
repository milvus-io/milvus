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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <unistd.h>
#include <memory>
#include <string>

#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/FieldMeta.h"
#include "common/File.h"
#include "common/Geometry.h"
#include "common/Types.h"
#include "storage/Event.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"

using namespace milvus;

TEST(chunk, test_int64_field) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT64);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(
        FieldName("a"), milvus::FieldId(1), DataType::INT64, false);
    auto chunk = create_chunk(field_meta, rb_reader);
    auto span = std::dynamic_pointer_cast<FixedWidthChunk>(chunk)->Span();
    EXPECT_EQ(span.row_count(), data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        auto n = *(int64_t*)((char*)span.data() + i * span.element_sizeof());
        EXPECT_EQ(n, data[i]);
    }
}

TEST(chunk, test_variable_field) {
    FixedVector<std::string> data = {
        "test1", "test2", "test3", "test4", "test5"};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::VARCHAR);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(
        FieldName("a"), milvus::FieldId(1), DataType::STRING, false);
    auto chunk = create_chunk(field_meta, rb_reader);
    auto views = std::dynamic_pointer_cast<StringChunk>(chunk)->StringViews(
        std::nullopt);
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(views.first[i], data[i]);
    }
}

TEST(chunk, test_json_field) {
    auto row_num = 100;
    FixedVector<Json> data;
    data.reserve(row_num);
    std::string json_str = "{\"key\": \"value\"}";
    for (auto i = 0; i < row_num; i++) {
        auto json = Json(json_str.data(), json_str.size());
        data.push_back(std::move(json));
    }
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::JSON);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();

    auto get_record_batch_reader =
        [&]() -> std::pair<std::shared_ptr<::arrow::RecordBatchReader>,
                           std::unique_ptr<parquet::arrow::FileReader>> {
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());
        return {rb_reader, std::move(arrow_reader)};
    };

    {
        auto [rb_reader, arrow_reader] = get_record_batch_reader();
        // nullable=false
        FieldMeta field_meta(
            FieldName("a"), milvus::FieldId(1), DataType::JSON, false);
        auto chunk = create_chunk(field_meta, rb_reader);
        {
            auto [views, valid] =
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::nullopt);
            EXPECT_EQ(row_num, views.size());
            for (size_t i = 0; i < row_num; ++i) {
                EXPECT_EQ(views[i], data[i].data());
                //nullable is false, no judging valid
            }
        }
        {
            auto start = 10;
            auto len = 20;
            auto [views, valid] =
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len));
            EXPECT_EQ(len, views.size());
            for (size_t i = 0; i < len; ++i) {
                EXPECT_EQ(views[i], data[i].data());
            }
        }
    }
    {
        auto [rb_reader, arrow_reader] = get_record_batch_reader();
        // nullable=true
        FieldMeta field_meta(
            FieldName("a"), milvus::FieldId(1), DataType::JSON, true);
        auto chunk = create_chunk(field_meta, rb_reader);
        {
            auto [views, valid] =
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::nullopt);
            EXPECT_EQ(row_num, views.size());
            for (size_t i = 0; i < row_num; ++i) {
                EXPECT_EQ(views[i], data[i].data());
                EXPECT_TRUE(valid[i]);  //no input valid map, all padded as true
            }
        }
        {
            auto start = 10;
            auto len = 20;
            auto [views, valid] =
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len));
            EXPECT_EQ(len, views.size());
            for (size_t i = 0; i < len; ++i) {
                EXPECT_EQ(views[i], data[i].data());
                EXPECT_TRUE(valid[i]);  //no input valid map, all padded as true
            }
        }
        {
            auto start = -1;
            auto len = 5;
            EXPECT_THROW(
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len)),
                milvus::SegcoreError);
        }
        {
            auto start = 0;
            auto len = row_num + 1;
            EXPECT_THROW(
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len)),
                milvus::SegcoreError);
        }
        {
            auto start = 95;
            auto len = 11;
            EXPECT_THROW(
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len)),
                milvus::SegcoreError);
        }
    }
}

TEST(chunk, test_null_field) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT64, true);
    uint8_t* valid_data_ = new uint8_t[1]{0x13};
    field_data->FillFieldData(data.data(), valid_data_, data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(
        FieldName("a"), milvus::FieldId(1), DataType::INT64, true);
    auto chunk = create_chunk(field_meta, rb_reader);
    auto span = std::dynamic_pointer_cast<FixedWidthChunk>(chunk)->Span();
    EXPECT_EQ(span.row_count(), data.size());
    data = {1, 2, 0, 0, 5};
    FixedVector<bool> valid_data = {true, true, false, false, true};
    for (size_t i = 0; i < data.size(); ++i) {
        auto n = *(int64_t*)((char*)span.data() + i * span.element_sizeof());
        EXPECT_EQ(n, data[i]);
        auto v = *(bool*)((char*)span.valid_data() + i);
        EXPECT_EQ(v, valid_data[i]);
    }
    delete[] valid_data_;
}

TEST(chunk, test_array) {
    milvus::proto::schema::ScalarField field_string_data;
    field_string_data.mutable_string_data()->add_data("test_array1");
    field_string_data.mutable_string_data()->add_data("test_array2");
    field_string_data.mutable_string_data()->add_data("test_array3");
    field_string_data.mutable_string_data()->add_data("test_array4");
    field_string_data.mutable_string_data()->add_data("test_array5");
    auto string_array = Array(field_string_data);
    FixedVector<Array> data = {string_array};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::ARRAY);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::ARRAY,
                         DataType::STRING,
                         false);
    auto chunk = create_chunk(field_meta, rb_reader);
    auto [views, valid] =
        std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(std::nullopt);
    EXPECT_EQ(views.size(), 1);
    auto& arr = views[0];
    for (size_t i = 0; i < arr.length(); ++i) {
        auto str = arr.get_data<std::string>(i);
        EXPECT_EQ(str, field_string_data.string_data().data(i));
    }
}

TEST(chunk, test_array_views) {
    milvus::proto::schema::ScalarField field_string_data;
    field_string_data.mutable_string_data()->add_data("a");
    field_string_data.mutable_string_data()->add_data("b");
    field_string_data.mutable_string_data()->add_data("c");
    field_string_data.mutable_string_data()->add_data("d");
    field_string_data.mutable_string_data()->add_data("e");
    auto string_array = Array(field_string_data);

    auto array_count = 10;
    FixedVector<Array> data;
    data.reserve(array_count);
    for (int i = 0; i < array_count; i++) {
        data.emplace_back(string_array);
    }

    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::ARRAY);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("field1"),
                         milvus::FieldId(1),
                         DataType::ARRAY,
                         DataType::STRING,
                         true);
    auto chunk = create_chunk(field_meta, rb_reader);
    {
        auto [views, valid] =
            std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(std::nullopt);
        EXPECT_EQ(views.size(), array_count);
        for (auto i = 0; i < array_count; i++) {
            auto& arr = views[i];
            for (size_t j = 0; j < arr.length(); ++j) {
                auto str = arr.get_data<std::string>(j);
                EXPECT_EQ(str, field_string_data.string_data().data(j));
            }
        }
    }
    {
        auto start = 2;
        auto len = 5;
        auto [views, valid] =
            std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(
                std::make_pair(start, len));
        EXPECT_EQ(views.size(), len);
        for (auto i = 0; i < len; i++) {
            auto& arr = views[i];
            for (size_t j = 0; j < arr.length(); ++j) {
                auto str = arr.get_data<std::string>(j);
                EXPECT_EQ(str, field_string_data.string_data().data(j));
            }
        }
    }
    {
        auto start = -1;
        auto len = 5;
        EXPECT_THROW(std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(
                         std::make_pair(start, len)),
                     milvus::SegcoreError);
    }
    {
        auto start = 0;
        auto len = array_count + 1;
        EXPECT_THROW(std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(
                         std::make_pair(start, len)),
                     milvus::SegcoreError);
    }
    {
        auto start = 5;
        auto len = 7;
        EXPECT_THROW(std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(
                         std::make_pair(start, len)),
                     milvus::SegcoreError);
    }
}

TEST(chunk, test_sparse_float) {
    auto n_rows = 100;
    auto vecs = milvus::segcore::GenerateRandomSparseFloatVector(
        n_rows, kTestSparseDim, kTestSparseVectorDensity);
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VECTOR_SPARSE_FLOAT, false, kTestSparseDim, n_rows);
    field_data->FillFieldData(vecs.get(), n_rows);

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::VECTOR_SPARSE_FLOAT,
                         kTestSparseDim,
                         "IP",
                         false);
    auto chunk = create_chunk(field_meta, rb_reader);
    auto vec = std::dynamic_pointer_cast<SparseFloatVectorChunk>(chunk)->Vec();
    for (size_t i = 0; i < n_rows; ++i) {
        auto v1 = vec[i];
        auto v2 = vecs[i];
        EXPECT_EQ(v1.size(), v2.size());
        for (size_t j = 0; j < v1.size(); ++j) {
            EXPECT_EQ(v1[j].val, v2[j].val);
        }
    }
}

class TempDir {
 public:
    TempDir() {
        auto path = boost::filesystem::unique_path("%%%%_%%%%");
        auto abs_path = boost::filesystem::temp_directory_path() / path;
        boost::filesystem::create_directory(abs_path);
        dir_ = abs_path;
    }

    ~TempDir() {
        boost::filesystem::remove_all(dir_);
    }

    std::string
    dir() {
        return dir_.string();
    }

 private:
    boost::filesystem::path dir_;
};

TEST(chunk, test_geometry_field) {
    // Create simple geometry data - just a few points
    FixedVector<std::string> data;
    data.reserve(3);

    // Create simple point geometries using WKT format
    std::string point1_wkt = "POINT(0 0)";
    std::string point2_wkt = "POINT(1 1)";
    std::string point3_wkt = "POINT(2 2)";

    // Convert WKT to WKB format
    data.push_back(Geometry(point1_wkt.data()).to_wkb_string());
    data.push_back(Geometry(point2_wkt.data()).to_wkb_string());
    data.push_back(Geometry(point3_wkt.data()).to_wkb_string());

    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::GEOMETRY);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(
        FieldName("a"), milvus::FieldId(1), DataType::GEOMETRY, false);
    auto chunk = create_chunk(field_meta, rb_reader);

    // Since GeometryChunk is an alias for StringChunk, we can use StringViews
    auto views = std::dynamic_pointer_cast<GeometryChunk>(chunk)->StringViews(
        std::nullopt);
    EXPECT_EQ(views.first.size(), data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(views.first[i], data[i]);
    }
}

TEST(chunk, test_geometry_field_nullable_all_valid) {
    // Prepare geometry data (WKB strings) – all rows valid but nullable flag enabled
    FixedVector<std::string> data;
    data.reserve(3);
    data.push_back(Geometry("POINT(0 0)").to_wkb_string());
    data.push_back(Geometry("POINT(1 1)").to_wkb_string());
    data.push_back(Geometry("POINT(2 2)").to_wkb_string());

    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::GEOMETRY, true);
    // All rows are valid – need explicit valid bitmap when nullable=true
    uint8_t* valid_bitmap_all =
        new uint8_t[1]{0x07};  // 0b00000111 (3 rows valid)
    field_data->FillFieldData(data.data(), valid_bitmap_all, data.size());

    storage::InsertEventData event_data;
    event_data.payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    ASSERT_TRUE(reader_builder.Open(buffer).ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());
    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

    FieldMeta field_meta(
        FieldName("geo"), milvus::FieldId(1), DataType::GEOMETRY, true);
    auto chunk = create_chunk(field_meta, rb_reader);

    auto [views, valid] =
        std::dynamic_pointer_cast<GeometryChunk>(chunk)->StringViews(
            std::nullopt);
    ASSERT_EQ(views.size(), data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(views[i], data[i]);
        EXPECT_TRUE(valid[i]);
    }

    delete[] valid_bitmap_all;
}

TEST(chunk, test_geometry_field_mmap_with_nulls) {
    // Prepare geometry data with one NULL row (middle)
    FixedVector<std::string> data;
    data.reserve(3);
    data.push_back(Geometry("POINT(0 0)").to_wkb_string());
    data.push_back(
        Geometry("POINT(1 1)").to_wkb_string());  // will be marked NULL
    data.push_back(Geometry("POINT(2 2)").to_wkb_string());

    // Validity bitmap: 0b00000101 -> rows 0 and 2 valid, row 1 invalid
    uint8_t* valid_bitmap = new uint8_t[1]{0x05};

    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::GEOMETRY, true);
    field_data->FillFieldData(data.data(), valid_bitmap, data.size());

    storage::InsertEventData event_data;
    event_data.payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    ASSERT_TRUE(reader_builder.Open(buffer).ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());
    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

    // Prepare mmap file
    TempDir temp;
    std::string temp_dir = temp.dir();
    auto file = File::Open(temp_dir + "/geo_mmap", O_CREAT | O_RDWR);
    int file_offset = 0;

    FieldMeta field_meta(
        FieldName("geo"), milvus::FieldId(1), DataType::GEOMETRY, true);
    auto chunk = create_chunk(field_meta, rb_reader);

    auto [views, valid] =
        std::dynamic_pointer_cast<GeometryChunk>(chunk)->StringViews(
            std::nullopt);
    ASSERT_EQ(views.size(), data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        if (valid[i]) {
            EXPECT_EQ(views[i], data[i]);
        } else {
            EXPECT_FALSE(valid[i]);
        }
    }
    file.Close();
    delete[] valid_bitmap;
}

TEST(array, test_geometry_array_output_data) {
    // Prepare two simple geometries (WKB strings)
    std::string wkb1 = Geometry("POINT(10 10)").to_wkb_string();
    std::string wkb2 = Geometry("POINT(20 20)").to_wkb_string();

    // Build raw buffer and offsets for two variable-length geometry elements
    // Need to support kGeometry in construct Array(const ScalarArray& field_data)
    uint32_t offsets_raw[2] = {0, static_cast<uint32_t>(wkb1.size())};
    size_t total_size = wkb1.size() + wkb2.size();
    std::vector<char> buf(total_size);
    std::copy(wkb1.begin(), wkb1.end(), buf.begin());
    std::copy(wkb2.begin(), wkb2.end(), buf.begin() + wkb1.size());

    // Construct Array with element_type = GEOMETRY
    Array geo_array(
        buf.data(), /*len=*/2, total_size, DataType::GEOMETRY, offsets_raw);

    auto serialized = geo_array.output_data();

    ASSERT_EQ(serialized.geometry_data().data_size(), 2);
    EXPECT_EQ(serialized.geometry_data().data(0), wkb1);
    EXPECT_EQ(serialized.geometry_data().data(1), wkb2);
}