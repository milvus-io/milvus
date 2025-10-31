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

#include <memory>
#include <vector>
#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/type_fwd.h>
#include <parquet/statistics.h>

#include "storage/Util.h"
#include "common/ChunkWriter.h"
#include "index/skipindex_stats/SkipIndexStats.h"

using namespace milvus;
using namespace milvus::index;

class SkipIndexStatsBuilderTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        auto config = milvus::Config();
        config["enable_bloom_filter"] = true;
        builder_ = std::make_unique<SkipIndexStatsBuilder>(config);
    }

    std::unique_ptr<SkipIndexStatsBuilder> builder_;
};

TEST_F(SkipIndexStatsBuilderTest, BuildFromArrowBatches) {
    // BOOL
    {
        auto schema = arrow::schema({arrow::field("col", arrow::boolean())});
        arrow::BooleanBuilder builder;
        ASSERT_TRUE(builder.Append(false).ok());
        ASSERT_TRUE(builder.Append(false).ok());
        ASSERT_TRUE(builder.Append(false).ok());
        ASSERT_TRUE(builder.Append(false).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 4, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::BOOL);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::BOOLEAN);

        std::vector<Metrics> query_true = {true, false};
        std::vector<Metrics> query_false = {false, true};
        EXPECT_TRUE(metrics->CanSkipIn(query_true));
        EXPECT_FALSE(metrics->CanSkipIn(query_false));
    }

    // INT8
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int8())});
        arrow::Int8Builder builder;
        std::vector<int8_t> values = {-10, 0, 5, 10, 20};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT8);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int8_t(5)));
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int8_t(100)));
    }

    // INT16
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int16())});
        arrow::Int16Builder builder;
        for (int16_t i = 0; i < 100; ++i) {
            ASSERT_TRUE(builder.Append(i * 10).ok());
        }

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 100, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT16);
        ASSERT_NE(metrics, nullptr);
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::GreaterThan, int16_t(1000)));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int16_t(50)));
    }

    // INT32
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int32())});
        arrow::Int32Builder builder;
        for (int32_t i = 0; i < 1000; ++i) {
            ASSERT_TRUE(builder.Append(i).ok());
        }

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 1000, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT32);
        ASSERT_NE(metrics, nullptr);

        std::vector<Metrics> in_values = {int32_t(2000), int32_t(3000)};
        EXPECT_TRUE(metrics->CanSkipIn(in_values));
        std::vector<Metrics> in_values2 = {int32_t(10), int32_t(20)};
        EXPECT_FALSE(metrics->CanSkipIn(in_values2));
    }

    // INT64
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int64())});
        arrow::Int64Builder builder;
        std::vector<int64_t> values = {INT64_MIN / 2, 0, INT64_MAX / 2};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT64);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int64_t(114514)));
    }

    // FLOAT
    {
        auto schema = arrow::schema({arrow::field("col", arrow::float32())});
        arrow::FloatBuilder builder;
        std::vector<float> values = {-1.5f, 0.0f, 1.5f, 3.14f};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::FLOAT);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::FLOAT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::LessThan, -2.0f));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::LessThan, 0.0f));
    }

    // DOUBLE
    {
        auto schema = arrow::schema({arrow::field("col", arrow::float64())});
        arrow::DoubleBuilder builder;
        std::vector<double> values = {
            -3.141592653589793, 0.0, 2.718281828459045};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::DOUBLE);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::FLOAT);
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, 0.0));
    }

    // STRING
    {
        auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
        arrow::StringBuilder builder;
        std::vector<std::string> values = {"apple", "banana", "cherry", "date"};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::STRING);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::STRING);
        EXPECT_FALSE(
            metrics->CanSkipUnaryRange(OpType::Equal, std::string("banana")));
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::Equal, std::string("zebra")));
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::LessThan, std::string("aaa")));
    }

    // STRING with N-gram
    {
        auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
        arrow::StringBuilder builder;
        std::vector<std::string> values = {"milvus_vector", "database_system"};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::STRING);
        ASSERT_NE(metrics, nullptr);

        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::InnerMatch,
                                               std::string("zzzzz")));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::InnerMatch,
                                                std::string("vector")));
    }

    // Contains null values
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int32())});
        arrow::Int32Builder builder;
        ASSERT_TRUE(builder.Append(10).ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.Append(20).ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.Append(30).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 5, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT32);
        ASSERT_NE(metrics, nullptr);

        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(10)));
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(5)));
    }

    // Multiple Batches
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int32())});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

        arrow::Int32Builder builder1;
        for (int i = 0; i < 100; ++i) {
            ASSERT_TRUE(builder1.Append(i).ok());
        }
        std::shared_ptr<arrow::Array> array1;
        ASSERT_TRUE(builder1.Finish(&array1).ok());
        batches.push_back(arrow::RecordBatch::Make(schema, 100, {array1}));

        arrow::Int32Builder builder2;
        for (int i = 100; i < 200; ++i) {
            ASSERT_TRUE(builder2.Append(i).ok());
        }
        std::shared_ptr<arrow::Array> array2;
        ASSERT_TRUE(builder2.Finish(&array2).ok());
        batches.push_back(arrow::RecordBatch::Make(schema, 100, {array2}));

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT32);
        ASSERT_NE(metrics, nullptr);

        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(50)));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(150)));
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(300)));
    }

    // Empty Batches
    {
        std::vector<std::shared_ptr<arrow::RecordBatch>> empty_batches;
        auto metrics = builder_->Build(empty_batches, 0, arrow::Type::INT32);

        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::NONE);
    }
}

TEST_F(SkipIndexStatsBuilderTest, BuildFromChunk) {
    // BOOL
    {
        FixedVector<bool> data = {true, true, true, true};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::BOOL, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::BOOL,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::BOOL, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::BOOLEAN);

        std::vector<Metrics> query_true = {true, false};
        std::vector<Metrics> query_false = {false, true};
        EXPECT_FALSE(metrics->CanSkipIn(query_true));
        EXPECT_TRUE(metrics->CanSkipIn(query_false));
    }

    // INT8
    {
        FixedVector<int8_t> data = {-50, -25, 0, 25, 50};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT8, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT8,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT8, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::LessThan, int8_t(-51)));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int8_t(0)));
    }

    // INT16
    {
        FixedVector<int16_t> data;
        for (int16_t i = 0; i < 100; ++i) {
            data.push_back(i * 10);
        }
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT16, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT16,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT16, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::GreaterThan, int16_t(1000)));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int16_t(50)));
    }

    // INT32
    {
        FixedVector<int32_t> data;
        for (int32_t i = 0; i < 1000; ++i) {
            data.push_back(i);
        }
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT32, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT32,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT32, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(500)));
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(2000)));
    }

    // INT64
    {
        FixedVector<int64_t> data = {-1145141919810, 0, 1145141919810};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT64, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT64,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT64, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int64_t(114514)));
    }

    // FLOAT
    {
        FixedVector<float> data = {-3.14f, -1.0f, 0.0f, 1.0f, 2.718f};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::FLOAT, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::FLOAT,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::FLOAT, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::FLOAT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::GreaterThan, 3.0f));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, 0.0f));
    }

    // DOUBLE
    {
        FixedVector<double> data = {-3.141592653589793, 0.0, 2.718281828459045};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::DOUBLE, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::DOUBLE,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::DOUBLE, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::FLOAT);
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, 0.0));
    }

    // VARCHAR
    {
        FixedVector<std::string> data = {
            "apple", "banana", "cherry", "date", "elderberry"};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::VARCHAR, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::STRING,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::VARCHAR, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::STRING);
        EXPECT_FALSE(
            metrics->CanSkipUnaryRange(OpType::Equal, std::string("banana")));
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::Equal, std::string("xyz")));
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::LessThan, std::string("aaa")));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::LessThan,
                                                std::string("cherry")));
    }
}

TEST_F(SkipIndexStatsBuilderTest, BuildFromArrowBatch_InQuery) {
    // Test INT64
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int64())});
        arrow::Int64Builder builder;
        std::vector<int64_t> values = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT64);
        ASSERT_NE(metrics, nullptr);

        std::vector<Metrics> in_values1 = {
            int64_t(50), int64_t(150), int64_t(200)};
        ASSERT_FALSE(metrics->CanSkipIn(in_values1));

        std::vector<Metrics> in_values2 = {int64_t(5), int64_t(10)};
        ASSERT_FALSE(metrics->CanSkipIn(in_values2));

        std::vector<Metrics> in_values3 = {int64_t(100), int64_t(110)};
        ASSERT_FALSE(metrics->CanSkipIn(in_values3));

        std::vector<Metrics> in_values4 = {int64_t(2), int64_t(3), int64_t(4)};
        ASSERT_TRUE(metrics->CanSkipIn(in_values4));

        std::vector<Metrics> in_values5 = {
            int64_t(110), int64_t(120), int64_t(130)};
        ASSERT_TRUE(metrics->CanSkipIn(in_values5));

        std::vector<Metrics> in_values6 = {
            int64_t(15), int64_t(23), int64_t(55)};
        ASSERT_TRUE(metrics->CanSkipIn(in_values6));
    }

    // Test STRING
    {
        auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
        arrow::StringBuilder builder;
        std::vector<std::string> values = {"apple",
                                           "banana",
                                           "cherry",
                                           "date",
                                           "elderberry",
                                           "fig",
                                           "grape",
                                           "honeydew",
                                           "kiwi",
                                           "lemon"};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::STRING);
        ASSERT_NE(metrics, nullptr);

        std::vector<Metrics> in_values1 = {std::string("banana"),
                                           std::string("zebra")};
        ASSERT_FALSE(metrics->CanSkipIn(in_values1));

        std::vector<Metrics> in_values2 = {std::string("aardvark"),
                                           std::string("apple")};
        ASSERT_FALSE(metrics->CanSkipIn(in_values2));

        std::vector<Metrics> in_values3 = {std::string("lemon"),
                                           std::string("mango")};
        ASSERT_FALSE(metrics->CanSkipIn(in_values3));

        std::vector<Metrics> in_values4 = {std::string("aaa"),
                                           std::string("aardvark")};
        ASSERT_TRUE(metrics->CanSkipIn(in_values4));

        std::vector<Metrics> in_values5 = {
            std::string("mango"), std::string("orange"), std::string("zebra")};
        ASSERT_TRUE(metrics->CanSkipIn(in_values5));

        std::vector<Metrics> in_values6 = {};
        ASSERT_FALSE(metrics->CanSkipIn(in_values6));
    }
}

TEST_F(SkipIndexStatsBuilderTest, BuildFromArrowBatch_InQuery_Nullable) {
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int64())});
        arrow::Int64Builder builder;

        // Data: [10, 20, NULL, NULL, 50], valid_data: 0b00010011 = 0x13
        ASSERT_TRUE(builder.Append(10).ok());
        ASSERT_TRUE(builder.Append(20).ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.Append(50).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 5, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT64);
        ASSERT_NE(metrics, nullptr);

        // Actual valid values: [10, 20, 50]
        std::vector<Metrics> in_values1 = {
            int64_t(20), int64_t(60), int64_t(70)};
        ASSERT_FALSE(metrics->CanSkipIn(in_values1));

        std::vector<Metrics> in_values2 = {int64_t(1), int64_t(2), int64_t(50)};
        ASSERT_FALSE(metrics->CanSkipIn(in_values2));

        std::vector<Metrics> in_values3 = {int64_t(1), int64_t(2), int64_t(3)};
        ASSERT_TRUE(metrics->CanSkipIn(in_values3));

        std::vector<Metrics> in_values4 = {
            int64_t(60), int64_t(70), int64_t(80)};
        ASSERT_TRUE(metrics->CanSkipIn(in_values4));

        // Should skip: NULL values (30, 40) are not in valid set
        std::vector<Metrics> in_values5 = {int64_t(30)};
        ASSERT_TRUE(metrics->CanSkipIn(in_values5));

        std::vector<Metrics> in_values6 = {};
        ASSERT_FALSE(metrics->CanSkipIn(in_values6));
    }

    // Test nullable STRING
    {
        auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
        arrow::StringBuilder builder;

        // Data: ["a", "b", NULL, NULL, "e"], valid_data: 0b00010011 = 0x13
        ASSERT_TRUE(builder.Append("a").ok());
        ASSERT_TRUE(builder.Append("b").ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.Append("e").ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 5, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::STRING);
        ASSERT_NE(metrics, nullptr);

        // Actual valid values: ["a", "b", "e"]
        std::vector<Metrics> in_values1 = {
            std::string("b"), std::string("x"), std::string("y")};
        ASSERT_FALSE(metrics->CanSkipIn(in_values1));

        std::vector<Metrics> in_values2 = {
            std::string("e"), std::string("x"), std::string("y")};
        ASSERT_FALSE(metrics->CanSkipIn(in_values2));

        std::vector<Metrics> in_values3 = {
            std::string("0"), std::string("1"), std::string("2")};
        ASSERT_TRUE(metrics->CanSkipIn(in_values3));

        std::vector<Metrics> in_values4 = {
            std::string("x"), std::string("y"), std::string("z")};
        ASSERT_TRUE(metrics->CanSkipIn(in_values4));

        // Should skip: NULL values ("c", "d") are not in valid set
        std::vector<Metrics> in_values5 = {std::string("c")};
        ASSERT_TRUE(metrics->CanSkipIn(in_values5));

        std::vector<Metrics> in_values6 = {};
        ASSERT_FALSE(metrics->CanSkipIn(in_values6));
    }
}

TEST_F(SkipIndexStatsBuilderTest, BuildFromChunk_InQuery) {
    // Test INT64
    {
        FixedVector<int64_t> data = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT64, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT64,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT64, chunk.get());
        ASSERT_NE(metrics, nullptr);

        std::vector<Metrics> in_values1 = {
            int64_t(50), int64_t(150), int64_t(200)};
        ASSERT_FALSE(metrics->CanSkipIn(in_values1));

        std::vector<Metrics> in_values2 = {int64_t(2), int64_t(3), int64_t(4)};
        ASSERT_TRUE(metrics->CanSkipIn(in_values2));
    }

    // Test STRING
    {
        FixedVector<std::string> data = {
            "apple", "banana", "cherry", "date", "elderberry"};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::VARCHAR, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::STRING,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::VARCHAR, chunk.get());
        ASSERT_NE(metrics, nullptr);

        std::vector<Metrics> in_values1 = {std::string("banana"),
                                           std::string("zebra")};
        ASSERT_FALSE(metrics->CanSkipIn(in_values1));

        std::vector<Metrics> in_values2 = {std::string("aaa"),
                                           std::string("aardvark")};
        ASSERT_TRUE(metrics->CanSkipIn(in_values2));

        std::vector<Metrics> in_values3 = {std::string("xyz"),
                                           std::string("zzz")};
        ASSERT_TRUE(metrics->CanSkipIn(in_values3));
    }
}

TEST_F(SkipIndexStatsBuilderTest, InQuery_DisableBloomFilter) {
    auto builder_no_bf = std::make_unique<SkipIndexStatsBuilder>();

    auto schema = arrow::schema({arrow::field("col", arrow::int64())});
    arrow::Int64Builder builder;

    std::vector<int64_t> values;
    for (int64_t i = 0; i < 100; ++i) {
        values.push_back(i * 10);
    }
    ASSERT_TRUE(builder.AppendValues(values).ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

    auto metrics = builder_no_bf->Build(batches, 0, arrow::Type::INT64);
    ASSERT_NE(metrics, nullptr);

    std::vector<Metrics> in_values1 = {int64_t(1), int64_t(2), int64_t(3)};
    ASSERT_FALSE(metrics->CanSkipIn(in_values1));

    std::vector<Metrics> in_values2 = {int64_t(15), int64_t(25)};
    ASSERT_FALSE(metrics->CanSkipIn(in_values2));

    // Values outside range should be skipped
    std::vector<Metrics> in_values3 = {int64_t(-100), int64_t(-50)};
    ASSERT_TRUE(metrics->CanSkipIn(in_values3));

    std::vector<Metrics> in_values4 = {int64_t(1000), int64_t(2000)};
    ASSERT_TRUE(metrics->CanSkipIn(in_values4));
}

TEST_F(SkipIndexStatsBuilderTest, BuildFromArrowBatch_StringNgramMatch) {
    // Multi-language test data
    auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
    arrow::StringBuilder builder;
    std::vector<std::string> strings = {
        "张华考上了北京大学；李萍进了中等技术学校；我在百货公司当售货员：我们都"
        "有光明的前途",
        "張華は北京大学に入学し、李平は中等技術学校に入学し、私はデパートの販売"
        "員として働き、私たち全員に明るい未来が約束されていました。",
        "Zhang Hua a été admis à l'Université de Pékin ; Li Ping est entré "
        "dans une école secondaire technique ; j'ai travaillé comme vendeur "
        "dans un grand magasin : nous avions tous un brillant avenir.",
        "Zhang Hua was admitted to Peking University; Li Ping entered a "
        "secondary technical school; I worked as a salesperson in a department "
        "store: we all had bright futures.",
        "Zhang Hua wurde an der Peking-Universität aufgenommen, Li Ping "
        "besuchte eine technische Sekundarschule, ich arbeitete als Verkäufer "
        "in einem Kaufhaus: Wir alle hatten eine glänzende Zukunft.",
        "😀😁😂",                       // Emoji (4-byte UTF-8)
        "Ñoño",                      // Combining characters
        "👨‍👩‍👧‍👦"  // Family emoji (multiple codepoints)
    };
    ASSERT_TRUE(builder.AppendValues(strings).ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(schema, strings.size(), {array});
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

    auto metrics = builder_->Build(batches, 0, arrow::Type::STRING);
    ASSERT_NE(metrics, nullptr);
    ASSERT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::STRING);

    // PrefixMatch tests - Chinese
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::PrefixMatch, std::string("李萍进了中等技术学校")));
    ASSERT_TRUE(metrics->CanSkipUnaryRange(OpType::PrefixMatch,
                                           std::string("烽火戏诸侯")));
    ASSERT_TRUE(metrics->CanSkipUnaryRange(OpType::PrefixMatch,
                                           std::string("测试中文分词效果")));

    // PrefixMatch tests - Japanese
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::PrefixMatch, std::string("私はデパートの販売員として働き")));
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::PrefixMatch, std::string("未来が約束されていました")));
    ASSERT_FALSE(metrics->CanSkipUnaryRange(OpType::PrefixMatch,
                                            std::string("北京大学")));
    ASSERT_TRUE(metrics->CanSkipUnaryRange(
        OpType::PrefixMatch,
        std::string("日本語の単語分割の効果をテストする")));

    // PrefixMatch tests - English
    ASSERT_TRUE(metrics->CanSkipUnaryRange(
        OpType::PrefixMatch, std::string("Ingenious Film Partners")));

    // InnerMatch tests
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::InnerMatch,
        std::string("Ping besuchte eine technische Sekundarschule")));
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::InnerMatch, std::string("張華は北京大学に入学し")));
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::InnerMatch,
        std::string(" der Peking-Universität aufgenommen")));
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::InnerMatch,
        std::string("Li Ping est entré dans une école secondaire technique ")));
    ASSERT_TRUE(metrics->CanSkipUnaryRange(OpType::InnerMatch,
                                           std::string("swimming, football")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("dog")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("xyz")));

    // PostfixMatch tests
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::PostfixMatch,
        std::string("Li Ping entered a secondary technical school;")));
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::PostfixMatch,
        std::string("nous avions tous un brillant avenir.")));
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::PostfixMatch, std::string("j'ai ")));
    ASSERT_FALSE(metrics->CanSkipUnaryRange(OpType::PostfixMatch,
                                            std::string("j'ai travaillé")));
    ASSERT_FALSE(metrics->CanSkipUnaryRange(
        OpType::PostfixMatch, std::string("Li Ping entered a secondary")));
    ASSERT_TRUE(metrics->CanSkipUnaryRange(OpType::PostfixMatch,
                                           std::string("後藤一里")));
    ASSERT_TRUE(metrics->CanSkipUnaryRange(OpType::PostfixMatch,
                                           std::string("ルフィ")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::PostfixMatch, std::string("zzz")));
    ASSERT_TRUE(metrics->CanSkipUnaryRange(OpType::PostfixMatch,
                                           std::string("Guillotine")));

    // Should handle emoji correctly
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("😀")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("xyz")));
}

TEST_F(SkipIndexStatsBuilderTest,
       BuildFromArrowBatch_StringNgramMatchNullable) {
    auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
    arrow::StringBuilder builder;

    // Test data: ["apple", "application", NULL, NULL, "candy"]
    // Valid data: 0b00010011 = 0x13 (bits 0, 1, 4 are valid)
    std::vector<std::string> strings = {
        "apple", "application", "abandon", "band", "candy"};
    ASSERT_TRUE(builder.Append("apple").ok());
    ASSERT_TRUE(builder.Append("application").ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append("candy").ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto batch = arrow::RecordBatch::Make(schema, 5, {array});
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

    auto metrics = builder_->Build(batches, 0, arrow::Type::STRING);
    ASSERT_NE(metrics, nullptr);
    ASSERT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::STRING);

    // PrefixMatch tests
    // Actual valid values: ["apple", "application", "candy"]
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::PrefixMatch, std::string("ap")));
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::PrefixMatch, std::string("app")));
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::PrefixMatch, std::string("ba")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::PrefixMatch, std::string("aba")));
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::PrefixMatch, std::string("can")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::PrefixMatch, std::string("xyz")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::PrefixMatch, std::string("dog")));

    // InnerMatch tests
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("ap")));
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("pp")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("ana")));
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("and")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("dog")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("xyz")));

    // PostfixMatch tests
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::PostfixMatch, std::string("le")));
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::PostfixMatch, std::string("on")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::PostfixMatch, std::string("ana")));
    ASSERT_FALSE(
        metrics->CanSkipUnaryRange(OpType::PostfixMatch, std::string("dy")));
    ASSERT_TRUE(metrics->CanSkipUnaryRange(OpType::PostfixMatch,
                                           std::string("milvus")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::PostfixMatch, std::string("zzz")));
    ASSERT_TRUE(
        metrics->CanSkipUnaryRange(OpType::PostfixMatch, std::string("xyz")));
}
