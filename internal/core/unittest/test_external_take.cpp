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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/VirtualPK.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "milvus-storage/reader.h"
#include "query/PlanImpl.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentSealed.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

constexpr int64_t kTestRows = 5;
constexpr int64_t kVecDim = 4;

// Mock Reader that returns a pre-built Arrow Table from take().
class MockTakeReader : public milvus_storage::api::Reader {
 public:
    std::shared_ptr<arrow::Table> table_;

    explicit MockTakeReader(std::shared_ptr<arrow::Table> table)
        : table_(std::move(table)) {
    }

    std::shared_ptr<milvus_storage::api::ColumnGroups>
    get_column_groups() const override {
        return nullptr;
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>
    get_record_batch_reader(const std::string&) const override {
        return arrow::Status::NotImplemented("mock");
    }

    arrow::Result<std::unique_ptr<milvus_storage::api::ChunkReader>>
    get_chunk_reader(
        int64_t,
        const std::shared_ptr<std::vector<std::string>>&) const override {
        return arrow::Status::NotImplemented("mock");
    }

    arrow::Result<std::shared_ptr<arrow::Table>>
    take(const std::vector<int64_t>& row_indices,
         size_t,
         const std::shared_ptr<std::vector<std::string>>&
             needed_columns) override {
        if (!table_) {
            return arrow::Status::Invalid("no table");
        }
        // Select columns matching needed_columns
        if (needed_columns && !needed_columns->empty()) {
            std::vector<std::shared_ptr<arrow::Field>> fields;
            std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
            for (const auto& name : *needed_columns) {
                auto col = table_->GetColumnByName(name);
                if (col) {
                    fields.push_back(table_->schema()->GetFieldByName(name));
                    columns.push_back(col);
                }
            }
            auto schema = arrow::schema(fields);
            auto filtered = arrow::Table::Make(schema, columns);
            // Select rows by indices
            return SelectRows(filtered, row_indices);
        }
        return SelectRows(table_, row_indices);
    }

    void
    set_keyretriever(
        const std::function<std::string(const std::string&)>&) override {
    }

 private:
    // Select rows from table at given indices
    static arrow::Result<std::shared_ptr<arrow::Table>>
    SelectRows(const std::shared_ptr<arrow::Table>& table,
               const std::vector<int64_t>& indices) {
        if (indices.empty()) {
            return table->Slice(0, 0);
        }
        // Build a take indices array
        arrow::Int64Builder idx_builder;
        ARROW_RETURN_NOT_OK(idx_builder.AppendValues(indices));
        std::shared_ptr<arrow::Array> idx_arr;
        ARROW_RETURN_NOT_OK(idx_builder.Finish(&idx_arr));

        std::vector<std::shared_ptr<arrow::ChunkedArray>> result_columns;
        for (int i = 0; i < table->num_columns(); i++) {
            auto chunked = table->column(i);
            // Combine chunks for simplicity
            auto combined = arrow::Concatenate(chunked->chunks());
            if (!combined.ok()) {
                return combined.status();
            }
            auto arr = *combined;
            // Manual take: select values at indices
            auto type = arr->type();
            if (type->Equals(arrow::boolean())) {
                auto typed =
                    std::static_pointer_cast<arrow::BooleanArray>(arr);
                arrow::BooleanBuilder b;
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(b.Append(typed->Value(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            } else if (type->Equals(arrow::int8())) {
                auto typed =
                    std::static_pointer_cast<arrow::Int8Array>(arr);
                arrow::Int8Builder b;
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(b.Append(typed->Value(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            } else if (type->Equals(arrow::int16())) {
                auto typed =
                    std::static_pointer_cast<arrow::Int16Array>(arr);
                arrow::Int16Builder b;
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(b.Append(typed->Value(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            } else if (type->Equals(arrow::int32())) {
                auto typed =
                    std::static_pointer_cast<arrow::Int32Array>(arr);
                arrow::Int32Builder b;
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(b.Append(typed->Value(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            } else if (type->Equals(arrow::int64())) {
                auto typed =
                    std::static_pointer_cast<arrow::Int64Array>(arr);
                arrow::Int64Builder b;
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(b.Append(typed->Value(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            } else if (type->Equals(arrow::float32())) {
                auto typed =
                    std::static_pointer_cast<arrow::FloatArray>(arr);
                arrow::FloatBuilder b;
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(b.Append(typed->Value(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            } else if (type->Equals(arrow::float64())) {
                auto typed =
                    std::static_pointer_cast<arrow::DoubleArray>(arr);
                arrow::DoubleBuilder b;
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(b.Append(typed->Value(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            } else if (type->Equals(arrow::utf8())) {
                auto typed =
                    std::static_pointer_cast<arrow::StringArray>(arr);
                arrow::StringBuilder b;
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(
                        b.Append(typed->GetString(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            } else if (type->id() == arrow::Type::FIXED_SIZE_BINARY) {
                auto typed =
                    std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arr);
                auto fsb_type =
                    std::static_pointer_cast<arrow::FixedSizeBinaryType>(type);
                arrow::FixedSizeBinaryBuilder b(fsb_type);
                for (auto idx : indices)
                    ARROW_RETURN_NOT_OK(b.Append(typed->Value(idx)));
                std::shared_ptr<arrow::Array> out;
                ARROW_RETURN_NOT_OK(b.Finish(&out));
                result_columns.push_back(
                    std::make_shared<arrow::ChunkedArray>(out));
            }
        }
        auto schema = table->schema();
        return arrow::Table::Make(schema, result_columns);
    }
};

// Build a test Arrow Table with kTestRows rows and all supported types.
std::shared_ptr<arrow::Table>
BuildTestArrowTable() {
    // BOOL
    arrow::BooleanBuilder bool_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(bool_b.Append(i % 2 == 0).ok());
    auto bool_arr = bool_b.Finish().ValueOrDie();

    // INT8
    arrow::Int8Builder int8_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int8_b.Append(static_cast<int8_t>(i * 10)).ok());
    auto int8_arr = int8_b.Finish().ValueOrDie();

    // INT16
    arrow::Int16Builder int16_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int16_b.Append(static_cast<int16_t>(i * 100)).ok());
    auto int16_arr = int16_b.Finish().ValueOrDie();

    // INT32
    arrow::Int32Builder int32_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int32_b.Append(i * 1000).ok());
    auto int32_arr = int32_b.Finish().ValueOrDie();

    // INT64
    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    // FLOAT
    arrow::FloatBuilder float_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(float_b.Append(i * 1.1f).ok());
    auto float_arr = float_b.Finish().ValueOrDie();

    // DOUBLE
    arrow::DoubleBuilder double_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(double_b.Append(i * 2.2).ok());
    auto double_arr = double_b.Finish().ValueOrDie();

    // VARCHAR
    arrow::StringBuilder str_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(str_b.Append("row_" + std::to_string(i)).ok());
    auto str_arr = str_b.Finish().ValueOrDie();

    // VECTOR_FLOAT (dim=4, 16 bytes per vector)
    auto fsb_type = arrow::fixed_size_binary(kVecDim * sizeof(float));
    arrow::FixedSizeBinaryBuilder vec_b(fsb_type);
    for (int i = 0; i < kTestRows; i++) {
        float v[kVecDim];
        for (int d = 0; d < kVecDim; d++)
            v[d] = static_cast<float>(i * kVecDim + d);
        EXPECT_TRUE(
            vec_b.Append(reinterpret_cast<const uint8_t*>(v)).ok());
    }
    auto vec_arr = vec_b.Finish().ValueOrDie();

    auto schema = arrow::schema({
        arrow::field("bool_col", arrow::boolean()),
        arrow::field("int8_col", arrow::int8()),
        arrow::field("int16_col", arrow::int16()),
        arrow::field("int32_col", arrow::int32()),
        arrow::field("int64_col", arrow::int64()),
        arrow::field("float_col", arrow::float32()),
        arrow::field("double_col", arrow::float64()),
        arrow::field("varchar_col", arrow::utf8()),
        arrow::field("vec_col", fsb_type),
    });

    return arrow::Table::Make(
        schema,
        {bool_arr, int8_arr, int16_arr, int32_arr, int64_arr, float_arr,
         double_arr, str_arr, vec_arr});
}

// Build an external schema with all supported types.
// Returns {schema, field_ids} where field_ids are in order:
// bool, int8, int16, int32, int64, float, double, varchar, vec
struct ExternalSchemaInfo {
    SchemaPtr schema;
    FieldId bool_id, int8_id, int16_id, int32_id, int64_id;
    FieldId float_id, double_id, varchar_id, vec_id;
};

ExternalSchemaInfo
BuildExternalSchema() {
    auto schema = std::make_shared<Schema>();

    // System fields (required by segment)
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    // User fields with external_field_mapping
    ExternalSchemaInfo info;
    info.bool_id = FieldId(100);
    info.int8_id = FieldId(101);
    info.int16_id = FieldId(102);
    info.int32_id = FieldId(103);
    info.int64_id = FieldId(104);
    info.float_id = FieldId(105);
    info.double_id = FieldId(106);
    info.varchar_id = FieldId(107);
    info.vec_id = FieldId(108);

    // Scalar fields: FieldMeta(name, id, type, nullable, default_value, external_field)
    schema->AddField(FieldMeta(FieldName("bool_col"),
                               info.bool_id,
                               DataType::BOOL,
                               false,
                               std::nullopt,
                               "bool_col"));
    schema->AddField(FieldMeta(FieldName("int8_col"),
                               info.int8_id,
                               DataType::INT8,
                               false,
                               std::nullopt,
                               "int8_col"));
    schema->AddField(FieldMeta(FieldName("int16_col"),
                               info.int16_id,
                               DataType::INT16,
                               false,
                               std::nullopt,
                               "int16_col"));
    schema->AddField(FieldMeta(FieldName("int32_col"),
                               info.int32_id,
                               DataType::INT32,
                               false,
                               std::nullopt,
                               "int32_col"));
    schema->AddField(FieldMeta(FieldName("int64_col"),
                               info.int64_id,
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "int64_col"));
    schema->AddField(FieldMeta(FieldName("float_col"),
                               info.float_id,
                               DataType::FLOAT,
                               false,
                               std::nullopt,
                               "float_col"));
    schema->AddField(FieldMeta(FieldName("double_col"),
                               info.double_id,
                               DataType::DOUBLE,
                               false,
                               std::nullopt,
                               "double_col"));
    // String field: FieldMeta(name, id, type, max_length, nullable, default_value, external_field)
    schema->AddField(FieldMeta(FieldName("varchar_col"),
                               info.varchar_id,
                               DataType::VARCHAR,
                               65535,
                               false,
                               std::nullopt,
                               "varchar_col"));
    // Vector field: FieldMeta(name, id, type, dim, metric_type, nullable, default_value, external_field)
    schema->AddField(FieldMeta(FieldName("vec_col"),
                               info.vec_id,
                               DataType::VECTOR_FLOAT,
                               kVecDim,
                               knowhere::metric::L2,
                               false,
                               std::nullopt,
                               "vec_col"));

    schema->set_primary_field_id(info.int64_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");

    info.schema = schema;
    return info;
}

// Helper: create a ChunkedSegmentSealedImpl with external schema
// Note: reader_ must be injected via SetReaderForTesting (MILVUS_UNIT_TEST)
ChunkedSegmentSealedImpl*
CreateExternalSegment(
    SegmentSealedUPtr& holder,
    const SchemaPtr& schema,
    int64_t segment_id = 1) {
    holder = CreateSealedSegment(schema, nullptr, segment_id);
    return dynamic_cast<ChunkedSegmentSealedImpl*>(holder.get());
}

// Mock Reader that always returns an error from take().
class ErrorMockTakeReader : public milvus_storage::api::Reader {
 public:
    std::shared_ptr<milvus_storage::api::ColumnGroups>
    get_column_groups() const override {
        return nullptr;
    }
    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>>
    get_record_batch_reader(const std::string&) const override {
        return arrow::Status::NotImplemented("mock");
    }
    arrow::Result<std::unique_ptr<milvus_storage::api::ChunkReader>>
    get_chunk_reader(
        int64_t,
        const std::shared_ptr<std::vector<std::string>>&) const override {
        return arrow::Status::NotImplemented("mock");
    }
    arrow::Result<std::shared_ptr<arrow::Table>>
    take(const std::vector<int64_t>&,
         size_t,
         const std::shared_ptr<std::vector<std::string>>&) override {
        return arrow::Status::Invalid("simulated take failure");
    }
    void
    set_keyretriever(
        const std::function<std::string(const std::string&)>&) override {
    }
};

// Schema with virtual PK (non-external) + external fields.
struct ExternalSchemaWithVirtualPK {
    SchemaPtr schema;
    FieldId pk_id;       // virtual PK, non-external
    FieldId int32_id;    // external
    FieldId varchar_id;  // external
    FieldId vec_id;      // external
};

ExternalSchemaWithVirtualPK
BuildExternalSchemaWithVirtualPK() {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    ExternalSchemaWithVirtualPK info;
    info.pk_id = FieldId(100);
    info.int32_id = FieldId(101);
    info.varchar_id = FieldId(102);
    info.vec_id = FieldId(103);

    // Virtual PK: INT64 with NO external_field_mapping
    schema->AddField(FieldMeta(
        FieldName("virtual_pk"), info.pk_id, DataType::INT64, false, std::nullopt));
    // External fields
    schema->AddField(FieldMeta(FieldName("int32_col"),
                               info.int32_id,
                               DataType::INT32,
                               false,
                               std::nullopt,
                               "int32_col"));
    schema->AddField(FieldMeta(FieldName("varchar_col"),
                               info.varchar_id,
                               DataType::VARCHAR,
                               65535,
                               false,
                               std::nullopt,
                               "varchar_col"));
    schema->AddField(FieldMeta(FieldName("vec_col"),
                               info.vec_id,
                               DataType::VECTOR_FLOAT,
                               kVecDim,
                               knowhere::metric::L2,
                               false,
                               std::nullopt,
                               "vec_col"));

    schema->set_primary_field_id(info.pk_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");
    info.schema = schema;
    return info;
}

// Build table for virtual PK schema (only external field columns).
std::shared_ptr<arrow::Table>
BuildVirtualPKTestTable() {
    arrow::Int32Builder int32_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int32_b.Append(i * 1000).ok());
    auto int32_arr = int32_b.Finish().ValueOrDie();

    arrow::StringBuilder str_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(str_b.Append("row_" + std::to_string(i)).ok());
    auto str_arr = str_b.Finish().ValueOrDie();

    auto fsb_type = arrow::fixed_size_binary(kVecDim * sizeof(float));
    arrow::FixedSizeBinaryBuilder vec_b(fsb_type);
    for (int i = 0; i < kTestRows; i++) {
        float v[kVecDim];
        for (int d = 0; d < kVecDim; d++)
            v[d] = static_cast<float>(i * kVecDim + d);
        EXPECT_TRUE(
            vec_b.Append(reinterpret_cast<const uint8_t*>(v)).ok());
    }
    auto vec_arr = vec_b.Finish().ValueOrDie();

    auto schema = arrow::schema({
        arrow::field("int32_col", arrow::int32()),
        arrow::field("varchar_col", arrow::utf8()),
        arrow::field("vec_col", fsb_type),
    });
    return arrow::Table::Make(schema, {int32_arr, str_arr, vec_arr});
}

// Schema with an ARRAY field (unsupported type for take fast path).
struct ExternalSchemaWithArray {
    SchemaPtr schema;
    FieldId int64_id;
    FieldId array_id;
};

ExternalSchemaWithArray
BuildExternalSchemaWithArray() {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    ExternalSchemaWithArray info;
    info.int64_id = FieldId(100);
    info.array_id = FieldId(101);

    schema->AddField(FieldMeta(FieldName("int64_col"),
                               info.int64_id,
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "int64_col"));
    schema->AddField(FieldMeta(FieldName("array_col"),
                               info.array_id,
                               DataType::ARRAY,
                               DataType::INT32,
                               false,
                               std::nullopt,
                               "array_col"));

    schema->set_primary_field_id(info.int64_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");
    info.schema = schema;
    return info;
}

// Table with int64_col + array_col (placeholder int32 for unsupported type test).
std::shared_ptr<arrow::Table>
BuildTableWithArrayColumn() {
    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    arrow::Int32Builder int32_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int32_b.Append(i).ok());
    auto array_arr = int32_b.Finish().ValueOrDie();

    auto schema = arrow::schema({
        arrow::field("int64_col", arrow::int64()),
        arrow::field("array_col", arrow::int32()),
    });
    return arrow::Table::Make(schema, {int64_arr, array_arr});
}

}  // namespace

// Test TryTakeForRetrieve with all supported data types
TEST(ExternalTakeTest, TryTakeForRetrieve_MultiTypes) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    // Build RetrievePlan with all external fields
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {bool_id,   int8_id,  int16_id,  int32_id, int64_id,
                        float_id,  double_id, varchar_id, vec_id};

    auto results =
        std::make_unique<proto::segcore::RetrieveResults>();

    // Use offsets [0, 2, 4] to test non-contiguous access + dedup
    std::vector<int64_t> offsets = {0, 2, 4};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);

    // Verify all fields are present
    ASSERT_EQ(results->fields_data_size(), 9);

    // Verify BOOL field
    auto& bool_data = results->fields_data(0);
    ASSERT_EQ(bool_data.field_id(), bool_id.get());
    ASSERT_EQ(bool_data.scalars().bool_data().data_size(), size);
    EXPECT_EQ(bool_data.scalars().bool_data().data(0), true);   // row 0
    EXPECT_EQ(bool_data.scalars().bool_data().data(1), true);   // row 2
    EXPECT_EQ(bool_data.scalars().bool_data().data(2), true);   // row 4

    // Verify INT8 field (promoted to int32 in protobuf)
    auto& int8_data = results->fields_data(1);
    ASSERT_EQ(int8_data.field_id(), int8_id.get());
    ASSERT_EQ(int8_data.scalars().int_data().data_size(), size);
    EXPECT_EQ(int8_data.scalars().int_data().data(0), 0);     // 0*10
    EXPECT_EQ(int8_data.scalars().int_data().data(1), 20);    // 2*10
    EXPECT_EQ(int8_data.scalars().int_data().data(2), 40);    // 4*10

    // Verify INT16 field (promoted to int32)
    auto& int16_data = results->fields_data(2);
    ASSERT_EQ(int16_data.field_id(), int16_id.get());
    ASSERT_EQ(int16_data.scalars().int_data().data_size(), size);
    EXPECT_EQ(int16_data.scalars().int_data().data(0), 0);
    EXPECT_EQ(int16_data.scalars().int_data().data(1), 200);
    EXPECT_EQ(int16_data.scalars().int_data().data(2), 400);

    // Verify INT32 field
    auto& int32_data = results->fields_data(3);
    ASSERT_EQ(int32_data.field_id(), int32_id.get());
    ASSERT_EQ(int32_data.scalars().int_data().data_size(), size);
    EXPECT_EQ(int32_data.scalars().int_data().data(0), 0);
    EXPECT_EQ(int32_data.scalars().int_data().data(1), 2000);
    EXPECT_EQ(int32_data.scalars().int_data().data(2), 4000);

    // Verify INT64 field
    auto& int64_data = results->fields_data(4);
    ASSERT_EQ(int64_data.field_id(), int64_id.get());
    ASSERT_EQ(int64_data.scalars().long_data().data_size(), size);
    EXPECT_EQ(int64_data.scalars().long_data().data(0), 0);
    EXPECT_EQ(int64_data.scalars().long_data().data(1), 20000);
    EXPECT_EQ(int64_data.scalars().long_data().data(2), 40000);

    // Verify FLOAT field
    auto& float_data = results->fields_data(5);
    ASSERT_EQ(float_data.field_id(), float_id.get());
    ASSERT_EQ(float_data.scalars().float_data().data_size(), size);
    EXPECT_FLOAT_EQ(float_data.scalars().float_data().data(0), 0.0f);
    EXPECT_FLOAT_EQ(float_data.scalars().float_data().data(1), 2.2f);
    EXPECT_FLOAT_EQ(float_data.scalars().float_data().data(2), 4.4f);

    // Verify DOUBLE field
    auto& double_data = results->fields_data(6);
    ASSERT_EQ(double_data.field_id(), double_id.get());
    ASSERT_EQ(double_data.scalars().double_data().data_size(), size);
    EXPECT_DOUBLE_EQ(double_data.scalars().double_data().data(0), 0.0);
    EXPECT_DOUBLE_EQ(double_data.scalars().double_data().data(1), 4.4);
    EXPECT_DOUBLE_EQ(double_data.scalars().double_data().data(2), 8.8);

    // Verify VARCHAR field
    auto& str_data = results->fields_data(7);
    ASSERT_EQ(str_data.field_id(), varchar_id.get());
    ASSERT_EQ(str_data.scalars().string_data().data_size(), size);
    EXPECT_EQ(str_data.scalars().string_data().data(0), "row_0");
    EXPECT_EQ(str_data.scalars().string_data().data(1), "row_2");
    EXPECT_EQ(str_data.scalars().string_data().data(2), "row_4");

    // Verify VECTOR_FLOAT field
    auto& vec_data = results->fields_data(8);
    ASSERT_EQ(vec_data.field_id(), vec_id.get());
    ASSERT_EQ(vec_data.vectors().dim(), kVecDim);
    auto& fv = vec_data.vectors().float_vector();
    ASSERT_EQ(fv.data_size(), size * kVecDim);
    // Row 0: [0,1,2,3], Row 2: [8,9,10,11], Row 4: [16,17,18,19]
    EXPECT_FLOAT_EQ(fv.data(0), 0.0f);
    EXPECT_FLOAT_EQ(fv.data(4), 8.0f);
    EXPECT_FLOAT_EQ(fv.data(8), 16.0f);
}

// Test TryTakeForSearch with all supported data types
TEST(ExternalTakeTest, TryTakeForSearch_MultiTypes) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    // Build Search Plan with target entries
    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {bool_id,   int8_id,  int16_id,  int32_id,
                             int64_id,  float_id, double_id, varchar_id,
                             vec_id};

    std::vector<int64_t> seg_offsets = {1, 3};
    int64_t size = seg_offsets.size();

    SearchResult results;
    bool ok = segment->TestTryTakeForSearch(
        plan.get(), seg_offsets.data(), size, results);
    ASSERT_TRUE(ok);

    // Verify all fields are populated
    ASSERT_EQ(results.output_fields_data_.size(), 9u);

    // Verify BOOL
    auto& bool_arr = results.output_fields_data_.at(bool_id);
    ASSERT_EQ(bool_arr->scalars().bool_data().data_size(), size);
    EXPECT_EQ(bool_arr->scalars().bool_data().data(0), false);  // row 1
    EXPECT_EQ(bool_arr->scalars().bool_data().data(1), false);  // row 3

    // Verify INT8
    auto& int8_arr = results.output_fields_data_.at(int8_id);
    ASSERT_EQ(int8_arr->scalars().int_data().data_size(), size);
    EXPECT_EQ(int8_arr->scalars().int_data().data(0), 10);
    EXPECT_EQ(int8_arr->scalars().int_data().data(1), 30);

    // Verify INT16
    auto& int16_arr = results.output_fields_data_.at(int16_id);
    ASSERT_EQ(int16_arr->scalars().int_data().data_size(), size);
    EXPECT_EQ(int16_arr->scalars().int_data().data(0), 100);
    EXPECT_EQ(int16_arr->scalars().int_data().data(1), 300);

    // Verify INT32
    auto& int32_arr = results.output_fields_data_.at(int32_id);
    ASSERT_EQ(int32_arr->scalars().int_data().data_size(), size);
    EXPECT_EQ(int32_arr->scalars().int_data().data(0), 1000);
    EXPECT_EQ(int32_arr->scalars().int_data().data(1), 3000);

    // Verify INT64
    auto& int64_arr = results.output_fields_data_.at(int64_id);
    ASSERT_EQ(int64_arr->scalars().long_data().data_size(), size);
    EXPECT_EQ(int64_arr->scalars().long_data().data(0), 10000);
    EXPECT_EQ(int64_arr->scalars().long_data().data(1), 30000);

    // Verify VARCHAR
    auto& str_arr = results.output_fields_data_.at(varchar_id);
    ASSERT_EQ(str_arr->scalars().string_data().data_size(), size);
    EXPECT_EQ(str_arr->scalars().string_data().data(0), "row_1");
    EXPECT_EQ(str_arr->scalars().string_data().data(1), "row_3");

    // Verify VECTOR_FLOAT
    auto& vec_arr = results.output_fields_data_.at(vec_id);
    ASSERT_EQ(vec_arr->vectors().dim(), kVecDim);
    ASSERT_EQ(vec_arr->vectors().float_vector().data_size(), size * kVecDim);
    // Row 1: [4,5,6,7], Row 3: [12,13,14,15]
    EXPECT_FLOAT_EQ(vec_arr->vectors().float_vector().data(0), 4.0f);
    EXPECT_FLOAT_EQ(vec_arr->vectors().float_vector().data(4), 12.0f);
}

// Test fallback: returns false for non-external collection
TEST(ExternalTakeTest, TryTakeForRetrieve_FallbackNonExternal) {
    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_id);
    // No external_source set => not external

    auto holder = CreateSealedSegment(schema);
    auto* segment =
        dynamic_cast<ChunkedSegmentSealedImpl*>(holder.get());
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {pk_id};

    auto results =
        std::make_unique<proto::segcore::RetrieveResults>();
    int64_t offset = 0;
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, &offset, 1, false, false);
    EXPECT_FALSE(ok);
}

// Test fallback: returns false when size exceeds threshold
TEST(ExternalTakeTest, TryTakeForRetrieve_FallbackOverThreshold) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    auto holder = CreateSealedSegment(schema);
    auto* segment =
        dynamic_cast<ChunkedSegmentSealedImpl*>(holder.get());

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results =
        std::make_unique<proto::segcore::RetrieveResults>();
    // Size > 10000 threshold
    std::vector<int64_t> offsets(10001, 0);
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), 10001, false, false);
    EXPECT_FALSE(ok);
}

// Test fallback: returns false when reader is null
TEST(ExternalTakeTest, TryTakeForRetrieve_FallbackNullReader) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    auto holder = CreateSealedSegment(schema);
    auto* segment =
        dynamic_cast<ChunkedSegmentSealedImpl*>(holder.get());
    // reader_ is null by default

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results =
        std::make_unique<proto::segcore::RetrieveResults>();
    int64_t offset = 0;
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, &offset, 1, false, false);
    EXPECT_FALSE(ok);
}

// Test FillTargetEntry override delegates to TryTakeForSearch
TEST(ExternalTakeTest, FillTargetEntry_DelegatesToTake) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {int64_id, varchar_id};

    SearchResult results;
    results.distances_ = {0.1f, 0.2f};
    results.seg_offsets_ = {0, 3};

    segment->TestFillTargetEntry(plan.get(), results);

    // Should have filled via TryTakeForSearch
    ASSERT_EQ(results.output_fields_data_.size(), 2u);

    auto& int64_arr = results.output_fields_data_.at(int64_id);
    ASSERT_EQ(int64_arr->scalars().long_data().data_size(), 2);
    EXPECT_EQ(int64_arr->scalars().long_data().data(0), 0);
    EXPECT_EQ(int64_arr->scalars().long_data().data(1), 30000);

    auto& str_arr = results.output_fields_data_.at(varchar_id);
    ASSERT_EQ(str_arr->scalars().string_data().data_size(), 2);
    EXPECT_EQ(str_arr->scalars().string_data().data(0), "row_0");
    EXPECT_EQ(str_arr->scalars().string_data().data(1), "row_3");
}

// ---------- TryTakeForRetrieve: error & edge-case paths ----------

// take() returns error → fallback
TEST(ExternalTakeTest, TryTakeForRetrieve_TakeFailure) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<ErrorMockTakeReader>());

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    int64_t offset = 0;
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, &offset, 1, false, false);
    EXPECT_FALSE(ok);
}

// Returned table missing expected column → fallback
TEST(ExternalTakeTest, TryTakeForRetrieve_ColumnNotFound) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();

    // Table with only int64_col; varchar_col is missing
    arrow::Int64Builder b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(b.Append(i * 10000).ok());
    auto arr = b.Finish().ValueOrDie();
    auto partial_table = arrow::Table::Make(
        arrow::schema({arrow::field("int64_col", arrow::int64())}), {arr});

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(
        std::make_unique<MockTakeReader>(partial_table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, varchar_id};  // varchar_col not in table

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    int64_t offset = 0;
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, &offset, 1, false, false);
    EXPECT_FALSE(ok);
}

// size == 0 → early return false
TEST(ExternalTakeTest, TryTakeForRetrieve_EmptySize) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, nullptr, 0, false, false);
    EXPECT_FALSE(ok);
}

// Virtual PK generation + fill_ids
TEST(ExternalTakeTest, TryTakeForRetrieve_VirtualPK_FillIds) {
    auto [schema, pk_id, int32_id, varchar_id, vec_id] =
        BuildExternalSchemaWithVirtualPK();
    auto table = BuildVirtualPKTestTable();
    constexpr int64_t kSegId = 42;
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema, kSegId);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {pk_id, int32_id, varchar_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 3};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, true);
    ASSERT_TRUE(ok);

    // Virtual PK should be in fields_data (ignore_non_pk=false)
    bool found_pk = false;
    for (int i = 0; i < results->fields_data_size(); i++) {
        if (results->fields_data(i).field_id() == pk_id.get()) {
            found_pk = true;
            auto& pk_data = results->fields_data(i);
            ASSERT_EQ(pk_data.scalars().long_data().data_size(), size);
            EXPECT_EQ(pk_data.scalars().long_data().data(0),
                       GetVirtualPK(kSegId, 0));
            EXPECT_EQ(pk_data.scalars().long_data().data(1),
                       GetVirtualPK(kSegId, 3));
        }
    }
    EXPECT_TRUE(found_pk);

    // fill_ids=true: ids should be populated
    ASSERT_EQ(results->ids().int_id().data_size(), size);
    EXPECT_EQ(results->ids().int_id().data(0), GetVirtualPK(kSegId, 0));
    EXPECT_EQ(results->ids().int_id().data(1), GetVirtualPK(kSegId, 3));

    // External fields should also be present
    bool found_int32 = false;
    for (int i = 0; i < results->fields_data_size(); i++) {
        if (results->fields_data(i).field_id() == int32_id.get()) {
            found_int32 = true;
            EXPECT_EQ(results->fields_data(i).scalars().int_data().data(0), 0);
            EXPECT_EQ(
                results->fields_data(i).scalars().int_data().data(1), 3000);
        }
    }
    EXPECT_TRUE(found_int32);
}

// ignore_non_pk=true with external PK: PK taken but not added to fields_data
// (by design: !ignore_non_pk guard prevents AddAllocated for external PK).
// With virtual PK + ignore_non_pk, the PK is generated and fill_ids populates ids.
TEST(ExternalTakeTest, TryTakeForRetrieve_IgnoreNonPK_VirtualPK) {
    auto [schema, pk_id, int32_id, varchar_id, vec_id] =
        BuildExternalSchemaWithVirtualPK();
    auto table = BuildVirtualPKTestTable();
    constexpr int64_t kSegId = 7;
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema, kSegId);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {pk_id, int32_id, varchar_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {1, 4};
    int64_t size = offsets.size();

    // ignore_non_pk=true, fill_ids=true
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, true, true);
    // Virtual PK (non-external) + ignore_non_pk → take_field_ids empty → false
    // Because all external fields are skipped (ignore_non_pk && !is_pk_field)
    // and PK itself is non-external → also skipped in take collection
    EXPECT_FALSE(ok);
}

// Duplicate offsets: dedup + result_mapping
TEST(ExternalTakeTest, TryTakeForRetrieve_DuplicateOffsets) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    // Offsets with duplicates: [2, 0, 2]
    std::vector<int64_t> offsets = {2, 0, 2};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);

    ASSERT_EQ(results->fields_data_size(), 1);
    auto& data = results->fields_data(0);
    ASSERT_EQ(data.scalars().long_data().data_size(), 3);
    // Expected: values at offsets 2, 0, 2 → 20000, 0, 20000
    EXPECT_EQ(data.scalars().long_data().data(0), 20000);
    EXPECT_EQ(data.scalars().long_data().data(1), 0);
    EXPECT_EQ(data.scalars().long_data().data(2), 20000);
}

// Unsupported data type (ARRAY) → clears results, returns false
TEST(ExternalTakeTest, TryTakeForRetrieve_UnsupportedType) {
    auto [schema, int64_id, array_id] = BuildExternalSchemaWithArray();
    auto table = BuildTableWithArrayColumn();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, array_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    int64_t offset = 0;
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, &offset, 1, false, false);
    EXPECT_FALSE(ok);
    // Results should be cleared
    EXPECT_EQ(results->fields_data_size(), 0);
}

// ---------- TryTakeForSearch: error & edge-case paths ----------

// Null reader → returns false
TEST(ExternalTakeTest, TryTakeForSearch_NullReader) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    // reader_ is null by default

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {int64_id};

    SearchResult results;
    int64_t offset = 0;
    bool ok = segment->TestTryTakeForSearch(plan.get(), &offset, 1, results);
    EXPECT_FALSE(ok);
}

// take() failure → returns false
TEST(ExternalTakeTest, TryTakeForSearch_TakeFailure) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<ErrorMockTakeReader>());

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {int64_id};

    SearchResult results;
    int64_t offset = 0;
    bool ok = segment->TestTryTakeForSearch(plan.get(), &offset, 1, results);
    EXPECT_FALSE(ok);
}

// Column not found in search take result → returns false
TEST(ExternalTakeTest, TryTakeForSearch_ColumnNotFound) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();

    // Table with only int64_col
    arrow::Int64Builder b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(b.Append(i).ok());
    auto arr = b.Finish().ValueOrDie();
    auto partial_table = arrow::Table::Make(
        arrow::schema({arrow::field("int64_col", arrow::int64())}), {arr});

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(
        std::make_unique<MockTakeReader>(partial_table));

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {int64_id, varchar_id};  // varchar_col missing

    SearchResult results;
    int64_t offset = 0;
    bool ok = segment->TestTryTakeForSearch(plan.get(), &offset, 1, results);
    EXPECT_FALSE(ok);
}

// No external fields in target_entries → returns false
TEST(ExternalTakeTest, TryTakeForSearch_EmptyTargetEntries) {
    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");
    // pk has no external_field_mapping → is_external_field() false

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {pk_id};  // not external

    SearchResult results;
    int64_t offset = 0;
    bool ok = segment->TestTryTakeForSearch(plan.get(), &offset, 1, results);
    EXPECT_FALSE(ok);
}

// Non-external collection → returns false
TEST(ExternalTakeTest, TryTakeForSearch_FallbackNonExternal) {
    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_id);

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {pk_id};

    SearchResult results;
    int64_t offset = 0;
    bool ok = segment->TestTryTakeForSearch(plan.get(), &offset, 1, results);
    EXPECT_FALSE(ok);
}

// size > threshold → returns false
TEST(ExternalTakeTest, TryTakeForSearch_FallbackOverThreshold) {
    auto [schema, bool_id, int8_id, int16_id, int32_id, int64_id, float_id,
          double_id, varchar_id, vec_id] = BuildExternalSchema();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {int64_id};

    SearchResult results;
    std::vector<int64_t> offsets(10001, 0);
    bool ok = segment->TestTryTakeForSearch(
        plan.get(), offsets.data(), 10001, results);
    EXPECT_FALSE(ok);
}

// Unsupported data type (ARRAY) in search → clears results, returns false
TEST(ExternalTakeTest, TryTakeForSearch_UnsupportedType) {
    auto [schema, int64_id, array_id] = BuildExternalSchemaWithArray();
    auto table = BuildTableWithArrayColumn();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {int64_id, array_id};

    SearchResult results;
    int64_t offset = 0;
    bool ok = segment->TestTryTakeForSearch(plan.get(), &offset, 1, results);
    EXPECT_FALSE(ok);
    EXPECT_TRUE(results.output_fields_data_.empty());
}

// NormalizeVectorArraysToFixedSizeBinary tests are skipped in this file
// because storage/Util.cpp requires additional runtime initialization that
// the standalone test binary does not provide.
// The LIST/FixedSizeList/passthrough/error paths are covered by the
// integration test that exercises GetFieldDatasFromManifest end-to-end.
