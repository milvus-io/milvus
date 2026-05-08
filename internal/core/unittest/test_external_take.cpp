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
#include <arrow/compute/api.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/VirtualPK.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "milvus-storage/properties.h"
#include "milvus-storage/reader.h"
#include "query/PlanImpl.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentSealed.h"
#include "storage/Util.h"
#include "storage/loon_ffi/util.h"

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
    get_chunk_reader(int64_t, const std::shared_ptr<std::vector<std::string>>&)
        const override {
        return arrow::Status::NotImplemented("mock");
    }

    arrow::Result<std::shared_ptr<arrow::Table>>
    take(const std::vector<int64_t>& row_indices,
         size_t,
         const std::shared_ptr<std::vector<std::string>>& needed_columns)
        override {
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
    // Select rows from table at given indices using arrow::compute::Take.
    static arrow::Result<std::shared_ptr<arrow::Table>>
    SelectRows(const std::shared_ptr<arrow::Table>& table,
               const std::vector<int64_t>& indices) {
        if (indices.empty()) {
            return table->Slice(0, 0);
        }
        arrow::Int64Builder idx_builder;
        ARROW_RETURN_NOT_OK(idx_builder.AppendValues(indices));
        std::shared_ptr<arrow::Array> idx_arr;
        ARROW_RETURN_NOT_OK(idx_builder.Finish(&idx_arr));
        ARROW_ASSIGN_OR_RAISE(auto result,
                              arrow::compute::Take(table, idx_arr));
        return result.table();
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
        EXPECT_TRUE(vec_b.Append(reinterpret_cast<const uint8_t*>(v)).ok());
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

    return arrow::Table::Make(schema,
                              {bool_arr,
                               int8_arr,
                               int16_arr,
                               int32_arr,
                               int64_arr,
                               float_arr,
                               double_arr,
                               str_arr,
                               vec_arr});
}

std::shared_ptr<arrow::Table>
BuildNullableVectorArrowTable() {
    auto fsb_type = arrow::fixed_size_binary(kVecDim * sizeof(float));
    arrow::FixedSizeBinaryBuilder vec_b(fsb_type);
    for (int i = 0; i < 3; i++) {
        if (i == 1) {
            EXPECT_TRUE(vec_b.AppendNull().ok());
            continue;
        }
        float v[kVecDim];
        for (int d = 0; d < kVecDim; d++) {
            v[d] = static_cast<float>(i * kVecDim + d);
        }
        EXPECT_TRUE(vec_b.Append(reinterpret_cast<const uint8_t*>(v)).ok());
    }
    auto vec_arr = vec_b.Finish().ValueOrDie();

    auto schema = arrow::schema({
        arrow::field("vec_col", fsb_type),
    });
    return arrow::Table::Make(schema, {vec_arr});
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
    // External scalar fields are nullable=true (forced by ValidateExternalCollectionSchema)
    schema->AddField(FieldMeta(FieldName("bool_col"),
                               info.bool_id,
                               DataType::BOOL,
                               true,
                               std::nullopt,
                               "bool_col"));
    schema->AddField(FieldMeta(FieldName("int8_col"),
                               info.int8_id,
                               DataType::INT8,
                               true,
                               std::nullopt,
                               "int8_col"));
    schema->AddField(FieldMeta(FieldName("int16_col"),
                               info.int16_id,
                               DataType::INT16,
                               true,
                               std::nullopt,
                               "int16_col"));
    schema->AddField(FieldMeta(FieldName("int32_col"),
                               info.int32_id,
                               DataType::INT32,
                               true,
                               std::nullopt,
                               "int32_col"));
    // int64_col is PK in this test schema — PK must not be nullable
    schema->AddField(FieldMeta(FieldName("int64_col"),
                               info.int64_id,
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "int64_col"));
    schema->AddField(FieldMeta(FieldName("float_col"),
                               info.float_id,
                               DataType::FLOAT,
                               true,
                               std::nullopt,
                               "float_col"));
    schema->AddField(FieldMeta(FieldName("double_col"),
                               info.double_id,
                               DataType::DOUBLE,
                               true,
                               std::nullopt,
                               "double_col"));
    // String field: FieldMeta(name, id, type, max_length, nullable, default_value, external_field)
    schema->AddField(FieldMeta(FieldName("varchar_col"),
                               info.varchar_id,
                               DataType::VARCHAR,
                               65535,
                               true,
                               std::nullopt,
                               "varchar_col"));
    // Vector field: FieldMeta(name, id, type, dim, metric_type, nullable, default_value, external_field)
    schema->AddField(FieldMeta(FieldName("vec_col"),
                               info.vec_id,
                               DataType::VECTOR_FLOAT,
                               kVecDim,
                               knowhere::metric::L2,
                               true,
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
CreateExternalSegment(SegmentSealedUPtr& holder,
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
    get_chunk_reader(int64_t, const std::shared_ptr<std::vector<std::string>>&)
        const override {
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
    schema->AddField(FieldMeta(FieldName("virtual_pk"),
                               info.pk_id,
                               DataType::INT64,
                               false,
                               std::nullopt));
    // External scalar fields (nullable=true, forced by ValidateExternalCollectionSchema)
    schema->AddField(FieldMeta(FieldName("int32_col"),
                               info.int32_id,
                               DataType::INT32,
                               true,
                               std::nullopt,
                               "int32_col"));
    schema->AddField(FieldMeta(FieldName("varchar_col"),
                               info.varchar_id,
                               DataType::VARCHAR,
                               65535,
                               true,
                               std::nullopt,
                               "varchar_col"));
    schema->AddField(FieldMeta(FieldName("vec_col"),
                               info.vec_id,
                               DataType::VECTOR_FLOAT,
                               kVecDim,
                               knowhere::metric::L2,
                               true,
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
        EXPECT_TRUE(vec_b.Append(reinterpret_cast<const uint8_t*>(v)).ok());
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

    // int64_col is PK in this test schema — PK must not be nullable
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
                               true,
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
    for (int i = 0; i < kTestRows; i++) EXPECT_TRUE(int32_b.Append(i).ok());
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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(true);

    // Build RetrievePlan with all external fields
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {bool_id,
                        int8_id,
                        int16_id,
                        int32_id,
                        int64_id,
                        float_id,
                        double_id,
                        varchar_id,
                        vec_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();

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
    EXPECT_EQ(bool_data.scalars().bool_data().data(0), true);  // row 0
    EXPECT_EQ(bool_data.scalars().bool_data().data(1), true);  // row 2
    EXPECT_EQ(bool_data.scalars().bool_data().data(2), true);  // row 4

    // Verify INT8 field (promoted to int32 in protobuf)
    auto& int8_data = results->fields_data(1);
    ASSERT_EQ(int8_data.field_id(), int8_id.get());
    ASSERT_EQ(int8_data.scalars().int_data().data_size(), size);
    EXPECT_EQ(int8_data.scalars().int_data().data(0), 0);   // 0*10
    EXPECT_EQ(int8_data.scalars().int_data().data(1), 20);  // 2*10
    EXPECT_EQ(int8_data.scalars().int_data().data(2), 40);  // 4*10

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

TEST(ExternalTakeTest, TryTakeForRetrieve_NullableVectorUsesCompactData) {
    auto info = BuildExternalSchema();
    auto table = BuildNullableVectorArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, info.schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(true);

    auto plan = std::make_unique<query::RetrievePlan>(info.schema);
    plan->field_ids_ = {info.vec_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 1, 2};

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), offsets.size(), false, false);
    ASSERT_TRUE(ok);
    ASSERT_EQ(results->fields_data_size(), 1);

    auto& vec_data = results->fields_data(0);
    ASSERT_EQ(vec_data.field_id(), info.vec_id.get());
    ASSERT_EQ(vec_data.valid_data_size(), 3);
    EXPECT_TRUE(vec_data.valid_data(0));
    EXPECT_FALSE(vec_data.valid_data(1));
    EXPECT_TRUE(vec_data.valid_data(2));

    auto& fv = vec_data.vectors().float_vector();
    ASSERT_EQ(fv.data_size(), 2 * kVecDim);
    EXPECT_FLOAT_EQ(fv.data(0), 0.0f);
    EXPECT_FLOAT_EQ(fv.data(1), 1.0f);
    EXPECT_FLOAT_EQ(fv.data(2), 2.0f);
    EXPECT_FLOAT_EQ(fv.data(3), 3.0f);
    EXPECT_FLOAT_EQ(fv.data(4), 8.0f);
    EXPECT_FLOAT_EQ(fv.data(5), 9.0f);
    EXPECT_FLOAT_EQ(fv.data(6), 10.0f);
    EXPECT_FLOAT_EQ(fv.data(7), 11.0f);
}

// Test TryTakeForSearch with all supported data types
TEST(ExternalTakeTest, TryTakeForSearch_MultiTypes) {
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(true);

    // Build Search Plan with target entries
    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {bool_id,
                             int8_id,
                             int16_id,
                             int32_id,
                             int64_id,
                             float_id,
                             double_id,
                             varchar_id,
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

TEST(ExternalTakeTest, TryTakeForSearch_NullableVectorUsesCompactData) {
    auto info = BuildExternalSchema();
    auto table = BuildNullableVectorArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, info.schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(true);

    auto plan = std::make_unique<query::Plan>(info.schema);
    plan->target_entries_ = {info.vec_id};

    std::vector<int64_t> seg_offsets = {0, 1, 2};
    SearchResult results;
    bool ok = segment->TestTryTakeForSearch(
        plan.get(), seg_offsets.data(), seg_offsets.size(), results);
    ASSERT_TRUE(ok);

    auto& vec_data = results.output_fields_data_.at(info.vec_id);
    ASSERT_EQ(vec_data->valid_data_size(), 3);
    EXPECT_TRUE(vec_data->valid_data(0));
    EXPECT_FALSE(vec_data->valid_data(1));
    EXPECT_TRUE(vec_data->valid_data(2));

    auto& fv = vec_data->vectors().float_vector();
    ASSERT_EQ(fv.data_size(), 2 * kVecDim);
    EXPECT_FLOAT_EQ(fv.data(0), 0.0f);
    EXPECT_FLOAT_EQ(fv.data(1), 1.0f);
    EXPECT_FLOAT_EQ(fv.data(2), 2.0f);
    EXPECT_FLOAT_EQ(fv.data(3), 3.0f);
    EXPECT_FLOAT_EQ(fv.data(4), 8.0f);
    EXPECT_FLOAT_EQ(fv.data(5), 9.0f);
    EXPECT_FLOAT_EQ(fv.data(6), 10.0f);
    EXPECT_FLOAT_EQ(fv.data(7), 11.0f);
}

// Test fallback: returns false for non-external collection
TEST(ExternalTakeTest, TryTakeForRetrieve_FallbackNonExternal) {
    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_id);
    // No external_source set => not external

    auto holder = CreateSealedSegment(schema);
    auto* segment = dynamic_cast<ChunkedSegmentSealedImpl*>(holder.get());
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {pk_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    int64_t offset = 0;
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, &offset, 1, false, false);
    EXPECT_FALSE(ok);
}

// Test fallback: returns false when size exceeds threshold
TEST(ExternalTakeTest, TryTakeForRetrieve_FallbackOverThreshold) {
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto holder = CreateSealedSegment(schema);
    auto* segment = dynamic_cast<ChunkedSegmentSealedImpl*>(holder.get());

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    // Size > 10000 threshold
    std::vector<int64_t> offsets(10001, 0);
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), 10001, false, false);
    EXPECT_FALSE(ok);
}

// Test fallback: returns false when reader is null
TEST(ExternalTakeTest, TryTakeForRetrieve_FallbackNullReader) {
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto holder = CreateSealedSegment(schema);
    auto* segment = dynamic_cast<ChunkedSegmentSealedImpl*>(holder.get());
    // reader_ is null by default

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    int64_t offset = 0;
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, &offset, 1, false, false);
    EXPECT_FALSE(ok);
}

// Test FillTargetEntry override delegates to TryTakeForSearch
TEST(ExternalTakeTest, FillTargetEntry_DelegatesToTake) {
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(true);

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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();

    // Table with only int64_col; varchar_col is missing
    arrow::Int64Builder b;
    for (int i = 0; i < kTestRows; i++) EXPECT_TRUE(b.Append(i * 10000).ok());
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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
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
    segment->SetUseTakeForOutputForTesting(true);

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
            EXPECT_EQ(results->fields_data(i).scalars().int_data().data(1),
                      3000);
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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(true);

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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();

    // Table with only int64_col
    arrow::Int64Builder b;
    for (int i = 0; i < kTestRows; i++) EXPECT_TRUE(b.Append(i).ok());
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
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
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

// ---------- Access mode threshold logic tests ----------

// use_take_for_output=false: TryTakeForRetrieve returns false
TEST(ExternalTakeAccessMode, RetrieveDisabledReturnsFalse) {
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(false);

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    int64_t offset = 0;
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, &offset, 1, false, false);
    EXPECT_FALSE(ok);
}

// use_take_for_output=true: TryTakeForRetrieve proceeds with take
TEST(ExternalTakeAccessMode, RetrieveEnabledUsesTake) {
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(true);

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 1, 2, 3, 4};
    int64_t size = offsets.size();
    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);
    ASSERT_EQ(results->fields_data_size(), 1);
    EXPECT_EQ(results->fields_data(0).scalars().long_data().data_size(), size);
}

// use_take_for_output=false: TryTakeForSearch returns false
TEST(ExternalTakeAccessMode, SearchDisabledReturnsFalse) {
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(false);

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {int64_id};

    SearchResult results;
    int64_t offset = 0;
    bool ok = segment->TestTryTakeForSearch(plan.get(), &offset, 1, results);
    EXPECT_FALSE(ok);
}

// use_take_for_output=true: TryTakeForSearch proceeds with take
TEST(ExternalTakeAccessMode, SearchEnabledUsesTake) {
    auto [schema,
          bool_id,
          int8_id,
          int16_id,
          int32_id,
          int64_id,
          float_id,
          double_id,
          varchar_id,
          vec_id] = BuildExternalSchema();
    auto table = BuildTestArrowTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));
    segment->SetUseTakeForOutputForTesting(true);

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {int64_id};

    SearchResult results;
    std::vector<int64_t> offsets = {0, 2, 4};
    int64_t size = offsets.size();
    bool ok = segment->TestTryTakeForSearch(
        plan.get(), offsets.data(), size, results);
    ASSERT_TRUE(ok);
    ASSERT_EQ(results.output_fields_data_.size(), 1u);
    auto& int64_arr = results.output_fields_data_.at(int64_id);
    ASSERT_EQ(int64_arr->scalars().long_data().data_size(), size);
    EXPECT_EQ(int64_arr->scalars().long_data().data(0), 0);
    EXPECT_EQ(int64_arr->scalars().long_data().data(1), 20000);
    EXPECT_EQ(int64_arr->scalars().long_data().data(2), 40000);
}

// NormalizeVectorArraysToFixedSizeBinary tests are skipped in this file
// because storage/Util.cpp requires additional runtime initialization that
// the standalone test binary does not provide.
// The LIST/FixedSizeList/passthrough/error paths are covered by the
// integration test that exercises GetFieldDatasFromManifest end-to-end.

// ============================================================
// Tests for GetExternalColumnNames
// ============================================================

TEST(SchemaExternalColumns, ExternalCollectionReturnsExternalFieldNames) {
    auto schema = std::make_shared<Schema>();
    // System fields without external_field mapping
    schema->AddField(FieldMeta(FieldName("__virtual_pk__"),
                               FieldId(1),
                               DataType::INT64,
                               false,
                               std::nullopt));
    schema->AddField(FieldMeta(
        FieldName("RowID"), FieldId(0), DataType::INT64, false, std::nullopt));
    // User fields with external_field mapping
    schema->AddField(FieldMeta(FieldName("pk"),
                               FieldId(100),
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "pk"));
    schema->AddField(FieldMeta(FieldName("label"),
                               FieldId(101),
                               DataType::VARCHAR,
                               256,
                               false,
                               std::nullopt,
                               "label"));
    schema->AddField(FieldMeta(FieldName("vector"),
                               FieldId(102),
                               DataType::VECTOR_FLOAT,
                               4,
                               knowhere::metric::L2,
                               false,
                               std::nullopt,
                               "float32_vector"));
    schema->set_external_source("s3://bucket/data");

    auto columns = schema->GetExternalColumnNames();
    ASSERT_NE(columns, nullptr);
    ASSERT_EQ(columns->size(), 3);
    EXPECT_EQ((*columns)[0], "pk");
    EXPECT_EQ((*columns)[1], "label");
    EXPECT_EQ((*columns)[2], "float32_vector");
}

TEST(SchemaExternalColumns, ExternalCollectionPreservesFieldOrder) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldMeta(FieldName("c"),
                               FieldId(102),
                               DataType::FLOAT,
                               false,
                               std::nullopt,
                               "col_c"));
    schema->AddField(FieldMeta(FieldName("a"),
                               FieldId(100),
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "col_a"));
    schema->AddField(FieldMeta(FieldName("b"),
                               FieldId(101),
                               DataType::DOUBLE,
                               false,
                               std::nullopt,
                               "col_b"));
    schema->set_external_source("s3://bucket/data");

    auto columns = schema->GetExternalColumnNames();
    ASSERT_EQ(columns->size(), 3);
    // Should follow field_ids_ insertion order
    EXPECT_EQ((*columns)[0], "col_c");
    EXPECT_EQ((*columns)[1], "col_a");
    EXPECT_EQ((*columns)[2], "col_b");
}

TEST(SchemaExternalColumns, NonExternalCollectionReturnsEmpty) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldMeta(
        FieldName("pk"), FieldId(100), DataType::INT64, false, std::nullopt));
    schema->AddField(FieldMeta(
        FieldName("data"), FieldId(101), DataType::FLOAT, false, std::nullopt));
    // No external_source set → not external collection

    auto columns = schema->GetExternalColumnNames();
    ASSERT_NE(columns, nullptr);
    EXPECT_TRUE(columns->empty());
}

TEST(SchemaExternalColumns, MixedFieldsSkipsSystemFields) {
    auto schema = std::make_shared<Schema>();
    // System fields (no external_field)
    schema->AddField(FieldMeta(
        FieldName("RowID"), FieldId(0), DataType::INT64, false, std::nullopt));
    schema->AddField(FieldMeta(FieldName("Timestamp"),
                               FieldId(1),
                               DataType::INT64,
                               false,
                               std::nullopt));
    // External fields
    schema->AddField(FieldMeta(FieldName("id"),
                               FieldId(100),
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "ext_id"));
    schema->AddField(FieldMeta(FieldName("vec"),
                               FieldId(101),
                               DataType::VECTOR_FLOAT,
                               8,
                               knowhere::metric::L2,
                               false,
                               std::nullopt,
                               "ext_vec"));
    schema->set_external_source("s3://bucket/data");

    auto columns = schema->GetExternalColumnNames();
    ASSERT_EQ(columns->size(), 2);
    EXPECT_EQ((*columns)[0], "ext_id");
    EXPECT_EQ((*columns)[1], "ext_vec");
}

TEST(SchemaExternalColumns, SingleFieldCollection) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldMeta(FieldName("data"),
                               FieldId(100),
                               DataType::VARCHAR,
                               1024,
                               false,
                               std::nullopt,
                               "raw_data"));
    schema->set_external_source("s3://bucket/data");

    auto columns = schema->GetExternalColumnNames();
    ASSERT_EQ(columns->size(), 1);
    EXPECT_EQ((*columns)[0], "raw_data");
}

TEST(SchemaExternalColumns, EmptySchemaReturnsEmpty) {
    auto schema = std::make_shared<Schema>();
    schema->set_external_source("s3://bucket/data");

    auto columns = schema->GetExternalColumnNames();
    ASSERT_NE(columns, nullptr);
    EXPECT_TRUE(columns->empty());
}

TEST(SchemaExternalColumns, ResolveColumnFieldIdConsistency) {
    // Verify GetExternalColumnNames returns names that ResolveColumnFieldId can resolve
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldMeta(FieldName("pk"),
                               FieldId(100),
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "parquet_pk"));
    schema->AddField(FieldMeta(FieldName("vec"),
                               FieldId(101),
                               DataType::VECTOR_FLOAT,
                               4,
                               knowhere::metric::L2,
                               false,
                               std::nullopt,
                               "parquet_vec"));
    schema->set_external_source("s3://bucket/data");

    auto columns = schema->GetExternalColumnNames();
    ASSERT_EQ(columns->size(), 2);

    // Each column name should resolve to the correct FieldId
    EXPECT_EQ(schema->ResolveColumnFieldId((*columns)[0]), FieldId(100));
    EXPECT_EQ(schema->ResolveColumnFieldId((*columns)[1]), FieldId(101));
}

// ---------------------------------------------------------------------------
// InjectExternalSpecProperties allowlist tests (Layer 3 defense-in-depth).
// The Go side (pkg/util/externalspec) is the primary filter; these tests
// verify that even if a caller bypasses Go validation, the C++ side drops
// any non-allowlisted extfs key from external_spec instead of forwarding
// it to milvus-storage.
// ---------------------------------------------------------------------------

TEST(InjectExtfsAllowlist, AllowlistedKeyIsApplied) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    std::string spec = R"({
        "format": "parquet",
        "extfs": {
            "region": "us-west-2",
            "use_ssl": "true"
        }
    })";

    ::InjectExternalSpecProperties(
        props, coll_id, "s3://s3.amazonaws.com/bucket/key", spec);

    // Layer 0 zero-initializes every field; Layer 2 overwrites the two
    // spec-provided keys. Verify the values land, not just presence
    // (presence alone would be trivially satisfied by Layer 0).
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.region")), "us-west-2");
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.use_ssl")), "true");
}

TEST(InjectExtfsAllowlist, UnknownKeyIsDropped) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    // "http_proxy" and "ld_preload" are NOT in the allowlist. A Go-side
    // bypass that manages to land either one here MUST be dropped.
    std::string spec = R"({
        "format": "parquet",
        "extfs": {
            "region": "us-west-2",
            "http_proxy": "http://evil.example.com:3128",
            "ld_preload": "/tmp/malicious.so"
        }
    })";

    ::InjectExternalSpecProperties(
        props, coll_id, "s3://s3.amazonaws.com/bucket/key", spec);

    // Allowed key landed with the spec-provided value.
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.region")), "us-west-2");
    // Dropped keys must not exist under the extfs namespace at all — Layer 0
    // only zero-initializes the 14 known fields, so these attacker-supplied
    // keys should not show up even as empty strings.
    EXPECT_EQ(props.count("extfs.42.http_proxy"), 0u);
    EXPECT_EQ(props.count("extfs.42.ld_preload"), 0u);
}

TEST(InjectExtfsAllowlist, CaseSensitiveAllowlist) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    // The allowlist is case-sensitive — an attacker cannot smuggle a secret
    // key by changing case. Only the lowercase form "access_key_id" is
    // recognised.
    std::string spec = R"({
        "format": "parquet",
        "extfs": {
            "Access_Key_Id": "AKIA_BYPASS"
        }
    })";

    ::InjectExternalSpecProperties(
        props, coll_id, "s3://s3.amazonaws.com/bucket/key", spec);

    // The non-allowlisted upper-case variant must not land under its own
    // name or be silently case-folded into the lowercase slot.
    EXPECT_EQ(props.count("extfs.42.Access_Key_Id"), 0u);
    // String fields are NOT Layer-0 zero-initialized (loon rejects empty
    // enum-constrained values); the case-variant bypass attempt must leave
    // extfs.42.access_key_id absent from the map entirely.
    EXPECT_EQ(props.count("extfs.42.access_key_id"), 0u);
}

TEST(InjectExtfsAllowlist, EmptySpecNoExtfsSection) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    // Spec without extfs makes Layer 2 a no-op. Layer 0 writes the bool-
    // valued extfs fields as "false". String fields are NOT zero-initialized
    // (loon would reject empty values on enum-constrained keys), so they
    // stay absent from the map unless Layer 1 URI-derive or Layer 2 spec
    // merge provides a value. The namespace is sparsely populated, never
    // carrying fs.* baseline leakage.
    std::string spec = R"({"format":"parquet"})";

    // Use a full Milvus-form URI so host=endpoint, path[0]=bucket.
    ::InjectExternalSpecProperties(
        props, coll_id, "s3://s3.amazonaws.com/my-bucket/key", spec);

    // Credential fields absent — spec provides none and Layer 0 does not
    // zero-init strings.
    EXPECT_EQ(props.count("extfs.42.access_key_id"), 0u);
    EXPECT_EQ(props.count("extfs.42.access_key_value"), 0u);
    EXPECT_EQ(props.count("extfs.42.region"), 0u);
    // use_iam stays "false" (the use_iam leak regression guard on C++ side).
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.use_iam")), "false");

    // URI-derived values land as expected.
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.bucket_name")),
              "my-bucket");
}

// Regression guard for the `use_iam` leak: even when the caller's Properties
// already contain a populated `fs.use_iam=true` (the Milvus-internal bucket
// uses IAM), InjectExternalSpecProperties must NOT copy it into extfs.{cid}.use_iam.
// The fix swaps Layer 1 from "copy fs.* baseline" to "zero-initialize", so
// extfs.{cid}.use_iam defaults to "false" regardless of what's in fs.*.
TEST(InjectExtfsAllowlist, NoBaselineLeakFromFsProperties) {
    milvus_storage::api::Properties props;
    // Simulate the Properties singleton carrying an IAM-based fs.* baseline.
    props["fs.use_iam"] = std::string("true");
    props["fs.access_key_id"] = std::string("MILVUS_INTERNAL_AK");
    props["fs.access_key_value"] = std::string("MILVUS_INTERNAL_SK");
    props["fs.region"] = std::string("us-west-2");

    const int64_t coll_id = 42;
    std::string spec =
        R"({"format":"parquet","extfs":{"access_key_id":"USER_AK","access_key_value":"USER_SK","region":"us-east-1"}})";

    ::InjectExternalSpecProperties(
        props, coll_id, "s3://user-bucket/key", spec);

    // use_iam MUST be false — neither fs.* nor spec sets it.
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.use_iam")), "false");
    // extfs credentials come ONLY from spec — never from fs.*.
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.access_key_id")),
              "USER_AK");
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.access_key_value")),
              "USER_SK");
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.region")), "us-east-1");
    // fs.* in the input map is left intact so the Milvus-internal bucket
    // path still authenticates correctly.
    EXPECT_EQ(std::get<std::string>(props.at("fs.use_iam")), "true");
    EXPECT_EQ(std::get<std::string>(props.at("fs.access_key_id")),
              "MILVUS_INTERNAL_AK");
}

// Azure endpoint derivation: AWS-form URI with cp=azure + region resolves via
// DeriveEndpoint to the sovereign-cloud bare authority. Swap relocates URI
// host (container) into bucket_name. AzureFileSystemProducer requires
// address to stay schemeless so its `.blob.`/`.dfs.` concatenation works.
TEST(InjectExtfsAllowlist, AzurePublicCloudWithExplicitRegion) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    std::string spec =
        R"({"format":"parquet","extfs":{"access_key_id":"myacct","access_key_value":"KEY","cloud_provider":"azure","region":"eastus"}})";

    ::InjectExternalSpecProperties(
        props, coll_id, "azure://mycontainer/data", spec);

    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.bucket_name")),
              "mycontainer");
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.address")),
              "core.windows.net");
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.cloud_provider")),
              "azure");
}

TEST(InjectExtfsAllowlist, AzureSovereignCloudEndpoints) {
    struct Case {
        std::string region;
        std::string expected_suffix;
    };
    std::vector<Case> cases = {
        {"chinanorth2", "core.chinacloudapi.cn"},
        {"chinaeast", "core.chinacloudapi.cn"},
        {"usgovvirginia", "core.usgovcloudapi.net"},
        {"usdodcentral", "core.usgovcloudapi.net"},
        {"germanynortheast", "core.cloudapi.de"},
        {"eastus", "core.windows.net"},
    };
    for (const auto& c : cases) {
        milvus_storage::api::Properties props;
        const int64_t coll_id = 42;
        std::string spec =
            R"({"format":"parquet","extfs":{"access_key_id":"myacct","access_key_value":"KEY","cloud_provider":"azure","region":")" +
            c.region + R"("}})";

        ::InjectExternalSpecProperties(
            props, coll_id, "azure://mycontainer/data", spec);

        EXPECT_EQ(std::get<std::string>(props.at("extfs.42.address")),
                  c.expected_suffix)
            << "region=" << c.region;
    }
}

TEST(InjectExtfsAllowlist, AzuriteMilvusFormURIUsesHostAsEndpoint) {
    // Azurite / Azure Stack Hub: custom endpoint expressed via Milvus-form URI
    // (path has ≥2 segments). URI.host is authoritative; cp+region are
    // signing metadata only.
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    std::string spec =
        R"({"format":"parquet","extfs":{"access_key_id":"myacct","access_key_value":"KEY","cloud_provider":"azure"}})";

    ::InjectExternalSpecProperties(
        props, coll_id, "azure://127.0.0.1:10000/mycontainer/data", spec);

    // Milvus-form: URI.host wins, no swap.
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.address")),
              "127.0.0.1:10000");
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.bucket_name")),
              "mycontainer");
}

TEST(InjectExtfsAllowlist, InvalidJsonIsHandledGracefully) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    // Structurally broken JSON (that simdjson lazily reports as an error
    // state rather than raising): simdjson's iterator pattern only throws
    // when you try to materialize a value, and `find_field("extfs")`
    // returning an error is checked before any iteration, so this input
    // falls through with no throw. Layer 0 and Layer 1 still populate the
    // extfs namespace. Layer 2 is a no-op. Go-side ParseExternalSpec
    // catches this kind of broken spec before it reaches C++; this test
    // just documents the non-throwing fallthrough.
    std::string bad_spec = "not-json{";

    EXPECT_NO_THROW(::InjectExternalSpecProperties(
        props, coll_id, "s3://s3.amazonaws.com/bucket/key", bad_spec));
    // Layer 0 still wrote zero values.
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.use_iam")), "false");
    // Layer 1 still derived bucket from URI.
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.bucket_name")),
              "bucket");
}

// Regression: URIs without a trailing slash (e.g. "s3://mybucket") must not
// crash via empty-host AssertInfo. The entire authority is the host when no
// path separator exists.
TEST(InjectExtfsAllowlist, URIWithoutTrailingSlashParsed) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    // Explicit cloud_provider=aws opts into AWS-form parsing. Without it,
    // the swap is suppressed (Milvus-form, host treated as endpoint) — see
    // ExplicitCloudProviderRequiredForAwsFormSwap below.
    std::string spec =
        R"({"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1","cloud_provider":"aws"}})";

    EXPECT_NO_THROW(
        ::InjectExternalSpecProperties(props, coll_id, "s3://mybucket", spec));
    // AWS-form path: URI.host=bucket, derived endpoint=regional S3.
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.bucket_name")),
              "mybucket");
}

// Regression: scheme-based cloud_provider inference must NOT drive the
// AWS-form swap. Self-hosted MinIO at `s3://localhost:9000/bucket/key`
// without an explicit cloud_provider must be treated as Milvus-form
// (host=endpoint, no swap). Auto-inferring cp=aws would falsely classify
// `localhost:9000` as a bucket.
TEST(InjectExtfsAllowlist, ExplicitCloudProviderRequiredForAwsFormSwap) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    std::string spec =
        R"({"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1"}})";

    ::InjectExternalSpecProperties(
        props, coll_id, "s3://localhost:9000/bucket/key", spec);

    // No cloud_provider → no swap. Layer 0 zero-init may seed empty slots,
    // but the swap path must NOT write URI.host as bucket_name (which would
    // misclassify localhost:9000 as a bucket name).
    if (props.count("extfs.42.bucket_name")) {
        EXPECT_NE(std::get<std::string>(props.at("extfs.42.bucket_name")),
                  "localhost:9000");
    }
}

// milvus-storage's fs.* property registry does not yet declare
// PROPERTY_FS_ANONYMOUS. Until upstream registers the key,
// InjectExternalSpecProperties must not zero-init extfs.{cid}.anonymous
// in Layer 0 — any SetValue on that slot triggers a strict "undefined
// key" error in ExtractExternalFsProperties on the iceberg explore path.
//
// We deliberately do NOT also drop user-supplied anonymous=true from
// Layer 2: when upstream finally registers the key, removing the Layer 0
// skip below is the only change needed for anonymous to start working.
// User-supplied anonymous=true today still surfaces the same upstream
// "undefined key" error, which is the right signal — silent no-op would
// be worse (caller would think public-bucket access is configured).
TEST(InjectExtfsAllowlist, AnonymousLayer0Skipped) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    std::string spec =
        R"({"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1"}})";

    ::InjectExternalSpecProperties(
        props, coll_id, "s3://s3.amazonaws.com/bucket/key", spec);

    // Other bool fields still get Layer 0 zero-init: sanity check the
    // skip is anonymous-specific, not a wholesale Layer 0 regression.
    // (use_ssl is excluded — Layer 1 derives it from the URI scheme and
    // overwrites the Layer 0 default.)
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.use_iam")), "false");
    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.use_virtual_host")),
              "false");
    // anonymous slot must be absent — no Layer 0 write, no SetValue.
    EXPECT_EQ(props.count("extfs.42.anonymous"), 0u);
}

TEST(InjectExtfsAllowlist, AnonymousLayer0SkippedWithStalePreseed) {
    // Pre-seed the slot to simulate stale Properties reuse. Layer 0 skip
    // must not overwrite or clear it (the skip is a continue, not a
    // SetValue("")). The pre-existing value is left as-is so the caller's
    // intent — including a future caller that has explicitly chosen to
    // route anonymous via a separate channel — survives.
    milvus_storage::api::Properties props;
    props["extfs.42.anonymous"] = std::string("true");
    const int64_t coll_id = 42;
    std::string spec =
        R"({"format":"parquet","extfs":{"access_key_id":"AK","access_key_value":"SK","region":"us-east-1"}})";

    ::InjectExternalSpecProperties(
        props, coll_id, "s3://s3.amazonaws.com/bucket/key", spec);

    EXPECT_EQ(std::get<std::string>(props.at("extfs.42.anonymous")), "true");
}

TEST(InjectExtfsAllowlist, IcebergSnapshotIDAcceptsString) {
    milvus_storage::api::Properties props;
    const int64_t coll_id = 42;
    std::string spec =
        R"({"format":"iceberg-table","snapshot_id":"5320540205222981137"})";

    ::InjectExternalSpecProperties(
        props, coll_id, "s3://s3.amazonaws.com/bucket/key", spec);

    EXPECT_EQ(std::get<std::string>(props.at("iceberg.snapshot_id")),
              "5320540205222981137");
}

// ============================================================
// Tests for NormalizeExternalArrow and internal-vs-external
// VARCHAR handling (regression for index build opt_field crash)
// ============================================================

namespace {

FieldMeta
MakeExternalFieldMetaForNormalizeTest(DataType data_type,
                                      int64_t dim,
                                      bool nullable,
                                      DataType element_type) {
    const auto name = FieldName("field");
    const auto field_id = FieldId(1000);
    const auto external_field = "external_field";
    if (data_type == DataType::VECTOR_ARRAY) {
        return FieldMeta(name,
                         field_id,
                         data_type,
                         element_type,
                         dim,
                         std::nullopt,
                         external_field);
    }
    if (IsVectorDataType(data_type)) {
        return FieldMeta(name,
                         field_id,
                         data_type,
                         dim,
                         std::nullopt,
                         nullable,
                         std::nullopt,
                         external_field);
    }
    if (IsStringDataType(data_type)) {
        return FieldMeta(name,
                         field_id,
                         data_type,
                         65535,
                         nullable,
                         std::nullopt,
                         external_field);
    }
    if (IsArrayDataType(data_type)) {
        return FieldMeta(name,
                         field_id,
                         data_type,
                         element_type,
                         nullable,
                         std::nullopt,
                         external_field);
    }
    return FieldMeta(
        name, field_id, data_type, nullable, std::nullopt, external_field);
}

std::shared_ptr<arrow::StringArray>
MakeStringArray(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& v : values) {
        auto s = builder.Append(v);
        assert(s.ok());
    }
    auto result = builder.Finish();
    assert(result.ok());
    return std::static_pointer_cast<arrow::StringArray>(result.ValueOrDie());
}

}  // namespace

// NormalizeExternalArrow converts VARCHAR STRING → BINARY for external data.
TEST(NormalizeExternalArrow, VarcharStringConvertedToBinary) {
    auto str_array = MakeStringArray({"hello", "world", "test"});
    ASSERT_EQ(str_array->type_id(), arrow::Type::STRING);

    auto field_meta = MakeExternalFieldMetaForNormalizeTest(
        milvus::DataType::VARCHAR, 0, false, milvus::DataType::NONE);
    auto result =
        milvus::storage::NormalizeExternalArrow(str_array, field_meta);

    EXPECT_EQ(result->type_id(), arrow::Type::BINARY);
    EXPECT_EQ(result->length(), 3);
}

// VARCHAR data already in BINARY format passes through unchanged.
TEST(NormalizeExternalArrow, VarcharBinaryPassthrough) {
    auto str_array = MakeStringArray({"abc", "def"});
    // Manually cast to binary (simulating internal binlog format after normalize)
    auto d = str_array->data();
    auto bin_data = arrow::ArrayData::Make(
        arrow::binary(), d->length, d->buffers, d->null_count, d->offset);
    auto bin_array = std::make_shared<arrow::BinaryArray>(bin_data);
    ASSERT_EQ(bin_array->type_id(), arrow::Type::BINARY);

    auto field_meta = MakeExternalFieldMetaForNormalizeTest(
        milvus::DataType::VARCHAR, 0, false, milvus::DataType::NONE);
    auto result =
        milvus::storage::NormalizeExternalArrow(bin_array, field_meta);

    // BINARY is not STRING, so the String→Binary branch does not fire; passthrough.
    EXPECT_EQ(result->type_id(), arrow::Type::BINARY);
    EXPECT_EQ(result.get(),
              bin_array.get());  // same pointer = true passthrough
}

// FillFieldData for VARCHAR succeeds with arrow::STRING input (internal binlog).
TEST(NormalizeExternalArrow, VarcharFillFieldDataSucceedsWithString) {
    auto str_array = MakeStringArray({"hello", "world", "foo"});
    auto chunked = std::make_shared<arrow::ChunkedArray>(str_array);

    auto field_data = std::make_shared<milvus::FieldData<std::string>>(
        milvus::DataType::VARCHAR, false, 3);

    // Use base class pointer to avoid overload ambiguity with StringArray.
    milvus::FieldDataBase* base = field_data.get();
    EXPECT_NO_THROW(base->FillFieldData(chunked));
    EXPECT_EQ(field_data->get_num_rows(), 3);
}

// FillFieldData for VARCHAR accepts arrow::BINARY input after normalize.
TEST(NormalizeExternalArrow, VarcharFillFieldDataSucceedsWithBinary) {
    auto str_array = MakeStringArray({"hello", "world"});

    // Simulate what NormalizeExternalArrow does: STRING → BINARY
    auto field_meta = MakeExternalFieldMetaForNormalizeTest(
        milvus::DataType::VARCHAR, 0, false, milvus::DataType::NONE);
    auto normalized =
        milvus::storage::NormalizeExternalArrow(str_array, field_meta);
    ASSERT_EQ(normalized->type_id(), arrow::Type::BINARY);

    auto chunked = std::make_shared<arrow::ChunkedArray>(normalized);
    auto field_data = std::make_shared<milvus::FieldData<std::string>>(
        milvus::DataType::VARCHAR, false, 2);

    // Use base class pointer to avoid overload ambiguity.
    milvus::FieldDataBase* base = field_data.get();
    EXPECT_NO_THROW(base->FillFieldData(chunked));
    EXPECT_EQ(field_data->get_num_rows(), 2);
}

// JSON STRING passes through NormalizeExternalArrow → BINARY (expected).
TEST(NormalizeExternalArrow, JsonStringConvertedToBinary) {
    auto str_array = MakeStringArray({R"({"a":1})", R"({"b":2})"});
    ASSERT_EQ(str_array->type_id(), arrow::Type::STRING);

    auto field_meta = MakeExternalFieldMetaForNormalizeTest(
        milvus::DataType::JSON, 0, false, milvus::DataType::NONE);
    auto result =
        milvus::storage::NormalizeExternalArrow(str_array, field_meta);

    EXPECT_EQ(result->type_id(), arrow::Type::BINARY);
    EXPECT_EQ(result->length(), 2);
}

// Non-matching type passes through (e.g. INT64 for INT64 field).
TEST(NormalizeExternalArrow, NonMatchingTypePassthrough) {
    arrow::Int64Builder builder;
    auto s = builder.AppendValues({1, 2, 3});
    assert(s.ok());
    auto arr = builder.Finish().ValueOrDie();

    auto field_meta = MakeExternalFieldMetaForNormalizeTest(
        milvus::DataType::INT64, 0, false, milvus::DataType::NONE);
    auto result = milvus::storage::NormalizeExternalArrow(arr, field_meta);

    EXPECT_EQ(result->type_id(), arrow::Type::INT64);
    EXPECT_EQ(result.get(), arr.get());
}
