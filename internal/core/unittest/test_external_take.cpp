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

// Additional vector types: VECTOR_BINARY, VECTOR_FLOAT16, VECTOR_BFLOAT16,
// VECTOR_INT8.
constexpr int64_t kBinVecDim =
    16;  // binary vector dimension (must be multiple of 8)

struct VectorTypesSchemaInfo {
    SchemaPtr schema;
    FieldId int64_id;     // PK
    FieldId bin_vec_id;   // VECTOR_BINARY
    FieldId f16_vec_id;   // VECTOR_FLOAT16
    FieldId bf16_vec_id;  // VECTOR_BFLOAT16
    FieldId int8_vec_id;  // VECTOR_INT8
};

VectorTypesSchemaInfo
BuildVectorTypesSchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    VectorTypesSchemaInfo info;
    info.int64_id = FieldId(200);
    info.bin_vec_id = FieldId(205);
    info.f16_vec_id = FieldId(206);
    info.bf16_vec_id = FieldId(207);
    info.int8_vec_id = FieldId(208);

    // PK
    schema->AddField(FieldMeta(FieldName("int64_col"),
                               info.int64_id,
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "int64_col"));
    // VECTOR_BINARY (dim=16 => 2 bytes per vector)
    schema->AddField(FieldMeta(FieldName("bin_vec_col"),
                               info.bin_vec_id,
                               DataType::VECTOR_BINARY,
                               kBinVecDim,
                               knowhere::metric::HAMMING,
                               false,
                               std::nullopt,
                               "bin_vec_col"));
    // VECTOR_FLOAT16 (dim=4 => 8 bytes per vector)
    schema->AddField(FieldMeta(FieldName("f16_vec_col"),
                               info.f16_vec_id,
                               DataType::VECTOR_FLOAT16,
                               kVecDim,
                               knowhere::metric::L2,
                               false,
                               std::nullopt,
                               "f16_vec_col"));
    // VECTOR_BFLOAT16 (dim=4 => 8 bytes per vector)
    schema->AddField(FieldMeta(FieldName("bf16_vec_col"),
                               info.bf16_vec_id,
                               DataType::VECTOR_BFLOAT16,
                               kVecDim,
                               knowhere::metric::L2,
                               false,
                               std::nullopt,
                               "bf16_vec_col"));
    // VECTOR_INT8 (dim=4 => 4 bytes per vector)
    schema->AddField(FieldMeta(FieldName("int8_vec_col"),
                               info.int8_vec_id,
                               DataType::VECTOR_INT8,
                               kVecDim,
                               knowhere::metric::L2,
                               false,
                               std::nullopt,
                               "int8_vec_col"));

    schema->set_primary_field_id(info.int64_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");
    info.schema = schema;
    return info;
}

// Build a test Arrow Table with kTestRows rows for the 4 vector types + PK.
std::shared_ptr<arrow::Table>
BuildVectorTypesTable() {
    // INT64 (PK)
    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    // VECTOR_BINARY (dim=16, 2 bytes per vector)
    int bin_byte_width = kBinVecDim / 8;
    auto bin_vec_type = arrow::fixed_size_binary(bin_byte_width);
    arrow::FixedSizeBinaryBuilder bin_vec_b(bin_vec_type);
    for (int i = 0; i < kTestRows; i++) {
        uint8_t bytes[2] = {static_cast<uint8_t>(i),
                            static_cast<uint8_t>(i + 1)};
        EXPECT_TRUE(bin_vec_b.Append(bytes).ok());
    }
    auto bin_vec_arr = bin_vec_b.Finish().ValueOrDie();

    // VECTOR_FLOAT16 (dim=4, 8 bytes per vector)
    auto f16_type = arrow::fixed_size_binary(kVecDim * 2);
    arrow::FixedSizeBinaryBuilder f16_b(f16_type);
    for (int i = 0; i < kTestRows; i++) {
        uint16_t f16[kVecDim];
        for (int d = 0; d < kVecDim; d++)
            f16[d] = static_cast<uint16_t>(i * kVecDim + d);
        EXPECT_TRUE(f16_b.Append(reinterpret_cast<const uint8_t*>(f16)).ok());
    }
    auto f16_arr = f16_b.Finish().ValueOrDie();

    // VECTOR_BFLOAT16 (dim=4, 8 bytes per vector)
    auto bf16_type = arrow::fixed_size_binary(kVecDim * 2);
    arrow::FixedSizeBinaryBuilder bf16_b(bf16_type);
    for (int i = 0; i < kTestRows; i++) {
        uint16_t bf16[kVecDim];
        for (int d = 0; d < kVecDim; d++)
            bf16[d] = static_cast<uint16_t>(i * kVecDim + d + 100);
        EXPECT_TRUE(bf16_b.Append(reinterpret_cast<const uint8_t*>(bf16)).ok());
    }
    auto bf16_arr = bf16_b.Finish().ValueOrDie();

    // VECTOR_INT8 (dim=4, 4 bytes per vector)
    auto int8_vec_type = arrow::fixed_size_binary(kVecDim);
    arrow::FixedSizeBinaryBuilder int8_vec_b(int8_vec_type);
    for (int i = 0; i < kTestRows; i++) {
        int8_t vals[kVecDim];
        for (int d = 0; d < kVecDim; d++)
            vals[d] = static_cast<int8_t>(i * kVecDim + d);
        EXPECT_TRUE(
            int8_vec_b.Append(reinterpret_cast<const uint8_t*>(vals)).ok());
    }
    auto int8_vec_arr = int8_vec_b.Finish().ValueOrDie();

    auto schema = arrow::schema({
        arrow::field("int64_col", arrow::int64()),
        arrow::field("bin_vec_col", bin_vec_type),
        arrow::field("f16_vec_col", f16_type),
        arrow::field("bf16_vec_col", bf16_type),
        arrow::field("int8_vec_col", int8_vec_type),
    });

    return arrow::Table::Make(
        schema, {int64_arr, bin_vec_arr, f16_arr, bf16_arr, int8_vec_arr});
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

// Test TryTakeForRetrieve with the 4 additional vector types
TEST(ExternalTakeTest, TryTakeForRetrieve_VectorTypes) {
    auto [schema, int64_id, bin_vec_id, f16_vec_id, bf16_vec_id, int8_vec_id] =
        BuildVectorTypesSchema();
    auto table = BuildVectorTypesTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {
        int64_id, bin_vec_id, f16_vec_id, bf16_vec_id, int8_vec_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 2, 4};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);
    ASSERT_EQ(results->fields_data_size(), 5);

    // Helper: find field data by field_id
    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++) {
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        }
        return nullptr;
    };

    // Verify INT64 (PK)
    auto* pk = find_field(int64_id.get());
    ASSERT_NE(pk, nullptr);
    ASSERT_EQ(pk->scalars().long_data().data_size(), size);
    EXPECT_EQ(pk->scalars().long_data().data(0), 0);      // row 0
    EXPECT_EQ(pk->scalars().long_data().data(1), 20000);  // row 2
    EXPECT_EQ(pk->scalars().long_data().data(2), 40000);  // row 4

    // Verify VECTOR_BINARY
    auto* bin_vec = find_field(bin_vec_id.get());
    ASSERT_NE(bin_vec, nullptr);
    EXPECT_EQ(bin_vec->vectors().dim(), kBinVecDim);
    auto& bv = bin_vec->vectors().binary_vector();
    int bin_bytes = kBinVecDim / 8;  // 2 bytes per vector
    ASSERT_EQ(static_cast<int>(bv.size()), size * bin_bytes);
    // Row 0: bytes [0, 1]
    EXPECT_EQ(static_cast<uint8_t>(bv[0]), 0);
    EXPECT_EQ(static_cast<uint8_t>(bv[1]), 1);
    // Row 2: bytes [2, 3]
    EXPECT_EQ(static_cast<uint8_t>(bv[2]), 2);
    EXPECT_EQ(static_cast<uint8_t>(bv[3]), 3);

    // Verify VECTOR_FLOAT16
    auto* f16 = find_field(f16_vec_id.get());
    ASSERT_NE(f16, nullptr);
    EXPECT_EQ(f16->vectors().dim(), kVecDim);
    auto& f16v = f16->vectors().float16_vector();
    ASSERT_EQ(static_cast<int>(f16v.size()), size * kVecDim * 2);
    // Row 0: uint16 values [0, 1, 2, 3]
    auto f16_data = reinterpret_cast<const uint16_t*>(f16v.data());
    EXPECT_EQ(f16_data[0], 0);
    EXPECT_EQ(f16_data[1], 1);

    // Verify VECTOR_BFLOAT16
    auto* bf16 = find_field(bf16_vec_id.get());
    ASSERT_NE(bf16, nullptr);
    EXPECT_EQ(bf16->vectors().dim(), kVecDim);
    auto& bf16v = bf16->vectors().bfloat16_vector();
    ASSERT_EQ(static_cast<int>(bf16v.size()), size * kVecDim * 2);
    // Row 0: uint16 values [100, 101, 102, 103]
    auto bf16_data = reinterpret_cast<const uint16_t*>(bf16v.data());
    EXPECT_EQ(bf16_data[0], 100);
    EXPECT_EQ(bf16_data[1], 101);

    // Verify VECTOR_INT8
    auto* i8v = find_field(int8_vec_id.get());
    ASSERT_NE(i8v, nullptr);
    EXPECT_EQ(i8v->vectors().dim(), kVecDim);
    auto& i8data = i8v->vectors().int8_vector();
    ASSERT_EQ(static_cast<int>(i8data.size()), size * kVecDim);
    // Row 0: [0, 1, 2, 3]
    EXPECT_EQ(static_cast<int8_t>(i8data[0]), 0);
    EXPECT_EQ(static_cast<int8_t>(i8data[1]), 1);
    // Row 2: [8, 9, 10, 11]
    EXPECT_EQ(static_cast<int8_t>(i8data[kVecDim]), 8);
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

// Test TryTakeForSearch with the 4 additional vector types
TEST(ExternalTakeTest, TryTakeForSearch_VectorTypes) {
    auto [schema, int64_id, bin_vec_id, f16_vec_id, bf16_vec_id, int8_vec_id] =
        BuildVectorTypesSchema();
    auto table = BuildVectorTypesTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::Plan>(schema);
    plan->target_entries_ = {
        int64_id, bin_vec_id, f16_vec_id, bf16_vec_id, int8_vec_id};

    std::vector<int64_t> seg_offsets = {1, 3};
    int64_t size = seg_offsets.size();

    SearchResult results;
    bool ok = segment->TestTryTakeForSearch(
        plan.get(), seg_offsets.data(), size, results);
    ASSERT_TRUE(ok);
    ASSERT_EQ(results.output_fields_data_.size(), 5u);

    // Verify VECTOR_BINARY
    auto& bv_arr = results.output_fields_data_.at(bin_vec_id);
    EXPECT_EQ(bv_arr->vectors().dim(), kBinVecDim);
    auto& bvd = bv_arr->vectors().binary_vector();
    int bin_bytes = kBinVecDim / 8;
    ASSERT_EQ(static_cast<int>(bvd.size()), size * bin_bytes);
    // Row 1: bytes [1, 2]
    EXPECT_EQ(static_cast<uint8_t>(bvd[0]), 1);
    EXPECT_EQ(static_cast<uint8_t>(bvd[1]), 2);

    // Verify VECTOR_FLOAT16
    auto& f16_arr = results.output_fields_data_.at(f16_vec_id);
    EXPECT_EQ(f16_arr->vectors().dim(), kVecDim);
    ASSERT_EQ(static_cast<int>(f16_arr->vectors().float16_vector().size()),
              size * kVecDim * 2);

    // Verify VECTOR_BFLOAT16
    auto& bf16_arr = results.output_fields_data_.at(bf16_vec_id);
    EXPECT_EQ(bf16_arr->vectors().dim(), kVecDim);
    ASSERT_EQ(static_cast<int>(bf16_arr->vectors().bfloat16_vector().size()),
              size * kVecDim * 2);

    // Verify VECTOR_INT8
    auto& i8_arr = results.output_fields_data_.at(int8_vec_id);
    EXPECT_EQ(i8_arr->vectors().dim(), kVecDim);
    ASSERT_EQ(static_cast<int>(i8_arr->vectors().int8_vector().size()),
              size * kVecDim);
}

// ---------------------------------------------------------------------------
// Tests for JSON, GEOMETRY (dual Arrow-type branches), and TEXT
// ---------------------------------------------------------------------------

// Schema with JSON (binary), GEOMETRY (binary), TEXT fields.
struct ScalarExtSchemaInfo {
    SchemaPtr schema;
    FieldId int64_id;
    FieldId json_id;
    FieldId geo_id;
    FieldId text_id;
};

ScalarExtSchemaInfo
BuildScalarExtSchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    ScalarExtSchemaInfo info;
    info.int64_id = FieldId(300);
    info.json_id = FieldId(301);
    info.geo_id = FieldId(302);
    info.text_id = FieldId(303);

    schema->AddField(FieldMeta(FieldName("int64_col"),
                               info.int64_id,
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "int64_col"));
    schema->AddField(FieldMeta(FieldName("json_col"),
                               info.json_id,
                               DataType::JSON,
                               false,
                               std::nullopt,
                               "json_col"));
    schema->AddField(FieldMeta(FieldName("geo_col"),
                               info.geo_id,
                               DataType::GEOMETRY,
                               false,
                               std::nullopt,
                               "geo_col"));
    schema->AddField(FieldMeta(FieldName("text_col"),
                               info.text_id,
                               DataType::TEXT,
                               65535,
                               false,
                               std::nullopt,
                               "text_col"));

    schema->set_primary_field_id(info.int64_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");
    info.schema = schema;
    return info;
}

// Table using BinaryArray for JSON/GEOMETRY (internal binlog format).
std::shared_ptr<arrow::Table>
BuildScalarExtTable_Binary() {
    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    // JSON as binary (raw JSON bytes)
    arrow::BinaryBuilder json_b;
    for (int i = 0; i < kTestRows; i++) {
        std::string json = R"({"k":)" + std::to_string(i) + "}";
        EXPECT_TRUE(json_b.Append(json).ok());
    }
    auto json_arr = json_b.Finish().ValueOrDie();

    // GEOMETRY as binary (WKB-like bytes)
    arrow::BinaryBuilder geo_b;
    for (int i = 0; i < kTestRows; i++) {
        std::string wkt = "POINT(" + std::to_string(i) + " 0)";
        EXPECT_TRUE(geo_b.Append(wkt).ok());
    }
    auto geo_arr = geo_b.Finish().ValueOrDie();

    // TEXT as utf8
    arrow::StringBuilder text_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(text_b.Append("text_" + std::to_string(i)).ok());
    auto text_arr = text_b.Finish().ValueOrDie();

    auto schema = arrow::schema({
        arrow::field("int64_col", arrow::int64()),
        arrow::field("json_col", arrow::binary()),
        arrow::field("geo_col", arrow::binary()),
        arrow::field("text_col", arrow::utf8()),
    });
    return arrow::Table::Make(schema, {int64_arr, json_arr, geo_arr, text_arr});
}

// Table using StringArray for JSON/GEOMETRY (external Parquet format).
std::shared_ptr<arrow::Table>
BuildScalarExtTable_String() {
    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    // JSON as utf8 string
    arrow::StringBuilder json_b;
    for (int i = 0; i < kTestRows; i++) {
        std::string json = R"({"k":)" + std::to_string(i) + "}";
        EXPECT_TRUE(json_b.Append(json).ok());
    }
    auto json_arr = json_b.Finish().ValueOrDie();

    // GEOMETRY as utf8 string (WKT)
    arrow::StringBuilder geo_b;
    for (int i = 0; i < kTestRows; i++) {
        std::string wkt = "POINT(" + std::to_string(i) + " 0)";
        EXPECT_TRUE(geo_b.Append(wkt).ok());
    }
    auto geo_arr = geo_b.Finish().ValueOrDie();

    // TEXT as utf8
    arrow::StringBuilder text_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(text_b.Append("text_" + std::to_string(i)).ok());
    auto text_arr = text_b.Finish().ValueOrDie();

    auto schema = arrow::schema({
        arrow::field("int64_col", arrow::int64()),
        arrow::field("json_col", arrow::utf8()),
        arrow::field("geo_col", arrow::utf8()),
        arrow::field("text_col", arrow::utf8()),
    });
    return arrow::Table::Make(schema, {int64_arr, json_arr, geo_arr, text_arr});
}

// JSON(BinaryArray) + GEOMETRY(BinaryArray) + TEXT — internal binlog format
TEST(ExternalTakeTest, TryTakeForRetrieve_ScalarExt_BinaryPath) {
    auto [schema, int64_id, json_id, geo_id, text_id] = BuildScalarExtSchema();
    auto table = BuildScalarExtTable_Binary();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, json_id, geo_id, text_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 2, 4};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);
    ASSERT_EQ(results->fields_data_size(), 4);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++) {
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        }
        return nullptr;
    };

    // JSON
    auto* json = find_field(json_id.get());
    ASSERT_NE(json, nullptr);
    ASSERT_EQ(json->scalars().json_data().data_size(), size);
    EXPECT_EQ(json->scalars().json_data().data(0), R"({"k":0})");
    EXPECT_EQ(json->scalars().json_data().data(1), R"({"k":2})");
    EXPECT_EQ(json->scalars().json_data().data(2), R"({"k":4})");

    // GEOMETRY
    auto* geo = find_field(geo_id.get());
    ASSERT_NE(geo, nullptr);
    ASSERT_EQ(geo->scalars().geometry_data().data_size(), size);
    EXPECT_EQ(geo->scalars().geometry_data().data(0), "POINT(0 0)");
    EXPECT_EQ(geo->scalars().geometry_data().data(1), "POINT(2 0)");
    EXPECT_EQ(geo->scalars().geometry_data().data(2), "POINT(4 0)");

    // TEXT
    auto* txt = find_field(text_id.get());
    ASSERT_NE(txt, nullptr);
    ASSERT_EQ(txt->scalars().string_data().data_size(), size);
    EXPECT_EQ(txt->scalars().string_data().data(0), "text_0");
    EXPECT_EQ(txt->scalars().string_data().data(1), "text_2");
    EXPECT_EQ(txt->scalars().string_data().data(2), "text_4");
}

// JSON(StringArray) + GEOMETRY(StringArray) — external Parquet format
TEST(ExternalTakeTest, TryTakeForRetrieve_ScalarExt_StringPath) {
    auto [schema, int64_id, json_id, geo_id, text_id] = BuildScalarExtSchema();
    auto table = BuildScalarExtTable_String();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, json_id, geo_id, text_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {1, 3};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);
    ASSERT_EQ(results->fields_data_size(), 4);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++) {
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        }
        return nullptr;
    };

    // JSON via StringArray
    auto* json = find_field(json_id.get());
    ASSERT_NE(json, nullptr);
    ASSERT_EQ(json->scalars().json_data().data_size(), size);
    EXPECT_EQ(json->scalars().json_data().data(0), R"({"k":1})");
    EXPECT_EQ(json->scalars().json_data().data(1), R"({"k":3})");

    // GEOMETRY via StringArray
    auto* geo = find_field(geo_id.get());
    ASSERT_NE(geo, nullptr);
    ASSERT_EQ(geo->scalars().geometry_data().data_size(), size);
    EXPECT_EQ(geo->scalars().geometry_data().data(0), "POINT(1 0)");
    EXPECT_EQ(geo->scalars().geometry_data().data(1), "POINT(3 0)");

    // TEXT (always StringArray, same in both tests)
    auto* txt = find_field(text_id.get());
    ASSERT_NE(txt, nullptr);
    ASSERT_EQ(txt->scalars().string_data().data_size(), size);
    EXPECT_EQ(txt->scalars().string_data().data(0), "text_1");
    EXPECT_EQ(txt->scalars().string_data().data(1), "text_3");
}

// ---------------------------------------------------------------------------
// VECTOR_ARRAY (List<FixedSizeBinary>) test
// ---------------------------------------------------------------------------

// Build schema with a VECTOR_ARRAY field (element_type = VECTOR_FLOAT, dim=4).
struct VectorArraySchemaInfo {
    SchemaPtr schema;
    FieldId int64_id;
    FieldId vec_arr_id;
};

VectorArraySchemaInfo
BuildVectorArraySchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    VectorArraySchemaInfo info;
    info.int64_id = FieldId(400);
    info.vec_arr_id = FieldId(401);

    schema->AddField(FieldMeta(FieldName("int64_col"),
                               info.int64_id,
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "int64_col"));
    // VECTOR_ARRAY: element_type=VECTOR_FLOAT, dim=4
    schema->AddField(FieldMeta(FieldName("vec_arr_col"),
                               info.vec_arr_id,
                               DataType::VECTOR_ARRAY,
                               DataType::VECTOR_FLOAT,
                               kVecDim,
                               knowhere::metric::L2,
                               "vec_arr_col"));

    schema->set_primary_field_id(info.int64_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");
    info.schema = schema;
    return info;
}

// Build table with List<FixedSizeBinary(dim*4)> for VECTOR_ARRAY.
// Each row has (i+1) vectors, each vector is dim=4 floats.
std::shared_ptr<arrow::Table>
BuildVectorArrayTable() {
    // INT64 PK
    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    // VECTOR_ARRAY as List<FixedSizeBinary(dim*4)>
    int byte_width = kVecDim * sizeof(float);
    auto fsb_type = arrow::fixed_size_binary(byte_width);
    arrow::FixedSizeBinaryBuilder value_builder(fsb_type);
    arrow::Int32Builder offset_builder;
    EXPECT_TRUE(offset_builder.Append(0).ok());
    int32_t offset = 0;
    for (int i = 0; i < kTestRows; i++) {
        int num_vecs = i + 1;  // row i has (i+1) vectors
        for (int v = 0; v < num_vecs; v++) {
            // Each vector: [i*100+v*10+d] for d in 0..dim-1
            float vals[kVecDim];
            for (int d = 0; d < kVecDim; d++)
                vals[d] = static_cast<float>(i * 100 + v * 10 + d);
            EXPECT_TRUE(
                value_builder.Append(reinterpret_cast<const uint8_t*>(vals))
                    .ok());
        }
        offset += num_vecs;
        EXPECT_TRUE(offset_builder.Append(offset).ok());
    }
    auto values = value_builder.Finish().ValueOrDie();
    auto offsets = offset_builder.Finish().ValueOrDie();
    auto list_result = arrow::ListArray::FromArrays(*offsets, *values);
    EXPECT_TRUE(list_result.ok());
    auto vec_arr_arr = *list_result;

    auto schema = arrow::schema({
        arrow::field("int64_col", arrow::int64()),
        arrow::field("vec_arr_col", arrow::list(fsb_type)),
    });
    return arrow::Table::Make(schema, {int64_arr, vec_arr_arr});
}

TEST(ExternalTakeTest, TryTakeForRetrieve_VectorArrayType) {
    auto [schema, int64_id, vec_arr_id] = BuildVectorArraySchema();
    auto table = BuildVectorArrayTable();
    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, vec_arr_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 2, 4};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);
    ASSERT_EQ(results->fields_data_size(), 2);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++) {
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        }
        return nullptr;
    };

    auto* va = find_field(vec_arr_id.get());
    ASSERT_NE(va, nullptr);
    EXPECT_EQ(va->vectors().dim(), kVecDim);
    auto& va_data = va->vectors().vector_array().data();
    ASSERT_EQ(va_data.size(), size);

    // Row 0: 1 vector [0,1,2,3]
    EXPECT_EQ(va_data[0].float_vector().data_size(), kVecDim);
    EXPECT_FLOAT_EQ(va_data[0].float_vector().data(0), 0.0f);
    EXPECT_FLOAT_EQ(va_data[0].float_vector().data(3), 3.0f);

    // Row 2: 3 vectors, first=[200,201,202,203], second=[210,211,212,213]
    EXPECT_EQ(va_data[1].float_vector().data_size(), 3 * kVecDim);
    EXPECT_FLOAT_EQ(va_data[1].float_vector().data(0), 200.0f);
    EXPECT_FLOAT_EQ(va_data[1].float_vector().data(kVecDim), 210.0f);

    // Row 4: 5 vectors
    EXPECT_EQ(va_data[2].float_vector().data_size(), 5 * kVecDim);
    EXPECT_FLOAT_EQ(va_data[2].float_vector().data(0), 400.0f);
}

// ---------------------------------------------------------------------------
// TEXT type test (verifies TEXT case label reaches StringArray handler)
// ---------------------------------------------------------------------------

TEST(ExternalTakeTest, TryTakeForRetrieve_TextType) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    FieldId pk_id(500);
    FieldId text_id(501);
    schema->AddField(FieldMeta(FieldName("pk_col"),
                               pk_id,
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "pk_col"));
    schema->AddField(FieldMeta(FieldName("text_col"),
                               text_id,
                               DataType::TEXT,
                               65535,
                               false,
                               std::nullopt,
                               "text_col"));
    schema->set_primary_field_id(pk_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");

    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++) EXPECT_TRUE(int64_b.Append(i).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    arrow::StringBuilder str_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(str_b.Append("text_" + std::to_string(i)).ok());
    auto str_arr = str_b.Finish().ValueOrDie();

    auto arrow_schema = arrow::schema({
        arrow::field("pk_col", arrow::int64()),
        arrow::field("text_col", arrow::utf8()),
    });
    auto table = arrow::Table::Make(arrow_schema, {int64_arr, str_arr});

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {pk_id, text_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 3};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++) {
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        }
        return nullptr;
    };

    auto* txt = find_field(text_id.get());
    ASSERT_NE(txt, nullptr);
    ASSERT_EQ(txt->scalars().string_data().data_size(), size);
    EXPECT_EQ(txt->scalars().string_data().data(0), "text_0");
    EXPECT_EQ(txt->scalars().string_data().data(1), "text_3");
}

// ---------------------------------------------------------------------------
// TIMESTAMPTZ tests (TimestampArray + Int64Array branches)
// ---------------------------------------------------------------------------

struct TimestamptzSchemaInfo {
    SchemaPtr schema;
    FieldId int64_id;
    FieldId ts_id;
};

TimestamptzSchemaInfo
BuildTimestamptzSchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    TimestamptzSchemaInfo info;
    info.int64_id = FieldId(600);
    info.ts_id = FieldId(601);

    schema->AddField(FieldMeta(FieldName("int64_col"),
                               info.int64_id,
                               DataType::INT64,
                               false,
                               std::nullopt,
                               "int64_col"));
    schema->AddField(FieldMeta(FieldName("ts_col"),
                               info.ts_id,
                               DataType::TIMESTAMPTZ,
                               false,
                               std::nullopt,
                               "ts_col"));

    schema->set_primary_field_id(info.int64_id);
    schema->set_external_source("s3://test-bucket/data");
    schema->set_external_spec(R"({"format":"parquet"})");
    info.schema = schema;
    return info;
}

// TIMESTAMPTZ via Int64Array (internal binlog format, already microseconds)
TEST(ExternalTakeTest, TryTakeForRetrieve_Timestamptz_Int64Path) {
    auto [schema, int64_id, ts_id] = BuildTimestamptzSchema();

    arrow::Int64Builder int64_b, ts_b;
    for (int i = 0; i < kTestRows; i++) {
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
        EXPECT_TRUE(ts_b.Append(i * 1000000).ok());  // microseconds
    }
    auto int64_arr = int64_b.Finish().ValueOrDie();
    auto ts_arr = ts_b.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("int64_col", arrow::int64()),
                       arrow::field("ts_col", arrow::int64())}),
        {int64_arr, ts_arr});

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, ts_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 2, 4};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++)
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        return nullptr;
    };

    auto* ts = find_field(ts_id.get());
    ASSERT_NE(ts, nullptr);
    ASSERT_EQ(ts->scalars().timestamptz_data().data_size(), size);
    EXPECT_EQ(ts->scalars().timestamptz_data().data(0), 0);        // row 0
    EXPECT_EQ(ts->scalars().timestamptz_data().data(1), 2000000);  // row 2
    EXPECT_EQ(ts->scalars().timestamptz_data().data(2), 4000000);  // row 4
}

// TIMESTAMPTZ via TimestampArray with NANO unit (external Parquet format)
// Verifies NANO → MICRO conversion: 1e9 ns → 1e6 us
TEST(ExternalTakeTest, TryTakeForRetrieve_Timestamptz_NanosConversion) {
    auto [schema, int64_id, ts_id] = BuildTimestamptzSchema();

    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    auto ts_type = arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
    arrow::TimestampBuilder ts_b(ts_type, arrow::default_memory_pool());
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(ts_b.Append(i * 1000000000LL).ok());  // nanoseconds
    auto ts_arr = ts_b.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("int64_col", arrow::int64()),
                       arrow::field("ts_col", ts_type)}),
        {int64_arr, ts_arr});

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, ts_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 1, 3};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++)
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        return nullptr;
    };

    auto* ts = find_field(ts_id.get());
    ASSERT_NE(ts, nullptr);
    ASSERT_EQ(ts->scalars().timestamptz_data().data_size(), size);
    EXPECT_EQ(ts->scalars().timestamptz_data().data(0), 0);  // 0 ns → 0 us
    EXPECT_EQ(ts->scalars().timestamptz_data().data(1),
              1000000);  // 1e9 ns → 1e6 us
    EXPECT_EQ(ts->scalars().timestamptz_data().data(2),
              3000000);  // 3e9 ns → 3e6 us
}

// ---------------------------------------------------------------------------
// ARRAY tests (ListArray + BinaryArray branches)
// ---------------------------------------------------------------------------

struct ArraySchemaInfo {
    SchemaPtr schema;
    FieldId int64_id;
    FieldId array_id;
};

ArraySchemaInfo
BuildArraySchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);

    ArraySchemaInfo info;
    info.int64_id = FieldId(700);
    info.array_id = FieldId(701);

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

// ARRAY via Arrow ListArray (external Parquet native format: list<int32>)
TEST(ExternalTakeTest, TryTakeForRetrieve_Array_ListPath) {
    auto [schema, int64_id, array_id] = BuildArraySchema();

    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    // Build List<Int32>: row i has elements [i*10, i*10+1, i*10+2]
    arrow::Int32Builder value_builder;
    arrow::Int32Builder offset_builder;
    EXPECT_TRUE(offset_builder.Append(0).ok());
    int32_t offset = 0;
    for (int i = 0; i < kTestRows; i++) {
        for (int j = 0; j < 3; j++)
            EXPECT_TRUE(value_builder.Append(i * 10 + j).ok());
        offset += 3;
        EXPECT_TRUE(offset_builder.Append(offset).ok());
    }
    auto values = value_builder.Finish().ValueOrDie();
    auto offsets_arr = offset_builder.Finish().ValueOrDie();
    auto list_result = arrow::ListArray::FromArrays(*offsets_arr, *values);
    EXPECT_TRUE(list_result.ok());
    auto list_arr = *list_result;

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("int64_col", arrow::int64()),
                       arrow::field("array_col", arrow::list(arrow::int32()))}),
        {int64_arr, list_arr});

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, array_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {0, 2, 4};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++)
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        return nullptr;
    };

    auto* arr = find_field(array_id.get());
    ASSERT_NE(arr, nullptr);
    ASSERT_EQ(arr->scalars().array_data().data_size(), size);

    // Row 0: [0, 1, 2]
    auto& a0 = arr->scalars().array_data().data(0);
    ASSERT_EQ(a0.int_data().data_size(), 3);
    EXPECT_EQ(a0.int_data().data(0), 0);
    EXPECT_EQ(a0.int_data().data(1), 1);
    EXPECT_EQ(a0.int_data().data(2), 2);

    // Row 2: [20, 21, 22]
    auto& a2 = arr->scalars().array_data().data(1);
    EXPECT_EQ(a2.int_data().data(0), 20);
    EXPECT_EQ(a2.int_data().data(1), 21);

    // Row 4: [40, 41, 42]
    auto& a4 = arr->scalars().array_data().data(2);
    EXPECT_EQ(a4.int_data().data(0), 40);
}

// ARRAY via BinaryArray (internal binlog format: protobuf-encoded ScalarField)
TEST(ExternalTakeTest, TryTakeForRetrieve_Array_BinaryPath) {
    auto [schema, int64_id, array_id] = BuildArraySchema();

    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++)
        EXPECT_TRUE(int64_b.Append(i * 10000).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    // Build BinaryArray with proto-encoded ScalarField
    arrow::BinaryBuilder bin_b;
    for (int i = 0; i < kTestRows; i++) {
        proto::schema::ScalarField sf;
        auto* int_data = sf.mutable_int_data();
        int_data->add_data(i * 10);
        int_data->add_data(i * 10 + 1);
        int_data->add_data(i * 10 + 2);
        std::string serialized;
        sf.SerializeToString(&serialized);
        EXPECT_TRUE(bin_b.Append(serialized).ok());
    }
    auto bin_arr = bin_b.Finish().ValueOrDie();

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("int64_col", arrow::int64()),
                       arrow::field("array_col", arrow::binary())}),
        {int64_arr, bin_arr});

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, array_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    std::vector<int64_t> offsets = {1, 3};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++)
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        return nullptr;
    };

    auto* arr = find_field(array_id.get());
    ASSERT_NE(arr, nullptr);
    ASSERT_EQ(arr->scalars().array_data().data_size(), size);

    // Row 1: [10, 11, 12]
    auto& a1 = arr->scalars().array_data().data(0);
    ASSERT_EQ(a1.int_data().data_size(), 3);
    EXPECT_EQ(a1.int_data().data(0), 10);
    EXPECT_EQ(a1.int_data().data(1), 11);

    // Row 3: [30, 31, 32]
    auto& a3 = arr->scalars().array_data().data(1);
    EXPECT_EQ(a3.int_data().data(0), 30);
}

// ---------------------------------------------------------------------------
// VECTOR_ARRAY empty list row regression test (P1 bug fix)
// ---------------------------------------------------------------------------

TEST(ExternalTakeTest, TryTakeForRetrieve_VectorArray_EmptyRow) {
    auto [schema, int64_id, vec_arr_id] = BuildVectorArraySchema();

    arrow::Int64Builder int64_b;
    for (int i = 0; i < kTestRows; i++) EXPECT_TRUE(int64_b.Append(i).ok());
    auto int64_arr = int64_b.Finish().ValueOrDie();

    // Build List<FixedSizeBinary(dim*4)> with row 0 = empty, row 1 = 2 vecs
    int byte_width = kVecDim * sizeof(float);
    auto fsb_type = arrow::fixed_size_binary(byte_width);
    arrow::FixedSizeBinaryBuilder value_builder(fsb_type);
    arrow::Int32Builder offset_builder;
    EXPECT_TRUE(offset_builder.Append(0).ok());

    // Row 0: empty (0 vectors)
    EXPECT_TRUE(offset_builder.Append(0).ok());

    // Row 1: 2 vectors
    for (int v = 0; v < 2; v++) {
        float vals[kVecDim];
        for (int d = 0; d < kVecDim; d++)
            vals[d] = static_cast<float>(v * 10 + d);
        EXPECT_TRUE(
            value_builder.Append(reinterpret_cast<const uint8_t*>(vals)).ok());
    }
    EXPECT_TRUE(offset_builder.Append(2).ok());

    // Row 2-4: 1 vector each
    for (int i = 2; i < kTestRows; i++) {
        float vals[kVecDim] = {static_cast<float>(i), 0, 0, 0};
        EXPECT_TRUE(
            value_builder.Append(reinterpret_cast<const uint8_t*>(vals)).ok());
        EXPECT_TRUE(offset_builder.Append(2 + (i - 1)).ok());
    }

    auto values = value_builder.Finish().ValueOrDie();
    auto offsets_arr = offset_builder.Finish().ValueOrDie();
    auto list_result = arrow::ListArray::FromArrays(*offsets_arr, *values);
    EXPECT_TRUE(list_result.ok());
    auto vec_arr_arr = *list_result;

    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("int64_col", arrow::int64()),
                       arrow::field("vec_arr_col", arrow::list(fsb_type))}),
        {int64_arr, vec_arr_arr});

    SegmentSealedUPtr holder;
    auto* segment = CreateExternalSegment(holder, schema);
    segment->SetReaderForTesting(std::make_unique<MockTakeReader>(table));

    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->field_ids_ = {int64_id, vec_arr_id};

    auto results = std::make_unique<proto::segcore::RetrieveResults>();
    // Include row 0 (empty) and row 1 (2 vectors) — should NOT crash
    std::vector<int64_t> offsets = {0, 1};
    int64_t size = offsets.size();

    bool ok = segment->TryTakeForRetrieve(
        plan.get(), results, offsets.data(), size, false, false);
    ASSERT_TRUE(ok);

    auto find_field = [&](int64_t fid) -> const proto::schema::FieldData* {
        for (int i = 0; i < results->fields_data_size(); i++)
            if (results->fields_data(i).field_id() == fid)
                return &results->fields_data(i);
        return nullptr;
    };

    auto* va = find_field(vec_arr_id.get());
    ASSERT_NE(va, nullptr);
    auto& va_data = va->vectors().vector_array().data();
    ASSERT_EQ(va_data.size(), size);

    // Row 0: empty list → empty VectorField (0 floats)
    EXPECT_EQ(va_data[0].float_vector().data_size(), 0);

    // Row 1: 2 vectors → 2*dim floats
    EXPECT_EQ(va_data[1].float_vector().data_size(), 2 * kVecDim);
    EXPECT_FLOAT_EQ(va_data[1].float_vector().data(0), 0.0f);
    EXPECT_FLOAT_EQ(va_data[1].float_vector().data(kVecDim), 10.0f);
}

// NormalizeVectorArraysToFixedSizeBinary tests are skipped in this file
// because storage/Util.cpp requires additional runtime initialization that
// the standalone test binary does not provide.
// The LIST/FixedSizeList/passthrough/error paths are covered by the
// integration test that exercises GetFieldDatasFromManifest end-to-end.
