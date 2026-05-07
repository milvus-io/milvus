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
#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <simdjson.h>
#include <string.h>
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/FieldMeta.h"
#include "common/Geometry.h"
#include "common/Json.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "geos_c.h"
#include "gtest/gtest.h"
#include "knowhere/object.h"
#include "knowhere/operands.h"
#include "knowhere/sparse_utils.h"
#include "pb/schema.pb.h"
#include "simdjson/padded_string.h"
#include "storage/DataCodec.h"
#include "storage/IndexData.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"

using namespace milvus;

namespace {

FieldMeta
MakeExternalFieldMetaForTest(DataType data_type,
                             DataType element_type = DataType::NONE,
                             bool nullable = false,
                             int64_t dim = 0) {
    proto::schema::FieldSchema field_schema;
    field_schema.set_fieldid(1000);
    field_schema.set_name("field");
    field_schema.set_external_field("field_col");
    field_schema.set_data_type(ToProtoDataType(data_type));
    field_schema.set_nullable(nullable);
    if (element_type != DataType::NONE) {
        field_schema.set_element_type(ToProtoDataType(element_type));
    }
    if (IsStringDataType(data_type)) {
        auto* max_length = field_schema.add_type_params();
        max_length->set_key("max_length");
        max_length->set_value("1024");
    }
    if (IsVectorDataType(data_type) &&
        !IsSparseFloatVectorDataType(data_type)) {
        auto* dim_param = field_schema.add_type_params();
        dim_param->set_key("dim");
        dim_param->set_value(std::to_string(dim));
    }
    return FieldMeta::ParseFrom(field_schema);
}

}  // namespace

TEST(storage, ExternalVarCharStringNormalizesToBinaryAndFillSucceeds) {
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append("hello").ok());
    ASSERT_TRUE(builder.Append("world").ok());
    std::shared_ptr<arrow::Array> string_array;
    ASSERT_TRUE(builder.Finish(&string_array).ok());

    auto field_meta = MakeExternalFieldMetaForTest(DataType::VARCHAR);
    auto normalized = storage::NormalizeExternalArrow(string_array, field_meta);
    ASSERT_EQ(normalized->type_id(), arrow::Type::BINARY);

    auto field_data = storage::CreateFieldData(storage::DataType::VARCHAR,
                                               DataType::NONE,
                                               false,
                                               normalized->length());
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(normalized);
    EXPECT_NO_THROW(field_data->FillFieldData(chunked_array));
}

TEST(storage, ExternalVarCharBinaryFillSucceeds) {
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("hello").ok());
    ASSERT_TRUE(builder.Append("world").ok());
    std::shared_ptr<arrow::Array> binary_array;
    ASSERT_TRUE(builder.Finish(&binary_array).ok());

    auto field_meta = MakeExternalFieldMetaForTest(DataType::VARCHAR);
    auto normalized = storage::NormalizeExternalArrow(binary_array, field_meta);
    ASSERT_EQ(normalized->type_id(), arrow::Type::BINARY);

    auto field_data = storage::CreateFieldData(storage::DataType::VARCHAR,
                                               DataType::NONE,
                                               false,
                                               normalized->length());
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(normalized);
    EXPECT_NO_THROW(field_data->FillFieldData(chunked_array));
}

TEST(storage, ExternalVarCharInt64NormalizeFails) {
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(1).ok());
    ASSERT_TRUE(builder.Append(2).ok());
    std::shared_ptr<arrow::Array> int64_array;
    ASSERT_TRUE(builder.Finish(&int64_array).ok());

    auto field_meta = MakeExternalFieldMetaForTest(DataType::VARCHAR);
    EXPECT_ANY_THROW(storage::NormalizeExternalArrow(int64_array, field_meta));
}

TEST(storage, ExternalInt8Int64NormalizeFails) {
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(1).ok());
    ASSERT_TRUE(builder.Append(2).ok());
    std::shared_ptr<arrow::Array> int64_array;
    ASSERT_TRUE(builder.Finish(&int64_array).ok());

    auto field_meta = MakeExternalFieldMetaForTest(DataType::INT8);
    EXPECT_ANY_THROW(storage::NormalizeExternalArrow(int64_array, field_meta));
}

TEST(storage, ExternalSampleSchemaValidationRejectsMismatchedIntType) {
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(1).ok());
    ASSERT_TRUE(builder.Append(2).ok());
    std::shared_ptr<arrow::Array> int64_array;
    ASSERT_TRUE(builder.Finish(&int64_array).ok());

    proto::schema::FieldSchema field_schema;
    field_schema.set_fieldid(100);
    field_schema.set_name("age");
    field_schema.set_external_field("age_col");
    field_schema.set_data_type(proto::schema::DataType::Int8);
    auto field_meta = FieldMeta::ParseFrom(field_schema);

    try {
        storage::NormalizeExternalArrow(int64_array, field_meta);
        FAIL() << "expected NormalizeExternalArrow to reject mismatched type";
    } catch (const std::exception& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("field 'age'"), std::string::npos);
        EXPECT_NE(msg.find("external_field 'age_col'"), std::string::npos);
        EXPECT_NE(msg.find("field type mismatch"), std::string::npos);
        EXPECT_NE(msg.find("expected Arrow int8"), std::string::npos);
        EXPECT_NE(msg.find("actual Arrow int64"), std::string::npos);
    }
}

TEST(storage,
     ExternalSampleSchemaValidationRejectsMismatchedVectorElementType) {
    auto value_builder = std::make_shared<arrow::Int64Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
    auto& int_builder =
        dynamic_cast<arrow::Int64Builder&>(*builder.value_builder());

    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(int_builder.Append(1).ok());
    ASSERT_TRUE(int_builder.Append(2).ok());
    std::shared_ptr<arrow::Array> list_array;
    ASSERT_TRUE(builder.Finish(&list_array).ok());

    proto::schema::FieldSchema field_schema;
    field_schema.set_fieldid(101);
    field_schema.set_name("embedding");
    field_schema.set_external_field("embedding_col");
    field_schema.set_data_type(proto::schema::DataType::FloatVector);
    auto* dim = field_schema.add_type_params();
    dim->set_key("dim");
    dim->set_value("2");
    auto field_meta = FieldMeta::ParseFrom(field_schema);

    try {
        storage::NormalizeExternalArrow(list_array, field_meta);
        FAIL() << "expected NormalizeExternalArrow to reject mismatched vector "
                  "element type";
    } catch (const std::exception& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("field 'embedding'"), std::string::npos);
        EXPECT_NE(msg.find("external_field 'embedding_col'"),
                  std::string::npos);
        EXPECT_NE(msg.find("vector element type mismatch"), std::string::npos);
        EXPECT_NE(msg.find("expected float"), std::string::npos);
        EXPECT_NE(msg.find("actual int64"), std::string::npos);
    }
}

TEST(storage, ExternalSampleSchemaValidationRejectsMismatchedArrayElementType) {
    auto value_builder = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
    auto& string_builder =
        dynamic_cast<arrow::StringBuilder&>(*builder.value_builder());

    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(string_builder.Append("1").ok());
    std::shared_ptr<arrow::Array> list_array;
    ASSERT_TRUE(builder.Finish(&list_array).ok());

    proto::schema::FieldSchema field_schema;
    field_schema.set_fieldid(102);
    field_schema.set_name("tags");
    field_schema.set_external_field("tags_col");
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(proto::schema::DataType::Int64);
    auto field_meta = FieldMeta::ParseFrom(field_schema);

    try {
        storage::NormalizeExternalArrow(list_array, field_meta);
        FAIL() << "expected NormalizeExternalArrow to reject mismatched array "
                  "element type";
    } catch (const std::exception& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("field 'tags'"), std::string::npos);
        EXPECT_NE(msg.find("external_field 'tags_col'"), std::string::npos);
        EXPECT_NE(msg.find("array element type mismatch"), std::string::npos);
        EXPECT_NE(msg.find("expected INT64"), std::string::npos);
        EXPECT_NE(msg.find("actual STRING"), std::string::npos);
    }
}

TEST(storage, ExternalNonNullableScalarRejectsNullValue) {
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.Append(1).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> int64_array;
    ASSERT_TRUE(builder.Finish(&int64_array).ok());

    proto::schema::FieldSchema field_schema;
    field_schema.set_fieldid(100);
    field_schema.set_name("age");
    field_schema.set_external_field("age_col");
    field_schema.set_data_type(proto::schema::DataType::Int64);
    auto field_meta = FieldMeta::ParseFrom(field_schema);

    try {
        storage::NormalizeExternalArrow(int64_array, field_meta);
        FAIL() << "expected NormalizeExternalArrow to reject null values";
    } catch (const std::exception& e) {
        std::string message = e.what();
        EXPECT_NE(message.find("field 'age'"), std::string::npos);
        EXPECT_NE(message.find("external_field 'age_col'"), std::string::npos);
    }
}

TEST(storage, ExternalVarCharLargeStringNormalizesAndFillSucceeds) {
    arrow::LargeStringBuilder builder;
    ASSERT_TRUE(builder.Append("hello").ok());
    ASSERT_TRUE(builder.Append("world").ok());
    std::shared_ptr<arrow::Array> large_string_array;
    ASSERT_TRUE(builder.Finish(&large_string_array).ok());

    auto field_meta = MakeExternalFieldMetaForTest(DataType::VARCHAR);
    auto normalized =
        storage::NormalizeExternalArrow(large_string_array, field_meta);
    ASSERT_EQ(normalized->type_id(), arrow::Type::BINARY);

    auto field_data = storage::CreateFieldData(storage::DataType::VARCHAR,
                                               DataType::NONE,
                                               false,
                                               normalized->length());
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(normalized);
    EXPECT_NO_THROW(field_data->FillFieldData(chunked_array));
}

TEST(storage, ExternalArrayListNormalizesToBinary) {
    auto value_builder = std::make_shared<arrow::Int64Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
    auto& int_builder =
        dynamic_cast<arrow::Int64Builder&>(*builder.value_builder());

    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(int_builder.Append(1).ok());
    ASSERT_TRUE(int_builder.Append(2).ok());
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(int_builder.Append(3).ok());

    std::shared_ptr<arrow::Array> list_array;
    ASSERT_TRUE(builder.Finish(&list_array).ok());

    auto field_meta =
        MakeExternalFieldMetaForTest(DataType::ARRAY, DataType::INT64);
    auto normalized = storage::NormalizeExternalArrow(list_array, field_meta);
    ASSERT_EQ(normalized->type_id(), arrow::Type::BINARY);
}

TEST(storage, ExternalFloatVectorListNormalizesToFixedSizeBinary) {
    auto value_builder = std::make_shared<arrow::FloatBuilder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
    auto& float_builder =
        dynamic_cast<arrow::FloatBuilder&>(*builder.value_builder());

    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(float_builder.Append(1.0F).ok());
    ASSERT_TRUE(float_builder.Append(2.0F).ok());
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(float_builder.Append(3.0F).ok());
    ASSERT_TRUE(float_builder.Append(4.0F).ok());

    std::shared_ptr<arrow::Array> list_array;
    ASSERT_TRUE(builder.Finish(&list_array).ok());

    auto field_meta = MakeExternalFieldMetaForTest(
        DataType::VECTOR_FLOAT, DataType::NONE, false, 2);
    auto normalized = storage::NormalizeExternalArrow(list_array, field_meta);
    ASSERT_EQ(normalized->type_id(), arrow::Type::FIXED_SIZE_BINARY);
}

TEST(storage, ExternalNullableFloatVectorBinaryIsAccepted) {
    std::array<float, 2> row = {1.0F, 2.0F};
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder
                    .Append(reinterpret_cast<const uint8_t*>(row.data()),
                            sizeof(float) * row.size())
                    .ok());
    std::shared_ptr<arrow::Array> binary_array;
    ASSERT_TRUE(builder.Finish(&binary_array).ok());

    auto field_meta = MakeExternalFieldMetaForTest(
        DataType::VECTOR_FLOAT, DataType::NONE, true, 2);
    auto normalized = storage::NormalizeExternalArrow(binary_array, field_meta);
    ASSERT_EQ(normalized->type_id(), arrow::Type::BINARY);
}

TEST(storage, ExternalNonNullableFloatVectorRejectsNullValue) {
    auto fsb_type = arrow::fixed_size_binary(sizeof(float) * 2);
    arrow::FixedSizeBinaryBuilder builder(fsb_type);
    std::array<float, 2> row = {1.0F, 2.0F};
    ASSERT_TRUE(
        builder.Append(reinterpret_cast<const uint8_t*>(row.data())).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> vector_array;
    ASSERT_TRUE(builder.Finish(&vector_array).ok());

    proto::schema::FieldSchema field_schema;
    field_schema.set_fieldid(101);
    field_schema.set_name("embedding");
    field_schema.set_external_field("embedding_col");
    field_schema.set_data_type(proto::schema::DataType::FloatVector);
    auto* type_param = field_schema.add_type_params();
    type_param->set_key("dim");
    type_param->set_value("2");
    auto field_meta = FieldMeta::ParseFrom(field_schema);

    EXPECT_ANY_THROW(storage::NormalizeExternalArrow(vector_array, field_meta));
}

TEST(storage, ExternalBinaryVectorListNormalizeFails) {
    auto value_builder = std::make_shared<arrow::UInt8Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder);
    auto& byte_builder =
        dynamic_cast<arrow::UInt8Builder&>(*builder.value_builder());

    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(byte_builder.Append(0).ok());
    ASSERT_TRUE(byte_builder.Append(1).ok());

    std::shared_ptr<arrow::Array> list_array;
    ASSERT_TRUE(builder.Finish(&list_array).ok());

    auto field_meta = MakeExternalFieldMetaForTest(
        DataType::VECTOR_BINARY, DataType::NONE, false, 16);
    EXPECT_ANY_THROW(storage::NormalizeExternalArrow(list_array, field_meta));
}

TEST(storage, ExternalNullableVarCharBinaryPreservesNullCount) {
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("hello").ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append("world").ok());
    std::shared_ptr<arrow::Array> binary_array;
    ASSERT_TRUE(builder.Finish(&binary_array).ok());

    auto field_meta =
        MakeExternalFieldMetaForTest(DataType::VARCHAR, DataType::NONE, true);
    auto normalized = storage::NormalizeExternalArrow(binary_array, field_meta);
    ASSERT_EQ(normalized->type_id(), arrow::Type::BINARY);

    auto field_data = storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE, true, normalized->length());
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(normalized);
    ASSERT_NO_THROW(field_data->FillFieldData(chunked_array));
    EXPECT_EQ(field_data->get_null_count(), 1);
    EXPECT_EQ(field_data->get_valid_rows(), 2);
    EXPECT_FALSE(field_data->is_valid(1));
}

TEST(storage, ExternalNullableVarCharBinaryAccumulatesNullCountAcrossChunks) {
    arrow::BinaryBuilder first_builder;
    ASSERT_TRUE(first_builder.Append("hello").ok());
    ASSERT_TRUE(first_builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> first_array;
    ASSERT_TRUE(first_builder.Finish(&first_array).ok());

    arrow::BinaryBuilder second_builder;
    ASSERT_TRUE(second_builder.AppendNull().ok());
    ASSERT_TRUE(second_builder.Append("world").ok());
    std::shared_ptr<arrow::Array> second_array;
    ASSERT_TRUE(second_builder.Finish(&second_array).ok());

    auto field_meta =
        MakeExternalFieldMetaForTest(DataType::VARCHAR, DataType::NONE, true);
    auto first_normalized =
        storage::NormalizeExternalArrow(first_array, field_meta);
    auto second_normalized =
        storage::NormalizeExternalArrow(second_array, field_meta);
    ASSERT_EQ(first_normalized->type_id(), arrow::Type::BINARY);
    ASSERT_EQ(second_normalized->type_id(), arrow::Type::BINARY);

    auto field_data = storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE, true, 4);
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(
        arrow::ArrayVector{first_normalized, second_normalized});
    ASSERT_NO_THROW(field_data->FillFieldData(chunked_array));
    EXPECT_EQ(field_data->get_null_count(), 2);
    EXPECT_EQ(field_data->get_valid_rows(), 2);
    EXPECT_FALSE(field_data->is_valid(1));
    EXPECT_FALSE(field_data->is_valid(2));
}

TEST(storage, InsertDataBool) {
    FixedVector<bool> data = {true, false, true, false, true};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::BOOL, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::BOOL);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    FixedVector<bool> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataBoolNullable) {
    FixedVector<bool> data = {true, false, false, false, true};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::BOOL, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;

    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::BOOL);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 2);
    FixedVector<bool> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    // valid_data is 0001 0011, read from LSB, '1' means the according index is valid
    ASSERT_EQ(data[0], new_data[0]);
    ASSERT_EQ(data[1], new_data[1]);
    ASSERT_EQ(data[4], new_data[4]);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataInt8) {
    FixedVector<int8_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT8, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT8);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    FixedVector<int8_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataInt8Nullable) {
    FixedVector<int8_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT8, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT8);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int8_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    data = {1, 2, 0, 0, 5};
    ASSERT_EQ(data, new_data);
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataInt16) {
    FixedVector<int16_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT16, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT16);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    FixedVector<int16_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataInt16Nullable) {
    FixedVector<int16_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT16, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT16);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int16_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    data = {1, 2, 0, 0, 5};
    ASSERT_EQ(data, new_data);
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataInt32) {
    FixedVector<int32_t> data = {true, false, true, false, true};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT32, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT32);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    FixedVector<int32_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataInt32Nullable) {
    FixedVector<int32_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT32, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT32);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int32_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    data = {1, 2, 0, 0, 5};
    ASSERT_EQ(data, new_data);
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataInt64) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT64, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT64);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    FixedVector<int64_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataInt64Nullable) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT64, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT64);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int64_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    data = {1, 2, 0, 0, 5};
    ASSERT_EQ(data, new_data);
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataGeometry) {
    auto ctx = GEOS_init_r();

    // Define geometries using WKT strings directly
    const char* point_wkt = "POINT (10.25 0.55)";
    const char* linestring_wkt =
        "LINESTRING (10.25 0.55, 9.75 -0.23, -8.50 1.44)";
    const char* polygon_wkt =
        "POLYGON ((10.25 0.55, 9.75 -0.23, -8.50 1.44, 10.25 0.55))";

    std::string str1, str2, str3;
    str1 = Geometry(ctx, point_wkt).to_wkb_string();
    str2 = Geometry(ctx, linestring_wkt).to_wkb_string();
    str3 = Geometry(ctx, polygon_wkt).to_wkb_string();

    GEOS_finish_r(ctx);
    FixedVector<std::string> data = {str1, str2, str3};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::GEOMETRY, storage::DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::GEOMETRY);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<std::string> new_data(data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    for (int i = 0; i < data.size(); ++i) {
        new_data[i] =
            *static_cast<const std::string*>(new_payload->RawValue(i));
        ASSERT_EQ(new_payload->DataSize(i), data[i].size());
    }
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataGeometryNullable) {
    auto ctx = GEOS_init_r();

    // Prepare five simple point geometries in WKB format using WKT strings directly
    const char* p1_wkt = "POINT (0.0 0.0)";
    const char* p2_wkt = "POINT (1.0 1.0)";
    const char* p3_wkt = "POINT (2.0 2.0)";
    const char* p4_wkt = "POINT (3.0 3.0)";
    const char* p5_wkt = "POINT (4.0 4.0)";

    std::string str1 = Geometry(ctx, p1_wkt).to_wkb_string();
    std::string str2 = Geometry(ctx, p2_wkt).to_wkb_string();
    std::string str3 = Geometry(ctx, p3_wkt).to_wkb_string();
    std::string str4 = Geometry(ctx, p4_wkt).to_wkb_string();
    std::string str5 = Geometry(ctx, p5_wkt).to_wkb_string();

    GEOS_finish_r(ctx);

    FixedVector<std::string> data = {str1, str2, str3, str4, str5};

    // Create nullable geometry FieldData
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::GEOMETRY, storage::DataType::NONE, true);
    // valid_data bitmap: 0xF3 (11110011 b) – rows 0,1,4 valid; rows 2,3 null
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    // Round-trip the payload through InsertData serialization pipeline
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());

    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));

    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::GEOMETRY);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    // Note: current geometry serialization path writes empty string for null
    // rows and loses Arrow null-bitmap, so null_count()==0 after round-trip.

    // Expected data: original rows preserved (bitmap ignored by codec)
    FixedVector<std::string> new_data(data.size());
    for (int i = 0; i < data.size(); ++i) {
        new_data[i] =
            *static_cast<const std::string*>(new_payload->RawValue(i));
        ASSERT_EQ(new_payload->DataSize(i), data[i].size());
    }
    ASSERT_EQ(data, new_data);
}
TEST(storage, InsertDataString) {
    FixedVector<std::string> data = {
        "test1", "test2", "test3", "test4", "test5"};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VARCHAR);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<std::string> new_data(data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    for (int i = 0; i < data.size(); ++i) {
        new_data[i] =
            *static_cast<const std::string*>(new_payload->RawValue(i));
        ASSERT_EQ(new_payload->DataSize(i), data[i].size());
    }
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataStringNullable) {
    FixedVector<std::string> data = {
        "test1", "test2", "test3", "test4", "test5"};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::STRING, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::STRING);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<std::string> new_data(data.size());
    data = {"test1", "test2", "", "", "test5"};
    for (int i = 0; i < data.size(); ++i) {
        new_data[i] =
            *static_cast<const std::string*>(new_payload->RawValue(i));
        ASSERT_EQ(new_payload->DataSize(i), data[i].size());
    }
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataFloat) {
    FixedVector<float> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::FLOAT, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::FLOAT);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    FixedVector<float> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataFloatNullable) {
    FixedVector<float> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::FLOAT, DataType::NONE, true);
    std::array<uint8_t, 1> valid_data = {0xF3};
    field_data->FillFieldData(data.data(), valid_data.data(), data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::FLOAT);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<float> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    data = {1, 2, 0, 0, 5};
    ASSERT_EQ(data, new_data);
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), valid_data[0]);
}

TEST(storage, InsertDataDouble) {
    FixedVector<double> data = {1.0, 2.0, 3.0, 4.2, 5.3};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::DOUBLE, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::DOUBLE);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    FixedVector<double> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataDoubleNullable) {
    FixedVector<double> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::DOUBLE, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::DOUBLE);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<double> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    data = {1, 2, 0, 0, 5};
    ASSERT_EQ(data, new_data);
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataTimestamptz) {
    FixedVector<int64_t> data = {
        1000000000, 2000000000, 3000000000, 400000, 5000};
    auto field_data = milvus::storage::CreateFieldData(
        DataType::TIMESTAMPTZ, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::TIMESTAMPTZ);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 0);
    FixedVector<int64_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataTimestamptzNullable) {
    FixedVector<int64_t> data = {
        1000000000, 2000000000, 3000000000, 400000, 5000};
    auto field_data = milvus::storage::CreateFieldData(
        DataType::TIMESTAMPTZ, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xF3};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::TIMESTAMPTZ);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int64_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    data = {1000000000, 2000000000, 0, 0, 5000};
    ASSERT_EQ(data, new_data);
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataFloatVector) {
    std::vector<float> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 2;
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VECTOR_FLOAT, DataType::NONE, false, DIM);
    field_data->FillFieldData(data.data(), data.size() / DIM);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VECTOR_FLOAT);
    ASSERT_EQ(new_payload->get_num_rows(), data.size() / DIM);
    ASSERT_EQ(new_payload->get_null_count(), 0);
    std::vector<float> new_data(data.size());
    memcpy(new_data.data(),
           new_payload->Data(),
           new_payload->get_num_rows() * sizeof(float) * DIM);
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataFloatVectorNullable) {
    int DIM = 4;
    int num_rows = 100;

    for (int null_percent : {0, 20, 100}) {
        int valid_count = num_rows * (100 - null_percent) / 100;
        bool is_nullable = true;

        std::vector<float> data(valid_count * DIM);
        for (int i = 0; i < valid_count * DIM; ++i) {
            data[i] = static_cast<float>(i) * 0.5f;
        }

        FieldDataPtr field_data;
        std::vector<uint8_t> valid_data((num_rows + 7) / 8, 0);
        for (int i = 0; i < valid_count; ++i) {
            valid_data[i >> 3] |= (1 << (i & 0x07));
        }

        field_data = milvus::storage::CreateFieldData(
            storage::DataType::VECTOR_FLOAT, DataType::NONE, true, DIM);
        auto field_data_impl =
            std::dynamic_pointer_cast<milvus::FieldData<milvus::FloatVector>>(
                field_data);
        field_data_impl->FillFieldData(
            data.data(), valid_data.data(), num_rows, 0);

        ASSERT_EQ(field_data->get_num_rows(), num_rows);
        ASSERT_EQ(field_data->get_valid_rows(), valid_count);
        ASSERT_EQ(field_data->get_null_count(), num_rows - valid_count);
        ASSERT_EQ(field_data->IsNullable(), is_nullable);

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
        insert_data.SetFieldDataMeta(field_data_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes =
            insert_data.Serialize(storage::StorageType::Remote);
        std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                       [&](uint8_t*) {});
        auto new_insert_data = storage::DeserializeFileData(
            serialized_data_ptr, serialized_bytes.size());
        ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
        ASSERT_EQ(new_insert_data->GetTimeRage(),
                  std::make_pair(Timestamp(0), Timestamp(100)));

        auto new_payload = new_insert_data->GetFieldData();

        ASSERT_EQ(new_payload->get_data_type(),
                  storage::DataType::VECTOR_FLOAT);
        ASSERT_EQ(new_payload->get_num_rows(), num_rows);
        ASSERT_EQ(new_payload->get_valid_rows(), valid_count);
        ASSERT_EQ(new_payload->get_null_count(), num_rows - valid_count);
        ASSERT_EQ(new_payload->IsNullable(), is_nullable);

        int valid_idx = 0;
        for (int i = 0; i < num_rows; ++i) {
            if (new_payload->is_valid(i)) {
                // RawValue takes logical offset, internally converts to physical
                auto vec_ptr =
                    static_cast<const float*>(new_payload->RawValue(i));
                for (int j = 0; j < DIM; ++j) {
                    ASSERT_FLOAT_EQ(vec_ptr[j], data[valid_idx * DIM + j]);
                }
                valid_idx++;
            }
        }
    }
}

TEST(storage, InsertDataSparseFloatVector) {
    auto n_rows = 100;
    auto vecs = milvus::segcore::GenerateRandomSparseFloatVector(
        n_rows, kTestSparseDim, kTestSparseVectorDensity);
    auto field_data =
        milvus::storage::CreateFieldData(DataType::VECTOR_SPARSE_U32_F32,
                                         DataType::NONE,
                                         false,
                                         kTestSparseDim,
                                         n_rows);
    field_data->FillFieldData(vecs.get(), n_rows);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_TRUE(new_payload->get_data_type() ==
                storage::DataType::VECTOR_SPARSE_U32_F32);
    ASSERT_EQ(new_payload->get_num_rows(), n_rows);
    ASSERT_EQ(new_payload->get_null_count(), 0);
    auto new_data = static_cast<
        const knowhere::sparse::SparseRow<milvus::SparseValueType>*>(
        new_payload->Data());

    for (auto i = 0; i < n_rows; ++i) {
        auto& original = vecs[i];
        auto& new_vec = new_data[i];
        ASSERT_EQ(original.size(), new_vec.size());
        for (auto j = 0; j < original.size(); ++j) {
            ASSERT_EQ(original[j].id, new_vec[j].id);
            ASSERT_EQ(original[j].val, new_vec[j].val);
        }
    }
}

TEST(storage, InsertDataSparseFloatVectorNullable) {
    int num_rows = 100;

    for (int null_percent : {0, 20, 100}) {
        int valid_count = num_rows * (100 - null_percent) / 100;
        bool is_nullable = true;
        auto vecs = milvus::segcore::GenerateRandomSparseFloatVector(
            valid_count, kTestSparseDim, kTestSparseVectorDensity);

        FieldDataPtr field_data;
        std::vector<uint8_t> valid_data((num_rows + 7) / 8, 0);
        for (int i = 0; i < valid_count; ++i) {
            valid_data[i >> 3] |= (1 << (i & 0x07));
        }

        field_data =
            milvus::storage::CreateFieldData(DataType::VECTOR_SPARSE_U32_F32,
                                             DataType::NONE,
                                             true,
                                             kTestSparseDim,
                                             num_rows);

        auto field_data_impl = std::dynamic_pointer_cast<
            milvus::FieldData<milvus::SparseFloatVector>>(field_data);
        field_data_impl->FillFieldData(
            vecs.get(), valid_data.data(), num_rows, 0);

        ASSERT_EQ(field_data->get_num_rows(), num_rows);
        ASSERT_EQ(field_data->get_valid_rows(), valid_count);
        ASSERT_EQ(field_data->IsNullable(), is_nullable);

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
        insert_data.SetFieldDataMeta(field_data_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes =
            insert_data.Serialize(storage::StorageType::Remote);
        std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                       [&](uint8_t*) {});
        auto new_insert_data = storage::DeserializeFileData(
            serialized_data_ptr, serialized_bytes.size());
        ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);

        auto new_payload = new_insert_data->GetFieldData();
        ASSERT_TRUE(new_payload->get_data_type() ==
                    storage::DataType::VECTOR_SPARSE_U32_F32);
        ASSERT_EQ(new_payload->get_num_rows(), num_rows);
        ASSERT_EQ(new_payload->IsNullable(), is_nullable);

        int valid_idx = 0;
        for (int i = 0; i < num_rows; ++i) {
            if (new_payload->is_valid(i)) {
                auto& original = vecs[valid_idx];
                auto new_vec = static_cast<const knowhere::sparse::SparseRow<
                    milvus::SparseValueType>*>(new_payload->RawValue(i));
                ASSERT_EQ(original.size(), new_vec->size());
                for (size_t j = 0; j < original.size(); ++j) {
                    ASSERT_EQ(original[j].id, (*new_vec)[j].id);
                    ASSERT_EQ(original[j].val, (*new_vec)[j].val);
                }
                valid_idx++;
            }
        }
    }
}

TEST(storage, InsertDataBinaryVector) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 16;
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VECTOR_BINARY, DataType::NONE, false, DIM);
    field_data->FillFieldData(data.data(), data.size() * 8 / DIM);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VECTOR_BINARY);
    ASSERT_EQ(new_payload->get_num_rows(), data.size() * 8 / DIM);
    ASSERT_EQ(new_payload->get_null_count(), 0);
    std::vector<uint8_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->DataSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataBinaryVectorNullable) {
    int DIM = 128;
    int num_rows = 100;

    for (int null_percent : {0, 20, 100}) {
        int valid_count = num_rows * (100 - null_percent) / 100;
        bool is_nullable = true;

        std::vector<uint8_t> data(valid_count * DIM / 8);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = static_cast<uint8_t>(i % 256);
        }

        FieldDataPtr field_data;
        std::vector<uint8_t> valid_data((num_rows + 7) / 8, 0);
        for (int i = 0; i < valid_count; ++i) {
            valid_data[i >> 3] |= (1 << (i & 0x07));
        }

        field_data = milvus::storage::CreateFieldData(
            storage::DataType::VECTOR_BINARY, DataType::NONE, true, DIM);
        auto field_data_impl =
            std::dynamic_pointer_cast<milvus::FieldData<milvus::BinaryVector>>(
                field_data);
        field_data_impl->FillFieldData(
            data.data(), valid_data.data(), num_rows, 0);

        ASSERT_EQ(field_data->get_num_rows(), num_rows);
        ASSERT_EQ(field_data->get_valid_rows(), valid_count);
        ASSERT_EQ(field_data->IsNullable(), is_nullable);

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
        insert_data.SetFieldDataMeta(field_data_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes =
            insert_data.Serialize(storage::StorageType::Remote);
        std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                       [&](uint8_t*) {});
        auto new_insert_data = storage::DeserializeFileData(
            serialized_data_ptr, serialized_bytes.size());
        ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);

        auto new_payload = new_insert_data->GetFieldData();
        ASSERT_EQ(new_payload->get_data_type(),
                  storage::DataType::VECTOR_BINARY);
        ASSERT_EQ(new_payload->get_num_rows(), num_rows);
        ASSERT_EQ(new_payload->IsNullable(), is_nullable);

        int valid_idx = 0;
        for (int i = 0; i < num_rows; ++i) {
            if (new_payload->is_valid(i)) {
                auto vec_ptr =
                    static_cast<const uint8_t*>(new_payload->RawValue(i));
                for (int j = 0; j < DIM / 8; ++j) {
                    ASSERT_EQ(vec_ptr[j], data[valid_idx * DIM / 8 + j]);
                }
                valid_idx++;
            }
        }
    }
}

TEST(storage, InsertDataFloat16Vector) {
    std::vector<float16> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 2;
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VECTOR_FLOAT16, DataType::NONE, false, DIM);
    field_data->FillFieldData(data.data(), data.size() / DIM);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VECTOR_FLOAT16);
    ASSERT_EQ(new_payload->get_num_rows(), data.size() / DIM);
    ASSERT_EQ(new_payload->get_null_count(), 0);
    std::vector<float16> new_data(data.size());
    memcpy(new_data.data(),
           new_payload->Data(),
           new_payload->get_num_rows() * sizeof(float16) * DIM);
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataFloat16VectorNullable) {
    int DIM = 4;
    int num_rows = 100;

    for (int null_percent : {0, 20, 100}) {
        int valid_count = num_rows * (100 - null_percent) / 100;
        bool is_nullable = true;

        std::vector<float16> data(valid_count * DIM);
        for (int i = 0; i < valid_count * DIM; ++i) {
            data[i] = static_cast<float16>(i * 0.5f);
        }

        FieldDataPtr field_data;
        std::vector<uint8_t> valid_data((num_rows + 7) / 8, 0);
        for (int i = 0; i < valid_count; ++i) {
            valid_data[i >> 3] |= (1 << (i & 0x07));
        }

        field_data = milvus::storage::CreateFieldData(
            storage::DataType::VECTOR_FLOAT16, DataType::NONE, true, DIM);
        auto field_data_impl =
            std::dynamic_pointer_cast<milvus::FieldData<milvus::Float16Vector>>(
                field_data);
        field_data_impl->FillFieldData(
            data.data(), valid_data.data(), num_rows, 0);

        ASSERT_EQ(field_data->get_num_rows(), num_rows);
        ASSERT_EQ(field_data->get_valid_rows(), valid_count);
        ASSERT_EQ(field_data->IsNullable(), is_nullable);

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
        insert_data.SetFieldDataMeta(field_data_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes =
            insert_data.Serialize(storage::StorageType::Remote);
        std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                       [&](uint8_t*) {});
        auto new_insert_data = storage::DeserializeFileData(
            serialized_data_ptr, serialized_bytes.size());
        ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);

        auto new_payload = new_insert_data->GetFieldData();
        ASSERT_EQ(new_payload->get_data_type(),
                  storage::DataType::VECTOR_FLOAT16);
        ASSERT_EQ(new_payload->get_num_rows(), num_rows);
        ASSERT_EQ(new_payload->IsNullable(), is_nullable);

        int valid_idx = 0;
        for (int i = 0; i < num_rows; ++i) {
            if (new_payload->is_valid(i)) {
                auto vec_ptr =
                    static_cast<const float16*>(new_payload->RawValue(i));
                for (int j = 0; j < DIM; ++j) {
                    ASSERT_EQ(vec_ptr[j], data[valid_idx * DIM + j]);
                }
                valid_idx++;
            }
        }
    }
}

TEST(storage, IndexData) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};
    storage::IndexData index_data(data.data(), data.size());
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    index_data.SetFieldDataMeta(field_data_meta);
    index_data.SetTimestamps(0, 100);
    storage::IndexMeta index_meta{102, 103, 104, 1};
    index_data.set_index_meta(index_meta);

    auto serialized_bytes = index_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_index_data = storage::DeserializeFileData(serialized_data_ptr,
                                                       serialized_bytes.size());
    ASSERT_EQ(new_index_data->GetCodecType(), storage::IndexDataType);
    ASSERT_EQ(new_index_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    ASSERT_TRUE(new_index_data->HasBinaryPayload());
    std::vector<uint8_t> new_data(data.size());
    memcpy(new_data.data(),
           new_index_data->PayloadData(),
           new_index_data->PayloadSize());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataStringArray) {
    milvus::proto::schema::ScalarField field_string_data;
    field_string_data.mutable_string_data()->add_data("test_array1");
    field_string_data.mutable_string_data()->add_data("test_array2");
    field_string_data.mutable_string_data()->add_data("test_array3");
    field_string_data.mutable_string_data()->add_data("test_array4");
    field_string_data.mutable_string_data()->add_data("test_array5");
    auto string_array = Array(field_string_data);
    FixedVector<Array> data = {string_array};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::ARRAY, DataType::NONE, false);
    field_data->FillFieldData(data.data(), data.size());

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::ARRAY);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<Array> new_data(data.size());
    for (int i = 0; i < data.size(); ++i) {
        new_data[i] = *static_cast<const Array*>(new_payload->RawValue(i));
        ASSERT_EQ(new_payload->DataSize(i), data[i].byte_size());
        ASSERT_TRUE(data[i].operator==(new_data[i]));
    }
}

TEST(storage, InsertDataStringArrayNullable) {
    milvus::proto::schema::ScalarField field_string_data;
    field_string_data.mutable_string_data()->add_data("test_array1");
    field_string_data.mutable_string_data()->add_data("test_array2");
    field_string_data.mutable_string_data()->add_data("test_array3");
    field_string_data.mutable_string_data()->add_data("test_array4");
    field_string_data.mutable_string_data()->add_data("test_array5");
    auto string_array = Array(field_string_data);
    milvus::proto::schema::ScalarField field_int_data;
    field_string_data.mutable_int_data()->add_data(1);
    field_string_data.mutable_int_data()->add_data(2);
    field_string_data.mutable_int_data()->add_data(3);
    field_string_data.mutable_int_data()->add_data(4);
    field_string_data.mutable_int_data()->add_data(5);
    auto int_array = Array(field_int_data);
    FixedVector<Array> data = {string_array, int_array};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::ARRAY, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xFD};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::ARRAY);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 1);
    FixedVector<Array> expected_data = {string_array, Array()};
    FixedVector<Array> new_data(data.size());
    for (int i = 0; i < data.size(); ++i) {
        new_data[i] = *static_cast<const Array*>(new_payload->RawValue(i));
        ASSERT_EQ(new_payload->DataSize(i), data[i].byte_size());
        ASSERT_TRUE(expected_data[i].operator==(new_data[i]));
    }
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataJsonNullable) {
    FixedVector<Json> data = {Json(),
                              Json(simdjson::padded_string(std::string("A")))};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::JSON, DataType::NONE, true);
    uint8_t valid_data_storage[1] = {0xFC};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::JSON);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    ASSERT_EQ(new_payload->get_null_count(), 2);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}

TEST(storage, InsertDataJsonFillWithNull) {
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::JSON, DataType::NONE, true);
    int64_t size = 2;
    uint8_t valid_data_storage[1] = {0xFC};
    uint8_t* valid_data = valid_data_storage;
    field_data->FillFieldData(std::nullopt, size);

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::JSON);
    ASSERT_EQ(new_payload->get_num_rows(), size);
    ASSERT_EQ(new_payload->get_null_count(), size);
    ASSERT_EQ(*new_payload->ValidData(), *valid_data);
}
