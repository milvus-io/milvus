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

#include <string>

#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "pb/schema.pb.h"

namespace milvus {

TEST(FieldMetaTest, NeedLoadReturnsTrueForNormalField) {
    auto field = FieldMeta(FieldName("normal_field"),
                           FieldId(100),
                           DataType::INT64,
                           false,
                           std::nullopt);
    EXPECT_TRUE(field.NeedLoad());
    EXPECT_TRUE(field.get_external_field_mapping().empty());
}

TEST(FieldMetaTest, NeedLoadReturnsFalseForExternalField) {
    auto field = FieldMeta(FieldName("external_field"),
                           FieldId(101),
                           DataType::INT64,
                           false,
                           std::nullopt,
                           "s3://bucket/path/field.parquet");
    EXPECT_FALSE(field.NeedLoad());
    EXPECT_EQ(field.get_external_field_mapping(),
              "s3://bucket/path/field.parquet");
}

TEST(FieldMetaTest, NeedLoadReturnsFalseForExternalVectorField) {
    auto field = FieldMeta(FieldName("external_vec"),
                           FieldId(102),
                           DataType::VECTOR_FLOAT,
                           128,
                           std::nullopt,
                           false,
                           std::nullopt,
                           "s3://bucket/path/vec.parquet");
    EXPECT_FALSE(field.NeedLoad());
}

TEST(FieldMetaTest, ParseFromWithExternalField) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(200);
    proto.set_name("ext_scalar");
    proto.set_data_type(milvus::proto::schema::DataType::Int64);
    proto.set_nullable(false);
    proto.set_external_field("s3://bucket/ext_scalar.parquet");

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_FALSE(field.NeedLoad());
    EXPECT_EQ(field.get_external_field_mapping(),
              "s3://bucket/ext_scalar.parquet");
}

TEST(FieldMetaTest, ParseFromWithoutExternalField) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(201);
    proto.set_name("normal_scalar");
    proto.set_data_type(milvus::proto::schema::DataType::Int64);
    proto.set_nullable(false);

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_TRUE(field.NeedLoad());
    EXPECT_TRUE(field.get_external_field_mapping().empty());
}

TEST(FieldMetaTest, RejectTypeSchemaForScalarField) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(202);
    proto.set_name("typed_scalar");
    proto.set_data_type(milvus::proto::schema::DataType::Int64);
    proto.mutable_type_schema()->set_leaf_type(
        milvus::proto::schema::DataType::Int64);

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
}

TEST(FieldMetaTest, RejectLegacyNestedArray) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(203);
    proto.set_name("legacy_nested_array");
    proto.set_data_type(milvus::proto::schema::DataType::Array);
    proto.set_element_type(milvus::proto::schema::DataType::Array);

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
}

TEST(FieldMetaTest, NestedArrayRoundTrip) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(203);
    proto.set_name("nested_array");
    proto.set_data_type(milvus::proto::schema::DataType::Array);
    proto.set_element_type(milvus::proto::schema::DataType::Array);
    auto* child = proto.mutable_type_schema()->mutable_array_element();
    child->mutable_array_element()->set_leaf_type(
        milvus::proto::schema::DataType::Int32);

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_EQ(field.get_data_type(), DataType::ARRAY);
    EXPECT_EQ(field.get_element_type(), DataType::ARRAY);
    EXPECT_TRUE(field.is_nested_array());

    auto serialized = field.ToProto();
    ASSERT_TRUE(serialized.has_type_schema());
    EXPECT_EQ(serialized.data_type(), milvus::proto::schema::DataType::Array);
    EXPECT_EQ(serialized.element_type(),
              milvus::proto::schema::DataType::Array);
    EXPECT_EQ(serialized.SerializeAsString(), proto.SerializeAsString());
}

TEST(FieldMetaTest, RejectUnsupportedNestedArrayLeafType) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(203);
    proto.set_name("nested_vector_array");
    proto.set_data_type(milvus::proto::schema::DataType::Array);
    proto.set_element_type(milvus::proto::schema::DataType::Array);
    proto.mutable_type_schema()
        ->mutable_array_element()
        ->mutable_array_element()
        ->set_leaf_type(milvus::proto::schema::DataType::FloatVector);

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
}

TEST(FieldMetaTest, NestedArrayRootNullableIsNormalizedIntoTypeSchema) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(204);
    proto.set_name("nullable_nested_array");
    proto.set_data_type(milvus::proto::schema::DataType::Array);
    proto.set_element_type(milvus::proto::schema::DataType::Array);
    proto.set_nullable(true);
    auto* child = proto.mutable_type_schema()->mutable_array_element();
    child->mutable_array_element()->set_leaf_type(
        milvus::proto::schema::DataType::Int32);

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_TRUE(field.is_nullable());
    EXPECT_TRUE(field.get_array_type_schema().nullable());

    auto serialized = field.ToProto();
    EXPECT_TRUE(serialized.nullable());
    EXPECT_TRUE(serialized.type_schema().nullable());
}

TEST(FieldMetaTest, NestedArrayRootNullableComesFromTypeSchema) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(205);
    proto.set_name("type_schema_nullable_nested_array");
    proto.set_data_type(milvus::proto::schema::DataType::Array);
    proto.set_element_type(milvus::proto::schema::DataType::Array);
    auto* type = proto.mutable_type_schema();
    type->set_nullable(true);
    type->mutable_array_element()->mutable_array_element()->set_leaf_type(
        milvus::proto::schema::DataType::Int32);

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_TRUE(field.is_nullable());
    EXPECT_TRUE(field.get_array_type_schema().nullable());
    EXPECT_TRUE(field.ToProto().nullable());
}

TEST(FieldMetaTest, RejectTypeSchemaOnlyNestedArray) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(203);
    proto.set_name("nested_array");
    auto* child = proto.mutable_type_schema()->mutable_array_element();
    child->mutable_array_element()->set_leaf_type(
        milvus::proto::schema::DataType::Int32);

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
}

TEST(FieldMetaTest, LocalFormatRoundTrip) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(202);
    proto.set_name("vortex_varchar");
    proto.set_data_type(milvus::proto::schema::DataType::VarChar);
    proto.set_nullable(true);
    auto* max_length = proto.add_type_params();
    max_length->set_key(MAX_LENGTH);
    max_length->set_value("128");
    auto* local_format = proto.add_type_params();
    local_format->set_key(LOCAL_FORMAT_KEY);
    local_format->set_value(LOCAL_FORMAT_VORTEX);

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_EQ(field.get_local_format(), LOCAL_FORMAT_VORTEX);

    auto serialized = field.ToProto();
    int local_format_count = 0;
    for (const auto& param : serialized.type_params()) {
        if (param.key() == LOCAL_FORMAT_KEY) {
            ++local_format_count;
            EXPECT_EQ(param.value(), LOCAL_FORMAT_VORTEX);
        }
    }
    EXPECT_EQ(local_format_count, 1);

    auto reparsed = FieldMeta::ParseFrom(serialized);
    EXPECT_EQ(reparsed.get_local_format(), LOCAL_FORMAT_VORTEX);
    EXPECT_EQ(reparsed.get_max_len(), 128);
}

TEST(FieldMetaTest, DecimalEnumMirrorsProto) {
    // Proto->internal conversion casts the raw enum value (see
    // FieldMeta::ParseFrom and the static_casts in query/PlanProto.cpp), so
    // the internal value must mirror schema.proto exactly. A drift here makes
    // every `case DataType::DECIMAL` branch silently unreachable.
    EXPECT_EQ(static_cast<int>(DataType::DECIMAL),
              static_cast<int>(milvus::proto::schema::DataType::Decimal));
}

TEST(FieldMetaTest, DecimalParseFromRoundTrip) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(204);
    proto.set_name("price");
    proto.set_data_type(milvus::proto::schema::DataType::Decimal);
    proto.set_nullable(true);
    auto* precision = proto.add_type_params();
    precision->set_key(DECIMAL_PRECISION);
    precision->set_value("18");
    auto* scale = proto.add_type_params();
    scale->set_key(DECIMAL_SCALE);
    scale->set_value("4");

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_EQ(field.get_data_type(), DataType::DECIMAL);
    EXPECT_EQ(field.get_decimal_precision(), 18);
    EXPECT_EQ(field.get_decimal_scale(), 4);
    EXPECT_TRUE(field.is_nullable());

    auto serialized = field.ToProto();
    EXPECT_EQ(serialized.data_type(), milvus::proto::schema::DataType::Decimal);
    int precision_count = 0;
    int scale_count = 0;
    for (const auto& param : serialized.type_params()) {
        if (param.key() == DECIMAL_PRECISION) {
            ++precision_count;
            EXPECT_EQ(param.value(), "18");
        } else if (param.key() == DECIMAL_SCALE) {
            ++scale_count;
            EXPECT_EQ(param.value(), "4");
        }
    }
    EXPECT_EQ(precision_count, 1);
    EXPECT_EQ(scale_count, 1);

    auto reparsed = FieldMeta::ParseFrom(serialized);
    EXPECT_EQ(reparsed.get_data_type(), DataType::DECIMAL);
    EXPECT_EQ(reparsed.get_decimal_precision(), 18);
    EXPECT_EQ(reparsed.get_decimal_scale(), 4);
}

TEST(FieldMetaTest, RawLocalFormatIsDefaultAndNotSerialized) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(203);
    proto.set_name("raw_scalar");
    proto.set_data_type(milvus::proto::schema::DataType::Int64);

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_EQ(field.get_local_format(), LOCAL_FORMAT_RAW);

    auto serialized = field.ToProto();
    for (const auto& param : serialized.type_params()) {
        EXPECT_NE(param.key(), LOCAL_FORMAT_KEY);
    }
}

TEST(FieldMetaTest, ShouldLoadFieldReturnsFalseForExternalField) {
    auto schema = std::make_shared<Schema>();

    // Add a normal field
    auto normal_field = FieldMeta(FieldName("normal"),
                                  FieldId(100),
                                  DataType::INT64,
                                  false,
                                  std::nullopt);
    schema->AddField(std::move(normal_field));

    // Add an external field
    auto external_field = FieldMeta(FieldName("external"),
                                    FieldId(101),
                                    DataType::INT64,
                                    false,
                                    std::nullopt,
                                    "s3://bucket/external.parquet");
    schema->AddField(std::move(external_field));

    // load_fields_ is empty, so normally all fields should load
    // But external field should NOT load
    EXPECT_TRUE(schema->ShouldLoadField(FieldId(100)));
    EXPECT_FALSE(schema->ShouldLoadField(FieldId(101)));
}

TEST(FieldMetaTest, ShouldLoadFieldExternalFieldIgnoredByLoadFields) {
    auto schema = std::make_shared<Schema>();

    auto normal_field = FieldMeta(FieldName("normal"),
                                  FieldId(100),
                                  DataType::INT64,
                                  false,
                                  std::nullopt);
    schema->AddField(std::move(normal_field));

    auto external_field = FieldMeta(FieldName("external"),
                                    FieldId(101),
                                    DataType::INT64,
                                    false,
                                    std::nullopt,
                                    "s3://bucket/external.parquet");
    schema->AddField(std::move(external_field));

    // Even if load_fields explicitly includes the external field, it should
    // still return false
    schema->UpdateLoadFields({100, 101});
    EXPECT_TRUE(schema->ShouldLoadField(FieldId(100)));
    EXPECT_FALSE(schema->ShouldLoadField(FieldId(101)));
}

TEST(FieldMetaTest, ShouldLoadFieldReturnsFalseForBM25FunctionOutput) {
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);

    auto* bm25_vector = schema_proto.add_fields();
    bm25_vector->set_fieldid(101);
    bm25_vector->set_name("sparse");
    bm25_vector->set_data_type(
        milvus::proto::schema::DataType::SparseFloatVector);
    bm25_vector->set_is_function_output(true);

    auto* function = schema_proto.add_functions();
    function->set_type(milvus::proto::schema::BM25);
    function->add_output_field_ids(101);

    auto schema = Schema::ParseFrom(schema_proto);

    EXPECT_TRUE(schema->ShouldLoadField(FieldId(100)));
    EXPECT_FALSE(schema->ShouldLoadField(FieldId(101)));

    schema->UpdateLoadFields({101});
    EXPECT_FALSE(schema->ShouldLoadField(FieldId(101)));
}

TEST(FieldMetaTest, ShouldLoadFieldIgnoresUnmarkedBM25FunctionOutput) {
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);

    auto* bm25_vector = schema_proto.add_fields();
    bm25_vector->set_fieldid(101);
    bm25_vector->set_name("sparse");
    bm25_vector->set_data_type(
        milvus::proto::schema::DataType::SparseFloatVector);

    auto* function = schema_proto.add_functions();
    function->set_type(milvus::proto::schema::BM25);
    function->add_output_field_ids(101);

    auto schema = Schema::ParseFrom(schema_proto);

    EXPECT_TRUE(schema->ShouldLoadField(FieldId(101)));
    EXPECT_FALSE(schema->is_function_output(FieldId(101)));
}

}  // namespace milvus
