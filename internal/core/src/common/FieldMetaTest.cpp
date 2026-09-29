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

#include <string>

#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "pb/schema.pb.h"
#include "storage/Util.h"

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

TEST(FieldMetaTest, RejectNestedArrayRootNullableMismatch) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(204);
    proto.set_name("nullable_nested_array");
    proto.set_data_type(milvus::proto::schema::DataType::Array);
    proto.set_element_type(milvus::proto::schema::DataType::Array);
    proto.set_nullable(true);
    auto* child = proto.mutable_type_schema()->mutable_array_element();
    child->mutable_array_element()->set_leaf_type(
        milvus::proto::schema::DataType::Int32);

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
}

TEST(FieldMetaTest, RejectNestedArrayTypeSchemaNullableMismatch) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(205);
    proto.set_name("type_schema_nullable_nested_array");
    proto.set_data_type(milvus::proto::schema::DataType::Array);
    proto.set_element_type(milvus::proto::schema::DataType::Array);
    auto* type = proto.mutable_type_schema();
    type->set_nullable(true);
    type->mutable_array_element()->mutable_array_element()->set_leaf_type(
        milvus::proto::schema::DataType::Int32);

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
}

TEST(FieldMetaTest, NativeListSchemaMatchesArrowContract) {
    constexpr struct {
        proto::schema::DataType proto_type;
        arrow::Type::type arrow_type;
    } leaves[] = {
        {proto::schema::DataType::Bool, arrow::Type::BOOL},
        {proto::schema::DataType::Int8, arrow::Type::INT8},
        {proto::schema::DataType::Int16, arrow::Type::INT16},
        {proto::schema::DataType::Int32, arrow::Type::INT32},
        {proto::schema::DataType::Int64, arrow::Type::INT64},
        {proto::schema::DataType::Float, arrow::Type::FLOAT},
        {proto::schema::DataType::Double, arrow::Type::DOUBLE},
        {proto::schema::DataType::VarChar, arrow::Type::STRING},
    };
    for (const auto& leaf : leaves) {
        proto::schema::FieldSchema field;
        field.set_fieldid(203);
        field.set_name("native_array");
        field.set_data_type(proto::schema::DataType::Array);
        field.set_element_type(leaf.proto_type);
        field.set_nullable(true);
        field.set_element_nullable(true);
        auto meta = FieldMeta::ParseFrom(field);
        ASSERT_TRUE(meta.is_native_list_array());
        ASSERT_FALSE(meta.is_nested_array());
        ASSERT_TRUE(meta.get_array_type_schema().nullable());
        ASSERT_TRUE(meta.get_array_type_schema().array_element().nullable());
        ASSERT_FALSE(meta.ToProto().has_type_schema());
        auto type =
            std::static_pointer_cast<arrow::ListType>(GetArrowDataType(meta));
        EXPECT_EQ(type->value_field()->name(), "item");
        EXPECT_TRUE(type->value_field()->nullable());
        EXPECT_EQ(type->value_type()->id(), leaf.arrow_type);
        EXPECT_TRUE(storage::CreateArrowSchema(meta)
                        ->field(0)
                        ->type()
                        ->Equals(GetArrowDataType(meta)));
        EXPECT_TRUE(storage::CreateArrowBuilder(meta)->type()->Equals(
            GetArrowDataType(meta)));

        field.set_element_type(proto::schema::DataType::Array);
        auto* root = field.mutable_type_schema();
        root->set_nullable(true);
        root->mutable_array_element()->set_nullable(true);
        auto* nested_leaf =
            root->mutable_array_element()->mutable_array_element();
        nested_leaf->set_leaf_type(leaf.proto_type);
        nested_leaf->set_nullable(true);
        auto nested = FieldMeta::ParseFrom(field);
        auto outer =
            std::static_pointer_cast<arrow::ListType>(GetArrowDataType(nested));
        EXPECT_TRUE(outer->value_field()->nullable());
        EXPECT_EQ(outer->value_field()->name(), "item");
        auto inner =
            std::static_pointer_cast<arrow::ListType>(outer->value_type());
        EXPECT_TRUE(inner->value_field()->nullable());
        EXPECT_EQ(inner->value_field()->name(), "item");
        EXPECT_EQ(inner->value_type()->id(), leaf.arrow_type);
    }
}

TEST(FieldMetaTest, SynthesizedTypeSchemaKeepsArrayParameters) {
    proto::schema::FieldSchema field;
    field.set_fieldid(203);
    field.set_name("nullable_strings");
    field.set_data_type(proto::schema::DataType::Array);
    field.set_element_type(proto::schema::DataType::VarChar);
    field.set_nullable(true);
    field.set_element_nullable(true);
    auto* capacity = field.add_type_params();
    capacity->set_key("max_capacity");
    capacity->set_value("16");
    auto* length = field.add_type_params();
    length->set_key(MAX_LENGTH);
    length->set_value("128");

    auto meta = FieldMeta::ParseFrom(field);
    const auto& type = meta.get_array_type_schema();
    ASSERT_EQ(type.type_params_size(), 1);
    EXPECT_EQ(type.type_params(0).key(), "max_capacity");
    EXPECT_EQ(type.type_params(0).value(), "16");
    ASSERT_EQ(type.array_element().type_params_size(), 1);
    EXPECT_EQ(type.array_element().type_params(0).key(), MAX_LENGTH);
    EXPECT_EQ(type.array_element().type_params(0).value(), "128");
    EXPECT_FALSE(meta.ToProto().has_type_schema());
    EXPECT_EQ(meta.ToProto().type_params_size(), 2);
}

TEST(FieldMetaTest, NullableVectorArrayUsesBinaryItems) {
    FieldMeta nullable(FieldName("vectors"),
                       FieldId(203),
                       DataType::VECTOR_ARRAY,
                       DataType::VECTOR_FLOAT,
                       4,
                       std::nullopt,
                       false,
                       true);
    auto nullable_list =
        std::static_pointer_cast<arrow::ListType>(GetArrowDataType(nullable));
    EXPECT_EQ(nullable_list->value_type()->id(), arrow::Type::BINARY);
    EXPECT_TRUE(nullable_list->value_field()->nullable());
    EXPECT_EQ(nullable_list->value_field()->name(), "item");

    FieldMeta plain(FieldName("vectors"),
                    FieldId(203),
                    DataType::VECTOR_ARRAY,
                    DataType::VECTOR_FLOAT,
                    4,
                    std::nullopt,
                    false,
                    false);
    auto fixed_list =
        std::static_pointer_cast<arrow::ListType>(GetArrowDataType(plain));
    EXPECT_EQ(fixed_list->value_type()->id(), arrow::Type::FIXED_SIZE_BINARY);
    EXPECT_TRUE(fixed_list->value_field()->nullable());
}

TEST(FieldMetaTest, CollectionArrowSchemasUseNativeListTypes) {
    Schema schema;
    schema.AddField(FieldMeta(FieldName("struct_array[values]"),
                              FieldId(203),
                              DataType::ARRAY,
                              DataType::INT8,
                              true,
                              true,
                              std::nullopt));
    for (const auto& converted : {schema.ConvertToArrowSchema(),
                                  schema.ConvertToLoonArrowSchema(false)}) {
        ASSERT_EQ(converted->num_fields(), 1);
        const auto& field = converted->field(0);
        EXPECT_TRUE(field->nullable());
        auto list = std::static_pointer_cast<arrow::ListType>(field->type());
        EXPECT_EQ(list->value_type()->id(), arrow::Type::INT8);
        EXPECT_TRUE(list->value_field()->nullable());
        EXPECT_EQ(list->value_field()->name(), "item");
    }
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

TEST(FieldMetaTest, ElementNullableArrayRoundTrip) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(204);
    proto.set_name("nullable_elements");
    proto.set_data_type(milvus::proto::schema::DataType::Array);
    proto.set_element_type(milvus::proto::schema::DataType::Int64);
    proto.set_nullable(true);
    proto.set_element_nullable(true);

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_EQ(field.get_data_type(), DataType::ARRAY);
    EXPECT_EQ(field.get_element_type(), DataType::INT64);
    EXPECT_TRUE(field.is_nullable());
    EXPECT_TRUE(field.is_element_nullable());

    auto serialized = field.ToProto();
    EXPECT_EQ(serialized.data_type(), milvus::proto::schema::DataType::Array);
    EXPECT_EQ(serialized.element_type(),
              milvus::proto::schema::DataType::Int64);
    EXPECT_TRUE(serialized.nullable());
    EXPECT_TRUE(serialized.element_nullable());
}

TEST(FieldMetaTest, ElementNullableVectorArrayRoundTrip) {
    constexpr int64_t kDim = 16;
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(205);
    proto.set_name("nullable_vectors");
    proto.set_data_type(milvus::proto::schema::DataType::ArrayOfVector);
    proto.set_element_type(milvus::proto::schema::DataType::FloatVector);
    proto.set_nullable(true);
    proto.set_element_nullable(true);
    auto* dim = proto.add_type_params();
    dim->set_key("dim");
    dim->set_value(std::to_string(kDim));

    auto field = FieldMeta::ParseFrom(proto);
    EXPECT_EQ(field.get_data_type(), DataType::VECTOR_ARRAY);
    EXPECT_EQ(field.get_element_type(), DataType::VECTOR_FLOAT);
    EXPECT_EQ(field.get_dim(), kDim);
    EXPECT_TRUE(field.is_nullable());
    EXPECT_TRUE(field.is_element_nullable());

    auto serialized = field.ToProto();
    EXPECT_EQ(serialized.data_type(),
              milvus::proto::schema::DataType::ArrayOfVector);
    EXPECT_EQ(serialized.element_type(),
              milvus::proto::schema::DataType::FloatVector);
    EXPECT_TRUE(serialized.nullable());
    EXPECT_TRUE(serialized.element_nullable());

    auto reparsed = FieldMeta::ParseFrom(serialized);
    EXPECT_EQ(reparsed.get_dim(), kDim);
    EXPECT_TRUE(reparsed.is_element_nullable());
}

TEST(FieldMetaTest, RejectElementNullableNonArrayFields) {
    milvus::proto::schema::FieldSchema scalar;
    scalar.set_fieldid(206);
    scalar.set_name("scalar");
    scalar.set_data_type(milvus::proto::schema::DataType::Int64);
    scalar.set_element_nullable(true);
    EXPECT_ANY_THROW(FieldMeta::ParseFrom(scalar));

    milvus::proto::schema::FieldSchema vector;
    vector.set_fieldid(207);
    vector.set_name("vector");
    vector.set_data_type(milvus::proto::schema::DataType::FloatVector);
    vector.set_element_nullable(true);
    EXPECT_ANY_THROW(FieldMeta::ParseFrom(vector));
}

TEST(FieldMetaTest, RejectUnsupportedVectorArrayElementType) {
    milvus::proto::schema::FieldSchema proto;
    proto.set_fieldid(208);
    proto.set_name("nested_vectors");
    proto.set_data_type(milvus::proto::schema::DataType::ArrayOfVector);
    proto.set_element_type(milvus::proto::schema::DataType::SparseFloatVector);
    auto* dim = proto.add_type_params();
    dim->set_key("dim");
    dim->set_value("16");

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
}

TEST(FieldMetaTest, RejectVectorArrayThroughOtherConstructors) {
    EXPECT_ANY_THROW((void)FieldMeta(FieldName("vector_array"),
                                     FieldId(209),
                                     DataType::VECTOR_ARRAY,
                                     16,
                                     std::nullopt,
                                     false,
                                     std::nullopt));

    EXPECT_ANY_THROW((void)FieldMeta(FieldName("vector_array"),
                                     FieldId(210),
                                     DataType::VECTOR_ARRAY,
                                     DataType::VECTOR_FLOAT,
                                     false,
                                     false,
                                     std::nullopt));
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

TEST(FieldMetaTest, RejectThreeLevelArrayTypeSchemaAtBothEntrances) {
    proto::schema::FieldSchema proto;
    proto.set_fieldid(206);
    proto.set_name("three_level_array");
    proto.set_data_type(proto::schema::DataType::Array);
    proto.set_element_type(proto::schema::DataType::Array);
    proto.mutable_type_schema()
        ->mutable_array_element()
        ->mutable_array_element()
        ->mutable_array_element()
        ->set_leaf_type(proto::schema::DataType::Int32);

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
    EXPECT_ANY_THROW(FieldMeta(FieldName("three_level_array"),
                               FieldId(206),
                               DataType::ARRAY,
                               DataType::ARRAY,
                               false,
                               false,
                               std::nullopt,
                               "",
                               LOCAL_FORMAT_RAW,
                               proto.type_schema()));
}

TEST(FieldMetaTest, RejectNullableFloatVectorArrayLeafAtBothEntrances) {
    proto::schema::FieldSchema proto;
    proto.set_fieldid(207);
    proto.set_name("nullable_vector_leaf");
    proto.set_data_type(proto::schema::DataType::Array);
    proto.set_element_type(proto::schema::DataType::FloatVector);
    proto.set_element_nullable(true);

    EXPECT_ANY_THROW(FieldMeta::ParseFrom(proto));
    EXPECT_ANY_THROW(FieldMeta(FieldName("nullable_vector_leaf"),
                               FieldId(207),
                               DataType::ARRAY,
                               DataType::VECTOR_FLOAT,
                               false,
                               true,
                               std::nullopt));
}

}  // namespace milvus
