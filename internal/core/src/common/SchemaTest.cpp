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
#include <stdint.h>
#include <algorithm>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include <arrow/util/key_value_metadata.h>

#include "common/Consts.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"

using namespace milvus;

class SchemaTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
    }

    std::shared_ptr<Schema> schema_;
};

TEST_F(SchemaTest, MmapEnabledNoSetting) {
    // Add a field without any mmap setting
    auto field_id = schema_->AddDebugField("test_field", DataType::INT64);
    schema_->set_primary_field_id(field_id);

    // When no mmap setting exists at any level, first should be false
    auto [has_setting, enabled] = schema_->MmapEnabled(field_id);
    EXPECT_FALSE(has_setting);
    // The enabled value is undefined when has_setting is false, so we don't check it
}

TEST_F(SchemaTest, MmapEnabledCollectionLevelEnabled) {
    // Create schema with collection-level mmap enabled via protobuf
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field = schema_proto.add_fields();
    field->set_fieldid(100);
    field->set_name("pk_field");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);

    // Set collection-level mmap enabled
    auto* prop = schema_proto.add_properties();
    prop->set_key("mmap.enabled");
    prop->set_value("true");

    auto parsed_schema = Schema::ParseFrom(schema_proto);
    FieldId pk_field_id(100);

    auto [has_setting, enabled] = parsed_schema->MmapEnabled(pk_field_id);
    EXPECT_TRUE(has_setting);
    EXPECT_TRUE(enabled);
}

TEST_F(SchemaTest, MmapEnabledCollectionLevelDisabled) {
    // Create schema with collection-level mmap disabled via protobuf
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field = schema_proto.add_fields();
    field->set_fieldid(100);
    field->set_name("pk_field");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);

    // Set collection-level mmap disabled
    auto* prop = schema_proto.add_properties();
    prop->set_key("mmap.enabled");
    prop->set_value("false");

    auto parsed_schema = Schema::ParseFrom(schema_proto);
    FieldId pk_field_id(100);

    auto [has_setting, enabled] = parsed_schema->MmapEnabled(pk_field_id);
    EXPECT_TRUE(has_setting);
    EXPECT_FALSE(enabled);
}

TEST_F(SchemaTest, MmapEnabledCollectionLevelCaseInsensitive) {
    // Test that mmap value parsing is case-insensitive
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field = schema_proto.add_fields();
    field->set_fieldid(100);
    field->set_name("pk_field");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);

    // Set collection-level mmap with uppercase TRUE
    auto* prop = schema_proto.add_properties();
    prop->set_key("mmap.enabled");
    prop->set_value("TRUE");

    auto parsed_schema = Schema::ParseFrom(schema_proto);
    FieldId pk_field_id(100);

    auto [has_setting, enabled] = parsed_schema->MmapEnabled(pk_field_id);
    EXPECT_TRUE(has_setting);
    EXPECT_TRUE(enabled);
}

TEST_F(SchemaTest, MmapEnabledFieldLevelOverridesCollectionLevel) {
    // Test that field-level mmap setting overrides collection-level setting
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field = schema_proto.add_fields();
    field->set_fieldid(100);
    field->set_name("pk_field");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);

    // Set collection-level mmap enabled
    auto* prop = schema_proto.add_properties();
    prop->set_key("mmap.enabled");
    prop->set_value("true");

    // Note: Field-level mmap settings are set via schema_proto.properties()
    // in the current implementation, which applies to all fields.
    // This test verifies the fallback behavior when no field-level override exists.

    auto parsed_schema = Schema::ParseFrom(schema_proto);
    FieldId pk_field_id(100);

    // Without field-level override, should use collection-level setting
    auto [has_setting, enabled] = parsed_schema->MmapEnabled(pk_field_id);
    EXPECT_TRUE(has_setting);
    EXPECT_TRUE(enabled);
}

TEST_F(SchemaTest, MmapEnabledNonExistentField) {
    // Test MmapEnabled with a field that doesn't exist in mmap_fields_
    // but collection-level setting exists
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field1 = schema_proto.add_fields();
    field1->set_fieldid(100);
    field1->set_name("pk_field");
    field1->set_data_type(milvus::proto::schema::DataType::Int64);
    field1->set_is_primary_key(true);

    auto* field2 = schema_proto.add_fields();
    field2->set_fieldid(101);
    field2->set_name("data_field");
    field2->set_data_type(milvus::proto::schema::DataType::Float);

    // Set collection-level mmap enabled
    auto* prop = schema_proto.add_properties();
    prop->set_key("mmap.enabled");
    prop->set_value("true");

    auto parsed_schema = Schema::ParseFrom(schema_proto);

    // Both fields should fallback to collection-level setting
    FieldId pk_field_id(100);
    auto [has_setting1, enabled1] = parsed_schema->MmapEnabled(pk_field_id);
    EXPECT_TRUE(has_setting1);
    EXPECT_TRUE(enabled1);

    FieldId data_field_id(101);
    auto [has_setting2, enabled2] = parsed_schema->MmapEnabled(data_field_id);
    EXPECT_TRUE(has_setting2);
    EXPECT_TRUE(enabled2);

    // Test with a field ID that was never added to the schema
    FieldId non_existent_field_id(999);
    auto [has_setting3, enabled3] =
        parsed_schema->MmapEnabled(non_existent_field_id);
    EXPECT_TRUE(has_setting3);  // Falls back to collection-level
    EXPECT_TRUE(enabled3);
}

TEST_F(SchemaTest, MmapEnabledMultipleFields) {
    // Test MmapEnabled with multiple fields, all using collection-level setting
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk_field");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);

    auto* int_field = schema_proto.add_fields();
    int_field->set_fieldid(101);
    int_field->set_name("int_field");
    int_field->set_data_type(milvus::proto::schema::DataType::Int32);

    auto* float_field = schema_proto.add_fields();
    float_field->set_fieldid(102);
    float_field->set_name("float_field");
    float_field->set_data_type(milvus::proto::schema::DataType::Float);

    // Set collection-level mmap disabled
    auto* prop = schema_proto.add_properties();
    prop->set_key("mmap.enabled");
    prop->set_value("false");

    auto parsed_schema = Schema::ParseFrom(schema_proto);

    // All fields should have the same collection-level setting
    for (int64_t id = 100; id <= 102; ++id) {
        FieldId field_id(id);
        auto [has_setting, enabled] = parsed_schema->MmapEnabled(field_id);
        EXPECT_TRUE(has_setting);
        EXPECT_FALSE(enabled);
    }
}

// WarmupPolicy tests

TEST_F(SchemaTest, WarmupPolicyNoSetting) {
    // Add a field without any warmup setting
    auto field_id = schema_->AddDebugField("test_field", DataType::INT64);
    schema_->set_primary_field_id(field_id);

    // When no warmup setting exists at any level, first should be false
    auto [has_setting, policy] = schema_->WarmupPolicy(
        field_id, /*is_vector=*/false, /*is_index=*/false);
    EXPECT_FALSE(has_setting);
    // The policy value is undefined when has_setting is false, so we don't check it
}

TEST_F(SchemaTest, WarmupPolicyCollectionLevelScalarField) {
    // Create schema with collection-level warmup.scalarField set to sync
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field = schema_proto.add_fields();
    field->set_fieldid(100);
    field->set_name("pk_field");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);

    // Set collection-level warmup.scalarField to sync
    auto* prop = schema_proto.add_properties();
    prop->set_key("warmup.scalarField");
    prop->set_value("sync");

    auto parsed_schema = Schema::ParseFrom(schema_proto);
    FieldId pk_field_id(100);

    // Scalar field (not index) should use warmup.scalarField
    auto [has_setting, policy] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/false, /*is_index=*/false);
    EXPECT_TRUE(has_setting);
    EXPECT_EQ(policy, "sync");

    // Scalar index should not have setting (warmup.scalarIndex not set)
    auto [has_setting_idx, policy_idx] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/false, /*is_index=*/true);
    EXPECT_FALSE(has_setting_idx);
}

TEST_F(SchemaTest, WarmupPolicyCollectionLevelVectorIndex) {
    // Create schema with collection-level warmup.vectorIndex set to disable
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field = schema_proto.add_fields();
    field->set_fieldid(100);
    field->set_name("pk_field");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);

    // Set collection-level warmup.vectorIndex to disable
    auto* prop = schema_proto.add_properties();
    prop->set_key("warmup.vectorIndex");
    prop->set_value("disable");

    auto parsed_schema = Schema::ParseFrom(schema_proto);
    FieldId pk_field_id(100);

    // Vector index should use warmup.vectorIndex
    auto [has_setting, policy] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/true, /*is_index=*/true);
    EXPECT_TRUE(has_setting);
    EXPECT_EQ(policy, "disable");

    // Vector field (not index) should not have setting
    auto [has_setting_field, policy_field] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/true, /*is_index=*/false);
    EXPECT_FALSE(has_setting_field);
}

TEST_F(SchemaTest, WarmupPolicyAllCollectionLevelSettings) {
    // Test all four collection-level warmup policies
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field = schema_proto.add_fields();
    field->set_fieldid(100);
    field->set_name("pk_field");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);

    // Set all four collection-level warmup policies
    auto* prop1 = schema_proto.add_properties();
    prop1->set_key("warmup.vectorIndex");
    prop1->set_value("sync");

    auto* prop2 = schema_proto.add_properties();
    prop2->set_key("warmup.scalarIndex");
    prop2->set_value("disable");

    auto* prop3 = schema_proto.add_properties();
    prop3->set_key("warmup.vectorField");
    prop3->set_value("sync");

    auto* prop4 = schema_proto.add_properties();
    prop4->set_key("warmup.scalarField");
    prop4->set_value("disable");

    auto parsed_schema = Schema::ParseFrom(schema_proto);
    FieldId pk_field_id(100);

    // Vector index
    auto [has1, policy1] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/true, /*is_index=*/true);
    EXPECT_TRUE(has1);
    EXPECT_EQ(policy1, "sync");

    // Scalar index
    auto [has2, policy2] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/false, /*is_index=*/true);
    EXPECT_TRUE(has2);
    EXPECT_EQ(policy2, "disable");

    // Vector field
    auto [has3, policy3] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/true, /*is_index=*/false);
    EXPECT_TRUE(has3);
    EXPECT_EQ(policy3, "sync");

    // Scalar field
    auto [has4, policy4] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/false, /*is_index=*/false);
    EXPECT_TRUE(has4);
    EXPECT_EQ(policy4, "disable");
}

TEST_F(SchemaTest, WarmupPolicyFieldLevelOverridesCollectionLevel) {
    // Test that field-level warmup policy overrides collection-level setting
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field = schema_proto.add_fields();
    field->set_fieldid(100);
    field->set_name("pk_field");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    field->set_is_primary_key(true);

    // Set field-level warmup policy to sync (key: "warmup")
    auto* field_prop = field->add_type_params();
    field_prop->set_key("warmup");
    field_prop->set_value("sync");

    // Set collection-level warmup.scalarField to disable
    auto* prop = schema_proto.add_properties();
    prop->set_key("warmup.scalarField");
    prop->set_value("disable");

    auto parsed_schema = Schema::ParseFrom(schema_proto);
    FieldId pk_field_id(100);

    // Field-level setting should override collection-level
    auto [has_setting, policy] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/false, /*is_index=*/false);
    EXPECT_TRUE(has_setting);
    EXPECT_EQ(policy, "sync");
}

TEST_F(SchemaTest, WarmupPolicyFallbackToCollectionLevel) {
    // Test that field without its own setting falls back to collection-level
    milvus::proto::schema::CollectionSchema schema_proto;

    auto* field1 = schema_proto.add_fields();
    field1->set_fieldid(100);
    field1->set_name("pk_field");
    field1->set_data_type(milvus::proto::schema::DataType::Int64);
    field1->set_is_primary_key(true);

    // Field1 has its own warmup policy (key: "warmup")
    auto* field1_prop = field1->add_type_params();
    field1_prop->set_key("warmup");
    field1_prop->set_value("sync");

    auto* field2 = schema_proto.add_fields();
    field2->set_fieldid(101);
    field2->set_name("data_field");
    field2->set_data_type(milvus::proto::schema::DataType::Float);
    // Field2 has no warmup policy, should use collection-level

    // Set collection-level warmup.scalarField policy
    auto* prop = schema_proto.add_properties();
    prop->set_key("warmup.scalarField");
    prop->set_value("disable");

    auto parsed_schema = Schema::ParseFrom(schema_proto);

    // Field1 should use its own setting
    FieldId pk_field_id(100);
    auto [has_setting1, policy1] = parsed_schema->WarmupPolicy(
        pk_field_id, /*is_vector=*/false, /*is_index=*/false);
    EXPECT_TRUE(has_setting1);
    EXPECT_EQ(policy1, "sync");

    // Field2 should fallback to collection-level
    FieldId data_field_id(101);
    auto [has_setting2, policy2] = parsed_schema->WarmupPolicy(
        data_field_id, /*is_vector=*/false, /*is_index=*/false);
    EXPECT_TRUE(has_setting2);
    EXPECT_EQ(policy2, "disable");
}

// ConvertToLoonArrowSchema tests

TEST_F(SchemaTest, ConvertToLoonArrowSchemaFieldNamesAreFieldIds) {
    auto pk_id = schema_->AddDebugField("pk_field", DataType::INT64, false);
    schema_->set_primary_field_id(pk_id);
    auto float_id =
        schema_->AddDebugField("float_field", DataType::FLOAT, false);

    auto loon_schema = schema_->ConvertToLoonArrowSchema();

    // Field names should be field ID strings, not field names
    ASSERT_EQ(loon_schema->num_fields(), 2);
    EXPECT_EQ(loon_schema->field(0)->name(), std::to_string(pk_id.get()));
    EXPECT_EQ(loon_schema->field(1)->name(), std::to_string(float_id.get()));
}

TEST_F(SchemaTest, ConvertToLoonArrowSchemaCachesTextLobBinarySchema) {
    schema_->AddDebugField("pk_field", DataType::INT64, false);
    auto text_id = schema_->AddDebugField("text_field", DataType::TEXT, true);

    auto first = schema_->ConvertToLoonArrowSchema();
    auto second = schema_->ConvertToLoonArrowSchema();
    EXPECT_NE(first.get(), second.get());

    auto lob_first =
        schema_->ConvertToLoonArrowSchema(/*text_lob_as_binary=*/true);
    auto lob_second =
        schema_->ConvertToLoonArrowSchema(/*text_lob_as_binary=*/true);
    EXPECT_EQ(lob_first.get(), lob_second.get());
    EXPECT_EQ(
        lob_first->GetFieldByName(std::to_string(text_id.get()))->type()->id(),
        arrow::Type::BINARY);

    auto float_id =
        schema_->AddDebugField("float_field", DataType::FLOAT, false);
    auto updated =
        schema_->ConvertToLoonArrowSchema(/*text_lob_as_binary=*/true);
    EXPECT_NE(lob_first.get(), updated.get());
    ASSERT_EQ(updated->num_fields(), 3);
    EXPECT_EQ(updated->field(2)->name(), std::to_string(float_id.get()));
}

TEST_F(SchemaTest, ConvertToLoonArrowSchemaVsConvertToArrowSchema) {
    auto pk_id = schema_->AddDebugField("pk_field", DataType::INT64, false);
    schema_->set_primary_field_id(pk_id);
    schema_->AddDebugField("varchar_field", DataType::VARCHAR, true);

    auto arrow_schema = schema_->ConvertToArrowSchema();
    auto loon_schema = schema_->ConvertToLoonArrowSchema();

    ASSERT_EQ(arrow_schema->num_fields(), loon_schema->num_fields());

    auto field_ids = schema_->get_field_ids();
    for (int i = 0; i < arrow_schema->num_fields(); ++i) {
        auto orig = arrow_schema->field(i);
        auto loon = loon_schema->field(i);

        // Arrow schema uses field name, Loon uses field ID string
        EXPECT_EQ(loon->name(), std::to_string(field_ids[i].get()));
        EXPECT_NE(orig->name(), loon->name());

        // Data type and nullability should be identical
        EXPECT_TRUE(orig->type()->Equals(loon->type()));
        EXPECT_EQ(orig->nullable(), loon->nullable());
    }
}

TEST_F(SchemaTest, ExternalFunctionOutputUsesFieldIdColumnName) {
    milvus::proto::schema::CollectionSchema schema_proto;
    schema_proto.set_external_source("s3://bucket/path");
    schema_proto.set_external_spec(R"({"format":"parquet"})");

    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);
    pk_field->set_external_field("id_col");

    auto* text_field = schema_proto.add_fields();
    text_field->set_fieldid(101);
    text_field->set_name("text");
    text_field->set_data_type(milvus::proto::schema::DataType::VarChar);
    text_field->set_external_field("sparse");
    auto* max_length = text_field->add_type_params();
    max_length->set_key("max_length");
    max_length->set_value("1024");

    auto* bm25_vector = schema_proto.add_fields();
    bm25_vector->set_fieldid(102);
    bm25_vector->set_name("sparse");
    bm25_vector->set_data_type(
        milvus::proto::schema::DataType::SparseFloatVector);
    bm25_vector->set_is_function_output(true);

    auto* function = schema_proto.add_functions();
    function->set_type(milvus::proto::schema::BM25);
    function->add_output_field_ids(102);

    auto schema = Schema::ParseFrom(schema_proto);

    EXPECT_EQ(schema->get_storage_column_name(FieldId(101)), "sparse");
    EXPECT_EQ(schema->get_storage_column_name(FieldId(102)), "102");
    EXPECT_EQ(schema->ResolveColumnFieldId("sparse"), FieldId(101));
    EXPECT_EQ(schema->ResolveColumnFieldId("102"), FieldId(102));

    auto parsed_field_id = ParseFieldIdColumnName("102");
    ASSERT_TRUE(parsed_field_id.has_value());
    EXPECT_EQ(parsed_field_id.value(), FieldId(102));

    const std::vector<std::string> invalid_field_id_columns = {
        "102abc",
        "-102",
        "0102",
        " 102",
        "102 ",
        "",
        "99999999999999999999",
    };
    for (const auto& column : invalid_field_id_columns) {
        EXPECT_FALSE(ParseFieldIdColumnName(column).has_value())
            << "column=" << column;
        EXPECT_THROW(schema->ResolveColumnFieldId(column), std::exception)
            << "column=" << column;
    }

    auto columns = schema->GetExternalColumnNames();
    EXPECT_NE(std::find(columns->begin(), columns->end(), "sparse"),
              columns->end());
    EXPECT_NE(std::find(columns->begin(), columns->end(), "102"),
              columns->end());
}

TEST_F(SchemaTest, ConvertToLoonArrowSchemaNullableDenseVectorUsesBinary) {
    auto pk_id = schema_->AddDebugField("pk_field", DataType::INT64, false);
    schema_->set_primary_field_id(pk_id);
    auto vector_id = schema_->AddDebugField("nullable_vector",
                                            DataType::VECTOR_FLOAT,
                                            128,
                                            knowhere::metric::L2,
                                            true);

    auto loon_schema = schema_->ConvertToLoonArrowSchema();
    auto vector_field =
        loon_schema->GetFieldByName(std::to_string(vector_id.get()));

    ASSERT_NE(vector_field, nullptr);
    EXPECT_EQ(vector_field->type()->id(), arrow::Type::BINARY);
    EXPECT_TRUE(vector_field->nullable());
    ASSERT_NE(vector_field->metadata(), nullptr);
    ASSERT_TRUE(vector_field->metadata()->Contains("dim"));
    auto dim_result = vector_field->metadata()->Get("dim");
    ASSERT_TRUE(dim_result.ok()) << dim_result.status().ToString();
    EXPECT_EQ(dim_result.ValueOrDie(), "128");
}

TEST_F(SchemaTest, ConvertToLoonArrowSchemaTextLobAsBinary) {
    auto text_id = schema_->AddDebugField("text_field", DataType::TEXT, true);
    auto varchar_id =
        schema_->AddDebugField("varchar_field", DataType::VARCHAR, true);

    auto default_schema = schema_->ConvertToLoonArrowSchema();
    auto default_text =
        default_schema->GetFieldByName(std::to_string(text_id.get()));
    ASSERT_NE(default_text, nullptr);
    EXPECT_EQ(default_text->type()->id(), arrow::Type::STRING);

    auto lob_schema =
        schema_->ConvertToLoonArrowSchema(/*text_lob_as_binary=*/true);
    auto lob_text = lob_schema->GetFieldByName(std::to_string(text_id.get()));
    auto lob_varchar =
        lob_schema->GetFieldByName(std::to_string(varchar_id.get()));

    ASSERT_NE(lob_text, nullptr);
    ASSERT_NE(lob_varchar, nullptr);
    EXPECT_EQ(lob_text->type()->id(), arrow::Type::BINARY);
    EXPECT_TRUE(lob_text->nullable());
    EXPECT_EQ(lob_varchar->type()->id(), arrow::Type::STRING);
}

TEST_F(SchemaTest,
       ExternalFunctionOutputUsesFieldFlagWithoutFunctionOutputIds) {
    milvus::proto::schema::CollectionSchema schema_proto;
    schema_proto.set_external_source("s3://bucket/path");
    schema_proto.set_external_spec(R"({"format":"parquet"})");

    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);
    pk_field->set_external_field("id_col");

    auto* text_field = schema_proto.add_fields();
    text_field->set_fieldid(101);
    text_field->set_name("text");
    text_field->set_data_type(milvus::proto::schema::DataType::VarChar);
    text_field->set_external_field("sparse");
    auto* max_length = text_field->add_type_params();
    max_length->set_key("max_length");
    max_length->set_value("1024");

    auto* bm25_vector = schema_proto.add_fields();
    bm25_vector->set_fieldid(102);
    bm25_vector->set_name("sparse");
    bm25_vector->set_data_type(
        milvus::proto::schema::DataType::SparseFloatVector);
    bm25_vector->set_is_function_output(true);

    auto* function = schema_proto.add_functions();
    function->set_type(milvus::proto::schema::BM25);
    function->add_output_field_names("sparse");

    auto schema = Schema::ParseFrom(schema_proto);

    EXPECT_EQ(schema->get_storage_column_name(FieldId(101)), "sparse");
    EXPECT_EQ(schema->get_storage_column_name(FieldId(102)), "102");
    EXPECT_EQ(schema->ResolveColumnFieldId("sparse"), FieldId(101));
    EXPECT_EQ(schema->ResolveColumnFieldId("102"), FieldId(102));

    auto columns = schema->GetExternalColumnNames();
    EXPECT_NE(std::find(columns->begin(), columns->end(), "sparse"),
              columns->end());
    EXPECT_NE(std::find(columns->begin(), columns->end(), "102"),
              columns->end());
}

TEST_F(SchemaTest, MilvusTableRealPKLoadsSourceTimestampColumn) {
    milvus::proto::schema::CollectionSchema schema_proto;
    schema_proto.set_external_source(
        "s3://bucket/snapshots/100/metadata/200.json");
    schema_proto.set_external_spec(R"({"format":"milvus-table"})");

    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(100);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);

    auto* vector_field = schema_proto.add_fields();
    vector_field->set_fieldid(101);
    vector_field->set_name("vector");
    vector_field->set_data_type(milvus::proto::schema::DataType::FloatVector);
    auto* dim = vector_field->add_type_params();
    dim->set_key("dim");
    dim->set_value("4");

    auto* ts_field = schema_proto.add_fields();
    ts_field->set_fieldid(TimestampFieldID.get());
    ts_field->set_name("Timestamp");
    ts_field->set_data_type(milvus::proto::schema::DataType::Int64);

    auto* rowid_field = schema_proto.add_fields();
    rowid_field->set_fieldid(RowFieldID.get());
    rowid_field->set_name("RowID");
    rowid_field->set_data_type(milvus::proto::schema::DataType::Int64);

    auto schema = Schema::ParseFrom(schema_proto);

    EXPECT_TRUE(schema->RequiresSourceInsertTimestamps());
    auto columns = schema->GetExternalColumnNames();
    EXPECT_NE(std::find(columns->begin(), columns->end(), "100"),
              columns->end());
    EXPECT_NE(std::find(columns->begin(), columns->end(), "101"),
              columns->end());
    EXPECT_NE(std::find(columns->begin(), columns->end(), "1"), columns->end());
    EXPECT_EQ(std::find(columns->begin(), columns->end(), "0"), columns->end());
}
