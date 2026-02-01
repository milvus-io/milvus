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
#include <memory>
#include <string>

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
