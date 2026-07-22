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
