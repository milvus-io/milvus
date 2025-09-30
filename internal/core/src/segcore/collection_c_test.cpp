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

#include "knowhere/comp/index_param.h"
#include "segcore/collection_c.h"
#include "test_utils/c_api_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

TEST(CApiTest, CollectionTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    DeleteCollection(collection);
}

TEST(CApiTest, UpdateSchemaTest) {
    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    // CCollection
    auto collection = NewCollection(schema_string.c_str());

    // create updated schema with extra field
    namespace schema = milvus::proto::schema;
    schema::CollectionSchema collection_schema;
    collection_schema.set_name("collection_test");

    auto vec_field_schema = collection_schema.add_fields();
    vec_field_schema->set_name("fakevec");
    vec_field_schema->set_fieldid(100);
    vec_field_schema->set_data_type(schema::DataType::FloatVector);
    auto metric_type_param = vec_field_schema->add_index_params();
    metric_type_param->set_key("metric_type");
    metric_type_param->set_value(knowhere::metric::L2);
    auto dim_param = vec_field_schema->add_type_params();
    dim_param->set_key("dim");
    dim_param->set_value(std::to_string(DIM));

    auto other_field_schema = collection_schema.add_fields();
    other_field_schema->set_name("counter");
    other_field_schema->set_fieldid(101);
    other_field_schema->set_data_type(schema::DataType::Int64);
    other_field_schema->set_is_primary_key(true);

    auto other_field_schema2 = collection_schema.add_fields();
    other_field_schema2->set_name("doubleField");
    other_field_schema2->set_fieldid(102);
    other_field_schema2->set_data_type(schema::DataType::Double);

    auto added_field = collection_schema.add_fields();
    added_field->set_name("added_field");
    added_field->set_fieldid(103);
    added_field->set_data_type(schema::DataType::Int64);
    added_field->set_nullable(true);

    std::string updated_schema_string;
    auto marshal = collection_schema.SerializeToString(&updated_schema_string);
    ASSERT_TRUE(marshal);

    // Call UpdateSchema CApi here
    // UpdateSchema(CCollection, const void*, const int64_t)
    auto status = UpdateSchema(collection,
                               updated_schema_string.c_str(),
                               updated_schema_string.length(),
                               100);
    ASSERT_EQ(status.error_code, Success);

    auto col = static_cast<milvus::segcore::Collection*>(collection);
    auto updated_schema = col->get_schema();
    auto add_field = updated_schema->operator[](FieldId(103));

    ASSERT_EQ(add_field.get_name().get(), "added_field");

    // Test failure case, no panicking with failure code
    status = UpdateSchema(collection, nullptr, 0, 200);
    ASSERT_NE(status.error_code, Success);

    DeleteCollection(collection);
    // free error msg, which shall be responsible for go side to call C.free()
    free(const_cast<char*>(status.error_msg));
}

TEST(CApiTest, SetIndexMetaTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());

    milvus::proto::segcore::CollectionIndexMeta indexMeta;
    indexMeta.ParseFromString(get_default_index_meta());
    char buffer[indexMeta.ByteSizeLong()];
    indexMeta.SerializeToArray(buffer, indexMeta.ByteSizeLong());
    SetIndexMeta(collection, buffer, indexMeta.ByteSizeLong());
    DeleteCollection(collection);
}

TEST(CApiTest, GetCollectionNameTest) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    auto name = GetCollectionName(collection);
    ASSERT_EQ(strcmp(name, "default-collection"), 0);
    DeleteCollection(collection);
    free((void*)(name));
}