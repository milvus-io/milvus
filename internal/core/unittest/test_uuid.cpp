// Copyright (C) 2019-2025 Zilliz. All rights reserved.
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
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldData.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "pb/schema.pb.h"

using namespace milvus;

// Verify the DataType enum value for UUID.
TEST(UuidTest, EnumValue) {
    ASSERT_EQ(static_cast<int>(DataType::UUID), 28);
}

// Verify that UUID is not treated as a string data type internally.
// UUID is stored as a string-like type in FieldData but is not grouped
// with VARCHAR/STRING/TEXT for schema handling purposes.
TEST(UuidTest, IsNotStringDataType) {
    ASSERT_FALSE(IsStringDataType(DataType::UUID));
}

// Verify ToProtoDataType maps internal DataType::UUID to proto DataType::UUID.
TEST(UuidTest, ToProtoDataTypeMapping) {
    auto proto_type = ToProtoDataType(DataType::UUID);
    ASSERT_EQ(proto_type, proto::schema::DataType::UUID);
}

// Verify that InitScalarFieldData creates a std::string-backed FieldData
// for UUID, consistent with STRING/VARCHAR handling.
TEST(UuidTest, InitScalarFieldData) {
    auto field_data = InitScalarFieldData(DataType::UUID, false, 10);
    ASSERT_NE(field_data, nullptr);
    ASSERT_EQ(field_data->get_data_type(), DataType::UUID);
}

// Verify that InitScalarFieldDataWithLength creates a std::string-backed
// FieldData for UUID at a given capacity.
TEST(UuidTest, InitScalarFieldDataWithLength) {
    constexpr int64_t kLength = 100;
    auto field_data = InitScalarFieldDataWithLength(DataType::UUID, kLength);
    ASSERT_NE(field_data, nullptr);
    ASSERT_EQ(field_data->get_data_type(), DataType::UUID);
    ASSERT_EQ(field_data->length(), kLength);
}

// Verify that a UUID field can be added to a Schema via AddDebugField.
// UUID is not a string type per IsStringDataType, so it goes through the
// non-string path (no max_length requirement).
TEST(UuidTest, SchemaAddField) {
    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("uuid_field", DataType::UUID);
    ASSERT_NE(field_id.get(), -1);

    auto& field_meta = schema->operator[](field_id);
    ASSERT_EQ(field_meta.get_data_type(), DataType::UUID);
    ASSERT_EQ(field_meta.get_name(), "uuid_field");
}


