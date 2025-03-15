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

#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "index/IndexFactory.h"
#include "index/JsonInvertedIndex.h"

#include <gtest/gtest.h>
#include <cstdint>
using namespace milvus;
using namespace milvus::index;

TEST(JsonIndexTest, TestBuildNonExistJsonPath) {
    std::string json_path = "hello";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::INVERTED_INDEX_TYPE,
        JsonCastType::DOUBLE,
        json_path,
        file_manager_ctx);
    auto json_index = std::unique_ptr<JsonInvertedIndex<int32_t>>(
        static_cast<JsonInvertedIndex<int32_t>*>(inv_index.release()));

    std::vector<std::string> json_raw_data = {R"({"hello": 1})",
                                              R"({"world": 2})"};

    std::vector<milvus::Json> jsons;
    for (auto& json : json_raw_data) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);
    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader();
}

TEST(JsonIndexTest, TestJSONErrRecorder) {
    std::vector<std::string> json_raw_data = {
        R"(1)",
        R"({"a": true})",
        R"({"a": 1.0})",
        R"({"a": 1})",
        R"({"a": null})",
        R"({"a": [1,2,3]})",
        R"({"a": [1.0,2,3]})",
        R"({"a": {"b": 1}})",
        R"({"a": "1"})",
        R"({"a": 1, "a": 1.0})",
    };

    std::string json_path = "/a";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());

    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::INVERTED_INDEX_TYPE,
        JsonCastType::DOUBLE,
        json_path,
        file_manager_ctx);
    auto json_index = std::unique_ptr<JsonInvertedIndex<double>>(
        static_cast<JsonInvertedIndex<double>*>(inv_index.release()));

    std::vector<milvus::Json> jsons;
    for (auto& json : json_raw_data) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);
    json_index->BuildWithFieldData({json_field});

    auto error_map = json_index->GetErrorRecorder().GetErrorMap();
    EXPECT_EQ(error_map.size(), 2);
    EXPECT_EQ(error_map[simdjson::error_code::INCORRECT_TYPE].count, 6);
    EXPECT_EQ(error_map[simdjson::error_code::INVALID_JSON_POINTER].count, 1);
}
