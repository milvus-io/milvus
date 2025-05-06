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

#include "common/Consts.h"
#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "index/IndexFactory.h"
#include "index/JsonInvertedIndex.h"
#include "mmap/Types.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/Types.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Util.h"
#include "test_utils/storage_test_utils.h"

#include <gtest/gtest.h>
#include <cstdint>
using namespace milvus;
using namespace milvus::index;

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
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::INVERTED_INDEX_TYPE,
        JsonCastType::FromString("DOUBLE"),
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
    EXPECT_EQ(error_map[simdjson::error_code::INCORRECT_TYPE].count, 5);
    EXPECT_EQ(error_map[simdjson::error_code::NO_SUCH_FIELD].count, 2);
}

TEST(JsonIndexTest, TestJsonContains) {
    std::vector<std::string> json_raw_data = {
        R"(1)",
        R"("a simple string")",
        R"(null)",
        R"(true)",
        R"([])",
        R"({})",
        R"([1, 2, 3])",
        R"([1.0, 2.0, 3.0])",
        R"([true, false, null])",
        R"(["hello", "world"])",
        R"([{"nested": true}, {"nested": false}])",
        R"({"a": true})",
        R"({"a": 1.0})",
        R"({"a": 1})",
        R"({"a": null})",
        R"({"a": "hello"})",
        R"({"a": {"nested": true}})",
        R"({"a": [1, 2, 3]})",
        R"({"a": [1.0, 2, 3]})",
        R"({"a": [true, false]})",
        R"({"a": ["x", "y"]})",
        R"({"a": [{"nested": true}, {"nested": false}]})",
        R"({"a": []})",
    };

    auto json_path = "/a";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::INVERTED_INDEX_TYPE,
        JsonCastType::FromString("ARRAY_DOUBLE"),
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
    json_index->finish();
    json_index->create_reader();

    auto segment = segcore::CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index = std::move(json_index);
    load_index_info.index_params = {{JSON_PATH, json_path}};
    segment->LoadIndex(load_index_info);

    auto field_data_info = FieldDataInfo{json_fid.get(),
                                         json_raw_data.size(),
                                         std::vector<FieldDataPtr>{json_field}};
    segment->LoadFieldData(json_fid, field_data_info);

    std::vector<std::tuple<proto::plan::GenericValue, std::vector<int64_t>>>
        test_cases;

    proto::plan::GenericValue value;
    value.set_int64_val(1);
    test_cases.push_back(std::make_tuple(value, std::vector<int64_t>{17, 18}));

    // proto::plan::GenericValue value2;
    // value2.set_bool_val(true);
    // test_cases.push_back(std::make_tuple(value2, std::vector<int64_t>{8}));

    // proto::plan::GenericValue value3;
    // value3.set_string_val("x");
    // test_cases.push_back(std::make_tuple(value3, std::vector<int64_t>{9}));

    for (auto& test_case : test_cases) {
        auto expr = std::make_shared<expr::JsonContainsExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
            proto::plan::JSONContainsExpr_JSONOp::
                JSONContainsExpr_JSONOp_Contains,
            true,
            std::vector<proto::plan::GenericValue>{value});

        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

        auto result = query::ExecuteQueryExpr(
            plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

        auto expect_result = std::get<1>(test_case);
        EXPECT_EQ(result.count(), expect_result.size());
        for (auto& id : expect_result) {
            EXPECT_TRUE(result[id]);
        }
    }
}
