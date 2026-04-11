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
#include <simdjson.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldData.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "index/BitmapIndex.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/JsonHybridScalarIndex.h"
#include "index/JsonIndexBuilder.h"
#include "index/JsonScalarIndexWrapper.h"
#include "index/Meta.h"
#include "index/ScalarIndexSort.h"
#include "pb/schema.pb.h"
#include "simdjson/padded_string.h"
#include "storage/Types.h"

using namespace milvus;
using namespace milvus::index;

namespace {

// Helper: create JSON FieldData from raw strings
std::shared_ptr<FieldData<Json>>
MakeJsonFieldData(const std::vector<std::string>& raw_jsons) {
    std::vector<Json> jsons;
    jsons.reserve(raw_jsons.size());
    for (const auto& s : raw_jsons) {
        jsons.emplace_back(simdjson::padded_string(s));
    }
    auto fd = std::make_shared<FieldData<Json>>(DataType::JSON, false);
    fd->add_json_data(jsons);
    return fd;
}

// Helper: create a proto FieldSchema for JSON
proto::schema::FieldSchema
MakeJsonSchema(int64_t field_id = 101, bool nullable = false) {
    proto::schema::FieldSchema schema;
    schema.set_data_type(proto::schema::JSON);
    schema.set_fieldid(field_id);
    schema.set_nullable(nullable);
    return schema;
}

// Helper: create a FileManagerContext for JSON field (no actual file manager)
storage::FileManagerContext
MakeTestContext(int64_t field_id = 101) {
    storage::FileManagerContext ctx;
    ctx.fieldDataMeta.field_schema.set_data_type(proto::schema::JSON);
    ctx.fieldDataMeta.field_schema.set_fieldid(field_id);
    ctx.fieldDataMeta.field_id = field_id;
    return ctx;
}

}  // namespace

// ============================================================
// 1. ConvertJsonToTypedFieldData tests
// ============================================================

TEST(JsonPathIndexTest, ConvertDouble_NormalExtraction) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": 1.5})",
        R"({"a": 2.0})",
        R"({"a": 3.7})",
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd}, schema, "/a", JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    ASSERT_EQ(result.field_datas.size(), 1);
    auto& fd = result.field_datas[0];
    EXPECT_EQ(fd->get_num_rows(), 3);
    // All rows valid
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(fd->is_valid(i));
    }
    EXPECT_TRUE(result.non_exist_offsets.empty());
}

TEST(JsonPathIndexTest, ConvertDouble_PathNotExist) {
    auto json_fd = MakeJsonFieldData({
        R"({"b": 1})",   // path /a doesn't exist
        R"({"a": 2.0})", // exists
        R"(100)",         // not an object, /a doesn't exist
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd}, schema, "/a", JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_datas[0];
    EXPECT_EQ(fd->get_num_rows(), 3);
    EXPECT_FALSE(fd->is_valid(0));  // path not exist
    EXPECT_TRUE(fd->is_valid(1));   // valid
    EXPECT_FALSE(fd->is_valid(2));  // path not exist

    // non_exist_offsets should contain 0 and 2
    ASSERT_EQ(result.non_exist_offsets.size(), 2);
    EXPECT_EQ(result.non_exist_offsets[0], 0);
    EXPECT_EQ(result.non_exist_offsets[1], 2);
}

TEST(JsonPathIndexTest, ConvertDouble_PathExistsButCastFails) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": "hello"})",  // path exists, but can't cast to DOUBLE
        R"({"a": 2.0})",     // valid
        R"({"a": [1,2,3]})", // path exists, but array can't cast to DOUBLE
        R"({"a": true})",    // path exists, bool can't cast to DOUBLE
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd}, schema, "/a", JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_datas[0];
    EXPECT_EQ(fd->get_num_rows(), 4);
    EXPECT_FALSE(fd->is_valid(0));  // cast fail
    EXPECT_TRUE(fd->is_valid(1));   // valid
    EXPECT_FALSE(fd->is_valid(2));  // cast fail
    EXPECT_FALSE(fd->is_valid(3));  // cast fail

    // Key: non_exist_offsets should be EMPTY because path exists in all rows
    EXPECT_TRUE(result.non_exist_offsets.empty());
}

TEST(JsonPathIndexTest, ConvertDouble_MixedRows) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": 1.0})",     // 0: valid
        R"({"b": 2})",       // 1: path not exist
        R"({"a": "str"})",   // 2: path exists, cast fail
        R"({"a": 3.0})",     // 3: valid
        R"(42)",              // 4: path not exist (not object)
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd}, schema, "/a", JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_datas[0];
    EXPECT_EQ(fd->get_num_rows(), 5);
    EXPECT_TRUE(fd->is_valid(0));
    EXPECT_FALSE(fd->is_valid(1));
    EXPECT_FALSE(fd->is_valid(2));
    EXPECT_TRUE(fd->is_valid(3));
    EXPECT_FALSE(fd->is_valid(4));

    // non_exist: only 1 and 4 (path truly missing)
    // offset 2 is NOT in non_exist (path exists but cast fails)
    ASSERT_EQ(result.non_exist_offsets.size(), 2);
    EXPECT_EQ(result.non_exist_offsets[0], 1);
    EXPECT_EQ(result.non_exist_offsets[1], 4);
}

TEST(JsonPathIndexTest, ConvertVarchar) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": "hello"})",
        R"({"a": "world"})",
        R"({"b": 1})",
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<std::string>(
        {json_fd}, schema, "/a", JsonCastType::FromString("VARCHAR"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_datas[0];
    EXPECT_EQ(fd->get_num_rows(), 3);
    EXPECT_TRUE(fd->is_valid(0));
    EXPECT_TRUE(fd->is_valid(1));
    EXPECT_FALSE(fd->is_valid(2));

    ASSERT_EQ(result.non_exist_offsets.size(), 1);
    EXPECT_EQ(result.non_exist_offsets[0], 2);
}

// ============================================================
// 2. JsonScalarIndexWrapper tests (Sort + Bitmap)
// ============================================================

TEST(JsonPathIndexTest, SortDouble_RangeQuery) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": 10.0})",
        R"({"a": 20.0})",
        R"({"a": 30.0})",
        R"({"a": 40.0})",
        R"({"a": 50.0})",
    });
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonScalarIndexWrapper<double, ScalarIndexSort<double>> idx(
        JsonCastType::FromString("DOUBLE"), "/a",
        JsonCastFunction::FromString("unknown"), schema, ctx);

    idx.BuildWithFieldData({json_fd});

    // Range: a > 25
    auto result = idx.Range(25.0, OpType::GreaterThan);
    EXPECT_EQ(result.count(), 3);  // 30, 40, 50

    // Range: 15 <= a <= 35
    auto result2 = idx.Range(15.0, true, 35.0, true);
    EXPECT_EQ(result2.count(), 2);  // 20, 30

    // In
    double vals[] = {10.0, 50.0};
    auto result3 = idx.In(2, vals);
    EXPECT_EQ(result3.count(), 2);
}

TEST(JsonPathIndexTest, BitmapVarchar_BuildAndCount) {
    // Note: BitmapIndex::BuildWithFieldData has a pre-existing issue where
    // build_mode_ is not set, so In()/Range() don't work correctly without
    // a Serialize→Load cycle. We test that the index builds successfully
    // and verify Exists/IsNotNull semantics instead.
    auto json_fd = MakeJsonFieldData({
        R"({"s": "active"})",
        R"({"s": "inactive"})",
        R"({"s": "active"})",
        R"({"s": "pending"})",
        R"({"s": "active"})",
    });
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonScalarIndexWrapper<std::string, BitmapIndex<std::string>> idx(
        JsonCastType::FromString("VARCHAR"), "/s",
        JsonCastFunction::FromString("unknown"), schema, ctx);

    idx.BuildWithFieldData({json_fd});
    EXPECT_EQ(idx.Count(), 5);

    // IsNotNull should return all rows (all valid)
    auto not_null = idx.IsNotNull();
    EXPECT_EQ(not_null.count(), 5);
}

TEST(JsonPathIndexTest, SortDouble_ExistsSemantics) {
    // Key test: Exists() must return true for rows where path exists
    // even if the value can't be cast to the index type.
    auto json_fd = MakeJsonFieldData({
        R"({"a": 1.0})",      // 0: valid double
        R"({"a": "hello"})",  // 1: path exists, cast fails
        R"({"b": 2})",        // 2: path not exist
        R"({"a": true})",     // 3: path exists, cast fails
        R"({"a": 5.0})",      // 4: valid double
    });
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonScalarIndexWrapper<double, ScalarIndexSort<double>> idx(
        JsonCastType::FromString("DOUBLE"), "/a",
        JsonCastFunction::FromString("unknown"), schema, ctx);

    idx.BuildWithFieldData({json_fd});

    auto exists = idx.Exists();
    EXPECT_EQ(exists.size(), 5);
    EXPECT_TRUE(exists[0]);   // path exists + valid
    EXPECT_TRUE(exists[1]);   // path exists + cast fail → still EXISTS
    EXPECT_FALSE(exists[2]);  // path not exist
    EXPECT_TRUE(exists[3]);   // path exists + cast fail → still EXISTS
    EXPECT_TRUE(exists[4]);   // path exists + valid
    EXPECT_EQ(exists.count(), 4);
}

TEST(JsonPathIndexTest, BitmapBool_ExistsSemantics) {
    auto json_fd = MakeJsonFieldData({
        R"({"f": true})",
        R"({"f": false})",
        R"({"g": 1})",       // path /f not exist
        R"({"f": "yes"})",   // path exists, cast fail
    });
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonScalarIndexWrapper<bool, BitmapIndex<bool>> idx(
        JsonCastType::FromString("BOOL"), "/f",
        JsonCastFunction::FromString("unknown"), schema, ctx);

    idx.BuildWithFieldData({json_fd});

    auto exists = idx.Exists();
    EXPECT_TRUE(exists[0]);
    EXPECT_TRUE(exists[1]);
    EXPECT_FALSE(exists[2]);  // path not exist
    EXPECT_TRUE(exists[3]);   // path exists but cast fail → EXISTS=true
}

// ============================================================
// 3. JsonHybridScalarIndex tests
// ============================================================

TEST(JsonPathIndexTest, Hybrid_LowCardinalitySelectsBitmap) {
    // 3 distinct values → low cardinality → should select BITMAP
    std::vector<std::string> raw;
    for (int i = 0; i < 100; i++) {
        std::string val = (i % 3 == 0) ? "a" : (i % 3 == 1) ? "b" : "c";
        raw.push_back(R"({"x": ")" + val + R"("})");
    }
    auto json_fd = MakeJsonFieldData(raw);
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonHybridScalarIndex<std::string> idx(
        JsonCastType::FromString("VARCHAR"), "/x",
        JsonCastFunction::FromString("unknown"), schema, 0, ctx);

    idx.BuildWithFieldData({json_fd});

    EXPECT_EQ(idx.internal_index_type_, ScalarIndexType::BITMAP);
    EXPECT_EQ(idx.Count(), 100);
}

TEST(JsonPathIndexTest, Hybrid_HighCardinalitySelectsSort) {
    // 1000 distinct values → high cardinality → should select STLSORT
    std::vector<std::string> raw;
    for (int i = 0; i < 1000; i++) {
        raw.push_back(R"({"n": )" + std::to_string(i) + "}");
    }
    auto json_fd = MakeJsonFieldData(raw);
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonHybridScalarIndex<double> idx(
        JsonCastType::FromString("DOUBLE"), "/n",
        JsonCastFunction::FromString("unknown"), schema, 0, ctx);

    idx.BuildWithFieldData({json_fd});

    EXPECT_EQ(idx.internal_index_type_, ScalarIndexType::STLSORT);

    // Verify range query works
    auto result = idx.Range(500.0, OpType::GreaterThan);
    EXPECT_EQ(result.count(), 499);  // 501..999
}

TEST(JsonPathIndexTest, Hybrid_CardinalityIgnoresInvalidRows) {
    // Only 3 valid distinct values, but many invalid rows.
    // Without the fix, invalid rows have default value (0.0) which
    // would add a 4th distinct value. This test ensures we don't count them.
    std::vector<std::string> raw;
    // 10 valid rows with 3 distinct values
    for (int i = 0; i < 10; i++) {
        double val = (i % 3) + 1.0;
        raw.push_back(R"({"v": )" + std::to_string(val) + "}");
    }
    // 100 rows where path doesn't exist or cast fails
    for (int i = 0; i < 50; i++) {
        raw.push_back(R"({"other": 1})");  // path /v not exist
    }
    for (int i = 0; i < 50; i++) {
        raw.push_back(R"({"v": "str"})");  // cast fail
    }

    auto json_fd = MakeJsonFieldData(raw);
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonHybridScalarIndex<double> idx(
        JsonCastType::FromString("DOUBLE"), "/v",
        JsonCastFunction::FromString("unknown"), schema, 0, ctx);

    idx.BuildWithFieldData({json_fd});

    // 3 distinct valid values → should be BITMAP (low cardinality)
    EXPECT_EQ(idx.internal_index_type_, ScalarIndexType::BITMAP);
}

TEST(JsonPathIndexTest, Hybrid_ExistsSemantics) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": 1.0})",
        R"({"a": "text"})",  // cast fail
        R"({"b": 2})",       // path not exist
        R"({"a": 3.0})",
    });
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonHybridScalarIndex<double> idx(
        JsonCastType::FromString("DOUBLE"), "/a",
        JsonCastFunction::FromString("unknown"), schema, 0, ctx);

    idx.BuildWithFieldData({json_fd});

    auto exists = idx.Exists();
    EXPECT_TRUE(exists[0]);   // valid
    EXPECT_TRUE(exists[1]);   // cast fail → still EXISTS
    EXPECT_FALSE(exists[2]);  // path not exist
    EXPECT_TRUE(exists[3]);   // valid
}

// ============================================================
// 4. IndexFactory routing tests
// ============================================================

TEST(JsonPathIndexTest, Factory_SortDouble) {
    auto ctx = MakeTestContext();
    CreateIndexInfo info;
    info.index_type = ASCENDING_SORT;
    info.field_type = DataType::JSON;
    info.json_cast_type = JsonCastType::FromString("DOUBLE");
    info.json_path = "/num";

    auto idx = IndexFactory::GetInstance().CreateJsonIndex(info, ctx);
    ASSERT_NE(idx, nullptr);
    EXPECT_EQ(idx->GetCastType().ToString(), JsonCastType::FromString("DOUBLE").ToString());
}

TEST(JsonPathIndexTest, Factory_BitmapVarchar) {
    auto ctx = MakeTestContext();
    CreateIndexInfo info;
    info.index_type = BITMAP_INDEX_TYPE;
    info.field_type = DataType::JSON;
    info.json_cast_type = JsonCastType::FromString("VARCHAR");
    info.json_path = "/label";

    auto idx = IndexFactory::GetInstance().CreateJsonIndex(info, ctx);
    ASSERT_NE(idx, nullptr);
    EXPECT_EQ(idx->GetCastType().ToString(), JsonCastType::FromString("VARCHAR").ToString());
}

TEST(JsonPathIndexTest, Factory_HybridDouble) {
    auto ctx = MakeTestContext();
    CreateIndexInfo info;
    info.index_type = HYBRID_INDEX_TYPE;
    info.field_type = DataType::JSON;
    info.json_cast_type = JsonCastType::FromString("DOUBLE");
    info.json_path = "/val";
    info.tantivy_index_version = 1;

    auto idx = IndexFactory::GetInstance().CreateJsonIndex(info, ctx);
    ASSERT_NE(idx, nullptr);
    EXPECT_EQ(idx->GetCastType().ToString(), JsonCastType::FromString("DOUBLE").ToString());
}

TEST(JsonPathIndexTest, Factory_BitmapDouble_Rejected) {
    auto ctx = MakeTestContext();
    CreateIndexInfo info;
    info.index_type = BITMAP_INDEX_TYPE;
    info.field_type = DataType::JSON;
    info.json_cast_type = JsonCastType::FromString("DOUBLE");
    info.json_path = "/num";

    EXPECT_THROW(
        IndexFactory::GetInstance().CreateJsonIndex(info, ctx),
        std::exception);
}

TEST(JsonPathIndexTest, Factory_SortBool_Rejected) {
    auto ctx = MakeTestContext();
    CreateIndexInfo info;
    info.index_type = ASCENDING_SORT;
    info.field_type = DataType::JSON;
    info.json_cast_type = JsonCastType::FromString("BOOL");
    info.json_path = "/flag";

    EXPECT_THROW(
        IndexFactory::GetInstance().CreateJsonIndex(info, ctx),
        std::exception);
}
