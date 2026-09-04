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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldData.h"
#include "common/Json.h"
#include "common/JsonCastFunction.h"
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
#include "storage/IndexData.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/storage_test_utils.h"

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

std::vector<bool>
BuildUploadAndReloadInvertedExists(
    const std::vector<std::string>& raw_jsons,
    int32_t scalar_index_engine_version,
    const std::string& cast_type = "DOUBLE",
    const std::function<void(IndexBase*)>& inspect = {}) {
    constexpr int64_t collection_id = 1;
    constexpr int64_t partition_id = 2;
    constexpr int64_t segment_id = 3;
    constexpr int64_t field_id = 101;
    constexpr int64_t index_build_id = 4000;
    constexpr int64_t index_version = 4000;

    auto field_meta = milvus::segcore::gen_field_meta(
        collection_id, partition_id, segment_id, field_id, DataType::JSON);
    auto index_meta =
        gen_index_meta(segment_id, field_id, index_build_id, index_version);

    auto root_path =
        (boost::filesystem::path(TestLocalPath) /
         boost::filesystem::unique_path("json-path-presence-%%%%-%%%%"))
            .string();
    auto storage_config = gen_local_storage_config(root_path);
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto arrow_fs = storage::InitArrowFileSystem(storage_config);
    ChunkManagerWrapper chunk_manager_guard(chunk_manager);

    std::vector<Json> jsons;
    jsons.reserve(raw_jsons.size());
    for (const auto& raw_json : raw_jsons) {
        jsons.emplace_back(simdjson::padded_string(raw_json));
    }
    auto json_field = std::make_shared<FieldData<Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);

    auto payload_reader = std::make_shared<storage::PayloadReader>(json_field);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);
    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto insert_file =
        (boost::filesystem::path(root_path) / "insert.binlog").string();
    chunk_manager_guard.Write(
        insert_file, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext context(
        field_meta, index_meta, chunk_manager, arrow_fs);
    CreateIndexInfo create_info;
    create_info.field_type = DataType::JSON;
    create_info.index_type = INVERTED_INDEX_TYPE;
    create_info.scalar_index_engine_version = scalar_index_engine_version;
    create_info.json_cast_type = JsonCastType::FromString(cast_type);
    create_info.json_path = "/a";

    auto build_index =
        IndexFactory::GetInstance().CreateJsonIndex(create_info, context);
    Config build_config;
    build_config[INSERT_FILES_KEY] = std::vector<std::string>{insert_file};
    build_config[INDEX_NUM_ROWS_KEY] = raw_jsons.size();
    build_config[SCALAR_INDEX_ENGINE_VERSION] = scalar_index_engine_version;
    build_index->Build(build_config);
    auto index_files = build_index->UploadUnified({})->GetIndexFiles();

    context.set_for_loading_index(true);
    auto loaded_index =
        IndexFactory::GetInstance().CreateJsonIndex(create_info, context);
    Config load_config;
    load_config[INDEX_FILES] = index_files;
    load_config[LOAD_PRIORITY] = proto::common::LoadPriority::HIGH;
    loaded_index->LoadUnified(load_config);

    if (inspect) {
        inspect(loaded_index.get());
    }

    auto exists = loaded_index->Exists();
    std::vector<bool> result(exists.size());
    for (size_t i = 0; i < exists.size(); ++i) {
        result[i] = exists[i];
    }
    return result;
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
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_data;
    EXPECT_EQ(fd->get_num_rows(), 3);
    // All rows valid
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(fd->is_valid(i));
    }
    EXPECT_TRUE(result.non_exist_offsets.empty());
}

TEST(JsonPathIndexTest, ConvertDouble_PathNotExist) {
    auto json_fd = MakeJsonFieldData({
        R"({"b": 1})",    // path /a doesn't exist
        R"({"a": 2.0})",  // exists
        R"(100)",         // not an object, /a doesn't exist
        R"([1, 2])",      // non-numeric array path doesn't exist
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_data;
    EXPECT_EQ(fd->get_num_rows(), 4);
    EXPECT_FALSE(fd->is_valid(0));  // path not exist
    EXPECT_TRUE(fd->is_valid(1));   // valid
    EXPECT_FALSE(fd->is_valid(2));  // path not exist
    EXPECT_FALSE(fd->is_valid(3));  // path not exist

    // non_exist_offsets should contain 0, 2, and 3
    ASSERT_EQ(result.non_exist_offsets.size(), 3);
    EXPECT_EQ(result.non_exist_offsets[0], 0);
    EXPECT_EQ(result.non_exist_offsets[1], 2);
    EXPECT_EQ(result.non_exist_offsets[2], 3);
}

TEST(JsonPathIndexTest, ConvertDouble_PathExistsButCastFails) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": "hello"})",  // path exists, but can't cast to DOUBLE
        R"({"a": 2.0})",      // valid
        R"({"a": [1,2,3]})",  // path exists, but array can't cast to DOUBLE
        R"({"a": true})",     // path exists, bool can't cast to DOUBLE
        R"({"a": []})",       // empty array is present, but can't cast
        R"({"a": [null]})",   // array is present regardless of its children
        R"({"a": {}})",       // empty object is present, but can't cast
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_data;
    EXPECT_EQ(fd->get_num_rows(), 7);
    EXPECT_FALSE(fd->is_valid(0));  // cast fail
    EXPECT_TRUE(fd->is_valid(1));   // valid
    EXPECT_FALSE(fd->is_valid(2));  // cast fail
    EXPECT_FALSE(fd->is_valid(3));  // cast fail
    EXPECT_FALSE(fd->is_valid(4));  // cast fail
    EXPECT_FALSE(fd->is_valid(5));  // cast fail
    EXPECT_FALSE(fd->is_valid(6));  // cast fail

    // Key: non_exist_offsets should be EMPTY because path exists in all rows
    EXPECT_TRUE(result.non_exist_offsets.empty());
}

TEST(JsonPathIndexTest, ConvertDouble_UnrepresentableNumberIsAbsentForExists) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": -1.48e-309})",             // representable subnormal
        R"({"a": 1e-400})",                 // underflows to zero
        R"({"a": 1e400})",                  // out of double range
        R"({"sibling": 1e400, "a": 2.5})",  // unrelated bad number
        R"({"a": 18446744073709551616})",   // larger than uint64_t
        R"({"b": 1})",                      // path missing
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_data;
    ASSERT_EQ(fd->get_num_rows(), 6);
    EXPECT_TRUE(fd->is_valid(0));
    EXPECT_TRUE(fd->is_valid(1));
    EXPECT_FALSE(fd->is_valid(2));
    EXPECT_TRUE(fd->is_valid(3));
    EXPECT_FALSE(fd->is_valid(4));
    EXPECT_FALSE(fd->is_valid(5));

    EXPECT_DOUBLE_EQ(*static_cast<const double*>(fd->RawValue(0)), -1.48e-309);
    EXPECT_EQ(*static_cast<const double*>(fd->RawValue(1)), 0.0);
    EXPECT_DOUBLE_EQ(*static_cast<const double*>(fd->RawValue(3)), 2.5);

    // Unrepresentable JSON numbers use the configured EXISTS=false contract.
    // A bad number in an unrelated sibling does not poison /a.
    ASSERT_EQ(result.non_exist_offsets.size(), 3);
    EXPECT_EQ(result.non_exist_offsets[0], 2);
    EXPECT_EQ(result.non_exist_offsets[1], 4);
    EXPECT_EQ(result.non_exist_offsets[2], 5);
}

TEST(JsonPathIndexTest, ConvertInt64_StrictCast) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": 42})",                   // int64, in range
        R"({"a": 2.0})",                  // integral double round-trips to 2
        R"({"a": 2.5})",                  // fractional double -> null
        R"({"a": 9223372036854775807})",  // INT64_MAX, in range
        R"({"a": 9223372036854775808})",  // > INT64_MAX (uint64) -> null
        R"({"a": 1e400})",                // unrepresentable -> null
        R"({"a": "42"})",                 // string -> null
        R"({"b": 1})",                    // path missing -> non-exist
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<int64_t>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("INT64"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_data;
    ASSERT_EQ(fd->get_num_rows(), 8);
    EXPECT_TRUE(fd->is_valid(0));
    EXPECT_TRUE(fd->is_valid(1));
    EXPECT_FALSE(fd->is_valid(2));
    EXPECT_TRUE(fd->is_valid(3));
    EXPECT_FALSE(fd->is_valid(4));
    EXPECT_FALSE(fd->is_valid(5));
    EXPECT_FALSE(fd->is_valid(6));
    EXPECT_FALSE(fd->is_valid(7));

    EXPECT_EQ(*static_cast<const int64_t*>(fd->RawValue(0)), 42);
    EXPECT_EQ(*static_cast<const int64_t*>(fd->RawValue(1)), 2);

    // 2.5, uint64 overflow, and the unrepresentable number keep the target
    // value present (EXISTS=true) but null in the typed column.
    ASSERT_EQ(result.non_exist_offsets.size(), 2);
    EXPECT_EQ(result.non_exist_offsets[0], 5);
    EXPECT_EQ(result.non_exist_offsets[1], 7);
}

TEST(JsonPathIndexTest, ConvertInt8_StrictCastRejectsOutOfRange) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": 127})",   // INT8_MAX, in range
        R"({"a": -128})",  // INT8_MIN, in range
        R"({"a": 128})",   // overflow -> null
        R"({"a": -129})",  // underflow -> null
        R"({"a": 300})",   // overflow -> null
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<int8_t>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("INT8"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_data;
    ASSERT_EQ(fd->get_num_rows(), 5);
    EXPECT_TRUE(fd->is_valid(0));
    EXPECT_TRUE(fd->is_valid(1));
    EXPECT_FALSE(fd->is_valid(2));
    EXPECT_FALSE(fd->is_valid(3));
    EXPECT_FALSE(fd->is_valid(4));

    EXPECT_EQ(*static_cast<const int8_t*>(fd->RawValue(0)), 127);
    EXPECT_EQ(*static_cast<const int8_t*>(fd->RawValue(1)), -128);
    EXPECT_TRUE(result.non_exist_offsets.empty());
}

TEST(JsonPathIndexTest, ConvertDouble_StringCastUsesSimdjsonNumberSemantics) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": "-1.48e-309"})",            // representable subnormal
        R"({"a": "1e-400"})",                // underflows to zero
        R"({"a": "1e400"})",                 // overflows double
        R"({"a": "1.5junk"})",               // not a complete number token
        R"({"a": "2.5"})",                   // ordinary finite value
        R"({"a": "18446744073709551616"})",  // larger than uint64_t
        R"({"a": 18446744073709551616})",    // same numeric JSON value
        R"({"b": 1})",                       // path missing
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("STRING_TO_DOUBLE"));

    auto& fd = result.field_data;
    ASSERT_EQ(fd->get_num_rows(), 8);
    EXPECT_TRUE(fd->is_valid(0));
    EXPECT_TRUE(fd->is_valid(1));
    EXPECT_FALSE(fd->is_valid(2));
    EXPECT_FALSE(fd->is_valid(3));
    EXPECT_TRUE(fd->is_valid(4));
    EXPECT_FALSE(fd->is_valid(5));
    EXPECT_FALSE(fd->is_valid(6));
    EXPECT_FALSE(fd->is_valid(7));

    EXPECT_DOUBLE_EQ(*static_cast<const double*>(fd->RawValue(0)), -1.48e-309);
    EXPECT_EQ(*static_cast<const double*>(fd->RawValue(1)), 0.0);
    EXPECT_DOUBLE_EQ(*static_cast<const double*>(fd->RawValue(4)), 2.5);

    // String cast failures remain present JSON strings. Only the invalid JSON
    // number token and the physically missing path use EXISTS=false.
    ASSERT_EQ(result.non_exist_offsets.size(), 2);
    EXPECT_EQ(result.non_exist_offsets[0], 6);
    EXPECT_EQ(result.non_exist_offsets[1], 7);

    auto cast = JsonCastFunction::FromString("STRING_TO_DOUBLE");
    Json out_of_range(simdjson::padded_string(std::string(R"({"a": 1e400})")));
    EXPECT_NO_THROW({
        auto value =
            JsonCastFunction::CastJsonValue<double>(cast, out_of_range, "/a");
        EXPECT_FALSE(value.has_value());
    });
}

TEST(JsonPathIndexTest, ConvertDouble_MixedRows) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": 1.0})",    // 0: valid
        R"({"b": 2})",      // 1: path not exist
        R"({"a": "str"})",  // 2: path exists, cast fail
        R"({"a": 3.0})",    // 3: valid
        R"(42)",            // 4: path not exist (not object)
    });
    auto schema = MakeJsonSchema();
    auto result = ConvertJsonToTypedFieldData<double>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_data;
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
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("VARCHAR"),
        JsonCastFunction::FromString("unknown"));

    auto& fd = result.field_data;
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
        JsonCastType::FromString("DOUBLE"),
        "/a",
        JsonCastFunction::FromString("unknown"),
        schema,
        ctx);

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
        JsonCastType::FromString("VARCHAR"),
        "/s",
        JsonCastFunction::FromString("unknown"),
        schema,
        ctx);

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
        JsonCastType::FromString("DOUBLE"),
        "/a",
        JsonCastFunction::FromString("unknown"),
        schema,
        ctx);

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

template <typename IndexT>
void
AssertDoubleComparisonUnknowns(IndexT& idx) {
    auto known = idx.IsNotNull();
    ASSERT_EQ(known.size(), 6);
    EXPECT_TRUE(known[0]);
    EXPECT_TRUE(known[1]);
    EXPECT_FALSE(known[2]);
    EXPECT_FALSE(known[3]);
    EXPECT_FALSE(known[4]);
    EXPECT_TRUE(known[5]);
    EXPECT_EQ(known.count(), 3);

    auto exists = idx.Exists();
    ASSERT_EQ(exists.size(), 6);
    EXPECT_TRUE(exists[0]);
    EXPECT_TRUE(exists[1]);
    EXPECT_TRUE(exists[2]);   // path exists, but cast fails
    EXPECT_FALSE(exists[3]);  // path missing
    EXPECT_FALSE(exists[4]);  // JSON null is not an existing comparable value
    EXPECT_TRUE(exists[5]);
    EXPECT_EQ(exists.count(), 4);

    double two = 2.0;
    auto not_in = idx.NotIn(1, &two);
    ASSERT_EQ(not_in.size(), 6);
    EXPECT_TRUE(not_in[0]);
    EXPECT_FALSE(not_in[1]);
    EXPECT_FALSE(not_in[2]);
    EXPECT_FALSE(not_in[3]);
    EXPECT_FALSE(not_in[4]);
    EXPECT_TRUE(not_in[5]);
    EXPECT_EQ(not_in.count(), 2);

    auto greater_than_one = idx.Range(1.0, OpType::GreaterThan);
    ASSERT_EQ(greater_than_one.size(), 6);
    EXPECT_FALSE(greater_than_one[0]);
    EXPECT_TRUE(greater_than_one[1]);
    EXPECT_FALSE(greater_than_one[2]);
    EXPECT_FALSE(greater_than_one[3]);
    EXPECT_FALSE(greater_than_one[4]);
    EXPECT_TRUE(greater_than_one[5]);
    EXPECT_EQ(greater_than_one.count(), 2);
}

std::shared_ptr<FieldData<Json>>
MakeMixedJsonDoubleFieldData() {
    return MakeJsonFieldData({
        R"({"a": 1.0})",
        R"({"a": 2.0})",
        R"({"a": "bad"})",
        R"({"b": 3.0})",
        R"({"a": null})",
        R"({"a": 3.0})",
    });
}

TEST(JsonPathIndexTest, SortDouble_ComparisonUnknowns) {
    auto json_fd = MakeMixedJsonDoubleFieldData();
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonScalarIndexWrapper<double, ScalarIndexSort<double>> idx(
        JsonCastType::FromString("DOUBLE"),
        "/a",
        JsonCastFunction::FromString("unknown"),
        schema,
        ctx);

    idx.BuildWithFieldData({json_fd});
    AssertDoubleComparisonUnknowns(idx);
}

TEST(JsonPathIndexTest, SortDouble_UnrepresentableNumberIsAbsentForExists) {
    auto json_fd = MakeJsonFieldData({
        R"({"a": 1.0})",
        R"({"a": 1e400})",
        R"({"b": 2.0})",
        R"({"a": 3.0})",
    });
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonScalarIndexWrapper<double, ScalarIndexSort<double>> idx(
        JsonCastType::FromString("DOUBLE"),
        "/a",
        JsonCastFunction::FromString("unknown"),
        schema,
        ctx);
    idx.BuildWithFieldData({json_fd});

    auto valid = idx.IsNotNull();
    ASSERT_EQ(valid.size(), 4);
    EXPECT_TRUE(valid[0]);
    EXPECT_FALSE(valid[1]);
    EXPECT_FALSE(valid[2]);
    EXPECT_TRUE(valid[3]);

    auto exists = idx.Exists();
    ASSERT_EQ(exists.size(), 4);
    EXPECT_TRUE(exists[0]);
    EXPECT_FALSE(exists[1]);
    EXPECT_FALSE(exists[2]);
    EXPECT_TRUE(exists[3]);

    auto range = idx.Range(0.0, OpType::GreaterThan);
    EXPECT_TRUE(range[0]);
    EXPECT_FALSE(range[1]);
    EXPECT_FALSE(range[2]);
    EXPECT_TRUE(range[3]);
}

TEST(JsonPathIndexTest, JsonExist_UsesNonNullTargetPresence) {
    auto exists = [](const std::string& json, std::string_view pointer) {
        return Json(simdjson::padded_string(json)).exist(pointer);
    };

    EXPECT_FALSE(exists(R"(1e400)", ""));
    EXPECT_FALSE(exists(R"(null)", ""));
    EXPECT_TRUE(exists(R"([])", ""));
    EXPECT_TRUE(exists(R"({})", ""));
    EXPECT_FALSE(exists(R"({"a":null})", "/a"));
    EXPECT_FALSE(exists(R"({"a":1e400})", "/a"));
    EXPECT_FALSE(exists(R"({"a":18446744073709551616})", "/a"));
    EXPECT_TRUE(exists(R"({"a":-1.48e-309})", "/a"));
    EXPECT_TRUE(exists(R"({"a":[]})", "/a"));
    EXPECT_TRUE(exists(R"({"a":[null]})", "/a"));
    EXPECT_TRUE(exists(R"({"a":[1e400]})", "/a"));
    EXPECT_TRUE(exists(R"({"a":[1e400,7]})", "/a"));
    EXPECT_TRUE(exists(R"({"a":{"bad":1e400}})", "/a"));
    EXPECT_TRUE(exists(R"({"a":{"bad":1e400,"ok":7}})", "/a"));
}

TEST(JsonPathIndexTest, ArrayDouble_SeparatesExistsAndOperandValidity) {
    auto json_fd = MakeJsonFieldData({
        R"({"a":[]})",
        R"({"a":[null]})",
        R"({"a":[1e400]})",
        R"({"a":[1e400,7]})",
        R"({"a":1e400})",
        R"({"a":7})",
        R"({"b":1})",
    });
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();
    JsonInvertedIndex<double> idx(JsonCastType::FromString("ARRAY_DOUBLE"),
                                  "/a",
                                  JsonCastFunction::FromString("unknown"),
                                  schema,
                                  ctx,
                                  TANTIVY_INDEX_LATEST_VERSION);

    idx.BuildWithFieldData({json_fd});
    idx.finish();
    idx.create_reader(milvus::index::SetBitsetSealed);

    auto exists_bits = idx.Exists();
    ASSERT_EQ(exists_bits.size(), 7);
    EXPECT_TRUE(exists_bits[0]);
    EXPECT_TRUE(exists_bits[1]);
    EXPECT_TRUE(exists_bits[2]);
    EXPECT_TRUE(exists_bits[3]);
    EXPECT_FALSE(exists_bits[4]);
    EXPECT_TRUE(exists_bits[5]);
    EXPECT_FALSE(exists_bits[6]);

    // ARRAY_* JSON path indexes are row-domain indexes, not nested
    // element-domain indexes. Their ordinary validity bitmap therefore
    // represents operand validity directly.
    EXPECT_FALSE(idx.IsNestedIndex());
    auto operand_valid = idx.IsNotNull();
    ASSERT_EQ(operand_valid.size(), 7);
    EXPECT_TRUE(operand_valid[0]);
    EXPECT_TRUE(operand_valid[1]);
    EXPECT_TRUE(operand_valid[2]);
    EXPECT_TRUE(operand_valid[3]);
    EXPECT_FALSE(operand_valid[4]);
    EXPECT_FALSE(operand_valid[5]);
    EXPECT_FALSE(operand_valid[6]);
}

TEST(JsonPathIndexTest, InvertedDouble_ComparisonUnknowns) {
    auto json_fd = MakeMixedJsonDoubleFieldData();
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonScalarIndexWrapper<double, InvertedIndexTantivy<double>> idx(
        JsonCastType::FromString("DOUBLE"),
        "/a",
        JsonCastFunction::FromString("unknown"),
        schema,
        ctx,
        TANTIVY_INDEX_LATEST_VERSION);

    idx.BuildWithFieldData({json_fd});
    idx.finish();
    idx.create_reader(milvus::index::SetBitsetSealed);

    EXPECT_EQ(idx.ValidityBitmapByteSize(), sizeof(uint64_t));
    AssertDoubleComparisonUnknowns(idx);
}

TEST(JsonPathIndexTest, BitmapBool_ExistsSemantics) {
    auto json_fd = MakeJsonFieldData({
        R"({"f": true})",
        R"({"f": false})",
        R"({"g": 1})",      // path /f not exist
        R"({"f": "yes"})",  // path exists, cast fail
    });
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonScalarIndexWrapper<bool, BitmapIndex<bool>> idx(
        JsonCastType::FromString("BOOL"),
        "/f",
        JsonCastFunction::FromString("unknown"),
        schema,
        ctx);

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
        JsonCastType::FromString("VARCHAR"),
        "/x",
        JsonCastFunction::FromString("unknown"),
        schema,
        0,
        ctx);

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

    JsonHybridScalarIndex<double> idx(JsonCastType::FromString("DOUBLE"),
                                      "/n",
                                      JsonCastFunction::FromString("unknown"),
                                      schema,
                                      0,
                                      ctx);

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

    JsonHybridScalarIndex<double> idx(JsonCastType::FromString("DOUBLE"),
                                      "/v",
                                      JsonCastFunction::FromString("unknown"),
                                      schema,
                                      0,
                                      ctx);

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

    JsonHybridScalarIndex<double> idx(JsonCastType::FromString("DOUBLE"),
                                      "/a",
                                      JsonCastFunction::FromString("unknown"),
                                      schema,
                                      0,
                                      ctx);

    idx.BuildWithFieldData({json_fd});

    auto exists = idx.Exists();
    EXPECT_TRUE(exists[0]);   // valid
    EXPECT_TRUE(exists[1]);   // cast fail → still EXISTS
    EXPECT_FALSE(exists[2]);  // path not exist
    EXPECT_TRUE(exists[3]);   // valid
}

TEST(JsonPathIndexTest, Hybrid_ComparisonUnknowns) {
    auto json_fd = MakeMixedJsonDoubleFieldData();
    auto schema = MakeJsonSchema();
    auto ctx = MakeTestContext();

    JsonHybridScalarIndex<double> idx(JsonCastType::FromString("DOUBLE"),
                                      "/a",
                                      JsonCastFunction::FromString("unknown"),
                                      schema,
                                      0,
                                      ctx);

    idx.bitmap_index_cardinality_limit_ = 3;
    idx.BuildWithFieldData({json_fd});
    AssertDoubleComparisonUnknowns(idx);
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
    EXPECT_EQ(idx->GetCastType().ToString(),
              JsonCastType::FromString("DOUBLE").ToString());
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
    EXPECT_EQ(idx->GetCastType().ToString(),
              JsonCastType::FromString("VARCHAR").ToString());
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
    EXPECT_EQ(idx->GetCastType().ToString(),
              JsonCastType::FromString("DOUBLE").ToString());
}

TEST(JsonPathIndexTest, Factory_BitmapDouble_Rejected) {
    auto ctx = MakeTestContext();
    CreateIndexInfo info;
    info.index_type = BITMAP_INDEX_TYPE;
    info.field_type = DataType::JSON;
    info.json_cast_type = JsonCastType::FromString("DOUBLE");
    info.json_path = "/num";

    EXPECT_THROW(IndexFactory::GetInstance().CreateJsonIndex(info, ctx),
                 std::exception);
}

TEST(JsonPathIndexTest, Factory_SortBool_Rejected) {
    auto ctx = MakeTestContext();
    CreateIndexInfo info;
    info.index_type = ASCENDING_SORT;
    info.field_type = DataType::JSON;
    info.json_cast_type = JsonCastType::FromString("BOOL");
    info.json_path = "/flag";

    EXPECT_THROW(IndexFactory::GetInstance().CreateJsonIndex(info, ctx),
                 std::exception);
}

TEST(JsonPathPresenceVersionTest, ScalarVersionSelectsSemantics) {
    Config config;
    EXPECT_EQ(ResolveJsonPathPresenceSemanticsForBuild(config),
              JsonPathPresenceSemantics::LEGACY_RECURSIVE_NON_EMPTY);

    config[SCALAR_INDEX_ENGINE_VERSION] = 5;
    EXPECT_EQ(ResolveJsonPathPresenceSemanticsForBuild(config),
              JsonPathPresenceSemantics::LEGACY_RECURSIVE_NON_EMPTY);

    config[SCALAR_INDEX_ENGINE_VERSION] = 6;
    EXPECT_EQ(ResolveJsonPathPresenceSemanticsForBuild(config),
              JsonPathPresenceSemantics::NON_NULL_TARGET);

    config[SCALAR_INDEX_ENGINE_VERSION] = 7;
    EXPECT_EQ(ResolveJsonPathPresenceSemanticsForBuild(config),
              JsonPathPresenceSemantics::NON_NULL_TARGET);
}

TEST(JsonPathPresenceVersionTest, ConverterBuildsSelectedExistsBitmap) {
    auto schema = MakeJsonSchema();
    auto json_fd = MakeJsonFieldData({
        R"({"a":[]})",
        R"({"a":{}})",
        R"({"a":[null]})",
        R"({"a":{"b":null}})",
        R"({"a":[[],{}]})",
        R"({"a":{"b":0}})",
        R"({"a":1.0})",
        R"({"b":1})",
        R"({"a":null})",
    });

    auto legacy = ConvertJsonToTypedFieldData<double>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"),
        JsonPathPresenceSemantics::LEGACY_RECURSIVE_NON_EMPTY);
    EXPECT_EQ(legacy.non_exist_offsets,
              (std::vector<size_t>{0, 1, 2, 3, 4, 7, 8}));

    auto current = ConvertJsonToTypedFieldData<double>(
        {json_fd},
        schema,
        "/a",
        JsonCastType::FromString("DOUBLE"),
        JsonCastFunction::FromString("unknown"),
        JsonPathPresenceSemantics::NON_NULL_TARGET);
    EXPECT_EQ(current.non_exist_offsets, (std::vector<size_t>{7, 8}));
}

TEST(JsonPathPresenceVersionTest,
     ProductionInvertedArtifactPreservesSelectedExistsBitmap) {
    const std::vector<std::string> raw_jsons = {
        R"({"a":[]})",
        R"({"a":{}})",
        R"({"a":[null]})",
        R"({"b":1})",
        R"({"a":null})",
        R"({"a":1.0})",
    };

    // This is the production artifact path: Build(config) reads the insert
    // binlog, UploadUnified persists non_exist_offsets, and a fresh instance
    // reconstructs Exists() through LoadUnified.
    auto legacy = BuildUploadAndReloadInvertedExists(raw_jsons, 5);
    EXPECT_EQ(legacy,
              (std::vector<bool>{false, false, false, false, false, true}));

    auto current = BuildUploadAndReloadInvertedExists(raw_jsons, 6);
    EXPECT_EQ(current,
              (std::vector<bool>{true, true, true, false, false, true}));
}

template <typename T>
class JsonNumericCastArtifactTest : public testing::Test {};
using JsonNumericArtifactTypes =
    testing::Types<int8_t, int16_t, int32_t, int64_t, double>;
TYPED_TEST_SUITE(JsonNumericCastArtifactTest, JsonNumericArtifactTypes);

TYPED_TEST(JsonNumericCastArtifactTest, IntegerAndDoubleSourcesRoundTrip) {
    using T = TypeParam;
    const std::string cast = std::is_same_v<T, double>
                                 ? "DOUBLE"
                                 : "INT" + std::to_string(sizeof(T) * 8);
    for (bool double_source : {false, true}) {
        SCOPED_TRACE(cast + (double_source ? "/double" : "/int64"));
        const std::vector<std::string> rows = {
            double_source ? R"({"a":2.0})" : R"({"a":2})",
            double_source ? R"({"a":2.5})" : R"({"a":3})",
            double_source ? R"({"a":9007199254740992.0})"
                          : R"({"a":9007199254740993})",
            R"({"a":9223372036854775808})",
            R"({"a":"2"})",
            R"({"a":null})",
            "{}",
            R"({"a":1e400})"};
        auto exists = BuildUploadAndReloadInvertedExists(
            rows, 6, cast, [&](IndexBase* loaded) {
                auto* typed = dynamic_cast<ScalarIndex<T>*>(loaded);
                ASSERT_NE(typed, nullptr);
                EXPECT_EQ(loaded->GetCastType().ToString(), cast);
                const std::vector<bool> expected_valid = {
                    true,
                    !double_source || std::is_same_v<T, double>,
                    sizeof(T) == 8,
                    std::is_same_v<T, double>,
                    false,
                    false,
                    false,
                    false};
                auto valid = typed->IsNotNull();
                auto nulls = typed->IsNull();
                T two = 2;
                auto matches = typed->In(1, &two);
                ASSERT_EQ(valid.size(), rows.size());
                for (size_t i = 0; i < rows.size(); ++i) {
                    EXPECT_EQ(valid[i], expected_valid[i]) << i;
                    EXPECT_EQ(nulls[i], !expected_valid[i]) << i;
                    EXPECT_EQ(matches[i], i == 0) << i;
                }
                if constexpr (sizeof(T) == 8) {
                    const T large = std::is_same_v<T, double> || double_source
                                        ? T(9007199254740992LL)
                                        : T(9007199254740993LL);
                    auto large_matches = typed->In(1, &large);
                    EXPECT_EQ(large_matches.count(), 1);
                    EXPECT_TRUE(large_matches[2]);
                }
            });
        EXPECT_EQ(exists,
                  (std::vector<bool>{
                      true, true, true, true, true, false, false, false}));
    }
}
