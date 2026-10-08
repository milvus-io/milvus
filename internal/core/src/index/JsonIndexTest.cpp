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

#include <cstdint>
#include <map>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <simdjson.h>

#include "NamedType/named_type_impl.hpp"
#include "bitset/bitset.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/Slice.h"
#include "storage/IndexData.h"
#include "common/FieldData.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/JsonScalarIndexWrapper.h"
#include "index/JsonFlatIndex.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "simdjson/error.h"
#include "simdjson/padded_string.h"
#include "storage/FileManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

namespace {

struct FileSliceSizeGuard {
    explicit FileSliceSizeGuard(int64_t slice_size)
        : old_slice_size_(FILE_SLICE_SIZE.load()) {
        FILE_SLICE_SIZE.store(slice_size);
    }

    ~FileSliceSizeGuard() {
        FILE_SLICE_SIZE.store(old_slice_size_);
    }

    int64_t old_slice_size_;
};

struct LoadedJsonOffsetStats {
    int64_t count;
    int64_t exists_count;
    int64_t null_count;
};

LoadedJsonOffsetStats
BuildAndLoadJsonInvertedIndexForOffsetRegression(
    const std::vector<std::string>& json_raw_data,
    const std::vector<uint8_t>* valid_data = nullptr) {
    constexpr int64_t collection_id = 1;
    constexpr int64_t partition_id = 2;
    constexpr int64_t segment_id = 3;
    constexpr int64_t field_id = 101;
    constexpr int64_t index_build_id = 4000;
    constexpr int64_t index_version = 4000;

    auto field_meta = milvus::segcore::gen_field_meta(
        collection_id, partition_id, segment_id, field_id, DataType::JSON);
    field_meta.field_schema.set_nullable(valid_data != nullptr);
    auto index_meta =
        gen_index_meta(segment_id, field_id, index_build_id, index_version);

    auto root_path =
        (boost::filesystem::path(TestLocalPath) /
         boost::filesystem::unique_path("json-offset-regression-%%%%-%%%%"))
            .string();
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    ChunkManagerWrapper cm_w(cm);

    storage::FileManagerContext build_ctx(field_meta, index_meta, cm, fs);
    index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index::INVERTED_INDEX_TYPE;
    create_index_info.json_cast_type = JsonCastType::FromString("DOUBLE");
    create_index_info.json_path = "/a";

    auto build_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        create_index_info, build_ctx);
    auto json_index = std::unique_ptr<JsonInvertedIndex<double>>(
        static_cast<JsonInvertedIndex<double>*>(build_index.release()));

    std::vector<milvus::Json> jsons;
    jsons.reserve(json_raw_data.size());
    for (auto& json : json_raw_data) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }

    auto json_field = std::make_shared<FieldData<milvus::Json>>(
        DataType::JSON, valid_data != nullptr);
    if (valid_data == nullptr) {
        json_field->add_json_data(jsons);
    } else {
        arrow::BinaryBuilder builder;
        for (size_t i = 0; i < jsons.size(); ++i) {
            if (((*valid_data)[i / 8] & (1U << (i % 8))) == 0) {
                AssertInfo(builder.AppendNull().ok(),
                           "failed to append null JSON");
                continue;
            }
            auto data = jsons[i].data();
            AssertInfo(
                builder.Append(data.data(), static_cast<int32_t>(data.size()))
                    .ok(),
                "failed to append JSON");
        }

        std::shared_ptr<arrow::Array> json_array;
        AssertInfo(builder.Finish(&json_array).ok(),
                   "failed to finish JSON array");
        json_field->FillFieldData(json_array);
    }
    json_index->BuildWithFieldData({json_field});

    auto stats = json_index->Upload();
    auto index_files = stats->GetIndexFiles();

    build_ctx.set_for_loading_index(true);
    auto load_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        create_index_info, build_ctx);
    auto loaded_json_index = std::unique_ptr<JsonInvertedIndex<double>>(
        static_cast<JsonInvertedIndex<double>*>(load_index.release()));

    Config load_config;
    load_config[index::INDEX_FILES] = index_files;
    load_config[milvus::LOAD_PRIORITY] =
        milvus::proto::common::LoadPriority::HIGH;

    loaded_json_index->Load(milvus::tracer::TraceContext{}, load_config);
    auto exists = loaded_json_index->Exists();
    auto nulls = loaded_json_index->IsNull();
    return {loaded_json_index->Count(),
            static_cast<int64_t>(exists.count()),
            static_cast<int64_t>(nulls.count())};
}

}  // namespace

TEST(JsonIndexTest, TestJsonContains) {
    milvus::test::ExprBatchSizeGuard batch_size_guard(8);
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
        R"({"a": [0, 2, 3]})",
        R"({"a": [{"b": 1}, 2.0, 3.0, "4", true, [1, 3.0], null]})",
        R"({"a": [9007199254740992]})",
        R"({"a": [9007199254740993]})",
        R"({"a": [9007199254740994]})",
    };

    auto json_path = "/a";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    index::CreateIndexInfo cii_array_double;
    cii_array_double.index_type = index::INVERTED_INDEX_TYPE;
    cii_array_double.json_cast_type = JsonCastType::FromString("ARRAY_DOUBLE");
    cii_array_double.json_path = json_path;
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        cii_array_double, file_manager_ctx);
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
    json_index->create_reader(milvus::index::SetBitsetSealed);

    auto segment = segcore::CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, json_path},
                                    {JSON_CAST_TYPE, "ARRAY_DOUBLE"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(json_index));
    segment->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    std::vector<std::tuple<proto::plan::GenericValue, std::vector<int64_t>>>
        test_cases;

    proto::plan::GenericValue value;
    value.set_int64_val(1);
    test_cases.push_back(std::make_tuple(value, std::vector<int64_t>{17, 18}));

    proto::plan::GenericValue value2;
    value2.set_int64_val(2);
    test_cases.push_back(
        std::make_tuple(value2, std::vector<int64_t>{17, 18, 23, 24}));

    // proto::plan::GenericValue value2;
    // value2.set_bool_val(true);
    // test_cases.push_back(std::make_tuple(value2, std::vector<int64_t>{8}));

    // proto::plan::GenericValue value3;
    // value3.set_string_val("x");
    // test_cases.push_back(std::make_tuple(value3, std::vector<int64_t>{9}));

    for (auto& test_case : test_cases) {
        auto query_value = std::get<0>(test_case);
        auto expr = std::make_shared<expr::JsonContainsExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
            proto::plan::JSONContainsExpr_JSONOp::
                JSONContainsExpr_JSONOp_Contains,
            true,
            std::vector<proto::plan::GenericValue>{query_value});

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

    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);
    proto::plan::GenericValue string_value;
    string_value.set_string_val("4");
    auto mixed_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
        proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        false,
        std::vector<proto::plan::GenericValue>{int_value, string_value});
    auto mixed_plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, mixed_expr);
    auto mixed_result = query::ExecuteQueryExpr(
        mixed_plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
    EXPECT_EQ(mixed_result.count(), 3);
    EXPECT_TRUE(mixed_result[17]);
    EXPECT_TRUE(mixed_result[18]);
    EXPECT_TRUE(mixed_result[24]);

    proto::plan::GenericValue large_int_value;
    large_int_value.set_int64_val(9007199254740993LL);
    auto large_int_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{large_int_value});
    EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
        large_int_expr, segment.get(), json_raw_data.size()));
    EXPECT_EQ(milvus::test::EvalExprBatchSizes(
                  large_int_expr, segment.get(), json_raw_data.size()),
              (std::vector<int64_t>{8, 8, 8, 4}));
    auto large_int_plan = std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, large_int_expr);
    auto large_int_result = query::ExecuteQueryExpr(
        large_int_plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
    EXPECT_EQ(large_int_result.count(), 1);
    EXPECT_TRUE(large_int_result[26]);
}

TEST(JsonIndexTest, TestJsonCast) {
    std::vector<std::string> json_raw_data = {
        R"(1)",
        R"({"a": 1.0})",
        R"({"a": 1})",
        R"({"a": "1.0"})",
        R"({"a": true})",
        R"({"a": [1, 2, 3]})",
        R"({"a": {"b": 1}})",
    };

    auto json_path = "/a";
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    index::CreateIndexInfo cii_cast;
    cii_cast.index_type = index::INVERTED_INDEX_TYPE;
    cii_cast.json_cast_type = JsonCastType::FromString("DOUBLE");
    cii_cast.json_path = json_path;
    cii_cast.json_cast_function = "STRING_TO_DOUBLE";
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        cii_cast, file_manager_ctx);
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
    json_index->create_reader(milvus::index::SetBitsetSealed);

    auto segment = segcore::CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.cache_index =
        CreateTestCacheIndex("", std::move(json_index));
    load_index_info.index_params = {{JSON_PATH, json_path},
                                    {JSON_CAST_TYPE, "DOUBLE"}};
    segment->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    std::vector<std::tuple<proto::plan::GenericValue, std::vector<int64_t>>>
        test_cases;

    proto::plan::GenericValue value;
    value.set_int64_val(1);
    test_cases.push_back(std::make_tuple(value, std::vector<int64_t>{1, 2, 3}));
    for (auto& test_case : test_cases) {
        auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"a"}, true),
            proto::plan::OpType::Equal,
            value,
            std::vector<proto::plan::GenericValue>{});

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

// Verify that JsonInvertedIndex::Serialize correctly produces sliced
// null_offset and non_exist_offset files, and that CompactIndexDatasByKey
// can reassemble each independently from the shared _meta_slice.
//
// This reproduces the bug where both offset files shared a single
// _meta_slice after Disassemble, but LoadIndexMetas downloaded them
// separately. The old CompactIndexDatas fails because it tries to
// find ALL slices referenced in _meta_slice, not just the target key's.
TEST(JsonIndexTest, TestSlicedOffsetFilesLoadIndependently) {
    // Use a small slice size so both offset files get sliced.
    FileSliceSizeGuard slice_size_guard(64);

    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    index::CreateIndexInfo cii;
    cii.index_type = index::INVERTED_INDEX_TYPE;
    cii.json_cast_type = JsonCastType::FromString("DOUBLE");
    cii.json_path = "/a";
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        cii, file_manager_ctx);
    auto json_index = std::unique_ptr<JsonInvertedIndex<double>>(
        static_cast<JsonInvertedIndex<double>*>(inv_index.release()));

    // Build with enough rows to produce large null_offset and
    // non_exist_offset arrays (each > FILE_SLICE_SIZE = 64 bytes).
    // - invalid row   -> null_offset + non_exist_offset (8 bytes per entry)
    // - {"b": 1}      -> null_offset + non_exist_offset (key "a" doesn't
    //                    exist, so the typed comparison value is invalid)
    // - {"a": 1.0}    -> valid data (neither offset)
    // 20 invalid + 20 missing-path rows make both files slice reliably.
    constexpr int kInvalidRows = 20;
    constexpr int kMissingPathRows = 20;
    constexpr int kValidRows = 10;
    std::vector<milvus::Json> jsons;
    for (int i = 0; i < kInvalidRows; i++) {
        jsons.push_back(milvus::Json(
            simdjson::padded_string(std::string_view(R"({"a": 1.0})"))));
    }
    for (int i = 0; i < kMissingPathRows; i++) {
        jsons.push_back(milvus::Json(
            simdjson::padded_string(std::string_view(R"({"b": 1})"))));
    }
    for (int i = 0; i < kValidRows; i++) {
        jsons.push_back(milvus::Json(
            simdjson::padded_string(std::string_view(R"({"a": 1.0})"))));
    }

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    std::vector<uint8_t> valid_data((jsons.size() + 7) / 8, 0xFF);
    for (size_t i = 0; i < kInvalidRows; ++i) {
        valid_data[i / 8] &= ~(1U << (i % 8));
    }

    arrow::BinaryBuilder builder;
    for (size_t i = 0; i < jsons.size(); ++i) {
        auto is_valid = (valid_data[i / 8] >> (i % 8)) & 1U;
        if (!is_valid) {
            ASSERT_TRUE(builder.AppendNull().ok());
            continue;
        }
        auto data = jsons[i].data();
        ASSERT_TRUE(
            builder.Append(data.data(), static_cast<int32_t>(data.size()))
                .ok());
    }

    std::shared_ptr<arrow::Array> json_array;
    ASSERT_TRUE(builder.Finish(&json_array).ok());
    json_field->FillFieldData(json_array);
    json_index->BuildWithFieldData({json_field});
    json_index->finish();

    // Serialize calls Disassemble which creates a shared _meta_slice.
    auto binary_set = json_index->Serialize({});

    // Verify slicing occurred: _meta_slice exists, originals are gone.
    ASSERT_TRUE(binary_set.Contains(INDEX_FILE_SLICE_META))
        << "_meta_slice should exist when both offset files are sliced";
    ASSERT_FALSE(binary_set.Contains(INDEX_NULL_OFFSET_FILE_NAME))
        << "null_offset should be sliced into parts";
    ASSERT_FALSE(binary_set.Contains(INDEX_NON_EXIST_OFFSET_FILE_NAME))
        << "non_exist_offset should be sliced into parts";

    auto slice_meta_bin = binary_set.binary_map_.at(INDEX_FILE_SLICE_META);
    auto slice_meta = Config::parse(
        std::string(reinterpret_cast<const char*>(slice_meta_bin->data.get()),
                    slice_meta_bin->size));

    auto slice_num_for = [&](const std::string& target_key) {
        for (const auto& item : slice_meta[META]) {
            if (item[NAME] == target_key) {
                return static_cast<int>(item[SLICE_NUM]);
            }
        }
        return 0;
    };

    // Helper: build a partial index_datas map containing only slices for
    // the given key plus _meta_slice (simulates LoadIndexToMemory).
    auto make_partial_datas = [&](const std::string& target_key) {
        std::map<std::string, std::unique_ptr<storage::DataCodec>> result;
        result[INDEX_FILE_SLICE_META] = std::make_unique<storage::IndexData>(
            slice_meta_bin->data.get(), slice_meta_bin->size);
        for (int i = 0; i < slice_num_for(target_key); ++i) {
            auto slice_name = GenSlicedFileName(target_key, i);
            auto bin = binary_set.binary_map_.at(slice_name);
            result[slice_name] = std::make_unique<storage::IndexData>(
                bin->data.get(), bin->size);
        }
        return result;
    };

    // --- Verify the bug: old CompactIndexDatas fails with partial data ---
    {
        auto partial = make_partial_datas(INDEX_NULL_OFFSET_FILE_NAME);
        ASSERT_GT(partial.size(), 1);
        // _meta_slice references both null_offset and non_exist_offset slices,
        // but only null_offset slices are in the map, so it asserts.
        EXPECT_THROW(CompactIndexDatas(partial), std::exception);
    }

    // --- Verify the fix: CompactIndexDatasByKey with null_offset ---
    {
        auto partial = make_partial_datas(INDEX_NULL_OFFSET_FILE_NAME);
        auto slice_meta = std::move(partial.at(INDEX_FILE_SLICE_META));
        auto result = CompactIndexDatasByKey(
            INDEX_NULL_OFFSET_FILE_NAME, std::move(slice_meta), partial);
        EXPECT_GT(result.codecs_.size(), 0);
        EXPECT_EQ(result.size_,
                  (kInvalidRows + kMissingPathRows) * sizeof(size_t));
    }

    // --- Verify the fix: CompactIndexDatasByKey with non_exist_offset ---
    {
        auto partial = make_partial_datas(INDEX_NON_EXIST_OFFSET_FILE_NAME);
        auto slice_meta = std::move(partial.at(INDEX_FILE_SLICE_META));
        auto result = CompactIndexDatasByKey(
            INDEX_NON_EXIST_OFFSET_FILE_NAME, std::move(slice_meta), partial);
        EXPECT_GT(result.codecs_.size(), 0);
        EXPECT_EQ(result.size_,
                  (kInvalidRows + kMissingPathRows) * sizeof(size_t));
    }
}

TEST(JsonIndexTest, TestLoadWithOnlySlicedNonExistOffsets) {
    FileSliceSizeGuard slice_size_guard(64);

    std::vector<std::string> json_raw_data;
    for (int i = 0; i < 20; ++i) {
        json_raw_data.emplace_back(R"({"b": 1})");
    }
    for (int i = 0; i < 10; ++i) {
        json_raw_data.emplace_back(R"({"a": 1.0})");
    }

    auto stats =
        BuildAndLoadJsonInvertedIndexForOffsetRegression(json_raw_data);
    EXPECT_EQ(stats.count, json_raw_data.size());
    EXPECT_EQ(stats.exists_count, 10);
}

TEST(JsonIndexTest, TestLoadWithOnlySlicedNullOffsets) {
    FileSliceSizeGuard slice_size_guard(64);

    std::vector<std::string> json_raw_data;
    for (int i = 0; i < 20; ++i) {
        json_raw_data.emplace_back(R"({"a": 1.0})");
    }
    for (int i = 0; i < 10; ++i) {
        json_raw_data.emplace_back(R"({"a": 1.0})");
    }
    std::vector<uint8_t> valid_data((json_raw_data.size() + 7) / 8, 0xFF);
    for (size_t i = 0; i < 20; ++i) {
        valid_data[i / 8] &= ~(1U << (i % 8));
    }

    auto stats = BuildAndLoadJsonInvertedIndexForOffsetRegression(json_raw_data,
                                                                  &valid_data);
    EXPECT_EQ(stats.count, json_raw_data.size());
    EXPECT_EQ(stats.exists_count, 10);
    EXPECT_EQ(stats.null_count, 20);
}

TEST(JsonIndexTest, TypedStringArrayContainsAllAnyAndExistsAfterReload) {
    const std::vector<std::string> rows{R"({"a":["apple","banana","apple"]})",
                                        R"({"a":["apple"]})",
                                        R"({"a":[]})",
                                        R"({"a":null})",
                                        R"({"a":"apple"})",
                                        R"({"a":[1,"banana",null]})",
                                        R"({"b":[]})",
                                        R"(null)",
                                        R"({})"};
    auto schema = std::make_shared<Schema>();
    auto fid = schema->AddDebugField("json", DataType::JSON, true);
    const auto root_path =
        (boost::filesystem::path(TestLocalPath) /
         boost::filesystem::unique_path("json-array-routing-%%%%-%%%%"))
            .string();
    auto config = gen_local_storage_config(root_path);
    auto cm = storage::CreateChunkManager(config);
    auto fs = storage::InitArrowFileSystem(config);
    ChunkManagerWrapper cm_guard(cm);
    auto field_meta =
        segcore::gen_field_meta(1, 2, 3, fid.get(), DataType::JSON);
    field_meta.field_schema.set_nullable(true);
    auto index_meta = gen_index_meta(3, fid.get(), 4100, 4100);
    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    CreateIndexInfo info;
    info.index_type = INVERTED_INDEX_TYPE;
    info.json_cast_type = JsonCastType::FromString("ARRAY_VARCHAR");
    info.json_path = "/a";
    info.tantivy_index_version = 7;

    arrow::BinaryBuilder builder;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i == 8) {
            ASSERT_TRUE(builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(builder.Append(rows[i].data(), rows[i].size()).ok());
        }
    }
    std::shared_ptr<arrow::Array> arrow_rows;
    ASSERT_TRUE(builder.Finish(&arrow_rows).ok());
    auto json_field = std::make_shared<FieldData<Json>>(DataType::JSON, true);
    json_field->FillFieldData(arrow_rows);
    auto built = IndexFactory::GetInstance().CreateJsonIndex(info, ctx);
    auto* built_scalar = dynamic_cast<ScalarIndex<std::string>*>(built.get());
    ASSERT_NE(built_scalar, nullptr);
    built_scalar->BuildWithFieldData({json_field});
    auto stats = built->Upload();
    ctx.set_for_loading_index(true);
    auto loaded = IndexFactory::GetInstance().CreateJsonIndex(info, ctx);
    Config load_config;
    load_config[INDEX_FILES] = stats->GetIndexFiles();
    loaded->Load(tracer::TraceContext{}, load_config);
    auto* scalar = dynamic_cast<ScalarIndex<std::string>*>(loaded.get());
    ASSERT_NE(scalar, nullptr);
    ASSERT_EQ(scalar->Count(), rows.size());
    const auto exists = scalar->Exists();
    ASSERT_EQ(exists.size(), rows.size());
    // Milvus JSON EXISTS excludes JSON null and empty arrays. A non-empty
    // scalar value still exists when the requested ARRAY cast fails.
    const std::vector<bool> expected_exists{
        true, true, false, false, true, true, false, false, false};
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(exists[i], expected_exists[i]) << "row " << i;
    }
    const auto known = scalar->IsNotNull();
    const std::vector<bool> expected_known{
        true, true, true, false, false, true, false, false, false};
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(known[i], expected_known[i]) << "row " << i;
    }
    const std::string apple = "apple";
    const auto apple_hits = scalar->In(1, &apple);
    EXPECT_EQ(apple_hits.count(), 2);
    EXPECT_TRUE(apple_hits[0]);
    EXPECT_TRUE(apple_hits[1]);

    auto segment = segcore::CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_info;
    load_info.field_id = fid.get();
    load_info.field_type = DataType::JSON;
    load_info.index_params = {{JSON_PATH, "/a"},
                              {JSON_CAST_TYPE, "ARRAY_VARCHAR"}};
    load_info.cache_index =
        CreateTestCacheIndex("json_array_string_reload", std::move(loaded));
    segment->LoadIndex(load_info);
    auto binlogs =
        PrepareSingleFieldInsertBinlog(1, 2, 3, fid.get(), {json_field}, cm);
    segment->LoadFieldData(binlogs);
    for (const auto op : {proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
                          proto::plan::JSONContainsExpr_JSONOp_ContainsAll}) {
        for (const auto& targets : std::vector<std::vector<std::string>>{
                 {"apple"}, {"apple", "banana", "apple"}, {"missing"}}) {
            std::vector<proto::plan::GenericValue> values;
            for (const auto& target : targets) {
                proto::plan::GenericValue value;
                value.set_string_val(target);
                values.push_back(std::move(value));
            }
            auto expr = std::make_shared<expr::JsonContainsExpr>(
                expr::ColumnInfo(fid, DataType::JSON, {"a"}, true),
                op,
                true,
                values);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            auto result = query::ExecuteQueryExpr(
                plan, segment.get(), rows.size(), MAX_TIMESTAMP);
            const bool missing = targets[0] == "missing";
            const bool multi = targets.size() > 1;
            for (size_t i = 0; i < rows.size(); ++i) {
                bool expected = !missing && (i == 0 || (!multi && i == 1));
                if (!missing && multi &&
                    op == proto::plan::JSONContainsExpr_JSONOp_ContainsAny) {
                    expected = i == 0 || i == 1 || i == 5;
                }
                EXPECT_EQ(result[i], expected) << "row " << i;
            }
        }
    }
}

TEST(JsonIndexTest, ArrayContainsContainerValidityMatchesRawForNaNNegatives) {
    using Value = proto::plan::GenericValue;
    auto number = [](double v) {
        Value value;
        value.set_float_val(v);
        return value;
    };
    auto text = [](const std::string& v) {
        Value value;
        value.set_string_val(v);
        return value;
    };
    const double nan = std::numeric_limits<double>::quiet_NaN();
    for (const auto& cast_type : {std::string("ARRAY_DOUBLE"),
                                  std::string("ARRAY_VARCHAR"),
                                  std::string("JSON")}) {
        SCOPED_TRACE(cast_type);
        const bool strings = cast_type == "ARRAY_VARCHAR";
        const bool flat = cast_type == "JSON";
        const std::vector<std::string> rows =
            strings
                ? std::vector<std::string>{R"({"a":["apple","NaN","apple"]})",
                                           R"({"a":[]})",
                                           R"({"a":null})",
                                           R"({"b":[]})",
                                           R"({"a":"apple"})",
                                           R"({"a":[1,true,null]})",
                                           R"({"a":["NaN",1,null]})",
                                           R"(null)",
                                           R"({})"}
                : std::vector<std::string>{R"({"a":[1,2,1]})",
                                           R"({"a":[]})",
                                           R"({"a":null})",
                                           R"({"b":[]})",
                                           R"({"a":1})",
                                           R"({"a":["wrong"]})",
                                           R"({"a":[2,"wrong",null]})",
                                           R"(null)",
                                           R"({})"};
        const std::vector<bool> containers{
            true, true, false, false, false, true, true, false, false};
        const std::vector<std::vector<Value>> members =
            strings ? std::vector<std::vector<Value>>{{text("apple"),
                                                       text("NaN"),
                                                       text("apple")},
                                                      {},
                                                      {},
                                                      {},
                                                      {},
                                                      {number(1)},
                                                      {text("NaN"), number(1)},
                                                      {},
                                                      {}}
                    : std::vector<std::vector<Value>>{
                          {number(1), number(2), number(1)},
                          {},
                          {},
                          {},
                          {},
                          {text("wrong")},
                          {number(2), text("wrong")},
                          {},
                          {}};
        auto schema = std::make_shared<Schema>();
        const auto fid = schema->AddDebugField("json", DataType::JSON, true);
        const auto root_path = (boost::filesystem::path(TestLocalPath) /
                                boost::filesystem::unique_path(
                                    "json-container-validity-%%%%-%%%%"))
                                   .string();
        auto storage_config = gen_local_storage_config(root_path);
        auto cm = storage::CreateChunkManager(storage_config);
        auto fs = storage::InitArrowFileSystem(storage_config);
        ChunkManagerWrapper cm_guard(cm);
        auto field_meta =
            segcore::gen_field_meta(1, 2, 3, fid.get(), DataType::JSON);
        field_meta.field_schema.set_nullable(true);
        storage::FileManagerContext ctx(
            field_meta, gen_index_meta(3, fid.get(), 4200, 4200), cm, fs);
        CreateIndexInfo info;
        info.index_type = INVERTED_INDEX_TYPE;
        info.json_cast_type = JsonCastType::FromString(cast_type);
        info.json_path = flat ? "" : "/a";
        info.tantivy_index_version = 7;
        arrow::BinaryBuilder builder;
        for (size_t i = 0; i < rows.size(); ++i) {
            if (i == 8) {
                ASSERT_TRUE(builder.AppendNull().ok());
            } else {
                ASSERT_TRUE(
                    builder.Append(rows[i].data(), rows[i].size()).ok());
            }
        }
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto field = std::make_shared<FieldData<Json>>(DataType::JSON, true);
        field->FillFieldData(array);
        auto built = IndexFactory::GetInstance().CreateJsonIndex(info, ctx);
        if (!strings && !flat) {
            dynamic_cast<ScalarIndex<double>*>(built.get())
                ->BuildWithFieldData({field});
        } else {
            dynamic_cast<ScalarIndex<std::string>*>(built.get())
                ->BuildWithFieldData({field});
        }
        auto stats = built->UploadUnified({});
        ctx.set_for_loading_index(true);
        auto loaded = IndexFactory::GetInstance().CreateJsonIndex(info, ctx);
        Config config;
        config[INDEX_FILES] = stats->GetIndexFiles();
        config[ENABLE_MMAP] = false;
        if (!strings && !flat) {
            dynamic_cast<ScalarIndex<double>*>(loaded.get())
                ->LoadUnified(config);
        } else {
            dynamic_cast<ScalarIndex<std::string>*>(loaded.get())
                ->LoadUnified(config);
        }
        auto raw_segment = segcore::CreateSealedSegment(schema);
        auto indexed_segment = segcore::CreateSealedSegment(schema);
        auto binlog =
            PrepareSingleFieldInsertBinlog(1, 2, 3, fid.get(), {field}, cm);
        raw_segment->LoadFieldData(binlog);
        indexed_segment->LoadFieldData(binlog);
        segcore::LoadIndexInfo index_info;
        index_info.field_id = fid.get();
        index_info.field_type = DataType::JSON;
        index_info.index_params = {{JSON_PATH, info.json_path},
                                   {JSON_CAST_TYPE, cast_type}};
        index_info.cache_index = CreateTestCacheIndex(
            "json-container-" + cast_type, std::move(loaded));
        indexed_segment->LoadIndex(index_info);
        std::vector<std::vector<Value>> queries{
            {number(nan)}, {number(nan), number(1)}, {number(1), number(nan)}};
        if (!flat) {
            if (strings) {
                queries.push_back({text("apple")});
                queries.push_back({text("apple"), text("NaN")});
            } else {
                queries.push_back({number(1)});
                queries.push_back({number(1), number(2)});
            }
        }
        for (const auto& targets : queries) {
            for (auto op : {proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
                            proto::plan::JSONContainsExpr_JSONOp_ContainsAll}) {
                auto contains = std::make_shared<expr::JsonContainsExpr>(
                    expr::ColumnInfo(fid, DataType::JSON, {"a"}, true),
                    op,
                    true,
                    targets);
                for (bool negate : {false, true}) {
                    expr::TypedExprPtr predicate = contains;
                    if (negate) {
                        predicate = std::make_shared<expr::LogicalUnaryExpr>(
                            expr::LogicalUnaryExpr::OpType::LogicalNot,
                            contains);
                    }
                    auto plan = std::make_shared<plan::FilterBitsNode>(
                        DEFAULT_PLANNODE_ID, predicate);
                    auto raw = query::ExecuteQueryExpr(
                        plan, raw_segment.get(), rows.size(), MAX_TIMESTAMP);
                    auto indexed =
                        query::ExecuteQueryExpr(plan,
                                                indexed_segment.get(),
                                                rows.size(),
                                                MAX_TIMESTAMP);
                    for (size_t row = 0; row < rows.size(); ++row) {
                        bool match =
                            op ==
                            proto::plan::JSONContainsExpr_JSONOp_ContainsAll;
                        for (const auto& target : targets) {
                            bool found = false;
                            for (const auto& member : members[row]) {
                                if (target.has_float_val() &&
                                    member.has_float_val()) {
                                    found |= target.float_val() ==
                                             member.float_val();
                                } else if (target.has_string_val() &&
                                           member.has_string_val()) {
                                    found |= target.string_val() ==
                                             member.string_val();
                                }
                            }
                            if (op == proto::plan::
                                          JSONContainsExpr_JSONOp_ContainsAll) {
                                match &= found;
                            } else {
                                match |= found;
                            }
                        }
                        const bool expected =
                            containers[row] && (negate ? !match : match);
                        EXPECT_EQ(raw[row], expected)
                            << "raw row " << row << " negate " << negate;
                        EXPECT_EQ(indexed[row], expected)
                            << "index row " << row << " negate " << negate;
                    }
                }
            }
        }
    }
}
