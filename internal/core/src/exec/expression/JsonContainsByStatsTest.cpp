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

#include <arrow/builder.h>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <simdjson.h>
#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "bitset/bitset.h"
#include "bitset/detail/element_vectorized.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/Json.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/expression/BinaryRangeExpr.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExistsExpr.h"
#include "exec/expression/JsonContainsExpr.h"
#include "exec/expression/TermExpr.h"
#include "exec/expression/UnaryExpr.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/IndexStats.h"
#include "index/IndexFactory.h"
#include "index/JsonFlatIndex.h"
#include "index/JsonScalarIndexWrapper.h"
#include "index/json_stats/JsonKeyStats.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "parquet/arrow/writer.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentSealed.h"
#include "simdjson/padded_string.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

class JsonStatsProjectionTestAccessor {
 public:
    static bool
    IsInMultiFieldColumnGroup(const JsonKeyStats& stats,
                              const std::string& field_name) {
        return stats.shredding_columns_.at(field_name)
            ->IsInMultiFieldColumnGroup();
    }
};

namespace {

class JsonInvertedIndexWithSelectablePresence
    : public JsonInvertedIndex<double> {
 public:
    using Base = JsonInvertedIndex<double>;
    using Base::Base;

    void
    BuildWithPresenceSemantics(const std::vector<FieldDataPtr>& field_datas,
                               JsonPathPresenceSemantics presence_semantics) {
        this->BuildInvertedWithJsonFieldData(field_datas, presence_semantics);
    }
};

bool
IsValidAt(const std::vector<uint8_t>& valid_data, size_t i) {
    return ((valid_data[i >> 3] >> (i & 0x07)) & 1) != 0;
}

std::shared_ptr<arrow::BinaryArray>
MakeNullableJsonArray(const std::vector<std::string>& json_strings,
                      const std::vector<uint8_t>& valid_data) {
    arrow::BinaryBuilder builder;
    for (size_t i = 0; i < json_strings.size(); ++i) {
        auto status = IsValidAt(valid_data, i) ? builder.Append(json_strings[i])
                                               : builder.AppendNull();
        AssertInfo(status.ok(),
                   "failed to build nullable JSON Arrow array: {}",
                   status.ToString());
    }

    std::shared_ptr<arrow::Array> array;
    auto status = builder.Finish(&array);
    AssertInfo(status.ok(),
               "failed to finish nullable JSON Arrow array: {}",
               status.ToString());
    return std::static_pointer_cast<arrow::BinaryArray>(array);
}

struct BuiltJsonStatsIndex {
    storage::FileManagerContext ctx;
    Config load_config;
    std::vector<std::string> index_files;
    std::string stats_base_path;
    milvus_storage::ArrowFileSystemPtr fs;
    int64_t json_stats_data_format;
};

BuiltJsonStatsIndex
BuildJsonStatsIndex(
    const std::vector<std::string>& json_strings,
    const milvus::FieldId json_fid,
    const std::string& root_path,
    int64_t collection_id,
    int64_t partition_id,
    int64_t segment_id,
    int64_t field_id,
    int64_t build_id,
    int64_t version_id,
    const std::vector<uint8_t>* valid_data = nullptr,
    int64_t json_stats_max_shredding_columns = 1024,
    int64_t json_stats_data_format = JSON_STATS_DATA_FORMAT_V4) {
    std::vector<milvus::Json> data;
    data.reserve(json_strings.size());
    for (const auto& s : json_strings) {
        data.emplace_back(simdjson::padded_string(s));
    }

    auto nullable = valid_data != nullptr;
    auto field_data =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, nullable);
    if (valid_data != nullptr) {
        field_data->FillFieldData(
            MakeNullableJsonArray(json_strings, *valid_data));
    } else {
        field_data->add_json_data(data);
    }

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);

    proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(proto::schema::DataType::JSON);
    field_schema.set_fieldid(json_fid.get());
    field_schema.set_nullable(nullable);

    storage::FieldDataMeta field_meta{
        collection_id, partition_id, segment_id, field_id, field_schema};
    storage::IndexMeta index_meta{segment_id, field_id, build_id, version_id};

    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    auto stats_base_path =
        storage::GenRemoteJsonStatsPathPrefix(chunk_manager,
                                              build_id,
                                              version_id,
                                              collection_id,
                                              partition_id,
                                              segment_id,
                                              field_id,
                                              json_stats_data_format);

    auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                root_path,
                                collection_id,
                                partition_id,
                                segment_id,
                                field_id,
                                0);
    chunk_manager->Write(
        log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);
    ctx.set_stats_base_path(stats_base_path);

    Config build_config;
    build_config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

    auto builder = std::make_shared<JsonKeyStats>(
        ctx, false, json_stats_max_shredding_columns);
    builder->SetDataFormatVersion(json_stats_data_format);
    builder->Build(build_config);

    auto create_index_result = builder->Upload(build_config);
    auto index_files = create_index_result->GetIndexFiles();

    Config load_config;
    load_config["index_files"] = index_files;
    load_config[milvus::LOAD_PRIORITY] =
        milvus::proto::common::LoadPriority::HIGH;
    load_config[STATS_BASE_PATH_KEY] = stats_base_path;

    return BuiltJsonStatsIndex{ctx,
                               std::move(load_config),
                               std::move(index_files),
                               std::move(stats_base_path),
                               std::move(fs),
                               json_stats_data_format};
}

std::shared_ptr<JsonKeyStats>
LoadBuiltJsonStatsIndex(const BuiltJsonStatsIndex& built_index) {
    auto reader = std::make_shared<JsonKeyStats>(built_index.ctx, true);
    reader->SetDataFormatVersion(built_index.json_stats_data_format);
    reader->Load(milvus::tracer::TraceContext{}, built_index.load_config);
    return reader;
}

std::shared_ptr<JsonKeyStats>
BuildAndLoadJsonKeyStats(
    const std::vector<std::string>& json_strings,
    const milvus::FieldId json_fid,
    const std::string& root_path,
    int64_t collection_id,
    int64_t partition_id,
    int64_t segment_id,
    int64_t field_id,
    int64_t build_id,
    int64_t version_id,
    const std::vector<uint8_t>* valid_data = nullptr,
    int64_t json_stats_max_shredding_columns = 1024,
    int64_t json_stats_data_format = JSON_STATS_DATA_FORMAT_V4) {
    auto built_index = BuildJsonStatsIndex(json_strings,
                                           json_fid,
                                           root_path,
                                           collection_id,
                                           partition_id,
                                           segment_id,
                                           field_id,
                                           build_id,
                                           version_id,
                                           valid_data,
                                           json_stats_max_shredding_columns,
                                           json_stats_data_format);
    return LoadBuiltJsonStatsIndex(built_index);
}

TEST(JsonStatsInvalidNumberTest,
     MatchesRawScanWithoutDroppingSiblingOrArrayElements) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    const std::vector<std::string> json_raw_data = {
        R"({"bad":1e400,"ok":7,"arr":[1e400,7,8],"large":9007199254740993,"empty":"","obj":{}})",
        R"({"bad":1.5,"ok":8,"arr":[1e400],"large":9007199254740992,"empty":"filled","obj":{"child":null}})",
        R"({"bad":18446744073709551616,"ok":9,"arr":[7,8],"large":9223372036854775808,"empty":null,"obj":null})",
    };

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1211,
                                          2211,
                                          3211,
                                          json_fid.get(),
                                          5211,
                                          1,
                                          nullptr);

    auto shared_stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                                 json_fid,
                                                 TestLocalPath,
                                                 1212,
                                                 2212,
                                                 3212,
                                                 json_fid.get(),
                                                 5212,
                                                 1,
                                                 nullptr,
                                                 0);

    auto make_json_field = [&] {
        auto field =
            std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
        std::vector<milvus::Json> jsons;
        jsons.reserve(json_raw_data.size());
        for (const auto& json : json_raw_data) {
            jsons.emplace_back(simdjson::padded_string(json));
        }
        field->add_json_data(jsons);
        return field;
    };
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto make_stats_segment = [&](const std::shared_ptr<JsonKeyStats>& index,
                                  int64_t segment_id) {
        auto segment = segcore::CreateSealedSegment(
            schema, milvus::empty_index_meta, segment_id);
        auto* sealed =
            dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
        EXPECT_NE(sealed, nullptr);
        sealed->SetJsonStatsForTesting(json_fid, index);
        auto load_info = PrepareSingleFieldInsertBinlog(
            0, 0, 0, json_fid.get(), {make_json_field()}, cm);
        segment->LoadFieldData(load_info);
        segment->DropFieldData(json_fid);
        EXPECT_FALSE(segment->HasFieldData(json_fid));
        return segment;
    };
    auto stats_segment = make_stats_segment(stats, 5282801);
    auto shared_stats_segment = make_stats_segment(shared_stats, 5282802);

    auto raw_segment =
        segcore::CreateSealedSegment(schema, milvus::empty_index_meta, 5282803);
    auto raw_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {make_json_field()}, cm);
    raw_segment->LoadFieldData(raw_load_info);

    auto evaluate = [&](const expr::TypedExprPtr& filter_expr,
                        const segcore::SegmentInternalInterface* segment) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        return milvus::test::gen_filter_res(
            plan.get(), segment, json_raw_data.size(), MAX_TIMESTAMP);
    };
    auto expect_same = [](const ColumnVectorPtr& raw,
                          const ColumnVectorPtr& shredded) {
        ASSERT_EQ(raw->size(), shredded->size());
        TargetBitmapView raw_result(raw->GetRawData(), raw->size());
        TargetBitmapView raw_valid(raw->GetValidRawData(), raw->size());
        TargetBitmapView shredded_result(shredded->GetRawData(),
                                         shredded->size());
        TargetBitmapView shredded_valid(shredded->GetValidRawData(),
                                        shredded->size());
        for (size_t i = 0; i < raw->size(); ++i) {
            EXPECT_EQ(shredded_valid[i], raw_valid[i]) << "row " << i;
            EXPECT_EQ(shredded_result[i], raw_result[i]) << "row " << i;
        }
    };
    auto compare = [&](const expr::TypedExprPtr& filter_expr) {
        auto raw = evaluate(filter_expr, raw_segment.get());
        auto shredded = evaluate(filter_expr, stats_segment.get());
        auto shared = evaluate(filter_expr, shared_stats_segment.get());
        expect_same(raw, shredded);
        expect_same(raw, shared);
        return raw;
    };
    auto check = [](const ColumnVectorPtr& result,
                    const std::vector<bool>& expected_result,
                    const std::vector<bool>& expected_valid) {
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_EQ(result_view[i], expected_result[i]) << "row " << i;
            EXPECT_EQ(valid_view[i], expected_valid[i]) << "row " << i;
        }
    };

    proto::plan::GenericValue one_point_five;
    one_point_five.set_float_val(1.5);
    proto::plan::GenericValue zero;
    zero.set_int64_val(0);
    proto::plan::GenericValue two;
    two.set_int64_val(2);
    auto bad_column = expr::ColumnInfo(json_fid, DataType::JSON, {"bad"});
    check(compare(std::make_shared<expr::TermFilterExpr>(
              bad_column,
              std::vector<proto::plan::GenericValue>{one_point_five},
              false)),
          {false, true, false},
          {false, true, false});
    check(compare(std::make_shared<expr::UnaryRangeFilterExpr>(
              bad_column,
              proto::plan::OpType::GreaterThan,
              zero,
              std::vector<proto::plan::GenericValue>())),
          {false, true, false},
          {false, true, false});
    check(compare(std::make_shared<expr::UnaryRangeFilterExpr>(
              bad_column,
              proto::plan::OpType::GreaterThan,
              one_point_five,
              std::vector<proto::plan::GenericValue>())),
          {false, false, false},
          {false, true, false});
    check(compare(std::make_shared<expr::BinaryRangeFilterExpr>(
              bad_column, zero, two, true, true)),
          {false, true, false},
          {false, true, false});
    proto::plan::GenericValue one;
    one.set_float_val(1.0);
    proto::plan::GenericValue two_point_zero;
    two_point_zero.set_float_val(2.0);
    check(compare(std::make_shared<expr::BinaryRangeFilterExpr>(
              bad_column, one, two_point_zero, true, true)),
          {false, true, false},
          {false, true, false});
    check(compare(std::make_shared<expr::ExistsExpr>(bad_column)),
          {false, true, false},
          {true, true, true});

    proto::plan::GenericValue seven;
    seven.set_int64_val(7);
    proto::plan::GenericValue eight;
    eight.set_int64_val(8);
    auto ok_column = expr::ColumnInfo(json_fid, DataType::JSON, {"ok"});
    check(compare(std::make_shared<expr::TermFilterExpr>(
              ok_column, std::vector<proto::plan::GenericValue>{seven}, false)),
          {true, false, false},
          {true, true, true});

    ASSERT_FALSE(stats->GetShreddingField("/empty", JSONType::STRING).empty());
    ASSERT_TRUE(
        shared_stats->GetShreddingField("/empty", JSONType::STRING).empty());
    auto empty_column = expr::ColumnInfo(json_fid, DataType::JSON, {"empty"});
    proto::plan::GenericValue empty_string;
    empty_string.set_string_val("");
    check(compare(std::make_shared<expr::TermFilterExpr>(
              empty_column,
              std::vector<proto::plan::GenericValue>{empty_string},
              false)),
          {true, false, false},
          {true, true, false});
    check(compare(std::make_shared<expr::ExistsExpr>(empty_column)),
          {true, true, false},
          {true, true, true});

    auto large_column = expr::ColumnInfo(json_fid, DataType::JSON, {"large"});
    proto::plan::GenericValue two_to_53;
    two_to_53.set_float_val(9007199254740992.0);
    check(compare(std::make_shared<expr::TermFilterExpr>(
              large_column,
              std::vector<proto::plan::GenericValue>{two_to_53},
              false)),
          {false, true, false},
          {true, true, true});
    check(compare(std::make_shared<expr::UnaryRangeFilterExpr>(
              large_column,
              proto::plan::OpType::Equal,
              two_to_53,
              std::vector<proto::plan::GenericValue>())),
          {false, true, false},
          {true, true, true});
    check(compare(std::make_shared<expr::UnaryRangeFilterExpr>(
              large_column,
              proto::plan::OpType::GreaterThan,
              two_to_53,
              std::vector<proto::plan::GenericValue>())),
          {true, false, true},
          {true, true, true});

    proto::plan::GenericValue two_to_53_plus_one;
    two_to_53_plus_one.set_int64_val(9007199254740993LL);
    check(compare(std::make_shared<expr::TermFilterExpr>(
              large_column,
              std::vector<proto::plan::GenericValue>{two_to_53_plus_one},
              false)),
          {true, false, false},
          {true, true, true});

    proto::plan::GenericValue int64_min;
    int64_min.set_int64_val(std::numeric_limits<int64_t>::min());
    check(compare(std::make_shared<expr::TermFilterExpr>(
              large_column,
              std::vector<proto::plan::GenericValue>{int64_min},
              false)),
          {false, false, false},
          {true, true, true});

    proto::plan::GenericValue two_to_63;
    two_to_63.set_float_val(9223372036854775808.0);
    check(compare(std::make_shared<expr::TermFilterExpr>(
              large_column,
              std::vector<proto::plan::GenericValue>{two_to_63},
              false)),
          {false, false, true},
          {true, true, true});

    auto array_column = expr::ColumnInfo(json_fid, DataType::JSON, {"arr"});
    check(compare(std::make_shared<expr::ExistsExpr>(array_column)),
          {true, true, true},
          {true, true, true});
    auto object_column = expr::ColumnInfo(json_fid, DataType::JSON, {"obj"});
    check(compare(std::make_shared<expr::ExistsExpr>(object_column)),
          {true, true, false},
          {true, true, true});
    check(compare(std::make_shared<expr::JsonContainsExpr>(
              array_column,
              proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
              false,
              std::vector<proto::plan::GenericValue>{seven})),
          {true, false, true},
          {true, true, true});
    check(compare(std::make_shared<expr::JsonContainsExpr>(
              array_column,
              proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
              false,
              std::vector<proto::plan::GenericValue>{seven, eight})),
          {true, false, true},
          {true, true, true});
}

TEST(JsonStatsV3CompatibilityTest, V3EmptyStringSentinelReadsBackAsNull) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    const std::vector<std::string> json_raw_data = {R"({"value":""})",
                                                    R"({"value":"non-empty"})"};

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1214,
                                          2214,
                                          3214,
                                          json_fid.get(),
                                          5214,
                                          1,
                                          nullptr,
                                          1024,
                                          JSON_STATS_DATA_FORMAT_V3);
    ASSERT_EQ(stats->GetDataFormatVersion(), JSON_STATS_DATA_FORMAT_V3);
    ASSERT_FALSE(stats->GetShreddingField("/value", JSONType::STRING).empty());

    auto segment =
        segcore::CreateSealedSegment(schema, milvus::empty_index_meta, 5282814);
    auto* sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetJsonStatsForTesting(json_fid, stats);

    auto field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;
    for (const auto& value : json_raw_data) {
        jsons.emplace_back(simdjson::padded_string(value));
    }
    field->add_json_data(jsons);
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info =
        PrepareSingleFieldInsertBinlog(0, 0, 0, json_fid.get(), {field}, cm);
    segment->LoadFieldData(load_info);
    ASSERT_TRUE(segment->HasFieldData(json_fid));

    proto::plan::GenericValue empty_string;
    empty_string.set_string_val("");
    auto equals_empty = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"value"}),
        std::vector<proto::plan::GenericValue>{empty_string},
        false);
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       equals_empty);
    auto result = milvus::test::gen_filter_res(
        plan.get(), segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
    TargetBitmapView matched(result->GetRawData(), result->size());
    TargetBitmapView valid(result->GetValidRawData(), result->size());
    // V3 stats are queryable directly. The V3 typed column conflates the real
    // "" with null, so row 0 reads back as UNKNOWN instead of matching.
    // This narrow, documented gap (see cross-path-semantics.md) closes once
    // the segment migrates to V4, which preserves "" as a real value.
    EXPECT_FALSE(matched[0]);
    EXPECT_FALSE(matched[1]);
    EXPECT_FALSE(valid[0]);
    EXPECT_TRUE(valid[1]);
}

TEST(JsonStatsExistsTest, MixedShreddedAndSharedParentUsesUnion) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    const std::vector<std::string> json_raw_data = {
        R"({"parent":{"typed":7,"bad":1e400}})",
        R"({"parent":{"typed":8}})",
        R"({"parent":{"typed":9}})",
    };

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1215,
                                          2215,
                                          3215,
                                          json_fid.get(),
                                          5215,
                                          1,
                                          nullptr,
                                          1);
    ASSERT_FALSE(
        stats->GetShreddingField("/parent/typed", JSONType::INT64).empty());
    ASSERT_TRUE(
        stats->GetShreddingField("/parent/bad", JSONType::DOUBLE).empty());

    auto make_json_field = [&] {
        auto field =
            std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
        std::vector<milvus::Json> jsons;
        jsons.reserve(json_raw_data.size());
        for (const auto& json : json_raw_data) {
            jsons.emplace_back(simdjson::padded_string(json));
        }
        field->add_json_data(jsons);
        return field;
    };
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();

    auto stats_segment =
        segcore::CreateSealedSegment(schema, milvus::empty_index_meta, 5282804);
    auto* sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(stats_segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetJsonStatsForTesting(json_fid, stats);
    auto stats_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {make_json_field()}, cm);
    stats_segment->LoadFieldData(stats_load_info);
    stats_segment->DropFieldData(json_fid);

    auto raw_segment =
        segcore::CreateSealedSegment(schema, milvus::empty_index_meta, 5282805);
    auto raw_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {make_json_field()}, cm);
    raw_segment->LoadFieldData(raw_load_info);

    auto exists = std::make_shared<expr::ExistsExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"parent"}));
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, exists);
    auto raw = milvus::test::gen_filter_res(
        plan.get(), raw_segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
    auto by_stats = milvus::test::gen_filter_res(
        plan.get(), stats_segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

    TargetBitmapView raw_result(raw->GetRawData(), raw->size());
    TargetBitmapView raw_valid(raw->GetValidRawData(), raw->size());
    TargetBitmapView stats_result(by_stats->GetRawData(), by_stats->size());
    TargetBitmapView stats_valid(by_stats->GetValidRawData(), by_stats->size());
    for (size_t i = 0; i < json_raw_data.size(); ++i) {
        EXPECT_TRUE(raw_valid[i]) << "raw row " << i;
        EXPECT_TRUE(stats_valid[i]) << "stats row " << i;
        EXPECT_TRUE(raw_result[i]) << "raw row " << i;
        EXPECT_TRUE(stats_result[i]) << "stats row " << i;
    }
}

TEST(JsonContainsNumericParityTest,
     RawShreddedAndSharedStatsUseTheSameNumericContract) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    std::vector<std::string> json_raw_data = {
        R"({"arr":[18446744073709551616,7]})",
        R"({"arr":[9223372036854775808]})",
        R"({"arr":[9223372036854775809]})",
        R"({"arr":[9007199254740993,"needle"]})",
        R"({"arr":[9007199254740992.0,"needle"]})",
        R"({"arr":[1e400,11]})",
        R"({"arr":[[18446744073709551616]]})",
        R"({"arr":[[9007199254740993]]})",
        R"({"arr":[[9007199254740992.0]]})",
        R"({"arr":[[9223372036854775808]]})",
        R"({"arr":[[9223372036854775809]]})",
        R"({"arr":[[9007199254740992.0],[9223372036854775808],[18446744073709551616],"nested-needle"]})",
    };
    std::string large_array = R"({"arr":[)";
    for (int i = 100; i <= 164; ++i) {
        if (i != 100) {
            large_array += ',';
        }
        large_array += std::to_string(i);
    }
    large_array += R"(,"large-needle"]})";
    json_raw_data.emplace_back(std::move(large_array));
    json_raw_data.emplace_back(R"({"arr":[2],"scalar":2})");
    json_raw_data.emplace_back(R"({"arr":[2.0],"scalar":2.0})");
    json_raw_data.emplace_back(R"({"arr":[2.5],"scalar":2.5})");

    auto shredded_stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                                   json_fid,
                                                   TestLocalPath,
                                                   1213,
                                                   2213,
                                                   3213,
                                                   json_fid.get(),
                                                   5213,
                                                   1);
    auto shared_stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                                 json_fid,
                                                 TestLocalPath,
                                                 1214,
                                                 2214,
                                                 3214,
                                                 json_fid.get(),
                                                 5214,
                                                 1,
                                                 nullptr,
                                                 0);

    auto make_json_field = [&] {
        auto field =
            std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
        std::vector<milvus::Json> jsons;
        jsons.reserve(json_raw_data.size());
        for (const auto& json : json_raw_data) {
            jsons.emplace_back(simdjson::padded_string(json));
        }
        field->add_json_data(jsons);
        return field;
    };
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto make_stats_segment = [&](const std::shared_ptr<JsonKeyStats>& stats,
                                  int64_t segment_id) {
        auto segment = segcore::CreateSealedSegment(
            schema, milvus::empty_index_meta, segment_id);
        auto* sealed =
            dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
        EXPECT_NE(sealed, nullptr);
        sealed->SetJsonStatsForTesting(json_fid, stats);
        auto load_info = PrepareSingleFieldInsertBinlog(
            0, 0, 0, json_fid.get(), {make_json_field()}, cm);
        segment->LoadFieldData(load_info);
        segment->DropFieldData(json_fid);
        return segment;
    };
    auto shredded_segment = make_stats_segment(shredded_stats, 5282811);
    auto shared_segment = make_stats_segment(shared_stats, 5282812);
    auto raw_segment =
        segcore::CreateSealedSegment(schema, milvus::empty_index_meta, 5282813);
    auto raw_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {make_json_field()}, cm);
    raw_segment->LoadFieldData(raw_load_info);

    auto evaluate = [&](const expr::TypedExprPtr& filter_expr,
                        const segcore::SegmentInternalInterface* segment) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        return milvus::test::gen_filter_res(
            plan.get(), segment, json_raw_data.size(), MAX_TIMESTAMP);
    };
    const std::vector<bool> all_rows_valid(json_raw_data.size(), true);
    auto check_filter = [&](const expr::TypedExprPtr& filter_expr,
                            const std::vector<bool>& expected,
                            const std::vector<bool>& expected_valid) {
        auto raw = evaluate(filter_expr, raw_segment.get());
        auto shredded = evaluate(filter_expr, shredded_segment.get());
        auto shared = evaluate(filter_expr, shared_segment.get());
        ASSERT_EQ(raw->size(), expected.size());
        ASSERT_EQ(shredded->size(), expected.size());
        ASSERT_EQ(shared->size(), expected.size());
        ASSERT_EQ(expected_valid.size(), expected.size());
        TargetBitmapView raw_result(raw->GetRawData(), raw->size());
        TargetBitmapView raw_valid(raw->GetValidRawData(), raw->size());
        TargetBitmapView shredded_result(shredded->GetRawData(),
                                         shredded->size());
        TargetBitmapView shredded_valid(shredded->GetValidRawData(),
                                        shredded->size());
        TargetBitmapView shared_result(shared->GetRawData(), shared->size());
        TargetBitmapView shared_valid(shared->GetValidRawData(),
                                      shared->size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(raw_valid[i], expected_valid[i]) << "raw row " << i;
            EXPECT_EQ(shredded_valid[i], expected_valid[i])
                << "shredded row " << i;
            EXPECT_EQ(shared_valid[i], expected_valid[i]) << "shared row " << i;
            EXPECT_EQ(raw_result[i], expected[i]) << "raw row " << i;
            EXPECT_EQ(shredded_result[i], expected[i]) << "shredded row " << i;
            EXPECT_EQ(shared_result[i], expected[i]) << "shared row " << i;
        }
    };
    auto check = [&](proto::plan::JSONContainsExpr_JSONOp op,
                     bool same_type,
                     std::vector<proto::plan::GenericValue> values,
                     const std::vector<bool>& expected) {
        check_filter(std::make_shared<expr::JsonContainsExpr>(
                         expr::ColumnInfo(json_fid, DataType::JSON, {"arr"}),
                         op,
                         same_type,
                         std::move(values)),
                     expected,
                     all_rows_valid);
    };
    auto expected_rows = [&](std::initializer_list<size_t> rows) {
        std::vector<bool> expected(json_raw_data.size(), false);
        for (auto row : rows) {
            expected[row] = true;
        }
        return expected;
    };

    proto::plan::GenericValue seven;
    seven.set_int64_val(7);
    check(proto::plan::JSONContainsExpr_JSONOp_Contains,
          true,
          {seven},
          expected_rows({0}));

    proto::plan::GenericValue eleven;
    eleven.set_int64_val(11);
    check(proto::plan::JSONContainsExpr_JSONOp_Contains,
          true,
          {eleven},
          expected_rows({5}));

    proto::plan::GenericValue two_to_64;
    two_to_64.set_float_val(18446744073709551616.0);
    check(proto::plan::JSONContainsExpr_JSONOp_Contains,
          true,
          {two_to_64},
          expected_rows({}));

    proto::plan::GenericValue two_to_63;
    two_to_63.set_float_val(9223372036854775808.0);
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
          true,
          {two_to_63},
          expected_rows({1, 2}));

    proto::plan::GenericValue two_to_53;
    two_to_53.set_float_val(9007199254740992.0);
    proto::plan::GenericValue missing;
    missing.set_string_val("missing");
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
          false,
          {two_to_53, missing},
          expected_rows({4}));

    proto::plan::GenericValue needle;
    needle.set_string_val("needle");
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
          false,
          {two_to_53, needle},
          expected_rows({4}));

    proto::plan::GenericValue integer_two;
    integer_two.set_int64_val(2);
    proto::plan::GenericValue double_two;
    double_two.set_float_val(2.0);
    proto::plan::GenericValue two_point_five;
    two_point_five.set_float_val(2.5);
    check(proto::plan::JSONContainsExpr_JSONOp_Contains,
          true,
          {integer_two},
          expected_rows({13, 14}));
    check(proto::plan::JSONContainsExpr_JSONOp_Contains,
          true,
          {double_two},
          expected_rows({13, 14}));
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
          false,
          {integer_two, two_point_five},
          expected_rows({13, 14, 15}));
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
          false,
          {integer_two, double_two},
          expected_rows({13, 14}));
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
          false,
          {integer_two, two_point_five},
          expected_rows({}));

    auto scalar_column = expr::ColumnInfo(json_fid, DataType::JSON, {"scalar"});
    check_filter(std::make_shared<expr::TermFilterExpr>(
                     scalar_column,
                     std::vector<proto::plan::GenericValue>{integer_two},
                     false),
                 expected_rows({13, 14}),
                 expected_rows({13, 14, 15}));
    check_filter(std::make_shared<expr::TermFilterExpr>(
                     scalar_column,
                     std::vector<proto::plan::GenericValue>{double_two},
                     false),
                 expected_rows({13, 14}),
                 expected_rows({13, 14, 15}));
    auto integer_term = std::make_shared<expr::TermFilterExpr>(
        scalar_column,
        std::vector<proto::plan::GenericValue>{integer_two},
        false);
    auto double_term = std::make_shared<expr::TermFilterExpr>(
        scalar_column,
        std::vector<proto::plan::GenericValue>{two_point_five},
        false);
    check_filter(
        std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, integer_term, double_term),
        expected_rows({13, 14, 15}),
        expected_rows({13, 14, 15}));
    for (auto mixed_values :
         {std::vector<proto::plan::GenericValue>{integer_two, two_point_five},
          std::vector<proto::plan::GenericValue>{two_point_five,
                                                 integer_two}}) {
        check_filter(std::make_shared<expr::TermFilterExpr>(
                         scalar_column, std::move(mixed_values), false),
                     expected_rows({13, 14, 15}),
                     expected_rows({13, 14, 15}));
    }

    std::vector<proto::plan::GenericValue> large_values;
    for (int64_t i = 100; i <= 164; ++i) {
        large_values.emplace_back().set_int64_val(i);
    }
    auto mixed_large_values = large_values;
    mixed_large_values.emplace_back().set_string_val("large-needle");
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
          true,
          std::move(large_values),
          expected_rows({12}));
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
          false,
          std::move(mixed_large_values),
          expected_rows({12}));

    proto::plan::GenericValue nested_two_to_64;
    nested_two_to_64.mutable_array_val()->add_array()->set_float_val(
        18446744073709551616.0);
    check(proto::plan::JSONContainsExpr_JSONOp_Contains,
          true,
          {nested_two_to_64},
          expected_rows({}));

    proto::plan::GenericValue nested_two_to_63;
    nested_two_to_63.mutable_array_val()->add_array()->set_float_val(
        9223372036854775808.0);
    check(proto::plan::JSONContainsExpr_JSONOp_Contains,
          true,
          {nested_two_to_63},
          expected_rows({9, 10, 11}));

    proto::plan::GenericValue nested_two_to_53;
    nested_two_to_53.mutable_array_val()->add_array()->set_float_val(
        9007199254740992.0);
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
          true,
          {nested_two_to_53, nested_two_to_64},
          expected_rows({8, 11}));
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
          true,
          {nested_two_to_53, nested_two_to_63},
          expected_rows({11}));

    proto::plan::GenericValue nested_missing;
    nested_missing.set_string_val("nested-missing");
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
          false,
          {nested_two_to_53, nested_missing},
          expected_rows({8, 11}));
    proto::plan::GenericValue nested_needle;
    nested_needle.set_string_val("nested-needle");
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
          false,
          {nested_two_to_53, nested_needle},
          expected_rows({11}));
}

TEST(JsonStatsSharedFallbackPruningTest,
     ExactTypedColumnsCanSkipSharedFallback) {
    const auto json_fid = FieldId(100);
    const std::vector<std::string> json_raw_data = {
        R"({"s": "a", "b": true, "arr": [1, 2], "n": 1})",
        R"({"s": "b", "b": false, "arr": [3, 4], "n": 2})",
        R"({"s": "c", "b": true, "arr": [5, 6], "n": 3})",
    };

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1110,
                                          2110,
                                          3110,
                                          json_fid.get(),
                                          5110,
                                          1);

    EXPECT_TRUE(stats->HasAllShreddingFields("/s", {JSONType::STRING}));
    EXPECT_TRUE(stats->HasAllShreddingFields("/b", {JSONType::BOOL}));
    EXPECT_TRUE(stats->HasAllShreddingFields("/arr", {JSONType::ARRAY}));

    EXPECT_TRUE(stats->HasAllShreddingFields("/n", {JSONType::INT64}));
    EXPECT_FALSE(stats->HasAllShreddingFields(
        "/n", {JSONType::INT64, JSONType::DOUBLE}));
    EXPECT_FALSE(stats->HasAllShreddingFields("/missing", {JSONType::STRING}));
}

std::string
FindFirstShreddingDataFile(const BuiltJsonStatsIndex& built_index) {
    auto it =
        std::find_if(built_index.index_files.begin(),
                     built_index.index_files.end(),
                     [](const std::string& file) {
                         return file.find(JSON_STATS_SHREDDING_DATA_PATH) !=
                                std::string::npos;
                     });
    AssertInfo(it != built_index.index_files.end(),
               "json stats index has no shredding data file");
    return built_index.stats_base_path + "/" + *it;
}

std::string
MakeShreddingDataFile(const BuiltJsonStatsIndex& built_index,
                      int64_t column_group_id,
                      int64_t file_id) {
    return fmt::format("{}/{}/{}/{}",
                       built_index.stats_base_path,
                       JSON_STATS_SHREDDING_DATA_PATH,
                       column_group_id,
                       file_id);
}

std::string
MakeShreddingDataRelativeFile(int64_t column_group_id, int64_t file_id) {
    return fmt::format(
        "{}/{}/{}", JSON_STATS_SHREDDING_DATA_PATH, column_group_id, file_id);
}

void
WriteShreddingParquetWithoutPackedFieldList(
    const BuiltJsonStatsIndex& built_index,
    int64_t column_group_id,
    int64_t file_id,
    const std::vector<int64_t>& values) {
    auto path = MakeShreddingDataFile(built_index, column_group_id, file_id);

    arrow::Int64Builder value_builder;
    AssertInfo(value_builder.AppendValues(values).ok(),
               "failed to append json stats values");
    auto value_array = value_builder.Finish().ValueOrDie();

    arrow::BinaryBuilder shared_builder;
    for (size_t i = 0; i < values.size(); ++i) {
        AssertInfo(shared_builder.AppendNull().ok(),
                   "failed to append shared json null");
    }
    auto shared_array = shared_builder.Finish().ValueOrDie();

    const auto value_field_id = START_JSON_STATS_FIELD_ID;
    const auto shared_field_id = START_JSON_STATS_FIELD_ID + 1;
    auto schema = arrow::schema({
        arrow::field(
            JsonKey("/a", JSONType::INT64).ToColumnName(),
            arrow::int64(),
            true,
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(value_field_id)})),
        arrow::field(
            JSON_KEY_STATS_SHARED_FIELD_NAME,
            arrow::binary(),
            true,
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(shared_field_id)})),
    });
    auto table = arrow::Table::Make(schema, {value_array, shared_array});

    auto row_group_metadata = milvus_storage::RowGroupMetadataVector(
        {milvus_storage::RowGroupMetadata(/*memory_size=*/128,
                                          static_cast<int64_t>(values.size()),
                                          /*row_offset=*/0)});
    auto file_metadata =
        arrow::key_value_metadata({milvus_storage::ROW_GROUP_META_KEY,
                                   milvus_storage::STORAGE_VERSION_KEY},
                                  {row_group_metadata.Serialize(), "2"});

    auto output_result = built_index.fs->OpenOutputStream(path);
    AssertInfo(output_result.ok(),
               "failed to open parquet output {}: {}",
               path,
               output_result.status().ToString());
    auto output = output_result.ValueOrDie();

    auto writer_result = parquet::arrow::FileWriter::Open(
        *schema, arrow::default_memory_pool(), output);
    AssertInfo(writer_result.ok(),
               "failed to open parquet writer: {}",
               writer_result.status().ToString());
    auto writer = std::move(writer_result).ValueOrDie();
    AssertInfo(writer->AddKeyValueMetadata(file_metadata).ok(),
               "failed to add parquet metadata");
    AssertInfo(writer->WriteTable(*table, values.size()).ok(),
               "failed to write parquet table");
    AssertInfo(writer->Close().ok(), "failed to close parquet writer");
    AssertInfo(output->Close().ok(), "failed to close parquet output");
}

void
OverwriteWithParquetMissingPackedFieldList(
    const BuiltJsonStatsIndex& built_index,
    const std::vector<int64_t>& values) {
    (void)FindFirstShreddingDataFile(built_index);
    WriteShreddingParquetWithoutPackedFieldList(
        built_index, /*column_group_id=*/0, /*file_id=*/0, values);
}

void
SetIndexFiles(BuiltJsonStatsIndex& built_index,
              std::vector<std::string> index_files) {
    built_index.index_files = std::move(index_files);
    built_index.load_config["index_files"] = built_index.index_files;
}

TargetBitmap
ReadJsonStatsInt64Equal(JsonKeyStats& stats,
                        const std::string& field_name,
                        int64_t expected,
                        size_t size) {
    TargetBitmap res(size);
    TargetBitmap valid_res(size);
    TargetBitmapView res_view(res);
    TargetBitmapView valid_res_view(valid_res);

    auto func = [expected](const int64_t* data,
                           ValidityView valid_data,
                           const int chunk_size,
                           TargetBitmapView res,
                           TargetBitmapView valid_res) {
        for (int i = 0; i < chunk_size; ++i) {
            valid_res[i] = valid_data[i];
            res[i] = valid_data[i] && data[i] == expected;
        }
    };

    auto processed_size = stats.ExecutorForShreddingData<int64_t>(
        nullptr, field_name, func, nullptr, res_view, valid_res_view);
    AssertInfo(processed_size == size,
               "processed json stats rows {} != {}",
               processed_size,
               size);
    return res;
}

void
AssertJsonStatsProjectionMode(const std::string& warmup_policy,
                              int64_t id_offset,
                              bool expect_multi_field_group) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    std::vector<std::string> json_raw_data = {
        R"({"a": 1, "b": 10})",
        R"({"a": 2, "b": 20})",
        R"({"a": 1, "b": 30})",
    };

    const int64_t collection_id = 1200 + id_offset;
    const int64_t partition_id = 2200 + id_offset;
    const int64_t segment_id = 3200 + id_offset;
    const int64_t field_id = json_fid.get();
    const int64_t build_id = 5200 + id_offset;
    const int64_t version_id = 1;
    const std::string root_path = TestLocalPath;

    auto built_index = BuildJsonStatsIndex(json_raw_data,
                                           json_fid,
                                           root_path,
                                           collection_id,
                                           partition_id,
                                           segment_id,
                                           field_id,
                                           build_id,
                                           version_id);
    built_index.load_config[milvus::index::WARMUP] = warmup_policy;
    ASSERT_TRUE(built_index.load_config.contains(milvus::index::WARMUP));
    ASSERT_EQ(
        built_index.load_config.at(milvus::index::WARMUP).get<std::string>(),
        warmup_policy);
    auto stats = LoadBuiltJsonStatsIndex(built_index);

    auto a_field = stats->GetShreddingField("/a", JSONType::INT64);
    auto b_field = stats->GetShreddingField("/b", JSONType::INT64);
    ASSERT_FALSE(a_field.empty());
    ASSERT_FALSE(b_field.empty());

    auto a_result =
        ReadJsonStatsInt64Equal(*stats, a_field, /*expected=*/1, /*size=*/3);
    EXPECT_TRUE(a_result[0]);
    EXPECT_FALSE(a_result[1]);
    EXPECT_TRUE(a_result[2]);

    EXPECT_EQ(JsonStatsProjectionTestAccessor::IsInMultiFieldColumnGroup(
                  *stats, a_field),
              expect_multi_field_group);
    EXPECT_EQ(JsonStatsProjectionTestAccessor::IsInMultiFieldColumnGroup(
                  *stats, b_field),
              expect_multi_field_group);
}

}  // namespace

TEST(JsonContainsByStatsTest, BasicContainsAnyOnArray) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto segment = segcore::CreateSealedSegment(schema);

    const int N = 10000;
    std::vector<std::string> json_raw_data;
    json_raw_data.reserve(N);
    for (int i = 0; i < N; ++i) {
        switch (i % 7) {
            case 0:
                json_raw_data.emplace_back(R"({"a": [1, 2, 3]})");
                break;
            case 1:
                json_raw_data.emplace_back(R"({"a": [4, 5]})");
                break;
            case 2:
                json_raw_data.emplace_back(R"({"a": [1]})");
                break;
            case 3:
                json_raw_data.emplace_back(R"({"a": []})");
                break;
            case 4:
                json_raw_data.emplace_back(R"({"b": [1, 2]})");
                break;
            case 5:
                json_raw_data.emplace_back(R"({"a": [10, 1, 20]})");
                break;
            case 6:
                json_raw_data.emplace_back(R"({"a": ["x", "y"]})");
                break;
        }
    }

    // Build and attach JsonKeyStats for the json field
    const int64_t collection_id = 1001;
    const int64_t partition_id = 2001;
    const int64_t segment_id = 3001;
    const int64_t field_id = json_fid.get();
    const int64_t build_id = 5001;
    const int64_t version_id = 1;
    const std::string root_path = TestLocalPath;

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          root_path,
                                          collection_id,
                                          partition_id,
                                          segment_id,
                                          field_id,
                                          build_id,
                                          version_id);
    auto* sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetJsonStatsForTesting(json_fid, stats);

    // Load raw field data into sealed segment for execution
    std::vector<milvus::Json> jsons;
    for (auto& s : json_raw_data) {
        jsons.emplace_back(simdjson::padded_string(s));
    }
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    // Build json_contains expr: json['a'] contains any 1
    proto::plan::GenericValue value;
    value.set_int64_val(1);
    auto expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(
            json_fid, DataType::JSON, std::vector<std::string>{"a"}, true),
        proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        true,
        std::vector<proto::plan::GenericValue>{value});

    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto result = query::ExecuteQueryExpr(
        plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

    // Expected matches: positions where (i % 7) in {0, 2, 5}
    int64_t expected_count = (N / 7) * 3;
    int rem = N % 7;
    for (int i = 0; i < rem; ++i) {
        if (i == 0 || i == 2 || i == 5) {
            expected_count++;
        }
    }
    EXPECT_EQ(result.count(), expected_count);
    for (int i = 0; i < N; ++i) {
        bool should_match = ((i % 7) == 0) || ((i % 7) == 2) || ((i % 7) == 5);
        EXPECT_EQ(bool(result[i]), should_match);
    }
}

TEST(JsonStatsAsyncLoadTest, LoadsShreddingParquetWithoutPackedFieldList) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    std::vector<std::string> json_raw_data = {
        R"({"a": 1})",
        R"({"a": 2})",
        R"({"a": 1})",
        R"({"a": 3})",
    };

    const int64_t collection_id = 1201;
    const int64_t partition_id = 2201;
    const int64_t segment_id = 3201;
    const int64_t field_id = json_fid.get();
    const int64_t build_id = 5201;
    const int64_t version_id = 1;
    const std::string root_path = TestLocalPath;

    auto built_index = BuildJsonStatsIndex(json_raw_data,
                                           json_fid,
                                           root_path,
                                           collection_id,
                                           partition_id,
                                           segment_id,
                                           field_id,
                                           build_id,
                                           version_id);
    OverwriteWithParquetMissingPackedFieldList(
        built_index, std::vector<int64_t>{1, 2, 1, 3});

    auto stats = LoadBuiltJsonStatsIndex(built_index);
    auto result = ReadJsonStatsInt64Equal(
        *stats, JsonKey("/a", JSONType::INT64).ToColumnName(), 1, 4);

    EXPECT_TRUE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_TRUE(result[2]);
    EXPECT_FALSE(result[3]);
    EXPECT_EQ(result.count(), 2);
}

TEST(JsonStatsAsyncLoadTest, LoadsMultipleShreddingParquetFilesInFileIdOrder) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    std::vector<std::string> json_raw_data = {
        R"({"a": 1})",
        R"({"a": 2})",
        R"({"a": 3})",
        R"({"a": 1})",
        R"({"a": 4})",
    };

    const int64_t collection_id = 1202;
    const int64_t partition_id = 2202;
    const int64_t segment_id = 3202;
    const int64_t field_id = json_fid.get();
    const int64_t build_id = 5202;
    const int64_t version_id = 1;
    const std::string root_path = TestLocalPath;

    auto built_index = BuildJsonStatsIndex(json_raw_data,
                                           json_fid,
                                           root_path,
                                           collection_id,
                                           partition_id,
                                           segment_id,
                                           field_id,
                                           build_id,
                                           version_id);
    WriteShreddingParquetWithoutPackedFieldList(
        built_index, /*column_group_id=*/0, /*file_id=*/0, {1, 2});
    WriteShreddingParquetWithoutPackedFieldList(
        built_index, /*column_group_id=*/0, /*file_id=*/1, {3, 1, 4});

    std::vector<std::string> shuffled_index_files{
        MakeShreddingDataRelativeFile(/*column_group_id=*/0, /*file_id=*/1)};
    shuffled_index_files.insert(shuffled_index_files.end(),
                                built_index.index_files.begin(),
                                built_index.index_files.end());
    SetIndexFiles(built_index, std::move(shuffled_index_files));

    auto stats = LoadBuiltJsonStatsIndex(built_index);
    auto result = ReadJsonStatsInt64Equal(
        *stats, JsonKey("/a", JSONType::INT64).ToColumnName(), 1, 5);

    EXPECT_TRUE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_FALSE(result[2]);
    EXPECT_TRUE(result[3]);
    EXPECT_FALSE(result[4]);
    EXPECT_EQ(result.count(), 2);
}

TEST(JsonStatsAsyncLoadTest, UsesSingleColumnProjectionWithoutWarmup) {
    AssertJsonStatsProjectionMode("disable",
                                  /*id_offset=*/3,
                                  /*expect_multi_field_group=*/false);
}

TEST(JsonStatsAsyncLoadTest, UsesFullColumnGroupProjectionWithWarmup) {
    AssertJsonStatsProjectionMode("sync",
                                  /*id_offset=*/4,
                                  /*expect_multi_field_group=*/true);
}

TEST(JsonStatsUnaryRangeTest, NotEqualKeepsJsonPathUnknownsAndMasksFieldNull) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    auto segment = segcore::CreateSealedSegment(schema);

    std::vector<std::string> json_raw_data = {
        R"({"a": "1"})",    // equal, filtered out
        R"({"a": "123"})",  // string mismatch, kept
        R"({"a": 1})",      // type mismatch for string compare, UNKNOWN
        R"({"b": 1})",      // path missing, UNKNOWN
        R"({"a": null})",   // JSON path null, UNKNOWN
        R"({})",            // path missing, UNKNOWN
        R"({"a": "321"})",  // string mismatch, kept
        R"({"a": "123"})",  // field-level null, filtered out by valid data
    };
    std::vector<uint8_t> valid_data{0b01111111};

    const int64_t collection_id = 1101;
    const int64_t partition_id = 2101;
    const int64_t segment_id = 3101;
    const int64_t field_id = json_fid.get();
    const int64_t build_id = 5101;
    const int64_t version_id = 1;
    const std::string root_path = TestLocalPath;

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          root_path,
                                          collection_id,
                                          partition_id,
                                          segment_id,
                                          field_id,
                                          build_id,
                                          version_id,
                                          &valid_data);
    auto* sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetJsonStatsForTesting(json_fid, stats);

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    json_field->FillFieldData(MakeNullableJsonArray(json_raw_data, valid_data));

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    proto::plan::GenericValue val;
    val.set_string_val("1");
    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::NotEqual,
        val,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    auto result = query::ExecuteQueryExpr(
        plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

    ASSERT_EQ(result.size(), json_raw_data.size());
    EXPECT_FALSE(result[0]);
    EXPECT_TRUE(result[1]);
    for (int i = 2; i <= 5; ++i) {
        EXPECT_FALSE(result[i]) << "row " << i;
    }
    EXPECT_TRUE(result[6]);
    EXPECT_FALSE(result[7]);
    EXPECT_EQ(result.count(), 2);
}

TEST(JsonStatsUnaryRangeTest, UsesStatsValidityWithoutReadingRawJsonValidity) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    auto segment = segcore::CreateSealedSegment(schema);

    const std::vector<std::string> json_raw_data = {
        R"({"a": 1})",
        R"({"a": 2})",
        R"({"b": 1})",
        R"({"a": 1})",
    };

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1102,
                                          2102,
                                          3102,
                                          json_fid.get(),
                                          5102,
                                          1);
    ASSERT_FALSE(stats->GetShreddingField("/a", JSONType::INT64).empty());

    auto* sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetJsonStatsForTesting(json_fid, stats);

    // Deliberately make raw top-level JSON validity disagree with the stats.
    // The stats path must rely on shredding/shared validity only; otherwise row
    // 0 would be masked out by ApplyFieldValidData on the raw JSON field.
    const std::vector<uint8_t> raw_valid_data{0b00001110};
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    json_field->FillFieldData(
        MakeNullableJsonArray(json_raw_data, raw_valid_data));

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    proto::plan::GenericValue val;
    val.set_int64_val(1);
    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        val,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    auto result = milvus::test::gen_filter_res(
        plan.get(), segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

    TargetBitmapView result_view(result->GetRawData(), result->size());
    TargetBitmapView valid_view(result->GetValidRawData(), result->size());
    ASSERT_EQ(result->size(), json_raw_data.size());

    EXPECT_TRUE(valid_view[0]);
    EXPECT_TRUE(result_view[0]);
    EXPECT_TRUE(valid_view[1]);
    EXPECT_FALSE(result_view[1]);
    EXPECT_FALSE(valid_view[2]);
    EXPECT_FALSE(result_view[2]);
    EXPECT_TRUE(valid_view[3]);
    EXPECT_TRUE(result_view[3]);
}

TEST(JsonStatsThreeValuedAuditTest,
     EmptyInAndLargeInt64KeepThreeValuedSemantics) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    auto segment = segcore::CreateSealedSegment(schema);

    const std::vector<std::string> json_raw_data = {
        R"({"a": 9007199254740992})",
        R"({"a": 9007199254740993})",
        R"({"a": 9007199254740994})",
        R"({"a": "abc"})",
        R"({})",
        R"({"a": null})",
        R"({"a": 9007199254740993})"};
    const std::vector<uint8_t> valid_data{0b00111111};

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1201,
                                          2201,
                                          3201,
                                          json_fid.get(),
                                          5201,
                                          1,
                                          &valid_data);
    auto* sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetJsonStatsForTesting(json_fid, stats);

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    json_field->FillFieldData(MakeNullableJsonArray(json_raw_data, valid_data));
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    auto evaluate = [&](const expr::TypedExprPtr& filter_expr) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        return milvus::test::gen_filter_res(
            plan.get(), segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
    };
    auto check = [](const ColumnVectorPtr& result,
                    const std::vector<bool>& expected_result,
                    const std::vector<bool>& expected_valid) {
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_EQ(valid_view[i], expected_valid[i]) << "row " << i;
            if (expected_valid[i]) {
                EXPECT_EQ(result_view[i], expected_result[i]) << "row " << i;
            }
        }
    };

    auto empty_term = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{},
        false);
    check(evaluate(empty_term),
          std::vector<bool>(json_raw_data.size(), false),
          std::vector<bool>(json_raw_data.size(), true));
    check(evaluate(std::make_shared<expr::LogicalUnaryExpr>(
              expr::LogicalUnaryExpr::OpType::LogicalNot, empty_term)),
          std::vector<bool>(json_raw_data.size(), true),
          std::vector<bool>(json_raw_data.size(), true));

    proto::plan::GenericValue value;
    value.set_int64_val(9007199254740993LL);
    const std::vector<bool> numeric_valid = {
        true, true, true, false, false, false, false};
    auto equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        value,
        std::vector<proto::plan::GenericValue>());
    check(evaluate(equal_expr),
          {false, true, false, false, false, false, false},
          numeric_valid);

    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{value},
        false);
    check(evaluate(term_expr),
          {false, true, false, false, false, false, false},
          numeric_valid);

    auto greater_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>());
    check(evaluate(greater_expr),
          {false, false, true, false, false, false, false},
          numeric_valid);

    auto between_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        value,
        value,
        true,
        true);
    check(evaluate(between_expr),
          {false, true, false, false, false, false, false},
          numeric_valid);
}

TEST(JsonStatsBinaryRangeTest, ShreddingMatchesRawData) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    const std::vector<std::string> json_raw_data = {
        R"({"n": 1.0, "s": "alpha", "i": 9007199254740992, "u": 9223372036854775809})",
        R"({"n": 2.0, "s": "beta", "i": 9007199254740993})",
        R"({"n": 3.5, "s": "gamma", "i": 9007199254740994})",
        R"({"n": "2", "s": 2, "i": "9007199254740993"})",
        R"({"other": 0})",
        R"({"n": null, "s": null, "i": null})",
        R"({"n": 4.0, "s": "delta", "i": 1})",
        R"({"n": 5.0, "s": "epsilon", "i": 2})",
    };
    const std::vector<uint8_t> valid_data{0b01111111};

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1203,
                                          2203,
                                          3203,
                                          json_fid.get(),
                                          5203,
                                          1,
                                          &valid_data);
    EXPECT_FALSE(stats
                     ->GetShreddingField(milvus::index::JsonPointer({"n"}),
                                         JSONType::DOUBLE)
                     .empty());
    EXPECT_FALSE(stats
                     ->GetShreddingField(milvus::index::JsonPointer({"s"}),
                                         JSONType::STRING)
                     .empty());
    EXPECT_FALSE(stats
                     ->GetShreddingField(milvus::index::JsonPointer({"i"}),
                                         JSONType::INT64)
                     .empty());

    auto stats_segment = segcore::CreateSealedSegment(schema);
    auto* sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(stats_segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetJsonStatsForTesting(json_fid, stats);
    auto raw_segment = segcore::CreateSealedSegment(schema);

    auto make_json_field = [&] {
        auto field =
            std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
        field->FillFieldData(MakeNullableJsonArray(json_raw_data, valid_data));
        return field;
    };
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto stats_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {make_json_field()}, cm);
    stats_segment->LoadFieldData(stats_load_info);
    stats_segment->DropFieldData(json_fid);
    ASSERT_FALSE(stats_segment->HasFieldData(json_fid));
    auto raw_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {make_json_field()}, cm);
    raw_segment->LoadFieldData(raw_load_info);

    auto evaluate = [&](const expr::TypedExprPtr& filter_expr,
                        const segcore::SegmentInternalInterface* segment,
                        exec::OffsetVector* offsets = nullptr) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        return milvus::test::gen_filter_res(
            plan.get(), segment, json_raw_data.size(), MAX_TIMESTAMP, offsets);
    };
    auto expect_same = [](const ColumnVectorPtr& raw,
                          const ColumnVectorPtr& shredded) {
        ASSERT_EQ(raw->size(), shredded->size());
        TargetBitmapView raw_result(raw->GetRawData(), raw->size());
        TargetBitmapView raw_valid(raw->GetValidRawData(), raw->size());
        TargetBitmapView shredded_result(shredded->GetRawData(),
                                         shredded->size());
        TargetBitmapView shredded_valid(shredded->GetValidRawData(),
                                        shredded->size());
        for (size_t i = 0; i < raw->size(); ++i) {
            EXPECT_EQ(shredded_valid[i], raw_valid[i]) << "row " << i;
            EXPECT_EQ(shredded_result[i], raw_result[i]) << "row " << i;
        }
    };

    proto::plan::GenericValue number_lower;
    number_lower.set_int64_val(2);
    proto::plan::GenericValue number_upper;
    number_upper.set_float_val(4.0);
    auto number_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"n"}),
        number_lower,
        number_upper,
        true,
        true);
    expect_same(evaluate(number_expr, raw_segment.get()),
                evaluate(number_expr, stats_segment.get()));
    exec::OffsetVector offsets = {7, 2, 4, 1, 3, 5, 6, 0, 2};
    expect_same(evaluate(number_expr, raw_segment.get(), &offsets),
                evaluate(number_expr, stats_segment.get(), &offsets));

    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"n"}),
        proto::plan::OpType::GreaterEqual,
        number_lower,
        std::vector<proto::plan::GenericValue>());
    {
        milvus::test::ExprBatchSizeGuard batch_size_guard(3);
        auto raw_batches = milvus::test::EvalExprInBatches(
            unary_expr, raw_segment.get(), json_raw_data.size());
        auto stats_batches = milvus::test::EvalExprInBatches(
            unary_expr, stats_segment.get(), json_raw_data.size());
        EXPECT_EQ(raw_batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));
        EXPECT_EQ(stats_batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));
        expect_same(raw_batches.result, stats_batches.result);
    }

    proto::plan::GenericValue string_lower;
    string_lower.set_string_val("beta");
    proto::plan::GenericValue string_upper;
    string_upper.set_string_val("gamma");
    auto string_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"s"}),
        string_lower,
        string_upper,
        true,
        false);
    expect_same(evaluate(string_expr, raw_segment.get()),
                evaluate(string_expr, stats_segment.get()));
    expect_same(evaluate(string_expr, raw_segment.get(), &offsets),
                evaluate(string_expr, stats_segment.get(), &offsets));

    proto::plan::GenericValue precise_lower;
    precise_lower.set_float_val(9007199254740992.0);
    proto::plan::GenericValue precise_upper;
    precise_upper.set_float_val(9007199254740994.0);
    auto precise_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"i"}),
        precise_lower,
        precise_upper,
        false,
        false);
    auto raw_precise = evaluate(precise_expr, raw_segment.get());
    auto shredded_precise = evaluate(precise_expr, stats_segment.get());
    expect_same(raw_precise, shredded_precise);
    expect_same(evaluate(precise_expr, raw_segment.get(), &offsets),
                evaluate(precise_expr, stats_segment.get(), &offsets));
    TargetBitmapView precise_result(raw_precise->GetRawData(),
                                    raw_precise->size());
    TargetBitmapView precise_valid(raw_precise->GetValidRawData(),
                                   raw_precise->size());
    EXPECT_TRUE(precise_valid[1]);
    EXPECT_TRUE(precise_result[1]);

    proto::plan::GenericValue uint64_double;
    uint64_double.set_float_val(9223372036854775808.0);
    auto uint64_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"u"}),
        uint64_double,
        uint64_double,
        true,
        true);
    auto raw_uint64 = evaluate(uint64_expr, raw_segment.get());
    auto stats_uint64 = evaluate(uint64_expr, stats_segment.get());
    expect_same(raw_uint64, stats_uint64);
    expect_same(evaluate(uint64_expr, raw_segment.get(), &offsets),
                evaluate(uint64_expr, stats_segment.get(), &offsets));
    TargetBitmapView uint64_result(raw_uint64->GetRawData(),
                                   raw_uint64->size());
    TargetBitmapView uint64_valid(raw_uint64->GetValidRawData(),
                                  raw_uint64->size());
    EXPECT_TRUE(uint64_valid[0]);
    EXPECT_TRUE(uint64_result[0]);
}

TEST(JsonStatsThreeValuedAuditTest,
     UnsafeInt64DoesNotAliasDoubleShreddingOrSharedData) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    auto segment = segcore::CreateSealedSegment(schema);

    const std::vector<std::string> json_raw_data = {
        R"({"typed": 9007199254740992.0, "shared": 9007199254740992.0, "typed_array": [9007199254740992.0], "shared_array": [9007199254740992.0]})",
        R"({"typed": 9007199254740994.0, "shared": 1.5, "typed_array": [9007199254740994.0], "shared_array": [1.5]})",
        R"({"typed": 1.0, "typed_array": [1.0]})",
        R"({"typed": 2.0, "typed_array": [2.0]})"};

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1202,
                                          2202,
                                          3202,
                                          json_fid.get(),
                                          5202,
                                          1);
    auto* sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    sealed->SetJsonStatsForTesting(json_fid, stats);

    std::vector<milvus::Json> jsons;
    jsons.reserve(json_raw_data.size());
    for (const auto& json : json_raw_data) {
        jsons.emplace_back(simdjson::padded_string(json));
    }
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    auto evaluate = [&](const expr::TypedExprPtr& filter_expr) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        return milvus::test::gen_filter_res(
            plan.get(), segment.get(), json_raw_data.size(), MAX_TIMESTAMP);
    };
    auto check_non_matches_are_known = [](const ColumnVectorPtr& result) {
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i : {0, 1}) {
            EXPECT_TRUE(valid_view[i]) << "row " << i;
            EXPECT_FALSE(result_view[i]) << "row " << i;
        }
    };

    proto::plan::GenericValue value;
    value.set_int64_val(9007199254740993LL);
    for (const auto* path : {"typed", "shared"}) {
        auto column = expr::ColumnInfo(json_fid, DataType::JSON, {path});
        check_non_matches_are_known(
            evaluate(std::make_shared<expr::UnaryRangeFilterExpr>(
                column,
                proto::plan::OpType::Equal,
                value,
                std::vector<proto::plan::GenericValue>())));
        check_non_matches_are_known(
            evaluate(std::make_shared<expr::TermFilterExpr>(
                column, std::vector<proto::plan::GenericValue>{value}, false)));
        check_non_matches_are_known(
            evaluate(std::make_shared<expr::BinaryRangeFilterExpr>(
                column, value, value, true, true)));
    }

    for (const auto* path : {"typed_array", "shared_array"}) {
        check_non_matches_are_known(
            evaluate(std::make_shared<expr::JsonContainsExpr>(
                expr::ColumnInfo(json_fid, DataType::JSON, {path}),
                proto::plan::JSONContainsExpr_JSONOp_Contains,
                true,
                std::vector<proto::plan::GenericValue>{value})));
    }
}

TEST(JsonCrossPathContractTest, RawStatsPathAndFlatAgreeUnlessDocumented) {
    struct FilterResult {
        std::vector<bool> matches;
        std::vector<bool> valid;
    };

    const std::vector<std::string> docs = {
        R"({"a":1,"c":[2],"n":2,"m":[2,"x",true],"s":"","cast":"1.0"})",
        R"({"a":[1],"n":2.0,"m":[3.5,"x",false],"s":"x"})",
        R"({"a":[[1]],"n":3.5,"m":[2,3.5,"x",true]})",
        R"({"a":[],"n":9007199254740993})",
        R"({"a":[null],"n":9223372036854775808})",
        R"({"a":[[]],"n":9223372036854775809})",
        R"({"a":{},"n":9007199254740992})",
        R"({"a":{"b":1}})",
    };

    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto make_field = [](const std::vector<std::string>& values) {
        auto field =
            std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
        std::vector<milvus::Json> jsons;
        jsons.reserve(values.size());
        for (const auto& value : values) {
            jsons.emplace_back(simdjson::padded_string(value));
        }
        field->add_json_data(jsons);
        return field;
    };

    auto load_field = [&](segcore::SegmentSealed* segment,
                          const std::shared_ptr<FieldData<milvus::Json>>& field,
                          int64_t id) {
        auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareSingleFieldInsertBinlog(
            id, id, id, json_fid.get(), {field}, cm);
        segment->LoadFieldData(load_info);
    };

    auto make_raw = [&](const std::vector<std::string>& values, int64_t id) {
        auto segment =
            segcore::CreateSealedSegment(schema, milvus::empty_index_meta, id);
        load_field(segment.get(), make_field(values), id);
        return segment;
    };

    auto make_stats = [&](const std::vector<std::string>& values,
                          int64_t id,
                          int64_t format,
                          bool drop_raw,
                          int64_t max_shredding_columns = 1024) {
        auto segment =
            segcore::CreateSealedSegment(schema, milvus::empty_index_meta, id);
        auto stats = BuildAndLoadJsonKeyStats(values,
                                              json_fid,
                                              TestLocalPath,
                                              id,
                                              id + 1,
                                              id + 2,
                                              json_fid.get(),
                                              id + 3,
                                              1,
                                              nullptr,
                                              max_shredding_columns,
                                              format);
        auto* sealed =
            dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
        AssertInfo(sealed != nullptr, "cross-path stats segment is not sealed");
        sealed->SetJsonStatsForTesting(json_fid, stats);
        load_field(segment.get(), make_field(values), id);
        if (drop_raw) {
            segment->DropFieldData(json_fid);
        }
        return segment;
    };

    auto make_index = [&](const std::vector<std::string>& values,
                          int64_t id,
                          const std::string& path,
                          const std::string& cast_type,
                          const std::string& cast_function = "unknown") {
        storage::FileManagerContext ctx;
        ctx.fieldDataMeta.field_schema.set_data_type(
            milvus::proto::schema::JSON);
        ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
        ctx.fieldDataMeta.field_id = json_fid.get();

        index::CreateIndexInfo create_info;
        create_info.index_type = index::INVERTED_INDEX_TYPE;
        create_info.json_cast_type = JsonCastType::FromString(cast_type);
        create_info.json_path = path;
        create_info.json_cast_function = cast_function;
        auto index = index::IndexFactory::GetInstance().CreateJsonIndex(
            create_info, ctx);
        auto field = make_field(values);
        if (cast_type == "JSON") {
            auto* typed = static_cast<index::JsonFlatIndex*>(index.get());
            typed->BuildWithFieldData({field});
            typed->finish();
            typed->create_reader(index::SetBitsetSealed);
        } else if (cast_type == "VARCHAR") {
            auto* typed = static_cast<index::JsonInvertedIndex<std::string>*>(
                index.get());
            typed->BuildWithFieldData({field});
            typed->finish();
            typed->create_reader(index::SetBitsetSealed);
        } else {
            auto* typed =
                static_cast<index::JsonInvertedIndex<double>*>(index.get());
            typed->BuildWithFieldData({field});
            typed->finish();
            typed->create_reader(index::SetBitsetSealed);
        }

        auto segment =
            segcore::CreateSealedSegment(schema, milvus::empty_index_meta, id);
        segcore::LoadIndexInfo load_info;
        load_info.field_id = json_fid.get();
        load_info.field_type = DataType::JSON;
        load_info.index_params = {{JSON_PATH, path},
                                  {JSON_CAST_TYPE, cast_type},
                                  {JSON_CAST_FUNCTION, cast_function}};
        load_info.cache_index = milvus::CreateTestCacheIndex(
            fmt::format("json-contract-{}", id), std::move(index));
        segment->LoadIndex(load_info);
        load_field(segment.get(), field, id);
        return segment;
    };

    auto make_double_path_index_with_presence =
        [&](const std::vector<std::string>& values,
            int64_t id,
            const std::string& path,
            JsonPathPresenceSemantics presence_semantics) {
            storage::FileManagerContext ctx;
            ctx.fieldDataMeta.field_schema.set_data_type(
                milvus::proto::schema::JSON);
            ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
            ctx.fieldDataMeta.field_id = json_fid.get();

            auto index =
                std::make_unique<JsonInvertedIndexWithSelectablePresence>(
                    JsonCastType::FromString("DOUBLE"),
                    path,
                    JsonCastFunction::FromString("unknown"),
                    ctx.fieldDataMeta.field_schema,
                    ctx,
                    TANTIVY_INDEX_LATEST_VERSION);
            auto field = make_field(values);
            index->BuildWithPresenceSemantics({field}, presence_semantics);
            index->finish();
            index->create_reader(index::SetBitsetSealed);

            auto segment = segcore::CreateSealedSegment(
                schema, milvus::empty_index_meta, id);
            segcore::LoadIndexInfo load_info;
            load_info.field_id = json_fid.get();
            load_info.field_type = DataType::JSON;
            load_info.index_params = {{JSON_PATH, path},
                                      {JSON_CAST_TYPE, "DOUBLE"},
                                      {JSON_CAST_FUNCTION, "unknown"}};
            load_info.cache_index = milvus::CreateTestCacheIndex(
                fmt::format("json-contract-presence-{}", id), std::move(index));
            segment->LoadIndex(load_info);
            load_field(segment.get(), field, id);
            return segment;
        };

    auto evaluate_filter = [](const expr::TypedExprPtr& filter,
                              const segcore::SegmentInternalInterface* segment,
                              size_t count) {
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, filter);
        auto column = milvus::test::gen_filter_res(
            plan.get(), segment, count, MAX_TIMESTAMP);
        TargetBitmapView bits(column->GetRawData(), column->size());
        TargetBitmapView valid(column->GetValidRawData(), column->size());
        FilterResult result;
        for (size_t i = 0; i < column->size(); ++i) {
            result.matches.push_back(bits[i]);
            result.valid.push_back(valid[i]);
        }
        return result;
    };

    auto uses_scalar_index =
        [](const expr::TypedExprPtr& filter,
           const segcore::SegmentInternalInterface* segment,
           size_t count) {
            auto query_context = std::make_shared<exec::QueryContext>(
                DEAFULT_QUERY_ID, segment, count, MAX_TIMESTAMP);
            exec::ExecContext exec_context(query_context.get());
            auto compiled =
                exec::CompileExpressions({filter}, &exec_context, {}, false);
            AssertInfo(compiled.size() == 1,
                       "expected one compiled expression, got {}",
                       compiled.size());
            auto* segment_expr =
                dynamic_cast<exec::SegmentExpr*>(compiled[0].get());
            AssertInfo(segment_expr != nullptr,
                       "expected a SegmentExpr for JSON path-priority test");
            return segment_expr->UseIndexCursor();
        };

    auto expect_same_filter_result = [](const std::string& name,
                                        const FilterResult& expected,
                                        const FilterResult& actual) {
        SCOPED_TRACE(name);
        EXPECT_EQ(actual.matches, expected.matches);
        EXPECT_EQ(actual.valid, expected.valid);
    };

    auto i64 = [](int64_t value) {
        proto::plan::GenericValue result;
        result.set_int64_val(value);
        return result;
    };
    auto f64 = [](double value) {
        proto::plan::GenericValue result;
        result.set_float_val(value);
        return result;
    };
    auto str = [](std::string value) {
        proto::plan::GenericValue result;
        result.set_string_val(std::move(value));
        return result;
    };
    auto boolean = [](bool value) {
        proto::plan::GenericValue result;
        result.set_bool_val(value);
        return result;
    };

    auto raw = make_raw(docs, 52828900);
    auto stats = make_stats(docs, 52828910, JSON_STATS_DATA_FORMAT_V4, true);
    auto shared_stats =
        make_stats(docs, 52828915, JSON_STATS_DATA_FORMAT_V4, true, 0);
    auto scalar_path = make_index(docs, 52828920, "/n", "DOUBLE");
    auto a_scalar_path = make_index(docs, 52828930, "/a", "DOUBLE");
    auto array_path = make_index(docs, 52828940, "/a", "ARRAY_DOUBLE");
    auto string_path = make_index(docs, 52828950, "/s", "VARCHAR");
    auto flat = make_index(docs, 52828960, "", "JSON");
    auto contains_path = make_index(docs, 52828970, "/c", "ARRAY_DOUBLE");
    auto mixed_path = make_index(docs, 52828980, "/m", "ARRAY_DOUBLE");

    auto expect_cross_path_agreement =
        [&](const std::string& name,
            const expr::TypedExprPtr& filter,
            const segcore::SegmentInternalInterface* path,
            bool path_uses_index,
            bool flat_uses_index) {
            EXPECT_EQ(milvus::test::CanExprExecuteAllAtOnce(
                          filter, path, docs.size()),
                      path_uses_index)
                << name << "/path";
            EXPECT_EQ(milvus::test::CanExprExecuteAllAtOnce(
                          filter, flat.get(), docs.size()),
                      flat_uses_index)
                << name << "/flat";
            auto expected = evaluate_filter(filter, raw.get(), docs.size());
            expect_same_filter_result(
                name + "/stats",
                expected,
                evaluate_filter(filter, stats.get(), docs.size()));
            expect_same_filter_result(
                name + "/shared-stats",
                expected,
                evaluate_filter(filter, shared_stats.get(), docs.size()));
            expect_same_filter_result(
                name + "/path",
                expected,
                evaluate_filter(filter, path, docs.size()));
            expect_same_filter_result(
                name + "/flat",
                expected,
                evaluate_filter(filter, flat.get(), docs.size()));
        };

    auto number = expr::ColumnInfo(json_fid, DataType::JSON, {"n"});
    expect_cross_path_agreement("2-equals-2.0",
                                std::make_shared<expr::UnaryRangeFilterExpr>(
                                    number,
                                    proto::plan::OpType::Equal,
                                    f64(2.0),
                                    std::vector<proto::plan::GenericValue>()),
                                scalar_path.get(),
                                true,
                                true);

    auto mixed_in = std::make_shared<expr::TermFilterExpr>(
        number,
        std::vector<proto::plan::GenericValue>{i64(2), f64(3.5)},
        false);
    expect_cross_path_agreement(
        "mixed-numeric-in", mixed_in, scalar_path.get(), true, true);
    expect_cross_path_agreement(
        "mixed-numeric-not-in",
        std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, mixed_in),
        scalar_path.get(),
        true,
        true);
    // Documented Path-index difference: this fixture uses a DOUBLE projection.
    // It answers large integers with double precision rather than declining to
    // a raw scan.
    // 2^53+1 (row 3) and 2^53 (row 6) share one double, so the Path index
    // reports both. Raw, stats and Flat keep exact integer semantics.
    auto large_int64 = std::make_shared<expr::TermFilterExpr>(
        number,
        std::vector<proto::plan::GenericValue>{i64(9007199254740993LL)},
        false);
    auto large_int64_raw = evaluate_filter(large_int64, raw.get(), docs.size());
    EXPECT_EQ(large_int64_raw.matches,
              (std::vector<bool>{
                  false, false, false, true, false, false, false, false}));
    expect_same_filter_result(
        "large-int64/stats",
        large_int64_raw,
        evaluate_filter(large_int64, stats.get(), docs.size()));
    expect_same_filter_result(
        "large-int64/shared-stats",
        large_int64_raw,
        evaluate_filter(large_int64, shared_stats.get(), docs.size()));
    expect_same_filter_result(
        "large-int64/flat",
        large_int64_raw,
        evaluate_filter(large_int64, flat.get(), docs.size()));
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        large_int64, scalar_path.get(), docs.size()))
        << "large-int64/path must stay on the DOUBLE Path index";
    auto large_int64_path =
        evaluate_filter(large_int64, scalar_path.get(), docs.size());
    EXPECT_EQ(large_int64_path.matches,
              (std::vector<bool>{
                  false, false, false, true, false, false, true, false}))
        << "large-int64/path double projection also matches 2^53";
    EXPECT_EQ(large_int64_path.valid, large_int64_raw.valid)
        << "large-int64/path must not change validity";

    // A double literal at 2^63 is already the value both uint64 rows project
    // to, so the DOUBLE Path index agrees with raw here while still running
    // on the index.
    expect_cross_path_agreement(
        "uint64-double-contract",
        std::make_shared<expr::TermFilterExpr>(
            number,
            std::vector<proto::plan::GenericValue>{f64(9223372036854775808.0)},
            false),
        scalar_path.get(),
        true,
        true);

    auto text = expr::ColumnInfo(json_fid, DataType::JSON, {"s"});
    auto empty_string = std::make_shared<expr::TermFilterExpr>(
        text, std::vector<proto::plan::GenericValue>{str("")}, false);
    expect_cross_path_agreement(
        "empty-string", empty_string, string_path.get(), true, true);
    auto v3_stats =
        make_stats(docs, 52829100, JSON_STATS_DATA_FORMAT_V3, false);
    // V3 stats are queryable directly. The /s path stays shared under the
    // shredding heuristic here, and shared V3 BSON preserves "" exactly, so
    // V3 agrees with raw on the whole fixture. The shredded-column "" gap is
    // pinned separately by
    // JsonStatsV3CompatibilityTest.V3EmptyStringSentinelReadsBackAsNull.
    expect_same_filter_result(
        "v3-empty-string",
        evaluate_filter(empty_string, raw.get(), docs.size()),
        evaluate_filter(empty_string, v3_stats.get(), docs.size()));

    auto mixed = expr::ColumnInfo(json_fid, DataType::JSON, {"m"});
    const std::vector<proto::plan::GenericValue> mixed_values = {
        i64(2), f64(3.5), str("x"), boolean(true)};
    for (auto op : {proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
                    proto::plan::JSONContainsExpr_JSONOp_ContainsAll}) {
        expect_cross_path_agreement(
            op == proto::plan::JSONContainsExpr_JSONOp_ContainsAny
                ? "mixed-contains-any"
                : "mixed-contains-all",
            std::make_shared<expr::JsonContainsExpr>(
                mixed, op, false, mixed_values),
            mixed_path.get(),
            false,
            false);
    }

    auto contains_two_as_double = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"c"}),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{f64(2.0)});
    expect_cross_path_agreement("contains-lossless-int-double",
                                contains_two_as_double,
                                contains_path.get(),
                                true,
                                true);

    auto a = expr::ColumnInfo(json_fid, DataType::JSON, {"a"});
    auto count_result_or_validity_differences = [](const FilterResult& lhs,
                                                   const FilterResult& rhs) {
        AssertInfo(lhs.matches.size() == rhs.matches.size() &&
                       lhs.valid.size() == rhs.valid.size() &&
                       lhs.matches.size() == lhs.valid.size(),
                   "cross-path result sizes differ");
        size_t count = 0;
        for (size_t i = 0; i < lhs.matches.size(); ++i) {
            count += lhs.matches[i] != rhs.matches[i] ||
                     lhs.valid[i] != rhs.valid[i];
        }
        return count;
    };
    size_t canonical_flat_row_op_divergences = 0;
    auto contains_one = std::make_shared<expr::JsonContainsExpr>(
        a,
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{i64(1)});
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        contains_one, array_path.get(), docs.size()));
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        contains_one, flat.get(), docs.size()));
    auto raw_contains = evaluate_filter(contains_one, raw.get(), docs.size());
    auto stats_contains =
        evaluate_filter(contains_one, stats.get(), docs.size());
    auto shared_stats_contains =
        evaluate_filter(contains_one, shared_stats.get(), docs.size());
    auto path_contains =
        evaluate_filter(contains_one, array_path.get(), docs.size());
    auto flat_contains = evaluate_filter(contains_one, flat.get(), docs.size());
    canonical_flat_row_op_divergences +=
        count_result_or_validity_differences(raw_contains, flat_contains);
    expect_same_filter_result("contains/stats", raw_contains, stats_contains);
    expect_same_filter_result(
        "contains/shared-stats", raw_contains, shared_stats_contains);
    expect_same_filter_result("contains/path", raw_contains, path_contains);
    EXPECT_EQ(raw_contains.matches,
              (std::vector<bool>{
                  false, true, false, false, false, false, false, false}));
    EXPECT_EQ(
        raw_contains.valid,
        (std::vector<bool>{false, true, true, true, true, true, false, false}));
    EXPECT_EQ(flat_contains.matches,
              (std::vector<bool>{
                  true, true, true, false, false, false, false, false}));
    EXPECT_EQ(flat_contains.valid,
              (std::vector<bool>{
                  true, true, true, false, false, false, false, false}));

    auto not_contains_one = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, contains_one);
    auto raw_not_contains =
        evaluate_filter(not_contains_one, raw.get(), docs.size());
    auto flat_not_contains =
        evaluate_filter(not_contains_one, flat.get(), docs.size());
    canonical_flat_row_op_divergences += count_result_or_validity_differences(
        raw_not_contains, flat_not_contains);
    EXPECT_EQ(raw_not_contains.matches,
              (std::vector<bool>{
                  false, false, true, true, true, true, false, false}));
    EXPECT_EQ(raw_not_contains.valid, raw_contains.valid);
    EXPECT_EQ(flat_not_contains.matches,
              (std::vector<bool>{
                  false, false, false, false, false, false, false, false}));
    EXPECT_EQ(flat_not_contains.valid, flat_contains.valid);

    auto equal_one = std::make_shared<expr::UnaryRangeFilterExpr>(
        a,
        proto::plan::OpType::Equal,
        i64(1),
        std::vector<proto::plan::GenericValue>());
    auto in_one = std::make_shared<expr::TermFilterExpr>(
        a, std::vector<proto::plan::GenericValue>{i64(1)}, false);
    for (const auto& [name, filter] :
         std::vector<std::pair<std::string, expr::TypedExprPtr>>{
             {"equal", equal_one}, {"in", in_one}}) {
        EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
            filter, a_scalar_path.get(), docs.size()));
        EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
            filter, flat.get(), docs.size()));
        auto expected = evaluate_filter(filter, raw.get(), docs.size());
        expect_same_filter_result(
            name + "/stats",
            expected,
            evaluate_filter(filter, stats.get(), docs.size()));
        expect_same_filter_result(
            name + "/shared-stats",
            expected,
            evaluate_filter(filter, shared_stats.get(), docs.size()));
        expect_same_filter_result(
            name + "/path",
            expected,
            evaluate_filter(filter, a_scalar_path.get(), docs.size()));
        EXPECT_EQ(expected.matches,
                  (std::vector<bool>{
                      true, false, false, false, false, false, false, false}));
        EXPECT_EQ(expected.valid,
                  (std::vector<bool>{
                      true, false, false, false, false, false, false, false}));
        auto flat_result = evaluate_filter(filter, flat.get(), docs.size());
        canonical_flat_row_op_divergences +=
            count_result_or_validity_differences(expected, flat_result);
        EXPECT_EQ(flat_result.matches,
                  (std::vector<bool>{
                      true, true, true, false, false, false, false, false}));
        EXPECT_EQ(flat_result.valid, flat_result.matches);
    }

    proto::plan::GenericValue nested;
    nested.mutable_array_val()->set_same_type(true);
    nested.mutable_array_val()->add_array()->set_int64_val(1);
    auto nested_contains = std::make_shared<expr::JsonContainsExpr>(
        a,
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{nested});
    expect_cross_path_agreement("nested-array-literal-raw-fallback",
                                nested_contains,
                                array_path.get(),
                                false,
                                false);

    proto::plan::GenericValue empty_array;
    empty_array.mutable_array_val()->set_same_type(true);
    auto empty_array_equal = std::make_shared<expr::UnaryRangeFilterExpr>(
        a,
        proto::plan::OpType::Equal,
        empty_array,
        std::vector<proto::plan::GenericValue>());
    auto empty_array_raw =
        evaluate_filter(empty_array_equal, raw.get(), docs.size());
    EXPECT_EQ(empty_array_raw.matches,
              (std::vector<bool>{
                  false, false, false, true, false, false, false, false}));
    expect_cross_path_agreement("empty-array-equality-raw-fallback",
                                empty_array_equal,
                                array_path.get(),
                                false,
                                false);

    const std::vector<std::string> invalid_docs = {
        R"({"bad":1e400,"ok":7})",
        R"({"bad":1e400,"ok":8})",
    };
    auto invalid_raw = make_raw(invalid_docs, 52829110);
    auto invalid_stats =
        make_stats(invalid_docs, 52829120, JSON_STATS_DATA_FORMAT_V4, true);
    auto invalid_shared_stats =
        make_stats(invalid_docs, 52829125, JSON_STATS_DATA_FORMAT_V4, true, 0);
    auto bad_path = make_index(invalid_docs, 52829130, "/bad", "DOUBLE");
    auto ok_path = make_index(invalid_docs, 52829140, "/ok", "DOUBLE");
    auto bad = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"bad"}),
        std::vector<proto::plan::GenericValue>{i64(7)},
        false);
    auto ok = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"ok"}),
        std::vector<proto::plan::GenericValue>{i64(7)},
        false);
    expect_same_filter_result(
        "invalid-target/stats",
        evaluate_filter(bad, invalid_raw.get(), invalid_docs.size()),
        evaluate_filter(bad, invalid_stats.get(), invalid_docs.size()));
    expect_same_filter_result(
        "invalid-target/shared-stats",
        evaluate_filter(bad, invalid_raw.get(), invalid_docs.size()),
        evaluate_filter(bad, invalid_shared_stats.get(), invalid_docs.size()));
    expect_same_filter_result(
        "invalid-target/path",
        evaluate_filter(bad, invalid_raw.get(), invalid_docs.size()),
        evaluate_filter(bad, bad_path.get(), invalid_docs.size()));
    expect_same_filter_result(
        "invalid-sibling/stats",
        evaluate_filter(ok, invalid_raw.get(), invalid_docs.size()),
        evaluate_filter(ok, invalid_stats.get(), invalid_docs.size()));
    expect_same_filter_result(
        "invalid-sibling/shared-stats",
        evaluate_filter(ok, invalid_raw.get(), invalid_docs.size()),
        evaluate_filter(ok, invalid_shared_stats.get(), invalid_docs.size()));
    expect_same_filter_result(
        "invalid-sibling/path",
        evaluate_filter(ok, invalid_raw.get(), invalid_docs.size()),
        evaluate_filter(ok, ok_path.get(), invalid_docs.size()));

    // Expected Flat divergence: either an invalid target or an invalid sibling
    // rejects the whole artifact because Flat has no uncovered-row merge.
    EXPECT_THROW(make_index({R"({"bad":1e400})"}, 52829150, "", "JSON"),
                 std::exception);
    EXPECT_THROW(make_index({R"({"bad":1e400,"ok":7})"}, 52829160, "", "JSON"),
                 std::exception);

    // Expected Path divergence: STRING_TO_DOUBLE changes only the persisted
    // typed path index. Raw, Stats and Flat still see a JSON string.
    auto cast_path =
        make_index(docs, 52829170, "/cast", "DOUBLE", "STRING_TO_DOUBLE");
    auto cast_equal = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"cast"}),
        proto::plan::OpType::Equal,
        f64(1.0),
        std::vector<proto::plan::GenericValue>());
    auto raw_cast = evaluate_filter(cast_equal, raw.get(), docs.size());
    expect_same_filter_result(
        "string-to-double/stats",
        raw_cast,
        evaluate_filter(cast_equal, stats.get(), docs.size()));
    expect_same_filter_result(
        "string-to-double/shared-stats",
        raw_cast,
        evaluate_filter(cast_equal, shared_stats.get(), docs.size()));
    expect_same_filter_result(
        "string-to-double/flat",
        raw_cast,
        evaluate_filter(cast_equal, flat.get(), docs.size()));
    auto indexed_cast =
        evaluate_filter(cast_equal, cast_path.get(), docs.size());
    EXPECT_FALSE(raw_cast.matches[0]);
    EXPECT_FALSE(raw_cast.valid[0]);
    EXPECT_TRUE(indexed_cast.matches[0]);
    EXPECT_TRUE(indexed_cast.valid[0]);

    // When V4 Stats and a compatible typed Path index coexist on one sealed
    // segment, the explicitly provisioned Path index takes priority. This is
    // observable for STRING_TO_DOUBLE because Stats retains the original
    // STRING type and cannot reproduce the Path index's cast semantics.
    auto cast_path_with_stats =
        make_index(docs, 52829180, "/cast", "DOUBLE", "STRING_TO_DOUBLE");
    auto cast_stats = BuildAndLoadJsonKeyStats(docs,
                                               json_fid,
                                               TestLocalPath,
                                               52829181,
                                               52829182,
                                               52829183,
                                               json_fid.get(),
                                               52829184,
                                               1);
    auto* cast_path_sealed = dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(
        cast_path_with_stats.get());
    ASSERT_NE(cast_path_sealed, nullptr);
    cast_path_sealed->SetJsonStatsForTesting(json_fid, cast_stats);
    cast_path_with_stats->DropFieldData(json_fid);

    auto cast_column = expr::ColumnInfo(json_fid, DataType::JSON, {"cast"});
    auto cast_in = std::make_shared<expr::TermFilterExpr>(
        cast_column,
        std::vector<proto::plan::GenericValue>{f64(0.5), f64(1.0)},
        false);
    auto cast_greater = std::make_shared<expr::UnaryRangeFilterExpr>(
        cast_column,
        proto::plan::OpType::GreaterThan,
        f64(0.5),
        std::vector<proto::plan::GenericValue>());
    auto cast_between = std::make_shared<expr::BinaryRangeFilterExpr>(
        cast_column, f64(0.5), f64(1.5), true, true);
    for (const auto& [name, filter] :
         std::vector<std::pair<std::string, expr::TypedExprPtr>>{
             {"string-to-double/path-wins-equal", cast_equal},
             {"string-to-double/path-wins-in", cast_in},
             {"string-to-double/path-wins-unary-range", cast_greater},
             {"string-to-double/path-wins-binary-range", cast_between}}) {
        auto expected = evaluate_filter(filter, cast_path.get(), docs.size());
        auto actual =
            evaluate_filter(filter, cast_path_with_stats.get(), docs.size());
        expect_same_filter_result(name, expected, actual);
        EXPECT_TRUE(
            uses_scalar_index(filter, cast_path_with_stats.get(), docs.size()))
            << name;
        EXPECT_TRUE(actual.matches[0]) << name;
        EXPECT_TRUE(actual.valid[0]) << name;
    }

    // Priority is not limited to cast functions: an ordinary compatible typed
    // Path index also wins, including for large int64 predicates evaluated
    // within a DOUBLE projection.
    auto typed_path_with_stats = make_index(docs, 52829190, "/n", "DOUBLE");
    auto* typed_path_sealed = dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(
        typed_path_with_stats.get());
    ASSERT_NE(typed_path_sealed, nullptr);
    typed_path_sealed->SetJsonStatsForTesting(json_fid, cast_stats);
    auto typed_equal = std::make_shared<expr::UnaryRangeFilterExpr>(
        number,
        proto::plan::OpType::Equal,
        f64(2.0),
        std::vector<proto::plan::GenericValue>());
    EXPECT_TRUE(uses_scalar_index(
        typed_equal, typed_path_with_stats.get(), docs.size()));
    // Path priority holds for large integers too: the DOUBLE projection stays
    // on the index and reports the documented double-precision answer rather
    // than dropping to stats or raw.
    EXPECT_TRUE(uses_scalar_index(
        large_int64, typed_path_with_stats.get(), docs.size()));
    expect_same_filter_result(
        "typed-path-with-stats/large-int64-stays-on-path",
        large_int64_path,
        evaluate_filter(large_int64, typed_path_with_stats.get(), docs.size()));

    // JsonFlatIndex is intentionally excluded from typed-Path priority and
    // therefore keeps the pre-existing Stats-first behavior.
    auto flat_with_stats = make_index(docs, 52829200, "", "JSON");
    auto* flat_with_stats_sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(flat_with_stats.get());
    ASSERT_NE(flat_with_stats_sealed, nullptr);
    flat_with_stats_sealed->SetJsonStatsForTesting(json_fid, cast_stats);
    EXPECT_FALSE(
        uses_scalar_index(typed_equal, flat_with_stats.get(), docs.size()));

    // Same-type JSON Contains variants use the compatible ARRAY_DOUBLE Path
    // index even when V4 Stats are loaded on the same segment.
    auto contains_path_with_stats =
        make_index(docs, 52829210, "/c", "ARRAY_DOUBLE");
    auto* contains_path_with_stats_sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(
            contains_path_with_stats.get());
    ASSERT_NE(contains_path_with_stats_sealed, nullptr);
    contains_path_with_stats_sealed->SetJsonStatsForTesting(json_fid,
                                                            cast_stats);
    contains_path_with_stats->DropFieldData(json_fid);
    for (const auto& [name, op] : std::vector<
             std::pair<std::string, proto::plan::JSONContainsExpr_JSONOp>>{
             {"contains", proto::plan::JSONContainsExpr_JSONOp_Contains},
             {"contains-any", proto::plan::JSONContainsExpr_JSONOp_ContainsAny},
             {"contains-all",
              proto::plan::JSONContainsExpr_JSONOp_ContainsAll}}) {
        auto filter = std::make_shared<expr::JsonContainsExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"c"}),
            op,
            true,
            std::vector<proto::plan::GenericValue>{f64(2.0)});
        EXPECT_TRUE(uses_scalar_index(
            filter, contains_path_with_stats.get(), docs.size()))
            << name;
        expect_same_filter_result(
            "typed-path-with-stats/" + name,
            evaluate_filter(filter, contains_path.get(), docs.size()),
            evaluate_filter(
                filter, contains_path_with_stats.get(), docs.size()));
    }

    // A mixed-type Contains operand cannot execute on ARRAY_DOUBLE. It must
    // not let the mere presence of that Path index steal the query from V4
    // Stats; without Stats the same expression retains its RawData fallback.
    auto mixed_path_with_stats =
        make_index(docs, 52829220, "/m", "ARRAY_DOUBLE");
    auto* mixed_path_with_stats_sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(
            mixed_path_with_stats.get());
    ASSERT_NE(mixed_path_with_stats_sealed, nullptr);
    mixed_path_with_stats_sealed->SetJsonStatsForTesting(json_fid, cast_stats);
    mixed_path_with_stats->DropFieldData(json_fid);
    auto mixed_contains = std::make_shared<expr::JsonContainsExpr>(
        mixed,
        proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
        false,
        mixed_values);
    EXPECT_FALSE(uses_scalar_index(
        mixed_contains, mixed_path_with_stats.get(), docs.size()));
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        mixed_contains, mixed_path_with_stats.get(), docs.size()));
    EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
        mixed_contains, mixed_path.get(), docs.size()));
    expect_same_filter_result(
        "typed-path-with-stats/mixed-contains-keeps-stats",
        evaluate_filter(mixed_contains, stats.get(), docs.size()),
        evaluate_filter(
            mixed_contains, mixed_path_with_stats.get(), docs.size()));

    auto unsafe_contains = std::make_shared<expr::JsonContainsExpr>(
        mixed,
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{i64(9007199254740993LL)});
    auto mixed_path_with_stats_and_raw =
        make_index(docs, 52829230, "/m", "ARRAY_DOUBLE");
    auto* mixed_path_with_stats_and_raw_sealed =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(
            mixed_path_with_stats_and_raw.get());
    ASSERT_NE(mixed_path_with_stats_and_raw_sealed, nullptr);
    mixed_path_with_stats_and_raw_sealed->SetJsonStatsForTesting(json_fid,
                                                                 cast_stats);
    // A large-integer CONTAINS element also stays on the ARRAY_DOUBLE Path
    // index. No /m element shares a double with 2^53+1, so the double
    // projection happens to agree with raw here.
    EXPECT_TRUE(uses_scalar_index(
        unsafe_contains, mixed_path_with_stats_and_raw.get(), docs.size()));
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        unsafe_contains, mixed_path_with_stats_and_raw.get(), docs.size()));
    expect_same_filter_result(
        "typed-path-with-stats/large-int64-contains-stays-on-path",
        evaluate_filter(unsafe_contains, raw.get(), docs.size()),
        evaluate_filter(
            unsafe_contains, mixed_path_with_stats_and_raw.get(), docs.size()));

    auto exists_a = std::make_shared<expr::ExistsExpr>(expr::ColumnInfo(
        json_fid, DataType::JSON, std::vector<std::string>{"a"}));
    const auto assert_exists_path_priority =
        [&](const std::string& name,
            int64_t id,
            JsonPathPresenceSemantics presence_semantics,
            const std::vector<bool>& expected_matches) {
            auto segment = make_double_path_index_with_presence(
                docs, id, "/a", presence_semantics);
            auto* sealed =
                dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(segment.get());
            ASSERT_NE(sealed, nullptr);
            sealed->SetJsonStatsForTesting(json_fid, cast_stats);
            segment->DropFieldData(json_fid);

            EXPECT_TRUE(uses_scalar_index(exists_a, segment.get(), docs.size()))
                << name;
            auto result = evaluate_filter(exists_a, segment.get(), docs.size());
            EXPECT_EQ(result.matches, expected_matches) << name;
            EXPECT_EQ(result.valid, std::vector<bool>(docs.size(), true))
                << name;
        };

    // Loaded pre-V6 artifacts keep their recursive/non-empty presence bitmap;
    // V6+ artifacts use non-null target presence. In both cases the Path
    // artifact is authoritative and cannot be shadowed by newer JSON Stats.
    assert_exists_path_priority(
        "exists/legacy-path-bitmap-wins",
        52829240,
        JsonPathPresenceSemantics::LEGACY_RECURSIVE_NON_EMPTY,
        {true, true, true, false, false, false, false, true});
    assert_exists_path_priority("exists/v6-path-bitmap-wins",
                                52829250,
                                JsonPathPresenceSemantics::NON_NULL_TARGET,
                                std::vector<bool>(docs.size(), true));

    // Flat remains Stats-first for operators beyond equality as well.
    EXPECT_FALSE(uses_scalar_index(
        contains_two_as_double, flat_with_stats.get(), docs.size()));
    EXPECT_FALSE(
        uses_scalar_index(exists_a, flat_with_stats.get(), docs.size()));

    // Expected persisted-Path divergence from the legacy prefix-accepting
    // cast contract: the new simdjson contract rejects trailing junk.
    auto strict_cast = JsonCastFunction::FromString("STRING_TO_DOUBLE");
    Json trailing_junk(
        simdjson::padded_string(std::string(R"({"a":"1.5junk"})")));
    EXPECT_FALSE(JsonCastFunction::CastJsonValue<double>(
                     strict_cast, trailing_junk, "/a")
                     .has_value());
    auto parse_cast_string = [&](const std::string& value) {
        Json json(
            simdjson::padded_string(fmt::format(R"({{"a":"{}"}})", value)));
        return JsonCastFunction::CastJsonValue<double>(strict_cast, json, "/a");
    };
    auto subnormal = parse_cast_string("-1.48e-309");
    ASSERT_TRUE(subnormal.has_value());
    EXPECT_DOUBLE_EQ(subnormal.value(), -1.48e-309);
    auto underflow = parse_cast_string("1e-400");
    ASSERT_TRUE(underflow.has_value());
    EXPECT_EQ(underflow.value(), 0.0);
    EXPECT_FALSE(parse_cast_string("18446744073709551616").has_value());

    // Count contract: the original 11-row review table has three non-parity
    // items (invalid target, invalid sibling, containers). Including the two
    // STRING_TO_DOUBLE findings gives five semantic issue categories. The
    // canonical container vectors above intentionally pin 14 row/op
    // divergences: Equal(2), IN(2), Contains(5), NOT Contains(5).
    constexpr size_t kReviewMatrixNonParityItems = 3;
    constexpr size_t kDocumentedSemanticIssueCategories = 5;
    static_assert(kReviewMatrixNonParityItems == 3);
    static_assert(kDocumentedSemanticIssueCategories == 5);
    EXPECT_EQ(canonical_flat_row_op_divergences, 14);
}
