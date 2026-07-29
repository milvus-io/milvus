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
#include "exec/expression/TermExpr.h"
#include "exec/expression/UnaryExpr.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/IndexStats.h"
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
};

BuiltJsonStatsIndex
BuildJsonStatsIndex(const std::vector<std::string>& json_strings,
                    const milvus::FieldId json_fid,
                    const std::string& root_path,
                    int64_t collection_id,
                    int64_t partition_id,
                    int64_t segment_id,
                    int64_t field_id,
                    int64_t build_id,
                    int64_t version_id,
                    const std::vector<uint8_t>* valid_data = nullptr) {
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

    Config build_config;
    build_config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

    auto builder = std::make_shared<JsonKeyStats>(ctx, false);
    builder->Build(build_config);

    auto create_index_result = builder->Upload(build_config);
    auto index_files = create_index_result->GetIndexFiles();

    Config load_config;
    load_config["index_files"] = index_files;
    load_config[milvus::LOAD_PRIORITY] =
        milvus::proto::common::LoadPriority::HIGH;
    auto stats_base_path = storage::GenRemoteJsonStatsPathPrefix(chunk_manager,
                                                                 build_id,
                                                                 version_id,
                                                                 collection_id,
                                                                 partition_id,
                                                                 segment_id,
                                                                 field_id);
    load_config[STATS_BASE_PATH_KEY] = stats_base_path;

    return BuiltJsonStatsIndex{ctx,
                               std::move(load_config),
                               std::move(index_files),
                               std::move(stats_base_path),
                               std::move(fs)};
}

std::shared_ptr<JsonKeyStats>
LoadBuiltJsonStatsIndex(const BuiltJsonStatsIndex& built_index) {
    auto reader = std::make_shared<JsonKeyStats>(built_index.ctx, true);
    reader->Load(milvus::tracer::TraceContext{}, built_index.load_config);
    return reader;
}

std::shared_ptr<JsonKeyStats>
BuildAndLoadJsonKeyStats(const std::vector<std::string>& json_strings,
                         const milvus::FieldId json_fid,
                         const std::string& root_path,
                         int64_t collection_id,
                         int64_t partition_id,
                         int64_t segment_id,
                         int64_t field_id,
                         int64_t build_id,
                         int64_t version_id,
                         const std::vector<uint8_t>* valid_data = nullptr) {
    auto built_index = BuildJsonStatsIndex(json_strings,
                                           json_fid,
                                           root_path,
                                           collection_id,
                                           partition_id,
                                           segment_id,
                                           field_id,
                                           build_id,
                                           version_id,
                                           valid_data);
    return LoadBuiltJsonStatsIndex(built_index);
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
                           const bool* valid_data,
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
