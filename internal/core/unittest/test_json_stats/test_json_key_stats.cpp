// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <boost/filesystem/operations.hpp>
#include <fmt/core.h>
#include <folly/FBVector.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <simdjson.h>
#include <stddef.h>
#include <atomic>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "segcore/default_fs.h"

#include "bitset/bitset.h"
#include "cachinglayer/CacheSlot.h"
#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/Tracer.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "common/bson_view.h"
#include "common/protobuf_utils.h"
#include "futures/Executor.h"
#include "futures/future_c.h"
#include "gtest/gtest.h"
#include "index/IndexInfo.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/bson_inverted.h"
#include "index/json_stats/utils.h"
#include "indexbuilder/IndexCreatorBase.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "simdjson/padded_string.h"
#include "storage/ChunkManager.h"
#include "storage/EntryStreamUtils.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"

using namespace milvus::index;
using namespace milvus::indexbuilder;
using namespace milvus;
using namespace milvus::index;

class BuildKeyStatsAccessor {
 public:
    static void
    Prepare(JsonKeyStats& stats,
            const std::map<JsonKey, JsonKeyLayoutType>& key_types,
            int64_t num_rows) {
        stats.num_rows_ = num_rows;
        stats.key_types_ = key_types;
        stats.shared_keys_.clear();
        stats.column_keys_.clear();
        for (const auto& [key, layout] : key_types) {
            if (layout == JsonKeyLayoutType::SHARED) {
                stats.shared_keys_.insert(key);
            } else {
                stats.column_keys_.insert(key);
            }
        }

        auto remote_prefix =
            stats.disk_file_manager_->GetRemoteJsonStatsShreddingPrefix();
        auto writer_context =
            ParquetWriterFactory::CreateContext(key_types, remote_prefix);
        stats.parquet_writer_->Init(std::move(writer_context));
    }

    static void
    CallLegacy(JsonKeyStats& stats,
               const std::vector<JsonStatsRowRange>& row_ranges,
               bool nullable) {
        stats.BuildKeyStats(row_ranges, nullable);
    }

    static void
    CallParallel(JsonKeyStats& stats,
                 const std::vector<JsonStatsRowRange>& row_ranges,
                 bool nullable) {
        stats.BuildKeyStatsParallel(row_ranges, nullable);
    }

    static void
    Finish(JsonKeyStats& stats) {
        auto status = stats.parquet_writer_->Close();
        AssertInfo(status.ok(),
                   "failed to close json stats test writer: {}",
                   status.ToString());
        stats.bson_inverted_index_->BuildIndex();
    }

    static std::vector<std::string>
    GetParquetFiles(const JsonKeyStats& stats) {
        std::vector<std::string> files;
        for (const auto& [path, size] :
             stats.parquet_writer_->GetPathsToSize()) {
            if (size > 0) {
                files.push_back(path);
            }
        }
        return files;
    }

    static IndexStatsPtr
    UploadBsonIndex(JsonKeyStats& stats) {
        return stats.bson_inverted_index_->UploadIndex();
    }
};

int64_t
GenerateRandomInt64(int64_t min, int64_t max) {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());

    std::uniform_int_distribution<int64_t> dist(min, max);
    return dist(gen);
}

static std::vector<milvus::Json>
GenerateJsons(int size) {
    std::vector<Json> jsons;
    std::default_random_engine random(42);
    std::normal_distribution<> distr(0, 1);
    for (int i = 0; i < size; i++) {
        std::string str;
        if (i % 10 < 2) {
            str = R"({"int_shared":)" + std::to_string(random()) +
                  R"(,"double_shared":)" +
                  std::to_string(static_cast<double>(random())) +
                  R"(,"string_shared":")" + std::to_string(random()) +
                  R"(","bool_shared": true)" + R"(, "array_shared": [1,2,3])" +
                  "}";
        } else {
            str = R"({"int":)" + std::to_string(random()) + R"(,"double":)" +
                  std::to_string(static_cast<double>(random())) +
                  R"(,"string":")" + std::to_string(random()) +
                  R"(","bool": true)" + R"(, "array": [1,2,3])" + "}";
        }
        jsons.push_back(milvus::Json(simdjson::padded_string(str)));
    }
    return jsons;
}

namespace {

milvus::FieldDataPtr
CreateMaterializeJsonFieldData(const std::vector<std::string>& json_rows) {
    auto field_data = milvus::storage::CreateFieldData(
        milvus::DataType::JSON, milvus::DataType::NONE, false);
    if (json_rows.empty()) {
        return field_data;
    }

    std::vector<milvus::Json> rows;
    rows.reserve(json_rows.size());
    for (const auto& json : json_rows) {
        rows.emplace_back(simdjson::padded_string(json));
    }
    field_data->FillFieldData(rows.data(), rows.size());
    return field_data;
}

milvus::FieldDataPtr
CreateNullableMaterializeJsonFieldData(
    const std::vector<std::string>& json_rows,
    const std::vector<bool>& valid_rows) {
    AssertInfo(json_rows.size() == valid_rows.size(),
               "json rows and validity rows must have the same size");
    auto field_data = milvus::storage::CreateFieldData(
        milvus::DataType::JSON, milvus::DataType::NONE, true);
    if (json_rows.empty()) {
        return field_data;
    }

    std::vector<milvus::Json> rows;
    rows.reserve(json_rows.size());
    for (const auto& json : json_rows) {
        rows.emplace_back(simdjson::padded_string(json));
    }

    std::vector<uint8_t> valid_bitmap((valid_rows.size() + 7) / 8, 0);
    for (size_t i = 0; i < valid_rows.size(); ++i) {
        if (valid_rows[i]) {
            valid_bitmap[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
        }
    }
    field_data->FillFieldData(rows.data(), valid_bitmap.data(), rows.size(), 0);
    return field_data;
}

struct MaterializeTestIndex {
    std::shared_ptr<storage::ChunkManager> chunk_manager;
    milvus_storage::ArrowFileSystemPtr fs;
    storage::FileManagerContext context;
    std::unique_ptr<JsonKeyStats> stats;
};

MaterializeTestIndex
CreateMaterializeTestIndex(bool nullable, int64_t write_batch_size) {
    static std::atomic<int64_t> next_id{900000};
    const auto id = next_id.fetch_add(1);

    proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(proto::schema::DataType::JSON);
    field_schema.set_nullable(nullable);
    storage::FieldDataMeta field_meta{11, 12, id, 101, field_schema};
    storage::IndexMeta index_meta{id, 101, id, 1};

    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);
    auto stats =
        std::make_unique<JsonKeyStats>(ctx, false, 1024, 0.3, write_batch_size);

    return MaterializeTestIndex{
        chunk_manager,
        fs,
        ctx,
        std::move(stats),
    };
}

struct MaterializedTable {
    std::shared_ptr<arrow::Table> table;
    std::vector<int64_t> row_group_rows;
};

MaterializedTable
ReadMaterializedTable(const milvus_storage::ArrowFileSystemPtr& fs,
                      const std::string& file) {
    auto reader_result = milvus_storage::FileRowGroupReader::Make(fs, file);
    AssertInfo(reader_result.ok(),
               "failed to create json stats parquet test reader: {}",
               reader_result.status().ToString());
    auto reader = reader_result.ValueOrDie();
    auto row_groups = reader->file_metadata()->GetRowGroupMetadataVector();
    AssertInfo(row_groups.size() > 0,
               "json stats parquet test file has no row groups");

    auto status = reader->SetRowGroupOffsetAndCount(
        0, static_cast<int>(row_groups.size()));
    AssertInfo(status.ok(),
               "failed to select json stats parquet row groups: {}",
               status.ToString());

    std::vector<std::shared_ptr<arrow::Table>> tables;
    std::vector<int64_t> row_group_rows;
    tables.reserve(row_groups.size());
    row_group_rows.reserve(row_groups.size());
    for (size_t i = 0; i < row_groups.size(); ++i) {
        std::shared_ptr<arrow::Table> table;
        status = reader->ReadNextRowGroup(&table);
        AssertInfo(status.ok(),
                   "failed to read json stats parquet row group: {}",
                   status.ToString());
        AssertInfo(table != nullptr,
                   "json stats parquet reader returned a null row group");
        tables.push_back(std::move(table));
        row_group_rows.push_back(row_groups.Get(i).row_num());
    }
    status = reader->Close();
    AssertInfo(status.ok(),
               "failed to close json stats parquet test reader: {}",
               status.ToString());

    auto concatenate_result = arrow::ConcatenateTables(tables);
    AssertInfo(concatenate_result.ok(),
               "failed to concatenate json stats parquet tables: {}",
               concatenate_result.status().ToString());
    auto combine_result = concatenate_result.ValueOrDie()->CombineChunks();
    AssertInfo(combine_result.ok(),
               "failed to combine json stats parquet chunks: {}",
               combine_result.status().ToString());
    return MaterializedTable{
        combine_result.ValueOrDie(),
        std::move(row_group_rows),
    };
}

struct MaterializeSnapshot {
    MaterializedTable parquet;
    std::map<std::string, std::vector<std::pair<uint32_t, uint32_t>>> postings;
};

MaterializeSnapshot
BuildMaterializeSnapshot(const std::vector<JsonStatsRowRange>& row_ranges,
                         const std::map<JsonKey, JsonKeyLayoutType>& key_types,
                         const std::vector<std::string>& shared_paths,
                         bool nullable,
                         bool parallel,
                         int64_t write_batch_size = 5) {
    int64_t num_rows = 0;
    for (const auto& range : row_ranges) {
        num_rows += range.row_count;
    }

    auto test_index = CreateMaterializeTestIndex(nullable, write_batch_size);
    BuildKeyStatsAccessor::Prepare(*test_index.stats, key_types, num_rows);
    if (parallel) {
        BuildKeyStatsAccessor::CallParallel(
            *test_index.stats, row_ranges, nullable);
    } else {
        BuildKeyStatsAccessor::CallLegacy(
            *test_index.stats, row_ranges, nullable);
    }
    BuildKeyStatsAccessor::Finish(*test_index.stats);

    auto files = BuildKeyStatsAccessor::GetParquetFiles(*test_index.stats);
    if (num_rows == 0) {
        AssertInfo(files.empty(),
                   "empty json stats build unexpectedly produced {} parquet "
                   "files",
                   files.size());
        return MaterializeSnapshot{
            MaterializedTable{nullptr, {}},
            {},
        };
    }
    AssertInfo(files.size() == 1,
               "expected one json stats parquet file, got {}",
               files.size());

    MaterializeSnapshot snapshot{
        ReadMaterializedTable(test_index.fs, files.front()),
        {},
    };
    auto bson_stats = BuildKeyStatsAccessor::UploadBsonIndex(*test_index.stats);
    auto load_disk_file_manager =
        std::make_shared<storage::DiskFileManagerImpl>(test_index.context);
    BsonInvertedIndex loaded_bson_index(load_disk_file_manager);
    loaded_bson_index.LoadIndex(bson_stats->GetIndexFiles(),
                                milvus::proto::common::LoadPriority::HIGH,
                                false);
    for (const auto& path : shared_paths) {
        loaded_bson_index.TermQueryEach(
            path, [&](uint32_t row_id, uint32_t offset) {
                snapshot.postings[path].emplace_back(row_id, offset);
            });
    }
    return snapshot;
}

void
ExpectMaterializeSnapshotsEqual(const MaterializeSnapshot& expected,
                                const MaterializeSnapshot& actual,
                                bool compare_row_groups = false) {
    ASSERT_EQ(expected.parquet.table == nullptr,
              actual.parquet.table == nullptr);
    if (expected.parquet.table == nullptr) {
        EXPECT_EQ(actual.postings, expected.postings);
        return;
    }

    EXPECT_TRUE(expected.parquet.table->Equals(*actual.parquet.table, false))
        << "expected:\n"
        << expected.parquet.table->ToString() << "\nactual:\n"
        << actual.parquet.table->ToString();
    ASSERT_EQ(expected.parquet.table->num_columns(),
              actual.parquet.table->num_columns());
    for (int column = 0; column < expected.parquet.table->num_columns();
         ++column) {
        EXPECT_TRUE(expected.parquet.table->field(column)->Equals(
            *actual.parquet.table->field(column), true));
    }
    if (compare_row_groups) {
        EXPECT_EQ(actual.parquet.row_group_rows,
                  expected.parquet.row_group_rows);
    }
    EXPECT_EQ(actual.postings, expected.postings);
}

struct MixedMaterializeDataset {
    std::vector<FieldDataPtr> field_datas;
    std::map<JsonKey, JsonKeyLayoutType> key_types;
    std::vector<std::string> shared_paths;
    int64_t row_count;
    int64_t expected_dense_postings;
};

MixedMaterializeDataset
CreateMixedMaterializeDataset() {
    constexpr int64_t kRowCount = 97;
    std::vector<std::string> rows;
    rows.reserve(kRowCount);
    for (int64_t i = 0; i < kRowCount; ++i) {
        nlohmann::json row;
        row["id"] = i % 2 == 0 ? i : -i;
        row["flag"] = i % 3 == 0;
        row["mixed"] = i % 2 == 0 ? nlohmann::json(i * 10)
                                  : nlohmann::json(fmt::format("mixed-{}", i));
        row["dense"] = fmt::format("dense-{}-雪", i);
        row["nested"]["value"] = i * 100;
        row["a/b"]["~key"] = fmt::format("escaped-{}", i);
        if (i % 2 == 0) {
            row["text"] = i % 10 == 0 ? "" : fmt::format("text-{}\nline", i);
        }
        if (i % 3 != 0) {
            row["score"] = static_cast<double>(i) + 0.125;
        }
        if (i % 4 == 0) {
            row["arr"] = nlohmann::json::array(
                {i, fmt::format("item-{}", i), i % 8 == 0});
        }
        if (i % 10 == 0) {
            row["empty_obj"] = nlohmann::json::object();
        }
        if (i % 13 == 0) {
            row["nullv"] = nullptr;
        }
        rows.push_back(row.dump());
    }

    for (auto row : {5, 44, 96}) {
        rows[row].clear();
    }

    std::vector<bool> valid_rows(kRowCount, true);
    constexpr int64_t kNullableBegin = 19;
    constexpr int64_t kNullableEnd = 67;
    for (int64_t row = kNullableBegin; row < kNullableEnd; ++row) {
        if (row % 11 == 0) {
            valid_rows[row] = false;
        }
    }

    int64_t expected_dense_postings = 0;
    for (int64_t row = 0; row < kRowCount; ++row) {
        if (valid_rows[row] && !rows[row].empty()) {
            ++expected_dense_postings;
        }
    }

    std::vector<std::string> first(rows.begin(), rows.begin() + kNullableBegin);
    std::vector<std::string> nullable(rows.begin() + kNullableBegin,
                                      rows.begin() + kNullableEnd);
    std::vector<bool> nullable_valid(valid_rows.begin() + kNullableBegin,
                                     valid_rows.begin() + kNullableEnd);
    std::vector<std::string> last(rows.begin() + kNullableEnd, rows.end());

    return MixedMaterializeDataset{
        {
            CreateMaterializeJsonFieldData(first),
            CreateMaterializeJsonFieldData({}),
            CreateNullableMaterializeJsonFieldData(nullable, nullable_valid),
            CreateMaterializeJsonFieldData(last),
        },
        {
            {JsonKey("/id", JSONType::INT64), JsonKeyLayoutType::TYPED},
            {JsonKey("/flag", JSONType::BOOL),
             JsonKeyLayoutType::TYPED_NOT_ALL},
            {JsonKey("/text", JSONType::STRING),
             JsonKeyLayoutType::TYPED_NOT_ALL},
            {JsonKey("/score", JSONType::DOUBLE),
             JsonKeyLayoutType::TYPED_NOT_ALL},
            {JsonKey("/arr", JSONType::ARRAY),
             JsonKeyLayoutType::TYPED_NOT_ALL},
            {JsonKey("/mixed", JSONType::INT64), JsonKeyLayoutType::DYNAMIC},
            {JsonKey("/mixed", JSONType::STRING), JsonKeyLayoutType::DYNAMIC},
            {JsonKey("/dense", JSONType::STRING), JsonKeyLayoutType::SHARED},
            {JsonKey("/nested/value", JSONType::INT64),
             JsonKeyLayoutType::SHARED},
            {JsonKey("/a~1b/~0key", JSONType::STRING),
             JsonKeyLayoutType::SHARED},
            {JsonKey("/empty_obj", JSONType::OBJECT),
             JsonKeyLayoutType::SHARED},
            {JsonKey("/nullv", JSONType::NONE), JsonKeyLayoutType::SHARED},
        },
        {
            "/dense",
            "/nested",
            "/nested/value",
            "/a~1b",
            "/a~1b/~0key",
            "/empty_obj",
            "/nullv",
        },
        kRowCount,
        expected_dense_postings,
    };
}

}  // namespace

TEST(JsonStatsMemoryBudgetTest, CapacityUpdateWakesBlockedBuild) {
    auto& budget = storage::TransientMemoryBudget::GetJsonStatsBuildBudget();
    EXPECT_NE(&budget,
              &storage::TransientMemoryBudget::GetLoadTransientBudget());
    ASSERT_EQ(budget.InflightBytes(), 0);
    auto original_capacity = budget.CapacityBytes();
    auto restore_capacity = folly::makeGuard([&]() {
        executor_set_json_stats_build_max_inflight_bytes(
            static_cast<int64_t>(original_capacity));
    });

    executor_set_json_stats_build_max_inflight_bytes(1);
    EXPECT_EQ(budget.CapacityBytes(), 1);
    budget.Acquire(1);
    auto release_budget = folly::makeGuard([&]() { budget.Release(1); });

    auto waiter = std::async(std::launch::async, [&]() {
        budget.Acquire(1);
        budget.Release(1);
    });
    EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);

    executor_set_json_stats_build_max_inflight_bytes(2);
    EXPECT_EQ(budget.CapacityBytes(), 2);
    EXPECT_EQ(waiter.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    waiter.get();

    executor_set_json_stats_build_max_inflight_bytes(-1);
    EXPECT_EQ(budget.CapacityBytes(), 0);
}

TEST(JsonStatsMaterializeTest,
     ParallelMatchesLegacyAcrossRangesAndWriterBatches) {
    std::map<JsonKey, JsonKeyLayoutType> key_types = {
        {JsonKey("/int", JSONType::INT64), JsonKeyLayoutType::TYPED_NOT_ALL},
        {JsonKey("/double", JSONType::DOUBLE),
         JsonKeyLayoutType::TYPED_NOT_ALL},
        {JsonKey("/bool", JSONType::BOOL), JsonKeyLayoutType::TYPED_NOT_ALL},
        {JsonKey("/text", JSONType::STRING), JsonKeyLayoutType::TYPED_NOT_ALL},
        {JsonKey("/arr", JSONType::ARRAY), JsonKeyLayoutType::TYPED_NOT_ALL},
        {JsonKey("/mixed", JSONType::INT64), JsonKeyLayoutType::DYNAMIC},
        {JsonKey("/mixed", JSONType::STRING), JsonKeyLayoutType::DYNAMIC},
        {JsonKey("/rare", JSONType::STRING), JsonKeyLayoutType::SHARED},
        {JsonKey("/obj", JSONType::OBJECT), JsonKeyLayoutType::SHARED},
        {JsonKey("/nullv", JSONType::NONE), JsonKeyLayoutType::SHARED},
        {JsonKey("/nested/value", JSONType::DOUBLE), JsonKeyLayoutType::SHARED},
    };

    std::vector<FieldDataPtr> field_datas = {
        CreateMaterializeJsonFieldData({
            R"({"int":1,"double":1.5,"bool":true,"text":"one","arr":[1,2],"mixed":1,"rare":"first"})",
            "",
        }),
        CreateNullableMaterializeJsonFieldData(
            {
                R"({"int":2,"double":2.5,"bool":false,"text":"two","arr":[],"mixed":"two","nested":{"value":2.25}})",
                R"({"int":300,"double":300.5,"bool":true,"text":"invalid-row","arr":[300],"mixed":300,"rare":"ignored"})",
                R"({})",
                R"({"int":5,"double":5.5,"bool":false,"text":"escaped\nvalue","arr":[{"x":1}],"mixed":5,"obj":{},"nullv":null})",
                R"({"int":6,"double":6.5,"bool":true,"text":"six","arr":[6],"mixed":"six","rare":"middle"})",
            },
            {true, false, true, true, true}),
        CreateMaterializeJsonFieldData({
            R"({"int":7,"double":7.5,"bool":false,"text":"seven","arr":[7,8],"mixed":7})",
            R"({"int":8,"double":8.5,"bool":true,"text":"eight","arr":[],"mixed":"eight","rare":"last","nested":{"value":8.25}})",
            R"({"int":9,"double":9.5,"bool":false,"text":"nine","arr":[9],"mixed":9,"obj":{}})",
            R"({"int":10,"double":10.5,"bool":true,"text":"ten","arr":[10],"mixed":"ten","nullv":null})",
        }),
    };
    auto row_ranges = CreateJsonStatsRowRanges(field_datas, 3);
    ASSERT_EQ(row_ranges.size(), 4);
    ASSERT_EQ(row_ranges[0].slices.size(), 2);
    ASSERT_EQ(row_ranges[2].slices.size(), 2);

    const std::vector<std::string> shared_paths = {
        "/rare", "/obj", "/nullv", "/nested", "/nested/value"};
    auto original_thread_num =
        milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    auto restore_threads = folly::makeGuard([&]() {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num));
    });

    auto legacy = BuildMaterializeSnapshot(
        row_ranges, key_types, shared_paths, true, false);
    EXPECT_EQ(legacy.parquet.table->num_rows(), 11);
    EXPECT_FALSE(legacy.parquet.row_group_rows.empty());

    for (int worker_count : {1, 2, 4}) {
        SCOPED_TRACE(::testing::Message() << "worker_count=" << worker_count);
        executor_set_json_stats_build_thread_num(worker_count);
        auto parallel = BuildMaterializeSnapshot(
            row_ranges, key_types, shared_paths, true, true);
        ExpectMaterializeSnapshotsEqual(legacy, parallel, true);
    }
}

TEST(JsonStatsMaterializeTest, ParallelMatchesLegacyForEmptyInput) {
    std::vector<FieldDataPtr> field_datas = {
        CreateMaterializeJsonFieldData({}),
        CreateNullableMaterializeJsonFieldData({}, {}),
    };
    auto row_ranges = CreateJsonStatsRowRanges(field_datas, 3);
    ASSERT_TRUE(row_ranges.empty());

    std::map<JsonKey, JsonKeyLayoutType> key_types = {
        {JsonKey("/int", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/rare", JSONType::STRING), JsonKeyLayoutType::SHARED},
    };
    auto legacy =
        BuildMaterializeSnapshot(row_ranges, key_types, {}, true, false, 3);
    ASSERT_EQ(legacy.parquet.table, nullptr);

    auto original_thread_num =
        milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    auto restore_threads = folly::makeGuard([&]() {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num));
    });
    for (int worker_count : {1, 4}) {
        SCOPED_TRACE(::testing::Message() << "worker_count=" << worker_count);
        executor_set_json_stats_build_thread_num(worker_count);
        auto parallel =
            BuildMaterializeSnapshot(row_ranges, key_types, {}, true, true, 1);
        ExpectMaterializeSnapshotsEqual(legacy, parallel);
    }
}

TEST(JsonStatsMaterializeTest,
     ParallelMatchesLegacyForNullLikeRowsAndEmptyFieldData) {
    std::map<JsonKey, JsonKeyLayoutType> key_types = {
        {JsonKey("/int", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/text", JSONType::STRING), JsonKeyLayoutType::TYPED_NOT_ALL},
        {JsonKey("/rare", JSONType::STRING), JsonKeyLayoutType::SHARED},
    };
    std::vector<FieldDataPtr> field_datas = {
        CreateMaterializeJsonFieldData({}),
        CreateMaterializeJsonFieldData({"", "{}", "   "}),
        CreateNullableMaterializeJsonFieldData(
            {
                R"({"int":99,"text":"ignored","rare":"ignored"})",
                "",
                "{}",
                R"({"rare":"also-ignored"})",
            },
            {false, true, true, false}),
        CreateMaterializeJsonFieldData({}),
    };
    auto legacy_ranges = CreateJsonStatsRowRanges(field_datas, 2);
    auto legacy = BuildMaterializeSnapshot(
        legacy_ranges, key_types, {"/rare"}, true, false, 3);
    ASSERT_NE(legacy.parquet.table, nullptr);
    EXPECT_EQ(legacy.parquet.table->num_rows(), 7);
    EXPECT_TRUE(legacy.postings.empty());

    struct Scenario {
        int64_t range_rows;
        int64_t write_batch_rows;
        int worker_count;
    };
    const std::vector<Scenario> scenarios = {
        {1, 1, 1},
        {3, 2, 2},
        {64, 16, 4},
    };

    auto original_thread_num =
        milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    auto restore_threads = folly::makeGuard([&]() {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num));
    });
    for (const auto& scenario : scenarios) {
        SCOPED_TRACE(::testing::Message()
                     << "range_rows=" << scenario.range_rows
                     << ", write_batch_rows=" << scenario.write_batch_rows
                     << ", worker_count=" << scenario.worker_count);
        executor_set_json_stats_build_thread_num(scenario.worker_count);
        auto row_ranges =
            CreateJsonStatsRowRanges(field_datas, scenario.range_rows);
        auto parallel = BuildMaterializeSnapshot(row_ranges,
                                                 key_types,
                                                 {"/rare"},
                                                 true,
                                                 true,
                                                 scenario.write_batch_rows);
        ExpectMaterializeSnapshotsEqual(legacy, parallel);
    }
}

TEST(JsonStatsMaterializeTest,
     ParallelMatchesLegacyAcrossRangeAndWriterBatchMatrix) {
    auto dataset = CreateMixedMaterializeDataset();
    auto legacy_ranges = CreateJsonStatsRowRanges(dataset.field_datas, 9);
    auto legacy = BuildMaterializeSnapshot(
        legacy_ranges, dataset.key_types, dataset.shared_paths, true, false, 5);
    ASSERT_NE(legacy.parquet.table, nullptr);
    EXPECT_EQ(legacy.parquet.table->num_rows(), dataset.row_count);
    ASSERT_TRUE(legacy.postings.find("/dense") != legacy.postings.end());
    const auto& dense_postings = legacy.postings.at("/dense");
    ASSERT_EQ(dense_postings.size(), dataset.expected_dense_postings);
    for (size_t i = 1; i < dense_postings.size(); ++i) {
        EXPECT_LT(dense_postings[i - 1].first, dense_postings[i].first);
    }

    struct Scenario {
        int64_t range_rows;
        int64_t write_batch_rows;
        int worker_count;
    };
    const std::vector<Scenario> scenarios = {
        {1, 1, 4},
        {2, 17, 2},
        {7, 3, 4},
        {16, 64, 3},
        {128, 5, 4},
    };

    auto original_thread_num =
        milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    auto restore_threads = folly::makeGuard([&]() {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num));
    });
    for (const auto& scenario : scenarios) {
        SCOPED_TRACE(::testing::Message()
                     << "range_rows=" << scenario.range_rows
                     << ", write_batch_rows=" << scenario.write_batch_rows
                     << ", worker_count=" << scenario.worker_count);
        executor_set_json_stats_build_thread_num(scenario.worker_count);
        auto row_ranges =
            CreateJsonStatsRowRanges(dataset.field_datas, scenario.range_rows);
        auto parallel = BuildMaterializeSnapshot(row_ranges,
                                                 dataset.key_types,
                                                 dataset.shared_paths,
                                                 true,
                                                 true,
                                                 scenario.write_batch_rows);
        ExpectMaterializeSnapshotsEqual(legacy, parallel);
    }
}

TEST(JsonStatsMaterializeTest, ParallelMatchesLegacyWithSharedOnlySchema) {
    auto dataset = CreateMixedMaterializeDataset();
    for (auto& entry : dataset.key_types) {
        entry.second = JsonKeyLayoutType::SHARED;
    }
    dataset.shared_paths = {
        "/id",
        "/flag",
        "/text",
        "/score",
        "/arr",
        "/mixed",
        "/dense",
        "/nested",
        "/nested/value",
        "/a~1b",
        "/a~1b/~0key",
        "/empty_obj",
        "/nullv",
    };

    auto legacy = BuildMaterializeSnapshot(
        CreateJsonStatsRowRanges(dataset.field_datas, 6),
        dataset.key_types,
        dataset.shared_paths,
        true,
        false,
        5);
    ASSERT_NE(legacy.parquet.table, nullptr);
    ASSERT_EQ(legacy.parquet.table->num_columns(), 1);
    ASSERT_EQ(legacy.postings.at("/dense").size(),
              dataset.expected_dense_postings);

    struct Scenario {
        int64_t range_rows;
        int64_t write_batch_rows;
        int worker_count;
    };
    const std::vector<Scenario> scenarios = {
        {1, 1, 1},
        {7, 3, 4},
        {128, 64, 4},
    };

    auto original_thread_num =
        milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    auto restore_threads = folly::makeGuard([&]() {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num));
    });
    for (const auto& scenario : scenarios) {
        SCOPED_TRACE(::testing::Message()
                     << "range_rows=" << scenario.range_rows
                     << ", write_batch_rows=" << scenario.write_batch_rows
                     << ", worker_count=" << scenario.worker_count);
        executor_set_json_stats_build_thread_num(scenario.worker_count);
        auto parallel = BuildMaterializeSnapshot(
            CreateJsonStatsRowRanges(dataset.field_datas, scenario.range_rows),
            dataset.key_types,
            dataset.shared_paths,
            true,
            true,
            scenario.write_batch_rows);
        ASSERT_EQ(parallel.parquet.table->num_columns(), 1);
        ExpectMaterializeSnapshotsEqual(legacy, parallel);
    }
}

TEST(JsonStatsMaterializeTest,
     TinyMemoryBudgetMatchesLegacyForMultipleAndOversizedChunks) {
    auto legacy_dataset = CreateMixedMaterializeDataset();
    auto parallel_dataset = CreateMixedMaterializeDataset();
    auto legacy = BuildMaterializeSnapshot(
        CreateJsonStatsRowRanges(legacy_dataset.field_datas, 8),
        legacy_dataset.key_types,
        legacy_dataset.shared_paths,
        true,
        false,
        5);

    auto original_thread_num =
        milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    auto& budget = storage::TransientMemoryBudget::GetJsonStatsBuildBudget();
    ASSERT_EQ(budget.InflightBytes(), 0);
    auto original_capacity = budget.CapacityBytes();
    auto restore_resources = folly::makeGuard([&]() {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num));
        executor_set_json_stats_build_max_inflight_bytes(
            static_cast<int64_t>(original_capacity));
    });

    executor_set_json_stats_build_thread_num(4);
    executor_set_json_stats_build_max_inflight_bytes(1);

    auto many_chunks = BuildMaterializeSnapshot(
        CreateJsonStatsRowRanges(parallel_dataset.field_datas, 1),
        parallel_dataset.key_types,
        parallel_dataset.shared_paths,
        true,
        true,
        2);
    ExpectMaterializeSnapshotsEqual(legacy, many_chunks);
    EXPECT_EQ(budget.InflightBytes(), 0);

    auto oversized_dataset = CreateMixedMaterializeDataset();
    auto one_oversized_chunk = BuildMaterializeSnapshot(
        CreateJsonStatsRowRanges(oversized_dataset.field_datas,
                                 oversized_dataset.row_count),
        oversized_dataset.key_types,
        oversized_dataset.shared_paths,
        true,
        true,
        64);
    ExpectMaterializeSnapshotsEqual(legacy, one_oversized_chunk);
    EXPECT_EQ(budget.InflightBytes(), 0);
}

TEST(JsonStatsMaterializeTest, ConcurrentBuildsSharingExecutorMatchLegacy) {
    auto legacy_dataset = CreateMixedMaterializeDataset();
    auto first_dataset = CreateMixedMaterializeDataset();
    auto second_dataset = CreateMixedMaterializeDataset();
    auto legacy = BuildMaterializeSnapshot(
        CreateJsonStatsRowRanges(legacy_dataset.field_datas, 8),
        legacy_dataset.key_types,
        legacy_dataset.shared_paths,
        true,
        false,
        5);

    auto original_thread_num =
        milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    auto& budget = storage::TransientMemoryBudget::GetJsonStatsBuildBudget();
    ASSERT_EQ(budget.InflightBytes(), 0);
    auto original_capacity = budget.CapacityBytes();
    auto restore_resources = folly::makeGuard([&]() {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num));
        executor_set_json_stats_build_max_inflight_bytes(
            static_cast<int64_t>(original_capacity));
    });
    executor_set_json_stats_build_thread_num(4);
    executor_set_json_stats_build_max_inflight_bytes(1);

    auto first_future = std::async(std::launch::async, [&]() {
        return BuildMaterializeSnapshot(
            CreateJsonStatsRowRanges(first_dataset.field_datas, 1),
            first_dataset.key_types,
            first_dataset.shared_paths,
            true,
            true,
            2);
    });
    auto second_future = std::async(std::launch::async, [&]() {
        return BuildMaterializeSnapshot(
            CreateJsonStatsRowRanges(second_dataset.field_datas, 13),
            second_dataset.key_types,
            second_dataset.shared_paths,
            true,
            true,
            11);
    });

    ASSERT_EQ(first_future.wait_for(std::chrono::seconds(30)),
              std::future_status::ready);
    ASSERT_EQ(second_future.wait_for(std::chrono::seconds(30)),
              std::future_status::ready);
    auto first = first_future.get();
    auto second = second_future.get();
    ExpectMaterializeSnapshotsEqual(legacy, first);
    ExpectMaterializeSnapshotsEqual(legacy, second);
    ExpectMaterializeSnapshotsEqual(first, second);
    EXPECT_EQ(budget.InflightBytes(), 0);
}

TEST(JsonStatsMaterializeTest, ParallelRethrowsLegacyParseError) {
    std::map<JsonKey, JsonKeyLayoutType> key_types = {
        {JsonKey("/int", JSONType::INT64), JsonKeyLayoutType::TYPED},
    };
    auto original_thread_num =
        milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    auto& budget = storage::TransientMemoryBudget::GetJsonStatsBuildBudget();
    ASSERT_EQ(budget.InflightBytes(), 0);
    auto original_capacity = budget.CapacityBytes();
    auto restore_resources = folly::makeGuard([&]() {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num));
        executor_set_json_stats_build_max_inflight_bytes(
            static_cast<int64_t>(original_capacity));
    });
    executor_set_json_stats_build_max_inflight_bytes(1);

    for (int error_row : {0, 4, 8}) {
        SCOPED_TRACE(::testing::Message() << "error_row=" << error_row);
        std::vector<std::string> rows;
        for (int row = 0; row < 9; ++row) {
            rows.push_back(fmt::format(R"({{"int":{}}})", row));
        }
        rows[error_row] = R"({"int":)";
        std::vector<FieldDataPtr> field_datas = {
            CreateMaterializeJsonFieldData(rows),
        };
        auto row_ranges = CreateJsonStatsRowRanges(field_datas, 2);
        ASSERT_EQ(row_ranges.size(), 5);

        auto capture_error = [&](bool parallel) {
            auto test_index = CreateMaterializeTestIndex(false, 3);
            BuildKeyStatsAccessor::Prepare(*test_index.stats, key_types, 9);
            try {
                if (parallel) {
                    BuildKeyStatsAccessor::CallParallel(
                        *test_index.stats, row_ranges, false);
                } else {
                    BuildKeyStatsAccessor::CallLegacy(
                        *test_index.stats, row_ranges, false);
                }
            } catch (const std::exception& e) {
                return std::string(e.what());
            }
            return std::string{};
        };

        auto legacy_error = capture_error(false);
        ASSERT_FALSE(legacy_error.empty());
        EXPECT_NE(legacy_error.find("Failed to parse Json"), std::string::npos);
        for (int worker_count : {1, 2, 4}) {
            SCOPED_TRACE(::testing::Message()
                         << "worker_count=" << worker_count);
            executor_set_json_stats_build_thread_num(worker_count);
            auto parallel_error = capture_error(true);
            ASSERT_FALSE(parallel_error.empty());
            EXPECT_EQ(parallel_error, legacy_error);
            EXPECT_EQ(budget.InflightBytes(), 0);
        }
    }

    std::vector<FieldDataPtr> valid_field_datas = {
        CreateMaterializeJsonFieldData(
            {R"({"int":1})", R"({"int":2})", R"({"int":3})"}),
    };
    auto valid_ranges = CreateJsonStatsRowRanges(valid_field_datas, 1);
    auto valid_legacy =
        BuildMaterializeSnapshot(valid_ranges, key_types, {}, false, false, 2);
    auto valid_parallel =
        BuildMaterializeSnapshot(valid_ranges, key_types, {}, false, true, 2);
    ExpectMaterializeSnapshotsEqual(valid_legacy, valid_parallel);
    EXPECT_EQ(budget.InflightBytes(), 0);
}

class JsonKeyStatsTest : public ::testing::TestWithParam<bool> {
 protected:
    void
    Init(int64_t collection_id,
         int64_t partition_id,
         int64_t segment_id,
         int64_t field_id,
         int64_t index_build_id,
         int64_t index_version,
         int64_t size) {
        proto::schema::FieldSchema field_schema;
        field_schema.set_data_type(proto::schema::DataType::JSON);
        field_schema.set_nullable(nullable_);
        auto field_meta = storage::FieldDataMeta{
            collection_id, partition_id, segment_id, field_id, field_schema};
        auto index_meta = storage::IndexMeta{
            segment_id, field_id, index_build_id, index_version};

        data_ = GenerateJsons(size);
        auto field_data =
            storage::CreateFieldData(DataType::JSON, DataType::NONE, nullable_);
        if (nullable_) {
            valid_data.reserve(size_);
            for (size_t i = 0; i < size_; i++) {
                if (i % 2 == 0) {
                    valid_data.push_back(true);
                } else {
                    valid_data.push_back(false);
                }
            }

            int byteSize = (size_ + 7) / 8;
            uint8_t* valid_data_ = new uint8_t[byteSize];
            for (int i = 0; i < size_; i++) {
                bool value = valid_data[i];
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                if (value) {
                    valid_data_[byteIndex] |= (1 << bitIndex);
                } else {
                    valid_data_[byteIndex] &= ~(1 << bitIndex);
                }
            }
            field_data->FillFieldData(
                data_.data(), valid_data_, data_.size(), 0);
            delete[] valid_data_;
        } else {
            field_data->FillFieldData(data_.data(), data_.size());
        }

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        insert_data.SetFieldDataMeta(field_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    TestLocalPath,
                                    collection_id,
                                    partition_id,
                                    segment_id,
                                    field_id,
                                    0);
        chunk_manager_->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        storage::FileManagerContext ctx(
            field_meta, index_meta, chunk_manager_, fs_);
        std::vector<std::string> index_files;

        Config config;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

        auto build_index = std::make_shared<JsonKeyStats>(ctx, false);
        build_index->Build(config);

        auto create_index_result = build_index->Upload(config);
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files = create_index_result->GetIndexFiles();

        index::CreateIndexInfo index_info{};
        config["index_files"] = index_files;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        config[milvus::index::ENABLE_MMAP] = true;
        config[milvus::index::MMAP_FILE_PATH] = TestLocalPath + "mmap-file";
        config[STATS_BASE_PATH_KEY] =
            storage::GenRemoteJsonStatsPathPrefix(chunk_manager_,
                                                  index_build_id,
                                                  index_version,
                                                  collection_id,
                                                  partition_id,
                                                  segment_id,
                                                  field_id);
        index_ = std::make_shared<JsonKeyStats>(ctx, true);
        index_->Load(milvus::tracer::TraceContext{}, config);
    }

    void
    SetUp() override {
        nullable_ = GetParam();
        type_ = DataType::JSON;
        int64_t collection_id = 1;
        int64_t partition_id = 2;
        int64_t segment_id = 3;
        field_id_ = 101;
        int64_t index_build_id = GenerateRandomInt64(1, 100000);
        int64_t index_version = 1;
        size_ = 1000;  // Use a larger size for better testing
        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = TestLocalPath;
        chunk_manager_ = storage::CreateChunkManager(storage_config);

        fs_ = milvus::segcore::GetDefaultArrowFileSystem();

        Init(collection_id,
             partition_id,
             segment_id,
             field_id_,
             index_build_id,
             index_version,
             size_);
    }

    virtual ~JsonKeyStatsTest() override {
    }

 public:
    std::shared_ptr<JsonKeyStats> index_;
    DataType type_;
    bool nullable_;
    size_t size_;
    int64_t field_id_;
    FixedVector<bool> valid_data;
    std::vector<milvus::Json> data_;
    std::vector<std::string> json_col;
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
    milvus_storage::ArrowFileSystemPtr fs_;
};

INSTANTIATE_TEST_SUITE_P(JsonKeyStatsTestSuite,
                         JsonKeyStatsTest,
                         ::testing::Values(true, false));

TEST_P(JsonKeyStatsTest, TestBasicOperations) {
    // Test Count
    EXPECT_EQ(index_->Count(), size_);

    // Test Size
    EXPECT_EQ(index_->Size(), size_);

    // Test HasRawData
    EXPECT_FALSE(index_->HasRawData());
}

TEST_P(JsonKeyStatsTest, TestExecuteForSharedData) {
    std::string path = "/int_shared";
    int count = 0;
    PinWrapper<BsonInvertedIndex*> bson_index{nullptr};
    index_->ExecuteForSharedData(
        nullptr,
        bson_index,
        path,
        [&](BsonView bson, uint32_t row_id, uint32_t offset) { count++; });
    std::cout << "count: " << count << std::endl;
    if (nullable_) {
        EXPECT_EQ(count, 100);
    } else {
        EXPECT_EQ(count, 200);
    }
}

TEST_P(JsonKeyStatsTest, TestExecutorForGettingValid) {
    std::string path = "/int";
    TargetBitmap valid_res(size_, true);
    TargetBitmapView valid_res_view(valid_res);
    auto shredding_fields = index_->GetShreddingFields(path);
    for (const auto& field : shredding_fields) {
        auto processed_size =
            index_->ExecutorForGettingValid(nullptr, field, valid_res_view);
        EXPECT_EQ(processed_size, size_);
    }
    std::cout << "can not skip shared" << std::endl;
    PinWrapper<BsonInvertedIndex*> bson_index{nullptr};
    index_->ExecuteForSharedData(
        nullptr,
        bson_index,
        path,
        [&](BsonView bson, uint32_t row_id, uint32_t offset) {
            valid_res[row_id] = true;
        });
    std::cout << "valid_res.count(): " << valid_res.count() << std::endl;
    if (nullable_) {
        EXPECT_EQ(valid_res.count(), 400);
    } else {
        EXPECT_EQ(valid_res.count(), 800);
    }
}

TEST_P(JsonKeyStatsTest, TestExecutorForShreddingData) {
    std::string path = "/int";
    TargetBitmap res(size_);
    TargetBitmap valid_res(size_, true);
    TargetBitmapView res_view(res);
    TargetBitmapView valid_res_view(valid_res);

    auto func = [](const int64_t* data,
                   ValidityView valid_data,
                   const int size,
                   TargetBitmapView res,
                   TargetBitmapView valid_res) {
        for (int i = 0; i < size; i++) {
            if (valid_data[i]) {
                res[i] = true;
                valid_res[i] = true;
            }
        }
    };

    auto field_name = *(index_->GetShreddingFields(path).begin());
    std::cout << "field_name: " << field_name << std::endl;
    int processed_size = index_->ExecutorForShreddingData<int64_t>(
        nullptr, field_name, func, nullptr, res_view, valid_res_view);
    std::cout << "processed_size: " << processed_size << std::endl;
    EXPECT_EQ(processed_size, size_);

    if (nullable_) {
        EXPECT_EQ(res.count(), 400);
    } else {
        EXPECT_EQ(res.count(), 800);
    }

    TargetBitmap skipped_res(size_, true);
    TargetBitmap skipped_valid_res(size_, true);
    processed_size = index_->ExecutorForShreddingData<int64_t>(
        nullptr,
        field_name,
        func,
        [](const SkipIndex&, std::string, int) { return true; },
        TargetBitmapView(skipped_res),
        TargetBitmapView(skipped_valid_res));
    EXPECT_EQ(processed_size, size_);
    const auto expected_valid_count = nullable_ ? 400 : 800;
    EXPECT_EQ(skipped_res.count(), expected_valid_count);
    EXPECT_EQ(skipped_valid_res.count(), expected_valid_count);
}

TEST_P(JsonKeyStatsTest, TestGetShreddingFields) {
    std::string pointer = "/int";
    auto fields = index_->GetShreddingFields(pointer);
    EXPECT_FALSE(fields.empty());

    std::vector<JSONType> types = {JSONType::INT64};
    auto typed_fields = index_->GetShreddingFields(pointer, types);
    EXPECT_FALSE(typed_fields.empty());
}

class JsonKeyStatsUploadLoadTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        collection_id_ = 1;
        partition_id_ = 2;
        segment_id_ = 3;
        field_id_ = 101;
        index_build_id_ = GenerateRandomInt64(1, 100000);
        index_version_ = 10000;
        root_path_ = TestLocalPath;

        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path_;
        chunk_manager_ = storage::CreateChunkManager(storage_config);

        fs_ = milvus::segcore::GetDefaultArrowFileSystem();
    }

    void
    TearDown() override {
    }

    void
    InitContext() {
        proto::schema::FieldSchema field_schema;
        field_schema.set_data_type(proto::schema::DataType::JSON);
        field_schema.set_nullable(false);
        field_meta_ = storage::FieldDataMeta{collection_id_,
                                             partition_id_,
                                             segment_id_,
                                             field_id_,
                                             field_schema};
        index_meta_ = storage::IndexMeta{
            segment_id_, field_id_, index_build_id_, index_version_};
    }

    void
    PrepareData(const std::vector<std::string>& json_strings) {
        data_.clear();
        for (const auto& str : json_strings) {
            data_.push_back(milvus::Json(simdjson::padded_string(str)));
        }
    }

    void
    BuildAndUpload(int64_t lack_binlog_rows = 0) {
        auto field_data =
            storage::CreateFieldData(DataType::JSON, DataType::NONE, false);
        field_data->FillFieldData(data_.data(), data_.size());

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        insert_data.SetFieldDataMeta(field_meta_);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    root_path_,
                                    collection_id_,
                                    partition_id_,
                                    segment_id_,
                                    field_id_,
                                    0);
        chunk_manager_->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        storage::FileManagerContext ctx(
            field_meta_, index_meta_, chunk_manager_, fs_);

        Config config;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
        config[INDEX_NUM_ROWS_KEY] =
            static_cast<int64_t>(data_.size()) + lack_binlog_rows;

        build_index_ = std::make_shared<JsonKeyStats>(ctx, false);
        build_index_->Build(config);

        auto create_index_result = build_index_->Upload(config);
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files_ = create_index_result->GetIndexFiles();
    }

    void
    Load() {
        storage::FileManagerContext ctx(
            field_meta_, index_meta_, chunk_manager_, fs_);
        Config config;
        config["index_files"] = index_files_;
        config[milvus::index::ENABLE_MMAP] = true;
        config[milvus::index::MMAP_FILE_PATH] = TestLocalPath + "mmap-file";
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        config[STATS_BASE_PATH_KEY] =
            storage::GenRemoteJsonStatsPathPrefix(chunk_manager_,
                                                  index_build_id_,
                                                  index_version_,
                                                  collection_id_,
                                                  partition_id_,
                                                  segment_id_,
                                                  field_id_);
        load_index_ = std::make_shared<JsonKeyStats>(ctx, true);
        load_index_->Load(milvus::tracer::TraceContext{}, config);
    }

    void
    VerifyBasicOperations() {
        EXPECT_EQ(load_index_->Count(), data_.size());
        EXPECT_EQ(load_index_->Size(), data_.size());
        EXPECT_FALSE(load_index_->HasRawData());
    }

    void
    VerifyPathInShared(const std::string& path) {
        TargetBitmap bitset(data_.size());
        TargetBitmapView bitset_view(bitset);
        PinWrapper<BsonInvertedIndex*> bson_index{nullptr};
        load_index_->ExecuteForSharedData(
            nullptr,
            bson_index,
            path,
            [&](BsonView bson, uint32_t row_id, uint32_t offset) {
                bitset[row_id] = true;
            });
        EXPECT_GT(bitset.size(), 0);
    }

    void
    VerifyPathInShredding(const std::string& path) {
        auto fields = load_index_->GetShreddingFields(path);
        EXPECT_GT(fields.size(), 0);
    }

    void
    VerifyJsonType(const std::string& path, JSONType expected_type) {
        auto type = load_index_->GetShreddingJsonType(path);
        EXPECT_EQ(int(type), int(expected_type));
    }

 public:
    int64_t collection_id_;
    int64_t partition_id_;
    int64_t segment_id_;
    int64_t field_id_;
    int64_t index_build_id_;
    int64_t index_version_;
    std::string root_path_;
    storage::FieldDataMeta field_meta_;
    storage::IndexMeta index_meta_;
    std::vector<milvus::Json> data_;
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
    std::shared_ptr<JsonKeyStats> build_index_;
    std::shared_ptr<JsonKeyStats> load_index_;
    std::vector<std::string> index_files_;
    milvus_storage::ArrowFileSystemPtr fs_;
};

TEST_F(JsonKeyStatsUploadLoadTest, TestSimpleJson) {
    std::vector<std::string> json_strings = {
        R"({"int": 1, "double": 1.5, "string": "test", "bool": true})",
        R"({"int": 2, "double": 2.5, "string": "test2", "bool": false})",
        R"({"int": 3, "double": 3.5, "string": "test3", "bool": true})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/int");
    VerifyPathInShredding("/double");
    VerifyPathInShredding("/string");
    VerifyPathInShredding("/bool");
    VerifyJsonType("/int_INT64", JSONType::INT64);
    VerifyJsonType("/double_DOUBLE", JSONType::DOUBLE);
    VerifyJsonType("/string_STRING", JSONType::STRING);
    VerifyJsonType("/bool_BOOL", JSONType::BOOL);
}

TEST_F(JsonKeyStatsUploadLoadTest, TestNestedJson) {
    std::vector<std::string> json_strings = {
        R"({"nested": {"int": 1, "double": 1.5}, "array": [1, 2, 3]})",
        R"({"nested": {"int": 2, "double": 2.5}, "array": [4, 5, 6]})",
        R"({"nested": {"int": 3, "double": 3.5}, "array": [7, 8, 9]})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/nested/int");
    VerifyPathInShredding("/nested/double");
    VerifyPathInShared("/array");
    VerifyJsonType("/nested/int_INT64", JSONType::INT64);
    VerifyJsonType("/nested/double_DOUBLE", JSONType::DOUBLE);
}

TEST_F(JsonKeyStatsUploadLoadTest, TestComplexJson) {
    std::vector<std::string> json_strings = {
        R"({
            "user": {
                "id": 1,
                "name": "John",
                "scores": [85, 90, 95],
                "address": {
                    "city": "New York",
                    "zip": 10001
                }
            },
            "timestamp": 1234567890
        })",
        R"({
            "user": {
                "id": 2,
                "name": "Jane",
                "scores": [88, 92, 98],
                "address": {
                    "city": "Los Angeles",
                    "zip": 90001
                }
            },
            "timestamp": 1234567891
        })"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/user/id");
    VerifyPathInShredding("/user/name");
    VerifyPathInShared("/user/scores");
    VerifyPathInShredding("/user/address/city");
    VerifyPathInShredding("/user/address/zip");
    VerifyPathInShredding("/timestamp");
    VerifyJsonType("/user/id_INT64", JSONType::INT64);
    VerifyJsonType("/user/name_STRING", JSONType::STRING);
    VerifyJsonType("/user/address/city_STRING", JSONType::STRING);
    VerifyJsonType("/user/address/zip_INT64", JSONType::INT64);
    VerifyJsonType("/timestamp_INT64", JSONType::INT64);
}

// Test that meta.json file is created and contains correct metadata
TEST_F(JsonKeyStatsUploadLoadTest, TestMetaFileCreation) {
    std::vector<std::string> json_strings = {
        R"({"int": 1, "string": "test1"})",
        R"({"int": 2, "string": "test2"})",
        R"({"int": 3, "string": "test3"})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();

    // Verify meta.json is in the index files
    bool meta_file_found = false;
    for (const auto& file : index_files_) {
        if (file.find(JSON_STATS_META_FILE_NAME) != std::string::npos &&
            file.find(JSON_STATS_SHARED_INDEX_PATH) == std::string::npos &&
            file.find(JSON_STATS_SHREDDING_DATA_PATH) == std::string::npos) {
            meta_file_found = true;
            break;
        }
    }
    EXPECT_TRUE(meta_file_found) << "meta.json should be in index files";

    // Load and verify
    Load();
    VerifyBasicOperations();
    VerifyPathInShredding("/int");
    VerifyPathInShredding("/string");
}

// Test full pipeline: build -> upload -> load -> query
TEST_F(JsonKeyStatsUploadLoadTest, TestFullPipelineWithMetaFile) {
    std::vector<std::string> json_strings;
    // Generate more data for a comprehensive test
    for (int i = 0; i < 100; i++) {
        json_strings.push_back(fmt::format(
            R"({{"id": {}, "name": "user_{}", "score": {}, "active": {}}})",
            i,
            i,
            i * 1.5,
            i % 2 == 0 ? "true" : "false"));
    }

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();

    // Verify index files structure
    bool has_meta = false;
    bool has_shredding = false;
    bool has_shared_index = false;
    for (const auto& file : index_files_) {
        if (file.find(JSON_STATS_META_FILE_NAME) != std::string::npos &&
            file.find(JSON_STATS_SHARED_INDEX_PATH) == std::string::npos &&
            file.find(JSON_STATS_SHREDDING_DATA_PATH) == std::string::npos) {
            has_meta = true;
        }
        if (file.find(JSON_STATS_SHREDDING_DATA_PATH) != std::string::npos) {
            has_shredding = true;
        }
        if (file.find(JSON_STATS_SHARED_INDEX_PATH) != std::string::npos) {
            has_shared_index = true;
        }
    }
    EXPECT_TRUE(has_meta) << "Should have meta.json file";
    EXPECT_TRUE(has_shredding) << "Should have shredding data files";
    EXPECT_TRUE(has_shared_index) << "Should have shared key index files";

    // Load and verify
    Load();

    // Verify count
    EXPECT_EQ(load_index_->Count(), 100);

    // Verify shredding fields exist
    auto id_fields = load_index_->GetShreddingFields("/id");
    EXPECT_FALSE(id_fields.empty());

    auto name_fields = load_index_->GetShreddingFields("/name");
    EXPECT_FALSE(name_fields.empty());

    auto score_fields = load_index_->GetShreddingFields("/score");
    EXPECT_FALSE(score_fields.empty());

    auto active_fields = load_index_->GetShreddingFields("/active");
    EXPECT_FALSE(active_fields.empty());

    // Verify types
    VerifyJsonType("/id_INT64", JSONType::INT64);
    VerifyJsonType("/name_STRING", JSONType::STRING);
    VerifyJsonType("/score_DOUBLE", JSONType::DOUBLE);
    VerifyJsonType("/active_BOOL", JSONType::BOOL);
}

// Test backward compatibility: load data without meta.json
// This simulates old format where metadata was stored in parquet files.
// Note: Since new code doesn't write metadata to parquet anymore, this test
// verifies that the loading code path handles missing meta.json gracefully
// and falls back to reading from parquet (even if parquet metadata is empty).
TEST_F(JsonKeyStatsUploadLoadTest, TestLoadWithoutMetaFile) {
    std::vector<std::string> json_strings = {
        R"({"int": 1, "string": "test1"})",
        R"({"int": 2, "string": "test2"})",
        R"({"int": 3, "string": "test3"})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();

    // Remove meta.json from index_files to simulate old format
    std::vector<std::string> index_files_without_meta;
    for (const auto& file : index_files_) {
        if (file.find(JSON_STATS_META_FILE_NAME) == std::string::npos ||
            file.find(JSON_STATS_SHARED_INDEX_PATH) != std::string::npos ||
            file.find(JSON_STATS_SHREDDING_DATA_PATH) != std::string::npos) {
            index_files_without_meta.push_back(file);
        }
    }

    // Verify we actually removed the meta file
    EXPECT_LT(index_files_without_meta.size(), index_files_.size());

    // Replace index_files_ with version without meta
    index_files_ = index_files_without_meta;

    // Load should still work - it will try to read from parquet metadata
    // (which is empty in new format, but the code path should not crash)
    Load();

    // Basic operations should still work
    EXPECT_EQ(load_index_->Count(), data_.size());
    EXPECT_EQ(load_index_->Size(), data_.size());

    // Note: GetShreddingFields may return empty because key_field_map_ is empty
    // when both meta.json and parquet metadata are missing/empty.
    // This is expected behavior for backward compatibility path.
}

// Test that multiple build-upload-load cycles work correctly
TEST_F(JsonKeyStatsUploadLoadTest, TestMultipleBuildCycles) {
    // First cycle
    {
        std::vector<std::string> json_strings = {R"({"a": 1, "b": "test1"})",
                                                 R"({"a": 2, "b": "test2"})"};

        InitContext();
        PrepareData(json_strings);
        BuildAndUpload();
        Load();

        EXPECT_EQ(load_index_->Count(), 2);
        VerifyPathInShredding("/a");
        VerifyPathInShredding("/b");
    }

    // Clean up for second cycle
    boost::filesystem::remove_all(chunk_manager_->GetRootPath());

    // Second cycle with different schema
    {
        index_build_id_ = GenerateRandomInt64(1, 100000);  // New build id
        std::vector<std::string> json_strings = {
            R"({"x": 100, "y": 200, "z": "data1"})",
            R"({"x": 101, "y": 201, "z": "data2"})",
            R"({"x": 102, "y": 202, "z": "data3"})"};

        InitContext();
        PrepareData(json_strings);
        BuildAndUpload();
        Load();

        EXPECT_EQ(load_index_->Count(), 3);
        VerifyPathInShredding("/x");
        VerifyPathInShredding("/y");
        VerifyPathInShredding("/z");
    }
}

// Regression test: BuildKeyStats must increment row_id for empty JSON strings.
// Before the fix, `continue` in the empty-string branch skipped `row_id++`,
// causing all subsequent shared-data inverted-index entries to record wrong
// row_ids.  This led to queries reading BSON from wrong parquet rows —
// producing either silent wrong results or assertion crashes.
//
// Scenario simulated: lack_binlog_rows prepends null rows (empty JSON) to a
// non-nullable JSON field (schema.nullable() == false).
TEST_F(JsonKeyStatsUploadLoadTest, TestEmptyJsonRowIdNotSkipped) {
    // 10 valid rows.  "/a" appears in all rows (high hit ratio → shredding).
    // "/rare" appears in only 1 row (low hit ratio → SHARED), so it goes
    // through the inverted-index path where the row_id bug manifests.
    const int64_t kLackRows = 2;  // 2 prepended empty-JSON rows
    const int kValidRows = 10;
    const int kTotalRows = kLackRows + kValidRows;
    const int kRareRowIndex = kTotalRows - 1;  // last row has "/rare"

    std::vector<std::string> json_strings;
    for (int i = 0; i < kValidRows; i++) {
        if (i == kValidRows - 1) {
            // last valid row has an extra "rare" key
            json_strings.push_back(
                fmt::format(R"({{"a": {}, "rare": 42}})", i));
        } else {
            json_strings.push_back(fmt::format(R"({{"a": {}}})", i));
        }
    }

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload(kLackRows);
    Load();

    // Total row count should include lack_binlog_rows
    EXPECT_EQ(load_index_->Count(), kTotalRows);

    // "/a" should be in shredding (high hit ratio)
    auto a_fields = load_index_->GetShreddingFields("/a");
    EXPECT_FALSE(a_fields.empty()) << "'/a' should be shredded";

    // "/rare" should be in shared data (low hit ratio < 0.3 threshold)
    // Verify via ExecuteForSharedData: the ONLY row that should
    // have "/rare" is row kRareRowIndex (the last row).
    TargetBitmap exists_bitset(kTotalRows);
    PinWrapper<BsonInvertedIndex*> bson_index{nullptr};
    load_index_->ExecuteForSharedData(
        nullptr,
        bson_index,
        "/rare",
        [&](BsonView bson, uint32_t row_id, uint32_t offset) {
            exists_bitset[row_id] = true;
        });

    EXPECT_EQ(exists_bitset.count(), 1)
        << "Exactly one row should have '/rare' in shared data";
    EXPECT_TRUE(exists_bitset[kRareRowIndex])
        << "'/rare' should exist at row " << kRareRowIndex
        << " (the last row), not at row " << (kRareRowIndex - kLackRows);

    // Double-check: the row_id recorded for "/rare" must NOT be the
    // off-by-kLackRows wrong position that the old buggy code produced.
    if (kRareRowIndex >= kLackRows) {
        EXPECT_FALSE(exists_bitset[kRareRowIndex - kLackRows])
            << "'/rare' must NOT appear at the wrong (shifted) row_id";
    }
}

// ==================== Special Characters Tests ====================

// Test JSON keys containing special characters (dots, spaces, hyphens, etc.)
TEST_F(JsonKeyStatsUploadLoadTest, TestSpecialCharacterKeys) {
    std::vector<std::string> json_strings = {
        R"({"key-with-dash": 1, "key.with.dot": 2, "key with space": 3})",
        R"({"key-with-dash": 4, "key.with.dot": 5, "key with space": 6})",
        R"({"key-with-dash": 7, "key.with.dot": 8, "key with space": 9})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/key-with-dash");
    VerifyPathInShredding("/key.with.dot");
    VerifyPathInShredding("/key with space");
}

// Test JSON string values with escape sequences
TEST_F(JsonKeyStatsUploadLoadTest, TestEscapedStringValues) {
    std::vector<std::string> json_strings = {
        R"({"msg": "hello\nworld", "path": "c:\\temp\\file", "quote": "say \"hi\""})",
        R"({"msg": "line1\tline2", "path": "d:\\data\\log", "quote": "say \"bye\""})",
        R"({"msg": "foo\bbar", "path": "e:\\usr\\bin", "quote": "say \"ok\""})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/msg");
    VerifyPathInShredding("/path");
    VerifyPathInShredding("/quote");
    VerifyJsonType("/msg_STRING", JSONType::STRING);
}

// Test JSON with unicode characters in keys and values
TEST_F(JsonKeyStatsUploadLoadTest, TestUnicodeContent) {
    std::vector<std::string> json_strings = {
        R"({"name": "\\u4e16\\u754c", "value": 1})",
        R"({"name": "\\u0041\\u0042", "value": 2})",
        R"({"name": "hello", "value": 3})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/name");
    VerifyPathInShredding("/value");
}

// Test JSON keys with underscores and numbers
TEST_F(JsonKeyStatsUploadLoadTest, TestKeysWithUnderscoresAndNumbers) {
    std::vector<std::string> json_strings = {
        R"({"field_1": 10, "_private": "a", "123abc": true})",
        R"({"field_1": 20, "_private": "b", "123abc": false})",
        R"({"field_1": 30, "_private": "c", "123abc": true})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/field_1");
    VerifyPathInShredding("/_private");
    VerifyPathInShredding("/123abc");
}

// ==================== Corner Case Tests ====================

// Test with a single row
TEST_F(JsonKeyStatsUploadLoadTest, TestSingleRow) {
    std::vector<std::string> json_strings = {
        R"({"only_key": 42, "name": "single"})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    EXPECT_EQ(load_index_->Count(), 1);
    EXPECT_EQ(load_index_->Size(), 1);
}

// Test with all null values in JSON
TEST_F(JsonKeyStatsUploadLoadTest, TestNullValues) {
    std::vector<std::string> json_strings = {R"({"a": null, "b": 1})",
                                             R"({"a": null, "b": 2})",
                                             R"({"a": null, "b": 3})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/b");
}

// Test with empty objects as values
TEST_F(JsonKeyStatsUploadLoadTest, TestEmptyObjectValues) {
    std::vector<std::string> json_strings = {R"({"meta": {}, "id": 1})",
                                             R"({"meta": {}, "id": 2})",
                                             R"({"meta": {}, "id": 3})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/id");
}

// Test with empty arrays as values
TEST_F(JsonKeyStatsUploadLoadTest, TestEmptyArrayValues) {
    std::vector<std::string> json_strings = {R"({"tags": [], "id": 1})",
                                             R"({"tags": [], "id": 2})",
                                             R"({"tags": [], "id": 3})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/id");
}

// Test with deeply nested JSON (5 levels)
TEST_F(JsonKeyStatsUploadLoadTest, TestDeeplyNestedJson) {
    std::vector<std::string> json_strings = {
        R"({"l1": {"l2": {"l3": {"l4": {"l5": 1}}}}})",
        R"({"l1": {"l2": {"l3": {"l4": {"l5": 2}}}}})",
        R"({"l1": {"l2": {"l3": {"l4": {"l5": 3}}}}})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/l1/l2/l3/l4/l5");
    VerifyJsonType("/l1/l2/l3/l4/l5_INT64", JSONType::INT64);
}

// Test with heterogeneous schemas across rows (some keys missing in some rows)
TEST_F(JsonKeyStatsUploadLoadTest, TestHeterogeneousSchemas) {
    std::vector<std::string> json_strings;
    // 10 rows: all have "common", only first 3 have "rare_a", only last 3 have "rare_b"
    for (int i = 0; i < 10; i++) {
        if (i < 3) {
            json_strings.push_back(
                fmt::format(R"({{"common": {}, "rare_a": {}}})", i, i * 10));
        } else if (i >= 7) {
            json_strings.push_back(
                fmt::format(R"({{"common": {}, "rare_b": {}}})", i, i * 10));
        } else {
            json_strings.push_back(fmt::format(R"({{"common": {}}})", i));
        }
    }

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    EXPECT_EQ(load_index_->Count(), 10);
    // "common" appears in all rows -> shredding
    VerifyPathInShredding("/common");
    // "rare_a" and "rare_b" appear in only 3/10 rows -> shared
    VerifyPathInShared("/rare_a");
    VerifyPathInShared("/rare_b");
}

// Test mixed type: same key with different types across rows
// e.g., "val" is int in some rows, string in others
TEST_F(JsonKeyStatsUploadLoadTest, TestMixedTypeSameKey) {
    std::vector<std::string> json_strings;
    for (int i = 0; i < 10; i++) {
        if (i % 2 == 0) {
            json_strings.push_back(fmt::format(R"({{"val": {}}})", i));  // int
        } else {
            json_strings.push_back(
                fmt::format(R"({{"val": "str_{}"}})", i));  // string
        }
    }

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    EXPECT_EQ(load_index_->Count(), 10);
    // Both typed variants should exist
    auto int_fields = load_index_->GetShreddingFields(
        "/val", std::vector<JSONType>{JSONType::INT64});
    auto str_fields = load_index_->GetShreddingFields(
        "/val", std::vector<JSONType>{JSONType::STRING});
    // At least one type should be present
    EXPECT_FALSE(int_fields.empty() && str_fields.empty());
}

// Test with very long string values
TEST_F(JsonKeyStatsUploadLoadTest, TestLongStringValues) {
    std::string long_str(4096, 'x');
    std::vector<std::string> json_strings = {
        fmt::format(R"({{"data": "{}", "id": 1}})", long_str),
        fmt::format(R"({{"data": "{}", "id": 2}})", long_str),
        fmt::format(R"({{"data": "{}", "id": 3}})", long_str)};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/data");
    VerifyPathInShredding("/id");
}

// Test with many keys per row (wide JSON)
TEST_F(JsonKeyStatsUploadLoadTest, TestWideJson) {
    std::vector<std::string> json_strings;
    for (int row = 0; row < 5; row++) {
        std::string json = "{";
        for (int k = 0; k < 50; k++) {
            if (k > 0)
                json += ",";
            json += fmt::format(R"("k{}": {})", k, row * 50 + k);
        }
        json += "}";
        json_strings.push_back(json);
    }

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    EXPECT_EQ(load_index_->Count(), 5);
    // Spot-check a few keys
    VerifyPathInShredding("/k0");
    VerifyPathInShredding("/k25");
    VerifyPathInShredding("/k49");
}

// Test numeric edge cases: large int64, negative numbers, zero, float precision
TEST_F(JsonKeyStatsUploadLoadTest, TestNumericEdgeCases) {
    std::vector<std::string> json_strings = {
        R"({"big": 9223372036854775806, "neg": -999999, "zero": 0, "float": 1.7976931348623157e+308})",
        R"({"big": 9223372036854775807, "neg": -1, "zero": 0, "float": 2.2250738585072014e-308})",
        R"({"big": 1, "neg": -9223372036854775807, "zero": 0, "float": 0.0})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/big");
    VerifyPathInShredding("/neg");
    VerifyPathInShredding("/zero");
    VerifyPathInShredding("/float");
    VerifyJsonType("/big_INT64", JSONType::INT64);
    VerifyJsonType("/neg_INT64", JSONType::INT64);
    VerifyJsonType("/float_DOUBLE", JSONType::DOUBLE);
}

// Test boolean edge cases
TEST_F(JsonKeyStatsUploadLoadTest, TestBooleanValues) {
    std::vector<std::string> json_strings = {R"({"flag": true, "id": 1})",
                                             R"({"flag": false, "id": 2})",
                                             R"({"flag": true, "id": 3})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/flag");
    VerifyJsonType("/flag_BOOL", JSONType::BOOL);
}

// Test nested arrays with different element types
TEST_F(JsonKeyStatsUploadLoadTest, TestComplexArrayValues) {
    std::vector<std::string> json_strings = {
        R"({"arr_int": [1, 2, 3], "arr_str": ["a", "b"], "arr_mixed": [1, "x", true], "id": 1})",
        R"({"arr_int": [4, 5], "arr_str": ["c"], "arr_mixed": [2, "y", false], "id": 2})",
        R"({"arr_int": [6], "arr_str": ["d", "e", "f"], "arr_mixed": [3, "z", null], "id": 3})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/id");
}

// ==================== lack_binlog_rows Corner Cases ====================

// Test lack_binlog_rows larger than actual data rows
TEST_F(JsonKeyStatsUploadLoadTest, TestLackBinlogRowsLargerThanData) {
    const int64_t kLackRows = 20;
    const int kValidRows = 3;
    const int kTotalRows = kLackRows + kValidRows;

    std::vector<std::string> json_strings = {R"({"a": 1, "b": "x"})",
                                             R"({"a": 2, "b": "y"})",
                                             R"({"a": 3, "b": "z"})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload(kLackRows);
    Load();

    EXPECT_EQ(load_index_->Count(), kTotalRows);
}

// Test lack_binlog_rows = 1 (minimum non-zero)
TEST_F(JsonKeyStatsUploadLoadTest, TestLackBinlogRowsSingle) {
    const int64_t kLackRows = 1;
    const int kValidRows = 5;
    const int kTotalRows = kLackRows + kValidRows;

    std::vector<std::string> json_strings;
    for (int i = 0; i < kValidRows; i++) {
        json_strings.push_back(fmt::format(R"({{"val": {}}})", i));
    }

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload(kLackRows);
    Load();

    EXPECT_EQ(load_index_->Count(), kTotalRows);
    VerifyPathInShredding("/val");
}

// Test lack_binlog_rows with all rows going to shared (low hit ratio)
TEST_F(JsonKeyStatsUploadLoadTest, TestLackBinlogRowsAllShared) {
    const int64_t kLackRows = 50;
    const int kValidRows = 3;
    const int kTotalRows = kLackRows + kValidRows;

    // Each row has a unique key -> all will be shared (very low hit ratio)
    std::vector<std::string> json_strings = {
        R"({"unique_a": 1, "common": 10})",
        R"({"unique_b": 2, "common": 20})",
        R"({"unique_c": 3, "common": 30})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload(kLackRows);
    Load();

    EXPECT_EQ(load_index_->Count(), kTotalRows);
    // With 50 lack rows + 3 valid rows, "common" has 3/53 hit ratio < 0.3 -> shared
    VerifyPathInShared("/common");
    VerifyPathInShared("/unique_a");
}

// Test lack_binlog_rows with shared data row_id correctness for multiple keys
TEST_F(JsonKeyStatsUploadLoadTest, TestLackBinlogRowsMultipleSharedKeys) {
    const int64_t kLackRows = 5;
    const int kValidRows = 10;
    const int kTotalRows = kLackRows + kValidRows;

    std::vector<std::string> json_strings;
    for (int i = 0; i < kValidRows; i++) {
        json_strings.push_back(
            fmt::format(R"({{"always": {}, "id": {}}})", i, i));
    }

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload(kLackRows);
    Load();

    EXPECT_EQ(load_index_->Count(), kTotalRows);

    // Verify shredding fields exist for high-hit-ratio key
    auto always_fields = load_index_->GetShreddingFields("/always");
    EXPECT_FALSE(always_fields.empty());

    // Verify the first kLackRows rows have no data via ExecuteForSharedData
    // (they are empty/null rows from lack_binlog_rows)
    TargetBitmap always_exists(kTotalRows);
    PinWrapper<BsonInvertedIndex*> bson_index{nullptr};
    load_index_->ExecuteForSharedData(
        nullptr,
        bson_index,
        "/always",
        [&](BsonView bson, uint32_t row_id, uint32_t offset) {
            always_exists[row_id] = true;
        });
    // The lack rows should NOT have "/always" in shared data
    for (int i = 0; i < kLackRows; i++) {
        EXPECT_FALSE(always_exists[i])
            << "lack_binlog row " << i
            << " should not have '/always' in shared";
    }
}
