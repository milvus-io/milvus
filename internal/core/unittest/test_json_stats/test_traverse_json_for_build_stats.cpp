// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/jsmn.h"
#include "common/protobuf_utils.h"
#include "futures/Executor.h"
#include "futures/future_c.h"
#include "gtest/gtest.h"
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/utils.h"
#include "pb/schema.pb.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"

using milvus::index::JsonKey;
using milvus::index::JsonKeyStats;
using milvus::index::JSONType;

// Friend accessor declared in JsonKeyStats to invoke private method for UT
class TraverseJsonForBuildStatsAccessor {
 public:
    static void
    Call(JsonKeyStats& s,
         const char* json,
         jsmntok_t* tokens,
         int& index,
         std::vector<std::string>& path,
         std::map<JsonKey, std::string>& values) {
        s.TraverseJsonForBuildStats(json, tokens, index, path, values);
    }
};

// Friend accessor declared in JsonKeyStats to invoke private method for UT
class CollectSingleJsonStatsInfoAccessor {
 public:
    static void
    Call(JsonKeyStats& s,
         std::string_view json,
         std::map<JsonKey, milvus::index::KeyStatsInfo>& infos) {
        s.CollectSingleJsonStatsInfo(json, infos);
    }
};

// Friend accessor declared in JsonKeyStats to invoke private method for UT
class CollectKeyInfoAccessor {
 public:
    static std::map<JsonKey, milvus::index::KeyStatsInfo>
    CallLegacy(JsonKeyStats& stats,
               const std::vector<milvus::FieldDataPtr>& field_datas,
               bool nullable) {
        return stats.CollectKeyInfo(field_datas, nullable);
    }

    static std::map<JsonKey, milvus::index::KeyStatsInfo>
    CallParallel(
        JsonKeyStats& stats,
        const std::vector<milvus::index::JsonStatsRowRange>& row_ranges,
        bool nullable) {
        return stats.CollectKeyInfo(row_ranges, nullable);
    }
};

namespace {

// Helper to tokenize JSON using jsmn
static std::vector<jsmntok_t>
Tokenize(const char* json) {
    jsmn_parser parser;
    jsmn_init(&parser);
    int token_capacity = 32;
    std::vector<jsmntok_t> tokens(token_capacity);
    while (true) {
        int r = jsmn_parse(
            &parser, json, strlen(json), tokens.data(), token_capacity);
        if (r == JSMN_ERROR_NOMEM) {
            token_capacity *= 2;
            tokens.resize(token_capacity);
            continue;
        }
        EXPECT_GE(r, 0) << "Failed to parse JSON with jsmn";
        tokens.resize(r);
        break;
    }
    return tokens;
}

milvus::FieldDataPtr
CreateJsonFieldData(int64_t row_count) {
    auto field_data = milvus::storage::CreateFieldData(
        milvus::DataType::JSON, milvus::DataType::NONE, false);
    if (row_count == 0) {
        return field_data;
    }

    std::vector<milvus::Json> rows;
    rows.reserve(row_count);
    const std::string json = R"({"value":1})";
    for (int64_t i = 0; i < row_count; ++i) {
        rows.emplace_back(simdjson::padded_string(json));
    }
    field_data->FillFieldData(rows.data(), rows.size());
    return field_data;
}

milvus::FieldDataPtr
CreateJsonFieldData(const std::vector<std::string>& json_rows) {
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
CreateNullableJsonFieldData(const std::vector<std::string>& json_rows,
                            const std::vector<bool>& valid_rows) {
    if (json_rows.size() != valid_rows.size()) {
        ADD_FAILURE() << "json rows and validity rows must have the same size";
        return nullptr;
    }

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

std::unique_ptr<JsonKeyStats>
CreateJsonKeyStatsForCollectTest() {
    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 100, {}};
    milvus::storage::IndexMeta index_meta{3, 100, 1, 1};
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto cm = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    return std::make_unique<JsonKeyStats>(ctx, true);
}

void
ExpectKeyInfoMapsEqual(
    const std::map<JsonKey, milvus::index::KeyStatsInfo>& expected,
    const std::map<JsonKey, milvus::index::KeyStatsInfo>& actual) {
    ASSERT_EQ(actual.size(), expected.size());
    for (const auto& [key, expected_info] : expected) {
        auto actual_it = actual.find(key);
        ASSERT_NE(actual_it, actual.end()) << "missing key " << key.ToString();
        EXPECT_EQ(actual_it->second.hit_row_num_, expected_info.hit_row_num_)
            << key.ToString();
        EXPECT_EQ(std::memcmp(actual_it->second.min_value_,
                              expected_info.min_value_,
                              sizeof(expected_info.min_value_)),
                  0)
            << key.ToString();
        EXPECT_EQ(std::memcmp(actual_it->second.max_value_,
                              expected_info.max_value_,
                              sizeof(expected_info.max_value_)),
                  0)
            << key.ToString();
    }
}

void
ExpectHitCount(const std::map<JsonKey, milvus::index::KeyStatsInfo>& infos,
               const JsonKey& key,
               int32_t expected_hit_count) {
    auto it = infos.find(key);
    ASSERT_NE(it, infos.end()) << "missing key " << key.ToString();
    EXPECT_EQ(it->second.hit_row_num_, expected_hit_count) << key.ToString();
}

std::map<JsonKey, milvus::index::KeyStatsInfo>
ExpectParallelCollectMatchesLegacy(
    const std::vector<milvus::FieldDataPtr>& field_datas,
    bool nullable,
    const std::vector<int64_t>& rows_per_range_values,
    const std::vector<int>& worker_counts) {
    int64_t expected_row_count = 0;
    for (const auto& data : field_datas) {
        expected_row_count += data->get_num_rows();
    }

    auto stats = CreateJsonKeyStatsForCollectTest();
    auto legacy_infos =
        CollectKeyInfoAccessor::CallLegacy(*stats, field_datas, nullable);
    EXPECT_EQ(stats->Count(), expected_row_count);

    for (auto rows_per_range : rows_per_range_values) {
        auto ranges = milvus::index::CreateJsonStatsRowRanges(field_datas,
                                                              rows_per_range);
        for (auto worker_count : worker_counts) {
            SCOPED_TRACE(::testing::Message()
                         << "rows_per_range=" << rows_per_range
                         << ", worker_count=" << worker_count
                         << ", nullable=" << nullable);
            executor_set_json_stats_build_thread_num(worker_count);
            auto parallel_infos =
                CollectKeyInfoAccessor::CallParallel(*stats, ranges, nullable);
            EXPECT_EQ(stats->Count(), expected_row_count);
            ExpectKeyInfoMapsEqual(legacy_infos, parallel_infos);
        }
    }
    return legacy_infos;
}

}  // namespace

class JsonStatsCollectKeyInfoTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        original_thread_num_ =
            milvus::futures::getJsonStatsBuildExecutor()->numThreads();
    }

    void
    TearDown() override {
        executor_set_json_stats_build_thread_num(
            static_cast<int>(original_thread_num_));
    }

    size_t original_thread_num_;
};

TEST(JsonStatsRowRangeTest, CoalescesAcrossFieldDataBoundaries) {
    auto first = CreateJsonFieldData(2);
    auto second = CreateJsonFieldData(5);
    auto third = CreateJsonFieldData(1);

    auto ranges =
        milvus::index::CreateJsonStatsRowRanges({first, second, third}, 3);

    ASSERT_EQ(ranges.size(), 3);

    EXPECT_EQ(ranges[0].sequence, 0);
    EXPECT_EQ(ranges[0].global_begin, 0);
    EXPECT_EQ(ranges[0].row_count, 3);
    ASSERT_EQ(ranges[0].slices.size(), 2);
    EXPECT_EQ(ranges[0].slices[0].data.get(), first.get());
    EXPECT_EQ(ranges[0].slices[0].local_begin, 0);
    EXPECT_EQ(ranges[0].slices[0].row_count, 2);
    EXPECT_EQ(ranges[0].slices[0].global_begin, 0);
    EXPECT_EQ(ranges[0].slices[1].data.get(), second.get());
    EXPECT_EQ(ranges[0].slices[1].local_begin, 0);
    EXPECT_EQ(ranges[0].slices[1].row_count, 1);
    EXPECT_EQ(ranges[0].slices[1].global_begin, 2);

    EXPECT_EQ(ranges[1].sequence, 1);
    EXPECT_EQ(ranges[1].global_begin, 3);
    EXPECT_EQ(ranges[1].row_count, 3);
    ASSERT_EQ(ranges[1].slices.size(), 1);
    EXPECT_EQ(ranges[1].slices[0].data.get(), second.get());
    EXPECT_EQ(ranges[1].slices[0].local_begin, 1);
    EXPECT_EQ(ranges[1].slices[0].row_count, 3);
    EXPECT_EQ(ranges[1].slices[0].global_begin, 3);

    EXPECT_EQ(ranges[2].sequence, 2);
    EXPECT_EQ(ranges[2].global_begin, 6);
    EXPECT_EQ(ranges[2].row_count, 2);
    ASSERT_EQ(ranges[2].slices.size(), 2);
    EXPECT_EQ(ranges[2].slices[0].data.get(), second.get());
    EXPECT_EQ(ranges[2].slices[0].local_begin, 4);
    EXPECT_EQ(ranges[2].slices[0].row_count, 1);
    EXPECT_EQ(ranges[2].slices[0].global_begin, 6);
    EXPECT_EQ(ranges[2].slices[1].data.get(), third.get());
    EXPECT_EQ(ranges[2].slices[1].local_begin, 0);
    EXPECT_EQ(ranges[2].slices[1].row_count, 1);
    EXPECT_EQ(ranges[2].slices[1].global_begin, 7);
}

TEST(JsonStatsRowRangeTest, MatchesLegacyTraversalForBoundarySizes) {
    std::vector<milvus::FieldDataPtr> field_datas = {
        CreateJsonFieldData(0),
        CreateJsonFieldData(2),
        CreateJsonFieldData(0),
        CreateJsonFieldData(5),
        CreateJsonFieldData(1),
        CreateJsonFieldData(0),
    };

    using RowPosition =
        std::tuple<const milvus::FieldDataBase*, int64_t, int64_t>;
    std::vector<RowPosition> legacy_rows;
    int64_t global_row = 0;
    for (const auto& data : field_datas) {
        for (int64_t local_row = 0; local_row < data->get_num_rows();
             ++local_row) {
            legacy_rows.emplace_back(data.get(), local_row, global_row++);
        }
    }

    for (int64_t max_rows_per_range : {1, 2, 3, 4, 8, 16}) {
        auto ranges = milvus::index::CreateJsonStatsRowRanges(
            field_datas, max_rows_per_range);
        std::vector<RowPosition> ranged_rows;
        int64_t expected_global_begin = 0;
        uint64_t expected_sequence = 0;

        for (const auto& range : ranges) {
            EXPECT_EQ(range.sequence, expected_sequence++);
            EXPECT_EQ(range.global_begin, expected_global_begin);
            EXPECT_GT(range.row_count, 0);
            EXPECT_LE(range.row_count, max_rows_per_range);

            int64_t rows_in_range = 0;
            for (const auto& slice : range.slices) {
                EXPECT_EQ(slice.global_begin,
                          range.global_begin + rows_in_range);
                for (int64_t offset = 0; offset < slice.row_count; ++offset) {
                    ranged_rows.emplace_back(slice.data.get(),
                                             slice.local_begin + offset,
                                             slice.global_begin + offset);
                }
                rows_in_range += slice.row_count;
            }
            EXPECT_EQ(rows_in_range, range.row_count);
            expected_global_begin += range.row_count;
        }

        EXPECT_EQ(ranged_rows, legacy_rows);
    }
}

TEST(JsonStatsRowRangeTest, HandlesEmptyInputAndRejectsZeroRangeSize) {
    EXPECT_TRUE(milvus::index::CreateJsonStatsRowRanges({}, 4).empty());
    EXPECT_TRUE(
        milvus::index::CreateJsonStatsRowRanges({CreateJsonFieldData(0)}, 4)
            .empty());
    EXPECT_ANY_THROW(
        milvus::index::CreateJsonStatsRowRanges({CreateJsonFieldData(1)}, 0));
}

TEST_F(JsonStatsCollectKeyInfoTest,
       ParallelMatchesLegacyAcrossFieldDataBoundaries) {
    auto first = CreateJsonFieldData(std::vector<std::string>{
        R"({"a":1,"b":"x","nested":{"flag":true}})",
        R"({"a":2,"b":"y"})",
        R"({"a":"mixed","items":[1,2]})",
    });
    auto second = CreateJsonFieldData(std::vector<std::string>{
        R"({})",
        R"({"nested":{"flag":false},"null_value":null})",
        R"({"a":3,"b":"z"})",
        R"([])",
        "",
    });
    std::vector<milvus::FieldDataPtr> field_datas = {first, second};
    auto ranges = milvus::index::CreateJsonStatsRowRanges(field_datas, 2);
    ASSERT_EQ(ranges.size(), 4);
    ASSERT_EQ(ranges[1].slices.size(), 2);

    auto stats = CreateJsonKeyStatsForCollectTest();
    auto legacy_infos =
        CollectKeyInfoAccessor::CallLegacy(*stats, field_datas, false);
    EXPECT_EQ(stats->Count(), 8);

    executor_set_json_stats_build_thread_num(1);
    auto single_worker_infos =
        CollectKeyInfoAccessor::CallParallel(*stats, ranges, false);
    EXPECT_EQ(stats->Count(), 8);
    ExpectKeyInfoMapsEqual(legacy_infos, single_worker_infos);

    executor_set_json_stats_build_thread_num(4);
    auto parallel_infos =
        CollectKeyInfoAccessor::CallParallel(*stats, ranges, false);
    EXPECT_EQ(stats->Count(), 8);
    ExpectKeyInfoMapsEqual(legacy_infos, parallel_infos);

    ExpectHitCount(parallel_infos, JsonKey{"/a", JSONType::INT64}, 3);
    ExpectHitCount(parallel_infos, JsonKey{"/a", JSONType::STRING}, 1);
    ExpectHitCount(parallel_infos, JsonKey{"/nested", JSONType::OBJECT}, 2);
    ExpectHitCount(parallel_infos, JsonKey{"/nested/flag", JSONType::BOOL}, 2);
    ExpectHitCount(parallel_infos, JsonKey{"/null_value", JSONType::NONE}, 1);
    ExpectHitCount(parallel_infos, JsonKey{"", JSONType::ARRAY}, 1);
}

TEST_F(JsonStatsCollectKeyInfoTest,
       ParallelPreservesNonNulTerminatedJsonViewLength) {
    const std::string json = R"({"a":1,"b":"x"})";
    std::string buffer = json;
    buffer.append(simdjson::SIMDJSON_PADDING, '\xAB');

    auto field_data = std::dynamic_pointer_cast<milvus::FieldDataJsonImpl>(
        milvus::storage::CreateFieldData(
            milvus::DataType::JSON, milvus::DataType::NONE, false));
    ASSERT_NE(field_data, nullptr);
    field_data->add_json_data({milvus::Json(buffer.data(), json.size())});

    auto ranges = milvus::index::CreateJsonStatsRowRanges({field_data}, 1);
    auto stats = CreateJsonKeyStatsForCollectTest();
    executor_set_json_stats_build_thread_num(2);
    auto infos = CollectKeyInfoAccessor::CallParallel(*stats, ranges, false);

    EXPECT_EQ(stats->Count(), 1);
    ExpectHitCount(infos, JsonKey{"/a", JSONType::INT64}, 1);
    ExpectHitCount(infos, JsonKey{"/b", JSONType::STRING}, 1);
}

TEST_F(JsonStatsCollectKeyInfoTest, ParallelMatchesLegacyForEmptyInputs) {
    auto empty_infos =
        ExpectParallelCollectMatchesLegacy({}, false, {1, 16 * 1024}, {1, 4});
    EXPECT_TRUE(empty_infos.empty());

    std::vector<milvus::FieldDataPtr> empty_field_datas = {
        CreateJsonFieldData(0),
        CreateJsonFieldData(0),
        CreateJsonFieldData(0),
    };
    auto empty_field_data_infos = ExpectParallelCollectMatchesLegacy(
        empty_field_datas, false, {1, 2, 16 * 1024}, {1, 4});
    EXPECT_TRUE(empty_field_data_infos.empty());
}

TEST_F(JsonStatsCollectKeyInfoTest,
       ParallelMatchesLegacyForAllJsonTypesAndDeepPaths) {
    auto first = CreateJsonFieldData(std::vector<std::string>{
        "42",
        "-42",
        "0",
        "1.5",
        "6.02e23",
        "true",
        "false",
        "null",
        "\"root string\"",
        "[]",
        R"([1,{"array_child":"is_not_traversed"}])",
        "{}",
    });
    auto second = CreateJsonFieldData(std::vector<std::string>{
        R"({"l1":{"l2":{"l3":{"int":1,"double":2.5,"bool":true,"null":null,"string":"x","array":[1,{"ignored":2}]}}},"a/b":{"~key":"escaped"}})",
        R"({"same":1})",
        R"({"same":"1"})",
        R"({"same":false})",
        R"({"same":null})",
        "",
    });
    std::vector<milvus::FieldDataPtr> field_datas = {
        first,
        CreateJsonFieldData(0),
        second,
    };

    auto infos = ExpectParallelCollectMatchesLegacy(
        field_datas, false, {1, 2, 5, 16, 64}, {1, 2, 4});

    ExpectHitCount(infos, JsonKey{"", JSONType::INT64}, 3);
    ExpectHitCount(infos, JsonKey{"", JSONType::DOUBLE}, 2);
    ExpectHitCount(infos, JsonKey{"", JSONType::BOOL}, 2);
    ExpectHitCount(infos, JsonKey{"", JSONType::NONE}, 1);
    ExpectHitCount(infos, JsonKey{"", JSONType::STRING}, 1);
    ExpectHitCount(infos, JsonKey{"", JSONType::ARRAY}, 2);
    ExpectHitCount(infos, JsonKey{"/l1/l2/l3/int", JSONType::INT64}, 1);
    ExpectHitCount(infos, JsonKey{"/a~1b/~0key", JSONType::STRING}, 1);
    ExpectHitCount(infos, JsonKey{"/same", JSONType::INT64}, 1);
    ExpectHitCount(infos, JsonKey{"/same", JSONType::STRING}, 1);
    ExpectHitCount(infos, JsonKey{"/same", JSONType::BOOL}, 1);
    ExpectHitCount(infos, JsonKey{"/same", JSONType::NONE}, 1);
}

TEST_F(JsonStatsCollectKeyInfoTest, ParallelMatchesLegacyForNullableRows) {
    auto first = CreateNullableJsonFieldData(
        {
            R"({"kept":1})",
            R"({"broken":)",
            R"({"kept":2,"nested":{"flag":true}})",
            "not-json",
        },
        {true, false, true, false});
    auto second = CreateNullableJsonFieldData(
        {
            R"({"ignored":1})",
            R"({"kept":"two"})",
            R"({"nested":{"flag":false}})",
        },
        {false, true, true});
    auto non_nullable =
        CreateJsonFieldData(std::vector<std::string>{R"({"always":true})"});
    std::vector<milvus::FieldDataPtr> field_datas = {
        first,
        CreateNullableJsonFieldData({}, {}),
        second,
        non_nullable,
    };

    auto infos_with_field_nullable = ExpectParallelCollectMatchesLegacy(
        field_datas, false, {1, 2, 3, 8}, {1, 4});
    auto infos_with_explicit_nullable = ExpectParallelCollectMatchesLegacy(
        field_datas, true, {1, 2, 3, 8}, {1, 4});
    ExpectKeyInfoMapsEqual(infos_with_field_nullable,
                           infos_with_explicit_nullable);

    ExpectHitCount(
        infos_with_field_nullable, JsonKey{"/kept", JSONType::INT64}, 2);
    ExpectHitCount(
        infos_with_field_nullable, JsonKey{"/kept", JSONType::STRING}, 1);
    ExpectHitCount(
        infos_with_field_nullable, JsonKey{"/nested/flag", JSONType::BOOL}, 2);
    ExpectHitCount(
        infos_with_field_nullable, JsonKey{"/always", JSONType::BOOL}, 1);
    EXPECT_EQ(
        infos_with_field_nullable.count(JsonKey{"/ignored", JSONType::INT64}),
        0);
}

TEST_F(JsonStatsCollectKeyInfoTest, ParallelMatchesLegacyAcrossManyRangeSizes) {
    std::vector<milvus::FieldDataPtr> field_datas = {
        CreateJsonFieldData(0),
        CreateJsonFieldData(std::vector<std::string>{R"({"value":1})"}),
        CreateJsonFieldData(0),
        CreateJsonFieldData(std::vector<std::string>{
            R"({"value":2})",
            R"({"value":3})",
            R"({"value":"three"})",
            R"({"nested":{"value":4}})",
            R"({"array":[1,2,3]})",
            R"({"flag":true})",
            R"({"null_value":null})",
        }),
        CreateJsonFieldData(0),
        CreateJsonFieldData(std::vector<std::string>{
            R"({"value":5})",
            R"({"value":6})",
            R"({"value":7})",
        }),
    };

    auto infos = ExpectParallelCollectMatchesLegacy(
        field_datas, false, {1, 2, 3, 4, 5, 8, 11, 32}, {1, 2, 4});
    ExpectHitCount(infos, JsonKey{"/value", JSONType::INT64}, 6);
    ExpectHitCount(infos, JsonKey{"/value", JSONType::STRING}, 1);
}

TEST_F(JsonStatsCollectKeyInfoTest,
       ParallelMatchesLegacyForLargeMultiRangeInput) {
    constexpr int64_t kRowsPerRange = 16 * 1024;
    std::vector<milvus::FieldDataPtr> field_datas = {
        CreateJsonFieldData(kRowsPerRange - 1),
        CreateJsonFieldData(2),
        CreateJsonFieldData(kRowsPerRange),
    };

    auto infos = ExpectParallelCollectMatchesLegacy(
        field_datas, false, {kRowsPerRange}, {1, 4});
    ExpectHitCount(
        infos, JsonKey{"/value", JSONType::INT64}, 2 * kRowsPerRange + 1);
}

TEST_F(JsonStatsCollectKeyInfoTest, LegacyAndParallelRethrowSameParseError) {
    auto invalid =
        CreateJsonFieldData(std::vector<std::string>{R"({"broken":)"});
    auto valid = CreateJsonFieldData(
        std::vector<std::string>{R"({"value":1})", R"({"value":2})"});
    std::vector<milvus::FieldDataPtr> field_datas = {invalid, valid};
    auto ranges = milvus::index::CreateJsonStatsRowRanges(field_datas, 1);

    milvus::ErrorCode legacy_error_code = milvus::UnexpectedError;
    std::string legacy_error_message;
    auto legacy_stats = CreateJsonKeyStatsForCollectTest();
    try {
        CollectKeyInfoAccessor::CallLegacy(*legacy_stats, field_datas, false);
        FAIL() << "expected invalid JSON to fail in legacy collect";
    } catch (const milvus::SegcoreError& error) {
        legacy_error_code = error.get_error_code();
        legacy_error_message = error.what();
        EXPECT_EQ(legacy_error_code, milvus::UnexpectedError);
        EXPECT_NE(legacy_error_message.find("Failed to parse Json"),
                  std::string::npos);
    }

    for (int worker_count : {1, 4}) {
        SCOPED_TRACE(::testing::Message() << "worker_count=" << worker_count);
        executor_set_json_stats_build_thread_num(worker_count);
        auto parallel_stats = CreateJsonKeyStatsForCollectTest();
        try {
            CollectKeyInfoAccessor::CallParallel(
                *parallel_stats, ranges, false);
            FAIL() << "expected invalid JSON to fail in parallel collect";
        } catch (const milvus::SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), legacy_error_code);
            EXPECT_EQ(std::string(error.what()), legacy_error_message);
        }
        EXPECT_EQ(
            milvus::futures::getJsonStatsBuildExecutor()->getTaskQueueSize(),
            0);
    }
}

TEST(TraverseJsonForBuildStatsTest,
     HandlesPrimitivesArraysNestedAndEmptyObject) {
    const char* json = R"(
        {"id": 34495370646 ,"type":"PublicEvent","actor":{"id":126890008,"login":"gegangene","display_login":"gegangene","gravatar_id":"",
        "url":"https:\/\/api.github.com\/users\/gegangene","avatar_url":"https:\/\/avatars.githubusercontent.com\/u\/126890008?"},
        "repo":{"id":737601171,"name":"gegangene\/scheduler","url":"https:\/\/api.github.com\/repos\/gegangene\/scheduler"},
        "payload":{},"public":true,"created_at":"2024-01-01T00:01:28Z",
        "msg":"line1\nline2\t\u4e2d\u6587 \/ backslash \\"}
    )";

    auto tokens = Tokenize(json);

    // We only need an instance to access the private method we exposed.
    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 100, {}};
    milvus::storage::IndexMeta index_meta{3, 100, 1, 1};
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto cm = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    JsonKeyStats stats(ctx, true);

    int index = 0;
    std::vector<std::string> path;
    std::map<JsonKey, std::string> values;
    TraverseJsonForBuildStatsAccessor::Call(
        stats, json, tokens.data(), index, path, values);

    // Expect collected key-value/type pairs
    auto expect_has = [&](const std::string& key,
                          JSONType type,
                          const std::string& value_substr) {
        JsonKey k{key, type};
        auto it = values.find(k);
        ASSERT_NE(it, values.end()) << "Missing key: " << key;
        EXPECT_EQ(it->second, value_substr);
    };

    expect_has("/id", JSONType::INT64, "34495370646");
    expect_has("/type", JSONType::STRING, "PublicEvent");
    expect_has("/actor/id", JSONType::INT64, "126890008");
    expect_has("/payload", JSONType::OBJECT, "{}");
    expect_has("/public", JSONType::BOOL, "true");
    expect_has("/created_at", JSONType::STRING, "2024-01-01T00:01:28Z");
    expect_has("/repo/url",
               JSONType::STRING,
               "https://api.github.com/repos/gegangene/scheduler");
    expect_has("/msg", JSONType::STRING, "line1\nline2\t中文 / backslash \\");
}

TEST(CollectSingleJsonStatsInfoTest, EmptyJsonStringThrows) {
    const char* json = "";

    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 100, {}};
    milvus::storage::IndexMeta index_meta{3, 100, 1, 1};
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto cm = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    JsonKeyStats stats(ctx, true);

    std::map<JsonKey, milvus::index::KeyStatsInfo> infos;
    EXPECT_NO_THROW(
        { CollectSingleJsonStatsInfoAccessor::Call(stats, json, infos); });
}

TEST(CollectSingleJsonStatsInfoTest, ParsesJsonWithoutNulTerminator) {
    // The JSON views stored in field data are string_views that are not
    // guaranteed to be NUL-terminated. The parse must rely on the view
    // length instead of strlen, otherwise trailing bytes leak into jsmn.
    std::string json = R"({"a": 1, "b": "x"})";
    std::string buffer = json;
    buffer.append(64, '\xAB');

    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 100, {}};
    milvus::storage::IndexMeta index_meta{3, 100, 1, 1};
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto cm = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    JsonKeyStats stats(ctx, true);

    std::map<JsonKey, milvus::index::KeyStatsInfo> infos;
    EXPECT_NO_THROW(CollectSingleJsonStatsInfoAccessor::Call(
        stats, std::string_view(buffer.data(), json.size()), infos));
    EXPECT_NE(infos.find(JsonKey{"/a", JSONType::INT64}), infos.end());
    EXPECT_NE(infos.find(JsonKey{"/b", JSONType::STRING}), infos.end());
}

namespace {

// Overwrite the stack and recycle same-sized heap allocations so that a
// Json view danging into a destroyed local shows up as corrupted content.
__attribute__((noinline)) void
ClobberStackAndHeap(const std::string& value) {
    volatile uint8_t stack_buf[4096];
    for (size_t i = 0; i < sizeof(stack_buf); i++) {
        stack_buf[i] = 0xAB;
    }
    for (int i = 0; i < 64; i++) {
        std::string chunk(value.size(), '\xAB');
        (void)chunk;
    }
}

}  // namespace

TEST(JsonFieldDataTest, DefaultValueRowsSharePreinitializedBuffer) {
    // Regression test for #52843: the default value used to be stored as a
    // non-owning Json view onto a local std::string that dies with
    // FillFieldData, leaving every filled row dangling.
    std::string default_json =
        R"({"default_key": 12345, "another_key": "value"})";
    milvus::proto::schema::ValueField default_value;
    default_value.set_bytes_data(default_json);

    auto field_data = milvus::storage::CreateFieldDataFromDefaultValue(
        milvus::DataType::JSON,
        true,
        5,
        milvus::DefaultValueType{default_value});

    ClobberStackAndHeap(default_json);

    const char* shared_buffer = nullptr;
    for (int i = 0; i < 5; i++) {
        auto* json = static_cast<const milvus::Json*>(field_data->RawValue(i));
        EXPECT_EQ(std::string_view(json->data()),
                  std::string_view(default_json));
        if (i == 0) {
            shared_buffer = json->data().data();
        } else {
            EXPECT_EQ(json->data().data(), shared_buffer);
        }
    }
}

TEST(FieldDataDefaultConstructionTest, InitializesScalarAndNullRows) {
    milvus::proto::schema::ValueField default_value;
    default_value.set_int_data(42);
    auto default_rows = milvus::storage::CreateFieldDataFromDefaultValue(
        milvus::DataType::INT32,
        true,
        3,
        milvus::DefaultValueType{default_value});

    ASSERT_EQ(default_rows->Length(), 3);
    ASSERT_EQ(default_rows->get_null_count(), 0);
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(default_rows->is_valid(i));
        EXPECT_EQ(*static_cast<const int32_t*>(default_rows->RawValue(i)), 42);
    }

    auto null_rows = milvus::storage::CreateFieldDataFromDefaultValue(
        milvus::DataType::INT32, true, 3, std::nullopt);
    ASSERT_EQ(null_rows->Length(), 3);
    ASSERT_EQ(null_rows->get_null_count(), 3);
    for (int i = 0; i < 3; ++i) {
        EXPECT_FALSE(null_rows->is_valid(i));
    }
}
