// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <vector>
#include <map>

#include "common/Json.h"
#include "common/protobuf_utils.h"
#include "index/json_stats/JsonKeyStats.h"
#include "storage/ChunkManager.h"
#include "storage/Types.h"
#include "storage/FileManager.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"

using milvus::index::JsonKey;
using milvus::index::JsonKeyStats;
using milvus::index::JsonStatsValue;
using milvus::index::JSONType;

// Friend accessor declared in JsonKeyStats to invoke private method for UT
class TraverseJsonForBuildStatsAccessor {
 public:
    static void
    Call(JsonKeyStats& s,
         const char* json,
         std::vector<std::string>& path,
         std::map<JsonKey, JsonStatsValue>& values) {
        milvus::Json parsed(simdjson::padded_string(json, std::strlen(json)));
        auto document = parsed.doc();
        ASSERT_EQ(document.error(), simdjson::SUCCESS);
        auto root = document.get_value();
        ASSERT_EQ(root.error(), simdjson::SUCCESS);
        s.TraverseJsonForBuildStats(root.value(), path, values);
    }
};

// Friend accessor declared in JsonKeyStats to invoke private method for UT
class CollectSingleJsonStatsInfoAccessor {
 public:
    static void
    Call(JsonKeyStats& s,
         const char* json,
         std::map<JsonKey, milvus::index::KeyStatsInfo>& infos) {
        milvus::Json parsed(simdjson::padded_string(json, std::strlen(json)));
        s.CollectSingleJsonStatsInfo(parsed, infos);
    }
};

TEST(TraverseJsonForBuildStatsTest,
     HandlesPrimitivesArraysNestedAndEmptyObject) {
    const char* json = R"(
        {"id": 34495370646 ,"type":"PublicEvent","actor":{"id":126890008,"login":"gegangene","display_login":"gegangene","gravatar_id":"",
        "url":"https:\/\/api.github.com\/users\/gegangene","avatar_url":"https:\/\/avatars.githubusercontent.com\/u\/126890008?"},
        "repo":{"id":737601171,"name":"gegangene\/scheduler","url":"https:\/\/api.github.com\/repos\/gegangene\/scheduler"},
        "payload":{},"public":true,"created_at":"2024-01-01T00:01:28Z",
        "msg":"line1\nline2\t\u4e2d\u6587 \/ backslash \\"}
    )";

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

    std::vector<std::string> path;
    std::map<JsonKey, JsonStatsValue> values;
    TraverseJsonForBuildStatsAccessor::Call(stats, json, path, values);

    // Expect collected key-value/type pairs
    auto expect_has = [&](const std::string& key,
                          JSONType type,
                          const std::string& value_substr) {
        JsonKey k{key, type};
        auto it = values.find(k);
        ASSERT_NE(it, values.end()) << "Missing key: " << key;
        EXPECT_EQ(it->second.raw_value, value_substr);
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

// Regression for https://github.com/milvus-io/milvus/issues/52806
// Subnormal doubles (e.g. -1.48e-309) used to be misclassified as UNKNOWN by
// the std::stof/std::stod based sniffing and aborted the whole stats build.
TEST(TraverseJsonForBuildStatsTest, MatchesSimdjsonNumberSemantics) {
    const char* json = R"({"sub": -1.4829972460841e-309, "underflow": 1e-400,)"
                       R"( "uint64": 18446744073709551615, "normal": 1.5})";

    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 100, {}};
    milvus::storage::IndexMeta index_meta{3, 100, 1, 1};
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto cm = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    JsonKeyStats stats(ctx, true);

    std::vector<std::string> path;
    std::map<JsonKey, JsonStatsValue> values;
    TraverseJsonForBuildStatsAccessor::Call(stats, json, path, values);

    EXPECT_EQ(values.at(JsonKey{"/sub", JSONType::DOUBLE}).raw_value,
              "-1.4829972460841e-309");
    EXPECT_DOUBLE_EQ(
        values.at(JsonKey{"/sub", JSONType::DOUBLE}).double_value.value(),
        -1.4829972460841e-309);
    EXPECT_DOUBLE_EQ(
        values.at(JsonKey{"/underflow", JSONType::DOUBLE}).double_value.value(),
        0.0);
    EXPECT_DOUBLE_EQ(
        values.at(JsonKey{"/uint64", JSONType::DOUBLE}).double_value.value(),
        18446744073709551615.0);
    EXPECT_DOUBLE_EQ(
        values.at(JsonKey{"/normal", JSONType::DOUBLE}).double_value.value(),
        1.5);
}

TEST(TraverseJsonForBuildStatsTest, InvalidNumberPreservesPathAndSibling) {
    const char* json =
        R"({"overflow":1e400,"bigint":18446744073709551616,"ok":7})";

    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 100, {}};
    milvus::storage::IndexMeta index_meta{3, 100, 1, 1};
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto cm = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    JsonKeyStats stats(ctx, true);

    std::vector<std::string> path;
    std::map<JsonKey, JsonStatsValue> values;
    TraverseJsonForBuildStatsAccessor::Call(stats, json, path, values);

    EXPECT_TRUE(
        values.at(JsonKey{"/overflow", JSONType::DOUBLE}).IsInvalidNumber());
    EXPECT_TRUE(
        values.at(JsonKey{"/bigint", JSONType::DOUBLE}).IsInvalidNumber());
    EXPECT_EQ(values.at(JsonKey{"/ok", JSONType::INT64}).raw_value, "7");
}

TEST(CollectSingleJsonStatsInfoTest, ClassifiesSubnormalNumbersAsDouble) {
    const char* json =
        R"({"sub": -1.4829972460841e-309, "tiny": -2.32430876e-316})";

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

    ASSERT_NE(infos.find(JsonKey{"/sub", JSONType::DOUBLE}), infos.end());
    ASSERT_NE(infos.find(JsonKey{"/tiny", JSONType::DOUBLE}), infos.end());
}
