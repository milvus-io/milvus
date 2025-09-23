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

#include "index/json_stats/JsonKeyStats.h"
#include "common/jsmn.h"
#include "storage/ChunkManager.h"
#include "storage/Types.h"
#include "storage/FileManager.h"
#include "storage/Util.h"

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

static std::string
Substr(const char* json, const jsmntok_t& tok) {
    return std::string(json + tok.start, tok.end - tok.start);
}

}  // namespace

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
    storage_config.root_path = "/tmp/test-traverse-json-build-stats";
    auto cm = milvus::storage::CreateChunkManager(storage_config);
    milvus::storage::FileManagerContext ctx(field_meta, index_meta, cm);
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
