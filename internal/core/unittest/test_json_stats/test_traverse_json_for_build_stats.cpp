// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License

#include <cstring>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/jsmn.h"
#include "common/protobuf_utils.h"
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

TEST(JsonFieldDataTest, FillFieldDataWithDefaultValueOwnsData) {
    // Regression test for #52843: the default value used to be stored as a
    // non-owning Json view onto a local std::string that dies with
    // FillFieldData, leaving every filled row dangling.
    std::string default_json =
        R"({"default_key": 12345, "another_key": "value"})";
    proto::schema::ValueField default_value;
    default_value.set_bytes_data(default_json);

    auto field_data = milvus::storage::CreateFieldData(
        milvus::DataType::JSON, milvus::DataType::NONE, false, 1, 5);
    field_data->FillFieldData(milvus::DefaultValueType{default_value}, 5);

    // All filled rows must share a single owned copy: identical backing
    // pointer, so a fill of N rows performs exactly one data copy.
    const char* shared_ptr =
        static_cast<const milvus::Json*>(field_data->RawValue(0))
            ->data()
            .data();
    for (int i = 1; i < 5; i++) {
        auto* json = static_cast<const milvus::Json*>(field_data->RawValue(i));
        EXPECT_EQ(json->data().data(), shared_ptr);
    }

    ClobberStackAndHeap(default_json);

    for (int i = 0; i < 5; i++) {
        auto* json = static_cast<const milvus::Json*>(field_data->RawValue(i));
        EXPECT_EQ(std::string_view(json->data()),
                  std::string_view(default_json));
    }
}
