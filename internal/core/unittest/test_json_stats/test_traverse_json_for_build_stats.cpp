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
using milvus::index::JsonStatsBuildValue;
using milvus::index::JSONType;

// Friend accessor declared in JsonKeyStats to invoke private method for UT
class TraverseJsonForBuildStatsAccessor {
 public:
    static void
    Call(JsonKeyStats& s,
         const char* json,
         std::vector<std::string>& path,
         std::map<JsonKey, JsonStatsBuildValue>& values) {
        milvus::Json parsed(simdjson::padded_string(json, std::strlen(json)));
        s.TraverseJsonDocumentForBuildStats(parsed, path, values);
    }
};

// Friend accessor declared in JsonKeyStats to invoke private method for UT
class CollectSingleJsonStatsInfoAccessor {
 public:
    static void
    Call(JsonKeyStats& s,
         std::string_view json,
         std::map<JsonKey, milvus::index::KeyStatsInfo>& infos) {
        milvus::Json parsed(simdjson::padded_string(json.data(), json.size()));
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
    std::map<JsonKey, JsonStatsBuildValue> values;
    TraverseJsonForBuildStatsAccessor::Call(stats, json, path, values);

    // Expect collected key-value/type pairs
    auto expect_has = [&](const std::string& key,
                          JSONType type,
                          const std::string& value_substr) {
        JsonKey k{key, type};
        auto it = values.find(k);
        ASSERT_NE(it, values.end()) << "Missing key: " << key;
        EXPECT_EQ(it->second.storage_value, value_substr);
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
    std::map<JsonKey, JsonStatsBuildValue> values;
    TraverseJsonForBuildStatsAccessor::Call(stats, json, path, values);

    EXPECT_EQ(values.at(JsonKey{"/sub", JSONType::DOUBLE}).storage_value,
              "-1.4829972460841e-309");
    EXPECT_DOUBLE_EQ(
        values.at(JsonKey{"/sub", JSONType::DOUBLE}).parsed_double.value(),
        -1.4829972460841e-309);
    EXPECT_DOUBLE_EQ(values.at(JsonKey{"/underflow", JSONType::DOUBLE})
                         .parsed_double.value(),
                     0.0);
    EXPECT_DOUBLE_EQ(
        values.at(JsonKey{"/uint64", JSONType::DOUBLE}).parsed_double.value(),
        18446744073709551615.0);
    EXPECT_DOUBLE_EQ(
        values.at(JsonKey{"/normal", JSONType::DOUBLE}).parsed_double.value(),
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
    std::map<JsonKey, JsonStatsBuildValue> values;
    TraverseJsonForBuildStatsAccessor::Call(stats, json, path, values);

    EXPECT_TRUE(values.at(JsonKey{"/overflow", JSONType::DOUBLE})
                    .IsUnrepresentableNumber());
    EXPECT_TRUE(values.at(JsonKey{"/bigint", JSONType::DOUBLE})
                    .IsUnrepresentableNumber());
    EXPECT_EQ(values.at(JsonKey{"/ok", JSONType::INT64}).storage_value, "7");
}

TEST(TraverseJsonForBuildStatsTest,
     V3AcceptsSubnormalAndPreservesInvalidNumber) {
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
    std::map<JsonKey, JsonStatsBuildValue> values;
    EXPECT_NO_THROW(TraverseJsonForBuildStatsAccessor::Call(
        stats, R"({"subnormal":-1.48e-309})", path, values));
    EXPECT_DOUBLE_EQ(values.at(JsonKey{"/subnormal", JSONType::DOUBLE})
                         .parsed_double.value(),
                     -1.48e-309);

    path.clear();
    values.clear();
    EXPECT_NO_THROW(TraverseJsonForBuildStatsAccessor::Call(
        stats, R"({"overflow":1e400})", path, values));
    EXPECT_TRUE(values.at(JsonKey{"/overflow", JSONType::DOUBLE})
                    .IsUnrepresentableNumber());
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

TEST(TraverseJsonForBuildStatsTest, HandlesRootScalarDocuments) {
    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 100, {}};
    milvus::storage::IndexMeta index_meta{3, 100, 1, 1};
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto cm = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);
    milvus::storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    JsonKeyStats stats(ctx, true);

    struct TestCase {
        const char* json;
        JSONType type;
        bool unrepresentable_number;
    };
    const std::vector<TestCase> cases = {
        {"1", JSONType::INT64, false},
        {"-1.48e-309", JSONType::DOUBLE, false},
        {"1e400", JSONType::DOUBLE, true},
        {R"("text")", JSONType::STRING, false},
        {"true", JSONType::BOOL, false},
        {"null", JSONType::NONE, false},
    };

    for (const auto& test : cases) {
        SCOPED_TRACE(test.json);
        std::vector<std::string> path;
        std::map<JsonKey, JsonStatsBuildValue> values;
        ASSERT_NO_THROW(TraverseJsonForBuildStatsAccessor::Call(
            stats, test.json, path, values));
        auto value = values.find(JsonKey{"", test.type});
        ASSERT_NE(value, values.end());
        EXPECT_EQ(value->second.IsUnrepresentableNumber(),
                  test.unrepresentable_number);

        std::map<JsonKey, milvus::index::KeyStatsInfo> infos;
        ASSERT_NO_THROW(
            CollectSingleJsonStatsInfoAccessor::Call(stats, test.json, infos));
        EXPECT_NE(infos.find(JsonKey{"", test.type}), infos.end());
    }
}
