#include <gtest/gtest.h>
#include "index/json_stats/utils.h"
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <nlohmann/json.hpp>

namespace milvus::index {

class UtilsTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }
};

TEST_F(UtilsTest, CreateSharedArrowBuilderTest) {
    auto builder = CreateSharedArrowBuilder();
    EXPECT_NE(builder, nullptr);
    EXPECT_EQ(builder->type()->id(), arrow::Type::BINARY);
}

TEST_F(UtilsTest, CreateSharedArrowFieldTest) {
    auto field = CreateSharedArrowField("test_field", 1);
    EXPECT_NE(field, nullptr);
    EXPECT_EQ(field->name(), "test_field");
    EXPECT_EQ(field->type()->id(), arrow::Type::BINARY);
    EXPECT_TRUE(field->nullable());
    EXPECT_TRUE(field->HasMetadata());
}

TEST_F(UtilsTest, CreateArrowBuildersTest) {
    std::map<JsonKey, JsonKeyLayoutType> column_map = {
        {JsonKey("int_key", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("string_key", JSONType::STRING), JsonKeyLayoutType::DYNAMIC},
        {JsonKey("double_key", JSONType::DOUBLE), JsonKeyLayoutType::TYPED},
        {JsonKey("bool_key", JSONType::BOOL), JsonKeyLayoutType::TYPED},
        {JsonKey("shared_key", JSONType::STRING), JsonKeyLayoutType::SHARED}};

    auto [builders, builders_map] = CreateArrowBuilders(column_map);
    EXPECT_EQ(builders.size(), column_map.size());
    EXPECT_EQ(builders_map.size(), column_map.size());

    auto schema = CreateArrowSchema(column_map);
    EXPECT_NE(schema, nullptr);
    EXPECT_EQ(schema->num_fields(), column_map.size());
}

TEST_F(UtilsTest, CreateParquetKVMetadataTest) {
    std::map<JsonKey, JsonKeyLayoutType> column_map = {
        {JsonKey("int_key", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("string_key", JSONType::STRING), JsonKeyLayoutType::DYNAMIC}};

    // After moving meta to separate file, this should return empty
    auto metadata = CreateParquetKVMetadata(column_map);
    EXPECT_TRUE(metadata.empty());
}

// JsonStatsMeta tests
class JsonStatsMetaTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }
};

TEST_F(JsonStatsMetaTest, StringValuesTest) {
    JsonStatsMeta meta;

    // Default version should be set
    auto version = meta.GetString(META_KEY_VERSION);
    EXPECT_TRUE(version.has_value());
    EXPECT_EQ(version.value(), META_CURRENT_VERSION);

    // Set and get custom string value
    meta.SetString("custom_key", "custom_value");
    auto value = meta.GetString("custom_key");
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(value.value(), "custom_value");

    // Non-existent key should return nullopt
    auto non_existent = meta.GetString("non_existent");
    EXPECT_FALSE(non_existent.has_value());
}

TEST_F(JsonStatsMetaTest, Int64ValuesTest) {
    JsonStatsMeta meta;

    meta.SetInt64(META_KEY_NUM_ROWS, 10000);
    auto num_rows = meta.GetInt64(META_KEY_NUM_ROWS);
    EXPECT_TRUE(num_rows.has_value());
    EXPECT_EQ(num_rows.value(), 10000);

    meta.SetInt64(META_KEY_NUM_SHREDDING_COLUMNS, 50);
    auto num_columns = meta.GetInt64(META_KEY_NUM_SHREDDING_COLUMNS);
    EXPECT_TRUE(num_columns.has_value());
    EXPECT_EQ(num_columns.value(), 50);

    // Non-existent key should return nullopt
    auto non_existent = meta.GetInt64("non_existent");
    EXPECT_FALSE(non_existent.has_value());
}

TEST_F(JsonStatsMetaTest, LayoutTypeMapTest) {
    JsonStatsMeta meta;

    std::map<JsonKey, JsonKeyLayoutType> layout_map = {
        {JsonKey("/a/b", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/a/c", JSONType::STRING), JsonKeyLayoutType::DYNAMIC},
        {JsonKey("/x/y", JSONType::DOUBLE), JsonKeyLayoutType::SHARED}};

    meta.SetLayoutTypeMap(layout_map);

    const auto& retrieved = meta.GetLayoutTypeMap();
    EXPECT_EQ(retrieved.size(), layout_map.size());

    for (const auto& [key, type] : layout_map) {
        auto it = retrieved.find(key);
        EXPECT_NE(it, retrieved.end());
        EXPECT_EQ(it->second, type);
    }
}

TEST_F(JsonStatsMetaTest, BuildKeyFieldMapTest) {
    JsonStatsMeta meta;

    std::map<JsonKey, JsonKeyLayoutType> layout_map = {
        {JsonKey("/a/b", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/a/b", JSONType::STRING), JsonKeyLayoutType::DYNAMIC},
        {JsonKey("/x/y", JSONType::DOUBLE), JsonKeyLayoutType::SHARED}};

    meta.SetLayoutTypeMap(layout_map);

    auto key_field_map = meta.BuildKeyFieldMap();

    // SHARED keys should be skipped
    EXPECT_EQ(key_field_map.size(), 1);  // Only /a/b
    EXPECT_TRUE(key_field_map.find("/a/b") != key_field_map.end());
    EXPECT_EQ(key_field_map["/a/b"].size(), 2);  // INT64 and STRING columns
    EXPECT_TRUE(key_field_map["/a/b"].count("/a/b_INT64") > 0);
    EXPECT_TRUE(key_field_map["/a/b"].count("/a/b_STRING") > 0);

    // SHARED key should not be in the map
    EXPECT_TRUE(key_field_map.find("/x/y") == key_field_map.end());
}

TEST_F(JsonStatsMetaTest, SerializeDeserializeTest) {
    JsonStatsMeta meta;

    // Set various values
    meta.SetString("custom_string", "test_value");
    meta.SetInt64(META_KEY_NUM_ROWS, 12345);
    meta.SetInt64(META_KEY_NUM_SHREDDING_COLUMNS, 67);

    std::map<JsonKey, JsonKeyLayoutType> layout_map = {
        {JsonKey("/a/b", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/c/d", JSONType::STRING), JsonKeyLayoutType::DYNAMIC}};
    meta.SetLayoutTypeMap(layout_map);

    // Serialize
    std::string serialized = meta.Serialize();
    EXPECT_FALSE(serialized.empty());

    // Deserialize
    JsonStatsMeta deserialized = JsonStatsMeta::Deserialize(serialized);

    // Verify string values
    EXPECT_EQ(deserialized.GetString(META_KEY_VERSION).value(),
              META_CURRENT_VERSION);
    EXPECT_EQ(deserialized.GetString("custom_string").value(), "test_value");

    // Verify int64 values
    EXPECT_EQ(deserialized.GetInt64(META_KEY_NUM_ROWS).value(), 12345);
    EXPECT_EQ(deserialized.GetInt64(META_KEY_NUM_SHREDDING_COLUMNS).value(),
              67);

    // Verify layout type map
    const auto& retrieved_layout = deserialized.GetLayoutTypeMap();
    EXPECT_EQ(retrieved_layout.size(), layout_map.size());
}

TEST_F(JsonStatsMetaTest, DeserializeToKeyFieldMapTest) {
    JsonStatsMeta meta;

    std::map<JsonKey, JsonKeyLayoutType> layout_map = {
        {JsonKey("/a/b", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/a/b", JSONType::STRING), JsonKeyLayoutType::DYNAMIC},
        {JsonKey("/c/d", JSONType::DOUBLE), JsonKeyLayoutType::TYPED},
        {JsonKey("/x/y", JSONType::BOOL), JsonKeyLayoutType::SHARED}};
    meta.SetLayoutTypeMap(layout_map);

    std::string serialized = meta.Serialize();

    // Use static method to deserialize directly to key_field_map
    auto key_field_map = JsonStatsMeta::DeserializeToKeyFieldMap(serialized);

    // SHARED keys should be skipped
    EXPECT_EQ(key_field_map.size(), 2);  // /a/b and /c/d

    // Check /a/b has two columns
    EXPECT_TRUE(key_field_map.find("/a/b") != key_field_map.end());
    EXPECT_EQ(key_field_map["/a/b"].size(), 2);

    // Check /c/d has one column
    EXPECT_TRUE(key_field_map.find("/c/d") != key_field_map.end());
    EXPECT_EQ(key_field_map["/c/d"].size(), 1);

    // SHARED key should not be present
    EXPECT_TRUE(key_field_map.find("/x/y") == key_field_map.end());
}

TEST_F(JsonStatsMetaTest, DeserializeInvalidJsonTest) {
    std::string invalid_json = "not a valid json";

    EXPECT_THROW(JsonStatsMeta::Deserialize(invalid_json), std::exception);
    EXPECT_THROW(JsonStatsMeta::DeserializeToKeyFieldMap(invalid_json),
                 std::exception);
}

TEST_F(JsonStatsMetaTest, GetSerializedSizeTest) {
    JsonStatsMeta meta;
    meta.SetInt64(META_KEY_NUM_ROWS, 100);

    size_t size = meta.GetSerializedSize();
    std::string serialized = meta.Serialize();

    EXPECT_EQ(size, serialized.size());
}

TEST_F(JsonStatsMetaTest, EmptyLayoutTypeMapTest) {
    JsonStatsMeta meta;

    // Empty layout type map
    std::string serialized = meta.Serialize();
    EXPECT_FALSE(serialized.empty());

    JsonStatsMeta deserialized = JsonStatsMeta::Deserialize(serialized);
    EXPECT_TRUE(deserialized.GetLayoutTypeMap().empty());

    auto key_field_map = JsonStatsMeta::DeserializeToKeyFieldMap(serialized);
    EXPECT_TRUE(key_field_map.empty());
}

}  // namespace milvus::index