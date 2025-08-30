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
        {JsonKey("string_key", JSONType::STRING), JsonKeyLayoutType::DYNAMIC},
        {JsonKey("double_key", JSONType::DOUBLE), JsonKeyLayoutType::TYPED},
        {JsonKey("bool_key", JSONType::BOOL), JsonKeyLayoutType::TYPED},
        {JsonKey("shared_key", JSONType::STRING), JsonKeyLayoutType::SHARED}};

    auto metadata = CreateParquetKVMetadata(column_map);
    EXPECT_FALSE(metadata.empty());
    EXPECT_EQ(metadata.size(), 1);  // layout_type

    // Parse and verify layout_type
    auto layout_type_json = nlohmann::json::parse(metadata[0].second);
    for (const auto& [key, type] : column_map) {
        std::string key_with_type = key.key_ + "_" + ToString(key.type_);
        EXPECT_TRUE(layout_type_json.contains(key_with_type));
        EXPECT_EQ(layout_type_json[key_with_type], ToString(type));
    }
}

}  // namespace milvus::index