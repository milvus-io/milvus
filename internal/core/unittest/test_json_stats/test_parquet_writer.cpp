#include <gtest/gtest.h>
#include "index/json_stats/parquet_writer.h"
#include <arrow/io/memory.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <memory>
#include <string>
#include <vector>
#include <map>

namespace milvus::index {
class ParquetWriterFactoryTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Setup test column map
        column_map_ = {
            {JsonKey("int_key", JSONType::INT64), JsonKeyLayoutType::DYNAMIC},
            {JsonKey("string_key", JSONType::STRING),
             JsonKeyLayoutType::DYNAMIC},
            {JsonKey("double_key", JSONType::DOUBLE), JsonKeyLayoutType::TYPED},
            {JsonKey("bool_key", JSONType::BOOL), JsonKeyLayoutType::TYPED},
            {JsonKey("shared_key", JSONType::STRING),
             JsonKeyLayoutType::SHARED}};

        path_prefix_ = "test_prefix";
    }

    std::map<JsonKey, JsonKeyLayoutType> column_map_;
    std::string path_prefix_;
};

TEST_F(ParquetWriterFactoryTest, ColumnGroupingStrategyFactoryTest) {
    // Test creating default strategy
    auto default_strategy = ColumnGroupingStrategyFactory::CreateStrategy(
        ColumnGroupingStrategyType::DEFAULT);
    EXPECT_NE(default_strategy, nullptr);

    // Test creating with invalid type
    EXPECT_THROW(ColumnGroupingStrategyFactory::CreateStrategy(
                     static_cast<ColumnGroupingStrategyType>(999)),
                 std::runtime_error);
}

TEST_F(ParquetWriterFactoryTest, CreateContextBasicTest) {
    // Test creating context with basic column map
    auto context =
        ParquetWriterFactory::CreateContext(column_map_, path_prefix_);

    // Verify schema
    EXPECT_NE(context.schema, nullptr);
    EXPECT_EQ(context.schema->num_fields(), column_map_.size());

    // Verify builders
    EXPECT_FALSE(context.builders.empty());
    EXPECT_FALSE(context.builders_map.empty());
    EXPECT_EQ(context.builders.size(), column_map_.size());
    EXPECT_EQ(context.builders_map.size(), column_map_.size());

    // Verify metadata
    EXPECT_FALSE(context.kv_metadata.empty());

    // Verify column groups
    EXPECT_FALSE(context.column_groups.empty());

    // Verify file paths
    EXPECT_FALSE(context.file_paths.empty());
    EXPECT_EQ(context.file_paths.size(), context.column_groups.size());
}

TEST_F(ParquetWriterFactoryTest, CreateContextWithSharedFields) {
    // Test creating context with shared fields
    std::map<JsonKey, JsonKeyLayoutType> shared_map = {
        {JsonKey("shared_key1", JSONType::STRING), JsonKeyLayoutType::SHARED},
        {JsonKey("shared_key2", JSONType::STRING), JsonKeyLayoutType::SHARED},
        {JsonKey("normal_key", JSONType::INT64), JsonKeyLayoutType::TYPED}};

    auto context =
        ParquetWriterFactory::CreateContext(shared_map, path_prefix_);

    // Verify schema includes shared fields
    EXPECT_NE(context.schema, nullptr);
    EXPECT_EQ(context.schema->num_fields(), 2);

    EXPECT_EQ(context.builders_map.size(), 3);
}

TEST_F(ParquetWriterFactoryTest, CreateContextWithColumnGroups) {
    // Test creating context and verify column grouping
    auto context =
        ParquetWriterFactory::CreateContext(column_map_, path_prefix_);

    // Verify column groups are created
    EXPECT_FALSE(context.column_groups.empty());

    // Verify each column is assigned to a group
    std::set<int> group_ids;
    for (const auto& group : context.column_groups) {
        for (const auto& col_idx : group) {
            group_ids.insert(col_idx);
        }
    }

    // All columns should be assigned to a group
    EXPECT_EQ(group_ids.size(), column_map_.size());
}

}  // namespace milvus::index