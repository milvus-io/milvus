#include <arrow/type.h>
#include <gtest/gtest.h>
#include <cmath>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/json_stats/parquet_writer.h"
#include "index/json_stats/utils.h"
#include "segcore/default_fs.h"
#include "test_utils/Constants.h"

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
    EXPECT_TRUE(context.kv_metadata.empty());

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

TEST_F(ParquetWriterFactoryTest, CloseReturnsStatusAndIsIdempotent) {
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    auto path_prefix =
        std::filesystem::path(TestLocalPath) / "json_stats_writer_close";
    std::filesystem::remove_all(path_prefix);
    ASSERT_TRUE(std::filesystem::create_directories(path_prefix));

    std::map<JsonKey, JsonKeyLayoutType> column_map = {
        {JsonKey("/int", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/shared", JSONType::STRING), JsonKeyLayoutType::SHARED},
    };

    milvus_storage::StorageConfig storage_config;
    JsonStatsParquetWriter writer(fs, storage_config, 16 * 1024 * 1024, 1024);
    auto context =
        ParquetWriterFactory::CreateContext(column_map, path_prefix.string());
    writer.Init(std::move(context));

    writer.AppendValue(JsonKey("/int", JSONType::INT64).ToColumnName(), "42");
    writer.AppendSharedRow(nullptr, 0);
    writer.AddCurrentRow();

    auto status = writer.Close();
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(writer.Close().ok());
    EXPECT_FALSE(writer.GetPathsToSize().empty());

    std::filesystem::remove_all(path_prefix);
}

TEST_F(ParquetWriterFactoryTest,
       AppendsTypedNullEmptyStringAndParsedDoubleDirectly) {
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    auto path_prefix =
        std::filesystem::path(TestLocalPath) / "json_stats_writer_null";
    std::filesystem::remove_all(path_prefix);
    ASSERT_TRUE(std::filesystem::create_directories(path_prefix));

    const auto double_key = JsonKey("/double", JSONType::DOUBLE).ToColumnName();
    const auto string_key = JsonKey("/string", JSONType::STRING).ToColumnName();
    std::map<JsonKey, JsonKeyLayoutType> column_map = {
        {JsonKey("/double", JSONType::DOUBLE), JsonKeyLayoutType::TYPED},
        {JsonKey("/string", JSONType::STRING), JsonKeyLayoutType::TYPED},
        {JsonKey("/shared", JSONType::STRING), JsonKeyLayoutType::SHARED},
    };
    milvus_storage::StorageConfig storage_config;
    JsonStatsParquetWriter writer(fs, storage_config, 16 * 1024 * 1024, 1024);
    auto context =
        ParquetWriterFactory::CreateContext(column_map, path_prefix.string());
    auto double_builder = std::static_pointer_cast<arrow::DoubleBuilder>(
        context.builders_map[double_key]);
    auto string_builder = std::static_pointer_cast<arrow::StringBuilder>(
        context.builders_map[string_key]);
    writer.Init(std::move(context));

    writer.AppendNull(double_key);
    writer.AppendValue(string_key, "");
    writer.AppendSharedRow(nullptr, 0);
    writer.AddCurrentRow();
    writer.AppendDouble(double_key, -1.4829972460841e-309);
    writer.AppendNull(string_key);
    writer.AppendSharedRow(nullptr, 0);
    writer.AddCurrentRow();

    ASSERT_EQ(double_builder->length(), 2);
    EXPECT_EQ(double_builder->null_count(), 1);
    ASSERT_EQ(string_builder->length(), 2);
    EXPECT_EQ(string_builder->null_count(), 1);
    ASSERT_TRUE(writer.Close().ok());
    std::filesystem::remove_all(path_prefix);
}

// Regression for https://github.com/milvus-io/milvus/issues/52806
// Conversion uses the same simdjson get_number() contract as raw predicates.
TEST(ConvertValueTest, MatchesSimdjsonNumberSemantics) {
    const double sub = -1.4829972460841e-309;
    EXPECT_DOUBLE_EQ(ConvertValue<double>(std::string("-1.4829972460841e-309")),
                     sub);
    EXPECT_DOUBLE_EQ(ConvertValue<double>(std::string("-2.32430876e-316")),
                     -2.32430876e-316);

    // Float narrowing still follows C++ conversion after a valid JSON double.
    EXPECT_TRUE(std::isinf(ConvertValue<float>(std::string("1e40"))));

    // Underflow-to-zero clamps instead of throwing.
    EXPECT_DOUBLE_EQ(ConvertValue<double>(std::string("1e-400")), 0.0);

    // Values rejected by raw get_number() are rejected here too, even when a
    // standalone double conversion could produce a rounded or infinite value.
    EXPECT_ANY_THROW(ConvertValue<double>(std::string("1e400")));
    EXPECT_ANY_THROW(ConvertValue<double>(std::string("18446744073709551616")));

    // Normal values keep exact round-trip.
    EXPECT_DOUBLE_EQ(ConvertValue<double>(std::string("-1.5")), -1.5);
    EXPECT_FLOAT_EQ(ConvertValue<float>(std::string("2.5")), 2.5f);

    // Malformed input still fails loudly.
    EXPECT_ANY_THROW(ConvertValue<double>(std::string("abc")));
    EXPECT_ANY_THROW(ConvertValue<double>(std::string("1.5xyz")));
}

}  // namespace milvus::index
