#include <gtest/gtest.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>
#include <filesystem>
#include <map>
#include <parquet/arrow/writer.h>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "index/json_stats/parquet_writer.h"
#include "index/json_stats/utils.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"

namespace milvus::index {
namespace {

class CloseFailingOutputStream : public arrow::io::OutputStream {
 public:
    explicit CloseFailingOutputStream(
        std::shared_ptr<arrow::io::OutputStream> delegate)
        : delegate_(std::move(delegate)) {
        set_mode(arrow::io::FileMode::WRITE);
    }

    arrow::Status
    Close() override {
        closed_ = true;
        auto status = delegate_->Close();
        if (!status.ok()) {
            return status;
        }
        return arrow::Status::IOError("injected final close failure");
    }

    bool
    closed() const override {
        return closed_ || delegate_->closed();
    }

    arrow::Result<int64_t>
    Tell() const override {
        return delegate_->Tell();
    }

    arrow::Status
    Write(const void* data, int64_t nbytes) override {
        return delegate_->Write(data, nbytes);
    }

    arrow::Status
    Flush() override {
        return delegate_->Flush();
    }

    using arrow::io::Writable::Write;

 private:
    std::shared_ptr<arrow::io::OutputStream> delegate_;
    bool closed_{false};
};

class CloseFailingLocalFileSystem : public arrow::fs::LocalFileSystem {
 public:
    using arrow::fs::LocalFileSystem::LocalFileSystem;

    arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
    OpenOutputStream(const std::string& path,
                     const std::shared_ptr<const arrow::KeyValueMetadata>&
                         metadata) override {
        auto result =
            arrow::fs::LocalFileSystem::OpenOutputStream(path, metadata);
        if (!result.ok()) {
            return result.status();
        }
        return std::make_shared<CloseFailingOutputStream>(result.ValueOrDie());
    }
};

}  // namespace

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

namespace {

milvus_storage::StorageConfig
CreatePackedWriterStorageConfig() {
    return milvus_storage::StorageConfig{};
}

milvus_storage::ArrowFileSystemPtr
CreateLocalArrowFileSystem() {
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    return milvus::storage::InitArrowFileSystem(storage_config);
}

}  // namespace

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
    auto fs = CreateLocalArrowFileSystem();
    ASSERT_NE(fs, nullptr);
    auto path_prefix = std::filesystem::path("json_stats_writer_close");
    auto local_path = std::filesystem::path(TestLocalPath) / path_prefix;
    std::filesystem::remove_all(local_path);
    ASSERT_TRUE(std::filesystem::create_directories(local_path));

    std::map<JsonKey, JsonKeyLayoutType> column_map = {
        {JsonKey("/int", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/shared", JSONType::STRING), JsonKeyLayoutType::SHARED},
    };

    auto storage_config = CreatePackedWriterStorageConfig();
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

    std::filesystem::remove_all(local_path);
}

TEST_F(ParquetWriterFactoryTest, ClosePropagatesFinalCloseFailure) {
    auto fs = std::make_shared<CloseFailingLocalFileSystem>();
    auto path_prefix =
        std::filesystem::path(TestLocalPath) / "json_stats_writer_close_fail";
    std::filesystem::remove_all(path_prefix);
    ASSERT_TRUE(std::filesystem::create_directories(path_prefix));

    std::map<JsonKey, JsonKeyLayoutType> column_map = {
        {JsonKey("/int", JSONType::INT64), JsonKeyLayoutType::TYPED},
        {JsonKey("/shared", JSONType::STRING), JsonKeyLayoutType::SHARED},
    };

    auto storage_config = CreatePackedWriterStorageConfig();
    JsonStatsParquetWriter writer(fs, storage_config, 16 * 1024 * 1024, 1024);
    auto context =
        ParquetWriterFactory::CreateContext(column_map, path_prefix.string());
    writer.Init(std::move(context));

    writer.AppendValue(JsonKey("/int", JSONType::INT64).ToColumnName(), "42");
    writer.AppendSharedRow(nullptr, 0);
    writer.AddCurrentRow();

    auto status = writer.Close();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.ToString().find("injected final close failure"),
              std::string::npos);

    std::filesystem::remove_all(path_prefix);
}

}  // namespace milvus::index
