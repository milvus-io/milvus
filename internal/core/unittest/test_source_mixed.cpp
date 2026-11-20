// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "milvus-storage/manifest.h"
#include "segcore/external_utils_c.h"
#include "storage/Util.h"
#include "storage/StatusToErrorCode.h"
#include "storage/loon_ffi/source_context.h"

namespace {
class DeniedMixedSourceProvider
    : public milvus_storage::SourceCredentialProvider {
 public:
    milvus_storage::SourceCredentialResponse
    Fetch(const milvus_storage::SourceCredentialRequest&) const override {
        ++calls;
        milvus_storage::SourceCredentialResponse response;
        response.status = LOON_SOURCE_CREDENTIAL_DENIED;
        return response;
    }
    mutable std::atomic<int> calls{0};
};

class SourceContextMixedData : public ::testing::Test {
 protected:
    void
    SetUp() override {
        char path[] = "/tmp/milvus-mixed-source-XXXXXX";
        auto* created = mkdtemp(path);
        ASSERT_NE(created, nullptr);
        base = created;
        std::filesystem::create_directories(base + "/_data");
        std::filesystem::create_directories(base + "/_metadata");
        manifest_path = nlohmann::json{{"base_path", base}, {"ver", 1}}.dump();
        properties = std::make_shared<milvus_storage::api::Properties>();
        milvus_storage::api::SetValue(*properties, "fs.storage_type", "local");
        milvus_storage::api::SetValue(*properties, "fs.root_path", "/");
        provider = std::make_shared<DeniedMixedSourceProvider>();
        milvus_storage::SourceReadContextOptions options;
        options.account_name = "account123";
        options.allowed_root_uris = {
            "abfss://data@account123.dfs.core.windows.net/table"};
        auto opened =
            milvus_storage::SourceReadContext::Create(options, provider);
        ASSERT_TRUE(opened.ok()) << opened.status();
        context = *opened;
    }

    void
    TearDown() override {
        if (!base.empty())
            std::filesystem::remove_all(base);
    }

    void
    WriteColumns(const std::string& path,
                 const std::vector<std::string>& names) {
        arrow::Int64Builder builder;
        ASSERT_TRUE(builder.AppendValues({4, 9}).ok());
        std::shared_ptr<arrow::Array> values;
        ASSERT_TRUE(builder.Finish(&values).ok());
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (const auto& name : names) {
            fields.push_back(arrow::field(name, arrow::int64(), false));
            columns.push_back(values);
        }
        auto table = arrow::Table::Make(arrow::schema(fields), columns);
        auto output = arrow::io::FileOutputStream::Open(path);
        ASSERT_TRUE(output.ok()) << output.status();
        ASSERT_TRUE(parquet::arrow::WriteTable(
                        *table, arrow::default_memory_pool(), *output, 2)
                        .ok());
        ASSERT_TRUE((*output)->Close().ok());
    }

    std::shared_ptr<milvus_storage::api::ColumnGroup>
    Group(std::vector<std::string> columns, const std::string& path) {
        auto group = std::make_shared<milvus_storage::api::ColumnGroup>();
        group->columns = std::move(columns);
        group->format = "parquet";
        group->files.push_back({path, 0, 2, {}});
        return group;
    }

    void
    WriteManifest(const milvus_storage::api::ColumnGroups& groups) {
        auto fs =
            milvus_storage::FilesystemCache::getInstance().get(*properties);
        ASSERT_TRUE(fs.ok()) << fs.status();
        ASSERT_TRUE(milvus_storage::api::Manifest::WriteTo(
                        *fs,
                        base + "/_metadata/manifest-1.avro",
                        milvus_storage::api::Manifest(groups))
                        .ok());
    }

    std::string base, manifest_path;
    std::shared_ptr<milvus_storage::api::Properties> properties;
    std::shared_ptr<DeniedMixedSourceProvider> provider;
    milvus_storage::SourceReadContextPtr context;
    const std::string source =
        "https://account123.blob.core.windows.net/data/table/source.parquet";
};

TEST_F(SourceContextMixedData, SelectsOnlyOwnedGeneratedGroups) {
    milvus_storage::api::ColumnGroups groups{
        Group({"text"}, source),
        Group({"110", "111"}, base + "/_data/output.parquet")};
    EXPECT_EQ(milvus::storage::SelectInternalColumnGroups(
                  groups, manifest_path, {"110", "111"}),
              std::vector<size_t>{1});
    EXPECT_TRUE(
        milvus::storage::SelectInternalColumnGroups(groups, manifest_path, {})
            .empty());
    // Unknown columns cannot acquire internal authority from their path.
    EXPECT_TRUE(milvus::storage::SelectInternalColumnGroups(
                    groups, manifest_path, {"110"})
                    .empty());
    for (const auto& path : {source,
                             base + "-sibling/_data/output.parquet",
                             base + "/_data/../secret.parquet",
                             base + "/_data/%2e%2e.parquet",
                             std::string("output.parquet")}) {
        groups[1]->files[0].path = path;
        EXPECT_THROW(milvus::storage::SelectInternalColumnGroups(
                         groups, manifest_path, {"110", "111"}),
                     milvus::SegcoreError);
    }
    groups[1]->files[0].path = base + "/_data/output.parquet";
    groups[0]->files[0].path = groups[1]->files[0].path;
    EXPECT_THROW(milvus::storage::SelectInternalColumnGroups(
                     groups, manifest_path, {"110", "111"}),
                 milvus::SegcoreError);
    EXPECT_EQ(provider->calls.load(), 0);
}

TEST_F(SourceContextMixedData, GeneratedTakeAndChunkDoNotUseSourceCredentials) {
    const auto output = base + "/_data/output.parquet";
    WriteColumns(output, {"110"});
    auto groups = std::make_shared<milvus_storage::api::ColumnGroups>(
        milvus_storage::api::ColumnGroups{Group({"text"}, source),
                                          Group({"110"}, output)});
    const auto selected = milvus::storage::SelectInternalColumnGroups(
        *groups, manifest_path, {"110"});
    auto projection = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"110"});
    auto reader = milvus::storage::CreateSourceReader(
        groups, nullptr, projection, properties, context, selected);
    auto rows = reader->take({0, 1}, 1);
    ASSERT_TRUE(rows.ok()) << rows.status();
    ASSERT_EQ((*rows)->num_rows(), 2);
    EXPECT_EQ((*rows)->column(0)->GetScalar(1).ValueOrDie()->ToString(), "9");
    auto chunk = reader->get_chunk_reader(1);
    ASSERT_TRUE(chunk.ok()) << chunk.status();
    EXPECT_TRUE((*chunk)->get_chunk(0).ok());
    EXPECT_EQ(provider->calls.load(), 0);
    // The same internal-looking path has no authority in the strict source API.
    auto strict = milvus::storage::CreateSourceReader(
        groups, nullptr, projection, properties, context);
    EXPECT_FALSE(strict->take({0}, 1).ok());
    auto denied =
        reader->get_chunk_reader(0,
                                 std::make_shared<std::vector<std::string>>(
                                     std::vector<std::string>{"text"}));
    ASSERT_FALSE(denied.ok());
    EXPECT_EQ(milvus::storage::SourceAwareStorageError(denied.status())
                  .get_error_code(),
              static_cast<milvus::ErrorCode>(117));
}

TEST_F(SourceContextMixedData, IndexProjectionReadsSharedGeneratedFile) {
    const auto output = base + "/_data/output.parquet";
    WriteColumns(output, {"110", "111"});
    WriteManifest({Group({"text"}, source), Group({"110", "111"}, output)});
    milvus::proto::schema::FieldSchema field;
    field.set_fieldid(110);
    field.set_name("generated");
    field.set_data_type(milvus::proto::schema::DataType::Int64);
    field.set_is_function_output(true);
    milvus::storage::FieldDataMeta meta{1, 2, 3, 110, field};
    auto data =
        milvus::storage::GetFieldDatasFromManifest(manifest_path,
                                                   properties,
                                                   meta,
                                                   milvus::DataType::INT64,
                                                   0,
                                                   std::nullopt,
                                                   0,
                                                   0,
                                                   std::nullopt,
                                                   context);
    ASSERT_EQ(data.size(), 1);
    EXPECT_EQ(data[0]->get_num_rows(), 2);
    EXPECT_EQ(*static_cast<const int64_t*>(data[0]->RawValue(1)), 9);
    EXPECT_EQ(provider->calls.load(), 0);
}

TEST_F(SourceContextMixedData, SamplingDoesNotOpenGeneratedFiles) {
    const auto input = base + "/_data/source.parquet";
    WriteColumns(input, {"text"});
    // An unreadable generated file must not affect source-field accounting.
    WriteManifest({Group({"text"}, input),
                   Group({"110"}, base + "/_data/missing-output.parquet")});
    milvus::proto::schema::CollectionSchema schema;
    auto* field = schema.add_fields();
    field->set_fieldid(100);
    field->set_name("text");
    field->set_external_field("text");
    field->set_data_type(milvus::proto::schema::DataType::Int64);
    auto* output = schema.add_fields();
    output->set_fieldid(110);
    output->set_name("generated");
    output->set_data_type(milvus::proto::schema::DataType::Int64);
    output->set_is_function_output(true);
    auto serialized = schema.SerializeAsString();
    CProto c_schema{serialized.data(), static_cast<int64_t>(serialized.size())};
    const char* keys[] = {"fs.storage_type", "fs.root_path"};
    const char* values[] = {"local", "/"};
    LoonProperties c_properties{};
    auto created = loon_properties_create(keys, values, 2, &c_properties);
    ASSERT_NE(loon_ffi_is_success(&created), 0);
    loon_ffi_free_result(&created);
    CFieldMemSizeList sizes{};
    auto status = SampleExternalSegmentFieldSizes(
        manifest_path.c_str(), 2, 1, &c_properties, c_schema, &sizes);
    loon_properties_free(&c_properties);
    EXPECT_EQ(status.error_code, 0)
        << (status.error_msg ? status.error_msg : "");
    if (status.error_msg)
        free(const_cast<char*>(status.error_msg));
    ASSERT_EQ(sizes.count, 1);
    EXPECT_STREQ(sizes.sizes[0].field_name, "text");
    EXPECT_GT(sizes.sizes[0].avg_mem_bytes, 0);
    FreeCFieldMemSizeList(&sizes);
}
}  // namespace
