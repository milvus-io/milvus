// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/Array.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "indexbuilder/BuildSession.h"
#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"
#include "test_utils/Constants.h"

namespace milvus::indexbuilder {
namespace {

namespace schemapb = milvus::proto::schema;

class BuildSessionTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        remote_ = storage::LocalDirectory::CreateOwned(
            TestRemotePath, "build_session_XXXXXX", "build session test");
    }

    proto::indexcgo::BuildIndexInfo
    BuildInfo(schemapb::DataType field_type,
              const char* index_type,
              int64_t row_count,
              int32_t engine_version) const {
        proto::indexcgo::BuildIndexInfo info;
        info.set_collectionid(1);
        info.set_partitionid(2);
        info.set_segmentid(3);
        info.set_buildid(1000);
        info.set_index_version(1);
        info.set_num_rows(row_count);
        info.set_current_scalar_index_version(engine_version);
        auto* schema = info.mutable_field_schema();
        schema->set_fieldid(101);
        schema->set_name("values");
        schema->set_data_type(field_type);
        auto* param = info.add_index_params();
        param->set_key(index::INDEX_TYPE);
        param->set_value(index_type);
        auto* config = info.mutable_storage_config();
        config->set_storage_type("local");
        config->set_root_path(remote_->Path());
        return info;
    }

    std::string
    InsertPath() const {
        // BuildSession orders V1 binlogs by their numeric basename.
        return remote_->Path() + "/insert/1";
    }

    void
    WriteInsert(const storage::FileManagerContext& context,
                const FieldDataPtr& data) const {
        auto payload = std::make_shared<storage::PayloadReader>(data);
        storage::InsertData insert(payload);
        insert.SetFieldDataMeta(context.fieldDataMeta);
        insert.SetTimestamps(0, 100);
        auto bytes = insert.Serialize(storage::StorageType::Remote);
        context.chunkManagerPtr->Write(
            InsertPath(), bytes.data(), bytes.size());
    }

    std::vector<std::string>
    RemoteEntries() const {
        std::vector<std::string> entries;
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(remote_->Path())) {
            entries.push_back(entry.path().string());
        }
        std::sort(entries.begin(), entries.end());
        return entries;
    }

    void
    ExpectSkippedPublication(BuildSession& session) const {
        const auto before = RemoteEntries();
        ASSERT_NO_THROW(session.BuildFromSource());
        EXPECT_EQ(RemoteEntries(), before);
        // SkippedEmpty remains publishable without creating an output sink or
        // writer, including a repeated publication of the retained outcome.
        for (int attempt = 0; attempt < 2; ++attempt) {
            const auto stats = session.Publish();
            EXPECT_EQ(stats.MemSize(), 0);
            EXPECT_TRUE(stats.Files().empty());
            const auto wire_stats = AdaptArtifactStats(stats);
            EXPECT_EQ(wire_stats.mem_size(), 0);
            EXPECT_EQ(wire_stats.serialized_index_infos_size(), 0);
            EXPECT_EQ(RemoteEntries(), before);
        }
    }

    std::shared_ptr<storage::LocalDirectory> remote_;
};

TEST_F(BuildSessionTest, TextFieldUsesTextFamilyAndSchemaAnalyzer) {
    const std::vector<std::string> texts = {
        "alpha-beta gamma", "alpha beta", "delta"};
    for (const auto* tokenizer : {"standard", "whitespace"}) {
        SCOPED_TRACE(tokenizer);
        const auto analyzer =
            std::string(R"({"tokenizer":")") + tokenizer + R"("})";
        auto info = BuildInfo(schemapb::DataType::Text,
                              index::INVERTED_INDEX_TYPE,
                              texts.size(),
                              3);
        auto* schema = info.mutable_field_schema();
        schema->set_name("text");
        auto* enable = schema->add_type_params();
        enable->set_key("enable_analyzer");
        enable->set_value("true");
        auto* schema_analyzer = schema->add_type_params();
        schema_analyzer->set_key("analyzer_params");
        schema_analyzer->set_value(analyzer);
        // The field schema owns analyzer configuration, even if an incoming
        // index-parameter bag contains a different tokenizer.
        auto* bag_analyzer = info.add_index_params();
        bag_analyzer->set_key("analyzer_params");
        bag_analyzer->set_value(R"({"tokenizer":"standard"})");
        info.set_analyzer_extra_info("{}");
        info.add_insert_files(InsertPath());

        auto prepared = AdaptBuildIndexInfo(info, BuildPurpose::TextIndex);
        ASSERT_EQ(prepared.request.family, index::families::kText);
        EXPECT_EQ(prepared.request.value_type, DataType::TEXT);
        EXPECT_EQ(prepared.request.params.at("is_text_match"), true);
        EXPECT_EQ(prepared.request.params.at("analyzer_name"),
                  "milvus_tokenizer");
        EXPECT_EQ(prepared.request.params.at("analyzer_params"), analyzer);
        EXPECT_EQ(prepared.request.params.at("analyzer_extra_info"), "{}");
        EXPECT_EQ(prepared.request.output.generation,
                  storage::Generation::V3);
        EXPECT_EQ(prepared.request.output.storage_namespace,
                  storage::ArtifactStorageNamespace::TextLog);

        auto data = storage::CreateFieldData(
            DataType::TEXT, DataType::NONE, false);
        data->FillFieldData(texts.data(), texts.size());
        WriteInsert(prepared.file_manager_context, data);

        storage::LoadOptions options;
        options.params = prepared.request.params;
        const auto packed_name = prepared.request.output.packed_file_name;
        BuildSession session(std::move(prepared.request),
                             prepared.file_manager_context);
        ASSERT_NO_THROW(session.BuildFromSource());
        const auto stats = session.Publish();
        ASSERT_EQ(stats.Files().size(), 1);
        EXPECT_GT(stats.MemSize(), 0);
        EXPECT_EQ(stats.Files()[0].file_name, packed_name);
        EXPECT_EQ(stats.Files()[0].file_size, stats.MemSize());

        auto loader =
            index::LoaderRegistry::Instance().Lookup(index::families::kText);
        ASSERT_TRUE(static_cast<bool>(loader));
        auto reader = loader.Load(
            {index::IndexFiles{
                 prepared.file_manager_context,
                 {stats.Files()[0].file_name},
                 index::PackedIndexStorageConfig{
                     storage::ArtifactStorageNamespace::TextLog}},
             options});
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->ValueType(), DataType::TEXT);
        EXPECT_EQ(reader->Count(), texts.size());
        const auto* text =
            dynamic_cast<const index::ITextMatchReader*>(reader.get());
        ASSERT_NE(text, nullptr);
        const auto alpha = text->MatchQuery("alpha", 1);
        ASSERT_EQ(alpha.size(), texts.size());
        EXPECT_EQ(alpha[0], std::string(tokenizer) == "standard");
        EXPECT_TRUE(alpha[1]);
        EXPECT_FALSE(alpha[2]);
        const auto compound = text->MatchQuery("alpha-beta", 1);
        ASSERT_EQ(compound.size(), texts.size());
        EXPECT_TRUE(compound[0]);
        EXPECT_EQ(compound[1], std::string(tokenizer) == "standard");
        EXPECT_FALSE(compound[2]);
    }
}

TEST_F(BuildSessionTest, EmptyNestedSourceSkipsPersistence) {
    constexpr int64_t row_count = 8;
    for (const auto* index_type :
         {index::ASCENDING_SORT, index::BITMAP_INDEX_TYPE}) {
        for (const auto element_type :
             {schemapb::DataType::Int32, schemapb::DataType::String}) {
            for (const auto engine_version : {1, 3}) {
                SCOPED_TRACE(::testing::Message()
                             << "index=" << index_type
                             << ", element=" << element_type
                             << ", version=" << engine_version);
                auto info = BuildInfo(schemapb::DataType::Array,
                                      index_type,
                                      row_count,
                                      engine_version);
                auto* schema = info.mutable_field_schema();
                schema->set_name("profile[values]");
                schema->set_element_type(element_type);
                info.add_insert_files(InsertPath());
                auto prepared =
                    AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
                EXPECT_EQ(prepared.request.value_type,
                          static_cast<DataType>(element_type));
                EXPECT_EQ(prepared.request.params.at("nested"), true);
                EXPECT_EQ(prepared.request.output.generation,
                          engine_version == 1 ? storage::Generation::V1V2
                                              : storage::Generation::V3);

                std::vector<Array> arrays;
                arrays.reserve(row_count);
                for (int64_t row = 0; row < row_count; ++row) {
                    schemapb::ScalarField scalar;
                    // An empty array must still carry its element type through
                    // the binlog payload and nested materializer.
                    if (element_type == schemapb::DataType::String) {
                        scalar.mutable_string_data();
                    } else {
                        scalar.mutable_int_data();
                    }
                    arrays.emplace_back(scalar);
                }
                auto data = storage::CreateFieldData(
                    DataType::ARRAY, static_cast<DataType>(element_type), false);
                data->FillFieldData(arrays.data(), arrays.size());
                WriteInsert(prepared.file_manager_context, data);

                BuildSession session(std::move(prepared.request),
                                     std::move(prepared.file_manager_context));
                ASSERT_NO_FATAL_FAILURE(ExpectSkippedPublication(session));
            }
        }
    }
}

TEST_F(BuildSessionTest, EmptyScalarSourceSkipsPersistence) {
    for (const auto* index_type :
         {index::ASCENDING_SORT, index::BITMAP_INDEX_TYPE}) {
        for (const auto engine_version : {1, 3}) {
            SCOPED_TRACE(::testing::Message()
                         << "index=" << index_type
                         << ", version=" << engine_version);
            auto info = BuildInfo(
                schemapb::DataType::Int32, index_type, 0, engine_version);
            // Direct raw-input builds were retired. An empty V1 source feeds
            // the same real scalar builder with a complete zero-row input.
            auto prepared =
                AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
            ASSERT_TRUE(std::holds_alternative<V1BinlogBuildSource>(
                prepared.request.source));
            EXPECT_TRUE(std::get<V1BinlogBuildSource>(prepared.request.source)
                            .files.empty());
            BuildSession session(std::move(prepared.request),
                                 std::move(prepared.file_manager_context));
            ASSERT_NO_FATAL_FAILURE(ExpectSkippedPublication(session));
        }
    }
}

}  // namespace
}  // namespace milvus::indexbuilder
