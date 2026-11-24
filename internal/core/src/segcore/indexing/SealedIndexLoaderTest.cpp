// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <barrier>
#include <filesystem>
#include <future>
#include <memory>
#include <string_view>
#include <vector>

#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/LoadResource.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "segcore/Types.h"
#include "segcore/indexing/SealedIndexLoader.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Util.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::segcore {
namespace {

class SealedIndexLoaderTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        root_ = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "scalar-translator-XXXXXX",
            "scalar translator test");
        storage::StorageConfig storage;
        storage.storage_type = "local";
        storage.root_path = root_->Path();
        storage::FieldDataMeta field{1, 2, 3, 101};
        field.field_schema.set_name("profile[values]");
        field.field_schema.set_data_type(proto::schema::DataType::Array);
        field.field_schema.set_element_type(proto::schema::DataType::VarChar);
        context_ =
            storage::FileManagerContext(field,
                                        storage::IndexMeta{3, 101, 1, 1},
                                        storage::CreateChunkManager(storage),
                                        storage::InitArrowFileSystem(storage));
    }

    LoadIndexInfo
    Info() const {
        LoadIndexInfo info{};
        info.collection_id = 1;
        info.partition_id = 2;
        info.segment_id = 3;
        info.field_id = 101;
        info.field_type = DataType::ARRAY;
        info.element_type = DataType::VARCHAR;
        info.index_id = 1;
        info.index_build_id = 1;
        info.index_version = 1;
        info.index_engine_version = 3;
        info.num_rows = 4;
        info.index_params = {{index::INDEX_TYPE, index::HYBRID_INDEX_TYPE},
                             {index::SCALAR_INDEX_ENGINE_VERSION, "3"}};
        info.warmup_policy = "disable";
        return info;
    }

    std::shared_ptr<storage::LocalDirectory> root_;
    storage::FileManagerContext context_;
};

TEST_F(SealedIndexLoaderTest,
       NestedHybridMetadataResolvesStandaloneAndSelectedSortFiles) {
    const std::vector<std::string_view> values{
        "alpha", "beta", "beta", "gamma"};
    const index::ScalarBuildBatch<std::string_view> batch{values, {}};
    const Config params{{"field_type", DataType::ARRAY},
                        {"value_type", DataType::VARCHAR},
                        {"element_type", DataType::VARCHAR},
                        {"nested", true},
                        {"nullable", false}};
    auto builder = index::BuilderRegistry<
                       index::ScalarBuildInput<std::string_view>>::Instance()
                       .Create(index::families::kSort, params);
    auto artifact = std::move(*builder).Build({std::span(&batch, 1)});
    storage::MemFileManagerImpl manager(context_);
    for (const bool selector : {false, true}) {
        SCOPED_TRACE(selector);
        const auto name = index::PackedScalarIndexFileName(
            selector ? index::ScalarIndexType::HYBRID
                     : index::ScalarIndexType::STLSORT);
        auto writer = manager.CreateIndexEntryWriterUnified(name);
        artifact->Serialize(*writer);
        if (selector)
            writer->PutMeta(
                index::INDEX_TYPE,
                static_cast<uint8_t>(index::ScalarIndexType::STLSORT));
        writer->Finish();
        auto info = Info();
        info.index_files = {manager.GetRemoteIndexObjectPrefix() + "/" + name};
        info.index_size = writer->GetTotalBytesWritten();
        auto input = manager.OpenInputStream(info.index_files.front());
        auto packed = storage::IndexEntryReader::Open(input, input->Size());
        EXPECT_EQ(packed->IndexMeta().contains(index::INDEX_TYPE), selector);
        for (const bool async : {false, true}) {
            SCOPED_TRACE(async);
            context_.use_async_load = async;
            Config config(info.index_params);
            config["nested"] = true;
            std::unique_ptr<SealedIndexLoader> translator;
            {
                auto construction_info = info;
                folly::CancellationSource cancel;
                OpContext creation(cancel.getToken());
                creation.runtime_load_priority = 1;
                translator =
                    std::make_unique<SealedIndexLoader>(&construction_info,
                                                        tracer::TraceContext{},
                                                        context_,
                                                        config,
                                                        &creation);
                cancel.requestCancellation();
            }
            EXPECT_EQ(translator->Family(), index::families::kSort);
            EXPECT_EQ(translator->ValueType(), DataType::VARCHAR);
            OpContext load;
            auto owner = translator->CreateIndex(&load);
            // Readers share the Index after both construction inputs and the
            // loader are gone, including concurrent first use of the Index.
            // Each request owns its context and reader.
            translator.reset();
            constexpr int kReaders = 8;
            std::barrier start(kReaders);
            std::vector<std::future<void>> queries;
            for (int reader_id = 0; reader_id < kReaders; ++reader_id) {
                queries.push_back(
                    std::async(std::launch::async, [&, reader_id] {
                        start.arrive_and_wait();
                        for (int iteration = 0; iteration < 32; ++iteration) {
                            OpContext query;
                            auto reader = owner->CreateReader(&query);
                            const auto* predicate = dynamic_cast<
                                const index::IScalarPredicateReader<
                                    std::string_view>*>(reader.get());
                            ASSERT_NE(predicate, nullptr);
                            const bool beta = (reader_id + iteration) % 2 == 0;
                            const std::string_view key =
                                beta ? "beta" : "gamma";
                            const auto hits = predicate->In(1, &key);
                            ASSERT_EQ(hits.size(), 4);
                            EXPECT_EQ(hits.count(), beta ? 2 : 1);
                            EXPECT_FALSE(hits[0]);
                            EXPECT_EQ(hits[1], beta);
                            EXPECT_EQ(hits[2], beta);
                            EXPECT_EQ(hits[3], !beta);
                        }
                    }));
            }
            for (auto& query : queries) {
                query.get();
            }

            auto request_reader = owner->CreateReader(&load);
            const auto* reader = request_reader.get();
            ASSERT_NE(reader, nullptr);
            EXPECT_EQ(reader->CoordDomain(), index::Domain::Element);
            EXPECT_EQ(reader->Count(), 4);
            const auto* predicate = dynamic_cast<
                const index::IScalarPredicateReader<std::string_view>*>(reader);
            ASSERT_NE(predicate, nullptr);
            const std::string_view key = "beta";
            const auto hits = predicate->In(1, &key);
            ASSERT_EQ(hits.size(), 4);
            EXPECT_FALSE(hits[0]);
            EXPECT_TRUE(hits[1]);
            EXPECT_TRUE(hits[2]);
            EXPECT_FALSE(hits[3]);

            // A later reader also needs no data from the destroyed input.
            request_reader.reset();
            auto later_reader = owner->CreateReader(&load);
            const auto* later_predicate = dynamic_cast<
                const index::IScalarPredicateReader<std::string_view>*>(
                later_reader.get());
            ASSERT_NE(later_predicate, nullptr);
            const auto later_hits = later_predicate->In(1, &key);
            ASSERT_EQ(later_hits.size(), 4);
            EXPECT_EQ(later_hits.count(), 2);
        }
    }
}

TEST_F(SealedIndexLoaderTest, MarisaCreatesIndexBeforeLoadingRequestStorage) {
    const std::vector<std::string_view> values{
        "alpha", "beta", "beta", "gamma"};
    const index::ScalarBuildBatch<std::string_view> batch{values, {}};
    const Config params{{"field_type", DataType::VARCHAR},
                        {"value_type", DataType::VARCHAR},
                        {"nullable", false}};
    auto builder = index::BuilderRegistry<
                       index::ScalarBuildInput<std::string_view>>::Instance()
                       .Create(index::families::kMarisa, params);
    auto artifact = std::move(*builder).Build({std::span(&batch, 1)});
    storage::MemFileManagerImpl manager(context_);
    const auto name =
        index::PackedScalarIndexFileName(index::ScalarIndexType::MARISA);
    auto writer = manager.CreateIndexEntryWriterUnified(name);
    artifact->Serialize(*writer);
    writer->Finish();
    auto info = Info();
    info.field_type = DataType::VARCHAR;
    info.element_type = DataType::NONE;
    info.index_params[index::INDEX_TYPE] = index::MARISA_TRIE;
    info.index_files = {manager.GetRemoteIndexObjectPrefix() + "/" + name};
    info.index_size = writer->GetTotalBytesWritten();
    for (const bool async : {false, true}) {
        context_.use_async_load = async;
        auto translator = std::make_unique<SealedIndexLoader>(
            &info, tracer::TraceContext{}, context_, Config(info.index_params));
        ASSERT_EQ(translator->Family(), index::families::kMarisa);
        auto index = translator->CreateIndex(nullptr);
        translator.reset();
        EXPECT_EQ(index->Metadata().count, 4);
        OpContext context;
        auto reader = index->CreateReader(&context);
        index.reset();
        const auto* predicate = dynamic_cast<
            const index::IScalarPredicateReader<std::string_view>*>(
            reader.get());
        ASSERT_NE(predicate, nullptr);
        const std::string_view key = "beta";
        const auto hits = predicate->In(1, &key);
        EXPECT_EQ(hits.size(), 4);
        EXPECT_EQ(hits.count(), 2);
        EXPECT_TRUE(hits[1]);
        EXPECT_TRUE(hits[2]);
    }
}

TEST_F(SealedIndexLoaderTest, PackedScalarLoadRequiresActualFileMetadata) {
    for (const auto type : {DataType::INT16,
                            DataType::INT32,
                            DataType::INT64,
                            DataType::VARCHAR}) {
        SCOPED_TRACE(static_cast<int>(type));
        auto info = Info();
        info.field_type = type;
        info.element_type = DataType::NONE;
        info.index_size = 1024;
        info.load_resource_request =
            LoadResourceRequest{2048, 512, 1024, 128, true};
        Config config(info.index_params);
        try {
            SealedIndexLoader translator(
                &info, tracer::TraceContext{}, context_, config);
            FAIL() << "an explicit resource estimate cannot replace packed "
                      "file metadata";
        } catch (const SegcoreError& error) {
            EXPECT_NE(std::string(error.what()).find("one V3 file"),
                      std::string::npos);
        }
    }
}

TEST_F(SealedIndexLoaderTest,
       SkipsLegacyStructSubFieldIndexesAndAcceptsCompatibleOnes) {
    const std::vector<std::string_view> values{
        "alpha", "beta", "beta", "gamma"};
    const index::ScalarBuildBatch<std::string_view> batch{values, {}};
    storage::MemFileManagerImpl manager(context_);

    auto write_v3 = [&](const std::string& file_name,
                        const storage::Artifact& artifact,
                        std::optional<index::ScalarIndexType> hybrid_selector,
                        bool strip_is_nested) {
        index::test::TestArtifactData data = index::test::SerializeV3(artifact);
        if (strip_is_nested) {
            data.metadata.erase("is_nested");
        }
        if (hybrid_selector.has_value()) {
            data.metadata[index::INDEX_TYPE] =
                static_cast<uint8_t>(*hybrid_selector);
        }
        auto writer = manager.CreateIndexEntryWriterUnified(file_name);
        for (const auto& [entry_name, bytes] : data.entries) {
            writer->WriteEntry(entry_name, bytes.data(), bytes.size());
        }
        for (const auto& [key, val] : data.metadata) {
            writer->PutMeta(key, val);
        }
        writer->Finish();
        return std::pair<std::string, int64_t>{
            manager.GetRemoteIndexObjectPrefix() + "/" + file_name,
            static_cast<int64_t>(writer->GetTotalBytesWritten())};
    };

    auto build_artifact = [&](const std::string& family, bool nested) {
        const Config params{{index::FIELD_ID, int64_t{101}},
                            {"field_type", DataType::ARRAY},
                            {"value_type", DataType::VARCHAR},
                            {"element_type", DataType::VARCHAR},
                            {"nested", nested},
                            {"nullable", false}};
        auto builder =
            index::BuilderRegistry<
                index::ScalarBuildInput<std::string_view>>::Instance()
                .Create(family, params);
        return std::move(*builder).Build({std::span(&batch, 1)});
    };

    // 1. Legacy V3 HYBRID->BITMAP, BITMAP, SORT, and INVERTED (v3) without
    // is_nested on StructArray sub-field must be skipped cleanly.
    struct LegacyCase {
        std::string name;
        std::string index_type;
        std::string build_family;
        std::optional<index::ScalarIndexType> hybrid_selector;
        int scalar_version;
        bool strip_is_nested;
        bool expect_skipped;
    };
    const std::vector<LegacyCase> cases = {
        {"LegacyBitmap",
         index::BITMAP_INDEX_TYPE,
         index::families::kBitmap,
         std::nullopt,
         3,
         true,
         true},
        {"LegacyHybridBitmap",
         index::HYBRID_INDEX_TYPE,
         index::families::kBitmap,
         index::ScalarIndexType::BITMAP,
         3,
         true,
         true},
        {"LegacySort",
         index::ASCENDING_SORT,
         index::families::kSort,
         std::nullopt,
         3,
         true,
         true},
        {"LegacyInvertedV3",
         index::INVERTED_INDEX_TYPE,
         index::families::kInverted,
         std::nullopt,
         3,
         true,
         true},
        {"LegacyInvertedV4",
         index::INVERTED_INDEX_TYPE,
         index::families::kInverted,
         std::nullopt,
         4,
         true,
         false},
        {"NewInvertedV3WithMeta",
         index::INVERTED_INDEX_TYPE,
         index::families::kInverted,
         std::nullopt,
         3,
         false,
         false},
    };

    for (const auto& tc : cases) {
        SCOPED_TRACE(tc.name);
        auto artifact = build_artifact(tc.build_family, true);
        const auto [remote_file, bytes] = write_v3(
            tc.name + ".v3", *artifact, tc.hybrid_selector, tc.strip_is_nested);
        auto info = Info();
        info.index_files = {remote_file};
        info.index_size = bytes;
        info.index_params[index::INDEX_TYPE] = tc.index_type;
        info.index_params[index::SCALAR_INDEX_ENGINE_VERSION] =
            std::to_string(tc.scalar_version);
        Config config(info.index_params);
        config["nested"] = true;
        SealedIndexLoader loader(
            &info, tracer::TraceContext{}, context_, config);
        EXPECT_EQ(loader.IsSkipped(), tc.expect_skipped);
        auto index = loader.CreateIndex(nullptr);
        if (tc.expect_skipped) {
            EXPECT_EQ(index, nullptr);
            EXPECT_FALSE(loader.Caps().predicate);
            EXPECT_FALSE(loader.Caps().pattern_match);
            EXPECT_FALSE(loader.Caps().nested);
        } else {
            ASSERT_NE(index, nullptr);
            auto reader = index->CreateReader(nullptr);
            ASSERT_NE(reader, nullptr);
            EXPECT_EQ(reader->CoordDomain(), index::Domain::Element);
        }
    }

    // 2. Ordinary (non-struct) scalar/ARRAY without is_nested remains usable
    // as Row.
    {
        const Config scalar_params{{"field_type", DataType::VARCHAR},
                                   {"value_type", DataType::VARCHAR},
                                   {"nested", false},
                                   {"nullable", false}};
        auto builder =
            index::BuilderRegistry<
                index::ScalarBuildInput<std::string_view>>::Instance()
                .Create(index::families::kBitmap, scalar_params);
        auto artifact = std::move(*builder).Build({std::span(&batch, 1)});
        const auto [remote_file, bytes] =
            write_v3("OrdinaryScalarBitmap.v3",
                     *artifact,
                     index::ScalarIndexType::BITMAP,
                     true);
        auto info = Info();
        info.field_type = DataType::VARCHAR;
        info.element_type = DataType::NONE;
        info.index_files = {remote_file};
        info.index_size = bytes;
        Config config(info.index_params);
        config["nested"] = false;
        SealedIndexLoader loader(
            &info, tracer::TraceContext{}, context_, config);
        EXPECT_FALSE(loader.IsSkipped());
        auto index = loader.CreateIndex(nullptr);
        ASSERT_NE(index, nullptr);
        auto reader = index->CreateReader(nullptr);
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(reader->CoordDomain(), index::Domain::Row);
    }
}

}  // namespace
}  // namespace milvus::segcore
