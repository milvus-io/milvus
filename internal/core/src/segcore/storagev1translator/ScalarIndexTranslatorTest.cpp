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

#include <filesystem>
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
#include "segcore/Types.h"
#include "segcore/storagev1translator/BsonInvertedIndexTranslator.h"
#include "segcore/storagev1translator/SealedIndexTranslator.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Util.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::segcore::storagev1translator {
namespace {

TEST(BsonInvertedIndexTranslatorTest, ReservesFixedScalarReaderMemoryOnce) {
    constexpr int64_t index_size = 4096;
    for (const bool mmap : {false, true}) {
        BsonInvertedIndexTranslator translator(
            {mmap, 3, 101, index_size, {}, 0, "disable", ""},
            storage::FileManagerContext{});
        const auto [loaded, overhead] =
            translator.estimated_byte_size_of_cell(0);
        EXPECT_EQ(
            loaded.memory_bytes,
            (mmap ? 0 : index_size) + index::kScalarIndexFixedResidentBytes);
        EXPECT_EQ(loaded.file_bytes, mmap ? index_size : 0);
        EXPECT_EQ(overhead.memory_bytes, mmap ? index_size : 0);
        EXPECT_EQ(overhead.file_bytes, mmap ? 0 : index_size);
    }
}

class ScalarIndexTranslatorTest : public ::testing::Test {
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

TEST_F(ScalarIndexTranslatorTest,
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
            SealedIndexTranslator translator(
                &info, tracer::TraceContext{}, context_, config);
            EXPECT_EQ(translator.Family(), index::families::kSort);
            EXPECT_EQ(translator.ValueType(), DataType::VARCHAR);
            auto cells = translator.get_cells(nullptr, {0});
            ASSERT_EQ(cells.size(), 1);
            const auto* reader = cells.front().second.get();
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
        }
    }
}

TEST_F(ScalarIndexTranslatorTest, PackedScalarLoadRequiresActualFileMetadata) {
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
            SealedIndexTranslator translator(
                &info, tracer::TraceContext{}, context_, config);
            FAIL() << "an explicit resource estimate cannot replace packed "
                      "file metadata";
        } catch (const SegcoreError& error) {
            EXPECT_NE(std::string(error.what()).find("one V3 file"),
                      std::string::npos);
        }
    }
}

}  // namespace
}  // namespace milvus::segcore::storagev1translator
