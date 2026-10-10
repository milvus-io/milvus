// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Util.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::storage {
namespace {

class ScalarArtifactTransportTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        root_ = LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "scalar-transport-XXXXXX",
            "scalar transport test");
        StorageConfig storage;
        storage.storage_type = "local";
        // The output writer must create this missing parent itself.
        storage.root_path = root_->Path() + "/remote";
        FieldDataMeta field{1, 2, 3, 101};
        field.field_schema.set_data_type(proto::schema::DataType::VarChar);
        context_ = FileManagerContext(field,
                                      IndexMeta{3, 101, 1000, 1},
                                      CreateChunkManager(storage),
                                      InitArrowFileSystem(storage));
    }

    std::string
    Publish(const Artifact& artifact) const {
        MemFileManagerImpl manager(context_);
        auto writer = manager.CreateIndexEntryWriterUnified(
            index::PackedScalarIndexFileName(index::ScalarIndexType::MARISA));
        artifact.Serialize(*writer);
        writer->Finish();
        return manager.GetRemoteIndexObjectPrefix() + "/" +
               index::PackedScalarIndexFileName(index::ScalarIndexType::MARISA);
    }

    index::IIndexReaderBasePtr
    Load(const std::string& path, bool mmap) const {
        LoadOptions options;
        options.enable_mmap = mmap;
        options.mmap_dir_path = root_->Path() + "/missing/staging";
        options.params = {{"field_type", DataType::VARCHAR},
                          {"value_type", DataType::VARCHAR},
                          {"nested", false}};
        return index::LoaderRegistry::Instance()
            .Lookup(index::families::kMarisa)
            .Load({index::IndexFiles{
                       context_, {path}, index::PackedIndexStorageConfig{}},
                   options});
    }

    ArtifactPtr
    Build(const std::vector<std::string_view>& values) const {
        const index::ScalarBuildBatch<std::string_view> batch{values, {}};
        auto builder = index::BuilderRegistry<
                           index::ScalarBuildInput<std::string_view>>::Instance()
                           .Create(index::families::kMarisa, {});
        return std::move(*builder).Build({std::span(&batch, 1)});
    }

    std::shared_ptr<LocalDirectory> root_;
    FileManagerContext context_;
};

TEST_F(ScalarArtifactTransportTest, MarisaCreatesMissingPublicationAndMmapParents) {
    const std::vector<std::string_view> values{"alpha", "beta", "alpha"};
    auto artifact = Build(values);
    ASSERT_FALSE(std::filesystem::exists(root_->Path() + "/remote"));
    const auto path = Publish(*artifact);
    ASSERT_TRUE(std::filesystem::is_regular_file(path));
    ASSERT_FALSE(std::filesystem::exists(root_->Path() + "/missing"));
    auto reader = Load(path, true);
    ASSERT_NE(reader, nullptr);
    ASSERT_TRUE(std::filesystem::is_directory(root_->Path() + "/missing/staging"));
    const auto* predicate = dynamic_cast<
        const index::IScalarPredicateReader<std::string_view>*>(reader.get());
    ASSERT_NE(predicate, nullptr);
    const std::string_view key = "alpha";
    const auto hits = predicate->In(1, &key);
    ASSERT_EQ(hits.size(), values.size());
    EXPECT_TRUE(hits[0]);
    EXPECT_FALSE(hits[1]);
    EXPECT_TRUE(hits[2]);
}

TEST_F(ScalarArtifactTransportTest, MarisaPackedCodecPreservesQueries) {
    const std::vector<std::string_view> values{"0", "9", "1", "9", "2"};
    auto artifact = Build(values);
    const auto path = Publish(*artifact);
    artifact.reset();
    for (const bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        auto reader = Load(path, mmap);
        ASSERT_EQ(reader->Count(), values.size());
        const auto* predicate = dynamic_cast<
            const index::IScalarPredicateReader<std::string_view>*>(reader.get());
        const auto* pattern =
            dynamic_cast<const index::IPatternMatchReader*>(reader.get());
        ASSERT_NE(predicate, nullptr);
        ASSERT_NE(pattern, nullptr);
        EXPECT_EQ(predicate->In(values.size(), values.data()).count(), values.size());
        EXPECT_EQ(predicate->NotIn(values.size(), values.data()).count(), 0);
        const std::string_view absent = "100";
        EXPECT_EQ(predicate->In(1, &absent).count(), 0);
        EXPECT_EQ(predicate->Range("0", index::CompareOp::GreaterEqual).count(), values.size());
        EXPECT_EQ(predicate->Range("90", index::CompareOp::LessThan).count(), values.size());
        EXPECT_EQ(predicate->Range("9", index::CompareOp::LessEqual).count(), values.size());
        EXPECT_EQ(predicate->Range("0", true, "9", true).count(), values.size());
        EXPECT_EQ(predicate->Range("0", true, "90", false).count(), values.size());
        for (size_t row = 0; row < values.size(); ++row) {
            const auto hits = pattern->PatternMatch(
                values[row], index::PatternOp::PrefixMatch);
            ASSERT_EQ(hits.size(), values.size());
            EXPECT_TRUE(hits[row]);
        }
    }
}

}  // namespace
}  // namespace milvus::storage
