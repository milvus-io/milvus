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

#include "index/Families.h"
#include "index/contracts/Registry.h"
#include "index/contracts/query/ITextMatchReader.h"
#include "storage/test_utils/TextPublicationTestUtils.h"

namespace milvus::storage {
namespace {

TEST(TextArtifactPublicationTest, LegacyAndPackedPublishRelativeTextLogNames) {
    for (const bool packed : {false, true}) {
        SCOPED_TRACE(packed);
        text_test::TextPublicationFixture fixture;
        const auto stats = fixture.Publish(packed);
        ASSERT_FALSE(stats.Files().empty());
        std::vector<std::string> paths;
        paths.reserve(stats.Files().size());
        for (const auto& file : stats.Files()) {
            EXPECT_FALSE(file.file_name.empty());
            EXPECT_EQ(file.file_name.find(fixture.root->Path()), std::string::npos);
            EXPECT_EQ(file.file_name.find(TEXT_LOG_ROOT_PATH), std::string::npos);
            EXPECT_GT(file.file_size, 0);
            paths.push_back(file.file_name);
        }
        if (packed) {
            ASSERT_EQ(paths.size(), 1);
            EXPECT_TRUE(paths.front().ends_with(".v3"));
        }
        LoadOptions options;
        options.params = {{"field_type", DataType::VARCHAR},
                          {"value_type", DataType::VARCHAR},
                          {"nested", false},
                          {"analyzer_params", "{}"}};
        const auto storage_config = packed
            ? std::variant<index::PackedIndexStorageConfig, index::LegacyIndexStorageConfig>{index::PackedIndexStorageConfig{ArtifactStorageNamespace::TextLog}}
            : std::variant<index::PackedIndexStorageConfig, index::LegacyIndexStorageConfig>{index::LegacyIndexStorageConfig{V1SourceLayout::DiskFiles, ArtifactStorageNamespace::TextLog}};
        auto reader = index::LoaderRegistry::Instance().Lookup(index::families::kText).Load(
            {index::IndexFiles{fixture.context, paths, storage_config}, options});
        ASSERT_NE(reader, nullptr);
        const auto* text = dynamic_cast<const index::ITextMatchReader*>(reader.get());
        ASSERT_NE(text, nullptr);
        const auto hits = text->MatchQuery("alpha", 1);
        ASSERT_EQ(hits.size(), 3);
        EXPECT_TRUE(hits[0]);
        EXPECT_FALSE(hits[1]);
        EXPECT_FALSE(hits[2]);
    }
}

}  // namespace
}  // namespace milvus::storage
