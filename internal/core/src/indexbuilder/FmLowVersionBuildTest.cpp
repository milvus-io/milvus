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

#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/FieldData.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/PackedIndexLoad.h"
#include "index/contracts/Registry.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "indexbuilder/BuildSession.h"
#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::indexbuilder {
namespace {

class FmLowVersionBuildTest : public ::testing::TestWithParam<int> {};

TEST_P(FmLowVersionBuildTest, PublishesPackedArtifactAndLoadsQueries) {
    const auto root = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "fm-low-version-build-XXXXXX",
        "FM low version build test");
    const std::vector<std::string> values{
        "alpha", "beta", "alpine", "gamma", "alphabet", "omega"};
    const std::array<uint8_t, 1> validity{0b00111011};
    proto::indexcgo::BuildIndexInfo info;
    info.set_collectionid(1);
    info.set_partitionid(2);
    info.set_segmentid(3);
    info.set_buildid(780078);
    info.set_index_version(1);
    info.set_num_rows(values.size());
    info.set_current_scalar_index_version(GetParam());
    auto* field = info.mutable_field_schema();
    field->set_fieldid(101);
    field->set_name("values");
    field->set_data_type(proto::schema::DataType::VarChar);
    field->set_nullable(true);
    auto* param = info.add_index_params();
    param->set_key(index::INDEX_TYPE);
    param->set_value(index::FMINDEX_INDEX_TYPE);
    info.mutable_storage_config()->set_storage_type("local");
    info.mutable_storage_config()->set_root_path(root->Path());

    auto prepared = AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
    auto data = storage::CreateFieldData(
        DataType::VARCHAR, DataType::NONE, true, 1, values.size());
    data->FillFieldData(values.data(), validity.data(), values.size(), 0);
    auto payload = std::make_shared<storage::PayloadReader>(data);
    storage::InsertData insert(payload);
    insert.SetFieldDataMeta(prepared.file_manager_context.fieldDataMeta);
    insert.SetTimestamps(0, 100);
    auto bytes = insert.Serialize(storage::Remote);
    const auto binlog_path = root->Path() + "/insert/1";
    prepared.file_manager_context.chunkManagerPtr->Write(
        binlog_path, bytes.data(), bytes.size());
    info.add_insert_files(binlog_path);

    prepared = AdaptBuildIndexInfo(info, BuildPurpose::ScalarIndex);
    const auto params = prepared.request.params;
    const auto context = prepared.file_manager_context;
    index::IIndexReaderBasePtr reader;
    {
        prepared.request.staging_parent = root->Path() + "/build";
        BuildSession session(std::move(prepared.request), context);
        session.BuildFromSource();
        const auto stats = session.Publish();
        ASSERT_EQ(stats.Files().size(), 1);
        EXPECT_GT(stats.MemSize(), 0);
        EXPECT_EQ(
            std::filesystem::path(stats.Files()[0].file_name).filename(),
            index::PackedScalarIndexFileName(index::ScalarIndexType::FMINDEX));
        auto source = index::InspectPackedIndexFile(
            {stats.Files()[0].file_name}, context);
        ASSERT_NE(source, nullptr);
        EXPECT_TRUE(source->Directory().HasEntry("fm_index.bin"));
        EXPECT_TRUE(source->Directory().HasEntry("fm_index_null_bitmap"));
        storage::LoadOptions options;
        options.params = params;
        reader = index::LoaderRegistry::Instance()
                     .Lookup(index::families::kFmIndex)
                     .Load({index::OpenedIndexSource{index::PackedIndexSource{
                                std::shared_ptr<storage::AsyncIndexEntryReader>(
                                    std::move(source))}},
                            std::move(options)});
    }
    // Query after destroying the build session and its retained artifact.
    ASSERT_NE(reader, nullptr);
    ASSERT_EQ(reader->Count(), values.size());
    const auto* patterns =
        dynamic_cast<const index::IPatternMatchReader*>(reader.get());
    const auto* nulls = dynamic_cast<const index::INullReader*>(reader.get());
    ASSERT_NE(patterns, nullptr);
    ASSERT_NE(nulls, nullptr);
    const auto prefix =
        patterns->PatternMatch("alp", index::PatternOp::PrefixMatch);
    const auto inner =
        patterns->PatternMatch("ha", index::PatternOp::InnerMatch);
    const auto suffix =
        patterns->PatternMatch("ta", index::PatternOp::PostfixMatch);
    const auto is_null = nulls->IsNull();
    const auto is_not_null = nulls->IsNotNull();
    ASSERT_EQ(prefix.size(), values.size());
    ASSERT_EQ(inner.size(), values.size());
    ASSERT_EQ(suffix.size(), values.size());
    ASSERT_EQ(is_null.size(), values.size());
    ASSERT_EQ(is_not_null.size(), values.size());
    for (size_t row = 0; row < values.size(); ++row) {
        const bool valid = row != 2;
        EXPECT_EQ(prefix[row], valid && values[row].starts_with("alp")) << row;
        EXPECT_EQ(inner[row],
                  valid && values[row].find("ha") != std::string::npos)
            << row;
        EXPECT_EQ(suffix[row], valid && values[row].ends_with("ta")) << row;
        EXPECT_EQ(is_null[row], !valid) << row;
        EXPECT_EQ(is_not_null[row], valid) << row;
    }
}

INSTANTIATE_TEST_SUITE_P(ScalarVersionsZeroToTwo,
                         FmLowVersionBuildTest,
                         ::testing::Values(0, 1, 2));

}  // namespace
}  // namespace milvus::indexbuilder
