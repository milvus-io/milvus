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

#pragma once

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "indexbuilder/BuildSession.h"
#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "knowhere/version.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"
#include "test_utils/Constants.h"

namespace milvus::indexbuilder::test {

// Source-backed integration fixtures use the production adapter, materializer,
// publication and loader. Each test owns its remote files and staging parent.
class SourceBuildTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        directory_ = storage::LocalDirectory::CreateOwned(
            TestRemotePath, "source_build_XXXXXX", "source build test");
    }

    PreparedBuild
    Prepare(DataType field_type,
            DataType element_type,
            bool nullable,
            const std::string& index_type,
            int64_t row_count,
            int64_t dim,
            const Config& params = Config::object(),
            int32_t scalar_version = 3) const {
        proto::indexcgo::BuildIndexInfo info;
        info.set_collectionid(1);
        info.set_partitionid(2);
        info.set_segmentid(3);
        info.set_buildid(1000);
        info.set_index_version(1);
        info.set_num_rows(row_count);
        info.set_dim(dim);
        info.set_current_scalar_index_version(scalar_version);
        info.set_current_index_version(
            knowhere::Version::GetCurrentVersion().VersionNumber());
        auto* schema = info.mutable_field_schema();
        schema->set_fieldid(100);
        schema->set_name("values");
        schema->set_data_type(static_cast<proto::schema::DataType>(field_type));
        schema->set_element_type(
            static_cast<proto::schema::DataType>(element_type));
        schema->set_nullable(nullable);
        auto* family = info.add_index_params();
        family->set_key(index::INDEX_TYPE);
        family->set_value(index_type);
        for (const auto& [key, value] : params.items()) {
            if (key == index::INDEX_TYPE) {
                continue;
            }
            auto* param = info.add_index_params();
            param->set_key(key);
            param->set_value(value.is_string() ? value.get<std::string>()
                                               : value.dump());
        }
        info.add_insert_files(directory_->Path() + "/insert/1");
        auto* config = info.mutable_storage_config();
        config->set_storage_type("local");
        config->set_root_path(directory_->Path());
        auto prepared = AdaptBuildIndexInfo(info,
                                            IsVectorDataType(field_type)
                                                ? BuildPurpose::VectorIndex
                                                : BuildPurpose::ScalarIndex);
        prepared.request.staging_parent = directory_->Path();
        // Loaders require schema-derived runtime values, independently of the
        // private normalization performed inside BuildSession.
        prepared.request.params["nullable"] = nullable;
        prepared.request.params["num_rows"] = row_count;
        return prepared;
    }

    void
    WriteInsert(const PreparedBuild& prepared, const FieldDataPtr& data) const {
        auto payload = std::make_shared<storage::PayloadReader>(data);
        storage::InsertData insert(payload);
        insert.SetFieldDataMeta(prepared.file_manager_context.fieldDataMeta);
        insert.SetTimestamps(0, 100);
        auto bytes = insert.Serialize(storage::StorageType::Remote);
        const auto& files =
            std::get<V1BinlogBuildSource>(prepared.request.source).files;
        AssertInfo(files.size() == 1, "fixture requires one insert binlog");
        prepared.file_manager_context.chunkManagerPtr->Write(
            files.front(), bytes.data(), bytes.size());
    }

    storage::ArtifactStats
    Publish(const PreparedBuild& prepared) const {
        BuildSession session(prepared.request, prepared.file_manager_context);
        session.BuildFromSource();
        // The session and its materialized input die before the loader opens.
        return session.Publish();
    }

    index::IIndexReaderBasePtr
    Open(const PreparedBuild& prepared,
         const storage::ArtifactStats& stats,
         bool mmap = false,
         std::vector<std::string> paths = {}) const {
        if (paths.empty()) {
            paths.reserve(stats.Files().size());
            for (const auto& file : stats.Files()) {
                paths.push_back(file.file_name);
            }
        }
        storage::LoadOptions options;
        options.params = prepared.request.params;
        options.enable_mmap = mmap;
        options.mmap_dir_path = directory_->Path();
        auto context = prepared.file_manager_context;
        context.set_for_loading_index(true);
        const auto loader =
            index::LoaderRegistry::Instance().Lookup(prepared.request.family);
        AssertInfo(static_cast<bool>(loader), "fixture family has no loader");
        if (prepared.request.output.generation == storage::Generation::V3) {
            return loader.Load(
                {index::IndexFiles{
                     std::move(context),
                     std::move(paths),
                     index::PackedIndexStorageConfig{
                         prepared.request.output.storage_namespace}},
                 std::move(options)});
        }
        return loader.Load(
            {index::IndexFiles{
                 std::move(context),
                 std::move(paths),
                 index::LegacyIndexStorageConfig{
                     prepared.request.family == index::families::kVectorDisk
                         ? storage::V1SourceLayout::DiskFiles
                         : storage::V1SourceLayout::MemoryEntries,
                     prepared.request.output.storage_namespace}},
             std::move(options)});
    }

    std::shared_ptr<storage::LocalDirectory> directory_;
};

}  // namespace milvus::indexbuilder::test
