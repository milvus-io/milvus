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

#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/Consts.h"
#include "common/FieldData.h"
#include "index/Meta.h"
#include "indexbuilder/BuildSession.h"
#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "storage/InsertData.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/PayloadReader.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::storage::text_test {

class TextPublicationFixture {
 public:
    TextPublicationFixture()
        : root(LocalDirectory::CreateOwned(
              std::filesystem::temp_directory_path().string(),
              "text-publication-XXXXXX", "text publication test")) {
    }

    ArtifactStats
    Publish(bool packed) {
        proto::indexcgo::BuildIndexInfo info;
        info.set_collectionid(1);
        info.set_partitionid(2);
        info.set_segmentid(3);
        info.set_buildid(1000);
        info.set_index_version(1);
        info.set_num_rows(3);
        info.set_current_scalar_index_version(packed ? 3 : 1);
        auto* field = info.mutable_field_schema();
        field->set_fieldid(101);
        field->set_name("text");
        field->set_data_type(proto::schema::DataType::VarChar);
        field->set_nullable(true);
        auto* max_length = field->add_type_params();
        max_length->set_key(MAX_LENGTH);
        max_length->set_value("64");
        auto* enable_analyzer = field->add_type_params();
        enable_analyzer->set_key("enable_analyzer");
        enable_analyzer->set_value("true");
        auto* param = info.add_index_params();
        param->set_key(index::INDEX_TYPE);
        param->set_value(index::INVERTED_INDEX_TYPE);
        info.mutable_storage_config()->set_storage_type("local");
        info.mutable_storage_config()->set_root_path(root->Path() + "/remote");
        const auto source = root->Path() + "/insert/1";
        info.add_insert_files(source);
        auto prepared = indexbuilder::AdaptBuildIndexInfo(info, indexbuilder::BuildPurpose::TextIndex);
        context = prepared.file_manager_context;
        prepared.request.staging_parent = root->Path() + "/build";
        const std::vector<std::string> values{"alpha", "", "beta"};
        constexpr uint8_t validity = 0b00000101;
        auto data = CreateFieldData(DataType::VARCHAR, DataType::NONE, true, 1, values.size());
        data->FillFieldData(values.data(), &validity, values.size(), 0);
        auto payload_reader = std::make_shared<PayloadReader>(data);
        InsertData insert(payload_reader);
        insert.SetFieldDataMeta(context.fieldDataMeta);
        insert.SetTimestamps(0, 100);
        auto bytes = insert.Serialize(Remote);
        context.chunkManagerPtr->Write(source, bytes.data(), bytes.size());
        indexbuilder::BuildSession session(std::move(prepared.request), context);
        session.BuildFromSource();
        return session.Publish();
    }

    Config
    LoadConfig(const ArtifactStats& stats, bool mmap) const {
        std::vector<std::string> paths;
        paths.reserve(stats.Files().size());
        for (const auto& file : stats.Files()) paths.push_back(file.file_name);
        MemFileManagerImpl manager(context);
        return {{index::INDEX_FILES, paths},
                {STATS_BASE_PATH_KEY, manager.GetRemoteTextLogPrefix()},
                {index::ENABLE_MMAP, mmap},
                {index::MMAP_FILE_PATH, root->Path() + "/loaded"}};
    }

    std::shared_ptr<LocalDirectory> root;
    FileManagerContext context;
};

}  // namespace milvus::storage::text_test
