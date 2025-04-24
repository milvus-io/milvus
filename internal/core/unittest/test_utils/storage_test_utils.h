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

#include "Constants.h"
#include "DataGen.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/Types.h"
#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "segcore/segment_c.h"
#include "storage/Types.h"
#include "storage/InsertData.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "segcore/SegmentSealed.h"
#include <boost/filesystem.hpp>

using milvus::DataType;
using milvus::FieldDataPtr;
using milvus::FieldId;
using milvus::segcore::GeneratedData;
using milvus::storage::ChunkManagerPtr;
using milvus::storage::FieldDataMeta;
using milvus::storage::InsertData;
using milvus::storage::MmapConfig;
using milvus::storage::StorageConfig;

namespace {

// test remote chunk manager with local disk
inline StorageConfig
get_default_local_storage_config() {
    StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestRemotePath;
    return storage_config;
}

inline MmapConfig
get_default_mmap_config() {
    MmapConfig mmap_config = {
        .cache_read_ahead_policy = "willneed",
        .mmap_path = "/tmp/test_mmap_manager/",
        .disk_limit =
            uint64_t(2) * uint64_t(1024) * uint64_t(1024) * uint64_t(1024),
        .fix_file_size = uint64_t(4) * uint64_t(1024) * uint64_t(1024),
        .growing_enable_mmap = false,
    };
    return mmap_config;
}

// This function uploads generated data into cm, and returns a load info that
// can be used by a segment to load the data.
inline LoadFieldDataInfo
PrepareInsertBinlog(int64_t collection_id,
                    int64_t partition_id,
                    int64_t segment_id,
                    const GeneratedData& dataset,
                    const ChunkManagerPtr cm,
                    std::string mmap_dir_path = "",
                    std::vector<int64_t> excluded_field_ids = {}) {
    bool enable_mmap = !mmap_dir_path.empty();
    LoadFieldDataInfo load_info;
    load_info.mmap_dir_path = mmap_dir_path;
    auto row_count = dataset.row_ids_.size();
    const std::string prefix = TestRemotePath;

    auto SaveFieldData = [&](const FieldDataPtr field_data,
                             const std::string& file,
                             const int64_t field_id) {
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        auto insert_data = std::make_shared<InsertData>(payload_reader);
        FieldDataMeta field_data_meta{
            collection_id, partition_id, segment_id, field_id};
        insert_data->SetFieldDataMeta(field_data_meta);
        auto serialized_insert_data = insert_data->serialize_to_remote_file();
        auto serialized_insert_size = serialized_insert_data.size();
        cm->Write(file, serialized_insert_data.data(), serialized_insert_size);

        load_info.field_infos.emplace(
            field_id,
            FieldBinlogInfo{field_id,
                            static_cast<int64_t>(row_count),
                            std::vector<int64_t>{int64_t(row_count)},
                            enable_mmap,
                            std::vector<std::string>{file}});
    };

    auto field_excluded = [&](int64_t field_id) {
        return std::find(excluded_field_ids.begin(),
                         excluded_field_ids.end(),
                         field_id) != excluded_field_ids.end();
    };

    if (!field_excluded(RowFieldID.get())) {
        auto field_data = std::make_shared<milvus::FieldData<int64_t>>(
            DataType::INT64, false);
        field_data->FillFieldData(dataset.row_ids_.data(), row_count);
        auto path = prefix + std::to_string(RowFieldID.get());
        SaveFieldData(field_data, path, RowFieldID.get());
    }
    if (!field_excluded(TimestampFieldID.get())) {
        auto field_data = std::make_shared<milvus::FieldData<int64_t>>(
            DataType::INT64, false);
        field_data->FillFieldData(dataset.timestamps_.data(), row_count);
        auto path = prefix + std::to_string(TimestampFieldID.get());
        SaveFieldData(field_data, path, TimestampFieldID.get());
    }
    auto fields = dataset.schema_->get_fields();
    for (auto& data : dataset.raw_->fields_data()) {
        int64_t field_id = data.field_id();
        if (!field_excluded(field_id)) {
            auto field_meta = fields.at(FieldId(field_id));
            auto field_data = milvus::segcore::CreateFieldDataFromDataArray(
                row_count, &data, field_meta);
            auto path = prefix + std::to_string(field_id);
            SaveFieldData(field_data, path, field_id);
        }
    }

    return load_info;
}

inline LoadFieldDataInfo
PrepareSingleFieldInsertBinlog(int64_t collection_id,
                               int64_t partition_id,
                               int64_t segment_id,
                               int64_t field_id,
                               std::vector<FieldDataPtr> field_datas,
                               const ChunkManagerPtr cm,
                               const std::string& mmap_dir_path = "") {
    bool enable_mmap = !mmap_dir_path.empty();
    LoadFieldDataInfo load_info;
    load_info.mmap_dir_path = mmap_dir_path;
    std::vector<std::string> files;
    files.reserve(field_datas.size());
    std::vector<int64_t> row_counts;
    row_counts.reserve(field_datas.size());
    int64_t row_count = 0;
    for (auto i = 0; i < field_datas.size(); ++i) {
        auto& field_data = field_datas[i];
        row_count += field_data->Length();
        auto file =
            "./data/test" + std::to_string(field_id) + "/" + std::to_string(i);
        files.push_back(file);
        row_counts.push_back(field_data->Length());
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        auto insert_data = std::make_shared<InsertData>(payload_reader);
        FieldDataMeta field_data_meta{
            collection_id, partition_id, segment_id, field_id};
        insert_data->SetFieldDataMeta(field_data_meta);
        auto serialized_insert_data = insert_data->serialize_to_remote_file();
        auto serialized_insert_size = serialized_insert_data.size();
        cm->Write(file, serialized_insert_data.data(), serialized_insert_size);
    }

    load_info.field_infos.emplace(
        field_id,
        FieldBinlogInfo{field_id,
                        static_cast<int64_t>(row_count),
                        row_counts,
                        enable_mmap,
                        files});

    return load_info;
}

inline void
LoadGeneratedDataIntoSegment(const GeneratedData& dataset,
                             milvus::segcore::SegmentSealed* segment,
                             bool with_mmap = false,
                             std::vector<int64_t> excluded_field_ids = {}) {
    std::string mmap_dir_path = with_mmap ? "./data/mmap-test" : "";
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareInsertBinlog(kCollectionID,
                                         kPartitionID,
                                         kSegmentID,
                                         dataset,
                                         cm,
                                         mmap_dir_path,
                                         excluded_field_ids);
    auto status = LoadFieldData(segment, &load_info);
    AssertInfo(status.error_code == milvus::Success,
               "Failed to load field data, error: {}",
               status.error_msg);
}

inline std::unique_ptr<milvus::segcore::SegmentSealed>
CreateSealedWithFieldDataLoaded(milvus::SchemaPtr schema,
                                const GeneratedData& dataset,
                                bool with_mmap = false,
                                std::vector<int64_t> excluded_field_ids = {}) {
    auto segment =
        milvus::segcore::CreateSealedSegment(schema, milvus::empty_index_meta);
    LoadGeneratedDataIntoSegment(
        dataset, segment.get(), with_mmap, excluded_field_ids);
    return segment;
}

inline std::vector<int64_t>
GetExcludedFieldIds(milvus::SchemaPtr schema,
                    std::vector<int64_t> field_ids_to_load) {
    auto fields = schema->get_fields();
    std::vector<int64_t> result;
    for (auto& field : fields) {
        if (std::find(field_ids_to_load.begin(),
                      field_ids_to_load.end(),
                      field.first.get()) == field_ids_to_load.end()) {
            result.push_back(field.first.get());
        }
    }
    return result;
}
auto
gen_field_data_meta(int64_t collection_id = 1,
                    int64_t partition_id = 2,
                    int64_t segment_id = 3,
                    int64_t field_id = 101) -> milvus::storage::FieldDataMeta {
    return milvus::storage::FieldDataMeta{
        .collection_id = collection_id,
        .partition_id = partition_id,
        .segment_id = segment_id,
        .field_id = field_id,
    };
}

auto
gen_index_meta(int64_t segment_id = 3,
               int64_t field_id = 101,
               int64_t index_build_id = 1000,
               int64_t index_version = 10000) -> milvus::storage::IndexMeta {
    return milvus::storage::IndexMeta{
        .segment_id = segment_id,
        .field_id = field_id,
        .build_id = index_build_id,
        .index_version = index_version,
    };
}

auto
gen_local_storage_config(const std::string& root_path)
    -> milvus::storage::StorageConfig {
    auto ret = milvus::storage::StorageConfig{};
    ret.storage_type = "local";
    ret.root_path = root_path;
    return ret;
}

struct ChunkManagerWrapper {
    ChunkManagerWrapper(milvus::storage::ChunkManagerPtr cm) : cm_(cm) {
    }

    ~ChunkManagerWrapper() {
        for (const auto& file : written_) {
            cm_->Remove(file);
        }

        boost::filesystem::remove_all(cm_->GetRootPath());
    }

    void
    Write(const std::string& filepath, void* buf, uint64_t len) {
        written_.insert(filepath);
        cm_->Write(filepath, buf, len);
    }

    const milvus::storage::ChunkManagerPtr cm_;
    std::unordered_set<std::string> written_;
};

}  // namespace
