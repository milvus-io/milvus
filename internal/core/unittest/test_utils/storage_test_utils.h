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
#include "common/Types.h"
#include "common/LoadInfo.h"
#include "storage/Types.h"
#include "storage/InsertData.h"
#include "storage/ThreadPools.h"
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

inline LoadFieldDataInfo
PrepareInsertBinlog(int64_t collection_id,
                    int64_t partition_id,
                    int64_t segment_id,
                    const std::string& prefix,
                    const GeneratedData& dataset,
                    const ChunkManagerPtr cm) {
    LoadFieldDataInfo load_info;
    auto row_count = dataset.row_ids_.size();

    auto SaveFieldData = [&](const FieldDataPtr field_data,
                             const std::string& file,
                             const int64_t field_id) {
        auto insert_data = std::make_shared<InsertData>(field_data);
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
                            false,
                            std::vector<std::string>{file}});
    };

    {
        auto field_data = std::make_shared<milvus::FieldData<int64_t>>(
            DataType::INT64, false);
        field_data->FillFieldData(dataset.row_ids_.data(), row_count);
        auto path = prefix + "/" + std::to_string(RowFieldID.get());
        SaveFieldData(field_data, path, RowFieldID.get());
    }
    {
        auto field_data = std::make_shared<milvus::FieldData<int64_t>>(
            DataType::INT64, false);
        field_data->FillFieldData(dataset.timestamps_.data(), row_count);
        auto path = prefix + "/" + std::to_string(TimestampFieldID.get());
        SaveFieldData(field_data, path, TimestampFieldID.get());
    }
    auto fields = dataset.schema_->get_fields();
    for (auto& data : dataset.raw_->fields_data()) {
        int64_t field_id = data.field_id();
        auto field_meta = fields.at(FieldId(field_id));
        auto field_data = milvus::segcore::CreateFieldDataFromDataArray(
            row_count, &data, field_meta);
        auto path = prefix + "/" + std::to_string(field_id);
        SaveFieldData(field_data, path, field_id);
    }

    return load_info;
}

std::map<std::string, int64_t>
PutFieldData(milvus::storage::ChunkManager* remote_chunk_manager,
             const std::vector<void*>& buffers,
             const std::vector<int64_t>& element_counts,
             const std::vector<std::string>& object_keys,
             FieldDataMeta& field_data_meta,
             milvus::FieldMeta& field_meta) {
    auto& pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<std::pair<std::string, size_t>>> futures;
    AssertInfo(buffers.size() == element_counts.size(),
               "inconsistent size of data slices with slice sizes!");
    AssertInfo(buffers.size() == object_keys.size(),
               "inconsistent size of data slices with slice names!");

    for (int64_t i = 0; i < buffers.size(); ++i) {
        futures.push_back(
            pool.Submit(milvus::storage::EncodeAndUploadFieldSlice,
                        remote_chunk_manager,
                        buffers[i],
                        element_counts[i],
                        field_data_meta,
                        field_meta,
                        object_keys[i]));
    }

    std::map<std::string, int64_t> remote_paths_to_size;
    for (auto& future : futures) {
        auto res = future.get();
        remote_paths_to_size[res.first] = res.second;
    }

    milvus::storage::ReleaseArrowUnused();
    return remote_paths_to_size;
}

auto
gen_field_meta(int64_t collection_id = 1,
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
