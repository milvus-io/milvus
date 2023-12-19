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

using milvus::DataType;
using milvus::storage::FieldDataMeta;
using milvus::FieldDataPtr;
using milvus::FieldId;
using milvus::segcore::GeneratedData;
using milvus::storage::ChunkManagerPtr;
using milvus::storage::InsertData;
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
        auto field_data =
            std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64);
        field_data->FillFieldData(dataset.row_ids_.data(), row_count);
        auto path = prefix + "/" + std::to_string(RowFieldID.get());
        SaveFieldData(field_data, path, RowFieldID.get());
    }
    {
        auto field_data =
            std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64);
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
             const std::vector<const uint8_t*>& buffers,
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
                        const_cast<uint8_t*>(buffers[i]),
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

}  // namespace
