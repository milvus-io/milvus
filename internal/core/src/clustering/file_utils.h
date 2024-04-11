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

#include "common/type_c.h"
#include <future>
#include "storage/ThreadPools.h"

#include "common/FieldData.h"
#include "common/LoadInfo.h"
#include "knowhere/comp/index_param.h"
#include "parquet/schema.h"
#include "storage/PayloadStream.h"
#include "storage/FileManager.h"
#include "storage/BinlogReader.h"
#include "storage/ChunkManager.h"
#include "storage/DataCodec.h"
#include "storage/Types.h"
#include "storage/space.h"

namespace milvus::clustering {

// put clustering result data to remote
// similar with PutIndexData, but do not need to encode, directly use PB format
// leave the ownership to golang side
void
PutClusteringResultData(milvus::storage::ChunkManager* remote_chunk_manager,
                        const std::vector<const uint8_t*>& data_slices,
                        const std::vector<int64_t>& slice_sizes,
                        const std::vector<std::string>& slice_names,
                        std::unordered_map<std::string, int64_t>& map) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<std::pair<std::string, int64_t>>> futures;
    AssertInfo(data_slices.size() == slice_sizes.size(),
               "inconsistent data slices size {} with slice sizes {}",
               data_slices.size(),
               slice_sizes.size());
    AssertInfo(data_slices.size() == slice_names.size(),
               "inconsistent data slices size {} with slice names size {}",
               data_slices.size(),
               slice_names.size());

    for (int64_t i = 0; i < data_slices.size(); ++i) {
        futures.push_back(pool.Submit(
            [&](milvus::storage::ChunkManager* chunk_manager,
                uint8_t* buf,
                int64_t batch_size,
                std::string object_key) -> std::pair<std::string, int64_t> {
                chunk_manager->Write(object_key, buf, batch_size);
                return std::make_pair(object_key, batch_size);
            },
            remote_chunk_manager,
            const_cast<uint8_t*>(data_slices[i]),
            slice_sizes[i],
            slice_names[i]));
    }

    for (auto& future : futures) {
        auto res = future.get();
        map[res.first] = res.second;
    }
}

void
AddClusteringResultFiles(milvus::storage::ChunkManager* remote_chunk_manager,
                         const BinarySet& binary_set,
                         const std::string& remote_prefix,
                         std::unordered_map<std::string, int64_t>& map) {
    std::vector<const uint8_t*> data_slices;
    std::vector<int64_t> slice_sizes;
    std::vector<std::string> slice_names;

    auto AddBatchClusteringFiles = [&]() {
        PutClusteringResultData(
            remote_chunk_manager, data_slices, slice_sizes, slice_names, map);
    };

    int64_t batch_size = 0;
    for (auto iter = binary_set.binary_map_.begin();
         iter != binary_set.binary_map_.end();
         iter++) {
        if (batch_size >= DEFAULT_FIELD_MAX_MEMORY_LIMIT) {
            AddBatchClusteringFiles();
            data_slices.clear();
            slice_sizes.clear();
            slice_names.clear();
            batch_size = 0;
        }

        data_slices.emplace_back(iter->second->data.get());
        slice_sizes.emplace_back(iter->second->size);
        slice_names.emplace_back(remote_prefix + "/" + iter->first);
        batch_size += iter->second->size;
    }

    if (data_slices.size() > 0) {
        AddBatchClusteringFiles();
    }
}
}  // namespace milvus::clustering