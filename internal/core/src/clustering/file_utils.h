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

namespace milvus::clustering {

void
AddClusteringResultFiles(milvus::storage::ChunkManager* remote_chunk_manager,
                         const uint8_t* data,
                         const int64_t data_size,
                         const std::string& remote_prefix,
                         std::unordered_map<std::string, int64_t>& map) {
    remote_chunk_manager->Write(
        remote_prefix, const_cast<uint8_t*>(data), data_size);
    map[remote_prefix] = data_size;
}

void
RemoveClusteringResultFiles(
    milvus::storage::ChunkManager* remote_chunk_manager,
    const std::unordered_map<std::string, int64_t>& map) {
    auto& pool = ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
    std::vector<std::future<void>> futures;

    for (auto& [file_path, file_size] : map) {
        futures.push_back(pool.Submit(
            [&, path = file_path]() { remote_chunk_manager->Remove(path); }));
    }
    std::exception_ptr first_exception = nullptr;
    for (auto& future : futures) {
        try {
            future.get();
        } catch (...) {
            if (!first_exception) {
                first_exception = std::current_exception();
            }
        }
    }
    if (first_exception) {
        std::rethrow_exception(first_exception);
    }
}

}  // namespace milvus::clustering
