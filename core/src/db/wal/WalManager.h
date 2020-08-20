// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "config/ServerConfig.h"
#include "db/DB.h"
#include "db/IDGenerator.h"
#include "db/Types.h"
#include "db/wal/WalOperation.h"
#include "utils/Status.h"

#include <string>
#include <vector>

namespace milvus {
namespace engine {

class WalManager {
 public:
    WalManager() = default;

    static WalManager&
    GetInstance();

    void
    SetWalPath(const std::string& path) {
        wal_path_ = path;
    }

    Status
    RecordOperation(const WalOperationPtr& operation, const DBPtr& db);

    Status
    OperationDone(id_t op_id);

 private:
    Status
    RecordInsertOperation(const InsertEntityOperationPtr& operation, const DBPtr& db);

    Status
    RecordDeleteOperation(const DeleteEntityOperationPtr& operation, const DBPtr& db);

    Status
    SplitChunk(const DataChunkPtr& chunk, std::vector<DataChunkPtr>& chunks);

 private:
    SafeIDGenerator id_gen_;

    std::string wal_path_ = config.wal.path();
    int64_t wal_buffer_size_ = config.wal.buffer_size();
    int64_t insert_buffer_size_ = config.cache.insert_buffer_size();
};

}  // namespace engine
}  // namespace milvus
