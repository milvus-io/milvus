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

#include "db/wal/WalManager.h"
#include "config/ServerConfig.h"
#include "db/Utils.h"

namespace milvus {
namespace engine {

WalManager::WalManager() {
    wal_path_ = config.wal.path();
    wal_buffer_size_ = config.wal.buffer_size();
    insert_buffer_size_ = config.cache.insert_buffer_size();
}

WalManager&
WalManager::GetInstance() {
    static WalManager s_mgr;
    return s_mgr;
}

Status
WalManager::RecordOperation(const WalOperationPtr& operation, const DBPtr& db) {
    if (operation == nullptr) {
        return Status(DB_ERROR, "Wal operation is null pointer");
    }

    Status status;
    switch (operation->Type()) {
        case WalOperationType::INSERT_ENTITY: {
            InsertEntityOperationPtr op = std::static_pointer_cast<InsertEntityOperation>(operation);
            status = RecordInsertOperation(op, db);
            break;
        }
        case WalOperationType::DELETE_ENTITY: {
            DeleteEntityOperationPtr op = std::static_pointer_cast<DeleteEntityOperation>(operation);
            status = RecordDeleteOperation(op, db);
            break;
        }
        default:
            break;
    }

    return Status::OK();
}

Status
WalManager::RecordInsertOperation(const InsertEntityOperationPtr& operation, const DBPtr& db) {
    std::vector<DataChunkPtr> chunks;
    SplitChunk(operation->data_chunk_, chunks);

    return Status::OK();
}

Status
WalManager::SplitChunk(const DataChunkPtr& chunk, std::vector<DataChunkPtr>& chunks) {
    int64_t chunk_size = utils::GetSizeOfChunk(chunk);
    if (chunk_size > insert_buffer_size_) {
    } else {
        chunks.push_back(chunk);
    }

    return Status::OK();
}

Status
WalManager::RecordDeleteOperation(const DeleteEntityOperationPtr& operation, const DBPtr& db) {
    return Status::OK();
}

Status
WalManager::OperationDone(id_t op_id) {
    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
