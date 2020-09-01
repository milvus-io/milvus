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

#include "db/wal/WalProxy.h"
#include "config/ServerConfig.h"
#include "db/SnapshotUtils.h"
#include "db/Utils.h"
#include "db/wal/WalManager.h"
#include "db/wal/WalOperation.h"
#include "utils/Exception.h"

namespace milvus {
namespace engine {

WalProxy::WalProxy(const DBPtr& db, const DBOptions& options) : DBProxy(db, options) {
    // db must implemented
    if (db == nullptr) {
        throw Exception(DB_ERROR, "null pointer");
    }
}

Status
WalProxy::Start() {
    // let service start
    auto status = db_->Start();
    if (!status.ok()) {
        return status;
    }

    if (options_.wal_enable_) {
        WalManager::GetInstance().Start(options_);
        WalManager::GetInstance().Recovery(db_);
    }

    return status;
}

Status
WalProxy::Stop() {
    auto status = db_->Stop();

    if (options_.wal_enable_) {
        WalManager::GetInstance().Stop();
    }

    return status;
}

Status
WalProxy::DropCollection(const std::string& collection_name) {
    WalManager::GetInstance().DropCollection(collection_name);
    return db_->DropCollection(collection_name);
}

Status
WalProxy::Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk,
                 idx_t op_id) {
    // get segment row count of this collection
    int64_t row_count_per_segment = DEFAULT_SEGMENT_ROW_COUNT;
    GetSegmentRowCount(collection_name, row_count_per_segment);

    // split chunk accordding to segment row count
    std::vector<DataChunkPtr> chunks;
    STATUS_CHECK(utils::SplitChunk(data_chunk, row_count_per_segment, chunks));
    if (chunks.size() > 0 && data_chunk != chunks[0]) {
        // data has been copied to new chunk, do this to free memory
        data_chunk->fixed_fields_.clear();
        data_chunk->variable_fields_.clear();
        data_chunk->count_ = 0;
    }

    // write operation into wal file, and insert to memory
    for (auto& chunk : chunks) {
        InsertEntityOperationPtr op = std::make_shared<InsertEntityOperation>();
        op->collection_name_ = collection_name;
        op->partition_name = partition_name;
        op->data_chunk_ = chunk;
        STATUS_CHECK(WalManager::GetInstance().RecordOperation(op, db_));
    }

    // return id field
    if (chunks.size() > 0 && data_chunk != chunks[0]) {
        int64_t row_count = 0;
        BinaryDataPtr id_data = std::make_shared<BinaryData>();
        for (auto& chunk : chunks) {
            auto iter = chunk->fixed_fields_.find(engine::FIELD_UID);
            if (iter != chunk->fixed_fields_.end()) {
                id_data->data_.insert(id_data->data_.end(), iter->second->data_.begin(), iter->second->data_.end());
                row_count += chunk->count_;
            }
        }
        data_chunk->count_ = row_count;
        data_chunk->fixed_fields_[engine::FIELD_UID] = id_data;
    }

    return Status::OK();
}

Status
WalProxy::DeleteEntityByID(const std::string& collection_name, const IDNumbers& entity_ids, idx_t op_id) {
    // write operation into disk
    DeleteEntityOperationPtr op = std::make_shared<DeleteEntityOperation>();
    op->collection_name_ = collection_name;
    op->entity_ids_ = entity_ids;

    return WalManager::GetInstance().RecordOperation(op, db_);
}

}  // namespace engine
}  // namespace milvus
