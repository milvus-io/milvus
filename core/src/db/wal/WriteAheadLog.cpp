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

#include "db/wal/WriteAheadLog.h"

#include "utils/Exception.h"

namespace milvus {
namespace engine {

WriteAheadLog::WriteAheadLog(const DBPtr& db) : DBProxy(db) {
    // db must implemented
    if (db == nullptr) {
        throw Exception(DB_ERROR, "null pointer");
    }
}

Status
WriteAheadLog::Insert(const std::string& collection_name, const std::string& partition_name, DataChunkPtr& data_chunk) {
    return db_->Insert(collection_name, partition_name, data_chunk);
}

Status
WriteAheadLog::DeleteEntityByID(const std::string& collection_name, const engine::IDNumbers& entity_ids) {
    return db_->DeleteEntityByID(collection_name, entity_ids);
}

Status
WriteAheadLog::Flush(const std::string& collection_name) {
    return db_->Flush(collection_name);
}

Status
WriteAheadLog::Flush() {
    return db_->Flush();
}

}  // namespace engine
}  // namespace milvus
