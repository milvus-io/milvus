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
#include "db/wal/WalFile.h"
#include "db/wal/WalOperation.h"
#include "utils/Status.h"
#include "utils/ThreadPool.h"

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace milvus {
namespace engine {

extern const char* WAL_MAX_OP_FILE_NAME;
extern const char* WAL_DEL_FILE_NAME;

using CollectionMaxOpIDMap = std::unordered_map<std::string, idx_t>;

class WalManager {
 public:
    static WalManager&
    GetInstance();

    Status
    Start(const DBOptions& options);

    Status
    Stop();

    Status
    DropCollection(const std::string& collection_name);

    Status
    RecordOperation(const WalOperationPtr& operation, const DBPtr& db);

    Status
    OperationDone(const std::string& collection_name, idx_t op_id);

    // max_op_ids is from meta system, wal also record a max id for each collection
    // compare the two max id, use the max one as recovery base
    Status
    Recovery(const DBPtr& db, const CollectionMaxOpIDMap& max_op_ids);

 private:
    WalManager();

    Status
    Init();

    Status
    RecordInsertOperation(const InsertEntityOperationPtr& operation, const DBPtr& db);

    Status
    RecordDeleteOperation(const DeleteEntityOperationPtr& operation, const DBPtr& db);

    std::string
    ConstructFilePath(const std::string& collection_name, const std::string& file_name);

    void
    AddCleanupTask(const std::string& collection_name);

    void
    TakeCleanupTask(std::string& collection_name);

    void
    StartCleanupThread();

    void
    WaitCleanupFinish();

    void
    CleanupThread();

    Status
    PerformOperation(const WalOperationPtr& operation, const DBPtr& db);

 private:
    SafeIDGenerator id_gen_;

    bool enable_ = false;
    std::string wal_path_;
    int64_t insert_buffer_size_ = 0;

    using WalFileMap = std::unordered_map<std::string, WalFilePtr>;
    WalFileMap file_map_;  // mapping collection name to file
    std::mutex file_map_mutex_;

    using MaxOpIdMap = std::unordered_map<std::string, idx_t>;
    MaxOpIdMap max_op_id_map_;  // mapping collection name to max operation id
    std::mutex max_op_mutex_;

    ThreadPool cleanup_thread_pool_;
    std::mutex cleanup_thread_mutex_;
    std::list<std::future<void>> cleanup_thread_results_;

    std::list<std::string> cleanup_tasks_;  // cleanup target collections
    std::mutex cleanup_task_mutex_;
};

}  // namespace engine
}  // namespace milvus
