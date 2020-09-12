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

class WalManager {
 public:
<<<<<<< HEAD
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

    Status
    Recovery(const DBPtr& db);

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

=======
    explicit WalManager(const MXLogConfiguration& config);
    ~WalManager();

    /*
     * init
     * @param meta
     * @retval error_code
     */
    ErrorCode
    Init(const meta::MetaPtr& meta);

    /*
     * Get next recovery
     * @param record[out]: record
     * @retval error_code
     */
    ErrorCode
    GetNextRecovery(MXLogRecord& record);

    ErrorCode
    GetNextEntityRecovery(MXLogRecord& record);

    /*
     * Get next record
     * @param record[out]: record
     * @retval error_code
     */
    ErrorCode
    GetNextRecord(MXLogRecord& record);

    ErrorCode
    GetNextEntityRecord(MXLogRecord& record);

    /*
     * Create collection
     * @param collection_id: collection id
     * @retval lsn
     */
    uint64_t
    CreateCollection(const std::string& collection_id);

    /*
     * Create partition
     * @param collection_id: collection id
     * @param partition_tag: partition tag
     * @retval lsn
     */
    uint64_t
    CreatePartition(const std::string& collection_id, const std::string& partition_tag);

    /*
     * Create hybrid collection
     * @param collection_id: collection id
     * @retval lsn
     */
    uint64_t
    CreateHybridCollection(const std::string& collection_id);

    /*
     * Drop collection
     * @param collection_id: collection id
     * @retval none
     */
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    void
    StartCleanupThread();

<<<<<<< HEAD
    void
    WaitCleanupFinish();
=======
    /*
     * Drop partition
     * @param collection_id: collection id
     * @param partition_tag: partition tag
     * @retval none
     */
    void
    DropPartition(const std::string& collection_id, const std::string& partition_tag);

    /*
     * Collection is flushed (update flushed_lsn)
     * @param collection_id: collection id
     * @param lsn: flushed lsn
     */
    void
    CollectionFlushed(const std::string& collection_id, uint64_t lsn);

    /*
     * Partition is flushed (update flushed_lsn)
     * @param collection_id: collection id
     * @param partition_tag: partition_tag
     * @param lsn: flushed lsn
     */
    void
    PartitionFlushed(const std::string& collection_id, const std::string& partition_tag, uint64_t lsn);

    /*
     * Collection is updated (update wal_lsn)
     * @param collection_id: collection id
     * @param partition_tag: partition_tag
     * @param lsn: flushed lsn
     */
    void
    CollectionUpdated(const std::string& collection_id, uint64_t lsn);

    /*
     * Partition is updated (update wal_lsn)
     * @param collection_id: collection id
     * @param partition_tag: partition_tag
     * @param lsn: flushed lsn
     */
    void
    PartitionUpdated(const std::string& collection_id, const std::string& partition_tag, uint64_t lsn);

    /*
     * Insert
     * @param collection_id: collection id
     * @param collection_id: partition tag
     * @param vector_ids: vector ids
     * @param vectors: vectors
     */
    template <typename T>
    bool
    Insert(const std::string& collection_id, const std::string& partition_tag, const IDNumbers& vector_ids,
           const std::vector<T>& vectors);

    /*
     * Insert
     * @param collection_id: collection id
     * @param partition_tag: partition tag
     * @param vector_ids: vector ids
     * @param vectors: vectors
     * @param attrs: attributes
     */
    template <typename T>
    bool
    InsertEntities(const std::string& collection_id, const std::string& partition_tag,
                   const milvus::engine::IDNumbers& entity_ids, const std::vector<T>& vectors,
                   const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                   const std::unordered_map<std::string, std::vector<uint8_t>>& attrs);

    /*
     * Insert
     * @param collection_id: collection id
     * @param vector_ids: vector ids
     */
    bool
    DeleteById(const std::string& collection_id, const IDNumbers& vector_ids);

    /*
     * Get flush lsn
     * @param collection_id: collection id (empty means all tables)
     * @retval if there is something not flushed, return lsn;
     *         else, return 0
     */
    uint64_t
    Flush(const std::string& collection_id = "");
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    void
    CleanupThread();

    Status
    PerformOperation(const WalOperationPtr& operation, const DBPtr& db);

 private:
<<<<<<< HEAD
    SafeIDGenerator id_gen_;

    bool enable_ = false;
    std::string wal_path_;
    int64_t insert_buffer_size_ = 0;
=======
    WalManager
    operator=(WalManager&);

    MXLogConfiguration mxlog_config_;

    MXLogBufferPtr p_buffer_;
    MXLogMetaHandlerPtr p_meta_handler_;

    struct TableLsn {
        uint64_t flush_lsn;
        uint64_t wal_lsn;
    };
    std::mutex mutex_;
    std::map<std::string, std::map<std::string, TableLsn>> collections_;
    std::atomic<uint64_t> last_applied_lsn_;

    // if multi-thread call Flush(), use list
    struct FlushInfo {
        std::string collection_id_;
        uint64_t lsn_ = 0;

        bool
        IsValid() {
            return (lsn_ != 0);
        }
        void
        Clear() {
            lsn_ = 0;
        }
    };
    FlushInfo flush_info_;
};
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

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
