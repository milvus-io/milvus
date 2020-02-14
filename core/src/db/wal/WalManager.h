// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <atomic>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "WalBuffer.h"
#include "WalDefinations.h"
#include "WalFileHandler.h"
#include "WalMetaHandler.h"
#include "utils/Error.h"

namespace milvus {
namespace engine {
namespace wal {

class WalManager {
 public:
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

    /*
     * Get next record
     * @param record[out]: record
     * @retval error_code
     */
    ErrorCode
    GetNextRecord(MXLogRecord& record);

    /*
     * Create table
     * @param table_id: table id
     * @retval lsn
     */
    uint64_t
    CreateTable(const std::string& table_id);

    /*
     * Drop table
     * @param table_id: table id
     * @retval none
     */
    void
    DropTable(const std::string& table_id);

    /*
     * Table is flushed
     * @param table_id: table id
     * @param lsn: flushed lsn
     */
    void
    TableFlushed(const std::string& table_id, uint64_t lsn);

    /*
     * Insert
     * @param table_id: table id
     * @param table_id: partition tag
     * @param vector_ids: vector ids
     * @param vectors: vectors
     */
    template <typename T>
    bool
    Insert(const std::string& table_id, const std::string& partition_tag, const IDNumbers& vector_ids,
           const std::vector<T>& vectors);

    /*
     * Insert
     * @param table_id: table id
     * @param vector_ids: vector ids
     */
    bool
    DeleteById(const std::string& table_id, const IDNumbers& vector_ids);

    /*
     * Get flush lsn
     * @param table_id: table id (empty means all tables)
     * @retval if there is something not flushed, return lsn;
     *         else, return 0
     */
    uint64_t
    Flush(const std::string table_id = "");

    void
    RemoveOldFiles(uint64_t flushed_lsn);

 private:
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
    std::map<std::string, TableLsn> tables_;
    std::atomic<uint64_t> last_applied_lsn_;

    // if multi-thread call Flush(), use list
    struct FlushInfo {
        std::string table_id_;
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

extern template bool
WalManager::Insert<float>(const std::string& table_id, const std::string& partition_tag, const IDNumbers& vector_ids,
                          const std::vector<float>& vectors);

extern template bool
WalManager::Insert<uint8_t>(const std::string& table_id, const std::string& partition_tag, const IDNumbers& vector_ids,
                            const std::vector<uint8_t>& vectors);

}  // namespace wal
}  // namespace engine
}  // namespace milvus
