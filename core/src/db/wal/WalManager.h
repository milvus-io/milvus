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
#include "WalDefinations.h"
#include "WalFileHandler.h"
#include "WalMetaHandler.h"
#include "WalBuffer.h"

namespace milvus {
namespace engine {
namespace wal {

class WalManager {
 public:
    WalManager();
    ~WalManager();

    /*
     * init
     * @param meta
     */
    void
    Init(const meta::MetaPtr& meta);

    /*
     * Get next recovery
     * @param record[out]: record
     */
    void
    GetNextRecovery(WALRecord &record);

    /*
     * Get next record
     * @param record[out]: record
     */
    void
    GetNextRecord(WALRecord &record);

    /*
     * Create table
     * @param table_id: table id
     * @retval lsn
     */
    uint64_t
    CreateTable(const std::string &table_id);

    /*
     * Drop table
     * @param table_id: table id
     * @retval none
     */
    void
    DropTable(const std::string &table_id);

    /*
     * Table is flushed
     * @param table_id: table id
     * @param lsn: flushed lsn
     */
    void
    TableFlushed(const std::string &table_id, uint64_t lsn);

    /*
     * Insert
     * @param table_id: table id
     * @param vector_ids: vector ids
     * @param vectors: vectors
     */
    template <typename T>
    bool
    Insert(const std::string &table_id,
           const IDNumbers &vector_ids,
           const std::vector<T> &vectors);

    /*
     * Insert
     * @param table_id: table id
     * @param vector_ids: vector ids
     */
    bool
    DeleteById(const std::string& table_id,
               const IDNumbers &vector_ids);

    /*
     * Get flush lsn
     * @param table_id: table id (empty means all tables)
     * @retval if there is something not flushed, return lsn;
     *         else, return 0
     */
    uint64_t
    Flush(const std::string table_id = "");


 private:
    WalManager operator = (WalManager&);

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
    std::pair<std::string, uint64_t> flush_info_;
};

extern template bool
WalManager::Insert<float>(
       const std::string &table_id,
       const IDNumbers &vector_ids,
       const std::vector<float> &vectors);

extern template bool
WalManager::Insert<uint8_t>(
    const std::string &table_id,
    const IDNumbers &vector_ids,
    const std::vector<uint8_t> &vectors);

} // wal
} // engine
} // milvus

