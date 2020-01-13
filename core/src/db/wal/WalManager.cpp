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

#include <unistd.h>
#include <src/server/Config.h>
#include "WalManager.h"
#include "stdio.h"

namespace milvus {
namespace engine {
namespace wal {

WalManager::WalManager() {
    server::Config& config = server::Config::GetInstance();
    config.GetWalConfigBufferSize(mxlog_config_.buffer_size);
    config.GetWalConfigRecordSize(mxlog_config_.record_size);
    config.GetWalConfigWalPath(mxlog_config_.mxlog_path);
}

bool
WalManager::Init(const meta::MetaPtr& meta, bool ignore_error) {
    uint64_t applied_lsn = 0;
    p_meta_handler_ = std::make_shared<MXLogMetaHandler>(mxlog_config_.mxlog_path);
    if (p_meta_handler_ != nullptr) {
        p_meta_handler_->GetMXLogInternalMeta(applied_lsn);
        last_applied_lsn_ = applied_lsn;
    }

    std::vector<meta::TableSchema> table_schema_array;
    if (meta != nullptr) {
        auto status = meta->AllTables(table_schema_array);
        if (!status.ok()) {
            // through exception
        }
    }

    // Todo: get recovery start point
    uint64_t recovery_start = applied_lsn;

    for (auto schema: table_schema_array) {
        TableLsn tb_lsn = {schema.flush_lsn_, applied_lsn};
        tables_[schema.table_id_] = tb_lsn;

        recovery_start = std::min(recovery_start, schema.flush_lsn_);
    }

    p_buffer_ = std::make_shared<MXLogBuffer>(
        mxlog_config_.mxlog_path,
        mxlog_config_.buffer_size);

    bool rst = p_buffer_->Init(recovery_start, applied_lsn);
    if (!rst && ignore_error) {
        p_buffer_->Reset(applied_lsn);
    }
    return rst;
}

void
WalManager::GetNextRecovery(WALRecord &record) {
    record.type = MXLogType::None;

    if (p_buffer_ != nullptr) {
        do {
            record.lsn = p_buffer_->Next(last_applied_lsn_,
                                         record.table_id,
                                         record.type,
                                         record.length,
                                         record.ids,
                                         record.dim,
                                         record.data);
            if (record.lsn == 0) {
                break;
            }

            // background thread has not started.
            // so, needn't lock here.
            auto it = tables_.find(record.table_id);
            if (it != tables_.end()) {
                if (it->second.flush_lsn < record.lsn) {
                    break;
                }
            }

        } while (1);
    }
}

void
WalManager::GetNextRecord(WALRecord &record) {
    record.type = MXLogType::None;
    record.lsn = p_buffer_->GetReadLsn();

    std::unique_lock<std::mutex> lck (mutex_);
    if (flush_info_.second != 0) {
        if (record.lsn >= flush_info_.second) {
            record.lsn = flush_info_.second;
            record.type = MXLogType::Flush;
            record.table_id = flush_info_.first;
            return;
        }
    }
    lck.unlock();

    if (p_buffer_ != nullptr) {
        do {
            record.lsn = p_buffer_->Next(last_applied_lsn_,
                                         record.table_id,
                                         record.type,
                                         record.length,
                                         record.ids,
                                         record.dim,
                                         record.data);
            if (record.lsn == 0) {
                break;
            }

            lck.lock();
            auto it = tables_.find(record.table_id);
            if (it != tables_.end()) {
                break;
            }
            lck.unlock();

        } while (1);
    }
}

uint64_t
WalManager::CreateTable(const std::string &table_id) {
    std::unique_lock<std::mutex> lck (mutex_);
    uint64_t applied_lsn = last_applied_lsn_;
    tables_[table_id] = {applied_lsn, applied_lsn};
    return applied_lsn;
}

void
WalManager::DropTable(const std::string &table_id) {
    std::unique_lock<std::mutex> lck (mutex_);
    tables_.erase(table_id);
}

void
WalManager::TableFlushed(const std::string &table_id, uint64_t lsn) {
    std::unique_lock<std::mutex> lck (mutex_);
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        it->second.flush_lsn = lsn;
    }
}

template <typename T>
bool
WalManager::Insert(const std::string &table_id,
                   const IDNumbers &vector_ids,
                   const std::vector<T> &vectors)
{
    MXLogType log_type;
    if (std::is_same<T, float>::value) {
        log_type = MXLogType::InsertVector;
    } else if (std::is_same<T, uint8_t>::value) {
        log_type = MXLogType::InsertBinary;
    } else {
        return false;
    }

    uint32_t max_record_data_size = mxlog_config_.record_size - (uint32_t)SizeOfMXLogRecordHeader;

    size_t vector_num = vector_ids.size();
    uint16_t dim = vectors.size() / vector_num;
    //split and insert into wal
    size_t vectors_per_record = (max_record_data_size - table_id.size()) / ((dim * sizeof(T)) + sizeof(IDNumber));
    __glibcxx_assert(vectors_per_record > 0);

    uint64_t new_lsn = 0;
    for (size_t i = 0; i < vector_num; i += vectors_per_record) {
        size_t insert_len = std::min(vector_num - i, vectors_per_record);
        new_lsn = p_buffer_->Append(table_id,
                                    log_type,
                                    insert_len,
                                    vector_ids.data() + i,
                                    dim,
                                    vectors.data() + i * dim);
        if(new_lsn == 0) {
            p_buffer_->SetWriteLsn(last_applied_lsn_);
            return false;
        }
    }

    std::unique_lock<std::mutex> lck (mutex_);
    last_applied_lsn_ = new_lsn;
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        it->second.wal_lsn = new_lsn;
    }
    lck.unlock();

    p_meta_handler_->SetMXLogInternalMeta(new_lsn);
    return true;
}

bool
WalManager::DeleteById(const std::string& table_id, const IDNumbers& vector_ids) {
    uint32_t max_record_data_size = mxlog_config_.record_size - (uint32_t)SizeOfMXLogRecordHeader;

    size_t vector_num = vector_ids.size();

    //split and insert into wal
    size_t vectors_per_record = (max_record_data_size - table_id.size()) / (sizeof(IDNumber));
    __glibcxx_assert(vectors_per_record > 0);

    uint64_t new_lsn = 0;
    for (size_t i = 0; i < vector_num; i += vectors_per_record) {
        size_t delete_len = std::min(vector_num - i, vectors_per_record);
        new_lsn = p_buffer_->Append(table_id,
                                    MXLogType::Delete,
                                    delete_len,
                                    vector_ids.data() + i,
                                    0,
                                    nullptr);
        if(new_lsn == 0) {
            return false;
        }
    }

    std::unique_lock<std::mutex> lck (mutex_);
    last_applied_lsn_ = new_lsn;
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        it->second.wal_lsn = new_lsn;
    }
    lck.unlock();

    p_meta_handler_->SetMXLogInternalMeta(new_lsn);
    return true;
}

uint64_t
WalManager::Flush(const std::string table_id) {
    std::unique_lock<std::mutex> lck (mutex_, std::defer_lock);
    uint64_t lsn = 0;
    if (table_id.empty()) {
        lck.lock();
        for (auto &it : tables_) {
            if (it.second.wal_lsn != it.second.flush_lsn) {
                lsn = last_applied_lsn_;
                break;
            }
        }

    } else {
        lck.lock();
        auto it = tables_.find(table_id);
        if (it != tables_.end()) {
            if (it->second.wal_lsn != it->second.flush_lsn) {
                lsn = it->second.wal_lsn;
            }
        }
    }

    if (lsn != 0) {
        flush_info_.first = table_id;
        flush_info_.second = lsn;
    }
    lck.unlock();

    return lsn;
}

} // wal
} // engine
} // milvus
