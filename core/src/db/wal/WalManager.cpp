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

#include "db/wal/WalManager.h"

#include <unistd.h>

#include <algorithm>
#include <memory>

#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {
namespace wal {

WalManager::WalManager(const MXLogConfiguration& config) {
    mxlog_config_.recovery_error_ignore = config.recovery_error_ignore;
    mxlog_config_.buffer_size = config.buffer_size * 1024 * 1024;
    mxlog_config_.record_size = std::max((uint32_t)1, config.record_size) * 1024 * 1024;
    mxlog_config_.mxlog_path = config.mxlog_path;

    // check the path end with '/'
    if (mxlog_config_.mxlog_path.back() != '/') {
        mxlog_config_.mxlog_path += '/';
    }
    // check path exist
    auto status = server::CommonUtil::CreateDirectory(mxlog_config_.mxlog_path);
    if (!status.ok()) {
        std::string msg = "failed to create wal directory " + mxlog_config_.mxlog_path;
        ENGINE_LOG_ERROR << msg;
        throw Exception(WAL_PATH_ERROR, msg);
    }
}

WalManager::~WalManager() {
}

ErrorCode
WalManager::Init(const meta::MetaPtr& meta) {
    uint64_t applied_lsn = 0;
    p_meta_handler_ = std::make_shared<MXLogMetaHandler>(mxlog_config_.mxlog_path);
    if (p_meta_handler_ != nullptr) {
        p_meta_handler_->GetMXLogInternalMeta(applied_lsn);
    }

    uint64_t recovery_start = 0;
    if (meta != nullptr) {
        meta->GetGlobalLastLSN(recovery_start);

        std::vector<meta::TableSchema> table_schema_array;
        auto status = meta->AllTables(table_schema_array);
        if (!status.ok()) {
            return WAL_META_ERROR;
        }

        if (!table_schema_array.empty()) {
            // get min and max flushed lsn
            uint64_t min_flused_lsn = table_schema_array[0].flush_lsn_;
            uint64_t max_flused_lsn = table_schema_array[0].flush_lsn_;
            for (size_t i = 1; i < table_schema_array.size(); i++) {
                if (min_flused_lsn > table_schema_array[i].flush_lsn_) {
                    min_flused_lsn = table_schema_array[i].flush_lsn_;
                } else if (max_flused_lsn < table_schema_array[i].flush_lsn_) {
                    max_flused_lsn = table_schema_array[i].flush_lsn_;
                }
            }
            if (applied_lsn < max_flused_lsn) {
                // a new WAL folder?
                applied_lsn = max_flused_lsn;
            }
            if (recovery_start < min_flused_lsn) {
                // not flush all yet
                recovery_start = min_flused_lsn;
            }

            for (auto& schema : table_schema_array) {
                TableLsn tb_lsn = {schema.flush_lsn_, applied_lsn};
                tables_[schema.table_id_] = tb_lsn;
            }
        }
    }

    // globalFlushedLsn <= max_flused_lsn is always true,
    // so recovery_start <= applied_lsn is always true.
    __glibcxx_assert(recovery_start <= applied_lsn);

    ErrorCode error_code = WAL_ERROR;
    p_buffer_ = std::make_shared<MXLogBuffer>(mxlog_config_.mxlog_path, mxlog_config_.buffer_size);
    if (p_buffer_ != nullptr) {
        if (p_buffer_->Init(recovery_start, applied_lsn)) {
            error_code = WAL_SUCCESS;
        } else if (mxlog_config_.recovery_error_ignore) {
            p_buffer_->Reset(applied_lsn);
            error_code = WAL_SUCCESS;
        } else {
            error_code = WAL_FILE_ERROR;
        }
    }

    // check record size
    auto record_max_size = p_buffer_->GetBufferSize();
    if (mxlog_config_.record_size > record_max_size) {
        WAL_LOG_INFO << "the record is changed to " << record_max_size;
        mxlog_config_.record_size = record_max_size;
    }

    last_applied_lsn_ = applied_lsn;
    return error_code;
}

ErrorCode
WalManager::GetNextRecovery(MXLogRecord& record) {
    ErrorCode error_code = WAL_SUCCESS;
    while (true) {
        error_code = p_buffer_->Next(last_applied_lsn_, record);
        if (error_code != WAL_SUCCESS) {
            if (mxlog_config_.recovery_error_ignore) {
                // reset and break recovery
                p_buffer_->Reset(last_applied_lsn_);

                record.type = MXLogType::None;
                error_code = WAL_SUCCESS;
            }
            break;
        }
        if (record.type == MXLogType::None) {
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
    }

    WAL_LOG_INFO << "record type " << (int32_t)record.type << " record lsn " << record.lsn << " error code  "
                 << error_code;

    return error_code;
}

ErrorCode
WalManager::GetNextRecord(MXLogRecord& record) {
    auto check_flush = [&]() -> bool {
        std::lock_guard<std::mutex> lck(mutex_);
        if (flush_info_.IsValid()) {
            if (p_buffer_->GetReadLsn() >= flush_info_.lsn_) {
                // can exec flush requirement
                record.type = MXLogType::Flush;
                record.table_id = flush_info_.table_id_;
                record.lsn = flush_info_.lsn_;
                flush_info_.Clear();

                WAL_LOG_INFO << "record flush table " << record.table_id << " lsn " << record.lsn;
                return true;
            }
        }
        return false;
    };

    if (check_flush()) {
        return WAL_SUCCESS;
    }

    ErrorCode error_code = WAL_SUCCESS;
    while (WAL_SUCCESS == p_buffer_->Next(last_applied_lsn_, record)) {
        if (record.type == MXLogType::None) {
            if (check_flush()) {
                return WAL_SUCCESS;
            }
            break;
        }

        std::lock_guard<std::mutex> lck(mutex_);
        auto it = tables_.find(record.table_id);
        if (it != tables_.end()) {
            break;
        }
    }

    WAL_LOG_INFO << "record type " << (int32_t)record.type << " table " << record.table_id << " lsn " << record.lsn;
    return error_code;
}

uint64_t
WalManager::CreateTable(const std::string& table_id) {
    WAL_LOG_INFO << "create table " << table_id << " " << last_applied_lsn_;
    std::lock_guard<std::mutex> lck(mutex_);
    uint64_t applied_lsn = last_applied_lsn_;
    tables_[table_id] = {applied_lsn, applied_lsn};
    return applied_lsn;
}

void
WalManager::DropTable(const std::string& table_id) {
    WAL_LOG_INFO << "drop table " << table_id;
    std::lock_guard<std::mutex> lck(mutex_);
    tables_.erase(table_id);
}

void
WalManager::TableFlushed(const std::string& table_id, uint64_t lsn) {
    std::unique_lock<std::mutex> lck(mutex_);
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        it->second.flush_lsn = lsn;
    }
    lck.unlock();

    WAL_LOG_INFO << table_id << " is flushed by lsn " << lsn;
}

template <typename T>
bool
WalManager::Insert(const std::string& table_id, const std::string& partition_tag, const IDNumbers& vector_ids,
                   const std::vector<T>& vectors) {
    MXLogType log_type;
    if (std::is_same<T, float>::value) {
        log_type = MXLogType::InsertVector;
    } else if (std::is_same<T, uint8_t>::value) {
        log_type = MXLogType::InsertBinary;
    } else {
        return false;
    }

    uint32_t max_record_data_size = mxlog_config_.record_size - SizeOfMXLogRecordHeader;

    size_t vector_num = vector_ids.size();
    if (vector_num == 0) {
        WAL_LOG_ERROR << "The ids is empty.";
        return false;
    }
    size_t dim = vectors.size() / vector_num;

    // split and insert into wal
    size_t vectors_per_record =
        (max_record_data_size - table_id.size() - partition_tag.size()) / ((dim * sizeof(T)) + sizeof(IDNumber));
    if (vectors_per_record == 0) {
        WAL_LOG_ERROR << "The record size is too small to save one row. " << mxlog_config_.record_size;
        return false;
    }

    MXLogRecord record;
    record.type = log_type;
    record.table_id = table_id;
    record.partition_tag = partition_tag;

    uint64_t new_lsn = 0;
    for (size_t i = 0; i < vector_num; i += vectors_per_record) {
        record.length = std::min(vector_num - i, vectors_per_record);
        record.ids = vector_ids.data() + i;
        record.data_size = record.length * dim * sizeof(T);
        record.data = vectors.data() + i * dim;

        auto error_code = p_buffer_->Append(record);
        if (error_code != WAL_SUCCESS) {
            p_buffer_->ResetWriteLsn(last_applied_lsn_);
            return false;
        }
        new_lsn = record.lsn;
    }

    std::unique_lock<std::mutex> lck(mutex_);
    last_applied_lsn_ = new_lsn;
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        it->second.wal_lsn = new_lsn;
    }
    lck.unlock();

    WAL_LOG_INFO << table_id << " insert in part " << partition_tag << " with lsn " << new_lsn;

    return p_meta_handler_->SetMXLogInternalMeta(new_lsn);
}

bool
WalManager::DeleteById(const std::string& table_id, const IDNumbers& vector_ids) {
    uint32_t max_record_data_size = mxlog_config_.record_size - SizeOfMXLogRecordHeader;

    size_t vector_num = vector_ids.size();

    // split and insert into wal
    size_t vectors_per_record = (max_record_data_size - table_id.size()) / (sizeof(IDNumber));
    __glibcxx_assert(vectors_per_record > 0);

    MXLogRecord record;
    record.type = MXLogType::Delete;
    record.table_id = table_id;
    record.partition_tag = "";

    uint64_t new_lsn = 0;
    for (size_t i = 0; i < vector_num; i += vectors_per_record) {
        record.length = std::min(vector_num - i, vectors_per_record);
        record.ids = vector_ids.data() + i;
        record.data_size = 0;
        record.data = nullptr;

        auto error_code = p_buffer_->Append(record);
        if (error_code != WAL_SUCCESS) {
            p_buffer_->ResetWriteLsn(last_applied_lsn_);
            return false;
        }
        new_lsn = record.lsn;
    }

    std::unique_lock<std::mutex> lck(mutex_);
    last_applied_lsn_ = new_lsn;
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        it->second.wal_lsn = new_lsn;
    }
    lck.unlock();

    WAL_LOG_INFO << table_id << " delete rows by id, lsn " << new_lsn;

    return p_meta_handler_->SetMXLogInternalMeta(new_lsn);
}

uint64_t
WalManager::Flush(const std::string table_id) {
    std::lock_guard<std::mutex> lck(mutex_);
    // At most one flush requirement is waiting at any time.
    // Otherwise, flush_info_ should be modified to a list.
    __glibcxx_assert(!flush_info_.IsValid());

    uint64_t lsn = 0;
    if (table_id.empty()) {
        // flush all tables
        for (auto& it : tables_) {
            if (it.second.wal_lsn > it.second.flush_lsn) {
                lsn = last_applied_lsn_;
                break;
            }
        }

    } else {
        // flush one table
        auto it = tables_.find(table_id);
        if (it != tables_.end()) {
            if (it->second.wal_lsn > it->second.flush_lsn) {
                lsn = it->second.wal_lsn;
            }
        }
    }

    if (lsn != 0) {
        flush_info_.table_id_ = table_id;
        flush_info_.lsn_ = lsn;
    }

    WAL_LOG_INFO << table_id << " want to be flush, lsn " << lsn;

    return lsn;
}

void
WalManager::RemoveOldFiles(uint64_t flushed_lsn) {
    if (p_buffer_ != nullptr) {
        p_buffer_->RemoveOldFiles(flushed_lsn);
    }
}

template bool
WalManager::Insert<float>(const std::string& table_id, const std::string& partition_tag, const IDNumbers& vector_ids,
                          const std::vector<float>& vectors);

template bool
WalManager::Insert<uint8_t>(const std::string& table_id, const std::string& partition_tag, const IDNumbers& vector_ids,
                            const std::vector<uint8_t>& vectors);

}  // namespace wal
}  // namespace engine
}  // namespace milvus
