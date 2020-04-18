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

#include <unistd.h>

#include <algorithm>
#include <memory>

#include "config/Config.h"
#include "utils/CommonUtil.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {
namespace wal {

WalManager::WalManager(const MXLogConfiguration& config) {
    __glibcxx_assert(config.buffer_size <= milvus::server::CONFIG_WAL_BUFFER_SIZE_MAX / 2);
    __glibcxx_assert(config.buffer_size >= milvus::server::CONFIG_WAL_BUFFER_SIZE_MIN / 2);

    mxlog_config_.recovery_error_ignore = config.recovery_error_ignore;
    mxlog_config_.buffer_size = config.buffer_size;
    mxlog_config_.mxlog_path = config.mxlog_path;

    // check the path end with '/'
    if (mxlog_config_.mxlog_path.back() != '/') {
        mxlog_config_.mxlog_path += '/';
    }
    // check path exist
    auto status = server::CommonUtil::CreateDirectory(mxlog_config_.mxlog_path);
    if (!status.ok()) {
        std::string msg = "failed to create wal directory " + mxlog_config_.mxlog_path;
        LOG_ENGINE_ERROR_ << msg;
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

        std::vector<meta::CollectionSchema> table_schema_array;
        auto status = meta->AllCollections(table_schema_array);
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
                tables_[schema.collection_id_] = tb_lsn;
            }
        }
    }

    // all tables are droped and a new wal path?
    if (applied_lsn < recovery_start) {
        applied_lsn = recovery_start;
    }

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

    // buffer size may changed
    mxlog_config_.buffer_size = p_buffer_->GetBufferSize();

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
        auto it = tables_.find(record.collection_id);
        if (it != tables_.end()) {
            if (it->second.flush_lsn < record.lsn) {
                break;
            }
        }
    }

    // print the log only when record.type != MXLogType::None
    if (record.type != MXLogType::None) {
        LOG_WAL_INFO_ << "record type " << (int32_t)record.type << " record lsn " << record.lsn << " error code  "
                      << error_code;
    }

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
                record.collection_id = flush_info_.collection_id_;
                record.lsn = flush_info_.lsn_;
                flush_info_.Clear();

                LOG_WAL_INFO_ << "record flush collection " << record.collection_id << " lsn " << record.lsn;
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
        auto it = tables_.find(record.collection_id);
        if (it != tables_.end()) {
            if (it->second.flush_lsn < record.lsn) {
                break;
            }
        }
    }

    LOG_WAL_INFO_ << "record type " << (int32_t)record.type << " collection " << record.collection_id << " lsn "
                  << record.lsn;
    return error_code;
}

uint64_t
WalManager::CreateCollection(const std::string& collection_id) {
    LOG_WAL_INFO_ << "create collection " << collection_id << " " << last_applied_lsn_;
    std::lock_guard<std::mutex> lck(mutex_);
    uint64_t applied_lsn = last_applied_lsn_;
    tables_[collection_id] = {applied_lsn, applied_lsn};
    return applied_lsn;
}

void
WalManager::DropCollection(const std::string& collection_id) {
    LOG_WAL_INFO_ << "drop collection " << collection_id;
    std::lock_guard<std::mutex> lck(mutex_);
    tables_.erase(collection_id);
}

void
WalManager::CollectionFlushed(const std::string& collection_id, uint64_t lsn) {
    std::unique_lock<std::mutex> lck(mutex_);
    auto it = tables_.find(collection_id);
    if (it != tables_.end()) {
        it->second.flush_lsn = lsn;
    }
    lck.unlock();

    LOG_WAL_INFO_ << collection_id << " is flushed by lsn " << lsn;
}

template <typename T>
bool
WalManager::Insert(const std::string& collection_id, const std::string& partition_tag, const IDNumbers& vector_ids,
                   const std::vector<T>& vectors) {
    MXLogType log_type;
    if (std::is_same<T, float>::value) {
        log_type = MXLogType::InsertVector;
    } else if (std::is_same<T, uint8_t>::value) {
        log_type = MXLogType::InsertBinary;
    } else {
        return false;
    }

    size_t vector_num = vector_ids.size();
    if (vector_num == 0) {
        LOG_WAL_ERROR_ << LogOut("[%s][%ld] The ids is empty.", "insert", 0);
        return false;
    }
    size_t dim = vectors.size() / vector_num;
    size_t unit_size = dim * sizeof(T) + sizeof(IDNumber);
    size_t head_size = SizeOfMXLogRecordHeader + collection_id.length() + partition_tag.length();

    MXLogRecord record;
    record.type = log_type;
    record.collection_id = collection_id;
    record.partition_tag = partition_tag;

    uint64_t new_lsn = 0;
    for (size_t i = 0; i < vector_num; i += record.length) {
        size_t surplus_space = p_buffer_->SurplusSpace();
        size_t max_rcd_num = 0;
        if (surplus_space >= head_size + unit_size) {
            max_rcd_num = (surplus_space - head_size) / unit_size;
        } else {
            max_rcd_num = (mxlog_config_.buffer_size - head_size) / unit_size;
        }
        if (max_rcd_num == 0) {
            LOG_WAL_ERROR_ << LogOut("[%s][%ld]", "insert", 0) << "Wal buffer size is too small "
                           << mxlog_config_.buffer_size << " unit " << unit_size;
            return false;
        }

        record.length = std::min(vector_num - i, max_rcd_num);
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
    auto it = tables_.find(collection_id);
    if (it != tables_.end()) {
        it->second.wal_lsn = new_lsn;
    }
    lck.unlock();

    LOG_WAL_INFO_ << LogOut("[%s][%ld]", "insert", 0) << collection_id << " insert in part " << partition_tag
                  << " with lsn " << new_lsn;

    return p_meta_handler_->SetMXLogInternalMeta(new_lsn);
}

bool
WalManager::DeleteById(const std::string& collection_id, const IDNumbers& vector_ids) {
    size_t vector_num = vector_ids.size();
    if (vector_num == 0) {
        LOG_WAL_ERROR_ << "The ids is empty.";
        return false;
    }

    size_t unit_size = sizeof(IDNumber);
    size_t head_size = SizeOfMXLogRecordHeader + collection_id.length();

    MXLogRecord record;
    record.type = MXLogType::Delete;
    record.collection_id = collection_id;
    record.partition_tag = "";

    uint64_t new_lsn = 0;
    for (size_t i = 0; i < vector_num; i += record.length) {
        size_t surplus_space = p_buffer_->SurplusSpace();
        size_t max_rcd_num = 0;
        if (surplus_space >= head_size + unit_size) {
            max_rcd_num = (surplus_space - head_size) / unit_size;
        } else {
            max_rcd_num = (mxlog_config_.buffer_size - head_size) / unit_size;
        }

        record.length = std::min(vector_num - i, max_rcd_num);
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
    auto it = tables_.find(collection_id);
    if (it != tables_.end()) {
        it->second.wal_lsn = new_lsn;
    }
    lck.unlock();

    LOG_WAL_INFO_ << collection_id << " delete rows by id, lsn " << new_lsn;

    return p_meta_handler_->SetMXLogInternalMeta(new_lsn);
}

uint64_t
WalManager::Flush(const std::string& collection_id) {
    std::lock_guard<std::mutex> lck(mutex_);
    // At most one flush requirement is waiting at any time.
    // Otherwise, flush_info_ should be modified to a list.
    __glibcxx_assert(!flush_info_.IsValid());

    uint64_t lsn = 0;
    if (collection_id.empty()) {
        // flush all tables
        for (auto& it : tables_) {
            if (it.second.wal_lsn > it.second.flush_lsn) {
                lsn = last_applied_lsn_;
                break;
            }
        }

    } else {
        // flush one collection
        auto it = tables_.find(collection_id);
        if (it != tables_.end()) {
            if (it->second.wal_lsn > it->second.flush_lsn) {
                lsn = it->second.wal_lsn;
            }
        }
    }

    if (lsn != 0) {
        flush_info_.collection_id_ = collection_id;
        flush_info_.lsn_ = lsn;
    }

    LOG_WAL_INFO_ << collection_id << " want to be flush, lsn " << lsn;

    return lsn;
}

void
WalManager::RemoveOldFiles(uint64_t flushed_lsn) {
    if (p_buffer_ != nullptr) {
        p_buffer_->RemoveOldFiles(flushed_lsn);
    }
}

template bool
WalManager::Insert<float>(const std::string& collection_id, const std::string& partition_tag,
                          const IDNumbers& vector_ids, const std::vector<float>& vectors);

template bool
WalManager::Insert<uint8_t>(const std::string& collection_id, const std::string& partition_tag,
                            const IDNumbers& vector_ids, const std::vector<uint8_t>& vectors);

}  // namespace wal
}  // namespace engine
}  // namespace milvus
