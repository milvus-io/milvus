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
#include <unordered_map>

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

        std::vector<meta::CollectionSchema> collention_schema_array;
        auto status = meta->AllCollections(collention_schema_array);
        if (!status.ok()) {
            return WAL_META_ERROR;
        }

        if (!collention_schema_array.empty()) {
            u_int64_t min_flushed_lsn = ~(u_int64_t)0;
            u_int64_t max_flushed_lsn = 0;
            auto update_limit_lsn = [&](u_int64_t lsn) {
                if (min_flushed_lsn > lsn) {
                    min_flushed_lsn = lsn;
                }
                if (max_flushed_lsn < lsn) {
                    max_flushed_lsn = lsn;
                }
            };

            for (auto& col_schema : collention_schema_array) {
                auto& collection = collections_[col_schema.collection_id_];
                auto& default_part = collection[""];
                default_part.flush_lsn = col_schema.flush_lsn_;
                update_limit_lsn(default_part.flush_lsn);

                std::vector<meta::CollectionSchema> partition_schema_array;
                status = meta->ShowPartitions(col_schema.collection_id_, partition_schema_array);
                if (!status.ok()) {
                    return WAL_META_ERROR;
                }
                for (auto& par_schema : partition_schema_array) {
                    auto& partition = collection[par_schema.partition_tag_];
                    partition.flush_lsn = par_schema.flush_lsn_;
                    update_limit_lsn(partition.flush_lsn);
                }
            }

            if (applied_lsn < max_flushed_lsn) {
                // a new WAL folder?
                applied_lsn = max_flushed_lsn;
            }
            if (recovery_start < min_flushed_lsn) {
                // not flush all yet
                recovery_start = min_flushed_lsn;
            }

            for (auto& col : collections_) {
                for (auto& part : col.second) {
                    part.second.wal_lsn = applied_lsn;
                }
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
        auto it_col = collections_.find(record.collection_id);
        if (it_col != collections_.end()) {
            auto it_part = it_col->second.find(record.partition_tag);
            if (it_part->second.flush_lsn < record.lsn) {
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
WalManager::GetNextEntityRecovery(milvus::engine::wal::MXLogRecord& record) {
    ErrorCode error_code = WAL_SUCCESS;
    while (true) {
        error_code = p_buffer_->NextEntity(last_applied_lsn_, record);
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
        auto it_col = collections_.find(record.collection_id);
        if (it_col != collections_.end()) {
            auto it_part = it_col->second.find(record.partition_tag);
            if (it_part->second.flush_lsn < record.lsn) {
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
        auto it_col = collections_.find(record.collection_id);
        if (it_col != collections_.end()) {
            auto it_part = it_col->second.find(record.partition_tag);
            if (it_part->second.flush_lsn < record.lsn) {
                break;
            }
        }
    }

    if (record.type != MXLogType::None) {
        LOG_WAL_INFO_ << "record type " << (int32_t)record.type << " collection " << record.collection_id << " lsn "
                      << record.lsn;
    }
    return error_code;
}

ErrorCode
WalManager::GetNextEntityRecord(milvus::engine::wal::MXLogRecord& record) {
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
    while (WAL_SUCCESS == p_buffer_->NextEntity(last_applied_lsn_, record)) {
        if (record.type == MXLogType::None) {
            if (check_flush()) {
                return WAL_SUCCESS;
            }
            break;
        }

        std::lock_guard<std::mutex> lck(mutex_);
        auto it_col = collections_.find(record.collection_id);
        if (it_col != collections_.end()) {
            auto it_part = it_col->second.find(record.partition_tag);
            if (it_part->second.flush_lsn < record.lsn) {
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
    collections_[collection_id][""] = {applied_lsn, applied_lsn};
    return applied_lsn;
}

uint64_t
WalManager::CreatePartition(const std::string& collection_id, const std::string& partition_tag) {
    LOG_WAL_INFO_ << "create collection " << collection_id << " " << partition_tag << " " << last_applied_lsn_;
    std::lock_guard<std::mutex> lck(mutex_);
    uint64_t applied_lsn = last_applied_lsn_;
    collections_[collection_id][partition_tag] = {applied_lsn, applied_lsn};
    return applied_lsn;
}

uint64_t
WalManager::CreateHybridCollection(const std::string& collection_id) {
    LOG_WAL_INFO_ << "create hybrid collection " << collection_id << " " << last_applied_lsn_;
    std::lock_guard<std::mutex> lck(mutex_);
    uint64_t applied_lsn = last_applied_lsn_;
    collections_[collection_id][""] = {applied_lsn, applied_lsn};
    return applied_lsn;
}

void
WalManager::DropCollection(const std::string& collection_id) {
    LOG_WAL_INFO_ << "drop collection " << collection_id;
    std::lock_guard<std::mutex> lck(mutex_);
    collections_.erase(collection_id);
}

void
WalManager::DropPartition(const std::string& collection_id, const std::string& partition_tag) {
    LOG_WAL_INFO_ << collection_id << " drop partition " << partition_tag;
    std::lock_guard<std::mutex> lck(mutex_);
    auto it = collections_.find(collection_id);
    if (it != collections_.end()) {
        it->second.erase(partition_tag);
    }
}

void
WalManager::CollectionFlushed(const std::string& collection_id, uint64_t lsn) {
    std::unique_lock<std::mutex> lck(mutex_);
    if (collection_id.empty()) {
        // all collections
        for (auto& col : collections_) {
            for (auto& part : col.second) {
                part.second.flush_lsn = lsn;
            }
        }

    } else {
        // one collection
        auto it_col = collections_.find(collection_id);
        if (it_col != collections_.end()) {
            for (auto& part : it_col->second) {
                part.second.flush_lsn = lsn;
            }
        }
    }
    lck.unlock();

    LOG_WAL_INFO_ << collection_id << " is flushed by lsn " << lsn;
}

void
WalManager::PartitionFlushed(const std::string& collection_id, const std::string& partition_tag, uint64_t lsn) {
    std::unique_lock<std::mutex> lck(mutex_);
    auto it_col = collections_.find(collection_id);
    if (it_col != collections_.end()) {
        auto it_part = it_col->second.find(partition_tag);
        if (it_part != it_col->second.end()) {
            it_part->second.flush_lsn = lsn;
        }
    }
    lck.unlock();

    LOG_WAL_INFO_ << collection_id << "    " << partition_tag << " is flushed by lsn " << lsn;
}

void
WalManager::CollectionUpdated(const std::string& collection_id, uint64_t lsn) {
    std::unique_lock<std::mutex> lck(mutex_);
    auto it_col = collections_.find(collection_id);
    if (it_col != collections_.end()) {
        for (auto& part : it_col->second) {
            part.second.wal_lsn = lsn;
        }
    }
    lck.unlock();
}

void
WalManager::PartitionUpdated(const std::string& collection_id, const std::string& partition_tag, uint64_t lsn) {
    std::unique_lock<std::mutex> lck(mutex_);
    auto it_col = collections_.find(collection_id);
    if (it_col != collections_.end()) {
        auto it_part = it_col->second.find(partition_tag);
        if (it_part != it_col->second.end()) {
            it_part->second.wal_lsn = lsn;
        }
    }
    lck.unlock();
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

    last_applied_lsn_ = new_lsn;
    PartitionUpdated(collection_id, partition_tag, new_lsn);

    LOG_WAL_INFO_ << LogOut("[%s][%ld]", "insert", 0) << collection_id << " insert in part " << partition_tag
                  << " with lsn " << new_lsn;

    return p_meta_handler_->SetMXLogInternalMeta(new_lsn);
}

template <typename T>
bool
WalManager::InsertEntities(const std::string& collection_id, const std::string& partition_tag,
                           const milvus::engine::IDNumbers& entity_ids, const std::vector<T>& vectors,
                           const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                           const std::unordered_map<std::string, std::vector<uint8_t>>& attrs) {
    MXLogType log_type;
    if (std::is_same<T, float>::value) {
        log_type = MXLogType::Entity;
    } else {
        return false;
    }

    size_t entity_num = entity_ids.size();
    if (entity_num == 0) {
        LOG_WAL_ERROR_ << LogOut("[%s][%ld] The ids is empty.", "insert", 0);
        return false;
    }
    size_t dim = vectors.size() / entity_num;

    MXLogRecord record;

    size_t attr_unit_size = 0;
    auto attr_it = attr_nbytes.begin();
    for (; attr_it != attr_nbytes.end(); attr_it++) {
        record.field_names.emplace_back(attr_it->first);
        attr_unit_size += attr_it->second;
    }

    size_t unit_size = dim * sizeof(T) + sizeof(IDNumber) + attr_unit_size;
    size_t head_size = SizeOfMXLogRecordHeader + collection_id.length() + partition_tag.length();

    // TODO(yukun): field_name put into MXLogRecord??？

    record.type = log_type;
    record.collection_id = collection_id;
    record.partition_tag = partition_tag;
    record.attr_nbytes = attr_nbytes;

    uint64_t new_lsn = 0;
    for (size_t i = 0; i < entity_num; i += record.length) {
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

        size_t length = std::min(entity_num - i, max_rcd_num);
        record.length = length;
        record.ids = entity_ids.data() + i;
        record.data_size = record.length * dim * sizeof(T);
        record.data = vectors.data() + i * dim;

        record.attr_data.clear();
        record.attr_data_size.clear();
        for (auto field_name : record.field_names) {
            size_t attr_size = length * attr_nbytes.at(field_name);
            record.attr_data_size.insert(std::make_pair(field_name, attr_size));
            std::vector<uint8_t> attr_data(attr_size, 0);
            memcpy(attr_data.data(), attrs.at(field_name).data() + i * attr_nbytes.at(field_name), attr_size);
            record.attr_data.insert(std::make_pair(field_name, attr_data));
        }

        auto error_code = p_buffer_->AppendEntity(record);
        if (error_code != WAL_SUCCESS) {
            p_buffer_->ResetWriteLsn(last_applied_lsn_);
            return false;
        }
        new_lsn = record.lsn;
    }

    last_applied_lsn_ = new_lsn;
    PartitionUpdated(collection_id, partition_tag, new_lsn);

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

    last_applied_lsn_ = new_lsn;
    CollectionUpdated(collection_id, new_lsn);

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
        for (auto& col : collections_) {
            for (auto& part : col.second) {
                if (part.second.wal_lsn > part.second.flush_lsn) {
                    lsn = last_applied_lsn_;
                    break;
                }
            }
        }

    } else {
        // flush one collection
        auto it_col = collections_.find(collection_id);
        if (it_col != collections_.end()) {
            for (auto& part : it_col->second) {
                auto wal_lsn = part.second.wal_lsn;
                auto flush_lsn = part.second.flush_lsn;
                if (wal_lsn > flush_lsn && wal_lsn > lsn) {
                    lsn = wal_lsn;
                }
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

template bool
WalManager::InsertEntities<float>(const std::string& collection_id, const std::string& partition_tag,
                                  const milvus::engine::IDNumbers& entity_ids, const std::vector<float>& vectors,
                                  const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                                  const std::unordered_map<std::string, std::vector<uint8_t>>& attrs);

template bool
WalManager::InsertEntities<uint8_t>(const std::string& collection_id, const std::string& partition_tag,
                                    const milvus::engine::IDNumbers& entity_ids, const std::vector<uint8_t>& vectors,
                                    const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                                    const std::unordered_map<std::string, std::vector<uint8_t>>& attrs);

}  // namespace wal
}  // namespace engine
}  // namespace milvus
