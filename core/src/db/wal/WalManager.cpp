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
#include "WalDefinations.h"

namespace milvus {
namespace engine {
namespace wal {

WalManager*
WalManager::GetInstance() {
    static WalManager wal_manager;
    return &wal_manager;
}

WalManager::WalManager() {
    Init();
}

void
WalManager::Init() {
    //todo: init p_buffer_ and other vars
    //todo: step2 init p_buffer_;
    //todo: step3 load meta
    //step4 run wal thread
    server::Config& config = server::Config::GetInstance();
    config.GetWalConfigBufferSize(mxlog_config_.buffer_size);
    config.GetWalConfigRecordSize(mxlog_config_.record_size);
    config.GetWalConfigWalPath(mxlog_config_.mxlog_path);
    meta_handler_.GetMXLogExternalMeta(p_table_meta_);
    p_buffer_ = std::make_shared<MXLogBuffer>(mxlog_config_.mxlog_path, mxlog_config_.buffer_size);
    is_recoverying = false;
    Start();
}

void
WalManager::Start() {
    Recovery();
    is_running_ = true;
    Run();
}

void
WalManager::Stop() {
    is_running_ = false;
    //todo: xjbx
    reader_cv.notify_one();
}

void
WalManager::Run() {
    auto work = [&]() {
        while (is_running_) {
            if (p_buffer_->Next()) {
                reader_is_waiting = false;
            } else {
                reader_is_waiting = true;
                reader_cv.wait();
            }
        }
    };
    reader_ = std::thread(work);
    reader_.join();
}

bool
WalManager::Insert(const std::string &table_id,
                   size_t n,
                   const float *vectors,
                   milvus::engine::IDNumbers &vector_ids) {

    //todo: should wait outside walmanager
    while (is_recoverying) {
        usleep(2);
    }
    uint32_t max_record_data_size = mxlog_config_.record_size - (uint32_t)SizeOfMXLogRecordHeader;
    auto table_meta = p_table_meta_->find(table_id);
    if (table_meta == p_table_meta_->end()) {
        //todo: get table_meta by table_id and cache it
        meta_handler_.GetMXLogExternalMeta(p_table_meta_);//pull newest meta
    }
    uint16_t dim = table_meta->second->dimension_;
    //split and insert into wal
    size_t vectors_per_record = (max_record_data_size - table_id.size()) / ((dim << 2) + 8);
    size_t i = 0;
    __glibcxx_assert(vectors_per_record > 0);
    meta_handler_.GetMXLogInternalMeta(last_applied_lsn_, current_file_no_);
    for (; i + vectors_per_record < n; i += vectors_per_record) {
        if(!p_buffer_->Append(table_id, MXLogType::Insert, vectors_per_record, dim, vectors + i * (dim << 2), vector_ids, i, false,last_applied_lsn_)) {
            return false;
        }
    }
    if (i < n) {
        if(!p_buffer_->Append(table_id, MXLogType::Insert, n - i, dim, vectors + i * (dim << 2), vector_ids, i, true, last_applied_lsn_)) {
            return false;
        }
    }
    //todo: consider sync and async flush
    current_file_no_ = (uint32_t)(last_applied_lsn_ >> 32);
    meta_handler_.SetMXLogInternalMeta(last_applied_lsn_, current_file_no_);
    return true;
}

//TBD
void
WalManager::DeleteById(const std::string& table_id, const milvus::engine::IDNumbers& vector_ids) {
    //todo: do it outside, in grpc queue, or there exisit invoke wal interface concurrently, which not support
    while (is_recoverying) {
        usleep(2);
    }
    //todo: similar to insert
}

void
WalManager::Flush(const std::string& table_id) {
    uint32_t max_record_data_size = mxlog_config_.record_size - (uint32_t)SizeOfMXLogRecordHeader;
    auto table_meta = p_table_meta_->find(table_id);
    if (table_meta == p_table_meta_->end()) {
        //todo: get table_meta by table_id and cache it
        meta_handler_.GetMXLogExternalMeta(p_table_meta_);//pull newest meta
    }
    MXLogType type;
    if ("" == table_id) {
        type = MXLogType::FlushAll;
    } else {
        type = MXLogType::Flush;
    }
    milvus::engine::IDNumbers useless_vec;
    meta_handler_.GetMXLogInternalMeta(last_applied_lsn_, current_file_no_);
    p_buffer_->Append(table_id, type, 0, 0, NULL, useless_vec, -1, true, last_applied_lsn_);
    current_file_no_ = (uint32_t)(last_applied_lsn_ >> 32);
    meta_handler_.SetMXLogInternalMeta(last_applied_lsn_, current_file_no_);
}

void
WalManager::Recovery() {
    //todo: how to judge that whether system exit normally last time?
    //todo: if (system.exit.normally) return;
    //todo: fetch meta
    is_recoverying = true;
    p_table_meta_->clear();
    meta_handler_.GetMXLogExternalMeta(p_table_meta_);
    uint64_t current_recovery_point = 0;
//    for (auto it = p_table_meta_->begin(), it != p_table_meta_->end(); ++ it) {
//        start_recovery_point = std::min(start_recovery_point, kv.second);
//    }

    p_buffer_->LoadForRecovery(current_recovery_point);
    while (current_recovery_point <= last_applied_lsn_) {
        std::string table_id;
        uint64_t next_lsn;
        if (p_buffer_->NextInfo(table_id, next_lsn)) {
            auto meta_schema = p_table_meta_->find(table_id);
            if (meta_schema->second->lsn < current_recovery_point) {
                Apply(current_recovery_point);
            }
        }
        current_recovery_point = next_lsn;
    }

    p_buffer_->ReSet();
    is_recoverying = false;
}

void
WalManager::Apply(const uint64_t &apply_lsn) {

}

} // wal
} // engine
} // milvus
