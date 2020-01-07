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
#include "stdio.h"

namespace milvus {
namespace engine {
namespace wal {

std::condition_variable reader_cv;
std::mutex reader_mutex;
bool reader_is_waiting = 0;

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
    // step1: load config & meta
    server::Config& config = server::Config::GetInstance();
    config.GetWalConfigBufferSize(mxlog_config_.buffer_size);
    config.GetWalConfigRecordSize(mxlog_config_.record_size);
    config.GetWalConfigWalPath(mxlog_config_.mxlog_path);
    p_meta_handler_ = std::make_shared<MXLogMetaHandler>(mxlog_config_.mxlog_path);
    p_buffer_ = std::make_shared<MXLogBuffer>(mxlog_config_.mxlog_path, mxlog_config_.buffer_size);

    // step2:
    Recovery();

    // step3: run wal thread
    Start();
}

void
WalManager::Start() {
    std::string table_id;
    MXLogType mxlog_type;
    size_t num_of_vectors;
    size_t vector_dim;
    float* vectors = NULL;
    milvus::engine::IDNumbers vector_ids;
    uint64_t lsn;
    auto clear = [&]() {
        table_id = "";
        num_of_vectors = 0;
        vector_dim = 0;
        lsn = 0;
        if (vectors)
            free(vectors);
        vector_ids.clear();
    };

    auto work = [&]() {
        while (is_running_) {
            if (p_buffer_->Next(table_id, mxlog_type, num_of_vectors, vector_dim, vectors, vector_ids, last_applied_lsn_, lsn)) {
                reader_is_waiting = false;
                Dispatch(table_id, mxlog_type, num_of_vectors, vector_dim, vectors, vector_ids, last_applied_lsn_, lsn);
            } else {
                // todo: wait-notify
                reader_is_waiting = true;
                std::unique_lock<std::mutex> reader_lock(reader_mutex);
//                reader_cv.wait(reader_lock, [] { return reader_is_waiting;});
                reader_cv.wait(reader_lock);
            }
        }
    };
    is_running_ = true;
    reader_ = std::thread(work);
    reader_.join();
}

void
WalManager::Stop() {
    is_running_ = false;
    // todo: wait-notify
    reader_cv.notify_one();
}

bool
WalManager::Insert(const std::string &table_id,
                   size_t n,
                   const float *vectors,
                   milvus::engine::IDNumbers &vector_ids) {

    uint32_t max_record_data_size = mxlog_config_.record_size - (uint32_t)SizeOfMXLogRecordHeader;
    auto table_meta = p_table_meta_->find(table_id);
    if (table_meta == p_table_meta_->end()) {
        //todo: get table_meta by table_id and cache it
    }
    uint16_t dim = table_meta->second->dimension_;
    //split and insert into wal
    size_t vectors_per_record = (max_record_data_size - table_id.size()) / ((dim << 2) + 8);
    __glibcxx_assert(vectors_per_record > 0);

    uint64_t new_lsn = 0;
    for (size_t i = 0; i < n; i += vectors_per_record) {
        size_t insert_len = std::min(n - i, vectors_per_record);
        new_lsn = p_buffer_->Append(table_id,
                                    MXLogType::Insert,
                                    insert_len,
                                    dim,
                                    vectors + i * (dim << 2),
                                    vector_ids,
                                    i);
        if(new_lsn == -1) {
            return false;
        }
    }

    last_applied_lsn_ = new_lsn;
    p_meta_handler_->SetMXLogInternalMeta(last_applied_lsn_);
    return true;
}

//TBD
void
WalManager::DeleteById(const std::string& table_id, const milvus::engine::IDNumbers& vector_ids) {
    //todo: similar to insert
}

void
WalManager::Flush(const std::string& table_id) {
    uint32_t max_record_data_size = mxlog_config_.record_size - (uint32_t)SizeOfMXLogRecordHeader;
    auto table_meta = p_table_meta_->find(table_id);
    if (table_meta == p_table_meta_->end()) {
        //todo: get table_meta by table_id and cache it
        // meta_handler_.GetMXLogExternalMeta(p_table_meta_);//pull newest meta
    }
    MXLogType type;
    if (table_id.empty()) {
        type = MXLogType::FlushAll;
    } else {
        type = MXLogType::Flush;
    }
    milvus::engine::IDNumbers useless_vec;
    // todo: flush-wait-notify
    p_buffer_->Append(table_id, type, 0, 0, NULL, useless_vec, -1);
    p_meta_handler_->SetMXLogInternalMeta(last_applied_lsn_);
}

void
WalManager::Recovery() {
    //todo: how to judge that whether system exit normally last time?
    //todo: if (system.exit.normally) return;
    //todo: fetch meta
    auto table_meta = Meta.GetAllMeta();
    p_meta_handler_->GetMXLogInternalMeta(last_applied_lsn_);
    uint64_t current_recovery_point = 0;
//    for (auto it = p_table_meta_->begin(), it != p_table_meta_->end(); ++ it) {
//        start_recovery_point = std::min(start_recovery_point, kv.second);
//    }

    // todo:
    // if (last_applied_lsn_ < current_recovery_point) {
    // read config
    //   case ignore: let wal lsn = largest flused lsn
    //   case try recover: let wal lsn = largest flused lsn
    //   case exit:
    // }

    while (current_recovery_point <= last_applied_lsn_) {
        std::string table_id;
        uint64_t next_lsn;
        p_buffer_->LoadForRecovery(current_recovery_point);
        if (p_buffer_->NextInfo(table_id, next_lsn)) {
            auto meta_schema = p_table_meta_->find(table_id);
            //todo: wait zhiru's interface
//            if (meta_schema->second->lsn < current_recovery_point) {
                Apply(current_recovery_point);
//            }
        }
        current_recovery_point = next_lsn;
    }

    p_buffer_->ReSet();
}

uint64_t WalManager::GetCurrentLsn() {
    return last_applied_lsn_;
}

void
WalManager::Apply(const uint64_t &apply_lsn) {
    std::string table_id;
    MXLogType type;
    size_t num_of_vectors;
    size_t dim;
    float* vectors = NULL;
    milvus::engine::IDNumbers vector_ids;
    uint64_t lsn;
    if (p_buffer_->Next(table_id, type, num_of_vectors, dim, vectors, vector_ids, last_applied_lsn_, lsn)) {
        Dispatch(table_id, type, num_of_vectors, dim, vectors, vector_ids, last_applied_lsn_, lsn);
    } else {
        //todo: log error
    }
}

void
WalManager::Dispatch(std::string &table_id,
                     milvus::engine::wal::MXLogType &mxl_type,
                     size_t &n,
                     size_t &dim,
                     float *vectors,
                     milvus::engine::IDNumbers &vector_ids,
                     const uint64_t &last_applied_lsn,
                     uint64_t &lsn) {

    switch (mxl_type) {
        case MXLogType::Flush : {
//            mem_mgr.GetInstance().Flush(table_id);
            break;
        }
        case MXLogType::FlushAll : {
//            mem_mgr.GetInstance().Flush("");
            break;
        }
        case MXLogType::Insert : {
//            mem_mgr.GetInstance().Insert(table_id, n, vectors, vector_ids);
            break;
        }
        case MXLogType::Delete : {
//            mem_mgr.GetInstance().DeleteById(table_id, vector_ids);
            break;
        }
        default:
            break;
    }
}

} // wal
} // engine
} // milvus
