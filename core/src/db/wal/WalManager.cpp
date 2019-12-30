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
    //todo: step1 load configuration about wal
    //todo: step2 init p_buffer_;
    //todo: step3 load meta
    //step4 run wal thread
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
    }
    uint16_t dim = table_meta->second->dimension_;
    //split and insert into wal
    size_t vectors_per_record = (max_record_data_size - table_id.size()) / ((dim << 2) + 8);
    size_t i = 0;
    __glibcxx_assert(vectors_per_record > 0);
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
    //todo: current_lsn atomic increase and update system meta
    //todo: consider sync and async flush
    return true;
}

//TBD
void
WalManager::DeleteById(const std::string& table_id, const milvus::engine::IDNumbers& vector_ids) {
    //todo: similar to insert
}

//useless
void
WalManager::Flush(const std::string& table_id) {
    p_buffer_->Flush(table_id);
}

void
WalManager::Recovery() {
    //todo: how to judge that whether system exit normally last time?
    //todo: if (system.exit.normally) return;
    //todo: fetch meta
    is_recoverying = true;
    std::unordered_map<std::string, uint64_t > last_flushed_meta;
    meta_handler_.GetMXLogExternalMeta(last_flushed_meta);
    uint64_t start_recovery_point = 0;
    for (auto &kv : last_flushed_meta) {
        start_recovery_point = std::min(start_recovery_point, kv.second);
    }
    if (meta_handler_){

    }
    p_buffer_->ReSet();
    is_recoverying = false;
}

} // wal
} // engine
} // milvus
