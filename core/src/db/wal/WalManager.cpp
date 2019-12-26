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

}

void
WalManager::Init() {
    //todo: init buffer_ and other vars
}

//no consideration about multi-thread insert
bool
WalManager::Insert(const std::string &table_id,
                   size_t n,
                   const float *vectors,
                   milvus::engine::IDNumbers &vector_ids) {

    uint32_t max_record_data_size = mxlog_config_.record_size - (uint32_t)SizeOfMXLogRecordHeader;
    auto table_meta = table_meta_.find(table_id);
    if (table_meta == table_meta_.end()) {
        //todo: get table_meta by table_id and cache it
    }
    uint16_t dim = table_meta->second->dimension_;
    //split and insert into wal
    size_t vectors_per_record = (max_record_data_size - table_id.size()) / ((dim << 2) + 8);
    size_t i = 0;
    __glibcxx_assert(vectors_per_record > 0);
    for (; i + vectors_per_record < n; i += vectors_per_record) {
        if(!buffer_.Append(table_id, vectors_per_record, dim, vectors + i * (dim << 2), vector_ids, i, current_lsn_)) {
            return false;
        }
    }
    if (i < n) {
        if(!buffer_.Append(table_id, n - i, dim, vectors + i * (dim << 2), vector_ids, i, current_lsn_)) {
            return false;
        }
    }
    //todo: current_lsn atomic increase and update system meta
    //todo: consider sync and async flush
//    ++ current_lsn_;
    return true;
}

//TBD
void
WalManager::DeleteById(const std::string& table_id, const milvus::engine::IDNumbers& vector_ids) {
    //todo: similar to insert
}

//useless
void
WalManager::Flush() {
    buffer_.Flush(current_lsn_);
}

void
WalManager::Recovery() {
    //todo: fetch meta
}

} // wal
} // engine
} // milvus
