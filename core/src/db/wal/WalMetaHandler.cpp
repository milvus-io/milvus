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

#include "db/wal/WalMetaHandler.h"

#include <cstring>

namespace milvus {
namespace engine {
namespace wal {

MXLogMetaHandler::MXLogMetaHandler(const std::string& internal_meta_file_path) {
    std::string file_full_path = internal_meta_file_path + WAL_META_FILE_NAME;

    wal_meta_fp_ = fopen(file_full_path.c_str(), "r+");
    if (wal_meta_fp_ == nullptr) {
        wal_meta_fp_ = fopen(file_full_path.c_str(), "w");

    } else {
        uint64_t all_wal_lsn[3] = {0, 0, 0};
        auto rt_val = fread(&all_wal_lsn, sizeof(all_wal_lsn), 1, wal_meta_fp_);
        if (rt_val == 1) {
            if (all_wal_lsn[2] == all_wal_lsn[1]) {
                latest_wal_lsn_ = all_wal_lsn[2];
            } else {
                latest_wal_lsn_ = all_wal_lsn[0];
            }
        }
    }
}

MXLogMetaHandler::~MXLogMetaHandler() {
    if (wal_meta_fp_ != nullptr) {
        fclose(wal_meta_fp_);
        wal_meta_fp_ = nullptr;
    }
}

bool
MXLogMetaHandler::GetMXLogInternalMeta(uint64_t& wal_lsn) {
    wal_lsn = latest_wal_lsn_;
    return true;
}

bool
MXLogMetaHandler::SetMXLogInternalMeta(uint64_t wal_lsn) {
    if (wal_meta_fp_ != nullptr) {
        uint64_t all_wal_lsn[3] = {latest_wal_lsn_, wal_lsn, wal_lsn};
        fseek(wal_meta_fp_, 0, SEEK_SET);
        auto rt_val = fwrite(&all_wal_lsn, sizeof(all_wal_lsn), 1, wal_meta_fp_);
        if (rt_val == 1) {
            fflush(wal_meta_fp_);
            latest_wal_lsn_ = wal_lsn;
            return true;
        }
    }
    return false;
}

}  // namespace wal
}  // namespace engine
}  // namespace milvus
