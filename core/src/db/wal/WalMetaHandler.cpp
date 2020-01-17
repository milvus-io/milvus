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
    if (wal_meta_fp_ == nullptr) {
        return false;
    }

    // todo: add crc
    fseek(wal_meta_fp_, 0, SEEK_SET);
    size_t read_len = fread(&wal_lsn, 1, sizeof(wal_lsn), wal_meta_fp_);
    if (read_len != sizeof(wal_lsn)) {
        wal_lsn = 0;
    }
    return true;
}

bool
MXLogMetaHandler::SetMXLogInternalMeta(const uint64_t& wal_lsn) {
    // todo: add crc

    fseek(wal_meta_fp_, 0, SEEK_SET);
    return (sizeof(wal_lsn) == fwrite(&wal_lsn, 1, sizeof(wal_lsn), wal_meta_fp_));
}

}  // namespace wal
}  // namespace engine
}  // namespace milvus
