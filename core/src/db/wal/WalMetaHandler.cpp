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

#include <cstring>
#include "WalMetaHandler.h"

namespace milvus {
namespace engine {
namespace wal {

MXLogMetaHandler::MXLogMetaHandler() : wal_lsn_(0), wal_file_no_(0) {

}

void
MXLogMetaHandler::GetMXLogInternalMeta(uint64_t &wal_lsn, uint32_t &wal_file_no) {
    auto meta_file_size = wal_meta_.GetFileSize();
    char *p_meta_buf = (char*)malloc(meta_file_size + 1);
    __glibcxx_assert(p_meta_buf != NULL);
    wal_meta_.Load(p_meta_buf);
    p_meta_buf[meta_file_size] = 0;
    uint64_t tmp_meta[WAL_META_AMOUNT];
    memset(tmp_meta, 0, sizeof(tmp_meta));
    char* p_buf = p_meta_buf;
    int idx = 0;
    do {
        while (*p_buf && (*p_buf) != '\n') {
            tmp_meta[idx] = tmp_meta[idx] * 10 + (*p_buf++ - '0');
        }
        idx ++;
        if (*p_buf)
            ++ p_buf;
    }while (*p_buf);
    //hard code right now
    wal_lsn = tmp_meta[0];
    wal_file_no = (uint32_t)tmp_meta[1];
    free(p_meta_buf);
}

void
MXLogMetaHandler::SetMXLogInternalMeta(const uint64_t &wal_lsn, const uint32_t &wal_file_no) {
    char* p_meta_buf = (char*)malloc(100);
    __glibcxx_assert(p_meta_buf != NULL);
    memset(p_meta_buf, 0, 100);
    sprintf(p_meta_buf, "%uld\n%u\n", wal_lsn, wal_file_no);
    wal_meta_.Write(p_meta_buf, sizeof(p_meta_buf));
    free(p_meta_buf);
}

void
MXLogMetaHandler::GetMXLogExternalMeta(TableMetaPtr global_meta) {
    //todo: wait interfaces from @zhiru
}

void
MXLogMetaHandler::SetMXLogInternalMetaFilePath(const std::string &internal_meta_file_path) {
    wal_meta_.SetFilePath(internal_meta_file_path);
}

} // wal
} // engine
} // milvus
