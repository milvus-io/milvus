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

#pragma once

#include <unordered_map>
#include "db/meta/Meta.h"
#include "db/meta/MetaTypes.h"
#include "db/meta/MetaFactory.h"
#include "db/wal/WalDefinations.h"
#include "db/wal/WalFileHandler.h"


namespace milvus {
namespace engine {
namespace wal {

static const char* WAL_META_FILE_NAME = "mxlog.meta";

class MXLogMetaHandler {
 public:
    MXLogMetaHandler();
    ~MXLogMetaHandler();

    void GetMXLogInternalMeta(uint64_t& wal_lsn, uint32_t& wal_file_no);
    void SetMXLogInternalMeta(const uint64_t& wal_lsn, const uint32_t& wal_file_no);
    void GetMXLogExternalMeta(TableMetaPtr global_meta);
    void SetMXLogInternalMetaFilePath(const std::string& internal_meta_file_path);

 private:
    MXLogFileHandler wal_meta_;
    uint64_t wal_lsn_;
    uint32_t wal_file_no_;
};


} // wal
} // engine
} // milvus