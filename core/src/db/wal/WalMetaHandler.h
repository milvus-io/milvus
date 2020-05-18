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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "db/meta/Meta.h"
#include "db/meta/MetaFactory.h"
#include "db/meta/MetaTypes.h"
#include "db/wal/WalDefinations.h"
#include "db/wal/WalFileHandler.h"

namespace milvus {
namespace engine {
namespace wal {

extern const char* WAL_META_FILE_NAME;

class MXLogMetaHandler {
 public:
    explicit MXLogMetaHandler(const std::string& internal_meta_file_path);
    ~MXLogMetaHandler();

    bool
    GetMXLogInternalMeta(uint64_t& wal_lsn);

    bool
    SetMXLogInternalMeta(uint64_t wal_lsn);

 private:
    FILE* wal_meta_fp_;
    uint64_t latest_wal_lsn_ = 0;
};

using MXLogMetaHandlerPtr = std::shared_ptr<MXLogMetaHandler>;

}  // namespace wal
}  // namespace engine
}  // namespace milvus
