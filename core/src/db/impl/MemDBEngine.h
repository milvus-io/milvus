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

#include <mutex>
#include <unordered_map>

#include "db/impl/DBEngine.h"
#include "utils/Status.h"

namespace milvus::engine {

class MemDBEngine: public DBEngine {
 private:
    using TableRaw = std::unordered_map<std::string, std::string>;

 public:
    MemDBEngine(){
        Init();
    }

    ~MemDBEngine() = default;

    Status
    Query(const DBQueryContext& context, AttrsMapList& attrs) override;

    Status
    ExecuteTransaction(const std::vector<DBApplyContext>& sql_contexts, std::vector<int64_t>& result_ids) override;

    Status
    TruncateAll() override;

 private:
    void
    Init();

    Status
    QueryNoLock(const DBQueryContext& context, AttrsMapList& attrs);

    Status
    AddNoLock(const DBApplyContext& add_context, int64_t& retult_id);

    Status
    UpdateNoLock(const DBApplyContext& add_context, int64_t& retult_id);

    Status
    DeleteNoLock(const DBApplyContext& add_context, int64_t& retult_id);

 private:
    std::mutex mutex_;
    std::unordered_map<std::string, int64_t> max_ip_map_;
    std::unordered_map<std::string, std::vector<TableRaw>> resources_;
};

}