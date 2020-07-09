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

#include <mysql++/mysql++.h>

#include "db/Options.h"
#include "db/impl/DBEngine.h"
#include "db/meta/MySQLConnectionPool.h"

namespace milvus::engine {

class MySqlEngine: public DBEngine {
 public:
    explicit
    MySqlEngine(const DBMetaOptions& options): options_(options) {
        Initialize();
    }

    ~MySqlEngine() = default;

    Status
    Query(const std::string& query_sql, AttrsMapList& attrs) override;

    Status
    ExecuteTransaction(const std::vector<SqlContext>& sql_contexts, std::vector<int64_t>& result_ids) override;

 private:
    Status
    Initialize();

 private:
    const DBMetaOptions options_;
//    const int mode_;

    std::shared_ptr<meta::MySQLConnectionPool> mysql_connection_pool_;
    bool safe_grab_ = false;  // Safely graps a connection from mysql pool

//    std::mutex meta_mutex_;
//    std::mutex genid_mutex_;
};

}  // namespace milvus::engine
