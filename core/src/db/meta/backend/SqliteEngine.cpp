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

#include "db/meta/backend/SqliteEngine.h"

#include <iostream>

namespace milvus::engine::meta {

SqliteEngine::SqliteEngine(const DBMetaOptions& options) : options_(options) {
    std::string meta_path = options_.path_ + "/meta.sqlite";
    int rc = sqlite3_open(meta_path.c_str(), &db_);
    if (rc) {
        std::string err = "Cannot open Sqlite database: ";
        err += sqlite3_errmsg(db_);
        std::cerr << err << std::endl;
        throw std::runtime_error(err);
    }

    Initialize();
}

SqliteEngine::~SqliteEngine() {
    sqlite3_close(db_);
}

Status
SqliteEngine::Initialize() {

    return Status::OK();
}

Status
SqliteEngine::Query(const MetaQueryContext& context, AttrsMapList& attrs) {
    return Status();
}

Status
SqliteEngine::ExecuteTransaction(const std::vector<MetaApplyContext>& sql_contexts, std::vector<int64_t>& result_ids) {
    return Status();
}

Status
SqliteEngine::TruncateAll() {
    return Status();
}

}
