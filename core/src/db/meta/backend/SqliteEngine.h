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
#include <vector>

#include <sqlite3.h>

#include "db/Options.h"
#include "db/meta/backend/MetaEngine.h"
#include "utils/Status.h"

namespace milvus::engine::meta {

class SqliteEngine : public MetaEngine {
 public:
    explicit SqliteEngine(const DBMetaOptions& options);

    ~SqliteEngine();

 public:
    Status
    Query(const MetaQueryContext& context, AttrsMapList& attrs) override;

    Status
    ExecuteTransaction(const std::vector<MetaApplyContext>& sql_contexts, std::vector<int64_t>& result_ids) override;

    Status
    TruncateAll() override;

 private:
    Status
    Initialize();

 private:
    DBMetaOptions options_;
    sqlite3* db_;
    std::mutex meta_mutex_;
};

}  // namespace milvus::engine::meta
