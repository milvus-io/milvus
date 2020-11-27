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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/meta/backend/MetaEngine.h"
#include "utils/Status.h"

namespace milvus::engine::meta {

class MockEngine : public MetaEngine {
 private:
    using TableRaw = std::unordered_map<std::string, std::string>;
    using TableEntity = std::pair<std::string, TableRaw>;

 public:
    MockEngine() {
        Init();
    }

    ~MockEngine() = default;

    Status
    Query(const MetaQueryContext& context, AttrsMapList& attrs) override;

    Status
    Filter(const MetaFilterContext& context, AttrsMapList& attrs) override;

    Status
    ExecuteTransaction(const std::vector<MetaApplyContext>& sql_contexts, std::vector<int64_t>& result_ids) override;

    Status
    TruncateAll() override;

 private:
    void
    Init();

    Status
    QueryNoLock(const MetaQueryContext& context, AttrsMapList& attrs);

    Status
    FilterNoLock(const MetaFilterContext &context, AttrsMapList &attrs);

    Status
    AddNoLock(const MetaApplyContext& add_context, int64_t& result_id, TableRaw& pre_raw);

    Status
    UpdateNoLock(const MetaApplyContext& add_context, int64_t& result_id, TableRaw& pre_raw);

    Status
    DeleteNoLock(const MetaApplyContext& add_context, int64_t& result_id, TableRaw& pre_raw);

    Status
    RollBackNoLock(const std::vector<std::pair<MetaContextOp, TableEntity>>& pre_raws);

 private:
    std::mutex mutex_;
    std::unordered_map<std::string, int64_t> max_ip_map_;
    std::unordered_map<std::string, std::vector<TableRaw>> resources_;
};

}  // namespace milvus::engine::meta
