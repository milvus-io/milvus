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
#include <vector>

#include "db/meta/backend/MetaContext.h"
#include "db/snapshot/ResourceTypes.h"
#include "utils/Status.h"

namespace milvus::engine::meta {

using AttrsMap = std::unordered_map<std::string, std::string>;
using AttrsMapList = std::vector<AttrsMap>;

class MetaEngine {
 public:
    virtual Status
    Query(const MetaQueryContext& context, AttrsMapList& attrs) = 0;

    virtual Status
    ExecuteTransaction(const std::vector<MetaApplyContext>& sql_contexts, std::vector<int64_t>& result_ids) = 0;

    virtual Status
    TruncateAll() = 0;
};

using MetaEnginePtr = std::shared_ptr<MetaEngine>;

}  // namespace milvus::engine::meta
