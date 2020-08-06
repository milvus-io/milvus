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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Types.h"
#include "query/GeneralQuery.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

using TargetFields = std::set<std::string>;

struct ExecutionEngineContext {
    query::QueryPtr query_ptr_;
    QueryResultPtr query_result_;
    TargetFields target_fields_;  // for build index task, which field should be build
};
using ExecutionEngineContextPtr = std::shared_ptr<ExecutionEngineContext>;

class ExecutionEngine {
 public:
    virtual Status
    Load(ExecutionEngineContext& context) = 0;

    virtual Status
    CopyToGpu(uint64_t device_id) = 0;

    virtual Status
    Search(ExecutionEngineContext& context) = 0;

    virtual Status
    BuildIndex() = 0;
};

using ExecutionEnginePtr = std::shared_ptr<ExecutionEngine>;

}  // namespace engine
}  // namespace milvus
