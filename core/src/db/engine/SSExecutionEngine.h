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
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Types.h"
#include "db/meta/MetaTypes.h"
#include "query/GeneralQuery.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class SSExecutionEngine {
 public:
    virtual Status
    Load(const query::QueryPtr& query_ptr) = 0;

    virtual Status
    CopyToGpu(uint64_t device_id) = 0;

    virtual Status
    Search(const query::QueryPtr& query_ptr, QueryResult& result) = 0;

    virtual Status
    BuildIndex(const std::string& field_name, const CollectionIndex& index, knowhere::VecIndexPtr& new_index) = 0;
};

using SSExecutionEnginePtr = std::shared_ptr<SSExecutionEngine>;

}  // namespace engine
}  // namespace milvus
