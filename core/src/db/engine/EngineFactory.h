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

#include "ExecutionEngine.h"
#include "utils/Json.h"
#include "utils/Status.h"

#include <string>

namespace milvus {
namespace engine {

class EngineFactory {
 public:
    static ExecutionEnginePtr
    Build(const std::string& dir_root, const std::string& collection_name, int64_t segment_id);

    // this method distribute fields to multiple groups:
    // put structured fields into one group
    // each vector field as a group
    static void
    GroupFieldsForIndex(const std::string& collection_name, TargetFieldGroups& field_groups);
};

}  // namespace engine
}  // namespace milvus
