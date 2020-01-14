// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "db/Types.h"
#include "utils/Status.h"

#include <memory>
#include <set>
#include <string>

namespace milvus {
namespace engine {

class MemManager {
 public:
    virtual Status
    InsertVectors(const std::string& table_id, VectorsData& vectors) = 0;

    virtual Status
    Serialize(std::set<std::string>& table_ids) = 0;

    virtual Status
    EraseMemVector(const std::string& table_id) = 0;

    virtual size_t
    GetCurrentMutableMem() = 0;

    virtual size_t
    GetCurrentImmutableMem() = 0;

    virtual size_t
    GetCurrentMem() = 0;
};  // MemManagerAbstract

using MemManagerPtr = std::shared_ptr<MemManager>;

}  // namespace engine
}  // namespace milvus
