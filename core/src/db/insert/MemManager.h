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
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Types.h"
#include "segment/Segment.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class MemManager {
 public:
    virtual Status
    InsertEntities(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk, uint64_t lsn) = 0;

    virtual Status
    DeleteEntity(int64_t collection_id, IDNumber vector_id, uint64_t lsn) = 0;

    virtual Status
    DeleteEntities(int64_t collection_id, int64_t length, const IDNumber* vector_ids, uint64_t lsn) = 0;

    virtual Status
    Flush(int64_t collection_id) = 0;

    virtual Status
    Flush(std::set<int64_t>& collection_ids) = 0;

    virtual Status
    EraseMem(int64_t collection_id) = 0;

    virtual Status
    EraseMem(int64_t collection_id, int64_t partition_id) = 0;

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
