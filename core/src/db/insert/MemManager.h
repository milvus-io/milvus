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
#include "utils/Status.h"

namespace milvus {
namespace engine {

class MemManager {
 public:
    virtual Status
    InsertVectors(const std::string& collection_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                  const float* vectors, uint64_t lsn, std::set<std::string>& flushed_tables) = 0;

    virtual Status
    InsertVectors(const std::string& collection_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                  const uint8_t* vectors, uint64_t lsn, std::set<std::string>& flushed_tables) = 0;

    virtual Status
    InsertEntities(const std::string& table_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                   const float* vectors, const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                   const std::unordered_map<std::string, uint64_t>& attr_size,
                   const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data, uint64_t lsn,
                   std::set<std::string>& flushed_tables) = 0;

    virtual Status
    DeleteVector(const std::string& collection_id, IDNumber vector_id, uint64_t lsn) = 0;

    virtual Status
    DeleteVectors(const std::string& collection_id, int64_t length, const IDNumber* vector_ids, uint64_t lsn) = 0;

    virtual Status
    Flush(const std::string& collection_id, bool apply_delete = true) = 0;

    virtual Status
    Flush(std::set<std::string>& table_ids, bool apply_delete = true) = 0;

    //    virtual Status
    //    Serialize(std::set<std::string>& table_ids) = 0;

    virtual Status
    EraseMemVector(const std::string& collection_id) = 0;

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
