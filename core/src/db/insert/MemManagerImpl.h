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

#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/insert/MemCollection.h"
#include "db/insert/MemManager.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class MemManagerImpl : public MemManager {
 public:
    using Ptr = std::shared_ptr<MemManagerImpl>;
    using MemCollectionMap = std::unordered_map<int64_t, MemCollectionPtr>;
    using MemList = std::vector<MemCollectionPtr>;

    explicit MemManagerImpl(const DBOptions& options) : options_(options) {
    }

<<<<<<< HEAD
    ~MemManagerImpl() = default;

    Status
    InsertEntities(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk, idx_t op_id) override;
=======
    Status
    InsertVectors(const std::string& collection_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                  const float* vectors, uint64_t lsn) override;

    Status
    InsertVectors(const std::string& collection_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                  const uint8_t* vectors, uint64_t lsn) override;

    Status
    InsertEntities(const std::string& collection_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                   const float* vectors, const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                   const std::unordered_map<std::string, uint64_t>& attr_size,
                   const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data, uint64_t lsn) override;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    Status
    DeleteEntities(int64_t collection_id, const std::vector<idx_t>& entity_ids, idx_t op_id) override;

    Status
    Flush(int64_t collection_id) override;

    Status
<<<<<<< HEAD
    Flush(std::set<int64_t>& collection_ids) override;

    Status
    EraseMem(int64_t collection_id) override;
=======
    Flush(const std::string& collection_id) override;

    Status
    Flush(std::set<std::string>& collection_ids) override;

    //    Status
    //    Serialize(std::set<std::string>& table_ids) override;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    Status
    EraseMem(int64_t collection_id, int64_t partition_id) override;

    size_t
    GetCurrentMutableMem() override;

    size_t
    GetCurrentImmutableMem() override;

    size_t
    GetCurrentMem() override;

 private:
    MemCollectionPtr
    GetMemByCollection(int64_t collection_id);

    Status
    ValidateChunk(int64_t collection_id, const DataChunkPtr& chunk);

    Status
    InsertEntitiesNoLock(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk, idx_t op_id);

    Status
    ToImmutable();

    Status
    ToImmutable(int64_t collection_id);

    Status
    InternalFlush(std::set<int64_t>& collection_ids);

 private:
    MemCollectionMap mem_map_;
    MemList immu_mem_list_;

    DBOptions options_;
    std::mutex mutex_;
    std::mutex serialization_mtx_;
};

}  // namespace engine
}  // namespace milvus
