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

#include "db/insert/SSMemCollection.h"
#include "db/insert/SSMemManager.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class SSMemManagerImpl : public SSMemManager {
 public:
    using Ptr = std::shared_ptr<SSMemManagerImpl>;
    using MemPartitionMap = std::map<int64_t, SSMemCollectionPtr>;
    using MemCollectionMap = std::map<int64_t, MemPartitionMap>;
    using MemList = std::vector<SSMemCollectionPtr>;

    explicit SSMemManagerImpl(const DBOptions& options) : options_(options) {
    }

    ~SSMemManagerImpl() = default;

    Status
    InsertEntities(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk, uint64_t lsn) override;

    Status
    DeleteEntity(int64_t collection_id, IDNumber vector_id, uint64_t lsn) override;

    Status
    DeleteEntities(int64_t collection_id, int64_t length, const IDNumber* vector_ids, uint64_t lsn) override;

    Status
    Flush(int64_t collection_id) override;

    Status
    Flush(std::set<int64_t>& collection_ids) override;

    Status
    EraseMemVector(int64_t collection_id) override;

    Status
    EraseMemVector(int64_t collection_id, int64_t partition_id) override;

    size_t
    GetCurrentMutableMem() override;

    size_t
    GetCurrentImmutableMem() override;

    size_t
    GetCurrentMem() override;

 private:
    SSMemCollectionPtr
    GetMemByTable(int64_t collection_id, int64_t partition_id);

    std::vector<SSMemCollectionPtr>
    GetMemByTable(int64_t collection_id);

    Status
    ValidateChunk(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk);

    Status
    InsertEntitiesNoLock(int64_t collection_id, int64_t partition_id, const SSVectorSourcePtr& source, uint64_t lsn);

    Status
    ToImmutable();

    Status
    ToImmutable(int64_t collection_id);

    uint64_t
    GetMaxLSN(const MemList& tables);

    MemCollectionMap mem_map_;
    MemList immu_mem_list_;

    DBOptions options_;
    std::mutex mutex_;
    std::mutex serialization_mtx_;
};  // NewMemManager

}  // namespace engine
}  // namespace milvus
