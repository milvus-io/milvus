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

    ~MemManagerImpl() = default;

    Status
    InsertEntities(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk, idx_t op_id) override;

    Status
    DeleteEntities(int64_t collection_id, const std::vector<idx_t>& entity_ids, idx_t op_id) override;

    Status
    Flush(int64_t collection_id) override;

    Status
    Flush(std::set<int64_t>& collection_ids) override;

    Status
    EraseMem(int64_t collection_id) override;

    Status
    EraseMem(int64_t collection_id, int64_t partition_id) override;

    bool
    RequireFlush(std::set<int64_t>& collection_ids) override;

 private:
    size_t
    GetCurrentMutableMem();

    size_t
    GetCurrentImmutableMem();

    size_t
    GetCurrentMem();

    MemCollectionPtr
    GetMemByCollection(int64_t collection_id);

    Status
    ValidateChunk(int64_t collection_id, const DataChunkPtr& chunk);

    Status
    ToImmutable();

    Status
    ToImmutable(int64_t collection_id);

    Status
    ToImmutable(MemList& mem_list);

    Status
    InternalFlush(std::set<int64_t>& collection_ids);

 private:
    MemCollectionMap mem_map_;
    MemList immu_mem_list_;

    DBOptions options_;
    std::mutex mem_mutex_;
    std::mutex immu_mem_mtx_;
    std::mutex flush_mtx_;
};

}  // namespace engine
}  // namespace milvus
