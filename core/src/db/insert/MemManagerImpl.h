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
#include <vector>

#include "MemManager.h"
#include "MemTable.h"
#include "db/meta/Meta.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

class MemManagerImpl : public MemManager {
 public:
    using Ptr = std::shared_ptr<MemManagerImpl>;
    using MemIdMap = std::map<std::string, MemTablePtr>;
    using MemList = std::vector<MemTablePtr>;

    MemManagerImpl(const meta::MetaPtr& meta, const DBOptions& options) : meta_(meta), options_(options) {
    }

    Status
    InsertVectors(const std::string& table_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                  const float* vectors, uint64_t lsn, std::set<std::string>& flushed_tables) override;

    Status
    InsertVectors(const std::string& table_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                  const uint8_t* vectors, uint64_t lsn, std::set<std::string>& flushed_tables) override;

    Status
    DeleteVector(const std::string& table_id, IDNumber vector_id, uint64_t lsn) override;

    Status
    DeleteVectors(const std::string& table_id, int64_t length, const IDNumber* vector_ids, uint64_t lsn) override;

    Status
    Flush(const std::string& table_id) override;

    Status
    Flush(std::set<std::string>& table_ids) override;

    //    Status
    //    Serialize(std::set<std::string>& table_ids) override;

    Status
    EraseMemVector(const std::string& table_id) override;

    size_t
    GetCurrentMutableMem() override;

    size_t
    GetCurrentImmutableMem() override;

    size_t
    GetCurrentMem() override;

 private:
    MemTablePtr
    GetMemByTable(const std::string& table_id);

    Status
    InsertVectorsNoLock(const std::string& table_id, const VectorSourcePtr& source, uint64_t lsn);

    Status
    ToImmutable();

    Status
    ToImmutable(const std::string& table_id);

    uint64_t
    GetMaxLSN(const MemList& tables);

    MemIdMap mem_id_map_;
    MemList immu_mem_list_;
    meta::MetaPtr meta_;
    DBOptions options_;
    std::mutex mutex_;
    std::mutex serialization_mtx_;
};  // NewMemManager

}  // namespace engine
}  // namespace milvus
