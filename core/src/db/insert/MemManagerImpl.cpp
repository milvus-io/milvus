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

#include "db/insert/MemManagerImpl.h"
#include "VectorSource.h"
#include "db/Constants.h"
#include "utils/Log.h"

#include <thread>

namespace milvus {
namespace engine {

MemTablePtr
MemManagerImpl::GetMemByTable(const std::string& table_id) {
    auto memIt = mem_id_map_.find(table_id);
    if (memIt != mem_id_map_.end()) {
        return memIt->second;
    }

    mem_id_map_[table_id] = std::make_shared<MemTable>(table_id, meta_, options_);
    return mem_id_map_[table_id];
}

Status
MemManagerImpl::InsertVectors(const std::string& table_id, VectorsData& vectors) {
    while (GetCurrentMem() > options_.insert_buffer_size_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertVectorsNoLock(table_id, vectors);
}

Status
MemManagerImpl::InsertVectorsNoLock(const std::string& table_id, VectorsData& vectors) {
    MemTablePtr mem = GetMemByTable(table_id);
    VectorSourcePtr source = std::make_shared<VectorSource>(vectors);

    auto status = mem->Add(source);
    if (status.ok()) {
        if (vectors.id_array_.empty()) {
            vectors.id_array_ = source->GetVectorIds();
        }
    }
    return status;
}

Status
MemManagerImpl::ToImmutable() {
    std::unique_lock<std::mutex> lock(mutex_);
    MemIdMap temp_map;
    for (auto& kv : mem_id_map_) {
        if (kv.second->Empty()) {
            // empty table, no need to serialize
            temp_map.insert(kv);
        } else {
            immu_mem_list_.push_back(kv.second);
        }
    }

    mem_id_map_.swap(temp_map);
    return Status::OK();
}

Status
MemManagerImpl::Serialize(std::set<std::string>& table_ids) {
    ToImmutable();
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    table_ids.clear();
    for (auto& mem : immu_mem_list_) {
        mem->Serialize();
        table_ids.insert(mem->GetTableId());
    }
    immu_mem_list_.clear();
    return Status::OK();
}

Status
MemManagerImpl::EraseMemVector(const std::string& table_id) {
    {  // erase MemVector from rapid-insert cache
        std::unique_lock<std::mutex> lock(mutex_);
        mem_id_map_.erase(table_id);
    }

    {  // erase MemVector from serialize cache
        std::unique_lock<std::mutex> lock(serialization_mtx_);
        MemList temp_list;
        for (auto& mem : immu_mem_list_) {
            if (mem->GetTableId() != table_id) {
                temp_list.push_back(mem);
            }
        }
        immu_mem_list_.swap(temp_list);
    }

    return Status::OK();
}

size_t
MemManagerImpl::GetCurrentMutableMem() {
    size_t total_mem = 0;
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& kv : mem_id_map_) {
        auto memTable = kv.second;
        total_mem += memTable->GetCurrentMem();
    }
    return total_mem;
}

size_t
MemManagerImpl::GetCurrentImmutableMem() {
    size_t total_mem = 0;
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    for (auto& mem_table : immu_mem_list_) {
        total_mem += mem_table->GetCurrentMem();
    }
    return total_mem;
}

size_t
MemManagerImpl::GetCurrentMem() {
    return GetCurrentMutableMem() + GetCurrentImmutableMem();
}

}  // namespace engine
}  // namespace milvus
