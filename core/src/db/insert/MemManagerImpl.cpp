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

#include <thread>

#include "VectorSource.h"
#include "db/Constants.h"
#include "utils/Log.h"

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
MemManagerImpl::InsertVectors(const std::string& table_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                              const float* vectors, uint64_t lsn, std::set<std::string>& flushed_tables) {
    flushed_tables.clear();
    if (GetCurrentMem() > options_.insert_buffer_size_) {
        ENGINE_LOG_DEBUG << "Insert buffer size exceeds limit. Performing force flush";
        auto status = Flush(flushed_tables);
        if (!status.ok()) {
            return status;
        }
    }

    VectorsData vectors_data;
    vectors_data.vector_count_ = length;
    vectors_data.float_data_.resize(length * dim);
    memcpy(vectors_data.float_data_.data(), vectors, length * dim * sizeof(float));
    vectors_data.id_array_.resize(length);
    memcpy(vectors_data.id_array_.data(), vector_ids, length * sizeof(IDNumber));
    VectorSourcePtr source = std::make_shared<VectorSource>(vectors_data);

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertVectorsNoLock(table_id, source, lsn);
}

Status
MemManagerImpl::InsertVectors(const std::string& table_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                              const uint8_t* vectors, uint64_t lsn, std::set<std::string>& flushed_tables) {
    flushed_tables.clear();
    if (GetCurrentMem() > options_.insert_buffer_size_) {
        ENGINE_LOG_DEBUG << "Insert buffer size exceeds limit. Performing force flush";
        auto status = Flush(flushed_tables);
        if (!status.ok()) {
            return status;
        }
    }

    VectorsData vectors_data;
    vectors_data.vector_count_ = length;
    vectors_data.binary_data_.resize(length * dim);
    memcpy(vectors_data.binary_data_.data(), vectors, length * dim * sizeof(uint8_t));
    vectors_data.id_array_.resize(length);
    memcpy(vectors_data.id_array_.data(), vector_ids, length * sizeof(IDNumber));
    VectorSourcePtr source = std::make_shared<VectorSource>(vectors_data);

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertVectorsNoLock(table_id, source, lsn);
}

Status
MemManagerImpl::InsertVectorsNoLock(const std::string& table_id, const VectorSourcePtr& source, uint64_t lsn) {
    MemTablePtr mem = GetMemByTable(table_id);
    mem->SetLSN(lsn);

    auto status = mem->Add(source);
    return status;
}

Status
MemManagerImpl::DeleteVector(const std::string& table_id, IDNumber vector_id, uint64_t lsn) {
    std::unique_lock<std::mutex> lock(mutex_);
    MemTablePtr mem = GetMemByTable(table_id);
    mem->SetLSN(lsn);
    auto status = mem->Delete(vector_id);
    return status;
}

Status
MemManagerImpl::DeleteVectors(const std::string& table_id, int64_t length, const IDNumber* vector_ids, uint64_t lsn) {
    std::unique_lock<std::mutex> lock(mutex_);
    MemTablePtr mem = GetMemByTable(table_id);
    mem->SetLSN(lsn);

    IDNumbers ids;
    ids.resize(length);
    memcpy(ids.data(), vector_ids, length * sizeof(IDNumber));

    auto status = mem->Delete(ids);
    if (!status.ok()) {
        return status;
    }

    //    // TODO(zhiru): loop for now
    //    for (auto& id : ids) {
    //        auto status = mem->Delete(id);
    //        if (!status.ok()) {
    //            return status;
    //        }
    //    }

    return Status::OK();
}

Status
MemManagerImpl::Flush(const std::string& table_id) {
    ToImmutable(table_id);
    // TODO: There is actually only one memTable in the immutable list
    MemList temp_immutable_list;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        immu_mem_list_.swap(temp_immutable_list);
    }

    std::unique_lock<std::mutex> lock(serialization_mtx_);
    auto max_lsn = GetMaxLSN(temp_immutable_list);
    for (auto& mem : temp_immutable_list) {
        auto status = mem->Serialize(max_lsn);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Flush table " << mem->GetTableId() << " failed";
            return status;
        }
    }

    ENGINE_LOG_DEBUG << "Flushed table " << table_id;

    return Status::OK();
}

Status
MemManagerImpl::Flush(std::set<std::string>& table_ids) {
    ToImmutable();

    MemList temp_immutable_list;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        immu_mem_list_.swap(temp_immutable_list);
    }

    std::unique_lock<std::mutex> lock(serialization_mtx_);
    table_ids.clear();
    auto max_lsn = GetMaxLSN(temp_immutable_list);
    for (auto& mem : temp_immutable_list) {
        ENGINE_LOG_DEBUG << "Flushing table: " << mem->GetTableId();
        auto status = mem->Serialize(max_lsn);
        if (!status.ok()) {
            ENGINE_LOG_ERROR << "Flush table " << mem->GetTableId() << " failed";
            return status;
        }
        table_ids.insert(mem->GetTableId());
        ENGINE_LOG_DEBUG << "Flushed table " << mem->GetTableId();
    }

    meta_->SetGlobalLastLSN(max_lsn);

    return Status::OK();
}

Status
MemManagerImpl::ToImmutable(const std::string& table_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto memIt = mem_id_map_.find(table_id);
    if (memIt != mem_id_map_.end()) {
        if (!memIt->second->Empty()) {
            immu_mem_list_.push_back(memIt->second);
            mem_id_map_.erase(memIt);
        }
        //        std::string err_msg = "Could not find table = " + table_id + " to flush";
        //        ENGINE_LOG_ERROR << err_msg;
        //        return Status(DB_NOT_FOUND, err_msg);
    }

    return Status::OK();
}

Status
MemManagerImpl::ToImmutable() {
    std::unique_lock<std::mutex> lock(mutex_);
    MemIdMap temp_map;
    for (auto& kv : mem_id_map_) {
        if (kv.second->Empty()) {
            // empty table without any deletes, no need to serialize
            temp_map.insert(kv);
        } else {
            immu_mem_list_.push_back(kv.second);
        }
    }

    mem_id_map_.swap(temp_map);
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

uint64_t
MemManagerImpl::GetMaxLSN(const MemList& tables) {
    uint64_t max_lsn = 0;
    for (auto& table : tables) {
        auto cur_lsn = table->GetLSN();
        if (table->GetLSN() > max_lsn) {
            max_lsn = cur_lsn;
        }
    }
    return max_lsn;
}

}  // namespace engine
}  // namespace milvus
