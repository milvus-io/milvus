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

#include "db/insert/SSMemManagerImpl.h"

#include <fiu-local.h>
#include <thread>

#include "SSVectorSource.h"
#include "db/Constants.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

SSMemCollectionPtr
SSMemManagerImpl::GetMemByTable(int64_t collection_id, int64_t partition_id) {
    auto mem_collection = mem_map_.find(collection_id);
    if (mem_collection != mem_map_.end()) {
        auto mem_partition = mem_collection->second.find(partition_id);
        if (mem_partition != mem_collection->second.end()) {
            return mem_partition->second;
        }
    }

    auto mem = std::make_shared<SSMemCollection>(collection_id, partition_id, options_);
    mem_map_[collection_id][partition_id] = mem;
    return mem;
}

std::vector<SSMemCollectionPtr>
SSMemManagerImpl::GetMemByTable(int64_t collection_id) {
    std::vector<SSMemCollectionPtr> result;
    auto mem_collection = mem_map_.find(collection_id);
    if (mem_collection != mem_map_.end()) {
        for (auto& pair : mem_collection->second) {
            result.push_back(pair.second);
        }
    }
    return result;
}

Status
SSMemManagerImpl::InsertVectors(int64_t collection_id, int64_t partition_id, int64_t length, const IDNumber* vector_ids,
                                int64_t dim, const float* vectors, uint64_t lsn) {
    VectorsData vectors_data;
    vectors_data.vector_count_ = length;
    vectors_data.float_data_.resize(length * dim);
    memcpy(vectors_data.float_data_.data(), vectors, length * dim * sizeof(float));
    vectors_data.id_array_.resize(length);
    memcpy(vectors_data.id_array_.data(), vector_ids, length * sizeof(IDNumber));
    VectorSourcePtr source = std::make_shared<VectorSource>(vectors_data);

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertVectorsNoLock(collection_id, partition_id, source, lsn);
}

Status
SSMemManagerImpl::InsertVectors(int64_t collection_id, int64_t partition_id, int64_t length, const IDNumber* vector_ids,
                                int64_t dim, const uint8_t* vectors, uint64_t lsn) {
    VectorsData vectors_data;
    vectors_data.vector_count_ = length;
    vectors_data.binary_data_.resize(length * dim);
    memcpy(vectors_data.binary_data_.data(), vectors, length * dim * sizeof(uint8_t));
    vectors_data.id_array_.resize(length);
    memcpy(vectors_data.id_array_.data(), vector_ids, length * sizeof(IDNumber));
    SSVectorSourcePtr source = std::make_shared<SSVectorSource>(vectors_data);

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertVectorsNoLock(collection_id, partition_id, source, lsn);
}

Status
SSMemManagerImpl::InsertEntities(int64_t collection_id, int64_t partition_id, int64_t length,
                                 const IDNumber* vector_ids, int64_t dim, const float* vectors,
                                 const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                                 const std::unordered_map<std::string, uint64_t>& attr_size,
                                 const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data, uint64_t lsn) {
    VectorsData vectors_data;
    vectors_data.vector_count_ = length;
    vectors_data.float_data_.resize(length * dim);
    memcpy(vectors_data.float_data_.data(), vectors, length * dim * sizeof(float));
    vectors_data.id_array_.resize(length);
    memcpy(vectors_data.id_array_.data(), vector_ids, length * sizeof(IDNumber));

    SSVectorSourcePtr source = std::make_shared<SSVectorSource>(vectors_data, attr_nbytes, attr_size, attr_data);

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertEntitiesNoLock(collection_id, partition_id, source, lsn);
}

Status
SSMemManagerImpl::InsertVectorsNoLock(int64_t collection_id, int64_t partition_id, const SSVectorSourcePtr& source,
                                      uint64_t lsn) {
    SSMemCollectionPtr mem = GetMemByTable(collection_id, partition_id);
    mem->SetLSN(lsn);

    auto status = mem->Add(source);
    return status;
}

Status
SSMemManagerImpl::InsertEntitiesNoLock(int64_t collection_id, int64_t partition_id,
                                       const milvus::engine::SSVectorSourcePtr& source, uint64_t lsn) {
    SSMemCollectionPtr mem = GetMemByTable(collection_id, partition_id);
    mem->SetLSN(lsn);

    auto status = mem->AddEntities(source);
    return status;
}

Status
SSMemManagerImpl::DeleteVector(int64_t collection_id, IDNumber vector_id, uint64_t lsn) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<SSMemCollectionPtr> mems = GetMemByTable(collection_id);

    for (auto& mem : mems) {
        mem->SetLSN(lsn);
        auto status = mem->Delete(vector_id);
        if (status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
SSMemManagerImpl::DeleteVectors(int64_t collection_id, int64_t length, const IDNumber* vector_ids, uint64_t lsn) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<SSMemCollectionPtr> mems = GetMemByTable(collection_id);

    for (auto& mem : mems) {
        mem->SetLSN(lsn);

        IDNumbers ids;
        ids.resize(length);
        memcpy(ids.data(), vector_ids, length * sizeof(IDNumber));

        auto status = mem->Delete(ids);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status
SSMemManagerImpl::Flush(int64_t collection_id) {
    ToImmutable(collection_id);
    // TODO: There is actually only one memTable in the immutable list
    MemList temp_immutable_list;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        immu_mem_list_.swap(temp_immutable_list);
    }

    std::unique_lock<std::mutex> lock(serialization_mtx_);
    auto max_lsn = GetMaxLSN(temp_immutable_list);
    for (auto& mem : temp_immutable_list) {
        LOG_ENGINE_DEBUG_ << "Flushing collection: " << mem->GetCollectionId();
        auto status = mem->Serialize(max_lsn);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Flush collection " << mem->GetCollectionId() << " failed";
            return status;
        }
        LOG_ENGINE_DEBUG_ << "Flushed collection: " << mem->GetCollectionId();
    }

    return Status::OK();
}

Status
SSMemManagerImpl::Flush(std::set<int64_t>& collection_ids) {
    ToImmutable();

    MemList temp_immutable_list;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        immu_mem_list_.swap(temp_immutable_list);
    }

    std::unique_lock<std::mutex> lock(serialization_mtx_);
    collection_ids.clear();
    auto max_lsn = GetMaxLSN(temp_immutable_list);
    for (auto& mem : temp_immutable_list) {
        LOG_ENGINE_DEBUG_ << "Flushing collection: " << mem->GetCollectionId();
        auto status = mem->Serialize(max_lsn);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Flush collection " << mem->GetCollectionId() << " failed";
            return status;
        }
        collection_ids.insert(mem->GetCollectionId());
        LOG_ENGINE_DEBUG_ << "Flushed collection: " << mem->GetCollectionId();
    }

    // TODO: global lsn?
    //    meta_->SetGlobalLastLSN(max_lsn);

    return Status::OK();
}

Status
SSMemManagerImpl::ToImmutable(int64_t collection_id) {
    std::unique_lock<std::mutex> lock(mutex_);

    auto mem_collection = mem_map_.find(collection_id);
    if (mem_collection != mem_map_.end()) {
        MemPartitionMap temp_map;
        for (auto& mem : mem_collection->second) {
            if (mem.second->Empty()) {
                temp_map.insert(mem);
            } else {
                immu_mem_list_.push_back(mem.second);
            }
        }

        mem_collection->second.swap(temp_map);
        if (temp_map.empty()) {
            mem_map_.erase(mem_collection);
        }
    }

    return Status::OK();
}

Status
SSMemManagerImpl::ToImmutable() {
    std::unique_lock<std::mutex> lock(mutex_);

    for (auto& mem_collection : mem_map_) {
        MemPartitionMap temp_map;
        for (auto& mem : mem_collection.second) {
            if (mem.second->Empty()) {
                temp_map.insert(mem);
            } else {
                immu_mem_list_.push_back(mem.second);
            }
        }

        mem_collection.second.swap(temp_map);
    }

    return Status::OK();
}

Status
SSMemManagerImpl::EraseMemVector(int64_t collection_id) {
    {  // erase MemVector from rapid-insert cache
        std::unique_lock<std::mutex> lock(mutex_);
        mem_map_.erase(collection_id);
    }

    {  // erase MemVector from serialize cache
        std::unique_lock<std::mutex> lock(serialization_mtx_);
        MemList temp_list;
        for (auto& mem : immu_mem_list_) {
            if (mem->GetCollectionId() != collection_id) {
                temp_list.push_back(mem);
            }
        }
        immu_mem_list_.swap(temp_list);
    }

    return Status::OK();
}

Status
SSMemManagerImpl::EraseMemVector(int64_t collection_id, int64_t partition_id) {
    {  // erase MemVector from rapid-insert cache
        std::unique_lock<std::mutex> lock(mutex_);
        auto mem_collection = mem_map_.find(collection_id);
        if (mem_collection != mem_map_.end()) {
            mem_collection->second.erase(partition_id);
            if (mem_collection->second.empty()) {
                mem_map_.erase(collection_id);
            }
        }
    }

    {  // erase MemVector from serialize cache
        std::unique_lock<std::mutex> lock(serialization_mtx_);
        MemList temp_list;
        for (auto& mem : immu_mem_list_) {
            if (mem->GetCollectionId() != collection_id && mem->GetPartitionId() != partition_id) {
                temp_list.push_back(mem);
            }
        }
        immu_mem_list_.swap(temp_list);
    }

    return Status::OK();
}

size_t
SSMemManagerImpl::GetCurrentMutableMem() {
    size_t total_mem = 0;
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& mem_collection : mem_map_) {
        for (auto& mem : mem_collection.second) {
            total_mem += mem.second->GetCurrentMem();
        }
    }
    return total_mem;
}

size_t
SSMemManagerImpl::GetCurrentImmutableMem() {
    size_t total_mem = 0;
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    for (auto& mem_table : immu_mem_list_) {
        total_mem += mem_table->GetCurrentMem();
    }
    return total_mem;
}

size_t
SSMemManagerImpl::GetCurrentMem() {
    return GetCurrentMutableMem() + GetCurrentImmutableMem();
}

uint64_t
SSMemManagerImpl::GetMaxLSN(const MemList& tables) {
    uint64_t max_lsn = 0;
    for (auto& collection : tables) {
        auto cur_lsn = collection->GetLSN();
        if (collection->GetLSN() > max_lsn) {
            max_lsn = cur_lsn;
        }
    }
    return max_lsn;
}

void
SSMemManagerImpl::OnInsertBufferSizeChanged(int64_t value) {
    options_.insert_buffer_size_ = value * GB;
}

}  // namespace engine
}  // namespace milvus
