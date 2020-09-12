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

<<<<<<< HEAD
#include <fiu/fiu-local.h>
=======
#include <fiu-local.h>
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
#include <thread>

#include "db/Constants.h"
#include "db/snapshot/Snapshots.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

MemCollectionPtr
MemManagerImpl::GetMemByCollection(int64_t collection_id) {
    auto mem_collection = mem_map_.find(collection_id);
    if (mem_collection != mem_map_.end()) {
        return mem_collection->second;
    }

    auto mem = std::make_shared<MemCollection>(collection_id, options_);
    mem_map_[collection_id] = mem;
    return mem;
}

Status
<<<<<<< HEAD
MemManagerImpl::InsertEntities(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk, idx_t op_id) {
    auto status = ValidateChunk(collection_id, chunk);
    if (!status.ok()) {
        return status;
    }
=======
MemManagerImpl::InsertVectors(const std::string& collection_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                              const float* vectors, uint64_t lsn) {
    VectorsData vectors_data;
    vectors_data.vector_count_ = length;
    vectors_data.float_data_.resize(length * dim);
    memcpy(vectors_data.float_data_.data(), vectors, length * dim * sizeof(float));
    vectors_data.id_array_.resize(length);
    memcpy(vectors_data.id_array_.data(), vector_ids, length * sizeof(IDNumber));
    VectorSourcePtr source = std::make_shared<VectorSource>(vectors_data);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    std::unique_lock<std::mutex> lock(mutex_);
    return InsertEntitiesNoLock(collection_id, partition_id, chunk, op_id);
}

Status
<<<<<<< HEAD
MemManagerImpl::ValidateChunk(int64_t collection_id, const DataChunkPtr& chunk) {
    if (chunk == nullptr) {
        return Status(DB_ERROR, "Null chunk pointer");
    }

    snapshot::ScopedSnapshotT ss;
    auto status = snapshot::Snapshots::GetInstance().GetSnapshot(ss, collection_id);
    if (!status.ok()) {
        std::string err_msg = "Could not get snapshot: " + status.ToString();
        LOG_ENGINE_ERROR_ << err_msg;
        return status;
    }
=======
MemManagerImpl::InsertVectors(const std::string& collection_id, int64_t length, const IDNumber* vector_ids, int64_t dim,
                              const uint8_t* vectors, uint64_t lsn) {
    VectorsData vectors_data;
    vectors_data.vector_count_ = length;
    vectors_data.binary_data_.resize(length * dim);
    memcpy(vectors_data.binary_data_.data(), vectors, length * dim * sizeof(uint8_t));
    vectors_data.id_array_.resize(length);
    memcpy(vectors_data.id_array_.data(), vector_ids, length * sizeof(IDNumber));
    VectorSourcePtr source = std::make_shared<VectorSource>(vectors_data);

    std::unique_lock<std::mutex> lock(mutex_);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

    std::vector<std::string> field_names = ss->GetFieldNames();
    for (auto& name : field_names) {
        auto iter = chunk->fixed_fields_.find(name);
        if (iter == chunk->fixed_fields_.end()) {
            std::string err_msg = "Missed chunk field: " + name;
            LOG_ENGINE_ERROR_ << err_msg;
            return Status(DB_ERROR, err_msg);
        }
        if (iter->second == nullptr) {
            continue;
        }

<<<<<<< HEAD
        size_t data_size = iter->second->data_.size();

        snapshot::FieldPtr field = ss->GetField(name);
        auto ftype = static_cast<DataType>(field->GetFtype());
        std::string err_msg = "Illegal data size for chunk field: ";
        switch (ftype) {
            case DataType::BOOL:
                if (data_size != chunk->count_ * sizeof(bool)) {
                    return Status(DB_ERROR, err_msg + name);
                }
                break;
            case DataType::DOUBLE:
                if (data_size != chunk->count_ * sizeof(double)) {
                    return Status(DB_ERROR, err_msg + name);
                }
                break;
            case DataType::FLOAT:
                if (data_size != chunk->count_ * sizeof(float)) {
                    return Status(DB_ERROR, err_msg + name);
                }
                break;
            case DataType::INT8:
                if (data_size != chunk->count_ * sizeof(uint8_t)) {
                    return Status(DB_ERROR, err_msg + name);
                }
                break;
            case DataType::INT16:
                if (data_size != chunk->count_ * sizeof(uint16_t)) {
                    return Status(DB_ERROR, err_msg + name);
                }
                break;
            case DataType::INT32:
                if (data_size != chunk->count_ * sizeof(uint32_t)) {
                    return Status(DB_ERROR, err_msg + name);
                }
                break;
            case DataType::INT64:
                if (data_size != chunk->count_ * sizeof(uint64_t)) {
                    return Status(DB_ERROR, err_msg + name);
                }
                break;
            case DataType::VECTOR_FLOAT:
            case DataType::VECTOR_BINARY: {
                json params = field->GetParams();
                if (params.find(knowhere::meta::DIM) == params.end()) {
                    std::string msg = "Vector field params must contain: dimension";
                    LOG_SERVER_ERROR_ << msg;
                    return Status(DB_ERROR, msg);
                }

                int64_t dimension = params[knowhere::meta::DIM];
                int64_t row_size = (ftype == DataType::VECTOR_BINARY) ? dimension / 8 : dimension * sizeof(float);
                if (data_size != chunk->count_ * row_size) {
                    return Status(DB_ERROR, err_msg + name);
                }

                break;
            }
            default:
                break;
        }
    }

    return Status::OK();
=======
Status
MemManagerImpl::InsertEntities(const std::string& collection_id, int64_t length, const IDNumber* vector_ids,
                               int64_t dim, const float* vectors,
                               const std::unordered_map<std::string, uint64_t>& attr_nbytes,
                               const std::unordered_map<std::string, uint64_t>& attr_size,
                               const std::unordered_map<std::string, std::vector<uint8_t>>& attr_data, uint64_t lsn) {
    VectorsData vectors_data;
    vectors_data.vector_count_ = length;
    vectors_data.float_data_.resize(length * dim);
    memcpy(vectors_data.float_data_.data(), vectors, length * dim * sizeof(float));
    vectors_data.id_array_.resize(length);
    memcpy(vectors_data.id_array_.data(), vector_ids, length * sizeof(IDNumber));

    VectorSourcePtr source = std::make_shared<VectorSource>(vectors_data, attr_nbytes, attr_size, attr_data);

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertEntitiesNoLock(collection_id, source, lsn);
}

Status
MemManagerImpl::InsertVectorsNoLock(const std::string& collection_id, const VectorSourcePtr& source, uint64_t lsn) {
    MemTablePtr mem = GetMemByTable(collection_id);
    mem->SetLSN(lsn);

    auto status = mem->Add(source);
    return status;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
}

Status
MemManagerImpl::InsertEntitiesNoLock(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk,
                                     idx_t op_id) {
    MemCollectionPtr mem = GetMemByCollection(collection_id);

    auto status = mem->Add(partition_id, chunk, op_id);
    return status;
}

Status
MemManagerImpl::DeleteEntities(int64_t collection_id, const std::vector<idx_t>& entity_ids, idx_t op_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    MemCollectionPtr mem = GetMemByCollection(collection_id);

    auto status = mem->Delete(entity_ids, op_id);
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status
<<<<<<< HEAD
MemManagerImpl::Flush(int64_t collection_id) {
=======
MemManagerImpl::Flush(const std::string& collection_id) {
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    ToImmutable(collection_id);

<<<<<<< HEAD
    std::set<int64_t> collection_ids;
    return InternalFlush(collection_ids);
}

Status
MemManagerImpl::Flush(std::set<int64_t>& collection_ids) {
=======
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    auto max_lsn = GetMaxLSN(temp_immutable_list);
    for (auto& mem : temp_immutable_list) {
        LOG_ENGINE_DEBUG_ << "Flushing collection: " << mem->GetTableId();
        auto status = mem->Serialize(max_lsn, true);
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Flush collection " << mem->GetTableId() << " failed";
            return status;
        }
        LOG_ENGINE_DEBUG_ << "Flushed collection: " << mem->GetTableId();
    }

    return Status::OK();
}

Status
MemManagerImpl::Flush(std::set<std::string>& collection_ids) {
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    ToImmutable();

    return InternalFlush(collection_ids);
}

Status
MemManagerImpl::InternalFlush(std::set<int64_t>& collection_ids) {
    MemList temp_immutable_list;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        immu_mem_list_.swap(temp_immutable_list);
    }

    std::unique_lock<std::mutex> lock(serialization_mtx_);
<<<<<<< HEAD
    for (auto& mem : temp_immutable_list) {
        int64_t collection_id = mem->GetCollectionId();
        LOG_ENGINE_DEBUG_ << "Flushing collection: " << collection_id;
        auto status = mem->Serialize();
=======
    collection_ids.clear();
    auto max_lsn = GetMaxLSN(temp_immutable_list);
    for (auto& mem : temp_immutable_list) {
        LOG_ENGINE_DEBUG_ << "Flushing collection: " << mem->GetTableId();
        auto status = mem->Serialize(max_lsn, true);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Flush collection " << collection_id << " failed";
            return status;
        }
<<<<<<< HEAD
        LOG_ENGINE_DEBUG_ << "Flushed collection: " << collection_id;
        collection_ids.insert(collection_id);
=======
        collection_ids.insert(mem->GetTableId());
        LOG_ENGINE_DEBUG_ << "Flushed collection: " << mem->GetTableId();
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    }

    return Status::OK();
}

Status
MemManagerImpl::ToImmutable(int64_t collection_id) {
    std::unique_lock<std::mutex> lock(mutex_);

    auto mem_collection = mem_map_.find(collection_id);
    if (mem_collection != mem_map_.end()) {
        immu_mem_list_.push_back(mem_collection->second);
        mem_map_.erase(mem_collection);
    }

    return Status::OK();
}

Status
MemManagerImpl::ToImmutable() {
    std::unique_lock<std::mutex> lock(mutex_);

    for (auto& mem_collection : mem_map_) {
        immu_mem_list_.push_back(mem_collection.second);
    }
    mem_map_.clear();

    return Status::OK();
}

Status
MemManagerImpl::EraseMem(int64_t collection_id) {
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
MemManagerImpl::EraseMem(int64_t collection_id, int64_t partition_id) {
    {  // erase MemVector from rapid-insert cache
        std::unique_lock<std::mutex> lock(mutex_);
        auto mem_collection = mem_map_.find(collection_id);
        if (mem_collection != mem_map_.end()) {
            mem_collection->second->EraseMem(partition_id);
        }
    }

    {  // erase MemVector from serialize cache
        std::unique_lock<std::mutex> lock(serialization_mtx_);
        MemList temp_list;
        for (auto& mem : immu_mem_list_) {
            mem->EraseMem(partition_id);
        }
    }

    return Status::OK();
}

size_t
MemManagerImpl::GetCurrentMutableMem() {
    size_t total_mem = 0;
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& mem_collection : mem_map_) {
        total_mem += mem_collection.second->GetCurrentMem();
    }
    return total_mem;
}

size_t
MemManagerImpl::GetCurrentImmutableMem() {
    size_t total_mem = 0;
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    for (auto& mem_collection : immu_mem_list_) {
        total_mem += mem_collection->GetCurrentMem();
    }
    return total_mem;
}

size_t
MemManagerImpl::GetCurrentMem() {
    return GetCurrentMutableMem() + GetCurrentImmutableMem();
}

}  // namespace engine
}  // namespace milvus
