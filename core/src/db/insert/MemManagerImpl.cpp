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

#include <fiu-local.h>
#include <thread>

#include "VectorSource.h"
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
MemManagerImpl::InsertEntities(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk, uint64_t lsn) {
    auto status = ValidateChunk(collection_id, chunk);
    if (!status.ok()) {
        return status;
    }

    VectorSourcePtr source = std::make_shared<VectorSource>(chunk);
    std::unique_lock<std::mutex> lock(mutex_);
    return InsertEntitiesNoLock(collection_id, partition_id, source, lsn);
}

Status
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

        size_t data_size = iter->second->data_.size();

        snapshot::FieldPtr field = ss->GetField(name);
        DataType ftype = static_cast<DataType>(field->GetFtype());
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
        }
    }

    return Status::OK();
}

Status
MemManagerImpl::InsertEntitiesNoLock(int64_t collection_id, int64_t partition_id,
                                     const milvus::engine::VectorSourcePtr& source, uint64_t lsn) {
    MemCollectionPtr mem = GetMemByCollection(collection_id);
    mem->SetLSN(lsn);

    auto status = mem->Add(partition_id, source);
    return status;
}

Status
MemManagerImpl::DeleteEntity(int64_t collection_id, IDNumber engity_id, uint64_t lsn) {
    std::unique_lock<std::mutex> lock(mutex_);
    MemCollectionPtr mem = GetMemByCollection(collection_id);

    mem->SetLSN(lsn);
    IDNumbers ids = {engity_id};
    auto status = mem->Delete(ids);
    if (status.ok()) {
        return status;
    }

    return Status::OK();
}

Status
MemManagerImpl::DeleteEntities(int64_t collection_id, int64_t length, const IDNumber* engity_ids, uint64_t lsn) {
    std::unique_lock<std::mutex> lock(mutex_);
    MemCollectionPtr mem = GetMemByCollection(collection_id);

    mem->SetLSN(lsn);

    IDNumbers ids;
    ids.resize(length);
    memcpy(ids.data(), engity_ids, length * sizeof(IDNumber));

    auto status = mem->Delete(ids);
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status
MemManagerImpl::Flush(int64_t collection_id) {
    ToImmutable(collection_id);

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
MemManagerImpl::Flush(std::set<int64_t>& collection_ids) {
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

uint64_t
MemManagerImpl::GetMaxLSN(const MemList& collections) {
    uint64_t max_lsn = 0;
    for (auto& collection : collections) {
        auto cur_lsn = collection->GetLSN();
        if (collection->GetLSN() > max_lsn) {
            max_lsn = cur_lsn;
        }
    }
    return max_lsn;
}

}  // namespace engine
}  // namespace milvus
