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

#include <fiu/fiu-local.h>
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
MemManagerImpl::InsertEntities(int64_t collection_id, int64_t partition_id, const DataChunkPtr& chunk, idx_t op_id) {
    auto status = ValidateChunk(collection_id, chunk);
    if (!status.ok()) {
        return status;
    }

    std::lock_guard<std::mutex> lock(mem_mutex_);
    MemCollectionPtr mem = GetMemByCollection(collection_id);
    return mem->Add(partition_id, chunk, op_id);
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
}

Status
MemManagerImpl::DeleteEntities(int64_t collection_id, const std::vector<idx_t>& entity_ids, idx_t op_id) {
    std::lock_guard<std::mutex> lock(mem_mutex_);
    MemCollectionPtr mem = GetMemByCollection(collection_id);

    auto status = mem->Delete(entity_ids, op_id);
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status
MemManagerImpl::Flush(int64_t collection_id) {
    ToImmutable(collection_id);

    std::set<int64_t> collection_ids;
    return InternalFlush(collection_ids);
}

Status
MemManagerImpl::Flush(std::set<int64_t>& collection_ids) {
    ToImmutable();

    return InternalFlush(collection_ids);
}

Status
MemManagerImpl::InternalFlush(std::set<int64_t>& collection_ids) {
    MemList temp_immutable_list;
    {
        std::lock_guard<std::mutex> lock(immu_mem_mtx_);
        immu_mem_list_.swap(temp_immutable_list);
        if (temp_immutable_list.empty()) {
            return Status::OK();
        }
    }

    std::lock_guard<std::mutex> lock(flush_mtx_);
    for (auto& mem : temp_immutable_list) {
        int64_t collection_id = mem->GetCollectionId();
        LOG_ENGINE_DEBUG_ << "Flushing collection: " << collection_id;
        auto status = mem->Serialize();
        if (!status.ok()) {
            LOG_ENGINE_ERROR_ << "Flush collection " << collection_id << " failed";
            return status;
        }
        LOG_ENGINE_DEBUG_ << "Flushed collection: " << collection_id;
        collection_ids.insert(collection_id);
    }

    return Status::OK();
}

Status
MemManagerImpl::ToImmutable(int64_t collection_id) {
    MemList temp_immutable_list;

    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        auto mem_collection = mem_map_.find(collection_id);
        if (mem_collection != mem_map_.end()) {
            temp_immutable_list.push_back(mem_collection->second);
            mem_map_.erase(mem_collection);
        }
    }

    return ToImmutable(temp_immutable_list);
}

Status
MemManagerImpl::ToImmutable() {
    MemList temp_immutable_list;

    {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        for (auto& pair : mem_map_) {
            temp_immutable_list.push_back(pair.second);
        }
        mem_map_.clear();
    }

    return ToImmutable(temp_immutable_list);
}

Status
MemManagerImpl::ToImmutable(MemList& mem_list) {
    // don't use swp() here, since muti-threads could call ToImmutable at same time
    // there will be several 'temp_immutable_list' need to combine into immu_mem_list_
    std::lock_guard<std::mutex> lock(immu_mem_mtx_);
    for (auto& mem : mem_list) {
        immu_mem_list_.push_back(mem);
    }
    mem_list.clear();

    return Status::OK();
}

Status
MemManagerImpl::EraseMem(int64_t collection_id) {
    {  // erase from rapid-insert cache
        std::lock_guard<std::mutex> lock(mem_mutex_);
        mem_map_.erase(collection_id);
    }

    {  // erase from serialize cache
        std::lock_guard<std::mutex> lock(immu_mem_mtx_);
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
    {  // erase from rapid-insert cache
        std::lock_guard<std::mutex> lock(mem_mutex_);
        auto mem_collection = mem_map_.find(collection_id);
        if (mem_collection != mem_map_.end()) {
            mem_collection->second->EraseMem(partition_id);
        }
    }

    {  // erase from serialize cache
        std::lock_guard<std::mutex> lock(immu_mem_mtx_);
        MemList temp_list;
        for (auto& mem : immu_mem_list_) {
            if (mem->GetCollectionId() == collection_id) {
                mem->EraseMem(partition_id);
            }
        }
    }

    return Status::OK();
}

bool
MemManagerImpl::RequireFlush(std::set<int64_t>& collection_ids) {
    bool require_flush = false;
    if (GetCurrentMem() > options_.insert_buffer_size_) {
        std::lock_guard<std::mutex> lock(mem_mutex_);
        for (auto& kv : mem_map_) {
            collection_ids.insert(kv.first);
        }
        require_flush = true;
    }

    return require_flush;
}

size_t
MemManagerImpl::GetCurrentMutableMem() {
    size_t total_mem = 0;
    std::lock_guard<std::mutex> lock(mem_mutex_);
    for (auto& mem_collection : mem_map_) {
        total_mem += mem_collection.second->GetCurrentMem();
    }
    return total_mem;
}

size_t
MemManagerImpl::GetCurrentImmutableMem() {
    size_t total_mem = 0;
    std::lock_guard<std::mutex> lock(immu_mem_mtx_);
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
