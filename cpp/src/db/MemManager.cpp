/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "MemManager.h"
#include "Meta.h"
#include "MetaConsts.h"
#include "EngineFactory.h"
#include "metrics/Metrics.h"
#include "Log.h"

#include <iostream>
#include <sstream>
#include <thread>
#include <easylogging++.h>


namespace zilliz {
namespace milvus {
namespace engine {

MemVectors::MemVectors(const std::shared_ptr<meta::Meta> &meta_ptr,
                       const meta::TableFileSchema &schema, const Options &options)
    : meta_(meta_ptr),
      options_(options),
      schema_(schema),
      id_generator_(new SimpleIDGenerator()),
      active_engine_(EngineFactory::Build(schema_.dimension_, schema_.location_, (EngineType) schema_.engine_type_)) {
}


Status MemVectors::Add(size_t n_, const float *vectors_, IDNumbers &vector_ids_) {
    if (active_engine_ == nullptr) {
        return Status::Error("index engine is null");
    }

    auto start_time = METRICS_NOW_TIME;
    id_generator_->GetNextIDNumbers(n_, vector_ids_);
    Status status = active_engine_->AddWithIds(n_, vectors_, vector_ids_.data());
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    server::Metrics::GetInstance().AddVectorsPerSecondGaugeSet(static_cast<int>(n_),
                                                               static_cast<int>(schema_.dimension_),
                                                               total_time);

    return status;
}

size_t MemVectors::RowCount() const {
    if (active_engine_ == nullptr) {
        return 0;
    }

    return active_engine_->Count();
}

size_t MemVectors::Size() const {
    if (active_engine_ == nullptr) {
        return 0;
    }

    return active_engine_->Size();
}

Status MemVectors::Serialize(std::string &table_id) {
    if (active_engine_ == nullptr) {
        return Status::Error("index engine is null");
    }

    table_id = schema_.table_id_;
    auto size = Size();
    auto start_time = METRICS_NOW_TIME;
    active_engine_->Serialize();
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    schema_.size_ = size;

    server::Metrics::GetInstance().DiskStoreIOSpeedGaugeSet(size / total_time);

    schema_.file_type_ = (size >= options_.index_trigger_size) ?
                         meta::TableFileSchema::TO_INDEX : meta::TableFileSchema::RAW;

    auto status = meta_->UpdateTableFile(schema_);

    LOG(DEBUG) << "New " << ((schema_.file_type_ == meta::TableFileSchema::RAW) ? "raw" : "to_index")
               << " file " << schema_.file_id_ << " of size " << (double) (active_engine_->Size()) / (double) meta::M
               << " M";

    if(options_.insert_cache_immediately_) {
        active_engine_->Cache();
    }

    return status;
}

MemVectors::~MemVectors() {
    if (id_generator_ != nullptr) {
        delete id_generator_;
        id_generator_ = nullptr;
    }
}

/*
 * MemManager
 */
MemManager::MemVectorsPtr MemManager::GetMemByTable(
    const std::string &table_id) {
    auto memIt = mem_id_map_.find(table_id);
    if (memIt != mem_id_map_.end()) {
        return memIt->second;
    }

    meta::TableFileSchema table_file;
    table_file.table_id_ = table_id;
    auto status = meta_->CreateTableFile(table_file);
    if (!status.ok()) {
        return nullptr;
    }

    mem_id_map_[table_id] = MemVectorsPtr(new MemVectors(meta_, table_file, options_));
    return mem_id_map_[table_id];
}

Status MemManager::InsertVectors(const std::string &table_id_,
                                 size_t n_,
                                 const float *vectors_,
                                 IDNumbers &vector_ids_) {

    std::unique_lock<std::mutex> lock(mutex_);

    return InsertVectorsNoLock(table_id_, n_, vectors_, vector_ids_);
}

Status MemManager::InsertVectorsNoLock(const std::string &table_id,
                                       size_t n,
                                       const float *vectors,
                                       IDNumbers &vector_ids) {

    MemVectorsPtr mem = GetMemByTable(table_id);
    if (mem == nullptr) {
        return Status::NotFound("Group " + table_id + " not found!");
    }

    //makesure each file size less than index_trigger_size
    if (mem->Size() > options_.index_trigger_size) {
        std::unique_lock<std::mutex> lock(serialization_mtx_);
        immu_mem_list_.push_back(mem);
        mem_id_map_.erase(table_id);
        return InsertVectorsNoLock(table_id, n, vectors, vector_ids);
    } else {
        return mem->Add(n, vectors, vector_ids);
    }
}

Status MemManager::ToImmutable() {
    std::unique_lock<std::mutex> lock(mutex_);
    MemIdMap temp_map;
    for (auto &kv: mem_id_map_) {
        if (kv.second->RowCount() == 0) {
            temp_map.insert(kv);
            continue;//empty vector, no need to serialize
        }
        immu_mem_list_.push_back(kv.second);
    }

    mem_id_map_.swap(temp_map);
    return Status::OK();
}

Status MemManager::Serialize(std::set<std::string> &table_ids) {
    ToImmutable();
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    std::string table_id;
    table_ids.clear();
    for (auto &mem : immu_mem_list_) {
        mem->Serialize(table_id);
        table_ids.insert(table_id);
    }
    immu_mem_list_.clear();
    return Status::OK();
}

Status MemManager::EraseMemVector(const std::string &table_id) {
    {//erase MemVector from rapid-insert cache
        std::unique_lock<std::mutex> lock(mutex_);
        mem_id_map_.erase(table_id);
    }

    {//erase MemVector from serialize cache
        std::unique_lock<std::mutex> lock(serialization_mtx_);
        MemList temp_list;
        for (auto &mem : immu_mem_list_) {
            if (mem->TableId() != table_id) {
                temp_list.push_back(mem);
            }
        }
        immu_mem_list_.swap(temp_list);
    }

    return Status::OK();
}

size_t MemManager::GetCurrentMutableMem() {
    size_t totalMem = 0;
    for (auto &kv : mem_id_map_) {
        auto memVector = kv.second;
        totalMem += memVector->Size();
    }
    return totalMem;
}

size_t MemManager::GetCurrentImmutableMem() {
    size_t totalMem = 0;
    for (auto &memVector : immu_mem_list_) {
        totalMem += memVector->Size();
    }
    return totalMem;
}

size_t MemManager::GetCurrentMem() {
    return GetCurrentMutableMem() + GetCurrentImmutableMem();
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
