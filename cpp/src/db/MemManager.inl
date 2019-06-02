/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "MemManager.h"
#include "Meta.h"
#include "MetaConsts.h"
#include "metrics/Metrics.h"

#include <iostream>
#include <sstream>
#include <thread>
#include <easylogging++.h>

namespace zilliz {
namespace vecwise {
namespace engine {

template<typename EngineT>
MemVectors<EngineT>::MemVectors(const std::shared_ptr<meta::Meta>& meta_ptr,
        const meta::TableFileSchema& schema, const Options& options)
  : pMeta_(meta_ptr),
    options_(options),
    schema_(schema),
    pIdGenerator_(new SimpleIDGenerator()),
    pEE_(new EngineT(schema_.dimension, schema_.location)) {
}

template<typename EngineT>
void MemVectors<EngineT>::Add(size_t n_, const float* vectors_, IDNumbers& vector_ids_) {
    pIdGenerator_->GetNextIDNumbers(n_, vector_ids_);
    pEE_->AddWithIds(n_, vectors_, vector_ids_.data());
}

template<typename EngineT>
size_t MemVectors<EngineT>::Total() const {
    return pEE_->Count();
}

template<typename EngineT>
size_t MemVectors<EngineT>::ApproximateSize() const {
    return pEE_->Size();
}

template<typename EngineT>
Status MemVectors<EngineT>::Serialize(std::string& table_id) {
    table_id = schema_.table_id;
    auto size = ApproximateSize();
    auto start_time = METRICS_NOW_TIME;
    pEE_->Serialize();
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    schema_.size = size;

    server::Metrics::GetInstance().DiskStoreIOSpeedGaugeSet(size/total_time);

    schema_.file_type = (size >= options_.index_trigger_size) ?
        meta::TableFileSchema::TO_INDEX : meta::TableFileSchema::RAW;

    auto status = pMeta_->UpdateTableFile(schema_);

    LOG(DEBUG) << "New " << ((schema_.file_type == meta::TableFileSchema::RAW) ? "raw" : "to_index")
        << " file " << schema_.file_id << " of size " << pEE_->Size() / meta::M << " M";

    pEE_->Cache();

    return status;
}

template<typename EngineT>
MemVectors<EngineT>::~MemVectors() {
    if (pIdGenerator_ != nullptr) {
        delete pIdGenerator_;
        pIdGenerator_ = nullptr;
    }
}

/*
 * MemManager
 */

template<typename EngineT>
typename MemManager<EngineT>::MemVectorsPtr MemManager<EngineT>::GetMemByTable(
        const std::string& table_id) {
    auto memIt = memMap_.find(table_id);
    if (memIt != memMap_.end()) {
        return memIt->second;
    }

    meta::TableFileSchema table_file;
    table_file.table_id = table_id;
    auto status = pMeta_->CreateTableFile(table_file);
    if (!status.ok()) {
        return nullptr;
    }

    memMap_[table_id] = MemVectorsPtr(new MemVectors<EngineT>(pMeta_, table_file, options_));
    return memMap_[table_id];
}

template<typename EngineT>
Status MemManager<EngineT>::InsertVectors(const std::string& table_id_,
        size_t n_,
        const float* vectors_,
        IDNumbers& vector_ids_) {
    std::unique_lock<std::mutex> lock(mutex_);
    return InsertVectorsNoLock(table_id_, n_, vectors_, vector_ids_);
}

template<typename EngineT>
Status MemManager<EngineT>::InsertVectorsNoLock(const std::string& table_id,
        size_t n,
        const float* vectors,
        IDNumbers& vector_ids) {
    MemVectorsPtr mem = GetMemByTable(table_id);
    if (mem == nullptr) {
        return Status::NotFound("Group " + table_id + " not found!");
    }
    mem->Add(n, vectors, vector_ids);

    return Status::OK();
}

template<typename EngineT>
Status MemManager<EngineT>::ToImmutable() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& kv: memMap_) {
        immMems_.push_back(kv.second);
    }

    memMap_.clear();
    return Status::OK();
}

template<typename EngineT>
Status MemManager<EngineT>::Serialize(std::vector<std::string>& table_ids) {
    ToImmutable();
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    std::string table_id;
    table_ids.clear();
    for (auto& mem : immMems_) {
        mem->Serialize(table_id);
        table_ids.push_back(table_id);
    }
    immMems_.clear();
    return Status::OK();
}


} // namespace engine
} // namespace vecwise
} // namespace zilliz
