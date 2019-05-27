/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#ifndef MEMMANGE_CPP__
#define MEMMANGE_CPP__

#include <iostream>
#include <sstream>
#include <thread>
#include <easylogging++.h>

#include "MemManager.h"
#include "Meta.h"
#include "MetaConsts.h"


namespace zilliz {
namespace vecwise {
namespace engine {

template<typename EngineT>
MemVectors<EngineT>::MemVectors(const std::shared_ptr<meta::Meta>& meta_ptr,
        const meta::TableFileSchema& schema, const Options& options)
  : pMeta_(meta_ptr),
    options_(options),
    schema_(schema),
    _pIdGenerator(new SimpleIDGenerator()),
    pEE_(new EngineT(schema_.dimension, schema_.location)) {
}

template<typename EngineT>
void MemVectors<EngineT>::add(size_t n_, const float* vectors_, IDNumbers& vector_ids_) {
    _pIdGenerator->getNextIDNumbers(n_, vector_ids_);
    pEE_->AddWithIds(n_, vectors_, vector_ids_.data());
}

template<typename EngineT>
size_t MemVectors<EngineT>::total() const {
    return pEE_->Count();
}

template<typename EngineT>
size_t MemVectors<EngineT>::approximate_size() const {
    return pEE_->Size();
}

template<typename EngineT>
Status MemVectors<EngineT>::serialize(std::string& table_id) {
    table_id = schema_.table_id;
    auto size = approximate_size();
    pEE_->Serialize();
    schema_.size = size;
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
    if (_pIdGenerator != nullptr) {
        delete _pIdGenerator;
        _pIdGenerator = nullptr;
    }
}

/*
 * MemManager
 */

template<typename EngineT>
typename MemManager<EngineT>::MemVectorsPtr MemManager<EngineT>::get_mem_by_group(
        const std::string& table_id) {
    auto memIt = _memMap.find(table_id);
    if (memIt != _memMap.end()) {
        return memIt->second;
    }

    meta::TableFileSchema table_file;
    table_file.table_id = table_id;
    auto status = _pMeta->CreateTableFile(table_file);
    if (!status.ok()) {
        return nullptr;
    }

    _memMap[table_id] = MemVectorsPtr(new MemVectors<EngineT>(_pMeta, table_file, options_));
    return _memMap[table_id];
}

template<typename EngineT>
Status MemManager<EngineT>::add_vectors(const std::string& table_id_,
        size_t n_,
        const float* vectors_,
        IDNumbers& vector_ids_) {
    std::unique_lock<std::mutex> lock(_mutex);
    return add_vectors_no_lock(table_id_, n_, vectors_, vector_ids_);
}

template<typename EngineT>
Status MemManager<EngineT>::add_vectors_no_lock(const std::string& table_id,
        size_t n,
        const float* vectors,
        IDNumbers& vector_ids) {
    MemVectorsPtr mem = get_mem_by_group(table_id);
    if (mem == nullptr) {
        return Status::NotFound("Group " + table_id + " not found!");
    }
    mem->add(n, vectors, vector_ids);

    return Status::OK();
}

template<typename EngineT>
Status MemManager<EngineT>::mark_memory_as_immutable() {
    std::unique_lock<std::mutex> lock(_mutex);
    for (auto& kv: _memMap) {
        _immMems.push_back(kv.second);
    }

    _memMap.clear();
    return Status::OK();
}

template<typename EngineT>
Status MemManager<EngineT>::serialize(std::vector<std::string>& table_ids) {
    mark_memory_as_immutable();
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    std::string table_id;
    table_ids.clear();
    for (auto& mem : _immMems) {
        mem->serialize(table_id);
        table_ids.push_back(table_id);
    }
    _immMems.clear();
    return Status::OK();
}


} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif
