#ifndef MEMMANGE_CPP__
#define MEMMANGE_CPP__

#include <iostream>
#include <sstream>
#include <thread>
#include <easylogging++.h>

#include "MemManager.h"
#include "Meta.h"


namespace zilliz {
namespace vecwise {
namespace engine {

template<typename EngineT>
MemVectors<EngineT>::MemVectors(const std::shared_ptr<meta::Meta>& meta_ptr,
        const meta::GroupFileSchema& schema, const Options& options)
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
    for(auto i=0 ; i<n_; i++) {
        vector_ids_.push_back(i);
    }
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
Status MemVectors<EngineT>::serialize(std::string& group_id) {
    group_id = schema_.group_id;
    auto rows = approximate_size();
    pEE_->Serialize();
    schema_.rows = rows;
    schema_.file_type = (rows >= options_.index_trigger_size) ?
        meta::GroupFileSchema::TO_INDEX : meta::GroupFileSchema::RAW;

    auto status = pMeta_->update_group_file(schema_);

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
typename MemManager<EngineT>::VectorsPtr MemManager<EngineT>::get_mem_by_group(
        const std::string& group_id) {
    auto memIt = _memMap.find(group_id);
    if (memIt != _memMap.end()) {
        return memIt->second;
    }

    meta::GroupFileSchema group_file;
    group_file.group_id = group_id;
    auto status = _pMeta->add_group_file(group_file);
    if (!status.ok()) {
        return nullptr;
    }

    _memMap[group_id] = VectorsPtr(new MemVectors<EngineT>(_pMeta, group_file, options_));
    return _memMap[group_id];
}

template<typename EngineT>
Status MemManager<EngineT>::add_vectors(const std::string& group_id_,
        size_t n_,
        const float* vectors_,
        IDNumbers& vector_ids_) {
    std::unique_lock<std::mutex> lock(_mutex);
    return add_vectors_no_lock(group_id_, n_, vectors_, vector_ids_);
}

template<typename EngineT>
Status MemManager<EngineT>::add_vectors_no_lock(const std::string& group_id,
        size_t n,
        const float* vectors,
        IDNumbers& vector_ids) {
    VectorsPtr mem = get_mem_by_group(group_id);
    if (mem == nullptr) {
        return Status::NotFound("Group " + group_id + " not found!");
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
Status MemManager<EngineT>::serialize(std::vector<std::string>& group_ids) {
    mark_memory_as_immutable();
    std::unique_lock<std::mutex> lock(serialization_mtx_);
    std::string group_id;
    group_ids.clear();
    for (auto& mem : _immMems) {
        mem->serialize(group_id);
        group_ids.push_back(group_id);
    }
    _immMems.clear();
    return Status::OK();
}


} // namespace engine
} // namespace vecwise
} // namespace zilliz

#endif
