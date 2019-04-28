#include <faiss/AutoTune.h>
#include <iostream>
#include <sstream>
#include <thread>

#include <wrapper/Index.h>
#include <cache/CpuCacheMgr.h>
#include <easylogging++.h>

#include "MemManager.h"
#include "Meta.h"


namespace zilliz {
namespace vecwise {
namespace engine {

MemVectors::MemVectors(const std::shared_ptr<meta::Meta>& meta_ptr,
        const meta::GroupFileSchema& schema, const Options& options)
  : pMeta_(meta_ptr),
    options_(options),
    schema_(schema),
    _pIdGenerator(new SimpleIDGenerator()),
    pIndex_(faiss::index_factory(schema_.dimension, "IDMap,Flat")) {
}

void MemVectors::add(size_t n_, const float* vectors_, IDNumbers& vector_ids_) {
    _pIdGenerator->getNextIDNumbers(n_, vector_ids_);
    pIndex_->add_with_ids(n_, vectors_, &vector_ids_[0]);
    for(auto i=0 ; i<n_; i++) {
        vector_ids_.push_back(i);
    }
}

size_t MemVectors::total() const {
    return pIndex_->ntotal;
}

size_t MemVectors::approximate_size() const {
    return total() * schema_.dimension;
}

Status MemVectors::serialize(std::string& group_id) {
    /* std::stringstream ss; */
    /* ss << "/tmp/test/" << _pIdGenerator->getNextIDNumber(); */
    /* faiss::write_index(pIndex_, ss.str().c_str()); */
    /* std::cout << pIndex_->ntotal << std::endl; */
    /* std::cout << _file_location << std::endl; */
    /* faiss::write_index(pIndex_, _file_location.c_str()); */
    group_id = schema_.group_id;
    auto rows = approximate_size();
    write_index(pIndex_.get(), schema_.location.c_str());
    schema_.rows = rows;
    schema_.file_type = (rows >= options_.index_trigger_size) ?
        meta::GroupFileSchema::TO_INDEX : meta::GroupFileSchema::RAW;

    auto status = pMeta_->update_group_file(schema_);

    zilliz::vecwise::cache::CpuCacheMgr::GetInstance(
            )->InsertItem(schema_.location, std::make_shared<Index>(pIndex_));

    return status;
}

MemVectors::~MemVectors() {
    if (_pIdGenerator != nullptr) {
        delete _pIdGenerator;
        _pIdGenerator = nullptr;
    }
}

/*
 * MemManager
 */

VectorsPtr MemManager::get_mem_by_group(const std::string& group_id) {
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

    _memMap[group_id] = std::shared_ptr<MemVectors>(new MemVectors(_pMeta, group_file, options_));
    return _memMap[group_id];
}

Status MemManager::add_vectors(const std::string& group_id_,
        size_t n_,
        const float* vectors_,
        IDNumbers& vector_ids_) {
    std::unique_lock<std::mutex> lock(_mutex);
    return add_vectors_no_lock(group_id_, n_, vectors_, vector_ids_);
}

Status MemManager::add_vectors_no_lock(const std::string& group_id,
        size_t n,
        const float* vectors,
        IDNumbers& vector_ids) {
    std::shared_ptr<MemVectors> mem = get_mem_by_group(group_id);
    if (mem == nullptr) {
        return Status::NotFound("Group " + group_id + " not found!");
    }
    mem->add(n, vectors, vector_ids);

    return Status::OK();
}

Status MemManager::mark_memory_as_immutable() {
    std::unique_lock<std::mutex> lock(_mutex);
    for (auto& kv: _memMap) {
        _immMems.push_back(kv.second);
    }

    _memMap.clear();
    return Status::OK();
}

/* bool MemManager::need_serialize(double interval) { */
/*     if (_immMems.size() > 0) { */
/*         return false; */
/*     } */

/*     auto diff = std::difftime(std::time(nullptr), _last_compact_time); */
/*     if (diff >= interval) { */
/*         return true; */
/*     } */

/*     return false; */
/* } */

Status MemManager::serialize(std::vector<std::string>& group_ids) {
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
