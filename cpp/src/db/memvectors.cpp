#include <faiss/IndexFlat.h>
#include <faiss/MetaIndexes.h>
#include <faiss/index_io.h>
#include <iostream>
#include <sstream>
#include <thread>

#include "memvectors.h"
#include "db_meta.h"


namespace zilliz {
namespace vecwise {
namespace engine {

MemVectors::MemVectors(size_t dimension_, const std::string& file_location_) :
    _file_location(file_location_),
    _pIdGenerator(new SimpleIDGenerator()),
    _dimension(dimension_),
    _pInnerIndex(new faiss::IndexFlat(_dimension)),
    _pIdMapIndex(new faiss::IndexIDMap(_pInnerIndex)) {
}

void MemVectors::add(size_t n_, const float* vectors_, IDNumbers& vector_ids_) {
    _pIdGenerator->getNextIDNumbers(n_, vector_ids_);
    _pIdMapIndex->add_with_ids(n_, vectors_, &vector_ids_[0]);
    for(auto i=0 ; i<n_; i++) {
        vector_ids_.push_back(i);
    }
}

size_t MemVectors::total() const {
    return _pIdMapIndex->ntotal;
}

size_t MemVectors::approximate_size() const {
    return total() * _dimension;
}

void MemVectors::serialize() {
    /* std::stringstream ss; */
    /* ss << "/tmp/test/" << _pIdGenerator->getNextIDNumber(); */
    /* faiss::write_index(_pIdMapIndex, ss.str().c_str()); */
    /* std::cout << _pIdMapIndex->ntotal << std::endl; */
    /* std::cout << _file_location << std::endl; */
    faiss::write_index(_pIdMapIndex, _file_location.c_str());
}

MemVectors::~MemVectors() {
    if (_pIdGenerator != nullptr) {
        delete _pIdGenerator;
        _pIdGenerator = nullptr;
    }
    if (_pIdMapIndex != nullptr) {
        delete _pIdMapIndex;
        _pIdMapIndex = nullptr;
    }
    if (_pInnerIndex != nullptr) {
        delete _pInnerIndex;
        _pInnerIndex = nullptr;
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

    GroupSchema group_info;
    Status status = _pMeta->get_group(group_id, group_info);
    if (!status.ok()) {
        return nullptr;
    }

    _memMap[group_id] = std::shared_ptr<MemVectors>(new MemVectors(group_info.dimension,
                group_info.next_file_location));
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

Status MemManager::serialize() {
    mark_memory_as_immutable();
    for (auto& mem : _immMems) {
        mem->serialize();
    }
    _immMems.clear();
    return Status::OK();
}


} // namespace engine
} // namespace vecwise
} // namespace zilliz
